%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2018 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc this module implements state machine that decides which
%% vbucket moves can be started when and when necessary view
%% compactions can be performed.
%%
%% Overall idea is we want to move as many vbuckets as possible in
%% parallel but there are certain limits that we still need to
%% enforce. More below.
%%
%% Input is old and new vbucket map, from which it computes moves as
%% well as 2 parameters that describe concurrency limits.
%%
%% First limit is number of concurrent backfills into/out-of any
%% node. The idea is moving vbucket involves reading entire vbucket
%% from disk and sending it to destination node where entire vbucket
%% needs to be persisted. While this phase of vbucket move occurs
%% between this two nodes it's undesirable to do backfill phase
%% affecting any of those two nodes concurrently. We support limit
%% higher than 1, but in actual product it's 1.
%%
%% Second limit is how many vbucket we move into/out-of any node
%% before pausing moves and forcing views compaction.
%%
%% Current model of actions required as part of vbucket move are:
%%
%% a) build complete replica of vbucket on future master (backfill
%% phase). For this phase as pointed out above we have first limit
%% that affects both old master and future master. Note: we
%% consciously ignore the fact that we can also have incoming
%% backfills into future replicas in this phase. Those backfills
%% currently are currently not affected by or affect any limits.
%%
%% b) ensure that indexes are built for new vbucket on new master and
%% rest of vbucket takeover. That phase notably can happen
%% concurrently for many vbuckets on any node for both incoming and
%% outgoing vbucket moves. We actually try to pack as many of them as
%% possible so that indexer which is currently slowest part of
%% rebalance is always busy.
%%
%% c) (involves multiple vbucket moves at once) do view
%% compaction. This phase _cannot_ happen concurrently with any
%% vbucket moves. I.e. we want views to be as quiescent as possible
%% (i.e. no massive indexing of incoming vbucket moves at least). As
%% noted above we try to do several vbucket moves before pausing for
%% views compactions. Because compacting after every single vbucket
%% move is expensive.
%%
%% See image below (drawn by Aaron Miller. Many thanks):
%%
%%           VBucket Move Scheduling
%% Time
%%
%%   |   /------------\
%%   |   | Backfill 0 |                       Backfills cannot happen
%%   |   \------------/                       concurrently.
%%   |         |             /------------\
%%   |   +------------+      | Backfill 1 |
%%   |   | Index File |      \------------/
%%   |   |     0      |            |
%%   |   |            |      +------------+   However, indexing _can_ happen
%%   |   |            |      | Index File |   concurrently with backfills and
%%   |   |            |      |     1      |   other indexing.
%%   |   |            |      |            |
%%   |   +------------+      |            |
%%   |         |             |            |
%%   |         |             +------------+
%%   |         |                   |
%%   |         \---------+---------/
%%   |                   |
%%   |   /--------------------------------\   Compaction for a set of vbucket moves
%%   |   |  Compact both source and dest. |   cannot happen concurrently with other
%%   v   \--------------------------------/   vbucket moves.
%%
%%
%% In that image you can see that backfills of 2 vbuckets between same
%% pair of nodes cannot happen concurrently, but next phase is
%% concurrent, after which there's view compaction on both nodes that
%% logically affect both moves (and prevent other concurrent moves)
%%
%% vbucket moves are picked w.r.t. this 2 constrains and we also have
%% heuristics to decide which moves to proceed based on the following
%% understanding of goodness:
%%
%% a) we want to start moving active vbuckets sooner. I.e. prioritize
%% moves that change master node and not just replicas. So that
%% balance w.r.t. node's load on GETs and SETs is more quickly
%% equalized.
%%
%% b) given that indexer is our bottleneck we want as much as possible
%% nodes to do some indexing work all or most of the time

-module(vbucket_move_scheduler).

-include("ns_common.hrl").

-export([prepare/6,
         is_done/1,
         choose_action/1,
         get_moves/1,
         extract_progress/1,
         note_backfill_done/2,
         note_move_completed/2,
         note_compaction_done/2]).

-type move() :: {VBucket :: vbucket_id(),
                 ChainBefore :: [node() | undefined],
                 ChainAfter :: [node() | undefined],
                 Quirks :: [rebalance_quirks:quirk()]}.

%% all possible types of actions are moves and compactions
-type action() :: {move, move()} |
                  {compact, node()}.

-record(restrictions, {
          backfills_limit :: non_neg_integer(),
          moves_before_compaction :: non_neg_integer(),
          moves_limit :: non_neg_integer()
         }).

-record(state, {
          restrictions :: #restrictions{},
          moves_left_count_per_node :: dict:dict(), % node() -> non_neg_integer()
          moves_left :: [move()],
          in_flight_backfills :: [move()],
          in_flight_moves :: [move()],

          %% pending moves when current master is undefined For them
          %% we don't have any limits and compaction is not needed.
          %% And that's first moves that we ever consider doing
          moves_from_undefineds :: [move()],

          compaction_countdown_per_node :: dict:dict(), % node() -> non_neg_integer()
          in_flight_per_node :: dict:dict(),            % node() -> non_neg_integer() (I.e. counts current moves)
          in_flight_compactions :: set:set(),           % set of nodes

          initial_move_counts :: dict:dict(),
          left_move_counts :: dict:dict()
         }).

%% @doc prepares state (list of moves etc) based on current and target map
prepare(CurrentMap, TargetMap, Quirks,
        BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves) ->
    %% Dictionary mapping old node to vbucket and new node
    MapTriples = lists:zip3(lists:seq(0, length(CurrentMap) - 1),
                            CurrentMap,
                            TargetMap),

    {Moves, UndefinedMoves, TrivialMoves} =
        lists:foldl(
          fun ({V, C1, C2}, {MovesAcc, UndefinedMovesAcc, TrivialMovesAcc}) ->
                  OldMaster = hd(C1),
                  case OldMaster of
                      undefined ->
                          Move = {V, C1, C2, []},
                          {MovesAcc, [Move | UndefinedMovesAcc], TrivialMovesAcc};
                      _ ->
                          MoveQuirks   = rebalance_quirks:get_node_quirks(OldMaster, Quirks),
                          TrivialMoves = rebalance_quirks:is_enabled(trivial_moves, MoveQuirks),

                          case C1 =:= C2 andalso not TrivialMoves of
                              true ->
                                  {MovesAcc, UndefinedMovesAcc, TrivialMovesAcc + 1};
                              false ->
                                  Move = {V, C1, C2, MoveQuirks},
                                  {[Move | MovesAcc], UndefinedMovesAcc, TrivialMovesAcc}
                          end
                  end
          end, {[], [], 0}, MapTriples),

    MovesPerNode =
        lists:foldl(
          fun ({_V, [Src|_], [Dst|_], _}, Acc) ->
                  case Src =:= Dst of
                      true ->
                          %% no index changes will be done here
                          Acc;
                      _ ->
                          D = dict:update_counter(Src, 1, Acc),
                          dict:update_counter(Dst, 1, D)
                  end
          end, dict:new(), Moves),

    InitialMoveCounts =
        lists:foldl(
          fun ({_V, [Src|_], [Dst|_], _}, Acc) ->
                  D = dict:update_counter(Src, 1, Acc),
                  dict:update_counter(Dst, 1, D)
          end, dict:new(), Moves),

    CompactionCountdownPerNode = dict:map(fun (_K, _V) ->
                                                  MovesBeforeCompaction
                                          end, InitialMoveCounts),

    InFlight = dict:map(fun (_K, _V) -> 0 end, InitialMoveCounts),
    Restrictions = #restrictions{
                      backfills_limit = BackfillsLimit,
                      moves_before_compaction = MovesBeforeCompaction,
                      moves_limit = MaxInflightMoves},

    State = #state{restrictions = Restrictions,
                   moves_left_count_per_node = MovesPerNode,
                   moves_left = Moves,
                   moves_from_undefineds = UndefinedMoves,
                   compaction_countdown_per_node = CompactionCountdownPerNode,
                   in_flight_per_node = InFlight,
                   in_flight_compactions = sets:new(),
                   in_flight_backfills = [],
                   in_flight_moves = [],
                   initial_move_counts = InitialMoveCounts,
                   left_move_counts = InitialMoveCounts},

    ?log_debug("The following count of vbuckets do not need to be moved "
               "at all: ~p", [TrivialMoves]),
    ?log_debug("The following moves are planned:~n~p",
               [UndefinedMoves ++ Moves]),
    State.

get_moves(#state{moves_left = Moves,
                 moves_from_undefineds = UndefinedMoves}) ->
    {Moves, UndefinedMoves}.

%% @doc true iff we're done. NOTE: is_done is only valid if
%% choose_action returned empty actions list
is_done(#state{moves_left = MovesLeft,
               moves_from_undefineds = UndefinedMoves,
               in_flight_moves = InFlightMoves,
               in_flight_compactions = InFlightCompactions} = _State) ->
    MovesLeft =:= [] andalso UndefinedMoves =:= []
        andalso InFlightMoves =:= [] andalso sets:new() =:= InFlightCompactions.

updatef(Record, Field, Body) ->
    V = erlang:element(Field, Record),
    NewV = Body(V),
    erlang:setelement(Field, Record, NewV).

consider_starting_compaction(State) ->
    MovesBeforeCompaction =
        State#state.restrictions#restrictions.moves_before_compaction,
    dict:fold(
      fun (Node, Counter, Acc0) ->
              CanDo0 = dict:fetch(Node, State#state.in_flight_per_node) =:= 0,
              CanDo1 = CanDo0 andalso not sets:is_element(Node, State#state.in_flight_compactions),
              CanDo2 = CanDo1 andalso
                       (Counter =:= 0 orelse
                        (Counter < MovesBeforeCompaction andalso
                         dict:fetch(
                           Node, State#state.moves_left_count_per_node) =:= 0)),
              case CanDo2 of
                  true ->
                      [Node | Acc0];
                  _ ->
                      Acc0
              end
      end, [], State#state.compaction_countdown_per_node).

%% builds list of actions to do now (in passed state) and returns it
%% with new state (assuming actions are started)
-spec choose_action(#state{}) -> {[action()], #state{}}.
choose_action(#state{moves_from_undefineds = [_|_] = Moves,
                     in_flight_moves = InFlightMoves} = State) ->
    NewState = State#state{moves_from_undefineds = [],
                           in_flight_moves = InFlightMoves ++ Moves},
    {OtherActions, NewState2} = choose_action(NewState),
    {OtherActions ++ [{move, M} || M <- Moves], NewState2};
choose_action(State) ->
    MovesBeforeCompaction =
        State#state.restrictions#restrictions.moves_before_compaction,
    Nodes = consider_starting_compaction(State),
    NewState = updatef(State, #state.in_flight_compactions,
                       fun (InFlightCompactions) ->
                               lists:foldl(fun sets:add_element/2, InFlightCompactions, Nodes)
                       end),
    NewState1 = updatef(NewState, #state.compaction_countdown_per_node,
                        fun (CompactionCountdownPerNode) ->
                                lists:foldl(
                                  fun (N, D0) ->
                                          dict:store(N, MovesBeforeCompaction,
                                                     D0)
                                  end, CompactionCountdownPerNode, Nodes)
                        end),
    {OtherActions, NewState2} = choose_action_not_compaction(NewState1),
    Actions = [{compact, N} || N <- Nodes] ++ OtherActions,

    {Actions, NewState2}.

sortby(List, KeyFn, LessEqFn) ->
    KeyedList = [{KeyFn(E), E} || E <- List],
    KeyedSorted = lists:sort(fun ({KA, _}, {KB, _}) ->
                                     LessEqFn(KA, KB)
                             end, KeyedList),
    [E || {_, E} <- KeyedSorted].

move_is_possible([Src | _] = OldChain,
                 [Dst | _] = NewChain,
                 NowBackfills, CompactionCountdown, InFlightMoves,
                 NowCompactions,
                 #restrictions{
                    backfills_limit = BackfillsLimit,
                    moves_limit = InFlightMovesLimit}) ->
    dict:fetch(Src, CompactionCountdown) > 0
        andalso dict:fetch(Dst, CompactionCountdown) > 0
        andalso dict:fetch(Dst, InFlightMoves) < InFlightMovesLimit
        andalso dict:fetch(Src, InFlightMoves) < InFlightMovesLimit
        andalso lists:all(fun (N) ->
                                  Val = case dict:find(N, NowBackfills) of
                                            {ok, V} ->
                                                V;
                                            _ ->
                                                %% Node not involved in
                                                %% in-progress backfills
                                                0
                                        end,
                                  Val < BackfillsLimit
                          end, backfill_nodes(OldChain, NewChain))
        andalso not sets:is_element(Src, NowCompactions)
        andalso not sets:is_element(Dst, NowCompactions).

backfill_nodes([OldMaster | _] = OldChain,
               [NewMaster | NewReplicas]) ->
    %% The old master is always charged a backfill as long as it exists. The
    %% reasons are:
    %%   - it's almost certain that it'll need to stream a lot of stuff
    %%   - if the master changes, it will also have to clean up views
    [OldMaster || OldMaster =/= undefined] ++
        %% The new master is charged a backfill as long as the vbucket is
        %% moved from a different node, even if the new master already has a
        %% copy. That's because views might need to be built, and that's
        %% expensive.
        [NewMaster || NewMaster =/= OldMaster] ++
        %% All replica nodes are charged a backfill as long as they don't
        %% already have the vbucket. This is to ensure that that we don't have
        %% lots of "free" replica moves into a node, which can significantly
        %% affect clients.
        [N || N <- NewReplicas,
              N =/= undefined,
              not lists:member(N, OldChain)].

increment_counter(Node, Node, Dict) ->
    dict:update_counter(Node, 1, Dict);
increment_counter(Src, Dst, Dict) ->
    dict:update_counter(Dst, 1, dict:update_counter(Src, 1, Dict)).

decrement_counter_if_active_move(Node, Node, Dict) ->
    Dict;
decrement_counter_if_active_move(Src, Dst, Dict) ->
    dict:update_counter(Dst, -1, dict:update_counter(Src, -1, Dict)).

update_counter_nodes(Nodes, Value, Dict) ->
    lists:foldl(fun (N, Acc) ->
                        dict:update_counter(N, Value, Acc)
                end, Dict, Nodes).

choose_action_not_compaction(#state{
                                restrictions = Restrictions,
                                in_flight_per_node = NowInFlight,
                                in_flight_compactions = NowCompactions,
                                moves_left_count_per_node = LeftCount,
                                moves_left = MovesLeft,
                                in_flight_backfills = CurrentBackfills,
                                in_flight_moves = CurrentMoves,
                                compaction_countdown_per_node = CompactionCountdown} = State) ->
    NowBackfills = lists:foldl(
                     fun ({_V, OldChain, NewChain, _Quirks}, Acc) ->
                             BackfillNodes = backfill_nodes(OldChain, NewChain),
                             update_counter_nodes(BackfillNodes, 1, Acc)
                     end, dict:new(), CurrentBackfills),
    PossibleMoves = lists:filter(
                      fun ({_V, OldChain, NewChain, _}) ->
                              move_is_possible(OldChain, NewChain,
                                               NowBackfills,
                                               CompactionCountdown,
                                               NowInFlight,
                                               NowCompactions,
                                               Restrictions)
                      end, MovesLeft),

    GoodnessFn =
        fun ({Vb, [Src | _], [Dst | _], _}) ->
                case Src =:= Dst of
                    true ->
                        %% we under-prioritize moves that
                        %% don't actually move active
                        %% position. Because a) they don't
                        %% affect indexes and we want indexes
                        %% to be build as early and as in
                        %% parallel as possible and b)
                        %% because we want to encourage
                        %% earlier improvement of balance
                        %% w.r.t active vbuckets to equalize
                        %% GET/SET load earlier
                        Vb;
                    _ ->
                        %% our goal is to keep indexer on all nodes
                        %% busy as much as possible at all times. Thus
                        %% we prefer nodes with least current
                        %% moves. And destination is more important
                        %% because on source is it's just cleanup and
                        %% thus much less work
                        NoCompactionsG = 10000 - 10 * dict:fetch(Dst, NowInFlight) - dict:fetch(Src, NowInFlight),
                        %% all else equals we don't want to delay
                        %% index compactions
                        G2 = NoCompactionsG * 100 - dict:fetch(Dst, CompactionCountdown) - dict:fetch(Src, CompactionCountdown),
                        G3 = G2 * 10000 + dict:fetch(Dst, LeftCount) + dict:fetch(Src, LeftCount),
                        G3 * 10000 + Vb
                end
        end,

    LessEqFn = fun (GoodnessA, GoodnessB) -> GoodnessA >= GoodnessB end,
    SortedMoves = sortby(PossibleMoves, GoodnessFn, LessEqFn),

    %% NOTE: we know that first move is always allowed
    {SelectedMoves, _NewNowBackfills, NewCompactionCountdown, NewNowInFlight, NewLeftCount} =
        misc:letrec(
          [SortedMoves, NowBackfills, CompactionCountdown, NowInFlight, LeftCount, []],
          fun (Rec, [{_V, [Src|_] = OldChain, [Dst|_] = NewChain, _} = Move | RestMoves],
               NowBackfills0, CompactionCountdown0, NowInFlight0, LeftCount0, Acc) ->
                  case move_is_possible(OldChain, NewChain, NowBackfills0,
                                        CompactionCountdown0, NowInFlight0,
                                        NowCompactions, Restrictions) of
                      true ->
                          NewNowBackfills =
                              lists:foldl(
                                fun (N, Acc0) ->
                                        dict:update_counter(N, 1, Acc0)
                                end, NowBackfills0, backfill_nodes(OldChain, NewChain)),

                          Rec(Rec, RestMoves,
                              NewNowBackfills,
                              decrement_counter_if_active_move(Src, Dst, CompactionCountdown0),
                              increment_counter(Src, Dst, NowInFlight0),
                              decrement_counter_if_active_move(Src, Dst, LeftCount0),
                              [Move | Acc]);
                      _ ->
                          Rec(Rec, RestMoves, NowBackfills0, CompactionCountdown0, NowInFlight0,
                              LeftCount0, Acc)
                  end;
              (_Rec, [], NowBackfills0, MovesBeforeCompaction0, NowInFlight0, LeftCount0, Acc) ->
                  {Acc, NowBackfills0, MovesBeforeCompaction0, NowInFlight0, LeftCount0}
          end),

    NewMovesLeft = MovesLeft -- SelectedMoves,

    NewState = State#state{in_flight_per_node = NewNowInFlight,
                           moves_left_count_per_node = NewLeftCount,
                           moves_left = NewMovesLeft,
                           in_flight_backfills = CurrentBackfills ++ SelectedMoves,
                           in_flight_moves = CurrentMoves ++ SelectedMoves,
                           compaction_countdown_per_node = NewCompactionCountdown},

    {[{move, M} || M <- SelectedMoves], NewState}.

extract_progress(#state{initial_move_counts = InitialCounts,
                        left_move_counts = LeftCounts} = _State) ->
    dict:map(fun (Node, ThisInitialCount) ->
                     ThisLeftCount = dict:fetch(Node, LeftCounts),
                     1.0 - ThisLeftCount / ThisInitialCount
             end, InitialCounts).

%% @doc marks backfill phase of previously started move as done. Users
%% of this code will call it when backfill is done to update state so
%% that next moves can be started.
note_backfill_done(State, {move, {_, [undefined | _], _, _}}) ->
    State;
note_backfill_done(State, {move, {VB, _, _, _}}) ->
    updatef(State, #state.in_flight_backfills,
            fun (CurrentBackfills) ->
                    lists:keydelete(VB, 1, CurrentBackfills)
            end).

%% @doc marks entire move that was previously started done. NOTE: this
%% assumes that backfill phase of this move was previously marked as
%% done. Users of this code will call it when move is done to update
%% state so that next moves and/or compactions can be started.
note_move_completed(State, {move, {VB, [undefined|_], [_Dst|_], _}}) ->
    updatef(State, #state.in_flight_moves,
            fun (CurrentMoves) ->
                    lists:keydelete(VB, 1, CurrentMoves)
            end);
note_move_completed(State, {move, {VB, [Src|_], [Dst|_], _}}) ->
    State0 = updatef(State, #state.in_flight_moves,
                     fun (CurrentMoves) ->
                             lists:keydelete(VB, 1, CurrentMoves)
                     end),
    State1 =
        updatef(State0, #state.in_flight_per_node,
                fun (NowInFlight) ->
                        NowInFlight1 = dict:update_counter(Src, -1, NowInFlight),
                        case Src =:= Dst of
                            true ->
                                NowInFlight1;
                            _ ->
                                dict:update_counter(Dst, -1, NowInFlight1)
                        end
                end),
    updatef(State1, #state.left_move_counts,
            fun (LeftMoveCounts) ->
                    D = dict:update_counter(Src, -1, LeftMoveCounts),
                    dict:update_counter(Dst, -1, D)
            end).

%% @doc marks previously started compaction as done. Users of this
%% code will call it when compaction is done to update state so that
%% next moves and/or compactions can be started.
note_compaction_done(State, {compact, Node}) ->
    updatef(State, #state.in_flight_compactions,
            fun (InFlightCompactions) ->
                    sets:del_element(Node, InFlightCompactions)
            end).
