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

increment_counter_keys(Nodes, Dict) ->
    update_counter_keys(Nodes, 1, Dict).

update_counter_keys(Nodes, Value, Dict) ->
    lists:foldl(fun (N, Acc) ->
                        dict:update_counter(N, Value, Acc)
                end, Dict, Nodes).

new_replicas(OldChain, NewChain) ->
    [N || N <- NewChain,
          N =/= undefined,
          not lists:member(N, OldChain)].

choose_action_not_compaction(#state{
                                restrictions = Restrictions,
                                in_flight_per_node = NowInFlight,
                                in_flight_compactions = NowCompactions,
                                moves_left_count_per_node = LeftCount,
                                moves_left = MovesLeft,
                                in_flight_backfills = CurrentBackfills,
                                in_flight_moves = CurrentMoves,
                                compaction_countdown_per_node = CompactionCountdown} = State) ->
    %% Number backfills per node.
    NowBackfills =
        lists:foldl(
          fun ({_V, OldChain, NewChain, _Quirks}, NBAcc) ->
                  BackfillNodes = backfill_nodes(OldChain, NewChain),
                  increment_counter_keys(BackfillNodes, NBAcc)
          end, dict:new(), CurrentBackfills),

    %% Identify all the connections(i.e, {Src, Dst}) have currently active
    %% backfills.
    ConnectionDict = lists:foldl(
                       fun ({_, [Src | _] = OldChain, NewChain, _}, Acc) ->
                               Connections = [{Src, Dst} ||
                                              Dst <- new_replicas(OldChain,
                                                                  NewChain)],
                               increment_counter_keys(Connections, Acc)
                       end, dict:new(), CurrentBackfills),

    %% Active moves, i.e., moves involving change in master, is accompanied by
    %% view building on the new master after backfill stage has been completed,
    %% see ns_single_vbucket_mover:wait_index_updated.
    %% ViewHeaviness account for this heaviness caused by views building.
    ViewHeaviness = lists:foldl(
                      fun ({_, [Src | _], [Dst | _], _}, Dict)
                            when Src =/= Dst ->
                              dict:update_counter(Dst, 1, Dict);
                          (_, Dict) ->
                              Dict
                      end, dict:new(), CurrentMoves),

    %% NodeWeights need to be calculated on all the MovesLeft not just
    %% PossibleMoves.
    %% NodeWeights determines the bottleneck nodes, and we want to keep them
    %% busy at all times.
    NodeWeights = lists:foldl(
                    fun ({_V, OldChain, NewChain, _}, Acc) ->
                            Nodes = backfill_nodes(OldChain, NewChain),
                            increment_counter_keys(Nodes, Acc)
                    end, dict:new(), MovesLeft),

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
        fun ({Vb, [OldMaster | _] = OldChain, [NewMaster | _] = NewChain, _}) ->
                %% 1. OldMaster from KV perspective since it needs to perform
                %% backfills.
                %% 2. NewReplicas from KV perspective since they need to process
                %% backfills.
                %% 2. NewMaster(if not same as OldMaster) from view perspective
                %% since view index needs to built on NewMaster.
                BackfillNodes = backfill_nodes(OldChain, NewChain),

                %% MoveWeight = Maximum NodeWeight of node involved in the move
                MoveWeight = lists:max([misc:dict_get(Node, NodeWeights, 0) ||
                                        Node <- BackfillNodes]),

                %% KV/Data service is limited in term of processing number of
                %% backfill streams per connection (at the time of writing this
                %% comment, KV can handle only one backfill stream at a time
                %% per connection, as they have one thread per connection for
                %% processing data).
                %% Therefore, in order to achieve the max amount of parallelism
                %% we need to schedule vbucket moves in such a fashion that we
                %% involve separate connection at any given point in time. For
                %% example, in a 4->4 swap rebalance case, when node3 is
                %% replaced by node4, we can achieve max data transfer when we
                %% have concurrent backfilling as below,
                %% 1. node0 -> node4 (replica move)
                %% 2. node1 -> node4 (replica move)
                %% 3. node2 -> node4 (replica move)
                %% 4. node3 -> node4 (active move)
                %%
                %% SerialScore is calculated on the new backfill connections for
                %% this move, this is where we have maximum data flow.
                %% If existing backfills use this connection we effectively
                %% serialize the moves involved.
                %% We are not only trying to determine the speed with which this
                %% move will complete but also how this move affects the
                %% existing moves.
                SerialScore = lists:sum(
                                [misc:dict_get({OldMaster, Dst},
                                               ConnectionDict, 0) ||
                                 Dst <- new_replicas(OldChain, NewChain)]),

                %% New view indexes are built on the NewMaster.
                %% We want to spread the view building load across the cluster.
                %% Therefore, prefer moves where index building happens on the
                %% least busy node.
                ViewEqualizer = case OldMaster =/= NewMaster of
                                    true ->
                                        1000 - misc:dict_get(NewMaster,
                                                             ViewHeaviness, 0);
                                    false ->
                                        0
                                end,

                %% Heaviness determines how busy the nodes involved, i.e.,
                %% BackfillNodes already are.
                Heaviness = lists:sum([misc:dict_get(N, NowBackfills, 0) ||
                                       N <- BackfillNodes]),

                CompactionDistance =
                    case OldMaster =/= NewMaster of
                        true ->
                            1000 - dict:fetch(NewMaster, CompactionCountdown)
                                 - dict:fetch(OldMaster, CompactionCountdown);
                        false ->
                            0
                    end,

                {
                 %% 1. Prefer moves which involve node with most moves left.
                 %% We also want to keep the bottleneck nodes involved so that
                 %% we are not stuck with moves to/from the same node(s) at the
                 %% end.
                 MoveWeight,

                 %% 2. Prefer moves which will give us the most parallelism.
                 %% Parallelism is achieved by scheduling moves that use
                 %% different connections(i.e., {Src, Dst}) for backfills.
                 %% We penalize moves that will result in multiple backfills on
                 %% same connection.
                 -SerialScore,

                 %% 3. Prefer active moves over replica moves, and prefer moves
                 %% that will help spread the view index building across the
                 %% cluster.
                 ViewEqualizer,

                 %% 4. Prefer nodes with least current moves.
                 %% We want to spread the load across the cluster, hence
                 %% penalise moves that are involved in the current moves.
                 -Heaviness,

                 %% 5. Prefer active moves over replica moves, and prefer
                 %% active moves closer to compaction.
                 CompactionDistance,

                 %% Last resort tie breaker.
                 Vb
                }
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
