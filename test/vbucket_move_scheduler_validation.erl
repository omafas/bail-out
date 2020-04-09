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
-module(vbucket_move_scheduler_validation).
-include("ns_common.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([prepare_verifier/4, choose_action/1, extract_progress/1,
        note_backfill_done/2,
        note_move_completed/2,
        note_compaction_done/2,
        is_done/1,
        all_performed_moves/1]).

-export([simulate_that_rebalance/0]).

-record(vs, { %% vs is verifier state
          max_backfills_per_node :: pos_integer(),
          max_moves_per_node :: pos_integer(),
          moves_before_compaction :: pos_integer(),
          sched_state :: tuple(),
          all_performed_moves :: list(),
          all_compactions :: list(),
          running_backfills :: list(),
          running_moves :: list(),
          running_compactions :: list()
         }).

prepare_verifier(SchedState, ConcurrentBackfills, MovesBeforeCompaction, MaxInflightMoves) ->
    #vs{max_backfills_per_node = ConcurrentBackfills,
        max_moves_per_node = MaxInflightMoves,
        moves_before_compaction = MovesBeforeCompaction,
        sched_state = SchedState,
        all_performed_moves = [],
        all_compactions = [],
        running_backfills = [],
        running_moves = [],
        running_compactions = []}.

%% -define(D(F,A), ?log_debug(F,A)).
-define(D(F,A), (true = is_integer(erlang:phash2({F, A})))).
-define(D2(F,A), (true = is_integer(erlang:phash2({F, A})))).

miniassert(true, _Label) -> ok.

choose_action(#vs{sched_state = Sub} = InitialState) ->
    {Actions, NewSub} = vbucket_move_scheduler:choose_action(Sub),
    ?D2("Sub:~n~p,~nNewSub:~n~p", [Sub, NewSub]),
    RetState =
        lists:foldl(fun ({move, M}, #vs{running_moves=RunningM,
                                        running_backfills=RunningB} = State) ->
                            ?D("Got move: ~p", [M]),
                            miniassert(not lists:member(M, RunningM), "not in moves"),
                            miniassert(not lists:member(M, RunningB), "not in backfills"),
                            verify_starting_move(M, State),
                            State#vs{
                              all_performed_moves = [M | State#vs.all_performed_moves],
                              running_moves = [M | RunningM],
                              running_backfills = [M | RunningB]};
                        ({compact, N}, #vs{running_compactions = RunningC} = State) ->
                            ?D("Got compaction: ~p", [N]),
                            miniassert(not lists:member(N, RunningC), "not in compactions"),
                            verify_starting_compaction(N, State),
                            State#vs{running_compactions = [N | RunningC],
                                     all_compactions = [N | State#vs.all_compactions]}
                    end, InitialState#vs{sched_state = NewSub}, Actions),
    {Actions, RetState}.

extract_progress(#vs{sched_state = S}) ->
    vbucket_move_scheduler:extract_progress(S).

note_backfill_done(#vs{sched_state = S,
                       running_backfills = B} = State,
                   {move, M} = Arg) ->
    ?D("Backfill done for: ~p", [M]),
    miniassert(lists:member(M, B), {"done-ing running backfill", B, M}),
    State#vs{sched_state = vbucket_move_scheduler:note_backfill_done(S, Arg),
             running_backfills = lists:delete(M, B)}.

note_move_completed(#vs{running_backfills = B,
                        running_moves = R,
                        sched_state = S} = State,
                    {move, M} = Arg) ->
    ?D("Move done for: ~p", [M]),
    NewS = vbucket_move_scheduler:note_move_completed(S, Arg),
    miniassert(lists:member(M, R), {"still running", R, M}),
    miniassert(not lists:member(M, B), {"backfill done", B, M}),
    State#vs{sched_state = NewS,
             running_moves = lists:delete(M, R)}.

note_compaction_done(#vs{sched_state = S,
                         running_compactions = C} = State,
                     {compact, N} = Arg) ->
    ?D("Compaction done for: ~p", [N]),
    NewS = vbucket_move_scheduler:note_compaction_done(S, Arg),
    miniassert(lists:member(N, C), {"still running", C, N}),
    State#vs{sched_state = NewS,
             running_compactions = lists:delete(N, C)}.

is_done(#vs{sched_state = S,
            %% running_compactions = C,
            running_moves = M,
            running_backfills = B}) ->
    RV = vbucket_move_scheduler:is_done(S),
    case RV of
        true ->
            %% miniassert(C =:= [], {"empty C", C}),
            miniassert(M =:= [], {"empty M", M}),
            miniassert(B =:= [], {"empty B", B});
        _ ->
            ok
    end,
    RV.

all_performed_moves(#vs{all_performed_moves = M}) ->
    M.

extract_move({_Vb, [Src|_], [Dst|_], []}) -> {Src, Dst}.

check_concurrent_moves(-1 = _ConcurrentMoves, _Node, _RunningB, Acc) ->
    Acc;
check_concurrent_moves(_ConcurrentMoves, _Node, [] = _RunningB, _Acc) ->
    true;
check_concurrent_moves(ConcurrentMoves, Node, [M | RestRunningB], Acc) ->
    {Src, Dst} = extract_move(M),
    case Src =:= Node orelse Dst =:= Node of
        true ->
            check_concurrent_moves(ConcurrentMoves-1, Node, RestRunningB, [M | Acc]);
        _ ->
            check_concurrent_moves(ConcurrentMoves, Node, RestRunningB, Acc)
    end.

verify_starting_move(M, #vs{running_backfills = RunningB,
                            max_backfills_per_node = ConcurrentBackfills,
                            max_moves_per_node = ConcurrentMoves,
                            running_moves = RunningM,
                            running_compactions = RunningC}) ->
    miniassert(not lists:member(M, RunningM), "not in moves"),
    miniassert(not lists:member(M, RunningB), "not in backfills"),
    {Src, Dst} = extract_move(M),
    miniassert(Dst =/= undefined, "Dst is sane"),
    case Src =:= undefined of
        true ->
            ok;
        _ ->
            miniassert(not lists:member(Src, RunningC), "src is not compacted"),
            miniassert(not lists:member(Dst, RunningC), "dst is not compacted"),
            miniassert(check_concurrent_moves(ConcurrentBackfills, Src, RunningB, []),
                       "max number of backfills is not reached on src"),
            miniassert(check_concurrent_moves(ConcurrentBackfills, Dst, RunningB, []),
                       "max number of backfills is not reached on dst"),
            miniassert(check_concurrent_moves(ConcurrentMoves, Src, RunningM, []),
                       "max number of moves is not reached on src"),
            miniassert(check_concurrent_moves(ConcurrentMoves, Dst, RunningM, []),
                       "max number of moves is not reached on dst")
    end.

verify_starting_compaction(N, #vs{running_moves = RunningM,
                                  running_compactions = RunningC}) ->
    miniassert(not lists:member(N, RunningC), "not in compactions"),
    [begin
         {Src, Dst} = extract_move(M),
         miniassert(case Src =/= N of true -> true; _ -> M end, "not in moves via src"),
         miniassert(case Dst =/= N of true -> true; _ -> M end, "not in moves via dst")
     end || M <- RunningM],
    ok.

generate_a_map(VBucketsCount, ReplicasCount, Nodes) ->
    Chain = lists:duplicate(ReplicasCount+1, undefined),
    EmptyMap = lists:duplicate(VBucketsCount, Chain),
    mb_map:generate_map(EmptyMap, ReplicasCount, lists:sort(Nodes), []).

simulate_rebalance(CurrentMap, TargetMap, BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves) ->
    S = prepare_verifier(vbucket_move_scheduler:prepare(
                           CurrentMap, TargetMap, [],
                           BackfillsLimit, MovesBeforeCompaction,
                           MaxInflightMoves),
                         BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves),

    R = lists:foldl(fun (_, R0) ->
                            {_, R1} = rand:uniform_s(R0),
                            R1
                    end, rand:seed_s(exrop, {1,2,3}), lists:duplicate(37, [])),

    InFlight = gb_sets:empty(),

    VirtualTime = 0,

    simulate_rebalance_loop(S, InFlight, R, VirtualTime, []).

%% generate approximately normal distribution with mean 0 and std
%% deviation of 1. Uses old school method of just adding 12 uniform
%% variables.
rnd_normal_s(R) ->
    rnd_normal_s_loop(R, 12, 0).

rnd_normal_s_loop(R, 0, Acc) ->
    {Acc - 6, R};
rnd_normal_s_loop(R, N, Acc) ->
    {V, R1} = rand:uniform_s(R),
    rnd_normal_s_loop(R1, N-1, Acc + V).

generate_action_run_time(Action, R) ->
    %% RandV is normally distributed random variable with mean of 0
    %% and std deviation of 1
    {RandV, R1} = rnd_normal_s(R),
    %% And then we add some mean time that depends on type move itself
    AvgTime = case Action of
                  {move, _} ->
                      %% backfill
                      13;
                  {compact, _} ->
                      26;
                  {complete_move, _} ->
                      130
              end,
    {erlang:max(0.001, RandV + AvgTime), R1}.

add_action(Action, InFlight, R, VirtualTime) ->
    {Runtime, R1} = generate_action_run_time(Action, R),
    EndTime = VirtualTime + Runtime,
    InFlight1 = gb_sets:insert({EndTime, Action}, InFlight),
    {InFlight1, R1}.

simulate_rebalance_loop(S, InFlight, R, VirtualTime, Acc) ->
    ?D("Time: ~p", [VirtualTime]),
    {Actions, S1} = choose_action(S),
    MoreRV = choose_action(S1),
    miniassert({[], S1} =:= MoreRV,
               {"no more actions", S1, Actions, MoreRV}),
    {NewInFlight, NewR} =
        lists:foldl(
          fun (Action, {InFlight0, R0}) ->
                  add_action(Action, InFlight0, R0, VirtualTime)
          end, {InFlight, R}, Actions),
    Acc1 = Acc ++ [{VirtualTime, start, A} || A <- Actions],
    case Actions =:= [] andalso is_done(S1) of
        true ->
            {S1, VirtualTime, Acc1};
        _ ->
            miniassert(not gb_sets:is_empty(NewInFlight), "NewInFlight is not empty"),
            {{NextVTime, DoneAction}, NewInFlight1} = gb_sets:take_smallest(NewInFlight),
            true = (NextVTime >= VirtualTime),
            case DoneAction of
                {move, M} ->
                    Acc2 = [{NextVTime, backfill_done, DoneAction} | Acc1],
                    S2 = note_backfill_done(S1, DoneAction),
                    ?D2("S1:~p~nS2.sub:~n~p", [S1#vs.sched_state, S2#vs.sched_state]),
                    {InFlight2, R2} = add_action({complete_move, M}, NewInFlight1, NewR, NextVTime),
                    simulate_rebalance_loop(S2, InFlight2, R2, NextVTime, Acc2);
                {complete_move, M} ->
                    Acc2 = [{NextVTime, done, {move, M}} | Acc1],
                    S2 = note_move_completed(S1, {move, M}),
                    simulate_rebalance_loop(S2, NewInFlight1, NewR, NextVTime, Acc2);
                {compact, _} ->
                    Acc2 = [{NextVTime, done, DoneAction} | Acc1],
                    S2 = note_compaction_done(S1, DoneAction),
                    simulate_rebalance_loop(S2, NewInFlight1, NewR, NextVTime, Acc2)
            end
    end.

test_rebalance(Replicas, VBuckets, BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves,
               NodesBefore, NodesAfter) ->
    InitialMap = generate_a_map(VBuckets, Replicas, NodesBefore),
    TargetMap = mb_map:generate_map(InitialMap, Replicas, NodesAfter, []),
    do_test_rebalance(VBuckets, BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves,
                      InitialMap, TargetMap).

do_test_rebalance(VBuckets, BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves,
                  InitialMap, TargetMap) ->
    try
        {S, _VirtualTime, _} = simulate_rebalance(InitialMap, TargetMap, BackfillsLimit,
                                                  MovesBeforeCompaction, MaxInflightMoves),
        AllMoves = all_performed_moves(S),
        InitialIndexed = lists:zip(lists:seq(0, VBuckets-1), InitialMap),
        FinalIndexed =
            lists:foldl(
              fun ({Vb, ChainBefore, ChainAfter, []}, CurrentIndexed) ->
                      miniassert(ChainBefore =/= ChainAfter, "non empty moves"),
                      {_, CurrentChain} = lists:keyfind(Vb, 1, CurrentIndexed),
                      miniassert(ChainBefore =:= CurrentChain, "before matches current"),
                      lists:keystore(Vb, 1, CurrentIndexed, {Vb, ChainAfter})
              end, InitialIndexed, AllMoves),
        {_, FinalMap} = lists:unzip(lists:sort(FinalIndexed)),
        miniassert(TargetMap =:= FinalMap, "final map"),
        AllCompactions = S#vs.all_compactions,
        AffectedNodes = lists:usort(
                          lists:flatten([[Src, Dst]
                                         || {_V, [Src|_], [Dst|_], []} <- AllMoves,
                                            Src =/= undefined,
                                            Src =/= Dst])),
        CompactedNodes = lists:usort(AllCompactions),
        miniassert(CompactedNodes =:= AffectedNodes,
                   {"every node at least once compacted", CompactedNodes, AffectedNodes}),
        ok
    catch T:E ->
            ST = erlang:get_stacktrace(),
            ?debugFmt("~p~n~p", [{T,E}, ST]),
            erlang:raise(T, E, ST)
    end.

maybe_parallel(_, T) ->
    [{inparallel, 64, T}].
%% maybe_parallel(T) ->
%%     T.

%% maybe_filter_out_4_to_3(R, VBs, L, C, Out) ->
%%     R =:= 3 andalso VBs =:= 4 andalso Out =:= c andalso L =:= 2 andalso C =:= 3.
maybe_filter_out_4_to_3(R, VBs, L, C, Out) ->
    erlang:phash2({R, VBs, L, C, Out}, 1031) < 100.

rebalance_4_to_3_test_() ->
    Nodes = [a, b, c, d],
    {timeout, 120,
     maybe_parallel('4_to_3',
       [begin
            Title = lists:flatten(io_lib:format("4->3: replicas: ~p, vbuckets: ~p, limit: ~p, countdown: ~p, maxmoves: ~p, remove: ~p",
                                                [R, VBs, L, C, M, Out])),
            Fun = fun () ->
                          test_rebalance(R, VBs, L, C, M, Nodes, lists:delete(Out, Nodes))
                  end,
            {timeout, 120, {Title, Fun}}
        end || R <- [1,0,3],
               VBs <- [16, 4, 2, 256],
               L <- [1,2],
               C <- [1,2,3],
               M <- [1,2,3],
               Out <- Nodes,
               maybe_filter_out_4_to_3(R, VBs, L, C, Out)])}.


maybe_filter_out_11_to_13(R, VBs, L, C, In1, In2) ->
    erlang:phash2({R, VBs, L, C, In1, In2}, 1031) < 60.

rebalance_11_to_13_test_() ->
    Nodes = [a, b, c, d, e,
             f, g, h, i, j,
             k, l, m],
    {timeout, 240,
     maybe_parallel('11_to_13',
       [begin
            Title = lists:flatten(
                      io_lib:format(
                        "11->13: replicas: ~p, vbuckets: ~p, limit: ~p, countdown: ~p, maxmoves: ~p, add: ~p",
                        [R, VBs, L, C, M, [In1, In2]])),
            Fun = fun () ->
                          Before = Nodes -- [In1, In2],
                          test_rebalance(R, VBs, L, C, M, Before, Nodes)
                  end,
            {timeout, 120, {Title, Fun}}
        end || R <- [1,0,2],
               VBs <- [64, 256],
               L <- [1,2],
               C <- [2,3],
               M <- [2,3],
               In1 <- Nodes,
               In2 <- Nodes,
               In1 =/= In2,
               maybe_filter_out_11_to_13(R, VBs, L, C, In1, In2)])}.

run_rebalance_after_data_loss(Nodes, FailedOverNodes, VBuckets, Replicas,
                              BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves) ->
    [] = FailedOverNodes -- Nodes,
    InitialMap = generate_a_map(VBuckets, Replicas, Nodes),
    AfterFailover = lists:foldl(fun (ToRemove, Map0) ->
                                        mb_map:promote_replicas(Map0, [ToRemove])
                                end, InitialMap, FailedOverNodes),
    TargetMap = mb_map:generate_map(AfterFailover, Replicas, Nodes, []),
    do_test_rebalance(VBuckets, BackfillsLimit, MovesBeforeCompaction, MaxInflightMoves,
                      AfterFailover, TargetMap).

rebalance_after_data_loss_test() ->
    run_rebalance_after_data_loss([a, b, c, d, e, f], [a, c], 16, 1, 1, 2, 2).

rebalance_after_data_loss_no_replicas_test() ->
    run_rebalance_after_data_loss([a, b, c, d, e, f], [a, c], 16, 0, 1, 2, 2).

simulate_that_rebalance() ->
    ok = application:start(ale),

    ok = ale:start_sink(stderr, ale_stderr_sink, []),

    lists:foreach(
      fun (Logger) ->
              ok = ale:start_logger(Logger, debug),
              ok = ale:add_sink(Logger, stderr)
      end,
      ?LOGGERS),

    NumReplicas = 0,
    Before = generate_a_map(256, NumReplicas, [a, b, c, d]),
    After = mb_map:generate_map(Before, NumReplicas, [a, b, c], []),
    {_, _, Events} = simulate_rebalance(Before, After, 1, 16, 16),
    {ok, F} = file:open("simulate-results", [write, binary]),
    Rows =
        [begin
             case Action of
                 {compact, N} ->
                     {struct, [{ts, TS},
                               {subtype, Type},
                               {type, compact},
                               {node, N}]};
                 {move, {Vb, ChainBefore, ChainAfter, _}} ->
                     {struct, [{ts, TS},
                               {subtype, Type},
                               {type, move},
                               {vb, Vb},
                               {chainBefore, ChainBefore},
                               {chainAfter, ChainAfter}]}
             end
         end || {TS, Type, Action} <- Events],
    [begin
         ok = file:write(F, iolist_to_binary([mochijson2:encode(J), <<"\n">>]))
     end || J <- Rows],
    ok = file:close(F).
