%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(rebalance_agent).

-behaviour(gen_server2).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([prepare_rebalance/2, unprepare_rebalance/2]).
-export([prepare_delta_recovery/3, complete_delta_recovery/2]).
-export([prepare_delta_recovery_bucket/4]).
-export([deactivate_bucket_data/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(JOB_QUEUE, rebalance_jobs).
-define(WORKER, list_to_atom(?MODULE_STRING ++ "-worker")).

-type multi_call_result(Success) ::
        Success | {error, {failed_nodes, [{node(), Error :: any()}]}}.

-record(state,
        { rebalancer     :: undefined | {pid(), reference()},
          delta_recovery :: undefined | {reference(), [bucket_name()]} }).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec prepare_rebalance([node()], pid()) -> multi_call_result(ok).
prepare_rebalance(Nodes, Pid) ->
    Timeout = ?get_timeout(prepare_rebalance, 30000),
    Results = call_prepare_rebalance(Nodes, Pid, Timeout),
    process_multi_call_results(Nodes, Results).

call_prepare_rebalance(Nodes, Pid, Timeout) ->
    misc:parallel_map_partial(
      call_prepare_rebalance_one_node(_, Pid), Nodes, Timeout).

call_prepare_rebalance_one_node(Node, Pid) ->
    try call(Node, {prepare_rebalance, Pid}) of
        RV -> RV
    catch
        T:E ->
            ?log_debug("Rebalance agent not yet ready on node ~p: ~p",
                       [Node, {T, E}]),
            timer:sleep(500),
            call_prepare_rebalance_one_node(Node, Pid)
    end.

-spec unprepare_rebalance([node()], pid()) -> multi_call_result(ok).
unprepare_rebalance(Nodes, Pid) ->
    Timeout = ?get_timeout(unprepare_rebalance, 30000),
    multi_call(Nodes, {unprepare_rebalance, Pid}, Timeout).

-spec prepare_delta_recovery([node()], pid(), [bucket_name()]) ->
                                    multi_call_result(ok).
prepare_delta_recovery(Nodes, Pid, Buckets) ->
    Timeout = ?get_timeout(prepare_delta_recovery, 120000),
    multi_call(Nodes, {prepare_delta_recovery, Pid, Buckets}, Timeout).

-spec prepare_delta_recovery_bucket(pid(), bucket_name(),
                                    NodeVBuckets, ActiveFailoverLogs) ->
                                           multi_call_result(OkResult) when
      NodeVBuckets :: [{node(), [vbucket_id()]}],
      ActiveFailoverLogs :: #{vbucket_id() => missing | FailoverLog},
      FailoverLog :: [{UID :: integer(), StartSeqno :: integer()}],
      OkResult :: {ok, [{node(), PresentVBuckets :: [vbucket_id()]}]}.
prepare_delta_recovery_bucket(Pid, Bucket, NodeVBuckets, ActiveFailoverLogs) ->
    %% We do a lot of stuff as part of preparetion for delta recovery, and
    %% certain interactions with memcached are not optimized as of right now,
    %% hence a hefty timeout.
    Timeout = ?get_timeout(prepare_delta_recovery_bucket,  300000),
    {Nodes, Results} =
        call_prepare_delta_recovery_bucket(Pid, Bucket, NodeVBuckets,
                                           ActiveFailoverLogs, Timeout),
    process_multi_call_results(Nodes, Results,
                               fun is_ok_tuple/1, fun recombine_oks/1).

call_prepare_delta_recovery_bucket(Pid, Bucket,
                                   NodeVBuckets, ActiveFailoverLogs, Timeout) ->
    Nodes = [N || {N, _VBs} <- NodeVBuckets],
    Results = misc:parallel_map_partial(
                fun ({Node, VBuckets}) ->
                        Request = {prepare_delta_recovery_bucket,
                                   Pid, Bucket, VBuckets,
                                   ActiveFailoverLogs},
                        safe_call(Node, Request)
                end, NodeVBuckets, Timeout),
    {Nodes, Results}.

-spec complete_delta_recovery([node()], pid()) -> multi_call_result(ok).
complete_delta_recovery(Nodes, Pid) ->
    Timeout = ?get_timeout(complete_delta_recovery, 30000),
    multi_call(Nodes, {complete_delta_recovery, Pid}, Timeout).

-spec deactivate_bucket_data(bucket_name(), [node()], pid()) ->
                                    multi_call_result(ok).
deactivate_bucket_data(Bucket, Nodes, Pid) ->
    Timeout = ?get_timeout(deactivate_bucket_data, 5000),
    multi_call(Nodes, {deactivate_bucket_data, Pid, Bucket}, Timeout).

%% callbacks
init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({prepare_rebalance, Pid}, _From, State) ->
    handle_prepare_rebalance(Pid, State);
handle_call({unprepare_rebalance, Pid}, _From, State) ->
    handle_unprepare_rebalance(Pid, State);
handle_call({prepare_delta_recovery, Pid, Buckets}, _From, State) ->
    handle_prepare_delta_recovery(Pid, Buckets, State);
handle_call({prepare_delta_recovery_bucket,
             Pid, Bucket, VBuckets, ActiveFailoverLogs}, From, State) ->
    handle_prepare_delta_recovery_bucket(Pid,
                                         Bucket, VBuckets, ActiveFailoverLogs,
                                         From, State);
handle_call({complete_delta_recovery, Pid}, _From, State) ->
    handle_complete_delta_recovery(Pid,State);
handle_call({deactivate_bucket_data, Pid, Bucket}, _From, State) ->
    handle_deactivate_bucket_data(Pid, Bucket, State);
handle_call(Call, From, State) ->
    ?log_warning("Received unexpected call ~p, from ~p. State:~n~p",
                 [Call, From, State]),
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_warning("Received an unexpected cast ~p. State:~n~p", [Cast, State]),
    {noreply, State}.

handle_info({'DOWN', MRef, _, _, Reason}, State) ->
    handle_down(MRef, Reason, State);
handle_info(Msg, State) ->
    ?log_warning("Received an unexpected message ~p. State:~n~p", [Msg, State]),
    {noreply, State}.

terminate(_Reason, State) ->
    case rebalancer_pid(State) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            ?log_debug("Terminating when rebalance "
                       "is running (rebalancer ~p)", [Pid]),
            unset_rebalancer(State)
    end.

%% internal
server(Node) ->
    {?SERVER, Node}.

handle_prepare_rebalance(Pid, State) ->
    NewState = maybe_unset_rebalancer(Pid, State),
    MRef = erlang:monitor(process, Pid),
    {reply, ok, NewState#state{rebalancer = {Pid, MRef}}}.

maybe_unset_rebalancer(Pid, State) ->
    case rebalancer_pid(State) of
        undefined ->
            State;
        OldPid ->
            ?log_debug("Received set_rebalancer (pid ~p) when the old "
                       "rebalancer ~p is still in the state. Unsetting.",
                       [Pid, OldPid]),
            unset_rebalancer(State)
    end.

handle_unprepare_rebalance(Pid, State) ->
    case check_rebalancer_pid(Pid, State) of
        ok ->
            {reply, ok, unset_rebalancer(State)};
        Error ->
            {reply, Error, State}
    end.

handle_prepare_delta_recovery(Pid, Buckets, State) ->
    case functools:sequence_([?cut(check_rebalancer_pid(Pid, State)),
                              ?cut(check_no_delta_recovery(State))]) of
        ok ->
            case ns_bucket_worker:start_transient_buckets(Buckets) of
                {ok, Ref} ->
                    ?log_debug("Started transient buckets ~p "
                               "for delta recovery.", [Buckets]),
                    {reply, ok,
                     State#state{delta_recovery = {Ref, Buckets}}};
                Error ->
                    ?log_error("Failed to start transient "
                               "buckets ~p for delta recovery: ~p",
                               [Buckets, Error]),
                    {reply, {error,
                             {failed_to_start_buckets, Buckets, Error}}, State}
            end;
        Error ->
            {reply, Error, State}
    end.

handle_prepare_delta_recovery_bucket(Pid, Bucket, VBuckets,
                                     ActiveFailoverLogs, From, State) ->
    case functools:sequence_(
           [?cut(check_rebalancer_pid(Pid, State)),
            ?cut(check_delta_recovery_bucket(Bucket, State))]) of
        ok ->
            ?log_debug("Received request to prepare bucket ~p for "
                       "delta recovery. VBuckets:~n~p~nFailover logs:~n~p",
                       [Bucket, VBuckets, ActiveFailoverLogs]),
            start_prepare_delta_recovery_bucket(Bucket, VBuckets,
                                                ActiveFailoverLogs, From),
            {noreply, State};
        Error ->
            {reply, Error, State}
    end.

start_prepare_delta_recovery_bucket(Bucket, VBuckets,
                                    ActiveFailoverLogs, From) ->
    run_async_job(
      ?cut(prepare_delta_recovery_bucket_job(Bucket, VBuckets,
                                             ActiveFailoverLogs)),
      handle_prepare_delta_recovery_result(Bucket, From, _, _)).

prepare_delta_recovery_bucket_job(Bucket, VBuckets, ActiveFailoverLogs) ->
    case prepare_vbuckets(Bucket, VBuckets) of
        {ok, ActualVBuckets} ->
            maybe_delete_diverged_vbuckets(Bucket,
                                           ActualVBuckets, ActiveFailoverLogs);
        Error ->
            Error
    end.

handle_prepare_delta_recovery_result(Bucket, From, Result, State) ->
    {Ref, Buckets} = State#state.delta_recovery,
    ?log_debug("Prepare delta recovery "
               "result for bucket ~p:~n~p", [Bucket, Result]),

    gen_server2:reply(From, Result),

    case Result of
        {ok, _} ->
            NewBuckets = lists:delete(Bucket, Buckets),
            {noreply, State#state{delta_recovery = {Ref, NewBuckets}}};
        _ ->
            {noreply, State}
    end.

handle_complete_delta_recovery(Pid, State) ->
    case functools:sequence_([?cut(check_rebalancer_pid(Pid, State)),
                              ?cut(check_delta_recovery_completed(State))]) of
        ok ->
            Statuses    = stop_transient_buckets(State),
            BadStatuses = [{B, S} || {B, S} <- Statuses, S =/= running],

            Reply =
                case BadStatuses =:= [] of
                    true ->
                        ok;
                    false ->
                        {error, {bad_bucket_statuses, BadStatuses}}
                end,
            {reply, Reply, State#state{delta_recovery = undefined}};
        Error ->
            {reply, Error, State}
    end.

handle_deactivate_bucket_data(Pid, Bucket, State) ->
    case check_rebalancer_pid(Pid, State) of
        ok ->
            ok = ns_bucket:deactivate_bucket_data_on_this_node(Bucket),
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end.

stop_transient_buckets(#state{delta_recovery = {Ref, _}}) ->
    {ok, Statuses} = ns_bucket_worker:stop_transient_buckets(Ref),
    Statuses.

handle_down(MRef, Reason, State) ->
    true = (rebalancer_mref(State) =:= MRef),
    ?log_info("Rebalancer process ~p died (reason ~p).",
              [rebalancer_pid(State), Reason]),
    {noreply, unset_rebalancer(State)}.

unset_rebalancer(#state{rebalancer = {_, MRef}} = State) ->
    erlang:demonitor(MRef, [flush]),
    abort_rebalance(State#state{rebalancer = undefined}).

rebalancer_pid(#state{rebalancer = undefined}) ->
    undefined;
rebalancer_pid(#state{rebalancer = {Pid, _}}) ->
    Pid.

rebalancer_mref(#state{rebalancer = undefined}) ->
    undefined;
rebalancer_mref(#state{rebalancer = {_, MRef}}) ->
    MRef.

check_rebalancer_pid(Pid, State) ->
    RebalancerPid = rebalancer_pid(State),
    case Pid =:= RebalancerPid of
        true ->
            ok;
        false ->
            {error, {bad_rebalancer_pid, RebalancerPid, Pid}}
    end.

check_no_delta_recovery(#state{delta_recovery = undefined}) ->
    ok;
check_no_delta_recovery(_) ->
    {error, delta_recovery_already_started}.

check_delta_recovery_completed(#state{delta_recovery = undefined}) ->
    {error, delta_recovery_not_started};
check_delta_recovery_completed(#state{delta_recovery = {_Ref, Buckets}}) ->
    case Buckets of
        [] ->
            ok;
        _ ->
            {error, {delta_recovery_not_finished, Buckets}}
    end.

check_delta_recovery_bucket(_Bucket, #state{delta_recovery = undefined}) ->
    {error, delta_recovery_not_started};
check_delta_recovery_bucket(Bucket, #state{delta_recovery = {_Ref, Buckets}}) ->
    case lists:member(Bucket, Buckets) of
        true ->
            ok;
        false ->
            {error, {missing_bucket, Bucket}}
    end.

multi_call(Nodes, Request, Timeout) ->
    process_multi_call_results(Nodes, do_multi_call(Nodes, Request, Timeout)).

do_multi_call(Nodes, Request, Timeout) ->
    misc:parallel_map_partial(safe_call(_, Request), Nodes, Timeout).

call(Node, Request) ->
    gen_server2:call(server(Node), Request, infinity).

safe_call(Node, Request) ->
    try
        call(Node, Request)
    catch
        T:E ->
            {error, {T, E}}
    end.

unwrap_call_results(Results) ->
    [case RV of
         {ok, WrappedRV} ->
             WrappedRV;
         Error ->
             Error
     end || RV <- Results].

is_ok(Response) ->
    Response =:= ok.

return_ok(_) ->
    ok.

is_ok_tuple({ok, _}) ->
    true;
is_ok_tuple(_) ->
    false.

recombine_oks(OKs) ->
    {ok, [{Node, RV} || {Node, {ok, RV}} <- OKs]}.

process_multi_call_results(Nodes, Results) ->
    process_multi_call_results(Nodes, Results, fun is_ok/1, fun return_ok/1).

process_multi_call_results(Nodes, Results, OkPred, HandleOKs) ->
    {OKs, Errors} =
        lists:partition(
          fun ({_, RV}) ->
                  OkPred(RV)
          end, lists:zip(Nodes, unwrap_call_results(Results))),

    case Errors of
        [] ->
            HandleOKs(OKs);
        _ ->
            {error, {failed_nodes, Errors}}
    end.

prepare_vbuckets(Bucket, VBuckets) ->
    case ns_memcached:list_vbuckets(Bucket) of
        {ok, States} ->
            case check_vbucket_states(VBuckets, States) of
                {ok, Fixup, ActualVBuckets} ->
                    ok = fixup_vbucket_states(Bucket, Fixup),
                    {ok, ActualVBuckets};
                Error ->
                    Error
            end;
        Error ->
            {error, {failed_to_get_vbucket_states, Bucket, Error}}
    end.

fixup_vbucket_states(Bucket, Fixup) ->
    case Fixup =:= [] of
        true ->
            ok;
        false ->
            ?log_debug("Changing vbuckets for bucket ~p to state "
                       "'replica' for delta recovery. Affected vbuckets:~n~p",
                       [Bucket, Fixup]),
            lists:foreach(
              fun ({VB, _}) ->
                      ok = ns_memcached:set_vbucket(Bucket, VB, replica)
              end, Fixup)
    end.

check_vbucket_states(VBuckets0, States) ->
    VBuckets = sets:from_list(VBuckets0),
    {Fixup, Extra, Missing} =
        lists:foldl(fun check_one_vbucket_state/2, {[], [], VBuckets}, States),

    %% Note, that we only check for "extra" vbuckets, not vbuckets that are
    %% missing. The reason for that is that we explicitly delete vbuckets that
    %% are incompatible with the corresponding failover log. So if the delta
    %% recovery is interrupted and retried again, we might find some vbuckets
    %% deleted. We don't want to error out in this case.
    case Extra =:= [] of
        true ->
            PresentVBuckets = sets:to_list(sets:subtract(VBuckets, Missing)),
            {ok, Fixup, PresentVBuckets};
        false ->
            {error, {found_extra_vbuckets, Extra}}
    end.

check_one_vbucket_state({VB, State}, {Fixup, Extra, Required}) ->
    case sets:is_element(VB, Required) of
        true ->
            NewRequired = sets:del_element(VB, Required),
            case State =:= replica of
                true ->
                    {Fixup, Extra, NewRequired};
                false ->
                    {[{VB, State} | Fixup], Extra, NewRequired}
            end;
        false ->
            {Fixup, [VB | Extra], Required}
    end.

-ifdef(TEST).
check_vbucket_states_test() ->
    States = [{0, active},
              {1, replica},
              {2, pending},
              {3, dead}],

    {ok, Fixup, Present} = check_vbucket_states([0, 1, 2, 3], States),
    ?assertEqual([{0, active},
                  {2, pending},
                  {3, dead}], lists:sort(Fixup)),
    ?assertEqual([0, 1, 2, 3], lists:sort(Present)),

    %% Excessive vubkcets are an error.
    ?assertMatch({error, _},
                 check_vbucket_states([0, 1], States)),

    %% Missing vbuckets are fine.
    {ok, Fixup2, Present2} = check_vbucket_states([0, 1, 2, 3, 4], States),
    ?assertEqual([{0, active},
                  {2, pending},
                  {3, dead}], lists:sort(Fixup2)),
    ?assertEqual([0, 1, 2, 3], lists:sort(Present2)).
-endif.

get_local_failover_info(Bucket, VBuckets) ->
    case get_local_high_seqnos(Bucket, VBuckets) of
        {ok, HighSeqnos} ->
            case ns_memcached:get_failover_logs(Bucket, VBuckets) of
                {ok, FailoverLogs} ->
                    {ok, lists:zip3(VBuckets, FailoverLogs, HighSeqnos)};
                Error ->
                    {error,
                     {failed_to_get_failover_logs, Bucket, VBuckets, Error}}
            end;
        Error ->
            Error
    end.

get_local_high_seqnos(Bucket, VBuckets) ->
    case ns_memcached:get_vbucket_details_stats(Bucket, ["high_seqno"]) of
        {ok, Stats} ->
            get_local_high_seqnos_handle_results(Bucket, VBuckets, Stats);
        Error ->
            {error, {failed_to_get_high_seqnos, Bucket, Error}}
    end.

get_local_high_seqnos_handle_results(Bucket, VBuckets, Stats) ->
    {HighSeqnos, BadVBuckets} =
        lists:foldr(
          fun (VBucket, {AccHighSeqnos, AccBad}) ->
                  case extract_vbucket_high_seqno(VBucket, Stats) of
                      {ok, HighSeqno} ->
                          {[HighSeqno | AccHighSeqnos], AccBad};
                      {error, Error} ->
                          {AccHighSeqnos, [{VBucket, Error} | AccBad]}
                  end
          end, {[], []}, VBuckets),

    case BadVBuckets of
        [] ->
            {ok, HighSeqnos};
        _ ->
            {error, {failed_to_get_high_seqnos, Bucket, BadVBuckets}}
    end.

extract_vbucket_high_seqno(VBucket, Stats) ->
    case dict:find(VBucket, Stats) of
        {ok, VBucketStats} ->
            try
                {_, HighSeqnoString} = lists:keyfind(
                                         "high_seqno", 1, VBucketStats),
                {ok, list_to_integer(HighSeqnoString)}
            catch
                _:_ ->
                    {error, {bad_stats, VBucketStats}}
            end;
        error ->
            {error, missing}
    end.

-ifdef(TEST).
get_local_high_seqnos_handle_results_test() ->
    Bucket = "bucket",
    Stats = dict:from_list([{0, [{"high_seqno", "42"}]},
                            {1, [{"high_seqno", "43"}]},
                            {2, [{"low_seqno", "44"}]},
                            {3, [{"high_seqno", "forty two"}]}]),

    ?assertEqual({ok, [42, 43]},
                 get_local_high_seqnos_handle_results(Bucket, [0, 1], Stats)),
    ?assertMatch({error, _},
                 get_local_high_seqnos_handle_results(Bucket, [1, 2], Stats)),
    ?assertMatch({error, _},
                 get_local_high_seqnos_handle_results(Bucket, [1, 3], Stats)),
    ?assertMatch({error, _},
                 get_local_high_seqnos_handle_results(Bucket, [2, 3], Stats)),
    ?assertMatch({error, _},
                 get_local_high_seqnos_handle_results(Bucket, [2, 5], Stats)).
-endif.

maybe_delete_diverged_vbuckets(Bucket, VBuckets, ActiveFailoverLogs) ->
    case get_local_failover_info(Bucket, VBuckets) of
        {ok, FailoverInfo} ->
            ?log_debug("Local failover "
                       "info for bucket ~p:~n~p", [Bucket, FailoverInfo]),

            Diverged = find_diverged_vbuckets(Bucket,
                                              ActiveFailoverLogs, FailoverInfo),
            ok = delete_vbuckets(Bucket, Diverged),
            {ok, VBuckets -- Diverged};
        Error ->
            Error
    end.

delete_vbuckets(Bucket, VBuckets) ->
    lists:foreach(
      fun (VB) ->
              ok = ns_memcached:sync_delete_vbucket(Bucket, VB)
      end, VBuckets).

find_diverged_vbuckets(Bucket, ActiveFailoverLogs, LocalFailoverInfo) ->
    Diverged = do_find_diverged_vbuckets(ActiveFailoverLogs, LocalFailoverInfo),
    case Diverged of
        [] ->
            ?log_debug("Didn't find diverged vbuckets in bucket ~p", [Bucket]);
        _ ->
            ?log_info("Found diverged vbuckets in bucket ~p. VBuckets:~n~p",
                      [Bucket, Diverged])
    end,

    [VBucket || {VBucket, _, _, _} <- Diverged].

do_find_diverged_vbuckets(ActiveFailoverLogs, LocalFailoverInfo) ->
    lists:filtermap(
      fun ({VBucket, FailoverLog, HighSeqno}) ->
              ActiveFailoverLog = maps:get(VBucket, ActiveFailoverLogs),
              case check_vbuckets_compatible(ActiveFailoverLog,
                                             FailoverLog, HighSeqno) of
                  true ->
                      false;
                  false ->
                      {true, {VBucket,
                              ActiveFailoverLog, FailoverLog, HighSeqno}}
              end
      end, LocalFailoverInfo).

-ifdef(TEST).
do_find_diverged_vbuckets_test() ->
    ActiveFailoverLogs =
        maps:from_list([{0, [{a, 0},
                             {b, 10}]},
                        {1, [{a, 0},
                             {b, 15}]},
                        {2, missing}]),

    ?assertEqual([], do_find_diverged_vbuckets(ActiveFailoverLogs,
                                               [{0, [{a, 0},
                                                     {c, 10}], 10},
                                                {1, [{a, 0},
                                                     {c, 15}], 15}])),
    ?assertMatch([{1, _, _, _}],
                 do_find_diverged_vbuckets(ActiveFailoverLogs,
                                           [{0, [{a, 0},
                                                 {c, 10}], 10},
                                            {1, [{a, 0},
                                                 {c, 15}], 16}])),

    ?assertMatch([{2, _, _, _}],
                 do_find_diverged_vbuckets(ActiveFailoverLogs,
                                           [{0, [{a, 0},
                                                 {c, 10}], 10},
                                            {1, [{a, 0},
                                                 {c, 15}], 15},
                                            {2, [{a, 0},
                                                 {b, 10}], 10}])).

-endif.

check_vbuckets_compatible(missing, _LocalFailoverLog, _LocalHighSeqno) ->
    %% This is for the case when the vbucket doesn't have an active copy
    %% anymore after a series of failovers. Even though we could potentially
    %% preserve the local vbucket (and hence recover some data), historically,
    %% we've been simply deleting such vbuckets.
    false;
check_vbuckets_compatible(_ActiveFailoverLog, _LocalFailoverLog, 0) ->
    %% The local high seqno is 0, hence the vbucket is empty and we are
    %% trivially compatible.
    true;
check_vbuckets_compatible(ActiveFailoverLog,
                          LocalFailoverLog, LocalHighSeqno) ->
    case {failover_uid_for_seqno(LocalFailoverLog, LocalHighSeqno),
          failover_uid_for_seqno(ActiveFailoverLog, LocalHighSeqno)} of
        {{ok, LocalUID}, {ok, ActiveUID}} ->
            ActiveUID =:= LocalUID;
        _ ->
            %% We'll get here if we can't find a UID for our high seqno either
            %% in the new active vbucket or in our local vbucket. This may
            %% happen since failover log is capped in size. So if there are
            %% enough events that create new failover entries (unclean
            %% restarts or failovers), old entries fall off.
            false
    end.

-ifdef(TEST).
check_vbuckets_compatible_test() ->
    History = [{a, 0},
               {b, 10}],
    HighSeqno = 15,

    %% There's no active copy for the vbucket left.
    ?assert(not check_vbuckets_compatible(missing, History, HighSeqno)),

    %% Graceful failover:
    %%
    %%  - No extra entry created on new master.
    %%  - Delta node may have an extra failover entry but no mutations.
    ?assert(check_vbuckets_compatible(History, History, HighSeqno)),
    ?assert(check_vbuckets_compatible(History,
                                      History ++ [{c, HighSeqno}], HighSeqno)),

    %% Failover:
    %%
    %% - New master gets an extra failover entry.
    %% - Delta node may or may not get an extra entry.
    %% - Delta node may or may not get extra mutations (these mutations might
    %%   be true mutations received after node was failed over, or simply
    %%   mutations not replicated to the new master).

    %% No mutations on delta node case, hence vbuckets are compatible.
    ?assert(check_vbuckets_compatible(History ++ [{c, HighSeqno}],
                                      History ++ [{d, HighSeqno}],
                                      HighSeqno)),
    ?assert(check_vbuckets_compatible(History ++ [{c, HighSeqno}],
                                      History,
                                      HighSeqno)),

    %% New (or unreplicated) mutations on delta node, vbuckets are not
    %% compatible.
    ?assert(not check_vbuckets_compatible(History ++ [{c, HighSeqno}],
                                          History ++ [{d, HighSeqno + 5}],
                                          HighSeqno + 5)),
    ?assert(not check_vbuckets_compatible(History ++ [{c, HighSeqno}],
                                          History,
                                          HighSeqno + 5)),

    %% New master lost track of history.
    LostHistory = [{e, 100},
                   {f, 150}],

    ?assert(not check_vbuckets_compatible(LostHistory, History, HighSeqno)),
    ?assert(not check_vbuckets_compatible(LostHistory,
                                          History ++ [{d, HighSeqno}],
                                          HighSeqno)),
    ?assert(not check_vbuckets_compatible(LostHistory,
                                          History,
                                          HighSeqno + 200)),
    ?assert(not check_vbuckets_compatible(LostHistory,
                                          History ++ [{d, HighSeqno + 200}],
                                          HighSeqno + 200)),

    %% Local high seqno fell behind local failover history
    LocalLostHistory = [{b, HighSeqno}],
    ?assert(not check_vbuckets_compatible(History,
                                          LocalLostHistory, HighSeqno)),
    ?assert(not check_vbuckets_compatible(History,
                                          LocalLostHistory ++ [{c, HighSeqno}],
                                          HighSeqno)),
    ?assert(not check_vbuckets_compatible(LostHistory,
                                          LocalLostHistory, HighSeqno)).


-endif.

%% Get a failover entry in which given seqno was (or would have been)
%% committed.
failover_uid_for_seqno(FailoverLog, Seqno) ->
    Entries = lists:dropwhile(
                fun ({_UUID, StartSeqno}) ->
                        StartSeqno >= Seqno
                end, lists:reverse(FailoverLog)),

    case Entries of
        [] ->
            %% We'll get here if the vbucket is empty (and hence current high
            %% seqno is 0). This needs to be handled specially higher up the
            %% call stack.
            %%
            %% Since ep-engine only keeps a fixed number of failover entries,
            %% it's possible the new active vbucket had many new failover log
            %% entries and hence pruned the one we are interested in. In that
            %% case we can't say if histories of two vbuckets are compatible.
            {error, not_found};
        [{UID, _} | _] ->
            {ok, UID}
    end.

-ifdef(TEST).
failover_uid_for_seqno_test() ->
    FailoverLog = [{a, 0},
                   {b, 10},
                   {c, 10},
                   {d, 15},
                   {e, 16}],

    ?assertEqual({ok, d}, failover_uid_for_seqno(FailoverLog, 16)),
    ?assertEqual({ok, e}, failover_uid_for_seqno(FailoverLog, 17)),
    ?assertEqual({ok, c}, failover_uid_for_seqno(FailoverLog, 14)),
    ?assertEqual({ok, a}, failover_uid_for_seqno(FailoverLog, 10)),

    ?assertEqual({error, not_found}, failover_uid_for_seqno(FailoverLog, 0)),
    ?assertEqual({error, not_found}, failover_uid_for_seqno([{d, 15},
                                                             {e, 16}], 10)).
-endif.

run_async_job(JobBody, ResultHandler) ->
    gen_server2:async_job(
      ?JOB_QUEUE,
      fun () ->
              %% Register to make sure that no two jobs run at the same time
              %% due to a bug of some kind or if rebalance_agent gets killed
              %% brutally.
              register(?WORKER, self()),
              try
                  JobBody()
              after
                  unregister(?WORKER)
              end
      end, ResultHandler).

abort_rebalance(State) ->
    gen_server2:abort_queue(?JOB_QUEUE, {error, aborted}, State),
    case State#state.delta_recovery of
        {_Ref, _} ->
            stop_transient_buckets(State),
            State#state{delta_recovery = undefined};
        undefined ->
            State
    end.
