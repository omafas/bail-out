%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_heart).

-behaviour(gen_server).

-include("ns_stats.hrl").
-include("ns_common.hrl").
-include("ns_heart.hrl").

-export([start_link/0, start_link_slow_updater/0, status_all/0,
         force_beat/0, grab_fresh_failover_safeness_infos/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% for hibernate
-export([slow_updater_loop/0]).

-record(state, {
          timer_ref :: reference() | undefined,
          event_handler :: pid() | undefined,
          slow_status = [] :: term(),
          slow_status_ts = 0 :: integer()
         }).


%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link_slow_updater() ->
    proc_lib:start_link(
      erlang, apply,
      [fun () ->
               erlang:register(ns_heart_slow_status_updater, self()),
               proc_lib:init_ack({ok, self()}),
               slow_updater_loop()
       end, []]).

is_interesting_buckets_event({started, _}) -> true;
is_interesting_buckets_event({loaded, _}) -> true;
is_interesting_buckets_event({stopped, _}) -> true;
is_interesting_buckets_event({warmed, _}) -> true;
is_interesting_buckets_event(_Event) -> false.

init([]) ->
    process_flag(trap_exit, true),

    State = send_beat_msg(0, #state{}),
    Self = self(),
    EventHandler =
        ns_pubsub:subscribe_link(
          buckets_events,
          fun (Event, _) ->
                  case is_interesting_buckets_event(Event) of
                      true ->
                          Self ! force_beat;
                      _ -> ok
                  end
          end, []),

    {ok, State#state{event_handler = EventHandler}}.

force_beat() ->
    ?MODULE ! force_beat.

maybe_send_forced_beat(#state{timer_ref = TRef} = State) ->
    %% If the expected time of the next heartbeat is less than or equal to
    %% 200 msecs then we don't need a forced beat.  If the expected time is
    %% greater than 200 msecs, we'll cancel the next heartbeat and send
    %% a new one to occur in 200 msecs.
    TimeLeft = case erlang:read_timer(TRef) of
                   false ->
                       %% Can happen when timer has expired
                       0;
                   TimeMS ->
                       TimeMS
               end,
    case TimeLeft > 200 of
        true ->
            State0 = cancel_normal_timer(State),
            send_beat_msg(200, State0);
        false ->
            State
    end.

cancel_normal_timer(#state{timer_ref = TRef} = State) ->
    erlang:cancel_timer(TRef),
    misc:flush(beat),
    State#state{timer_ref = undefined}.

handle_call(status, _From, State) ->
    {Status, NewState} = update_current_status(State),
    {reply, Status, NewState};
handle_call(Request, _From, State) ->
    {reply, {unhandled, ?MODULE, Request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({'EXIT', EventHandler, _} = ExitMsg,
            #state{event_handler=EventHandler} = State) ->
    ?log_debug("Dying because our event subscription was cancelled~n~p~n",
               [ExitMsg]),
    {stop, normal, State};
handle_info({slow_update, NewStatus, ReqTS}, State) ->
    {noreply, State#state{slow_status = NewStatus, slow_status_ts = ReqTS}};
handle_info(beat, State) ->
    {Status, State0} = update_current_status(State),
    heartbeat(Status),
    State1 = send_beat_msg(?HEART_BEAT_PERIOD, State0),
    {noreply, State1};
handle_info(force_beat, State) ->
    {noreply, maybe_send_forced_beat(State)};
handle_info(_, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% API
heartbeat(Status) ->
    catch misc:parallel_map(
            fun (N) ->
                    gen_server:cast({ns_doctor, N}, {heartbeat, node(), Status})
            end, [node() | nodes()], ?HEART_BEAT_PERIOD - 1000).

status_all() ->
    {Replies, _} = gen_server:multi_call([node() | nodes()], ?MODULE, status, 5000),
    Replies.

erlang_stats() ->
    try
        Stats = [wall_clock, context_switches, garbage_collection, io, reductions,
                 run_queue, runtime, run_queues],
        [{Stat, statistics(Stat)} || Stat <- Stats]
    catch _:_ ->
            %% NOTE: dialyzer doesn't like run_queues stat
            %% above. Given that this is useful but not a must, it
            %% makes sense to simply cover any exception
            []
    end.

%% Internal fuctions

send_beat_msg(Interval, State) ->
    TRef = erlang:send_after(Interval, self(), beat),
    State#state{timer_ref = TRef}.

eat_earlier_slow_updates(TS) ->
    receive
        {slow_update, _, ResTS} when ResTS < TS ->
            eat_earlier_slow_updates(TS)
    after 0 ->
            ok
    end.

update_current_status(#state{slow_status = []} = State) ->
    %% we don't have slow status at all; so compute it synchronously
    TS = erlang:monotonic_time(),
    QuickStatus = current_status_quick(TS),
    SlowStatus = current_status_slow(TS),
    NewState = State#state{slow_status = SlowStatus,
                           slow_status_ts = TS},
    {QuickStatus ++ SlowStatus, NewState};
update_current_status(State) ->
    TS = erlang:monotonic_time(),
    ns_heart_slow_status_updater ! {req, TS, self()},
    QuickStatus = current_status_quick(TS),

    receive
        %% NOTE: TS is bound already
        {slow_update, _, TS} = Msg ->
            eat_earlier_slow_updates(TS),
            {noreply, NewState} = handle_info(Msg, State),
            update_current_status_tail(NewState, QuickStatus)
    after ?HEART_BEAT_PERIOD ->
            update_current_status_no_reply(State, QuickStatus)
    end.

update_current_status_no_reply(State, QuickStatus) ->
    receive
        %% if we haven't see our reply yet, refresh status at
        %% least as much as there are replies seen
        {slow_update, _, _} = Msg ->
            {noreply, NewState} = handle_info(Msg, State),
            update_current_status_no_reply(NewState, QuickStatus)
    after 0 ->
            QuickStatus2 = [{stale_slow_status, State#state.slow_status_ts} | QuickStatus],
            update_current_status_tail(State, QuickStatus2)
    end.

update_current_status_tail(State, QuickStatus) ->
    Status = QuickStatus ++ State#state.slow_status,
    {Status, State}.

current_status_quick(TS) ->
    [{now, TS},
     {active_buckets, ns_memcached:active_buckets()},
     {ready_buckets, ns_memcached:warmed_buckets()}].

eat_all_reqs(TS, Count) ->
    receive
        {req, AnotherTS, _} ->
            true = (AnotherTS > TS),
            eat_all_reqs(AnotherTS, Count + 1)
    after 0 ->
            {TS, Count}
    end.

slow_updater_loop() ->
    receive
        {req, TS0, From} ->
            {TS, Eaten} = eat_all_reqs(TS0, 0),
            case Eaten > 0 of
                true -> ?log_warning("Dropped ~B heartbeat requests", [Eaten]);
                _ -> ok
            end,
            Status = current_status_slow(TS),
            From ! {slow_update, Status, TS},
            erlang:hibernate(?MODULE, slow_updater_loop, [])
    end.

current_status_slow(TS) ->
    Status0 = current_status_slow_inner(),

    Now  = erlang:monotonic_time(),
    Diff = misc:convert_time_unit(Now - TS, millisecond),

    ns_server_stats:notify_histogram(<<"status_latency">>, Diff),
    [{status_latency, Diff} | Status0].


current_status_slow_inner() ->
    BucketNames = ns_bucket:node_bucket_names(node()),

    PerBucketInterestingStats = stats_interface:buckets_interesting(),
    ProcessesStats = stats_interface:sysproc(),
    SystemStats = stats_interface:system(),

    InterestingStats =
        lists:foldl(fun ({BucketName, InterestingValues}, Acc) ->
                            ValuesDict = orddict:from_list(InterestingValues),
                            orddict:merge(fun (K, V1, V2) ->
                                                  try
                                                      V1 + V2
                                                  catch error:badarith ->
                                                          ?log_debug("Ignoring badarith when agregating interesting stats:~n~p~n",
                                                                     [{BucketName, K, V1, V2}]),
                                                          V1
                                                  end
                                          end, Acc, ValuesDict)
                    end, orddict:new(), PerBucketInterestingStats),

    Tasks = lists:filter(
        fun (Task) ->
                is_view_task(Task) orelse is_bucket_compaction_task(Task)
        end, ns_couchdb_api:get_tasks(2000, []) ++ local_tasks:all())
        ++ grab_local_xdcr_replications()
        ++ grab_samples_loading_tasks()
        ++ grab_warmup_tasks()
        ++ cluster_logs_collection_task:maybe_build_cluster_logs_task(),

    StorageConf = ns_storage_conf:query_storage_conf(),

    ProcFSFiles = grab_procfs_files(),
    ServiceStatuses = grab_service_statuses(),
    [{cpu_cores_available, CoresAvailable}] =
        sigar:get_gauges([cpu_cores_available]),
    ns_bootstrap:ensure_os_mon(),
    failover_safeness_level:build_local_safeness_info(BucketNames) ++
        ServiceStatuses ++
        ProcFSFiles ++
        [{local_tasks, Tasks},
         {memory, misc:memory()},
         {cpu_count, ceil(CoresAvailable)},
         {system_memory_data, memsup:get_system_memory_data()},
         {node_storage_conf, StorageConf},
         {statistics, erlang_stats()},
         {system_stats, [{N, proplists:get_value(N, SystemStats, 0)}
                         || N <- [cpu_utilization_rate, cpu_stolen_rate,
                                  swap_total, swap_used,
                                  mem_total, mem_free, mem_limit,
                                  cpu_cores_available, allocstall]]},
         {interesting_stats, InterestingStats},
         {per_bucket_interesting_stats, PerBucketInterestingStats},
         {processes_stats, ProcessesStats}
         | element(2, ns_info:basic_info())].

%% returns dict as if returned by ns_doctor:get_nodes/0 but containing only
%% failover safeness fields (or down bool property). Instead of going
%% to doctor it actually contacts all nodes and tries to grab fresh
%% information. See failover_safeness_level:build_local_safeness_info
grab_fresh_failover_safeness_infos(BucketNames) ->
    do_grab_fresh_failover_safeness_infos(BucketNames, 2000).

do_grab_fresh_failover_safeness_infos(BucketNames, Timeout) ->
    Nodes = ns_node_disco:nodes_actual(),
    {NodeResp, NodeErrors, DownNodes} =
        misc:rpc_multicall_with_plist_result(
          Nodes,
          failover_safeness_level, build_local_safeness_info,
          [BucketNames], Timeout),

    case NodeErrors =:= [] andalso DownNodes =:= [] of
        true ->
            ok;
        false ->
            ?log_warning("Some nodes didn't return their failover "
                         "safeness infos: ~n~p", [{NodeErrors, DownNodes}])
    end,

    dict:from_list(NodeResp).

is_view_task(Task) ->
    lists:keyfind(set, 1, Task) =/= false andalso
        begin
            {type, Type} = lists:keyfind(type, 1, Task),
            Type =:= indexer orelse
                Type =:= view_compaction
        end.

is_bucket_compaction_task(Task) ->
    {type, Type} = lists:keyfind(type, 1, Task),
    Type =:= bucket_compaction.

-define(STALE_XDCR_ERROR_SECONDS, ?get_param(xdcr_stale_error_seconds, 7200)).

%% NOTE: also removes datetime component
-spec filter_out_stale_xdcr_errors([{erlang:timestamp(), binary()}], integer()) -> [binary()].
filter_out_stale_xdcr_errors(Errors, NowGregorian) ->
    [Msg
     || {DateTime, Msg} <- Errors,
        NowGregorian - calendar:datetime_to_gregorian_seconds(DateTime) < ?STALE_XDCR_ERROR_SECONDS].

grab_local_xdcr_replications() ->
    NowGregorian = calendar:datetime_to_gregorian_seconds(erlang:localtime()),
    try goxdcr_rest:all_local_replication_infos() of
        Infos ->
            [begin
                 Errors = filter_out_stale_xdcr_errors(LastErrors, NowGregorian),
                 [{type, xdcr},
                  {id, Id},
                  {errors, Errors}
                  | Props]
             end || {Id, Props, LastErrors} <- Infos]
    catch T:E:S ->
            ?log_debug("Ignoring exception getting xdcr replication infos~n~p",
                       [{T, E, S}]),
            []
    end.

grab_samples_loading_tasks() ->
    try samples_loader_tasks:get_tasks(2000) of
        RawTasks ->
            [[{type, loadingSampleBucket},
              {bucket, list_to_binary(Name)},
              {pid, list_to_binary(pid_to_list(Pid))}]
             || {Name, Pid} <- RawTasks]
    catch T:E:S ->
            ?log_error("Failed to grab samples loader tasks: ~p",
                       [{T, E, S}]),
            []
    end.

grab_warmup_task(Bucket) ->
    Stats = try ns_memcached:warmup_stats(Bucket)
            catch exit:{noproc, _} ->
                    % it is possible that heartbeat happens before ns_memcached is started
                    [{<<"ep_warmup_state">>,
                      <<"starting ep-engine">>}]
            end,

    case Stats of
        [] ->
            [];
        _ ->
            [[{type, warming_up},
              {bucket, list_to_binary(Bucket)},
              {node, node()},
              {recommendedRefreshPeriod, 2.0},
              {stats, {struct, Stats}}]]
    end.

grab_warmup_tasks() ->
    BucketNames = ns_bucket:node_bucket_names(node()),
    lists:foldl(fun (Bucket, Acc) ->
                        Acc ++ grab_warmup_task(Bucket)
                end, [], BucketNames).

grab_service_statuses() ->
    Services = [S || S <- ns_cluster_membership:topology_aware_services(),
                     ns_cluster_membership:should_run_service(S, node())],
    [{{service_status, S}, grab_one_service_status(S)} || S <- Services].

grab_one_service_status(Service) ->
    try
        service_agent:get_status(Service, 2000)
    catch
        T:E:S ->
            ?log_error("Failed to grab service ~p status: ~p",
                       [Service, {T, E, S}]),
            []
    end.

grab_procfs_files() ->
    case misc:is_linux() of
        true ->
            Files = [{meminfo, "/proc/meminfo"},
                     {loadavg, "/proc/loadavg"},
                     {cpu_pressure, "/proc/pressure/cpu"},
                     {memory_pressure, "/proc/pressure/memory"},
                     {io_pressure, "/proc/pressure/io"}],
            [{Name, case misc:raw_read_file(Path) of
                        {ok, Content} ->
                            Content;
                        Error ->
                            Error
                    end} ||
                {Name, Path} <- Files];
        false ->
            []
    end.
