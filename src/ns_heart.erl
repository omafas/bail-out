%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2019 Couchbase, Inc.
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
          forced_beat_timer :: reference() | undefined,
          event_handler :: pid(),
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

    timer2:send_interval(?HEART_BEAT_PERIOD, beat),
    self() ! beat,
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

    {ok, #state{event_handler=EventHandler}}.

force_beat() ->
    ?MODULE ! force_beat.

arm_forced_beat_timer(#state{forced_beat_timer = TRef} = State) when TRef =/= undefined ->
    State;
arm_forced_beat_timer(State) ->
    TRef = erlang:send_after(200, self(), beat),
    State#state{forced_beat_timer = TRef}.

disarm_forced_beat_timer(#state{forced_beat_timer = undefined} = State) ->
    State;
disarm_forced_beat_timer(#state{forced_beat_timer = TRef} = State) ->
    erlang:cancel_timer(TRef),
    State#state{forced_beat_timer = undefined}.


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
    NewState = disarm_forced_beat_timer(State),
    Dropped = misc:flush(beat),
    case Dropped of
        0 ->
            ok;
        _ ->
            ?log_warning("Dropped ~p heartbeats", [Dropped])
    end,
    {Status, NewState2} = update_current_status(NewState),
    heartbeat(Status),
    {noreply, NewState2};
handle_info(force_beat, State) ->
    {noreply, arm_forced_beat_timer(State)};
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

is_interesting_stat({curr_items, _}) -> true;
is_interesting_stat({curr_items_tot, _}) -> true;
is_interesting_stat({vb_replica_curr_items, _}) -> true;
is_interesting_stat({mem_used, _}) -> true;
is_interesting_stat({couch_docs_actual_disk_size, _}) -> true;
is_interesting_stat({couch_views_actual_disk_size, _}) -> true;
is_interesting_stat({couch_spatial_disk_size, _}) -> true;
is_interesting_stat({couch_docs_data_size, _}) -> true;
is_interesting_stat({couch_views_data_size, _}) -> true;
is_interesting_stat({couch_spatial_data_size, _}) -> true;
is_interesting_stat({vb_active_num_non_resident, _}) -> true;
is_interesting_stat({cmd_get, _}) -> true;
is_interesting_stat({get_hits, _}) -> true;
is_interesting_stat({ep_bg_fetched, _}) -> true;
is_interesting_stat({ops, _}) -> true;
is_interesting_stat(_) -> false.


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
        {req, AnotherTS} ->
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
    Diff = misc:convert_time_unit(Now - TS, microsecond),

    system_stats_collector:add_histo(status_latency, Diff),
    [{status_latency, Diff} | Status0].

grab_latest_stats(Bucket) ->
    case catch stats_archiver:latest_sample(Bucket, minute) of
        {ok, #stat_entry{values = Values}} ->
            Values;
        Error ->
            ?log_debug("Ignoring failure to grab ~p stats:~n~p~n", [Bucket, Error]),
            []
    end.

add_interesting_index_stats(BucketName, BucketStats) ->
    IndexStats = grab_latest_stats("@index-" ++ BucketName),

    DataSizeKey =
        service_stats_collector:global_stat(service_index, <<"data_size">>),
    DiskSizeKey =
        service_stats_collector:global_stat(service_index, <<"disk_size">>),

    DataSize = proplists:get_value(DataSizeKey, IndexStats, 0),
    DiskSize = proplists:get_value(DiskSizeKey, IndexStats, 0),
    orddict:store(index_disk_size, DiskSize,
                  orddict:store(index_data_size, DataSize, BucketStats)).

current_status_slow_inner() ->
    [SystemStats, ProcessesStats] =
        [grab_latest_stats(PseudoBucket) || PseudoBucket <- ["@system", "@system-processes"]],

    BucketNames = ns_bucket:node_bucket_names(node()),

    IndexStatus = grab_index_status(),
    Indexes = proplists:get_value(indexes, IndexStatus, []),
    IndexBuckets = sets:from_list([binary_to_list(B) || {B, _} <- Indexes]),

    PerBucketInterestingStats =
        lists:foldl(fun (BucketName, Acc) ->
                            Values = grab_latest_stats(BucketName),
                            InterestingValues0 = lists:filter(fun is_interesting_stat/1, Values),

                            InterestingValues =
                                case sets:is_element(BucketName, IndexBuckets) of
                                    true ->
                                        add_interesting_index_stats(BucketName, InterestingValues0);
                                    false ->
                                        InterestingValues0
                                end,

                            [{BucketName, InterestingValues} | Acc]
                    end, [], BucketNames),

    InterestingStats =
        lists:foldl(fun ({BucketName, InterestingValues}, Acc) ->
                            orddict:merge(fun (K, V1, V2) ->
                                                  try
                                                      V1 + V2
                                                  catch error:badarith ->
                                                          ?log_debug("Ignoring badarith when agregating interesting stats:~n~p~n",
                                                                     [{BucketName, K, V1, V2}]),
                                                          V1
                                                  end
                                          end, Acc, InterestingValues)
                    end, [], PerBucketInterestingStats),

    Tasks = lists:filter(
        fun (Task) ->
                is_view_task(Task) orelse is_bucket_compaction_task(Task)
        end, ns_couchdb_api:get_tasks(2000, []) ++ local_tasks:all())
        ++ grab_local_xdcr_replications()
        ++ grab_samples_loading_tasks()
        ++ grab_warmup_tasks()
        ++ cluster_logs_collection_task:maybe_build_cluster_logs_task(),

    ClusterCompatVersion = cluster_compat_mode:effective_cluster_compat_version(),

    StorageConf = ns_storage_conf:query_storage_conf(),

    InterestingProcessesStats =
        lists:filter(
          fun ({Name, _}) ->
                  case binary:split(Name, <<$/>>, [global]) of
                      [_Process, StatName] ->
                          lists:member(StatName,
                                       [<<"mem_resident">>, <<"mem_size">>,
                                        <<"cpu_utilization">>, <<"major_faults_raw">>]);
                      _ ->
                          false
                  end;
              (_) ->
                  false
          end, ProcessesStats),

    ProcFSFiles = grab_procfs_files(),
    ServiceStatuses = grab_service_statuses(),

    ns_bootstrap:ensure_os_mon(),
    failover_safeness_level:build_local_safeness_info(BucketNames) ++
        ServiceStatuses ++
        ProcFSFiles ++
        [{local_tasks, Tasks},
         {memory, misc:memory()},
         {cpu_count, misc:cpu_count()},
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
         {processes_stats, InterestingProcessesStats},
         {cluster_compatibility_version, ClusterCompatVersion}
         | element(2, ns_info:basic_info())].

grab_index_status() ->
    case ns_cluster_membership:should_run_service(index, node()) of
        true ->
            try
                {ok, Status} = service_index:get_status(2000),
                Status
            catch T:E ->
                    ?log_debug("ignoring failure to get index status: ~p~n~p",
                               [{T, E}, erlang:get_stacktrace()]),
                    []
            end;
        false ->
            []
    end.

%% returns dict as if returned by ns_doctor:get_nodes/0 but containing only
%% failover safeness fields (or down bool property). Instead of going
%% to doctor it actually contacts all nodes and tries to grab fresh
%% information. See failover_safeness_level:build_local_safeness_info
grab_fresh_failover_safeness_infos(BucketsAll) ->
    do_grab_fresh_failover_safeness_infos(BucketsAll, 2000).

do_grab_fresh_failover_safeness_infos(BucketsAll, Timeout) ->
    Nodes = ns_node_disco:nodes_actual(),
    BucketNames = proplists:get_keys(BucketsAll),
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
    catch T:E ->
            ?log_debug("Ignoring exception getting xdcr replication infos~n~p", [{T,E,erlang:get_stacktrace()}]),
            []
    end.

grab_samples_loading_tasks() ->
    try samples_loader_tasks:get_tasks(2000) of
        RawTasks ->
            [[{type, loadingSampleBucket},
              {bucket, list_to_binary(Name)},
              {pid, list_to_binary(pid_to_list(Pid))}]
             || {Name, Pid} <- RawTasks]
    catch T:E ->
            ?log_error("Failed to grab samples loader tasks: ~p", [{T,E,erlang:get_stacktrace()}]),
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
        T:E ->
            ?log_error("Failed to grab service ~p status: ~p",
                       [Service, {T, E, erlang:get_stacktrace()}]),
            []
    end.

grab_procfs_files() ->
    case os:type() of
        {_, linux} ->
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
        _ ->
            []
    end.
