%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(index_monitor).

-behavior(health_monitor).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include("ns_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DISK_ISSUE_THRESHOLD, ?get_param(disk_issue_threshold, 60)).

-export([start_link/0]).
-export([get_statuses/0,
         analyze_status/2,
         is_node_down/1,
         can_refresh/1,
         init/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).

-ifdef(TEST).
-export([common_test_setup/0,
         common_test_teardown/0,
         health_monitor_test_setup/0,
         health_monitor_t/0,
         health_monitor_test_teardown/0]).
-endif.

start_link() ->
    health_monitor:start_link(?MODULE).

get_statuses() ->
    gen_server:call(?MODULE, get_statuses).

is_node_down(health_check_slow) ->
    {true, {"The index service took too long to respond to the health check",
            health_check_slow}};
is_node_down(io_failed) ->
    {true, {"I/O failures are detected by index service", io_failure}};
is_node_down({_, health_check_error} = Error) ->
    {true, Error}.

analyze_status(Node, AllNodes) ->
    health_monitor:analyze_local_status(
      Node, AllNodes, index, fun functools:id/1, healthy).

init(BaseMonitorState) ->
    Self = self(),
    self() ! reload_config,
    service_agent:spawn_connection_waiter(Self, index),

    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({auto_failover_cfg, _}) ->
              Self ! reload_config;
          (_) ->
              ok
      end),

    {ok, HealthChecker} = work_queue:start_link(),
    BaseMonitorState#{health_checker => HealthChecker,
                      tick => tock,
                      disk_failures => 0,
                      prev_disk_failures => undefined,
                      last_tick_time => 0,
                      last_tick_error => ok,
                      enabled => false,
                      num_samples => undefined,
                      health_info => <<>>}.

handle_cast({got_connection, Pid}, MonitorState) ->
    ?log_debug("Observed json_rpc_connection ~p", [Pid]),

    %% We only ever expect to see a got_connection cast once, on completion
    %% of the service_agent spawning the connection for us. As such we can
    %% send ourselves a refresh message here without worrying about having
    %% multiple refreshes (as the only thing that should otherwise send a
    %% refresh is the health_monitor behaviour and that won't start
    %% refreshing til we process this first refresh).
    self() ! refresh,

    {noreply, MonitorState#{connection_established => true}};

handle_cast(Msg, State) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p",
                 [Msg, State]),
    {noreply, State}.

handle_call(get_statuses, _From, MonitorState) ->
    #{tick := Tick,
      num_samples := NumSamples,
      health_info := HealthInfo,
      last_tick_time := LastTickTime,
      last_tick_error := LastTickError,
      refresh_interval := RefreshInterval} = MonitorState,
    Time =
        case Tick of
            tock ->
                LastTickTime;
            {tick, StartTS} ->
                TS = os:timestamp(),
                max(timer:now_diff(TS, StartTS), LastTickTime)
        end,
    Status =
        case Time >= RefreshInterval * 1000 of
            true ->
                ?log_debug("Last health check API call was slower than ~pms",
                           [RefreshInterval]),
                health_check_slow;
            false ->
                case LastTickError of
                    {error, Error} ->
                        ?log_debug("Detected health check error ~p", [Error]),
                        {Error, health_check_error};
                    ok ->
                        case is_unhealthy(HealthInfo, NumSamples) of
                            true ->
                                ?log_debug("Detected IO failure"),
                                io_failed;
                            false ->
                                healthy
                        end
                end
        end,
    {reply, dict:from_list([{node(), Status}]), MonitorState};

handle_call(Call, From, State) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, State]),
    {reply, nack, State}.

handle_info({tick, HealthCheckResult}, MonitorState) ->
    #{tick := {tick, StartTS}} = MonitorState,
    TS = os:timestamp(),
    NewState = case HealthCheckResult of
                   {ok, DiskFailures} ->
                       MonitorState#{disk_failures => DiskFailures,
                                     last_tick_error => ok};
                   {error, _} = Error ->
                       MonitorState#{last_tick_error => Error}
               end,

    {noreply, NewState#{tick => tock,
                        last_tick_time => timer:now_diff(TS, StartTS)}};

handle_info(reload_config, MonitorState) ->
    #{refresh_interval := RefreshInterval} = MonitorState,
    Cfg = auto_failover:get_cfg(),
    {Enabled, NumSamples} =
        case auto_failover:is_enabled(Cfg) of
            false ->
                {false, undefined};
            true ->
                {true,
                 case menelaus_web_auto_failover:get_failover_on_disk_issues(
                        Cfg) of
                     {false, _} ->
                         undefined;
                     {true, TimePeriod} ->
                         round((TimePeriod * 1000)/RefreshInterval)
                 end}
        end,
    {noreply, MonitorState#{num_samples => NumSamples,
                            enabled => Enabled}};

handle_info(refresh, #{tick := {tick, StartTS},
                       health_info := HealthInfo,
                       num_samples := NumSamples} = MonitorState) ->
    ?log_debug("Health check initiated at ~p didn't respond in time. "
               "Tick is missing", [StartTS]),
    NewHealthInfo = register_tick(true, HealthInfo, NumSamples),
    {noreply, MonitorState#{health_info => NewHealthInfo}};

handle_info(refresh, #{tick := tock,
                       disk_failures := DiskFailures,
                       prev_disk_failures := PrevDiskFailures,
                       health_info := HealthInfo,
                       num_samples := NumSamples} = MonitorState) ->
    Healthy = PrevDiskFailures == undefined orelse
        DiskFailures =< PrevDiskFailures,
    NewHealthInfo = register_tick(Healthy, HealthInfo, NumSamples),
    NewState = MonitorState#{prev_disk_failures => DiskFailures,
                             health_info => NewHealthInfo},
    {noreply, initiate_health_check(NewState)};

handle_info(Info, State) ->
    ?log_warning("Unexpected message ~p when in state:~n~p", [Info, State]),
    {noreply, State}.

health_check(#{enabled := false,
               disk_failures := DiskFailures}) ->
    {ok, DiskFailures};
health_check(#{enabled := true}) ->
    case service_api:health_check(index) of
        {ok, {[{<<"diskFailures">>, DiskFailures}]}} ->
            {ok, DiskFailures};
        {error, Error} ->
            {error, Error}
    end.

initiate_health_check(#{health_checker := HealthChecker,
                        tick := tock} = MonitorState) ->
    Self = self(),
    TS = os:timestamp(),
    work_queue:submit_work(HealthChecker,
                           ?cut(Self ! {tick, health_check(MonitorState)})),
    MonitorState#{tick => {tick, TS}}.

register_tick(_Healthy, _HealthInfo, undefined) ->
    <<>>;
register_tick(Healthy, HealthInfo, NumSamples) ->
    kv_stats_monitor:register_tick(Healthy, HealthInfo, NumSamples).

is_unhealthy(_HealthInfo, undefined) ->
    false;
is_unhealthy(HealthInfo, NumSamples) ->
    Threshold = round(NumSamples * ?DISK_ISSUE_THRESHOLD / 100),
    kv_stats_monitor:is_unhealthy(HealthInfo, Threshold).

can_refresh(#{connection_established := CanRefresh}) ->
    CanRefresh;
can_refresh(_State) ->
    false.

-ifdef(TEST).
%% See health_monitor.erl for tests common to all monitors that use these
%% functions
common_test_setup() ->
    ?meckNew(service_agent),
    meck:expect(service_agent,
                spawn_connection_waiter,
                fun(_,index) ->
                        ok
                end),

    ?meckNew(ns_pubsub),
    meck:expect(ns_pubsub, subscribe_link,
                fun(_,_) ->
                        ok
                end),

    ?meckNew(auto_failover, [passthrough]),
    meck:expect(auto_failover, get_cfg, fun() -> [] end).

health_monitor_test_setup() ->
    %% Mock ourselves so that we can check some history later
    ?meckNew(index_monitor, [passthrough]),

    common_test_setup().

health_monitor_t() ->
    {state, index_monitor, #{tick := InitialTick}} = sys:get_state(?MODULE),
    ?assertEqual(tock, InitialTick),

    %% Processing got_connection should result in us sending (and processing)
    %% a refresh message
    gen_server:cast(?MODULE, {got_connection, pid}),
    meck:wait(index_monitor, handle_cast, [{got_connection, pid}, '_'], 1000),

    meck:wait(index_monitor, handle_info, [refresh, '_'], 1000).

common_test_teardown() ->
    ?meckUnload(service_agent),
    ?meckUnload(ns_pubsub),
    ?meckUnload(auto_failover).

health_monitor_test_teardown() ->
    ?meckUnload(index_monitor),

    common_test_teardown().

-endif.
