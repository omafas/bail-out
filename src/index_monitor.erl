%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(index_monitor).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include("cut.hrl").

-define(REFRESH_INTERVAL, ?get_param(refresh_interval, 2000)).
-define(DISK_ISSUE_THRESHOLD, ?get_param(disk_issue_threshold, 60)).
-define(MAX_HEALTH_CHECK_DURATION, ?get_param(max_health_check_duration, 2000)).

-record(state, {
          refresh_timer_ref :: undefined | reference(),
          health_checker :: pid(),
          tick = tock :: {tick, erlang:timestamp()} | tock,
          disk_failures = 0 :: integer(),
          prev_disk_failures :: integer() | undefined,
          last_tick_time = 0 :: integer(),
          last_tick_error = ok :: ok | {error, string()},
          enabled = false :: boolean(),
          num_samples :: integer() | undefined,
          health_info = <<>> :: binary()
         }).

-export([start_link/0]).
-export([get_nodes/0,
         analyze_status/2,
         is_node_down/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

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

init([]) ->
    Self = self(),
    Self ! reload_config,
    service_agent:spawn_connection_waiter(Self, index),

    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({auto_failover_cfg, _}) ->
              Self ! reload_config;
          (_) ->
              ok
      end),

    {ok, HealthChecker} = work_queue:start_link(),
    {ok, #state{health_checker = HealthChecker}}.

handle_cast({got_connection, Pid}, State) ->
    ?log_debug("Observed json_rpc_connection ~p", [Pid]),
    self() ! refresh,
    {noreply, State}.

handle_call(get_nodes, _From,
            #state{tick = Tick,
                   num_samples = NumSamples,
                   health_info = HealthInfo,
                   last_tick_time = LastTickTime,
                   last_tick_error = LastTickError} = State) ->
    Time =
        case Tick of
            tock ->
                LastTickTime;
            {tick, StartTS} ->
                TS = os:timestamp(),
                max(timer:now_diff(TS, StartTS), LastTickTime)
        end,
    Status =
        case Time >= ?MAX_HEALTH_CHECK_DURATION * 1000 of
            true ->
                ?log_debug("Last health check API call was slower than ~pms",
                           [?MAX_HEALTH_CHECK_DURATION]),
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
    {reply, dict:from_list([{node(), Status}]), State}.

handle_info({tick, HealthCheckResult},
            #state{tick = {tick, StartTS}} = State) ->
    TS = os:timestamp(),
    NewState = case HealthCheckResult of
                   {ok, DiskFailures} ->
                       State#state{disk_failures = DiskFailures,
                                   last_tick_error = ok};
                   {error, _} = Error ->
                       State#state{last_tick_error = Error}
               end,

    {noreply, NewState#state{tick = tock,
                             last_tick_time = timer:now_diff(TS, StartTS)}};

handle_info(reload_config, State) ->
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
                         round((TimePeriod * 1000)/?REFRESH_INTERVAL)
                 end}
        end,
    {noreply, State#state{num_samples = NumSamples, enabled = Enabled}};

handle_info(refresh, #state{tick = {tick, StartTS},
                            health_info = HealthInfo,
                            num_samples = NumSamples} = State) ->
    ?log_debug("Health check initiated at ~p didn't respond in time. "
               "Tick is missing", [StartTS]),
    NewHealthInfo = register_tick(true, HealthInfo, NumSamples),
    {noreply, resend_refresh_msg(State#state{health_info = NewHealthInfo})};

handle_info(refresh, #state{tick = tock,
                            disk_failures = DiskFailures,
                            prev_disk_failures = PrevDiskFailures,
                            health_info = HealthInfo,
                            num_samples = NumSamples} = State) ->
    Healthy = PrevDiskFailures == undefined orelse
        DiskFailures =< PrevDiskFailures,
    NewHealthInfo = register_tick(Healthy, HealthInfo, NumSamples),
    NewState = State#state{prev_disk_failures = DiskFailures,
                           health_info = NewHealthInfo},
    {noreply, resend_refresh_msg(initiate_health_check(NewState))}.

health_check(#state{enabled = false,
                    disk_failures = DiskFailures}) ->
    {ok, DiskFailures};
health_check(#state{enabled = true}) ->
    case service_api:health_check(index) of
        {ok, {[{<<"diskFailures">>, DiskFailures}]}} ->
            {ok, DiskFailures};
        {error, Error} ->
            {error, Error}
    end.

resend_refresh_msg(#state{refresh_timer_ref = undefined} = State) ->
    Ref = erlang:send_after(?REFRESH_INTERVAL, self(), refresh),
    State#state{refresh_timer_ref = Ref};
resend_refresh_msg(#state{refresh_timer_ref = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    resend_refresh_msg(State#state{refresh_timer_ref = undefined}).

initiate_health_check(#state{health_checker = HealthChecker,
                             tick = tock} = State) ->
    Self = self(),
    TS = os:timestamp(),
    work_queue:submit_work(HealthChecker,
                           ?cut(Self ! {tick, health_check(State)})),
    State#state{tick = {tick, TS}}.

register_tick(_Healthy, _HealthInfo, undefined) ->
    <<>>;
register_tick(Healthy, HealthInfo, NumSamples) ->
    kv_stats_monitor:register_tick(Healthy, HealthInfo, NumSamples).

is_unhealthy(_HealthInfo, undefined) ->
    false;
is_unhealthy(HealthInfo, NumSamples) ->
    Threshold = round(NumSamples * ?DISK_ISSUE_THRESHOLD / 100),
    kv_stats_monitor:is_unhealthy(HealthInfo, Threshold).
