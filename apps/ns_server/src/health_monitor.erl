%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%% @doc
%% health_monitor is a common gen_server used by the various health monitors
%% to gather status information about the cluster to feed into auto_failover.
%% Health monitors should implement the "health_monitor" behaviour to provide
%% the required APIs. index_monitor is a notable exception to this as it is
%% implemented almost entirely independently of the code here.
%% @end
-module(health_monitor).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include("ns_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%% State created by and required by this base monitor behaviour.
-type base_state() :: #{refresh_interval := pos_integer(),
                        nodes := dict:dict(),
                        nodes_wanted := [node()],
                        any() => any()}.

-type service_monitor() :: service() | ns_server.

%% @doc
%% Behaviour that the actual monitors should implement. Much of this is
%% implemented via a gen_server style API, note the differing number of
%% parameters.
%% @end
-callback start_link() -> term().
%% Returns a list of monitor specific state unknown to us here so just use map()
-callback init(base_state()) -> base_state().
-callback handle_call(term(), term(), base_state()) ->
    noreply | {reply, nack} | {reply, term(), base_state()}.
-callback handle_cast(term(), base_state()) ->
    noreply | {noreply, base_state()}.
-callback handle_info(term(), base_state()) ->
    noreply | {noreply, base_state()}.

%% Other API required for the behaviour.
-callback get_statuses() -> term().
-callback can_refresh(map()) -> boolean().

%% We wait for ?INACTIVE_TICKS ticks before considering a node inactive and
%% eligible for failover.
-define(INACTIVE_TICKS, ?get_param(inactive_ticks, 2)).
-define(DEFAULT_REFRESH_INTERVAL, 1000). % 1 second heartbeat and refresh

%% Not all monitors support sub-second refreshing. In particular, some of the
%% service monitors. We don't want to scale the refresh intervals of these
%% monitors below one second as it could cause instability and spurious
%% auto-failovers.
-define(MONITORS_SUPPORTING_SUB_SECOND_REFRESH,
        ?get_param(monitor_supporting_sub_second_refresh,
                   [ns_server_monitor,
                    node_monitor,
                    node_status_analyzer,
                    kv_stats_monitor,
                    index_monitor,
                    kv_monitor])).

%% Auto failover configs with less than a 5 second timeout will scale down
%% refresh intervals automatically.
-define(AUTO_FAILOVER_MIN_SCALING_TIMEOUT, 5).

%% The number of ticks expected within an auto failover timeout. It should be
%% noted that with an auto failover timeout value of 5 seconds no refresh
%% interval scaling is applied and monitors will refresh at a 1 second
%% interval. This implies a scaling factor of 5 rather than 10. This
%% behaviour is maintained for existing use cases to avoid increasing CPU
%% usage, and the refresh inveral scaling factor of 10  will be applied to any
%% auto failover timeout value lower than 5 second.
-define(REFRESH_INTERVAL_SCALING_FACTOR,
        ?get_param(refresh_interval_scaling_factor, 10)).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).
-export([calculate_inactive_time/1,
         time_diff_to_status/2,
         erase_unknown_nodes/2,
         local_monitors/0,
         node_monitors/1,
         supported_services/1,
         get_module/1,
         send_heartbeat/3, send_heartbeat/4,
         analyze_local_status/5,
         maybe_calculate_refresh_interval/1,
         maybe_scale_refresh_interval/2,
         get_refresh_interval/1]).

-ifdef(TEST).
-export([common_test_setup/0,
         common_test_teardown/0]).
-endif.

-record(state, {
                monitor_module,
                %% Map such that monitors can add any state they wish
                monitor_state :: map()
               }).

start_link(MonModule) ->
    gen_server:start_link({local, MonModule}, ?MODULE, [MonModule], []).

%% gen_server callbacks
init([MonModule]) ->
    %% All monitors using this module should have at least this state, rather
    %% than define it in every file we can merge it into the MonitorState
    %% (which it exists in because it's needed in both this file and the
    %% specialised monitor).
    BaseMonitorState = #{nodes => dict:new(),
                         nodes_wanted => ns_node_disco:nodes_wanted(),
                         refresh_interval => get_refresh_interval(MonModule)},

    MonitorState = MonModule:init(BaseMonitorState),

    case MonModule:can_refresh(MonitorState) of
        false -> ok;
        true -> self() ! refresh
    end,

    chronicle_compat_events:notify_if_key_changes(
      [auto_failover_cfg,
       health_monitor_refresh_interval,
       nodes_wanted], config_updated),

    {ok, #state{monitor_module = MonModule,
                monitor_state = MonitorState}}.

handle_call(Call, From,
            #state{monitor_module = MonModule,
                   monitor_state = MonState} =
                State) ->
    case MonModule:handle_call(Call, From, MonState) of
        {ReplyType, Reply} ->
            {ReplyType, Reply, State};
        {ReplyType, Reply, NewMonitorState} ->
            {ReplyType, Reply, State#state{monitor_state = NewMonitorState}}
    end.

handle_cast({heartbeat, Node}, State) ->
    handle_cast({heartbeat, Node, empty}, State);
handle_cast({heartbeat, Node, Status},
            #state{monitor_module = MonModule,
                   monitor_state = MonState}
            = State) ->
    #{nodes := Statuses,
      nodes_wanted := NodesWanted} = MonState,
    case lists:member(Node, NodesWanted) of
        true ->
            NewStatus = MonModule:annotate_status(Status),
            NewStatuses = dict:store(Node, NewStatus, Statuses),
            {noreply, State#state{monitor_state =
                                      MonState#{nodes => NewStatuses}}};
        false ->
            ?log_debug("Ignoring heartbeat from an unknown node ~p", [Node]),
            {noreply, State}
    end;

handle_cast(Cast, State) ->
    handle_message(handle_cast, Cast, State).

handle_info(refresh, #state{monitor_module = MonModule,
                            monitor_state = MonState} = State) ->
    true = MonModule:can_refresh(MonState),
    #{refresh_interval := RefreshInterval} = MonState,

    RV = handle_message(handle_info, refresh, State),
    erlang:send_after(RefreshInterval, self(), refresh),
    RV;

handle_info(config_updated, #state{monitor_module = MonModule,
                                   monitor_state = MonState} = State) ->
    #{nodes := Statuses} = MonState,
    NewNodesSorted = lists:usort(ns_node_disco:nodes_wanted()),
    FilteredStatuses = erase_unknown_nodes(Statuses, NewNodesSorted),
    RefreshInterval = get_refresh_interval(MonModule),
    {noreply,
     State#state{monitor_state =
                     MonState#{nodes => FilteredStatuses,
                               nodes_wanted => NewNodesSorted,
                               refresh_interval => RefreshInterval}}};

handle_info(Info, State) ->
    handle_message(handle_info, Info, State).

handle_message(Fun, Msg, #state{monitor_module = MonModule,
                                monitor_state = MonState} = State) ->
    case erlang:apply(MonModule, Fun, [Msg, MonState]) of
        noreply ->
            {noreply, State};
        {noreply, NewMonitorState} ->
            %% Note, we're just replacing the monitor_state here rather than
            %% merging it under the expectation that the monitor
            %% implementation will return the full state rather than some
            %% partial one.
            {noreply, State#state{monitor_state = NewMonitorState}}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% APIs
-spec calculate_inactive_time(map()) -> integer().
calculate_inactive_time(MonitorState) ->
    #{refresh_interval := RefreshInterval} = MonitorState,
    erlang:convert_time_unit(RefreshInterval, millisecond, microsecond) *
        ?INACTIVE_TICKS.

time_diff_to_status(Diff, InactiveTime) ->
    case erlang:convert_time_unit(Diff, native, microsecond)
        =< InactiveTime of
        true ->
            active;
        false ->
            inactive
    end.

erase_unknown_nodes(Statuses, Nodes) ->
    SortedNodes = ordsets:from_list(Nodes),
    dict:filter(fun (Node, _Status) ->
                        ordsets:is_element(Node, SortedNodes)
                end, Statuses).

-spec send_heartbeat(atom(), [node()], map()) -> ok.
send_heartbeat(MonModule, SendNodes, MonitorState) ->
    #{refresh_interval := RefreshInterval} = MonitorState,
    send_heartbeat_inner(MonModule, SendNodes, {heartbeat, node()},
                         RefreshInterval).

-spec send_heartbeat(atom(), [node()], term(), map()) -> ok.
send_heartbeat(MonModule, SendNodes, Payload, MonitorState) ->
    #{refresh_interval := RefreshInterval} = MonitorState,
    send_heartbeat_inner(MonModule, SendNodes, {heartbeat, node(), Payload},
                         RefreshInterval).

-spec local_monitors() -> [service_monitor()].
local_monitors() ->
    node_monitors(node()).

-spec node_monitors(node()) -> [service_monitor()].
node_monitors(Node) ->
    [ns_server] ++ supported_services(Node).

-spec supported_services(node()) -> [service_monitor()].
supported_services(Node) ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    Services =
        [S || S <- supported_services(),
              ns_cluster_membership:should_run_service(Snapshot, S, Node)],
    %% we don't want services to trigger auto-failover to occur on an otherwise
    %% healthy kv node
    case lists:member(kv, Services) of
        true ->
            [kv];
        false ->
            Services
    end.

supported_services() ->
    [kv, index].

get_module(Monitor) ->
    list_to_atom(atom_to_list(Monitor) ++ "_monitor").

-spec get_refresh_interval(atom()) -> integer().
get_refresh_interval(MonModule) ->
    case ns_config:read_key_fast(health_monitor_refresh_interval, []) of
        Intervals when is_list(Intervals) ->
            case proplists:get_value(MonModule, Intervals) of
                undefined ->
                    maybe_calculate_refresh_interval(MonModule,
                                                     ?DEFAULT_REFRESH_INTERVAL);
                Value ->
                    Value
            end
    end.

%% Internal functions
send_heartbeat_inner(MonModule, SendNodes, Payload, RefreshInterval) ->
    SendTo = SendNodes -- skip_heartbeats_to(MonModule),
    try
        misc:parallel_map(
          fun (N) ->
                  gen_server:cast({MonModule, N}, Payload)
          end, SendTo, RefreshInterval - 10)
    catch exit:timeout ->
            ?log_warning("~p send heartbeat timed out~n", [MonModule])
    end.

skip_heartbeats_to(MonModule) ->
    TestCondition = list_to_atom(atom_to_list(MonModule) ++ "_skip_heartbeat"),
    case testconditions:get(TestCondition) of
        false ->
            [];
        SkipList ->
            ?log_debug("~p skip heartbeats to ~p ~n", [MonModule, SkipList]),
            SkipList
    end.

analyze_local_status(Node, AllNodes, Service, Fun, Default) ->
    case lists:keyfind(Node, 1, AllNodes) of
        false ->
            Default;
        {Node, active, View} ->
            case proplists:get_value(Node, View, []) of
                [] ->
                    Default;
                Status ->
                    case proplists:get_value(Service, Status, unknown) of
                        unknown ->
                            Default;
                        [] ->
                            Default;
                        Stuff ->
                            Fun(Stuff)
                    end
            end;
        _ ->
            Default
    end.

scale_refresh_interval_for_timeout(Timeout) ->
    TimeoutMs = erlang:convert_time_unit(Timeout, second, millisecond),
    round(TimeoutMs / ?REFRESH_INTERVAL_SCALING_FACTOR).

maybe_calculate_refresh_interval(MonModule, DefaultValue) ->
    case lists:member(MonModule,
                      ?MONITORS_SUPPORTING_SUB_SECOND_REFRESH) of
        true ->
            maybe_calculate_refresh_interval(DefaultValue);
        false ->
            DefaultValue
    end.

-spec maybe_calculate_refresh_interval(pos_integer()) -> pos_integer().
maybe_calculate_refresh_interval(DefaultValue) ->
    AutoFailoverCfg = ns_config:read_key_fast(auto_failover_cfg, undefined),
    case AutoFailoverCfg of
        undefined ->
            DefaultValue;
        Cfg ->
            case proplists:get_value(timeout, Cfg) of
                undefined ->
                    DefaultValue;
                Timeout ->
                    maybe_scale_refresh_interval(Timeout, DefaultValue)
            end
    end.

-spec maybe_scale_refresh_interval(pos_integer(), pos_integer()) ->
          pos_integer().
maybe_scale_refresh_interval(Timeout, DefaultValue) ->
    case Timeout < ?AUTO_FAILOVER_MIN_SCALING_TIMEOUT of
        false -> DefaultValue;
        true -> scale_refresh_interval_for_timeout(Timeout)
    end.

-ifdef(TEST).
common_test_setup() ->
    ?meckNew(chronicle_compat_events, [passthrough]),
    meck:expect(chronicle_compat_events,
                notify_if_key_changes,
                fun (_,_) ->
                        ok
                end),

    ?meckNew(ns_node_disco),
    meck:expect(ns_node_disco,
                nodes_wanted,
                fun() ->
                        [node()]
                end),

    ?meckNew(ns_config),
    meck:expect(ns_config, read_key_fast,
                fun(health_monitor_refresh_interval, DefaultVal) ->
                        DefaultVal;
                   (auto_failover_cfg, _) ->
                        [{timeout, 1}]
                end),

    meck:expect(ns_config, search_node_with_default,
                fun(_, DefaultValue) ->
                        DefaultValue
                end),

    ?meckNew(ns_cluster_membership, [passthrough]),
    meck:expect(ns_cluster_membership,
                get_snapshot,
                fun() ->
                        #{}
                end),

    meck:expect(ns_cluster_membership,
                should_run_service,
                fun(_,_,_) ->
                        false
                end),

    ?meckNew(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode,
                get_compat_version,
                fun() ->
                        ?VERSION_76
                end),

    ?meckNew(testconditions),
    meck:expect(testconditions,
                get,
                fun(_) ->
                        false
                end).

basic_test_setup(Monitor) ->
    common_test_setup(),

    %% Do all of our common setup before the monitor specific setup in case the
    %% specific monitor needs the ?meckNew() calls (it's not safe to call twice)
    Monitor:health_monitor_test_setup().

common_test_teardown() ->
    ?meckUnload(chronicle_compat_events),
    ?meckUnload(ns_node_disco),
    ?meckUnload(ns_config),
    ?meckUnload(ns_cluster_membership),
    ?meckUnload(cluster_compat_mode),
    ?meckUnload(testconditions).

basic_test_teardown(Monitor, _X) ->
    Monitor:health_monitor_test_teardown(),
    common_test_teardown().

test_refresh_interval_scaling(Monitor) ->
    ExpectedTimeout =
        case lists:member(Monitor,
                          ?MONITORS_SUPPORTING_SUB_SECOND_REFRESH) of
            false -> ?DEFAULT_REFRESH_INTERVAL;
            true -> scale_refresh_interval_for_timeout(1)
        end,

    ?assertEqual(ExpectedTimeout,
                 get_refresh_interval(Monitor)).

basic_test_t(Monitor) ->
    {ok, Pid} = Monitor:start_link(),

    %% Jump into some monitor specific tests first, they may care about
    %% initial states
    Monitor:health_monitor_t(),

    %% Some unknown cast shouldn't crash us, but it also should not do
    %% anything interesting that we can test. It does log something, but
    %% mecking ale doesn't appear to be possible. Whilst we can test the
    %% message was passed on as expected, the handlers for these messages
    %% are very permissive so it's difficult to test that we are doing
    %% the right things...
    gen_server:cast(Pid, undefined),

    %% Some unknown info shouldn't crash us, but it also should not do
    %% anything interesting that we can test...
    Pid ! undefined,

    %% Refresh tested automatically by any refreshing monitor, but doing
    %% it explicitly doesn't hurt.
    case Monitor:can_refresh(sys:get_state(Pid)) of
        true ->
            Pid ! refresh;
        false ->
            ok
    end,

    %% And the same for a call...
    ?assertEqual(nack, gen_server:call(Pid, undefined)),

    test_refresh_interval_scaling(Monitor),

    gen_server:stop(Pid).

basic_test_() ->
    Monitors =
        [ns_server_monitor,
         dcp_traffic_monitor,
         node_status_analyzer,
         node_monitor,
         kv_monitor,
         kv_stats_monitor,
         index_monitor],

    {foreachx,
     fun basic_test_setup/1,
     fun basic_test_teardown/2,
     [{Monitor, fun (M, _) ->
                        {"Testing " ++ atom_to_list(M), ?cut(basic_test_t(M))}
                end} || Monitor <- Monitors]}.

-endif.
