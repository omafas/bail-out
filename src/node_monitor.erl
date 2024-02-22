%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% node_monitor:
%%  - Gathers status information from all monitors running on the local node
%%  and exchanges this information with the orchestrator.
%%

-module(node_monitor).

-behaviour(health_monitor).

-include("ns_common.hrl").

-ifdef(TEST).
-include("ns_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([get_statuses/0,
         can_refresh/1,
         annotate_status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-ifdef(TEST).
-export([common_test_setup/0,
         common_test_teardown/0,
         health_monitor_test_setup/0,
         health_monitor_t/0,
         health_monitor_test_teardown/0]).
-endif.

start_link() ->
    health_monitor:start_link(?MODULE).

init(BaseMonitorState) ->
    Monitors = health_monitor:local_monitors(),

    %% We only run non-ns_server monitors for active (non-failed over) nodes,
    %% subscribe to updates for the local node state. In theory services
    %% shouldn't change once we setup a node, but subscribe to that too just
    %% in case.
    chronicle_compat_events:notify_if_key_changes(
        [{node, node(), membership},
         {node, node(), services}], node_changed),


    BaseMonitorState#{local_monitors => Monitors}.

handle_call(get_statuses, _From, MonitorState) ->
    #{nodes := Statuses} = MonitorState,
    Now = erlang:monotonic_time(),
    InactiveTime =
        health_monitor:calculate_inactive_time(MonitorState),
    RV = dict:fold(
           fun (Node, {Status, {recv_ts, RecvTS}}, Acc) ->
                   [{Node, health_monitor:time_diff_to_status(
                             Now - RecvTS, InactiveTime), Status} | Acc]
           end, [], Statuses),
    {reply, RV};

handle_call(Call, From, MonitorState) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, MonitorState]),
    {reply, nack}.

handle_cast(Cast, MonitorState) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, MonitorState]),
    noreply.

handle_info(refresh, MonitorState) ->
    #{nodes_wanted := NodesWanted,
      local_monitors := LocalMonitors} = MonitorState,
    %% We need to send our status to the node where the auto-failover logic
    %% is running. This is the mb_master node. Normally, this is also
    %% the node where the orchestrator is running.
    %% But, during certain scenarios, mb_master and the orchestrator node may
    %% be different.
    %% E.g. #1: During orchestrator failover, it make take some time for
    %% the new master to register the singleton processes. During
    %% that period, node where the orchestrator is registered will be
    %% different from the mb_master:master_node().
    %% E.g. #2: If for some reason mb_master heartbeats from
    %% the current master are not reaching some nodes, those
    %% nodes will try to become the new master. There may be a period
    %% where the mb_master ping pongs among two or more nodes. But,
    %% the node where orchestrator is registered will remain the same.
    %% Send our status to both the mb_master and the orchestrator.
    Orchestrator = case leader_registry:whereis_name(ns_orchestrator) of
                       undefined ->
                           [];
                       OC ->
                           [node(OC)]
                   end,
    %% Future possibility:
    %% Store identitity of the master in local state.
    %% Whenever master changes, mb_master can post an event which this
    %% module can listen to.
    %% Similar thing can be done for the orchestrator.
    Master = case mb_master:master_node() of
                 undefined ->
                     [];
                 M ->
                     [M]
             end,
    SendTo = lists:umerge3(Orchestrator, Master, [node()]),

    Payload = latest_status(NodesWanted, LocalMonitors),
    health_monitor:send_heartbeat(?MODULE, SendTo, Payload, MonitorState),
    noreply;

handle_info(node_changed, MonitorState) ->
    Monitors = health_monitor:local_monitors(),
    {noreply, MonitorState#{local_monitors => Monitors}};

handle_info(Info, MonitorState) ->
    ?log_warning("Unexpected message ~p when in state:~n~p",
                 [Info, MonitorState]),
    noreply.

%% APIs
get_statuses() ->
    gen_server:call(?MODULE, get_statuses).

annotate_status(Status) ->
    {Status, {recv_ts, erlang:monotonic_time()}}.

%% Internal functions

%% Get latest status from all local health monitors
%% Output:
%%   [{Node1, [{monitor1, <node1_status>}, {monitor2, <node1_status>}, ...]},
%%    {Node2, [{monitor1, <node2_status>}, {monitor2, <node2_status>}, ...]},
%%    ...]
latest_status(NodesWanted, LocalMonitors) ->
    AllMonitors = lists:map(fun (Monitor) ->
                                    Module = health_monitor:get_module(Monitor),
                                    {Monitor, Module:get_statuses()}
                            end, LocalMonitors),
    lists:map(
      fun (Node) ->
              Status = [{Monitor, node_status(Node, NodesDict)} ||
                           {Monitor, NodesDict} <- AllMonitors],
              {Node, Status}
      end, NodesWanted).

node_status(Node, Dict) ->
    case dict:find(Node, Dict) of
        {ok, Status} ->
            Status;
        _ ->
            []
    end.

can_refresh(_State) ->
    true.

-ifdef(TEST).
common_test_setup() ->
    ?meckNew(mb_master),
    meck:expect(mb_master, master_node, fun() -> node() end).

%% See health_monitor.erl for tests common to all monitors that use these
%% functions
health_monitor_test_setup() ->
    common_test_setup(),

    ?meckNew(ns_server_monitor, [passthrough]),
    meck:expect(ns_server_monitor,
                get_statuses,
                fun() ->
                        dict:append(node(), dict:new(), dict:new())
                end),

    ?meckNew(kv_monitor, [passthrough]),
    meck:expect(kv_monitor,
                get_statuses,
                fun() ->
                        dict:append(node(), dict:new(), dict:new())
                end),
    ok.

health_monitor_t() ->
    %% Bit of a hack, but this is a test, we can grab the internal state of
    %% the monitor via sys:get_state to check that we are tracking the
    %% correct monitors.
    {state, node_monitor, #{local_monitors := LocalMonitors1}} =
        sys:get_state(?MODULE),

    ?assertEqual([ns_server], LocalMonitors1),

    %% Should not have called kv_monitor get_statuses yet because we aren't
    %% tracking a kv_monitor.
    ?assertNot(meck:called(kv_monitor, get_statuses, [])),

    %% Meck so that when we process the node_changed message we add the
    %% kv_monitor to the list
    meck:expect(ns_cluster_membership, should_run_service,
                fun(_Snapshot, _Service, _Node) ->
                        true
                end),

    %% Now lets check that we update the local monitors when we see a
    %% node_changed message
    ?MODULE ! node_changed,

    {state, node_monitor, #{local_monitors := LocalMonitors2}} =
        sys:get_state(?MODULE),
    ?assertEqual([ns_server, kv], LocalMonitors2),

    %% Send a refresh to check that we now pull the state from the kv_monitor
    ?MODULE ! refresh,

    %% Do a get_statuses (handle_call) to ensure that we have processed the
    %% refresh for the next test
    get_statuses(),

    %% Finally, check that we have now called kv_monitor:get_statuses()
    ?assert(meck:called(kv_monitor, get_statuses, [])).

common_test_teardown() ->
    ?meckUnload(mb_master).

health_monitor_test_teardown() ->
    common_test_teardown(),
    ?meckUnload(ns_server_monitor),
    ?meckUnload(kv_monitor).

-endif.
