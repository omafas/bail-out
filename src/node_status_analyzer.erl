%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% node_status_analyzer runs on each node and analyzes status of all nodes
%% in the cluster.
%%
%%  - Periodically, it fetches the status information stored
%%    by the node_monitor and analyzes it.
%%
%%  - node_monitor on each node send the status information to the
%%    orchestrator/master.
%%
%%  - The status returned by the node_monitor on master contains each node’s
%%    view of every other node in the cluster. Different monitors running
%%    on a node can have different view of the status of other nodes
%%    in the cluster.
%%    This monitor specific status is contained in the information returned
%%    by the node_monitor.
%%
%%    E.g. information returned by the node_monitor on the master:
%%
%%    [{node1, <======== node1's view of other nodes
%%             node1_status, <======== "active" if node1 sent this recently
%%             [{node2, [{monitor1, node2_status}, {monitor2, node2_status}]},
%%              {node1, [{monitor1, node1_status}, {monitor2, node1_status}]},
%%              {node3, ...},
%%              ...]},
%%     {node2, <======== node2's view of other nodes
%%             node2_status,
%%             [{node2, [{monitor1, node2_status}, {monitor2, node2_status}]},
%%              {node1, [{monitor1, node1_status}, {monitor2, node1_status}]},
%%              {node3, ...},
%%              ...]},
%%     {node3, ..., [...]},
%%     ...]
%%
%%  - node_status_analyzer then calls monitor specific analyzers to interpret
%%    the above information. These analyzers determine health of a particular
%%    node by taking view of all nodes into consideration.
%%
%%  - At the end of the analysis, a node is considered:
%%      - healthy: if all monitors report that the node is healthy.
%%      - unhealthy: if all monitor report the node is unhealthy.
%%      - {needs_attention, <analysis_returned_by_the_monitor>}:
%%           if different monitors return different status for the node.
%%           E.g. ns_server analyzer reports the node is healthy but KV
%%           analyzer reports that some buckets are not ready.

-module(node_status_analyzer).

-behaviour(health_monitor).

-include("ns_common.hrl").

-ifdef(TEST).
-include("ns_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([get_statuses/0,
         can_refresh/1]).
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

%% gen_server callbacks
init(BaseMonitorState) ->
    NodesWanted = ns_node_disco:nodes_wanted(),

    Self = self(),
    chronicle_compat_events:subscribe(
      fun ({node, _, membership}) ->
              true;
          ({node, _, services}) ->
              true;
          (nodes_wanted) ->
              true;
          (_) -> false
      end,
      fun (_) ->
              Self ! node_changed
      end),

    BaseMonitorState#{monitors => get_monitors(NodesWanted)}.

handle_call(get_statuses, _From, MonitorState) ->
    #{nodes := Statuses} = MonitorState,
    {reply, Statuses};

handle_call(Call, From, MonitorState) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, MonitorState]),
    {reply, nack}.

handle_cast(Cast, MonitorState) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, MonitorState]),
    noreply.

handle_info(refresh, MonitorState) ->
    #{nodes := Statuses,
      nodes_wanted := NodesWanted,
      monitors := AllMonitors} = MonitorState,
    %% Fetch each node's view of every other node and analyze it.
    AllNodes = node_monitor:get_statuses(),
    NewStatuses = lists:foldl(
                    fun (Node, Acc) ->
                            NewState = analyze_status(Node, AllNodes,
                                                      AllMonitors),
                            Status = case dict:find(Node, Statuses) of
                                         {ok, {NewState, _} = OldStatus} ->
                                             %% Node state has not changed.
                                             %% Do not update the timestamp.
                                             OldStatus;
                                         _ ->
                                             {NewState, erlang:monotonic_time()}
                                     end,
                            dict:store(Node, Status, Acc)
                    end, dict:new(), NodesWanted),

    {noreply, MonitorState#{nodes => NewStatuses}};

handle_info(node_changed, MonitorState) ->
    NodesWanted = ns_node_disco:nodes_wanted(),
    {noreply,
     MonitorState#{monitors => get_monitors(NodesWanted)}};

handle_info(Info, MonitorState) ->
    ?log_warning("Unexpected message ~p when in state:~n~p",
                 [Info, MonitorState]),
    noreply.

%% APIs
get_statuses() ->
    gen_server:call(?MODULE, get_statuses).

%% Internal functions

get_monitors(NodesWanted) ->
    lists:map(
        fun(Node) ->
            NodeMonitors = health_monitor:node_monitors(Node),
            {Node, NodeMonitors}
        end, NodesWanted).

analyze_status(Node, AllNodes, AllMonitors) ->
    NodeMonitors =
        case proplists:get_value(Node, AllMonitors) of
            undefined ->
                %% Health monitors not found in the cached monitors for the
                %% given node, just fetch them straight from the config.
                health_monitor:node_monitors(Node);
            Monitors -> Monitors
        end,
    {Healthy, Unhealthy, Other} = lists:foldl(
                                    fun (Monitor, Accs) ->
                                            analyze_monitor_status(Monitor,
                                                                   Node,
                                                                   AllNodes,
                                                                   Accs)
                                    end, {[], [], []}, NodeMonitors),

    case lists:subtract(NodeMonitors, Healthy) of
        [] ->
            healthy;
        _ ->
            case lists:subtract(NodeMonitors, Unhealthy) of
                [] ->
                    unhealthy;
                _ ->
                    {needs_attention, [{U, unhealthy} || U <- Unhealthy] ++
                         Other}
            end
    end.

analyze_monitor_status(Monitor, Node, AllNodes,
                       {Healthy, Unhealthy, Other}) ->
    Mod = health_monitor:get_module(Monitor),
    case Mod:analyze_status(Node, AllNodes) of
        healthy ->
            {[Monitor | Healthy], Unhealthy, Other};
        unhealthy ->
            {Healthy, [Monitor | Unhealthy], Other};
        State ->
            {Healthy, Unhealthy, [{Monitor, State} | Other]}
    end.

can_refresh(_State) ->
    true.

-ifdef(TEST).
common_test_setup() ->
    ?meckNew(ns_pubsub),
    %% Setup a fake ns_pubsub capturing the key update handler function so
    %% that we can drive config updates as though they came via chronicle.
    ?meckNew(fake_ns_pubsub, [non_strict]),

    meck:expect(ns_pubsub, subscribe_link,
        fun(_, Handler) ->
            %% Stash the handler in some function, notify_key
            meck:expect(fake_ns_pubsub, notify_key,
                fun(Key) ->
                    Handler(Key)
                end),
            ok
        end).

%% See health_monitor.erl for tests common to all monitors that use these
%% functions
health_monitor_test_setup() ->
    common_test_setup(),

    ?meckNew(node_monitor, [passthrough]),
    meck:expect(node_monitor,
                get_statuses,
                fun() ->
                        []
                end),

    %% If we refresh with the kv_monitor as one of our active monitors then
    %% we will attempt to analyze statuses from it which calls functions that
    %% we don't care to mock here. Just mock the kv_monitor function result
    %% instead.
    ?meckNew(kv_monitor, [passthrough]),
    meck:expect(kv_monitor,
                analyze_status,
                fun(_,_) ->
                        []
                end).

get_monitors_from_state() ->
    %% Do a get_statuses (handle_call) to ensure that we have processed anything
    %% that would cause a state update
    get_statuses(),

    %% Bit of a hack, but this is a test, we can grab the internal state of
    %% the monitor via sys:get_state to check that we are tracking the
    %% correct monitors.
    {state, node_status_analyzer, #{monitors := Monitors}}
        = sys:get_state(?MODULE),
    Monitors.

health_monitor_t() ->
    %% Test that we find new monitors
    ?assertEqual([{node(), [ns_server]}], get_monitors_from_state()),

    meck:expect(ns_cluster_membership, should_run_service,
                fun(_Snapshot, _Service, _Node) ->
                        true
                end),

    fake_ns_pubsub:notify_key(nodes_wanted),

    ?assertEqual([{node(), [ns_server, kv]}], get_monitors_from_state()),

    %% Test new node is added and tracked
    meck:expect(ns_node_disco,
                nodes_wanted,
                fun() ->
                        [node(), "otherNode"]
                end),

    fake_ns_pubsub:notify_key(nodes_wanted),

    %% We are now tracking monitors for "otherNode"
    ?assertEqual([{node(), [ns_server, kv]},
                  {"otherNode", [ns_server, kv]}], get_monitors_from_state()).

common_test_teardown() ->
    ok.

health_monitor_test_teardown() ->
    common_test_teardown(),

    ?meckUnload(node_monitor),
    ?meckUnload(kv_monitor).

-endif.
