%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(ns_server_monitor).

-include("ns_common.hrl").

-behaviour(health_monitor).

-export([start_link/0]).
-export([get_statuses/0,
         can_refresh/1,
         annotate_status/1,
         analyze_status/2,
         is_node_down/1]).
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
    BaseMonitorState.

handle_call(get_statuses, _From, MonitorState) ->
    #{nodes := Statuses} = MonitorState,
    Now = erlang:monotonic_time(),
    InactiveTime =
        health_monitor:calculate_inactive_time(MonitorState),
    RV = dict:map(
           fun (_Node, {recv_ts, RecvTS}) ->
                   health_monitor:time_diff_to_status(Now - RecvTS,
                                                      InactiveTime);
               (_Node, Status) ->
                   Status
           end, Statuses),
    {reply, RV};

handle_call(Call, From, MonitorState) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, MonitorState]),
    {reply, nack}.

handle_cast(Cast, MonitorState) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, MonitorState]),
    noreply.

handle_info(refresh, MonitorState) ->
    #{nodes_wanted := NodesWanted} = MonitorState,
    health_monitor:send_heartbeat(?MODULE, NodesWanted, MonitorState),
    noreply;

handle_info(Info, State) ->
    ?log_warning("Unexpected message ~p when in state:~n~p", [Info, State]),
    noreply.

%% APIs
get_statuses() ->
    gen_server:call(?MODULE, get_statuses).

annotate_status(empty) ->
    {recv_ts, erlang:monotonic_time()}.

is_node_down(unhealthy) ->
    {true, {"The cluster manager did not respond.", cluster_manager_down}};
is_node_down(_) ->
    false.

analyze_status(Node, AllNodes) ->
    %% AllNodes contains each node's view of every other node in the
    %% cluster.
    %% Find which node's have Node as active and which don't.
    {Actives, Inactives} = lists:foldl(
                             fun (OtherNodeView, Accs) ->
                                     analyze_node_view(OtherNodeView,
                                                       Node,
                                                       Accs)
                             end, {[], []}, AllNodes),
    case Inactives of
        [] ->
            %% Things are healthy if all other node's say Node is active.
            healthy;
        _ ->
            case Actives of
                [] ->
                    unhealthy;
                _ ->
                    %% If some nodes say Node is active and other's
                    %% don't then it is potentially a network
                    %% partition or communication is flaky.
                    {potential_network_partition, lists:sort(Inactives)}
            end
    end.

%% Internal functions
analyze_node_view({OtherNode, inactive, _}, Node, {Active, Inactive}) ->
    case Node =:= OtherNode of
        %% Flag this node as inactive if we're missing an update from it.
        %% Note: If all other nodes continue to receive updates from this node,
        %% they'll consider this node healthy even if the node itself stops
        %% sending its monitor statuses to the orchestrator (but continues to
        %% send heartbeats to all nodes in the cluster). This may happen because
        %% of an asymmetric network connectivity issue where it stops receiving
        %% traffic from other nodes.
        true -> {Active, [Node | Inactive]};
        %% Ignore the node's stale view of _other_ nodes since it hasn't been
        %% updated in the last refresh interval.
        _ -> {Active, Inactive}
    end;
analyze_node_view({OtherNode, _, NodeView}, Node, {Active, Inactive}) ->
    Status = proplists:get_value(Node, NodeView, []),
    case proplists:get_value(ns_server, Status, unknown) of
        active ->
            {[OtherNode | Active], Inactive};
        _ ->
            {Active, [OtherNode | Inactive]}
    end.

can_refresh(_State) ->
    true.

-ifdef(TEST).
common_test_setup() ->
    ok.

%% See health_monitor.erl for tests common to all monitors that use these
%% functions
health_monitor_test_setup() ->
    common_test_setup().

health_monitor_t() ->
    ok.

common_test_teardown() ->
    ok.

health_monitor_test_teardown() ->
    common_test_teardown().

-endif.
