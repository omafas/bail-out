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

-include("ns_common.hrl").

-export([start_link/0]).
-export([get_nodes/0]).
-export([init/0, handle_call/4, handle_cast/3, handle_info/3]).

start_link() ->
    health_monitor:start_link(?MODULE).

%% gen_server callbacks
init() ->
    health_monitor:common_init(?MODULE, with_refresh).

handle_call(get_nodes, _From, Statuses, _Nodes) ->
    {reply, Statuses};

handle_call(Call, From, Statuses, _Nodes) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, Statuses]),
    {reply, nack}.

handle_cast(Cast, Statuses, _Nodes) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, Statuses]),
    noreply.

handle_info(refresh, Statuses, NodesWanted) ->
    %% Fetch each node's view of every other node and analyze it.
    AllNodes = node_monitor:get_nodes(),
    NewStatuses = lists:foldl(
                    fun (Node, Acc) ->
                            NewState = analyze_status(Node, AllNodes),
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
    {noreply, NewStatuses};

handle_info(Info, Statuses, _Nodes) ->
    ?log_warning("Unexpected message ~p when in state:~n~p", [Info, Statuses]),
    noreply.

%% APIs
get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

%% Internal functions
analyze_status(Node, AllNodes) ->
    Monitors = health_monitor:node_monitors(Node),
    {Healthy, Unhealthy, Other} = lists:foldl(
                                    fun (Monitor, Accs) ->
                                            analyze_monitor_status(Monitor,
                                                                   Node,
                                                                   AllNodes,
                                                                   Accs)
                                    end, {[], [], []}, Monitors),

    case lists:subtract(Monitors, Healthy) of
        [] ->
            healthy;
        _ ->
            case lists:subtract(Monitors, Unhealthy) of
                [] ->
                    unhealthy;
                _ ->
                    {needs_attention, lists:sort(Unhealthy ++ Other)}
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
