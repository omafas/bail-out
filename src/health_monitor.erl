%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% Common gen_server used by various monitors.
%%
-module(health_monitor).

-behaviour(gen_server).

-include("ns_common.hrl").

-define(INACTIVE_TIME, 2000000). % 2 seconds in microseconds
-define(REFRESH_INTERVAL, 1000). % 1 second heartbeat and refresh

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).
-export([common_init/1, common_init/2,
         time_diff_to_status/1,
         erase_unknown_nodes/2,
         local_monitors/0,
         node_monitors/1,
         supported_services/1,
         get_module/1,
         send_heartbeat/2, send_heartbeat/3,
         analyze_local_status/5]).

-record(state, {
          nodes :: dict:dict(),
          nodes_wanted :: [node()],
          monitor_module
         }).

start_link(MonModule) ->
    gen_server:start_link({local, MonModule}, ?MODULE, [MonModule], []).

%% gen_server callbacks
init([MonModule]) ->
    MonModule:init().

common_init(MonModule, with_refresh) ->
    self() ! refresh,
    common_init(MonModule).

common_init(MonModule) ->
    chronicle_compat_events:notify_if_key_changes([nodes_wanted],
                                                  peers_changed),
    {ok, #state{nodes = dict:new(),
                nodes_wanted = ns_node_disco:nodes_wanted(),
                monitor_module = MonModule}}.

handle_call(Call, From, #state{nodes = Statuses, nodes_wanted = NodesWanted,
                               monitor_module = MonModule} = State) ->
    case MonModule:handle_call(Call, From, Statuses, NodesWanted) of
        {ReplyType, Reply} ->
            {ReplyType, Reply, State};
        {ReplyType, Reply, NewStatuses} ->
            {ReplyType, Reply, State#state{nodes = NewStatuses}}
    end.

handle_cast({heartbeat, Node}, State) ->
    handle_cast({heartbeat, Node, empty}, State);
handle_cast({heartbeat, Node, Status},
            #state{nodes = Statuses, nodes_wanted = NodesWanted,
                   monitor_module = MonModule} = State) ->
    case lists:member(Node, NodesWanted) of
        true ->
            NewStatus = MonModule:annotate_status(Status),
            NewStatuses = dict:store(Node, NewStatus, Statuses),
            {noreply, State#state{nodes = NewStatuses}};
        false ->
            ?log_debug("Ignoring heartbeat from an unknown node ~p", [Node]),
            {noreply, State}
    end;

handle_cast(Cast, State) ->
    handle_message(handle_cast, Cast, State).

handle_info(refresh, State) ->
    RV = handle_message(handle_info, refresh, State),
    erlang:send_after(?REFRESH_INTERVAL, self(), refresh),
    RV;

handle_info(peers_changed, #state{nodes = Statuses} = State) ->
    NewNodesSorted = lists:usort(ns_node_disco:nodes_wanted()),
    FilteredStatuses = erase_unknown_nodes(Statuses, NewNodesSorted),
    {noreply, State#state{nodes = FilteredStatuses,
                          nodes_wanted = NewNodesSorted}};

handle_info(Info, State) ->
    handle_message(handle_info, Info, State).

handle_message(Fun, Msg, #state{nodes = Statuses, nodes_wanted = NodesWanted,
                                monitor_module = MonModule} = State) ->
    case erlang:apply(MonModule, Fun, [Msg, Statuses, NodesWanted]) of
        noreply ->
            {noreply, State};
        {noreply, NewStatuses} ->
            {noreply, State#state{nodes = NewStatuses}}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% APIs
time_diff_to_status(Diff) ->
    case erlang:convert_time_unit(Diff, native, microsecond)
        =< ?INACTIVE_TIME of
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

send_heartbeat(MonModule, SendNodes) ->
    send_heartbeat_inner(MonModule, SendNodes, {heartbeat, node()}).
send_heartbeat(MonModule, SendNodes, Payload) ->
    send_heartbeat_inner(MonModule, SendNodes, {heartbeat, node(), Payload}).

local_monitors() ->
    node_monitors(node()).

node_monitors(Node) ->
    [ns_server] ++ supported_services(Node).

supported_services(Node) ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    Services =
        [S || S <- supported_services_by_version(
                     cluster_compat_mode:get_compat_version()),
              ns_cluster_membership:should_run_service(Snapshot, S, Node)],
    %% we don't want services to trigger auto-failover to occur on an otherwise
    %% healthy kv node
    case lists:member(kv, Services) of
        true ->
            [kv];
        false ->
            Services
    end.

supported_services_by_version(ClusterVersion) ->
    [kv] ++
        case cluster_compat_mode:is_version_NEO(ClusterVersion) of
            true ->
                [index];
            false ->
                []
        end.

get_module(Monitor) ->
    list_to_atom(atom_to_list(Monitor) ++ "_monitor").

%% Internal functions
send_heartbeat_inner(MonModule, SendNodes, Payload) ->
    SendTo = SendNodes -- skip_heartbeats_to(MonModule),
    try
        misc:parallel_map(
          fun (N) ->
                  gen_server:cast({MonModule, N}, Payload)
          end, SendTo, ?REFRESH_INTERVAL - 10)
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
