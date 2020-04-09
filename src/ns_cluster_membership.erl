%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
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
-module(ns_cluster_membership).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_nodes_with_status/1,
         get_nodes_with_status/2,
         get_nodes_with_status/3,
         active_nodes/0,
         active_nodes/1,
         inactive_added_nodes/0,
         actual_active_nodes/0,
         actual_active_nodes/1,
         get_cluster_membership/1,
         get_cluster_membership/2,
         get_node_server_group/2,
         activate/1,
         deactivate/1,
         failover/2,
         re_failover/1,
         system_joinable/0,
         start_rebalance/3,
         stop_rebalance/1,
         get_rebalance_status/0,
         is_balanced/0,
         get_recovery_type/2,
         update_recovery_type/2,
         is_newly_added_node/1,
         attach_node_uuids/2
        ]).

-export([supported_services/0,
         allowed_services/1,
         supported_services_for_version/1,
         cluster_supported_services/0,
         topology_aware_services/0,
         topology_aware_services_for_version/1,
         default_services/0,
         set_service_map/2,
         get_service_map/2,
         failover_service_nodes/3,
         service_has_pending_failover/2,
         service_clear_pending_failover/1,
         node_active_services/1,
         node_active_services/2,
         node_services/1,
         node_services/2,
         service_active_nodes/1,
         service_active_nodes/2,
         service_actual_nodes/2,
         service_nodes/2,
         service_nodes/3,
         should_run_service/2,
         should_run_service/3,
         user_friendly_service_name/1]).

get_nodes_with_status(PredOrStatus) ->
    get_nodes_with_status(ns_config:latest(), PredOrStatus).

get_nodes_with_status(Config, PredOrStatus) ->
    get_nodes_with_status(Config,
                          ns_node_disco:nodes_wanted(Config), PredOrStatus).

get_nodes_with_status(Config, Nodes, Status)
  when is_atom(Status) ->
    get_nodes_with_status(Config, Nodes, _ =:= Status);
get_nodes_with_status(Config, Nodes, Pred)
  when is_function(Pred, 1) ->
    [Node || Node <- Nodes,
             Pred(get_cluster_membership(Node, Config))].

active_nodes() ->
    active_nodes(ns_config:get()).

active_nodes(Config) ->
    get_nodes_with_status(Config, active).

inactive_added_nodes() ->
    get_nodes_with_status(inactiveAdded).

actual_active_nodes() ->
    actual_active_nodes(ns_config:get()).

actual_active_nodes(Config) ->
    get_nodes_with_status(Config, ns_node_disco:nodes_actual(), active).

get_cluster_membership(Node) ->
    get_cluster_membership(Node, ns_config:get()).

get_cluster_membership(Node, Config) ->
    case ns_config:search(Config, {node, Node, membership}) of
        {value, Value} ->
             Value;
        _ ->
            inactiveAdded
    end.

get_node_server_group(Node, Config) ->
    {value, Groups} = ns_config:search(Config, server_groups),
    get_node_server_group_inner(Node, Groups).

get_node_server_group_inner(_, []) ->
    undefined;
get_node_server_group_inner(Node, [SG | Rest]) ->
    case lists:member(Node, proplists:get_value(nodes, SG)) of
        true ->
            proplists:get_value(name, SG);
        false ->
            get_node_server_group_inner(Node, Rest)
    end.

system_joinable() ->
    ns_node_disco:nodes_wanted() =:= [node()].

get_rebalance_status() ->
    ns_orchestrator:rebalance_progress().

start_rebalance(KnownNodes, EjectedNodes, DeltaRecoveryBuckets) ->
    ns_orchestrator:start_rebalance(KnownNodes, EjectedNodes, DeltaRecoveryBuckets).

activate(Nodes) ->
    ns_config:set([{{node, Node, membership}, active} ||
                      Node <- Nodes]).

deactivate(Nodes) ->
    ns_config:set([{{node, Node, membership}, inactiveFailed}
                   || Node <- Nodes]).

is_newly_added_node(Node) ->
    get_cluster_membership(Node) =:= inactiveAdded andalso
        get_recovery_type(ns_config:latest(), Node) =:= none.

is_stop_rebalance_safe() ->
    case ns_config:search(rebalancer_pid) of
        false ->
            true;
        {value, undefined} ->
            true;
        {value, Pid} ->
            PidNode = node(Pid),
            MasterNode = mb_master:master_node(),
            PidNode =:= MasterNode
    end.

stop_rebalance(AllowUnsafe) ->
    case AllowUnsafe of
        true ->
            stop_rebalance();
        false ->
            stop_rebalance_if_safe()
    end.

stop_rebalance() ->
    ns_orchestrator:stop_rebalance().

stop_rebalance_if_safe() ->
    %% NOTE: this is inherently raceful. But race is tiny and largely
    %% harmless. So we KISS instead.
    case is_stop_rebalance_safe() of
        false ->
            unsafe;
        _ ->
            stop_rebalance()
    end.

is_balanced() ->
    not ns_orchestrator:needs_rebalance().

failover(Nodes, AllowUnsafe) ->
    ns_orchestrator:failover(Nodes, AllowUnsafe).

re_failover_possible(NodeString) ->
    case (catch list_to_existing_atom(NodeString)) of
        Node when is_atom(Node) ->
            RecoveryType = ns_config:search(ns_config:latest(), {node, Node, recovery_type}, none),
            Membership = ns_config:search(ns_config:latest(), {node, Node, membership}),
            Ok = (lists:member(Node, ns_node_disco:nodes_wanted())
                  andalso RecoveryType =/= none
                  andalso Membership =:= {value, inactiveAdded}),
            case Ok of
                true ->
                    {ok, Node};
                _ ->
                    not_possible
            end;
        _ ->
            not_possible
    end.

%% moves node from pending-recovery state to failed over state
%% used when users hits Cancel for pending-recovery node on UI
re_failover(NodeString) ->
    true = is_list(NodeString),
    case re_failover_possible(NodeString) of
        {ok, Node} ->
            KVList = [{{node, Node, membership}, inactiveFailed},
                      {{node, Node, recovery_type}, none}],
            ns_config:set(KVList),
            ok;
        not_possible ->
            not_possible
    end.

get_recovery_type(Config, Node) ->
    ns_config:search(Config, {node, Node, recovery_type}, none).

-spec update_recovery_type(node(), delta | full) -> ok | bad_node | conflict.
update_recovery_type(Node, NewType) ->
    RV = ns_config:run_txn(
           fun (Config, Set) ->
                   Membership = ns_config:search(Config, {node, Node, membership}),

                   case (Membership =:= {value, inactiveAdded}
                         andalso get_recovery_type(Config, Node) =/= none)
                       orelse Membership =:= {value, inactiveFailed} of
                       true ->
                           Config1 = Set({node, Node, membership}, inactiveAdded, Config),
                           {commit,
                            Set({node, Node, recovery_type}, NewType, Config1)};
                       false ->
                           {abort, {error, bad_node}}
                   end
           end),

    case RV of
        {commit, _} ->
            ok;
        {abort, not_needed} ->
            ok;
        {abort, {error, Error}} ->
            Error;
        retry_needed ->
            erlang:error(exceeded_retries)
    end.

supported_services() ->
    supported_services_for_version(cluster_compat_mode:supported_compat_version()).

allowed_services(enterprise) ->
    supported_services();
allowed_services(community) ->
    supported_services() -- enterprise_only_services().

enterprise_only_services() ->
    [cbas, eventing].

-define(PREHISTORIC, [0, 0]).

services_by_version() ->
    [{?PREHISTORIC, [kv, n1ql, index, fts]},
     {?VERSION_55,  [cbas, eventing]}].

topology_aware_services_by_version() ->
    [{?PREHISTORIC, [fts, index]},
     {?VERSION_55,  [cbas, eventing]}].

filter_services_by_version(Version, ServicesTable) ->
    lists:flatmap(fun ({V, Services}) ->
                          case cluster_compat_mode:is_enabled_at(Version, V) of
                              true ->
                                  Services;
                              false ->
                                  []
                          end
                  end, ServicesTable).

supported_services_for_version(ClusterVersion) ->
    filter_services_by_version(ClusterVersion, services_by_version()).


cluster_supported_services() ->
    supported_services_for_version(cluster_compat_mode:get_compat_version()).

default_services() ->
    [kv].

topology_aware_services_for_version(Version) ->
    filter_services_by_version(Version, topology_aware_services_by_version()).

topology_aware_services() ->
    topology_aware_services_for_version(cluster_compat_mode:get_compat_version()).

set_service_map(kv, _Nodes) ->
    %% kv is special; it's dealt with using different set of functions
    ok;
set_service_map(Service, Nodes) ->
    master_activity_events:note_set_service_map(Service, Nodes),
    ns_config:set({service_map, Service}, Nodes).

get_service_map(Config, kv) ->
    %% kv is special; just return active kv nodes
    ActiveNodes = active_nodes(Config),
    service_nodes(Config, ActiveNodes, kv);
get_service_map(Config, Service) ->
    ns_config:search(Config, {service_map, Service}, []).

failover_service_nodes(Config, Service, Nodes) ->
    Map = get_service_map(Config, Service),
    NewMap = Map -- Nodes,
    ok = ns_config:set([{{service_map, Service}, NewMap},
                        {{service_failover_pending, Service}, true}]).

service_has_pending_failover(Config, Service) ->
    ns_config:search(Config, {service_failover_pending, Service}, false).

service_clear_pending_failover(Service) ->
    ns_config:set({service_failover_pending, Service}, false).

node_active_services(Node) ->
    node_active_services(ns_config:latest(), Node).

node_active_services(Config, Node) ->
    AllServices = node_services(Config, Node),
    [S || S <- AllServices,
          lists:member(Node, service_active_nodes(Config, S))].

node_services(Node) ->
    node_services(ns_config:latest(), Node).

node_services(Config, Node) ->
    case ns_config:search(Config, {node, Node, services}) of
        false ->
            default_services();
        {value, Value} ->
            Value
    end.

should_run_service(Service, Node) ->
    should_run_service(ns_config:latest(), Service, Node).

should_run_service(Config, Service, Node) ->
    case ns_config_auth:is_system_provisioned()
        andalso get_cluster_membership(Node, Config) =:= active  of
        false -> false;
        true ->
            Svcs = node_services(Config, Node),
            lists:member(Service, Svcs)
    end.

service_active_nodes(Service) ->
    service_active_nodes(ns_config:latest(), Service).

service_active_nodes(Config, Service) ->
    get_service_map(Config, Service).

service_actual_nodes(Config, Service) ->
    ActualNodes = ordsets:from_list(actual_active_nodes(Config)),
    ServiceActiveNodes = ordsets:from_list(service_active_nodes(Config, Service)),
    ordsets:intersection(ActualNodes, ServiceActiveNodes).

service_nodes(Nodes, Service) ->
    service_nodes(ns_config:latest(), Nodes, Service).

service_nodes(Config, Nodes, Service) ->
    [N || N <- Nodes,
          ServiceC <- node_services(Config, N),
          ServiceC =:= Service].

user_friendly_service_name(kv) ->
    "data";
user_friendly_service_name(n1ql) ->
    "query";
user_friendly_service_name(fts) ->
    "full text search";
user_friendly_service_name(cbas) ->
    "analytics";
user_friendly_service_name(Service) ->
    atom_to_list(Service).

attach_node_uuids(Nodes, Config) ->
    UUIDDict = ns_config:get_node_uuid_map(Config),
    lists:map(
      fun (Node) ->
              case dict:find(Node, UUIDDict) of
                  {ok, UUID} ->
                      {Node, UUID};
                  error ->
                      {Node, undefined}
              end
      end, Nodes).

-ifdef(TEST).
supported_services_for_version_test() ->
    ?assertEqual(lists:sort([fts,kv,index,n1ql]),
                 lists:sort(supported_services_for_version(?VERSION_50))),
    ?assertEqual(lists:sort([fts,kv,index,n1ql,cbas,eventing]),
                 lists:sort(supported_services_for_version(?VERSION_55))).

topology_aware_services_for_version_test() ->
    ?assertEqual(lists:sort([fts,index]),
                 lists:sort(topology_aware_services_for_version(?VERSION_50))),
    ?assertEqual(lists:sort([fts,index,cbas,eventing]),
                 lists:sort(topology_aware_services_for_version(?VERSION_55))).
-endif.
