%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
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
-module(service_janitor).

-include("cut.hrl").
-include("ns_common.hrl").

-export([cleanup/0, cleanup/1, complete_service_failover/3]).

-define(INITIAL_REBALANCE_TIMEOUT, ?get_timeout(initial_rebalance, 120000)).
-define(CLEAR_FAILOVER_CONFIG_SYNC_TIMEOUT,
        ?get_timeout(clear_failover_sync, 2000)).

cleanup() ->
    Config = ns_config:get(),
    case ns_config_auth:is_system_provisioned(Config) of
        true ->
            cleanup(Config);
        false ->
            ok
    end.

cleanup(Config) ->
    case maybe_init_services(Config) of
        ok ->
            %% config might have been changed by maybe_init_services
            NewConfig = ns_config:get(),
            maybe_complete_pending_failovers(NewConfig);
        Error ->
            Error
    end.

maybe_init_services(Config) ->
    ActiveNodes = ns_cluster_membership:active_nodes(Config),
    case ActiveNodes of
        [Node] when Node =:= node() ->
            Services = ns_cluster_membership:node_services(Config, Node),
            RVs = [maybe_init_service(Config, S) || S <- Services],
            handle_results(RVs);
        _ ->
            ok
    end.

maybe_init_service(_Config, kv) ->
    ok;
maybe_init_service(Config, Service) ->
    case lists:member(Service, ns_cluster_membership:topology_aware_services()) of
        true ->
            maybe_init_topology_aware_service(Config, Service);
        false ->
            maybe_init_simple_service(Config, Service)
    end.

maybe_init_simple_service(Config, Service) ->
    case ns_cluster_membership:get_service_map(Config, Service) of
        [] ->
            ns_cluster_membership:set_service_map(Service, [node()]),
            ?log_debug("Created initial service map for service `~p'", [Service]),
            ok;
        _ ->
            ok
    end.

maybe_init_topology_aware_service(Config, Service) ->
    case ns_cluster_membership:get_service_map(Config, Service) of
        [] ->
            ?log_debug("Doing initial topology change for service `~p'", [Service]),
            case orchestrate_initial_rebalance(Service) of
                ok ->
                    ?log_debug("Initial rebalance for `~p` finished successfully",
                               [Service]),
                    ok;
                Error ->
                    ?log_error("Initial rebalance for `~p` failed: ~p",
                               [Service, Error]),
                    Error
            end;
        _ ->
            ok
    end.

orchestrate_initial_rebalance(Service) ->
    misc:with_trap_exit(?cut(do_orchestrate_initial_rebalance(Service))).

do_orchestrate_initial_rebalance(Service) ->
    ProgressCallback =
        fun (Progress) ->
                ?log_debug("Initial rebalance progress for `~p': ~p",
                           [Service, dict:to_list(Progress)])
        end,

    KeepNodes = [node()],
    EjectNodes = [],
    DeltaNodes = [],

    {Pid, MRef} = service_rebalancer:spawn_monitor_rebalance(Service, KeepNodes,
                                                             EjectNodes,
                                                             DeltaNodes,
                                                             ProgressCallback),
    receive
        {'DOWN', MRef, _, Pid, Reason} ->
            case Reason of
                normal ->
                    ns_cluster_membership:set_service_map(Service, KeepNodes),
                    ok;
                _ ->
                    {error, {initial_rebalance_failed, Service, Reason}}
            end;
        {'EXIT', _, Reason} = Exit ->
            ?log_debug("Received exit message ~p. Terminating initial rebalance",
                       [Exit]),
            misc:terminate_and_wait(Pid, Reason),
            exit(Reason)
    after
        ?INITIAL_REBALANCE_TIMEOUT ->
            ?log_error("Initial rebalance of service `~p` takes too long (timeout ~p)",
                       [Service, ?INITIAL_REBALANCE_TIMEOUT]),
            misc:terminate_and_wait(Pid, shutdown),
            {error, {initial_rebalance_timeout, Service}}
    end.

maybe_complete_pending_failovers(Config) ->
    Services = ns_cluster_membership:cluster_supported_services(),
    RVs = [maybe_complete_pending_failover(Config, S) || S <- Services],
    handle_results(RVs).

maybe_complete_pending_failover(Config, Service) ->
    {ok, RV} =
        leader_activities:run_activity(
          {service_janitor, Service, maybe_complete_pending_failover},
          majority,
          fun () ->
                  {ok, maybe_complete_pending_failover_body(Config, Service)}
          end,
          [quiet]),

    RV.

maybe_complete_pending_failover_body(Config, Service) ->
    case ns_cluster_membership:service_has_pending_failover(Config, Service) of
        true ->
            ?log_debug("Found unfinished failover for service ~p", [Service]),
            RV = complete_service_failover(Config, Service),
            case RV of
                ok ->
                    ?log_debug("Completed failover for service ~p successfully",
                               [Service]);
                Error ->
                    ?log_debug("Failed to complete service ~p failover: ~p",
                               [Service, Error])
            end,
            RV;
        false ->
            ok
    end.

complete_service_failover(Config, Service) ->
    FailedNodes = ns_cluster_membership:get_nodes_with_status(Config,
                                                              inactiveFailed),
    complete_service_failover(Config, Service, FailedNodes).

complete_service_failover(Config, Service, FailedNodes) ->
    true = ns_cluster_membership:service_has_pending_failover(Config, Service),

    TopologyAwareServices = ns_cluster_membership:topology_aware_services(),
    RV = case lists:member(Service, TopologyAwareServices) of
             true ->
                 complete_topology_aware_service_failover(Config, Service);
             false ->
                 ok
         end,

    case RV of
        ok ->
            clear_pending_failover(Config, Service, FailedNodes);
        _ ->
            ok
    end,

    RV.

clear_pending_failover(Config, Service, FailedNodes) ->
    ns_cluster_membership:service_clear_pending_failover(Service),

    OtherNodes = ns_node_disco:nodes_wanted(Config) -- FailedNodes,
    LiveNodes  = leader_utils:live_nodes(Config, OtherNodes),

    ns_config_rep:ensure_config_seen_by_nodes(
      LiveNodes, ?CLEAR_FAILOVER_CONFIG_SYNC_TIMEOUT).

complete_topology_aware_service_failover(Config, Service) ->
    NodesLeft = ns_cluster_membership:get_service_map(Config, Service),
    case NodesLeft of
        [] ->
            ok;
        _ ->
            orchestrate_service_failover(Service, NodesLeft)
    end.

orchestrate_service_failover(Service, Nodes) ->
    misc:with_trap_exit(
      ?cut(do_orchestrate_service_failover(Service, Nodes))).

do_orchestrate_service_failover(Service, Nodes) ->
    {Pid, MRef} = service_rebalancer:spawn_monitor_failover(Service, Nodes),

    receive
        {'DOWN', MRef, _, Pid, Reason} ->
            case Reason of
                normal ->
                    ok;
                _ ->
                    {error, {failover_failed, Service, Reason}}
            end;
        {'EXIT', _, Reason} = Exit ->
            ?log_debug("Received exit message ~p. Terminating failover",
                       [Exit]),
            misc:terminate_and_wait(Pid, Reason),
            exit(Reason)
    end.

handle_results(RVs) ->
    NotOKs = [R || R <- RVs, R =/= ok],
    case NotOKs of
        [] ->
            ok;
        _ ->
            failed
    end.
