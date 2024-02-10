%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(service_janitor).

-include("cut.hrl").
-include("ns_common.hrl").

-export([cleanup/0, complete_service_failover/1]).

-define(INITIAL_REBALANCE_TIMEOUT, ?get_timeout(initial_rebalance, 120000)).

cleanup() ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            do_cleanup();
        false ->
            ok
    end.

do_cleanup() ->
    case maybe_init_services() of
        ok ->
            %% config might have been changed by maybe_init_services, so
            %% re-pull the snapshot
            Snapshot = get_snapshot_from_quorum(),
            Services = ns_cluster_membership:cluster_supported_services(),
            RVs =
                [maybe_complete_pending_failover(Snapshot, S) || S <- Services],
            handle_results(RVs);
        Error ->
            Error
    end.

get_snapshot_from_quorum() ->
    %% pull quorum view of chronicle to make sure that we do not access
    %% locally cached version of service_failover_pending
    chronicle_compat:pull(),
    ns_cluster_membership:get_snapshot().

maybe_init_services() ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    ActiveNodes = ns_cluster_membership:active_nodes(Snapshot),
    case ActiveNodes of
        [Node] when Node =:= node() ->
            Services = ns_cluster_membership:node_services(Snapshot, Node),
            RVs = [maybe_init_service(Snapshot, S) || S <- Services],
            handle_results(RVs);
        _ ->
            ok
    end.

maybe_init_service(_Snapshot, kv) ->
    ok;
maybe_init_service(Snapshot, Service) ->
    case ns_cluster_membership:get_service_map(Snapshot, Service) of
        [] ->
            case lists:member(
                   Service,
                   ns_cluster_membership:topology_aware_services()) of
                true ->
                    init_topology_aware_service(Service);
                false ->
                    init_simple_service(Service)
            end;
        _ ->
            ok
    end.

init_simple_service(Service) ->
    ok = ns_cluster_membership:set_service_map(Service, [node()]),
    ?log_debug("Created initial service map for service `~p'", [Service]),
    ok.

init_topology_aware_service(Service) ->
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
    end.

orchestrate_initial_rebalance(Service) ->
    ProgressCallback =
        fun (Progress) ->
                ?log_debug("Initial rebalance progress for `~p': ~p",
                           [Service, dict:to_list(Progress)])
        end,

    KeepNodes = [node()],
    EjectNodes = [],
    DeltaNodes = [],

    Result =
        try
            service_manager:rebalance(
              Service, KeepNodes, EjectNodes, DeltaNodes, ProgressCallback,
              #{timeout => ?INITIAL_REBALANCE_TIMEOUT})
        catch
            exit:{service_rebalance_failed, _, _} = E ->
                {error, E}
        end,

    case Result of
        ok ->
            ok = ns_cluster_membership:set_service_map(Service, KeepNodes);
        {error, {service_rebalance_failed, Service, Reason}} ->
            {error, {initial_rebalance_failed, Service, Reason}};
        {error, {service_rebalance_timeout, Service}} ->
            ?log_error("Initial rebalance of service `~p` takes too long "
                       "(timeout ~p)", [Service, ?INITIAL_REBALANCE_TIMEOUT]),
            {error, {initial_rebalance_timeout, Service}}
    end.

maybe_complete_pending_failover(Snapshot, Service) ->
    {ok, RV} =
        leader_activities:run_activity(
          {service_janitor, Service, maybe_complete_pending_failover},
          majority,
          fun () ->
                  {ok, maybe_complete_pending_failover_body(Snapshot, Service)}
          end,
          [quiet]),

    RV.

maybe_complete_pending_failover_body(Snapshot, Service) ->
    case ns_cluster_membership:service_has_pending_failover(Snapshot,
                                                            Service) of
        true ->
            ?log_debug("Found unfinished failover for service ~p", [Service]),
            RV = complete_service_failover(Snapshot, Service),
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

complete_service_failover(Service) ->
    complete_service_failover(get_snapshot_from_quorum(), Service).

complete_service_failover(Snapshot, Service) ->
    true = ns_cluster_membership:service_has_pending_failover(
             Snapshot, Service),

    TopologyAwareServices = ns_cluster_membership:topology_aware_services(),
    RV = case lists:member(Service, TopologyAwareServices) of
             true ->
                 complete_topology_aware_service_failover(Snapshot, Service);
             false ->
                 ok
         end,

    case RV of
        ok ->
            {ok, _} =
                ns_cluster_membership:service_clear_pending_failover(Service);
        _ ->
            ok
    end,

    RV.

complete_topology_aware_service_failover(Snapshot, Service) ->
    NodesLeft = ns_cluster_membership:get_service_map(Snapshot, Service),
    case NodesLeft of
        [] ->
            ok;
        _ ->
            orchestrate_service_failover(Service, NodesLeft)
    end.

orchestrate_service_failover(Service, Nodes) ->
    try
        service_manager:failover(
          Service, Nodes, #{})
    catch
        exit:{service_failover_failed, Service, Reason} ->
            {error, {failover_failed, Service, Reason}}
    end.

handle_results(RVs) ->
    NotOKs = [R || R <- RVs, R =/= ok],
    case NotOKs of
        [] ->
            ok;
        _ ->
            failed
    end.
