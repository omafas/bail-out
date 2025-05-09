%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_cluster_membership).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").
-include("ns_config.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_nodes_with_status/1,
         get_nodes_with_status/2,
         get_nodes_with_status/3,
         nodes_wanted/0,
         nodes_wanted/1,
         server_groups/0,
         server_groups/1,
         active_nodes/0,
         active_nodes/1,
         active_nodes/2,
         inactive_added_nodes/0,
         inactive_added_nodes/1,
         inactive_failed_nodes/1,
         actual_active_nodes/0,
         actual_active_nodes/1,
         get_cluster_membership/1,
         get_cluster_membership/2,
         rack_aware/1,
         get_node_server_group/2,
         get_nodes_server_groups/1,
         get_nodes_server_groups/2,
         activate/2,
         update_membership_sets/2,
         deactivate/2,
         re_failover/1,
         system_joinable/0,
         is_balanced/0,
         get_recovery_type/2,
         update_recovery_type/2,
         clear_recovery_type_sets/1,
         add_node/4,
         remove_nodes/2,
         prepare_to_join/2,
         is_newly_added_node/1,
         get_node_uuids/2,
         attach_node_uuids/2,
         fetch_snapshot/1,
         get_snapshot/1,
         get_snapshot/0,
         node_membership_keys/1
        ]).

-export([supported_services/0,
         supported_services/1,
         supported_services_for_version/2,
         cluster_supported_services/0,
         hosted_services/1,
         topology_aware_services/0,
         topology_aware_services_for_version/1,
         default_services/0,
         set_service_map/2,
         update_service_map/2,
         get_service_map/2,
         failover_service_nodes_commits/2,
         add_service_nodes/2,
         set_map_and_topology/3,
         service_has_pending_failover/2,
         service_clear_pending_failover/1,
         node_active_services/1,
         node_active_services/2,
         node_services/1,
         node_services/2,
         nodes_services/2,
         service_active_nodes/1,
         service_active_nodes/2,
         service_actual_nodes/2,
         service_nodes/2,
         service_nodes/3,
         should_run_service/2,
         should_run_service/3,
         user_friendly_service_name/1,
         json_service_name/1,
         get_max_replicas/2,
         pick_service_node/2,
         pick_service_node/3]).

fetch_snapshot(Txn) ->
    Snapshot =
        chronicle_compat:txn_get_many(
          lists:flatten(
            [nodes_wanted, server_groups,
             [chronicle_compat:service_keys(S) ||
                 S <- supported_services()]]), Txn),
    maps:merge(
      Snapshot,
      chronicle_compat:txn_get_many(
        lists:flatten([node_membership_keys(N) || N <- nodes_wanted(Snapshot)]),
        Txn)).

node_membership_keys(Node) ->
    [{node, Node, membership},
     {node, Node, services},
     {node, Node, recovery_type}].

get_snapshot() ->
    get_snapshot(#{}).

get_snapshot(Opts) ->
    chronicle_compat:get_snapshot([fetch_snapshot(_)], Opts).

get_nodes_with_status(PredOrStatus) ->
    get_nodes_with_status(direct, PredOrStatus).

get_nodes_with_status(Snapshot, PredOrStatus) ->
    get_nodes_with_status(Snapshot, nodes_wanted(Snapshot), PredOrStatus).

get_nodes_with_status(Snapshot, Nodes, any) ->
    get_nodes_with_status(Snapshot, Nodes, fun (_) -> true end);
get_nodes_with_status(Snapshot, Nodes, Status)
  when is_atom(Status) ->
    get_nodes_with_status(Snapshot, Nodes, _ =:= Status);
get_nodes_with_status(Snapshot, Nodes, Pred)
  when is_function(Pred, 1) ->
    [Node || Node <- Nodes,
             Pred(get_cluster_membership(Node, Snapshot))].

nodes_wanted() ->
    nodes_wanted(direct).

nodes_wanted(Snapshot) ->
    lists:usort(chronicle_compat:get(Snapshot, nodes_wanted, #{default => []})).

server_groups() ->
    server_groups(direct).

server_groups(Snapshot) ->
    chronicle_compat:get(Snapshot, server_groups, #{required => true}).

active_nodes() ->
    active_nodes(get_snapshot()).

active_nodes(Snapshot) ->
    get_nodes_with_status(Snapshot, active).

active_nodes(Snapshot, Nodes) ->
    get_nodes_with_status(Snapshot, Nodes, active).

inactive_added_nodes() ->
    inactive_added_nodes(get_snapshot()).

inactive_added_nodes(Snapshot) ->
    get_nodes_with_status(Snapshot, inactiveAdded).

inactive_failed_nodes(Snapshot) ->
    get_nodes_with_status(Snapshot, inactiveFailed).

actual_active_nodes() ->
    actual_active_nodes(get_snapshot()).

actual_active_nodes(Snapshot) ->
    get_nodes_with_status(Snapshot, ns_node_disco:nodes_actual(), active).

get_cluster_membership(Node) ->
    get_cluster_membership(Node, direct).

get_cluster_membership(Node, Snapshot) ->
    chronicle_compat:get(Snapshot, {node, Node, membership},
                         #{default => inactiveAdded}).

rack_aware(ServerGroups) ->
    case [G || G <- ServerGroups,
               proplists:get_value(nodes, G) =/= []] of
        [_] ->
            false;
        _ ->
            true
    end.

get_node_server_group(Node, Snapshot) ->
    get_node_server_group_inner(Node, server_groups(Snapshot)).

get_node_server_group_inner(_, []) ->
    undefined;
get_node_server_group_inner(Node, [SG | Rest]) ->
    case lists:member(Node, proplists:get_value(nodes, SG)) of
        true ->
            proplists:get_value(name, SG);
        false ->
            get_node_server_group_inner(Node, Rest)
    end.

get_nodes_server_groups(Nodes) ->
    get_nodes_server_groups(Nodes, server_groups()).

get_nodes_server_groups(Nodes, Groups) ->
    NodeSet = sets:from_list(Nodes),
    lists:filtermap(
      fun (Group) ->
              GroupNodes = proplists:get_value(nodes, Group),
              NewGroupNodes =
                  lists:filter(sets:is_element(_, NodeSet), GroupNodes),

              case NewGroupNodes of
                  [] ->
                      false;
                  _ ->
                      {true, lists:keystore(nodes, 1, Group,
                                            {nodes, NewGroupNodes})}
              end
      end, Groups).

get_max_replicas(NumKvNodes, KvServerGroups) ->
    lists:foldl(fun (Group, MaxReplicas) ->
                    NumGroupNodes = length(proplists:get_value(nodes, Group)),
                    min(MaxReplicas, NumKvNodes - NumGroupNodes)
                end, NumKvNodes - 1, KvServerGroups).

system_joinable() ->
    nodes_wanted() =:= [node()].

activate(Nodes, Transaction) ->
    ?log_debug("Activate nodes ~p", [Nodes]),
    update_membership(Nodes, active, Transaction).

deactivate(Nodes, Transaction) ->
    ?log_debug("Deactivate nodes ~p", [Nodes]),
    update_membership(Nodes, inactiveFailed, Transaction).

update_membership_sets(Nodes, Membership) ->
    [{{node, Node, membership}, Membership} || Node <- Nodes].

update_membership(Nodes, Type, Transaction) ->
    Sets = [{set, K, V} || {K, V} <- update_membership_sets(Nodes, Type)],
    Transaction(fun (_) -> {commit, Sets} end).

is_newly_added_node(Node) ->
    get_cluster_membership(Node) =:= inactiveAdded andalso
        get_recovery_type(direct, Node) =:= none.

is_balanced() ->
    not ns_orchestrator:needs_rebalance().

get_failover_node(NodeString) ->
    true = is_list(NodeString),
    case (catch list_to_existing_atom(NodeString)) of
        Node when is_atom(Node) ->
            Node;
        _ ->
            undefined
    end.

%% moves node from pending-recovery state to failed over state
%% used when users hits Cancel for pending-recovery node on UI
re_failover(NodeString) ->
    case get_failover_node(NodeString) of
        undefined ->
            not_possible;
        Node ->
            ?log_debug("Move node ~p from pending-recovery state to failed "
                       "over state", [Node]),
            chronicle_compat:transaction(
              [nodes_wanted,
               {node, Node, membership},
               {node, Node, recovery_type}],
              fun (Snapshot) ->
                      case lists:member(Node, nodes_wanted(Snapshot)) andalso
                          get_recovery_type(Snapshot, Node) =/= none andalso
                          get_cluster_membership(Node,
                                                 Snapshot) =:= inactiveAdded of
                          true ->
                              {commit,
                               [{set, {node, Node, membership}, inactiveFailed},
                                {set, {node, Node, recovery_type}, none}]};
                          false ->
                              {abort, not_possible}
                      end
              end)
    end.

get_recovery_type(Snapshot, Node) ->
    chronicle_compat:get(Snapshot, {node, Node, recovery_type},
                         #{default => none}).

-spec update_recovery_type(node(), delta | full) -> {ok, term()} | bad_node.
update_recovery_type(Node, NewType) ->
    ?log_debug("Update recovery type of ~p to ~p", [Node, NewType]),
    chronicle_compat:transaction(
      [{node, Node, membership},
       {node, Node, recovery_type}],
      fun (Snapshot) ->
              Membership = get_cluster_membership(Node, Snapshot),
              case (Membership =:= inactiveAdded
                    andalso get_recovery_type(Snapshot, Node) =/= none)
                  orelse Membership =:= inactiveFailed of
                  true ->
                      {commit, [{set, {node, Node, membership}, inactiveAdded},
                                {set, {node, Node, recovery_type}, NewType}]};
                  false ->
                      {abort, bad_node}
              end
      end).

clear_recovery_type_sets(Nodes) ->
    [{{node, N, recovery_type}, none} || N <- Nodes].

add_node(Node, GroupUUID, Services, Transaction) ->
    ?log_debug("Add node ~p, GroupUUID = ~p, Services = ~p",
               [Node, GroupUUID, Services]),
    Transaction(
      fun (Txn) ->
              Snapshot = chronicle_compat:txn_get_many(
                           [nodes_wanted, server_groups], Txn),
              NodesWanted = nodes_wanted(Snapshot),
              case lists:member(Node, NodesWanted) of
                  true ->
                      {abort, node_present};
                  false ->
                      Groups = server_groups(Snapshot),
                      case add_node_to_groups(Groups, GroupUUID, Node) of
                          {error, Error} ->
                              {abort, Error};
                          NewGroups ->
                              {commit,
                               [{set, nodes_wanted,
                                 lists:usort([Node | NodesWanted])},
                                {set, {node, Node, membership}, inactiveAdded},
                                {set, {node, Node, services}, Services},
                                {set, server_groups, NewGroups} |
                                collections_sets(Node, Txn)]}
                      end
              end
      end).

collections_sets(Node, Txn) ->
    {ok, {Buckets, _}} = chronicle_compat:txn_get(ns_bucket:root(), Txn),
    Snapshot = chronicle_compat:txn_get_many(
                 [collections:key(B) || B <- Buckets], Txn),
    lists:filtermap(
      fun (Bucket) ->
              case collections:get_manifest(Bucket, Snapshot) of
                  undefined ->
                      false;
                  Manifest ->
                      {true, collections:last_seen_ids_set(
                               Node, Bucket, Manifest)}
              end
      end, Buckets).

add_node_to_groups(Groups, GroupUUID, Node) ->
    MaybeGroup0 = [G || G <- Groups,
                        proplists:get_value(uuid, G) =:= GroupUUID],
    MaybeGroup = case MaybeGroup0 of
                     [] ->
                         case GroupUUID of
                             undefined ->
                                 [hd(Groups)];
                             _ ->
                                 []
                         end;
                     _ ->
                         true = (undefined =/= GroupUUID),
                         MaybeGroup0
                 end,
    case MaybeGroup of
        [] ->
            {error, group_not_found};
        [TheGroup] ->
            GroupNodes = proplists:get_value(nodes, TheGroup),
            true = (is_list(GroupNodes)),
            NewGroupNodes = lists:usort([Node | GroupNodes]),
            NewGroup =
                lists:keystore(nodes, 1, TheGroup, {nodes, NewGroupNodes}),
            lists:usort([NewGroup | (Groups -- MaybeGroup)])
    end.

validate_removal(Txn, ToRemoveNodes, BucketsConfig) ->
    ClusterSnapshot = fetch_snapshot(Txn),
    lists:all(
      fun(Node) ->
              case node_active_services(ClusterSnapshot, Node) of
                  [] ->
                      true;
                  [kv] ->
                      lists:all(
                        fun({_BName, BConfig}) ->
                                Servers = ns_bucket:get_servers(BConfig),
                                not lists:member(Node, Servers)
                        end, BucketsConfig);
                  _ ->
                      false
              end
      end, ToRemoveNodes).

%% Note: We do not delete the node keys for this node.
remove_nodes(RemoteNodes, Transaction) ->
    RV = Transaction(
           fun (Txn) ->
                   Snapshot =
                       chronicle_compat:txn_get_many(
                         [nodes_wanted, server_groups, ns_bucket:root()], Txn),

                   %% Remove the per-node override keys for RemoteNodes from
                   %% all the buckets.
                   BucketsSnapshot =
                       ns_bucket:fetch_snapshot(all, Txn, [props]),
                   BucketConfigs = ns_bucket:get_buckets(BucketsSnapshot),
                   UpdatedBucketConfigs =
                       ns_bucket:remove_override_props_many(
                         RemoteNodes, BucketConfigs),

                   Buckets = ns_bucket:get_bucket_names(Snapshot),
                   NodeKeys = lists:flatten(
                                [chronicle_compat:node_keys(RN, Buckets) ||
                                    RN <- RemoteNodes,
                                    RN =/= node()]),

                   %% Failing this check is guaranteed config corruption so
                   %% we abort such a transaction in the first place
                   case validate_removal(Txn, RemoteNodes,
                                         UpdatedBucketConfigs) of
                       true ->
                           {commit,
                            [{set, nodes_wanted,
                              nodes_wanted(Snapshot) -- RemoteNodes},
                             {set, server_groups,
                              remove_nodes_from_server_groups(
                                RemoteNodes, server_groups(Snapshot))} |
                             [{set, ns_bucket:sub_key(BN, props), UBC} ||
                                 {BN, UBC} <- UpdatedBucketConfigs]] ++
                                [{delete, K} || K <- NodeKeys]};
                       false ->
                           {abort, {error, fail_remove_node}}
                   end

           end),
    case RV of
        {ok, _} ->
            ok = ns_config:update(
                   fun ({{node, Node, _}, _}) ->
                           case lists:member(Node, RemoteNodes) andalso
                                Node =/= node() of
                               true ->
                                   delete;
                               false ->
                                   skip
                           end;
                       (_Other) ->
                           skip
                   end);
        {error, _} = Error ->
            Error;
        _ ->
            ok
    end,
    RV.

remove_nodes_from_server_groups(NodesToRemove, Groups) ->
    [lists:keystore(nodes, 1, G,
                    {nodes, proplists:get_value(nodes, G) -- NodesToRemove}) ||
        G <- Groups].

prepare_to_join(RemoteNode, Cookie) ->
    MyNode = node(),
    %% Generate new node UUID while joining a cluster.
    %% We want to prevent situations where multiple nodes in
    %% the same cluster end up having same node uuid because they
    %% were created from same virtual machine image.
    ns_config:regenerate_node_uuid(),
    tombstone_agent:wipe(),

    InitialKVs =
        [{otp, [{cookie, Cookie}]},
         {nodes_wanted, [MyNode, RemoteNode]},
         {cluster_compat_mode, undefined},
         {{node, MyNode, membership}, inactiveAdded}],

    %% For the keys that are being preserved and have vclocks,
    %% we will just update_vclock so that these keys get stamped
    %% with new node uuid vclock.
    ns_config:update(
      fun ({directory,_}) ->
              skip;
          ({{node, _, services}, _}) ->
              erase;
          ({{node, Node, Key}, Value}) when Node =:= MyNode ->
              %% Attach a fresh vector clock to the value. This will cause an
              %% intentional conflict with any non-deleted values from
              %% previous incarnations of this (or other of the same) node in
              %% the cluster we are joining. We create a fresh vector clock as
              %% opposed to incrementing the existing one, so we don't carry
              %% redundant history.
              NewValue = pre_76_node_key_clean_up(Key, Value),
              {set_fresh, {{node, Node, Key}, NewValue}};
          ({K, _V}) ->
              %% Don't erase values we are about to set_initial, just to be
              %% extra safe in terms of preserving how it behaved previously.
              case lists:keyfind(K, 1, InitialKVs) of
                  false ->
                      erase;
                  Pair ->
                      {set_initial, Pair}
              end
      end),

    %% Some of the keys from InitialKVs could have been entirely removed from
    %% the config while the node was part of 7.0 cluster due to tombstone
    %% purging. So those need to be set explicitly.
    lists:foreach(
      fun ({Key, Value}) ->
              ns_config:set_initial(Key, Value)
      end, InitialKVs).

%% This is needed for the case when 7.6 node is added to pre-7.6 cluster
pre_76_node_key_clean_up(memcached, List) ->
    Res = misc:key_update(
            admin_pass,
            List,
            fun ({v2, [Password | _]}) -> Password;
                (Password) when is_list(Password) -> Password
            end),
    case Res of
        false -> List;
        NewList when is_list(NewList) -> NewList
    end;
pre_76_node_key_clean_up(_Key, Value) ->
    Value.

supported_services() ->
    supported_services(cluster_compat_mode:is_enterprise()).

supported_services(IsEnterprise) ->
    supported_services_for_version(
      cluster_compat_mode:supported_compat_version(),
      IsEnterprise).

enterprise_only_services() ->
    [cbas, eventing, backup].

-define(PREHISTORIC, [0, 0]).

services_by_version() ->
    [{?PREHISTORIC, [kv, n1ql, index, fts, cbas, eventing, backup]}].

topology_aware_services_by_version() ->
    [{?PREHISTORIC, [fts, index, cbas, eventing, backup, n1ql]}].

filter_services_by_version(Version, ServicesTable) ->
    lists:flatmap(fun ({V, Services}) ->
                          case cluster_compat_mode:is_enabled_at(Version, V) of
                              true ->
                                  Services;
                              false ->
                                  []
                          end
                  end, ServicesTable).

supported_services_for_version(ClusterVersion, IsEnterprise) ->
    NotSupported =
        case IsEnterprise of
            true ->
                [];
            false ->
                enterprise_only_services()
        end,
    FixedServices = config_profile:get_value(fixedServices, undefined),
    case FixedServices of
        undefined ->
            filter_services_by_version(ClusterVersion,
                                       services_by_version()) -- NotSupported;
        _ ->
            FixedServices
    end.

cluster_supported_services() ->
    supported_services_for_version(cluster_compat_mode:get_compat_version(),
                                   cluster_compat_mode:is_enterprise()).

hosted_services(Snapshot) ->
    nodes_services(Snapshot, nodes_wanted(Snapshot)).

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
    case chronicle_kv:set(kv, {service_map, Service}, Nodes) of
        {ok, _} ->
            announce_set_service_map(Service, Nodes),
            ok;
        Error ->
            Error
    end.

update_service_map(Service, Nodes) ->
    RV = chronicle_kv:transaction(
           kv, [{service_map, Service}],
           fun (Snapshot) ->
                   Sets = update_service_map_sets(Service, Nodes, Snapshot),
                   {commit, Sets, Sets =/= []}
           end),
    case RV of
        {ok, _, true} ->
            announce_set_service_map(Service, Nodes),
            {ok, true};
        {ok, _, false} ->
            {ok, false};
        Other ->
            Other
    end.

update_service_map_sets(Service, Nodes, Snapshot) ->
    CurrentNodes = lists:sort(get_service_map(Snapshot, Service)),
    ServiceNodes = lists:sort(Nodes),

    case CurrentNodes =:= ServiceNodes of
        true ->
            [];
        false ->
            [{set, {service_map, Service}, ServiceNodes}]
    end.

announce_set_service_map(Service, Nodes) ->
    ?log_info("Service map for service ~p updated to ~p", [Service, Nodes]),
    master_activity_events:note_set_service_map(Service, Nodes).

-spec set_map_and_topology(atom(), [node()], [node()]) ->
          {ok, boolean()} | {error, term()}.
set_map_and_topology(Service, Nodes, KnownNodes) ->
    services_transaction(
      fun (Snapshot) ->
              Sets = update_service_map_sets(Service, Nodes, Snapshot) ++
                  add_service_nodes_sets(Service, Nodes, Snapshot) ++
                  delete_service_nodes_sets(Service, KnownNodes -- Nodes,
                                            Snapshot),
              {Sets, Sets =/= []}
      end, KnownNodes, [{service_map, Service}]).

get_service_map(Snapshot, kv) ->
    %% kv is special; just return active kv nodes
    ActiveNodes = active_nodes(Snapshot),
    service_nodes(Snapshot, ActiveNodes, kv);
get_service_map(Snapshot, Service) ->
    chronicle_compat:get(Snapshot, {service_map, Service}, #{default => []}).

failover_service_nodes_commits(Nodes, Snapshot) ->
    Services0 = lists:flatmap(node_services(Snapshot, _), Nodes),
    Services  = lists:usort(Services0) -- [kv],

    SvcMap = lists:flatmap(
               fun(Service) ->
                       Map = get_service_map(Snapshot, Service),
                       [{set, {service_map, Service}, Map -- Nodes},
                        {set, {service_failover_pending, Service}, true}]
               end, Services),

    case Services of
        [] ->
            {[], []};
        _ ->
            {Services, SvcMap}
    end.

service_has_pending_failover(Snapshot, Service) ->
    chronicle_compat:get(Snapshot, {service_failover_pending, Service},
                         #{default => false}).

service_clear_pending_failover(Service) ->
    ?log_debug("Clear pending failover for service ~p", [Service]),
    chronicle_kv:set(kv, {service_failover_pending, Service}, false).

node_active_services(Node) ->
    node_active_services(direct, Node).

node_active_services(Snapshot, Node) ->
    AllServices = node_services(Snapshot, Node),
    [S || S <- AllServices,
          lists:member(Node, service_active_nodes(Snapshot, S))].

node_services(Node) ->
    node_services(direct, Node).

node_services(Snapshot, Node) ->
    chronicle_compat:get(Snapshot, {node, Node, services},
                         #{default => default_services()}).

nodes_services(Snapshot, Nodes) ->
    lists:usort(lists:flatmap(node_services(Snapshot, _), Nodes)).

should_run_service(Service, Node) ->
    should_run_service(direct, Service, Node).

should_run_service(Snapshot, Service, Node) ->
    case ns_config_auth:is_system_provisioned()
        andalso get_cluster_membership(Node, Snapshot) =:= active  of
        false -> false;
        true ->
            Svcs = node_services(Snapshot, Node),
            lists:member(Service, Svcs)
    end.

service_active_nodes(Service) ->
    service_active_nodes(direct, Service).

service_active_nodes(Snapshot, Service) ->
    get_service_map(Snapshot, Service).

service_actual_nodes(Snapshot, Service) ->
    ActualNodes = ordsets:from_list(actual_active_nodes(Snapshot)),
    ServiceActiveNodes =
        ordsets:from_list(service_active_nodes(Snapshot, Service)),
    ordsets:intersection(ActualNodes, ServiceActiveNodes).

service_nodes(Nodes, Service) ->
    service_nodes(direct, Nodes, Service).

service_nodes(Snapshot, Nodes, Service) ->
    [N || N <- Nodes,
          ServiceC <- node_services(Snapshot, N),
          ServiceC =:= Service].

pack_commit(Sets) when is_list(Sets) ->
    {commit, Sets};
pack_commit({Sets, Info}) when is_list(Sets) ->
    {commit, Sets, Info}.

services_transaction(Fun, Nodes) ->
    services_transaction(Fun, Nodes, []).

services_transaction(_Fun, [], _) ->
    ok;
services_transaction(Fun, Nodes, ExtraKeys) ->
    RV = chronicle_kv:transaction(
           kv, [{node, N, services} || N <- Nodes] ++ ExtraKeys,
           ?cut(pack_commit(Fun(_)))),
    case RV of
        {ok, _} ->
            ok;
        {ok, _, Info} ->
            {ok, Info};
        Other ->
            Other
    end.

-spec add_service_nodes(atom(), [node()]) -> ok | {error, term()}.
add_service_nodes(Service, NewNodes) ->
    services_transaction(add_service_nodes_sets(Service, NewNodes, _),
                         NewNodes).

add_service_nodes_sets(Service, NewNodes, Snapshot) ->
    lists:filtermap(
      fun (N) ->
              Key = {node, N, services},
              {Services, _R} = maps:get(Key, Snapshot),
              case lists:member(Service, Services) of
                  true ->
                      false;
                  false ->
                      {true, {set, Key, [Service | Services]}}
              end
      end, NewNodes).

delete_service_nodes_sets(Service, ToDelete, Snapshot) ->
    lists:filtermap(
      fun (N) ->
              Key = {node, N, services},
              {Services, _R} = maps:get(Key, Snapshot),
              case Services -- [Service] of
                  Services ->
                      false;
                  NewServices ->
                      {true, {set, Key, NewServices}}
              end
      end, ToDelete).

-ifdef(TEST).
add_service_nodes_sets_test() ->
    ?assertEqual([{set, {node, b, services}, [index, kv, n1ql]}],
                 lists:sort(
                   add_service_nodes_sets(
                     index, [a, b],
                     #{{node, a, services} => {[index, kv], rev},
                       {node, b, services} => {[kv, n1ql], rev}}))).

delete_service_nodes_sets_test() ->
    ?assertEqual([{set, {node, a, services}, [kv]}],
                 delete_service_nodes_sets(
                   index, [a, b],
                   #{{node, a, services} => {[index, kv], rev},
                     {node, b, services} => {[kv, n1ql], rev},
                     {node, d, services} => {[index, kv], rev}})).
-endif.

pick_service_node(Snapshot, Service) ->
    pick_service_node(Snapshot, Service, []).

pick_service_node(Snapshot, Service, DownNodes) ->
    ActiveNodes = service_active_nodes(Snapshot, Service),

    case ActiveNodes -- DownNodes of
        [] ->
            undefined;
        [FirstNode | _] = ServiceAliveNodes ->
            case lists:member(node(), ServiceAliveNodes) of
                true ->
                    node();
                false ->
                    FirstNode
            end
    end.

user_friendly_service_name(kv) ->
    "data";
user_friendly_service_name(n1ql) ->
    "query";
user_friendly_service_name(fts) ->
    "full text search";
user_friendly_service_name(cbas) ->
    "analytics";
user_friendly_service_name(backup) ->
    "backup";
user_friendly_service_name(Service) ->
    atom_to_list(Service).

json_service_name(kv) -> data;
json_service_name(fts) -> fullTextSearch;
json_service_name(n1ql) -> query;
json_service_name(cbas) -> analytics;
json_service_name(ns_server) -> clusterManager;
json_service_name(xdcr) -> xdcr;
json_service_name(Service) -> Service.

get_node_uuids(Nodes, UUIDDict) ->
    lists:map(
      fun (Node) ->
              case dict:find(Node, UUIDDict) of
                  {ok, UUID} ->
                      UUID;
                  error ->
                      undefined
              end
      end, Nodes).

attach_node_uuids(Nodes, UUIDDict) ->
    lists:zip(Nodes, get_node_uuids(Nodes, UUIDDict)).
