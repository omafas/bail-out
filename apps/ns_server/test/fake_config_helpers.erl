%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% Helpers for setting up fake config (chronicle_kv/ns_config)for tests.
-module(fake_config_helpers).

-export([setup_node_config/1,
         setup_bucket_config/1]).

-spec add_service_map_to_snapshot(atom(), list(), map()) -> map().
add_service_map_to_snapshot(Node, Services, Snapshot) ->
    lists:foldl(
        fun(kv, AccSnapshot) ->
                %% KV is handled in a special way
                AccSnapshot;
            (Service, S) ->
                case maps:find({service_map, Service}, S) of
                    error -> S#{{service_map, Service} => [Node]};
                    {ok, Nodes} -> S#{{service_map, Service} => [Node | Nodes]}
                end
        end, Snapshot, Services).

%% Map should be of the form Key => {State, Services (list)}.
-spec setup_node_config(map()) -> true.
setup_node_config(NodesMap) ->
    ClusterSnapshot =
        maps:fold(
            fun(Node, {State, Services}, Snapshot) ->
                    S = add_service_map_to_snapshot(Node, Services, Snapshot),
                    S#{{node, Node, membership} => State,
                        {node, Node, services} => Services,
                        {node, Node, failover_vbuckets} => []}
            end, #{}, NodesMap),
    fake_chronicle_kv:update_snapshot(ClusterSnapshot),

    Nodes = maps:keys(NodesMap),
    fake_chronicle_kv:update_snapshot(nodes_wanted, Nodes).

%% Takes a list of bucket names (strings).
%% Requires that node config is setup (i.e. we must be able to read from the
%% config which nodes have the data service).
-spec setup_bucket_config(list()) -> true.
setup_bucket_config(Buckets) ->
    fake_chronicle_kv:update_snapshot(bucket_names, Buckets),

    AllNodes = ns_cluster_membership:nodes_wanted(),
    AllKVNodes = ns_cluster_membership:service_nodes(AllNodes, kv),
    ActiveKVNodes = ns_cluster_membership:service_active_nodes(kv),

    %% Using a simple generated map with 4 vBuckets and 1 replica (2 copies).
    Map0 = mb_map:random_map(4, 2, AllKVNodes),
    Map1 = mb_map:generate_map(Map0, 1, AllKVNodes, []),
    Map = mb_map:promote_replicas(Map1, AllKVNodes -- ActiveKVNodes),

    Val = [
           {type, membase},
           {servers, ActiveKVNodes},
           {map, Map}
          ],

    fake_chronicle_kv:update_snapshot(
      maps:from_list([{{bucket, B, props}, Val} || B <- Buckets])).
