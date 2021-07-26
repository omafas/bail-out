%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2021 Couchbase, Inc.
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
%% @doc This service maintains public ETS table that's caching
%% json-inified bucket infos. See vbucket_map_mirror module for
%% explanation how this works.
-module(bucket_info_cache).
-include("ns_common.hrl").

-export([start_link/0,
         terse_bucket_info/1]).

-export([build_node_services/0]).

%% for diagnostics
-export([submit_full_reset/0]).

%% NOTE: we're doing global replace of this string. So it must not
%% need any JSON escaping and it must not otherwise occur in terse
%% bucket info
-define(LOCALHOST_MARKER_STRING, "$HOST").

start_link() ->
    work_queue:start_link(bucket_info_cache, fun cache_init/0).

cache_init() ->
    {ok, _} = gen_event:start_link({local, bucket_info_cache_invalidations}),
    ets:new(bucket_info_cache, [set, named_table]),
    ets:new(bucket_info_cache_buckets, [ordered_set, named_table]),
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events, fun cleaner_loop/2, Self),
    {value, [{configs, NewBuckets}]} = ns_config:search(buckets),
    submit_new_buckets(Self, NewBuckets),
    submit_full_reset().

cleaner_loop({buckets, [{configs, NewBuckets}]}, Parent) ->
    submit_new_buckets(Parent, NewBuckets),
    Parent;
cleaner_loop({{_, _, alternate_addresses}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{_, _, capi_port}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{_, _, ssl_capi_port}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{_, _, ssl_rest_port}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{_, _, rest}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({rest, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{node, _, memcached}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{node, _, membership}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({cluster_compat_version, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({developer_preview_enabled, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{node, _, services}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{service_map, _}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop(_, Cleaner) ->
    Cleaner.

submit_new_buckets(Pid, Buckets0) ->
    work_queue:submit_work(
      Pid,
      fun () ->
              Buckets = lists:sort(Buckets0),
              BucketNames = compute_buckets_to_invalidate(Buckets),
              [begin
                   ets:delete(bucket_info_cache, Name),
                   ets:delete(bucket_info_cache_buckets, Name)
               end || Name <- BucketNames],
              [gen_event:notify(bucket_info_cache_invalidations, Name) || Name <- BucketNames],
              ok
      end).

compute_buckets_to_invalidate(Buckets) ->
    CachedBuckets = ets:tab2list(bucket_info_cache_buckets),
    Inv = ordsets:subtract(CachedBuckets, Buckets),
    [BucketName || {BucketName, _} <- Inv].

submit_full_reset() ->
    work_queue:submit_work(
      bucket_info_cache,
      fun () ->
              ets:delete_all_objects(bucket_info_cache),
              ets:delete_all_objects(bucket_info_cache_buckets),
              gen_event:notify(bucket_info_cache_invalidations, '*')
      end).

maybe_build_ext_hostname(Node) ->
    H = misc:extract_node_address(Node),
    case misc:is_localhost(H) of
        true  -> [];
        false -> [{hostname, list_to_binary(H)}]
    end.

alternate_addresses_json(Node, Config, WantedPorts) ->
    menelaus_util:strip_json_struct(
      menelaus_web_node:alternate_addresses_json(Node, Config, WantedPorts)).

build_nodes_ext([] = _Nodes, _Config, NodesExtAcc) ->
    lists:reverse(NodesExtAcc);
build_nodes_ext([Node | RestNodes], Config, NodesExtAcc) ->
    Services = [rest | ns_cluster_membership:node_active_services(Config, Node)],
    NI1 = maybe_build_ext_hostname(Node),
    NI2 = case Node =:= node() of
              true ->
                  [{'thisNode', true} | NI1];
              _ ->
                  NI1
          end,
    WantedPorts = service_ports:services_port_keys(Services),

    NI3 = NI2 ++ alternate_addresses_json(Node, Config, WantedPorts),
    NodeInfo = {[{services, {service_ports:get_ports_for_services(
                               Node, Config, Services)}} | NI3]},
    build_nodes_ext(RestNodes, Config, [NodeInfo | NodesExtAcc]).

do_compute_bucket_info(Bucket, Config) ->
    case ns_bucket:get_bucket(Bucket, Config) of
        {ok, BucketConfig} ->
            compute_bucket_info_with_config(Bucket, Config, BucketConfig);
        not_present ->
            not_present
    end.

node_bucket_info(Node, Config, Bucket, BucketUUID, BucketConfig) ->
    HostName = menelaus_web_node:build_node_hostname(Config, Node,
                                                     ?LOCALHOST_MARKER_STRING),
    Ports = {[{direct, service_ports:get_port(memcached_port, Config, Node)}]},
    WantedPorts = [rest_port, memcached_port],

    Info0 = [{hostname, list_to_binary(HostName)},
             {ports, Ports}] ++
        alternate_addresses_json(Node, Config, WantedPorts),
    Info = case ns_bucket:bucket_type(BucketConfig) of
               membase ->
                   Url = capi_utils:capi_bucket_url_bin(
                           Node, Bucket, BucketUUID, ?LOCALHOST_MARKER_STRING),
                   [{couchApiBase, Url} | Info0];
               _ ->
                   Info0
           end,
    {Info}.

compute_bucket_info_with_config(Bucket, Config, BucketConfig) ->
    {_, Servers0} = lists:keyfind(servers, 1, BucketConfig),

    %% we do sorting to make nodes list match order of servers inside vBucketServerMap
    Servers = lists:sort(Servers0),
    BucketUUID = ns_bucket:bucket_uuid(BucketConfig),

    NIs = lists:map(fun (Node) ->
                            node_bucket_info(Node, Config, Bucket,
                                             BucketUUID, BucketConfig)
                    end, Servers),
    AllServers = Servers ++ ordsets:subtract(ns_cluster_membership:active_nodes(Config), Servers),
    NEIs = build_nodes_ext(AllServers, Config, []),

    {_, UUID} = lists:keyfind(uuid, 1, BucketConfig),

    BucketBin = list_to_binary(Bucket),

    Caps = menelaus_web_buckets:build_bucket_capabilities(BucketConfig) ++
             build_cluster_capabilities(Config),

    MaybeVBMapDDocs =
        case lists:keyfind(type, 1, BucketConfig) of
            {_, memcached} ->
                Caps;
            _ ->
                {struct, VBMap} = ns_bucket:json_map_with_full_config(?LOCALHOST_MARKER_STRING,
                                                                      BucketConfig, Config),
                VBMapInfo = [{vBucketServerMap, {VBMap}} | Caps],
                case ns_bucket:storage_mode(BucketConfig) of
                    couchstore ->
                        DDocsUri =
                            menelaus_util:bin_concat_path(["pools", "default",
                                                           "buckets", Bucket,
                                                           "ddocs"]),
                        [{ddocs, {[{uri, DDocsUri}]}}
                         | VBMapInfo];
                    _ ->
                        VBMapInfo
                end
        end,

    %% We're computing rev using config's global rev which allows us
    %% to track changes to node services and set of active nodes.
    Rev = ns_config:compute_global_rev(Config),

    BucketUuid = [{"bucket_uuid", UUID}],
    Uri = menelaus_util:bin_concat_path(["pools", "default", "buckets",
                                         Bucket], BucketUuid),
    StreamingUri = menelaus_util:bin_concat_path(["pools", "default",
                                                  "bucketsStreaming",
                                                  Bucket], BucketUuid),

    J = {[{rev, Rev},
          {name, BucketBin},
          {uri, Uri},
          {streamingUri, StreamingUri},
          {nodes, NIs},
          {nodesExt, NEIs},
          {nodeLocator, ns_bucket:node_locator(BucketConfig)},
          {uuid, UUID}
          | MaybeVBMapDDocs]},
    {ok, Rev, ejson:encode(J), BucketConfig}.

compute_bucket_info(Bucket) ->
    Config = ns_config:get(),
    try do_compute_bucket_info(Bucket, Config)
    catch T:E ->
            {T, E, erlang:get_stacktrace()}
    end.


call_compute_bucket_info(BucketName) ->
    work_queue:submit_sync_work(
      bucket_info_cache,
      fun () ->
              case ets:lookup(bucket_info_cache, BucketName) of
                  [] ->
                      case compute_bucket_info(BucketName) of
                          {ok, Rev, V, BucketConfig} ->
                              ets:insert(bucket_info_cache,
                                         {BucketName, Rev, V}),
                              ets:insert(bucket_info_cache_buckets,
                                         {BucketName, BucketConfig}),
                              {ok, Rev, V};
                          Other ->
                              %% note: we might consider caching
                              %% exceptions but they're supposedly
                              %% rare anyways
                              Other
                      end;
                  [{_, Rev, V}] ->
                      {ok, Rev, V}
              end
      end).

terse_bucket_info(BucketName) ->
    case ets:lookup(bucket_info_cache, BucketName) of
        [] ->
            call_compute_bucket_info(BucketName);
        [{_, Rev, V}] ->
            {ok, Rev, V}
    end.

build_node_services() ->
    case ets:lookup(bucket_info_cache, 'node_services') of
        [] ->
            case call_build_node_services() of
                {ok, Rev, V} -> {Rev, V};
                {T, E, Stack} ->
                    erlang:raise(T, E, Stack)
            end;
        [{_, Rev, V}] ->
            {Rev, V}
    end.

call_build_node_services() ->
    work_queue:submit_sync_work(
      bucket_info_cache,
      fun () ->
              case ets:lookup(bucket_info_cache, 'node_services') of
                  [] ->
                      try do_build_node_services() of
                          {Rev, V} ->
                              ets:insert(bucket_info_cache,
                                         {'node_services', Rev, V}),
                              {ok, Rev, V}
                      catch T:E ->
                              {T, E, erlang:get_stacktrace()}
                      end;
                  [{_, Rev, V}] ->
                      {ok, Rev, V}
              end
      end).

build_cluster_capabilities(Config) ->
    case cluster_compat_mode:get_cluster_capabilities(Config) of
        [] ->
            [];
        Caps ->
            [{clusterCapabilitiesVer, [1, 0]},
             {clusterCapabilities, {Caps}}]
    end.

do_build_node_services() ->
    Config = ns_config:get(),
    NEIs = build_nodes_ext(ns_cluster_membership:active_nodes(Config),
                           Config, []),
    Caps = build_cluster_capabilities(Config),
    Rev = ns_config:compute_global_rev(Config),
    J = {[{rev, Rev},
          {nodesExt, NEIs}] ++ Caps},
    {Rev, ejson:encode(J)}.
