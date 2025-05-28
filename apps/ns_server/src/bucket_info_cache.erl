%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc This service maintains public ETS table that's caching
%% json-inified bucket infos. See vbucket_map_mirror module for
%% explanation how this works.
-module(bucket_info_cache).
-include("ns_common.hrl").
-include("ns_bucket.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         terse_bucket_info/1,
         contains_empty_vbmap/1]).

-export([build_node_services/0,
         build_pools_uri/1,
         build_pools_uri/2,
         build_short_bucket_info/3,
         build_name_and_locator/2,
         build_vbucket_map/2,
         build_ddocs/2]).

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
    chronicle_compat_events:subscribe(fun is_interesting/1,
                                      fun handle_config_event/1),
    submit_new_buckets(),
    submit_full_reset().

handle_config_event(buckets) ->
    submit_new_buckets();
handle_config_event(counters) ->
    submit_full_reset();
handle_config_event(Key) ->
    case ns_bucket:sub_key_match(Key) of
        {true, Bucket, _} ->
            invalidate_bucket(Bucket);
        false ->
            submit_full_reset()
    end.

is_interesting(buckets) -> true;
is_interesting({_, _, alternate_addresses}) -> true;
is_interesting({_, _, capi_port}) -> true;
is_interesting({_, _, ssl_capi_port}) -> true;
is_interesting({_, _, ssl_rest_port}) -> true;
is_interesting({_, _, rest}) -> true;
is_interesting(rest) -> true;
is_interesting({node, _, memcached}) -> true;
is_interesting({node, _, membership}) -> true;
is_interesting(cluster_compat_version) -> true;
is_interesting(cluster_name) -> true;
is_interesting(developer_preview_enabled) -> true;
is_interesting({node, _, services}) -> true;
is_interesting(server_groups) -> true;
is_interesting({service_map, _}) -> true;
is_interesting(counters) -> true;
is_interesting(app_telemetry) -> true;
is_interesting(Key) ->
    case ns_bucket:sub_key_match(Key) of
        {true, _, _} ->
            true;
        false ->
            false
    end.

submit_new_buckets() ->
    work_queue:submit_work(
      ?MODULE,
      fun () ->
              Buckets = lists:sort(ns_bucket:get_buckets()),
              do_invalidate_buckets(compute_buckets_to_invalidate(Buckets))
      end).

invalidate_bucket(Bucket) ->
    work_queue:submit_work(?MODULE, ?cut(do_invalidate_buckets([Bucket]))).

do_invalidate_buckets(BucketNames) ->
    [begin
         ets:delete(bucket_info_cache, Name),
         ets:delete(bucket_info_cache_buckets, Name)
     end || Name <- BucketNames],
    [gen_event:notify(bucket_info_cache_invalidations, Name) ||
        Name <- BucketNames],
    ok.

compute_buckets_to_invalidate(Buckets) ->
    CachedBuckets = ets:tab2list(bucket_info_cache_buckets),
    Inv = ordsets:subtract(CachedBuckets, Buckets),
    [BucketName || {BucketName, _} <- Inv].

submit_full_reset() ->
    work_queue:submit_work(
      ?MODULE,
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

alternate_addresses_json(Node, Config, Snapshot, WantedPorts) ->
    menelaus_web_node:alternate_addresses_json(Node, Config, Snapshot,
                                               WantedPorts).

build_nodes_ext([] = _Nodes, _Config, _Snapshot, NodesExtAcc) ->
    lists:reverse(NodesExtAcc);
build_nodes_ext([Node | RestNodes], Config, Snapshot, NodesExtAcc) ->
    Services =
        [rest | ns_cluster_membership:node_active_services(Snapshot, Node)],
    NI1 = maybe_build_ext_hostname(Node),
    NI2 = case Node =:= node() of
              true ->
                  [{'thisNode', true} | NI1];
              _ ->
                  NI1
          end,
    WantedPorts = service_ports:services_port_keys(Services),

    NI3 = NI2 ++ alternate_addresses_json(Node, Config, Snapshot, WantedPorts),
    %% Build and deDup the ports list
    PortInfo = lists:usort(service_ports:get_ports_for_services(Node,
                                                                Config,
                                                                Services)),
    ServerGroup =
        ns_cluster_membership:get_node_server_group(Node, Snapshot),
    NI4 = NI3 ++ [{serverGroup, ServerGroup} ||
                     cluster_compat_mode:is_enterprise()],
    NI5 = [{services, {PortInfo}} | NI4],
    UUID = ns_config:search_node_with_default(Node, Config, uuid, undefined),
    NI6 = case UUID of
              undefined -> NI5;
              _ -> [{nodeUUID, UUID} | NI5]
          end,
    NodeInfo =
        case menelaus_web_app_telemetry:is_accepting_connections() of
            false ->
                {NI6};
            true ->
                {[{appTelemetryPath,
                   list_to_binary("/" ++ ?APP_TELEMETRY_PATH)}
                 | NI6]}
        end,
    build_nodes_ext(RestNodes, Config, Snapshot, [NodeInfo | NodesExtAcc]).

do_compute_bucket_info(Bucket, Config) ->
    {Snapshot, Rev} = chronicle_compat:get_snapshot_with_revision(
                        [ns_bucket:fetch_snapshot(Bucket, _),
                         ns_cluster_membership:fetch_snapshot(_),
                         chronicle_compat:txn_get_many([counters], _)],
                        #{ns_config => Config}),

    case ns_bucket:get_bucket(Bucket, Snapshot) of
        {ok, BucketConfig} ->
            compute_bucket_info_with_config(Bucket, Config, Snapshot,
                                            BucketConfig, Rev);
        not_present ->
            not_present
    end.

node_bucket_info(Node, Config, Snapshot, Bucket, BucketUUID, BucketConfig) ->
    HostName = menelaus_web_node:build_node_hostname(Config, Node,
                                                     ?LOCALHOST_MARKER_STRING),
    Ports = {[{direct, service_ports:get_port(memcached_port, Config, Node)}]},
    WantedPorts = [rest_port, memcached_port],

    Info0 = [{hostname, HostName}, {ports, Ports}] ++
        alternate_addresses_json(Node, Config, Snapshot, WantedPorts),
    Info = case ns_bucket:bucket_type(BucketConfig) of
               membase ->
                  ?COUCHDB_ENABLED(
                     [{couchApiBase,
                       capi_utils:capi_bucket_url_bin(Node, Bucket, BucketUUID,
                                                      ?LOCALHOST_MARKER_STRING)}],
                      [])
                   ++ Info0;
                       _ ->
                   Info0
           end,
    {Info}.

build_short_bucket_info(Id, BucketConfig, Snapshot) ->
    BucketUUID = ns_bucket:uuid(Id, Snapshot),
    [build_name_and_locator(Id, BucketConfig),
     {bucketType, ns_bucket:external_bucket_type(BucketConfig)},
     build_storage_backend(BucketConfig),
     {uuid, BucketUUID},
     {uri, build_pools_uri(["buckets", Id], BucketUUID)},
     {streamingUri, build_pools_uri(["bucketsStreaming", Id], BucketUUID)},
     build_num_vbuckets(BucketConfig),
     build_bucket_capabilities(BucketConfig),
     build_collections_manifest_id(Id, Snapshot)].

build_storage_backend(BucketConfig) ->
    Is76 = cluster_compat_mode:is_cluster_76(),
    case ns_bucket:storage_backend(BucketConfig) of
        undefined when Is76 -> [];
        SB -> {storageBackend, SB}
    end.

build_num_vbuckets(BucketConfig) ->
    case ns_bucket:bucket_type(BucketConfig) of
        memcached ->
            [];
        membase ->
            {numVBuckets, ns_bucket:get_num_vbuckets(BucketConfig)}
    end.

build_name_and_locator(Id, BucketConfig) ->
    [{name, list_to_binary(Id)},
     {nodeLocator, ns_bucket:node_locator(BucketConfig)}].

build_vbucket_map(LocalAddr, BucketConfig) ->
    case ns_bucket:bucket_type(BucketConfig) of
        memcached ->
            [];
        membase ->
            do_build_vbucket_map(LocalAddr, BucketConfig, ns_config:latest())
    end.

equal_len_chains([]) ->
    [];
equal_len_chains(Map) ->
    MaxChainLen = length(misc:min_by(?cut(length(_1) > length(_2)), Map)),
    [misc:align_list(Chain, MaxChainLen, undefined) || Chain <- Map].

do_build_vbucket_map(LocalAddr, BucketConfig, Config) ->
    NumReplicas = ns_bucket:num_replicas(BucketConfig),
    EMap = equal_len_chains(proplists:get_value(map, BucketConfig, [])),
    BucketNodes = ns_bucket:get_servers(BucketConfig),
    ENodes = lists:delete(undefined, lists:usort(lists:append([BucketNodes |
                                                                EMap]))),
    Servers = lists:map(
                fun (ENode) ->
                        Port = service_ports:get_port(memcached_port, Config,
                                                      ENode),
                        H = misc:extract_node_address(ENode),
                        Host = case misc:is_localhost(H) of
                                   true  -> LocalAddr;
                                   false -> H
                               end,
                        list_to_binary(misc:join_host_port(Host, Port))
                end, ENodes),
    {_, NodesToPositions0}
        = lists:foldl(fun (N, {Pos,Dict}) ->
                              {Pos+1, dict:store(N, Pos, Dict)}
                      end, {0, dict:new()}, ENodes),
    NodesToPositions = dict:store(undefined, -1, NodesToPositions0),
    Map = [[dict:fetch(N, NodesToPositions) || N <- Chain] || Chain <- EMap],
    FastForwardMapList =
        case proplists:get_value(fastForwardMap, BucketConfig) of
            undefined -> [];
            FFM ->
                [{vBucketMapForward,
                  [[dict:fetch(N, NodesToPositions) || N <- Chain]
                   || Chain <- FFM]}]
        end,
    {vBucketServerMap,
     {[{hashAlgorithm, <<"CRC">>},
       {numReplicas, NumReplicas},
       {serverList, Servers},
       {vBucketMap, Map} |
       FastForwardMapList]}}.

build_ddocs(Id, BucketConfig) ->
    [{ddocs, {[{uri, build_pools_uri(["buckets", Id, "ddocs"])}]}} ||
        ns_bucket:can_have_views(BucketConfig)].

build_collections_manifest_id(Id, Snapshot) ->
    case collections:uid(Id, Snapshot) of
        undefined ->
            [];
        Uid ->
            {collectionsManifestUid, Uid}
    end.

build_pools_uri(Tail) ->
    build_pools_uri(Tail, undefined).

build_pools_uri(Tail, UUID) ->
    menelaus_util:bin_concat_path(
      ["pools", "default"] ++ Tail,
      [{"bucket_uuid", UUID} || UUID =/= undefined]).

maybe_range_scan_capability(BucketConfig) ->
    case ns_bucket:is_persistent(BucketConfig) of
        true ->
            [{rangeScan, cluster_compat_mode:is_cluster_76()}];
        false ->
            []
    end.

build_bucket_capabilities(BucketConfig) ->
    Caps =
        case ns_bucket:bucket_type(BucketConfig) of
            membase ->
                Conditional =
                    [{collections, collections:enabled(BucketConfig)},
                     {durableWrite, true},
                     {tombstonedUserXAttrs, true},
                     {couchapi, ns_bucket:can_have_views(BucketConfig)},
                     {'subdoc.ReplaceBodyWithXattr', true},
                     {'subdoc.DocumentMacroSupport', true},
                     {'subdoc.ReviveDocument', true},
                     {'nonDedupedHistory',
                      cluster_compat_mode:is_cluster_72() and
                      cluster_compat_mode:is_enterprise() and
                      ns_bucket:is_magma(BucketConfig)},
                     {'dcp.IgnorePurgedTombstones',
                      cluster_compat_mode:is_cluster_72()},
                     {preserveExpiry,
                      cluster_compat_mode:is_cluster_76()},
                     {querySystemCollection,
                      cluster_compat_mode:is_cluster_76()},
                     {mobileSystemCollection,
                      cluster_compat_mode:is_cluster_76()},
                     {'subdoc.ReplicaRead',
                      cluster_compat_mode:is_cluster_76()},
                     {'subdoc.BinaryXattr',
                      cluster_compat_mode:is_cluster_morpheus()},
                     {'subdoc.AccessDeleted',
                      cluster_compat_mode:is_cluster_morpheus()}] ++
                     maybe_range_scan_capability(BucketConfig),

                [C || {C, true} <- Conditional] ++
                    [dcp, cbhello, touch, cccp, xdcrCheckpointing, nodesExt,
                     xattr];
            memcached ->
                [cbhello, nodesExt]
        end,

    [{bucketCapabilitiesVer, ''},
     {bucketCapabilities, Caps}].

%% Clients expect these revisions to grow monotonically.
%% This doesn't handle chronicle quorum failovers, but we may
%% deal with it later.
compute_global_rev(Config, {_, ChronicleRev}) ->
    ns_config:compute_global_rev(Config) + ChronicleRev;
compute_global_rev(Config, no_rev) ->
    ns_config:compute_global_rev(Config).

get_rev_epoch(Snapshot) ->
    ns_cluster:counter(Snapshot, quorum_failover_success, 0) + 1.

compute_bucket_info_with_config(Id, Config, Snapshot, BucketConfig,
                                ChronicleRev) ->
    %% we do sorting to make nodes list match order of servers inside
    %% vBucketServerMap
    Servers = lists:sort(ns_bucket:get_servers(BucketConfig)),
    BucketUUID = ns_bucket:uuid(Id, Snapshot),

    AllServers = Servers ++
        ordsets:subtract(ns_cluster_membership:active_nodes(Snapshot), Servers),

    %% We're computing rev using config's global rev which allows us
    %% to track changes to node services and set of active nodes.
    Rev = compute_global_rev(Config, ChronicleRev),
    RevEpoch = get_rev_epoch(Snapshot),
    Json =
        {lists:flatten(
           [{rev, Rev},
            {revEpoch, RevEpoch},
            build_short_bucket_info(Id, BucketConfig, Snapshot),
            build_ddocs(Id, BucketConfig),
            build_vbucket_map(?LOCALHOST_MARKER_STRING, BucketConfig),
            {nodes,
             [node_bucket_info(Node, Config, Snapshot,
                               Id, BucketUUID, BucketConfig)
              || Node <- Servers]},
            {nodesExt, build_nodes_ext(AllServers, Config, Snapshot, [])},
            menelaus_web_buckets:build_hibernation_state(BucketConfig),
            build_cluster_capabilities()])},
    {ok, Rev, RevEpoch, ejson:encode(Json), BucketConfig}.

%% Examines the bucket info returned by compute_bucket_info_with_config to
%% determine if it contains an empty vbmap.
contains_empty_vbmap(BucketInfo) ->
    {BucketInfo1} = ejson:decode(BucketInfo),
    case proplists:get_value(<<"vBucketServerMap">>, BucketInfo1) of
        {ServerMap} ->
            case proplists:get_value(<<"vBucketMap">>, ServerMap, []) of
                [] ->
                    true;
                _ ->
                    false
            end;
        _ ->
            true
    end.

compute_bucket_info(Bucket) ->
    try do_compute_bucket_info(Bucket, ns_config:get())
    catch T:E:S ->
            {T, E, S}
    end.


call_compute_bucket_info(BucketName) ->
    work_queue:submit_sync_work(
      bucket_info_cache,
      fun () ->
              case ets:lookup(bucket_info_cache, BucketName) of
                  [] ->
                      case compute_bucket_info(BucketName) of
                          {ok, Rev, RevEpoch, V, BucketConfig} ->
                              ets:insert(bucket_info_cache,
                                         {BucketName, Rev, RevEpoch, V}),
                              ets:insert(bucket_info_cache_buckets,
                                         {BucketName, BucketConfig}),
                              {ok, Rev, RevEpoch, V};
                          Other ->
                              %% note: we might consider caching
                              %% exceptions but they're supposedly
                              %% rare anyways
                              Other
                      end;
                  [{_, Rev, RevEpoch, V}] ->
                      {ok, Rev, RevEpoch, V}
              end
      end).

terse_bucket_info(BucketName) ->
    case ets:lookup(bucket_info_cache, BucketName) of
        [] ->
            call_compute_bucket_info(BucketName);
        [{_, Rev, RevEpoch, V}] ->
            {ok, Rev, RevEpoch, V}
    end.

build_node_services() ->
    case ets:lookup(bucket_info_cache, 'node_services') of
        [] ->
            case call_build_node_services() of
                {ok, Rev, RevEpoch, V, NodesExtHash} ->
                    {Rev, RevEpoch, V, NodesExtHash};
                {T, E, Stack} ->
                    erlang:raise(T, E, Stack)
            end;
        [{_, Rev, RevEpoch, V, NodesExtHash}] ->
            {Rev, RevEpoch, V, NodesExtHash}
    end.

call_build_node_services() ->
    work_queue:submit_sync_work(
      bucket_info_cache,
      fun () ->
              case ets:lookup(bucket_info_cache, 'node_services') of
                  [] ->
                      try do_build_node_services() of
                          {Rev, RevEpoch, V, NodesExtHash} ->
                              ets:insert(bucket_info_cache,
                                         {'node_services', Rev, RevEpoch, V,
                                          NodesExtHash}),
                              {ok, Rev, RevEpoch, V, NodesExtHash}
                      catch T:E:S ->
                              {T, E, S}
                      end;
                  [{_, Rev, RevEpoch, V, NodesExtHash}] ->
                      {ok, Rev, RevEpoch, V, NodesExtHash}
              end
      end).

build_cluster_capabilities() ->
    Caps = cluster_compat_mode:get_cluster_capabilities(),
    [{clusterCapabilitiesVer, [1, 0]},
     {clusterCapabilities, {Caps}},
     {clusterUUID, menelaus_web:get_uuid()},
     {clusterName, list_to_binary(menelaus_web_pools:get_cluster_name())}].

do_build_node_services() ->
    Config = ns_config:get(),
    {Snapshot, ChronicleRev} =
        chronicle_compat:get_snapshot_with_revision(
          [fun ns_cluster_membership:fetch_snapshot/1,
           chronicle_compat:txn_get_many([counters], _)],
          #{ns_config => Config}),

    NEIs = build_nodes_ext(ns_cluster_membership:active_nodes(Snapshot),
                           Config, Snapshot, []),
    NodesExtHash = integer_to_binary(erlang:phash2(NEIs)),
    Caps = build_cluster_capabilities(),
    Rev = compute_global_rev(Config, ChronicleRev),
    RevEpoch = get_rev_epoch(Snapshot),
    J = {[{rev, Rev}, {nodesExt, NEIs}, {revEpoch, RevEpoch}] ++ Caps},
    {Rev, RevEpoch, ejson:encode(J), NodesExtHash}.

-ifdef(TEST).

setup_compat_mode_for(Version, IsEnterprise) ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, get_compat_version, fun() -> Version end),
    meck:expect(cluster_compat_mode, is_enterprise, fun() -> IsEnterprise end),

    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, search,
        fun(_, cluster_compat_version, undefined) -> Version end),

    meck:new(chronicle_compat, [passthrough]),
    meck:expect(chronicle_compat, get,
        fun(cluster_compat_version, _) -> Version end).

teardown_compat_mode() ->
    meck:unload(cluster_compat_mode),
    meck:unload(ns_config),
    meck:unload(chronicle_compat).

setup_membase_bucket(IsMagma) ->
    meck:new(ns_bucket, [passthrough]),
    meck:expect(ns_bucket, bucket_type, fun(_) -> membase end),
    meck:expect(ns_bucket, can_have_views, fun(_) -> not IsMagma end),
    meck:expect(ns_bucket, is_magma, fun(_) -> IsMagma end),
    meck:expect(ns_bucket, is_persistent, fun(_) -> true end).

teardown_membase_bucket_with_views() ->
    meck:unload(ns_bucket).

membase_bucket_capabilities_test_setup(Version, IsEnterprise, IsMagma) ->
    setup_compat_mode_for(Version, IsEnterprise),
    setup_membase_bucket(IsMagma).

membase_bucket_capabilities_test_teardown() ->
    teardown_compat_mode(),
    teardown_membase_bucket_with_views().

get_bucket_capabilities_for_version(Version, IsEnterprise, IsMagma) ->
    BucketCapabilitiesBase = [collections, durableWrite, tombstonedUserXAttrs,
                              'subdoc.ReplaceBodyWithXattr',
                              'subdoc.DocumentMacroSupport',
                              'subdoc.ReviveDocument',
                              dcp, cbhello, touch, cccp,
                              xdcrCheckpointing, nodesExt, xattr],
    Specific = get_bucket_capabilities(Version, IsEnterprise, IsMagma),
    BucketCapabilitiesBase ++ Specific.

get_bucket_capabilities(?VERSION_72,
                        true = _IsEnterprise,
                        true = _IsMagma) ->
    ['dcp.IgnorePurgedTombstones', nonDedupedHistory];
get_bucket_capabilities(?VERSION_72,
                        false = _IsEnterprise,
                        true = _IsMagma) ->
    ['dcp.IgnorePurgedTombstones'];
get_bucket_capabilities(?VERSION_72,
                        _IsEnterprise,
                        _IsMagma) ->
    [couchapi, 'dcp.IgnorePurgedTombstones' ];
get_bucket_capabilities(?VERSION_76,
                        true = _IsEnterprise,
                        true = _IsMagma) ->
    ['dcp.IgnorePurgedTombstones', nonDedupedHistory, rangeScan,
     preserveExpiry, 'subdoc.ReplicaRead', querySystemCollection,
     mobileSystemCollection];
get_bucket_capabilities(?VERSION_76,
                        false = _IsEnterprise,
                        true = _IsMagma) ->
    ['dcp.IgnorePurgedTombstones', rangeScan, preserveExpiry,
     'subdoc.ReplicaRead', querySystemCollection, mobileSystemCollection];
get_bucket_capabilities(?VERSION_76,
                        _IsEnterprise,
                        _IsMagma) ->
    [couchapi, 'dcp.IgnorePurgedTombstones', rangeScan, preserveExpiry,
     'subdoc.ReplicaRead', querySystemCollection, mobileSystemCollection];
get_bucket_capabilities(?VERSION_MORPHEUS,
                        IsEnterprise,
                        IsMagma) ->
    get_bucket_capabilities(?VERSION_76, IsEnterprise, IsMagma) ++
    ['subdoc.BinaryXattr', 'subdoc.AccessDeleted'];
get_bucket_capabilities(_Version, _IsEnterprise, false = _IsMagma) ->
    [couchapi];
get_bucket_capabilities(_Version, _IsEnterprise, _IsMagma) ->
    [].

membase_bucket_capabilities_test_() ->
    Tests = [{?VERSION_71, false, false},
             {?VERSION_71, true, false},
             {?VERSION_71, false, true},
             {?VERSION_71, true, true},
             {?VERSION_72, false, false},
             {?VERSION_72, true, false},
             {?VERSION_72, false, true},
             {?VERSION_72, true, true},
             {?VERSION_76, false, false},
             {?VERSION_76, true, false},
             {?VERSION_76, false, true},
             {?VERSION_76, true, true},
             {?LATEST_VERSION_NUM, false, false},
             {?LATEST_VERSION_NUM, true, false},
             {?LATEST_VERSION_NUM, false, true},
             {?LATEST_VERSION_NUM, true, true}],

    TestFun =
        fun ({Version, IsEnterprise, IsMagma}, _R) ->
                fun() ->
                        [{bucketCapabilitiesVer,''},
                         {bucketCapabilities, Capabilities}] =
                            build_bucket_capabilities([]),
                        ?assertEqual(
                           lists:sort(Capabilities),
                           lists:sort(
                             get_bucket_capabilities_for_version(Version,
                                                                 IsEnterprise,
                                                                 IsMagma)))
                end
        end,

    {foreachx,
        fun ({Version, IsEnterprise, IsMagma}) ->
            membase_bucket_capabilities_test_setup(Version, IsEnterprise,
                                                   IsMagma)
        end,
        fun (_X, _R) ->
            membase_bucket_capabilities_test_teardown()
        end,
        [{Test, TestFun} || Test <- Tests]}.

%% This test verifies the dependency "contains_empty_vbmap" has on
%% the output (bucket info blob) of "compute_bucket_info_with_config".
verify_compatibility_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    config_profile:mock_default_profile(),
    meck:expect(cluster_compat_mode, is_cluster_72, fun () -> true end),
    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> true end),
    meck:expect(cluster_compat_mode, is_cluster_morpheus, fun () -> true end),
    meck:expect(cluster_compat_mode, is_enterprise, fun () -> true end),
    meck:expect(cluster_compat_mode, get_cluster_capabilities,
                fun () -> [{n1ql, [costBasedOptimizer, indexAdvisor]}] end),
    meck:new(ns_bucket, [passthrough]),
    meck:expect(ns_bucket, uuid, fun (_,_) -> 456 end),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, compute_global_rev, fun (_) -> 111 end),
    meck:expect(ns_config, read_key_fast, fun(_, Default) -> Default end),
    meck:new(service_ports, [passthrough]),
    meck:expect(service_ports, get_port, fun (_,_,_) -> 12000 end),
    meck:new(capi_utils, [passthrough]),
    meck:expect(capi_utils, capi_bucket_url_bin,
                fun (_,_,_,_) -> <<"http://$HOST:9500/bucket%2Buuid">> end),
    meck:new(ns_cluster_membership, [passthrough]),
    meck:expect(ns_cluster_membership, get_node_server_group,
                fun (_,_) -> undefined end),
    meck:new(menelaus_web, [passthrough]),
    meck:expect(menelaus_web, get_uuid, fun () -> <<"77777">> end),
    meck:new(menelaus_web_pools, [passthrough]),
    meck:expect(menelaus_web_pools, get_cluster_name,
                fun () -> "test_cluster" end),

    BC = [{servers, [node()]},
          {type, membase},
          {num_replicas, 1}],
    Snap = #{{bucket, "testBucket",uuid} => 157},
    SnapRev = {hello, 77},

    try
        %% The bucket config has no vbucket map
        {ok, _, _, Blob, _} =
            compute_bucket_info_with_config("testBucket", [], Snap, BC,
                                            SnapRev),
        ?assert(contains_empty_vbmap(Blob)),

        %% Add the vbucket map and ensure it is found.
        BC2 = BC ++ [{map, [['n_0@127.0.0.1','n_1@127.0.0.1']]}],
        {ok, _, _, Blob2, _} =
            compute_bucket_info_with_config("testBucket", [], Snap, BC2,
                                            SnapRev),
        ?assertNot(contains_empty_vbmap(Blob2))
    after
        meck:unload()
    end.
-endif.
