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
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         terse_bucket_info/1]).

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
is_interesting(developer_preview_enabled) -> true;
is_interesting({node, _, services}) -> true;
is_interesting({service_map, _}) -> true;
is_interesting(counters) -> true;
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
    menelaus_util:strip_json_struct(
      menelaus_web_node:alternate_addresses_json(Node, Config, Snapshot,
                                                 WantedPorts)).

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

    NodeInfo = {[{services, {PortInfo}} | NI3]},
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
                   Url = capi_utils:capi_bucket_url_bin(
                           Node, Bucket, BucketUUID, ?LOCALHOST_MARKER_STRING),
                   [{couchApiBase, Url} | Info0];
               _ ->
                   Info0
           end,
    {Info}.

build_short_bucket_info(Id, BucketConfig, Snapshot) ->
    BucketUUID = ns_bucket:uuid(Id, Snapshot),
    [build_name_and_locator(Id, BucketConfig),
     {bucketType, ns_bucket:external_bucket_type(BucketConfig)},
     {storageBackend, ns_bucket:storage_backend(BucketConfig)},
     {uuid, BucketUUID},
     {uri, build_pools_uri(["buckets", Id], BucketUUID)},
     {streamingUri, build_pools_uri(["bucketsStreaming", Id], BucketUUID)},
     build_bucket_capabilities(BucketConfig),
     build_collections_manifest_id(Id, Snapshot)].

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

build_bucket_capabilities(BucketConfig) ->
    Caps =
        case ns_bucket:bucket_type(BucketConfig) of
            membase ->
                Conditional =
                    [{collections, collections:enabled(BucketConfig)},
                     {durableWrite, true},
                     {tombstonedUserXAttrs,
                      cluster_compat_mode:is_cluster_66()},
                     {couchapi, ns_bucket:can_have_views(BucketConfig)},
                     {'subdoc.ReplaceBodyWithXattr',
                      cluster_compat_mode:is_cluster_70()},
                     {'subdoc.DocumentMacroSupport',
                      cluster_compat_mode:is_cluster_70()},
                     {'subdoc.ReviveDocument',
                      cluster_compat_mode:is_cluster_71()},
                     {'nonDedupedHistory',
                      cluster_compat_mode:is_cluster_72() and
                      cluster_compat_mode:is_enterprise() and
                      ns_bucket:is_magma(BucketConfig)},
                     {'dcp.IgnorePurgedTombstones',
                      cluster_compat_mode:is_cluster_72()}],

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

build_global_rev_epoch(Config, Snapshot) ->
    case cluster_compat_mode:is_cluster_70(Config) of
         true ->
            Failovers = ns_cluster:counter(Snapshot, quorum_failover_success,
                                           0),
            [{revEpoch, Failovers + 1}];
         false -> []
    end.

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
    RevEpochJSON = build_global_rev_epoch(Config, Snapshot),
    RevEpoch = case RevEpochJSON of
                   [] -> not_present;
                   [{revEpoch, E}] -> E
               end,
    Json =
        {lists:flatten(
           [{rev, Rev},
            RevEpochJSON,
            build_short_bucket_info(Id, BucketConfig, Snapshot),
            build_ddocs(Id, BucketConfig),
            build_vbucket_map(?LOCALHOST_MARKER_STRING, BucketConfig),
            {nodes,
             [node_bucket_info(Node, Config, Snapshot,
                               Id, BucketUUID, BucketConfig)
              || Node <- Servers]},
            {nodesExt, build_nodes_ext(AllServers, Config, Snapshot, [])},
            build_cluster_capabilities(Config)])},
    {ok, Rev, RevEpoch, ejson:encode(Json), BucketConfig}.

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

build_cluster_capabilities(Config) ->
    Caps = cluster_compat_mode:get_cluster_capabilities(Config),
    [{clusterCapabilitiesVer, [1, 0]},
     {clusterCapabilities, {Caps}}].

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
    Caps = build_cluster_capabilities(Config),
    Rev = compute_global_rev(Config, ChronicleRev),
    RevEpochJSON = build_global_rev_epoch(Config, Snapshot),
    RevEpoch = case RevEpochJSON of
                   [] -> not_present;
                   [{revEpoch, E}] -> E
               end,
    J = {[{rev, Rev},
          {nodesExt, NEIs}] ++ Caps ++ RevEpochJSON},
    {Rev, RevEpoch, ejson:encode(J), NodesExtHash}.

-ifdef(TEST).

setup_compat_mode_for(Version, IsEnterprise) ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, get_compat_version, fun(_) -> Version end),
    meck:expect(cluster_compat_mode, is_enterprise, fun() -> IsEnterprise end),

    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, search,
        fun(_, cluster_compat_version, undefined) -> Version end),

    meck:new(chronicle_compat, [passthrough]),
    meck:expect(chronicle_compat, get,
        fun(_, cluster_compat_version, _) -> Version end).

teardown_compat_mode() ->
    meck:unload(cluster_compat_mode),
    meck:unload(ns_config),
    meck:unload(chronicle_compat).

setup_membase_bucket(IsMagma) ->
    meck:new(ns_bucket, [passthrough]),
    meck:expect(ns_bucket, bucket_type, fun(_) -> membase end),
    meck:expect(ns_bucket, can_have_views, fun(_) -> not IsMagma end),
    meck:expect(ns_bucket, is_magma, fun(_) -> IsMagma end).

teardown_membase_bucket_with_views() ->
    meck:unload(ns_bucket).

membase_bucket_capabilities_test_setup(Version, IsEnterprise, IsMagma) ->
    setup_compat_mode_for(Version, IsEnterprise),
    setup_membase_bucket(IsMagma).

membase_bucket_capabilities_test_teardown() ->
    teardown_compat_mode(),
    teardown_membase_bucket_with_views().

get_bucket_capabilities_for_version(?LATEST_VERSION_NUM,
                                    true = _IsEnterprise,
                                    true = _IsMagma) ->
    %% 7.2+
    [{bucketCapabilitiesVer,''},
        {bucketCapabilities,
            [collections, durableWrite, tombstonedUserXAttrs,
             'subdoc.ReplaceBodyWithXattr', 'subdoc.DocumentMacroSupport',
             'subdoc.ReviveDocument', nonDedupedHistory,
             'dcp.IgnorePurgedTombstones', dcp, cbhello, touch, cccp,
             xdcrCheckpointing, nodesExt, xattr]}];
get_bucket_capabilities_for_version(?LATEST_VERSION_NUM,
                                    false = _IsEnterprise,
                                    true = _IsMagma) ->
    [{bucketCapabilitiesVer,''},
        {bucketCapabilities,
            [collections, durableWrite, tombstonedUserXAttrs,
                'subdoc.ReplaceBodyWithXattr', 'subdoc.DocumentMacroSupport',
                'subdoc.ReviveDocument', 'dcp.IgnorePurgedTombstones',
                dcp, cbhello, touch, cccp, xdcrCheckpointing, nodesExt,
                xattr]}];
get_bucket_capabilities_for_version(?LATEST_VERSION_NUM,
    _IsEnterprise,
    false = _IsMagma) ->
    [{bucketCapabilitiesVer,''},
        {bucketCapabilities,
            [collections, durableWrite, tombstonedUserXAttrs, couchapi,
                'subdoc.ReplaceBodyWithXattr', 'subdoc.DocumentMacroSupport',
                'subdoc.ReviveDocument', 'dcp.IgnorePurgedTombstones',
                dcp, cbhello, touch, cccp, xdcrCheckpointing, nodesExt,
                xattr]}];
get_bucket_capabilities_for_version(?VERSION_71, _IsEnterprise,
                                    false = _IsMagma) ->
    [{bucketCapabilitiesVer,''},
        {bucketCapabilities,
            [collections, durableWrite, tombstonedUserXAttrs, couchapi,
             'subdoc.ReplaceBodyWithXattr', 'subdoc.DocumentMacroSupport',
             'subdoc.ReviveDocument', dcp, cbhello, touch, cccp,
             xdcrCheckpointing, nodesExt, xattr]}];
get_bucket_capabilities_for_version(?VERSION_71, _IsEnterprise,
                                    true = _IsMagma) ->
    [{bucketCapabilitiesVer,''},
        {bucketCapabilities,
            [collections, durableWrite, tombstonedUserXAttrs,
             'subdoc.ReplaceBodyWithXattr', 'subdoc.DocumentMacroSupport',
             'subdoc.ReviveDocument', dcp, cbhello, touch, cccp,
             xdcrCheckpointing, nodesExt, xattr]}].

membase_bucket_capabilities_test_() ->
    Tests = [{?VERSION_71, false, false},
             {?VERSION_71, true, false},
             {?VERSION_71, false, true},
             {?VERSION_71, true, true},
             {?LATEST_VERSION_NUM, false, false},
             {?LATEST_VERSION_NUM, true, false},
             {?LATEST_VERSION_NUM, false, true},
             {?LATEST_VERSION_NUM, true, true}],

    TestFun = fun ({Version, IsEnterprise, IsMagma}, _R) ->
                  fun() ->
                      ?assertEqual(
                          get_bucket_capabilities_for_version(Version,
                                                              IsEnterprise,
                                                              IsMagma),
                          build_bucket_capabilities([]))
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

-endif.
