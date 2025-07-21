%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc handlers for bucket related REST API's

-module(menelaus_web_buckets).

-author('NorthScale <info@northscale.com>').

-include("menelaus_web.hrl").
-include("ns_common.hrl").
-include("couch_db.hrl").
-include("ns_stats.hrl").
-include("ns_bucket.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("bucket_hibernation.hrl").
-include("cb_cluster_secrets.hrl").

-define(CAS_GET_TIMEOUT, 60000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_bucket_list/1,
         handle_bucket_info/3,
         handle_sasl_buckets_streaming/2,
         handle_bucket_info_streaming/3,
         handle_bucket_delete/3,
         handle_bucket_update/3,
         handle_bucket_create/2,
         handle_start_pause/1,
         handle_start_resume/1,
         handle_stop_pause/1,
         handle_stop_resume/1,
         handle_bucket_flush/3,
         handle_compact_bucket/3,
         handle_purge_compact_bucket/3,
         handle_cancel_bucket_compaction/3,
         handle_compact_databases/3,
         handle_cancel_databases_compaction/3,
         handle_compact_view/4,
         handle_cancel_view_compaction/4,
         handle_ddocs_list/3,
         handle_set_ddoc_update_min_changes/4,
         handle_local_random_key/2,
         handle_local_random_key/4,
         maybe_cleanup_old_buckets/0,
         serve_short_bucket_info/2,
         serve_streaming_short_bucket_info/2,
         build_hibernation_state/1,
         get_ddocs_list/2]).

-import(menelaus_util,
        [reply/2,
         reply/3,
         reply_text/3,
         reply_json/2,
         reply_json/3,
         concat_url_path/1,
         bin_concat_path/1,
         bin_concat_path/2,
         handle_streaming/2]).

-define(MAX_BUCKET_NAME_LEN, 100).
-define(MIN_VERSION_PRUNING_WINDOW_HRS, 24).

-define(FUSION_LOGSTORE_URI, "fusionLogstoreURI").

get_info_level(Req) ->
    case proplists:get_value("basic_stats", mochiweb_request:parse_qs(Req)) of
        undefined ->
            normal;
        _ ->
            for_ui
    end.

handle_bucket_list(Req) ->
    Ctx = menelaus_web_node:get_context(Req, all, false, unstable),
    Snapshot = menelaus_web_node:get_snapshot(Ctx),

    BucketsUnsorted =
        menelaus_auth:filter_accessible_buckets(
          ?cut({[{bucket, _}, settings], read}),
          ns_bucket:get_bucket_names(Snapshot), Req),
    Buckets = lists:sort(fun (A,B) -> A =< B end, BucketsUnsorted),

    reply_json(Req, build_buckets_info(Req, Buckets, Ctx, get_info_level(Req))).

handle_bucket_info(_PoolId, Id, Req) ->
    Ctx = menelaus_web_node:get_context(Req, [Id], false, unstable),
    [Json] = build_buckets_info(Req, [Id], Ctx, get_info_level(Req)),
    reply_json(Req, Json).

build_bucket_nodes_info(BucketName, BucketUUID, BucketConfig, Ctx) ->
    %% Only list nodes this bucket is mapped to
    F = menelaus_web_node:build_nodes_info_fun(Ctx, true),
    Nodes = ns_bucket:get_servers(BucketConfig),
    %% NOTE: there's potential inconsistency here between BucketConfig
    %% and (potentially more up-to-date) vbuckets dict. Given that
    %% nodes list is mostly informational I find it ok.
    Dict = case vbucket_map_mirror:node_vbuckets_dict(BucketName) of
               {ok, DV} -> DV;
               {error, not_present} -> dict:new();
               {error, no_map} -> dict:new()
           end,
    LocalAddr = menelaus_web_node:get_local_addr(Ctx),
    add_couch_api_base_loop(Nodes, BucketName, BucketUUID, BucketConfig,
                            LocalAddr, F, Dict, [], []).


add_couch_api_base_loop([], _BucketName, _BucketUUID, _BucketConfig,
                        _LocalAddr, _F, _Dict, CAPINodes, NonCAPINodes) ->
    CAPINodes ++ NonCAPINodes;
add_couch_api_base_loop([Node | RestNodes],
                        BucketName, BucketUUID, BucketConfig, LocalAddr, F,
                        Dict, CAPINodes, NonCAPINodes) ->
    {KV} = F(Node, BucketName),
    case dict:find(Node, Dict) of
        {ok, V} when V =/= [] ->
            %% note this is generally always expected, but let's play safe just in case
            S = {add_couch_api_base(BucketName, BucketUUID, BucketConfig, KV,
                                    Node, LocalAddr)},
            add_couch_api_base_loop(RestNodes, BucketName, BucketUUID,
                                    BucketConfig, LocalAddr, F, Dict,
                                    [S | CAPINodes], NonCAPINodes);
        _ ->
            S = {KV},
            add_couch_api_base_loop(RestNodes, BucketName, BucketUUID,
                                    BucketConfig, LocalAddr, F, Dict,
                                    CAPINodes, [S | NonCAPINodes])
    end.

add_couch_api_base(BucketName, BucketUUID, BucketConfig, KV, Node,
                   LocalAddr) ->
    %% Must completely remove these, as they are used as a signal to the SDK's
    %% regarding whether or not we support views on this couchbase cluster.
    NodesKeysList = ?COUCHDB_ENABLED([{Node, couchApiBase},
                                      {{ssl, Node}, couchApiBaseHTTPS}], []),
    lists:foldl(fun({N, Key}, KVAcc) ->
                        case capi_utils:capi_bucket_url_bin(N, BucketName,
                                                            BucketUUID, LocalAddr) of
                            undefined ->
                                KVAcc;
                            Url ->
                                case ns_bucket:bucket_type(BucketConfig) of
                                    membase ->
                                        [{Key, Url} | KVAcc];
                                    _ ->
                                        KVAcc
                                end
                        end
                end, KV, NodesKeysList).

build_auto_compaction_info(BucketConfig) ->
    case ns_bucket:is_persistent(BucketConfig) of
        true ->
            ACSettings = case proplists:get_value(autocompaction,
                                                  BucketConfig) of
                             undefined -> false;
                             false -> false;
                             ACSettingsX -> ACSettingsX
                         end,

            case ACSettings of
                false ->
                    [{autoCompactionSettings, false}];
                _ ->
                    BackendStorage = ns_bucket:storage_backend(BucketConfig),
                    [{autoCompactionSettings,
                      menelaus_web_autocompaction:build_bucket_settings(
                        ACSettings, BackendStorage)}]
            end;
        false ->
            case ns_bucket:storage_mode(BucketConfig) of
                ephemeral ->
                    [];
                undefined ->
                    %% When the bucket type is memcached.
                    [{autoCompactionSettings, false}]
            end
    end.

build_purge_interval_info(BucketConfig) ->
    case ns_bucket:is_persistent(BucketConfig) of
        true ->
            case proplists:get_value(autocompaction, BucketConfig, false) of
                false ->
                    [];
                _Val ->
                    PInterval = case proplists:get_value(purge_interval,
                                                         BucketConfig) of
                                    undefined ->
                                        compaction_api:get_purge_interval(global);
                                    PI -> PI
                                end,
                    [{purgeInterval, PInterval}]
            end;
        false ->
            case ns_bucket:storage_mode(BucketConfig) of
                ephemeral ->
                    [{purgeInterval, proplists:get_value(purge_interval,
                                                         BucketConfig)}];
                undefined ->
                    %% When the bucket type is memcached.
                    []
            end
    end.

build_eviction_policy(BucketConfig) ->
    case ns_bucket:eviction_policy(BucketConfig) of
        value_only ->
            <<"valueOnly">>;
        full_eviction ->
            <<"fullEviction">>;
        no_eviction ->
            <<"noEviction">>;
        nru_eviction ->
            <<"nruEviction">>
    end.

build_durability_min_level(BucketConfig) ->
    case ns_bucket:durability_min_level(BucketConfig) of
        undefined ->
            <<"none">>;
        none ->
            <<"none">>;
        majority ->
            <<"majority">>;
        majority_and_persist_on_master ->
            <<"majorityAndPersistActive">>;
        persist_to_majority ->
            <<"persistToMajority">>
    end.

build_durability_impossible_fallback(BucketConfig) ->
    case ns_bucket:durability_impossible_fallback(BucketConfig) of
        disabled -> <<"disabled">>;
        fallback_to_master_ack -> <<"fallbackToActiveAck">>
    end.

build_buckets_info(Req, Buckets, Ctx, InfoLevel) ->
    SkipMap = InfoLevel =/= streaming andalso
        proplists:get_value(
          "skipMap", mochiweb_request:parse_qs(Req)) =:= "true",
    [build_bucket_info(BucketName, Ctx, InfoLevel, SkipMap) ||
        BucketName <- Buckets].

build_bucket_info(Id, Ctx, InfoLevel, SkipMap) ->
    Snapshot = menelaus_web_node:get_snapshot(Ctx),
    {ok, BucketConfig} = ns_bucket:get_bucket(Id, Snapshot),
    BucketUUID = ns_bucket:uuid(Id, Snapshot),

    {lists:flatten(
       [bucket_info_cache:build_short_bucket_info(Id, BucketConfig, Snapshot),
        bucket_info_cache:build_ddocs(Id, BucketConfig),
        [bucket_info_cache:build_vbucket_map(
           menelaus_web_node:get_local_addr(Ctx), BucketConfig)
         || not SkipMap],
        {localRandomKeyUri,
         bucket_info_cache:build_pools_uri(["buckets", Id, "localRandomKey"])},
        {controllers, {build_controllers(Id, BucketConfig)}},
        {nodes, build_bucket_nodes_info(Id, BucketUUID, BucketConfig, Ctx)},
        {stats,
         {[{uri, bucket_info_cache:build_pools_uri(["buckets", Id, "stats"])},
           {directoryURI,
            bucket_info_cache:build_pools_uri(["buckets", Id, "stats",
                                               "Directory"])},
           {nodeStatsListURI,
            bucket_info_cache:build_pools_uri(["buckets", Id, "nodes"])}]}},
        %% Needed by XDCR on versions prior to 7.0. This must remain
        %% until there are no supported pre-7.0 versions that can
        %% replicate to us
        {authType, sasl},
        build_auto_compaction_info(BucketConfig),
        build_purge_interval_info(BucketConfig),
        build_replica_index(BucketConfig),
        build_bucket_placer_params(BucketConfig),
        build_hibernation_state(BucketConfig),
        build_storage_limits(BucketConfig),
        build_throttle_limits(BucketConfig),
        build_bucket_rank(BucketConfig),
        build_cross_cluster_versioning_params(BucketConfig),
        build_vbuckets_max_cas(BucketConfig),
        build_vp_window_hrs(BucketConfig),
        build_dynamic_bucket_info(InfoLevel, Id, BucketConfig, Ctx),
        build_encryption_at_rest_bucket_info(BucketConfig)])}.

get_internal_default(Key, Default) ->
    ns_config:read_key_fast(Key, Default).

build_bucket_rank(BucketConfig) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            [{rank, ns_bucket:rank(BucketConfig)}];
        false ->
            []
    end.

build_cross_cluster_versioning_params(BucketConfig) ->
    CcVersioningEnabledVal = ns_bucket:get_cc_versioning_enabled(BucketConfig),
    case cluster_compat_mode:is_cluster_76() andalso
         CcVersioningEnabledVal =/= undefined of
        true ->
            [{enableCrossClusterVersioning, CcVersioningEnabledVal}];
        false ->
            []
    end.

build_vbuckets_max_cas(BucketConfig) ->
    CasValues = ns_bucket:get_vbuckets_max_cas(BucketConfig),
    case cluster_compat_mode:is_cluster_76() andalso
         CasValues =/= undefined of
        true ->
            [{vbucketsMaxCas, [list_to_binary(Val) || Val <- CasValues]}];
        false ->
            []
    end.

build_vp_window_hrs(BucketConfig) ->
    VpWindowHrs = ns_bucket:get_vp_window_hrs(BucketConfig),
    case cluster_compat_mode:is_cluster_76() andalso
         VpWindowHrs =/= undefined of
        true ->
            [{versionPruningWindowHrs, VpWindowHrs}];
        false ->
            []
    end.

build_limits(BucketConfig, ProfileKey, AttributesFunc) ->
    case config_profile:get_bool(ProfileKey) of
        false -> [];
        true ->
            [{Param, proplists:get_value(Key,
                                         BucketConfig,
                                         get_internal_default(Key, Default))} ||
                {Param, Key, Default, _, _} <- AttributesFunc()]
    end.

build_storage_limits(BucketConfig) ->
    build_limits(BucketConfig, enable_storage_limits,
                 fun menelaus_web_settings:get_storage_limit_attributes/0).

build_throttle_limits(BucketConfig) ->
    build_limits(BucketConfig, enable_throttle_limits,
                 fun menelaus_web_settings:get_throttle_limit_attributes/0).

build_bucket_placer_params(BucketConfig) ->
    case ns_bucket:get_width(BucketConfig) of
        undefined ->
            [];
        Width ->
            [{width, Width}, {weight, ns_bucket:get_weight(BucketConfig)}]
    end.

build_hibernation_state(BucketConfig) ->
    case ns_bucket:get_hibernation_state(BucketConfig) of
        undefined ->
            [];
        State ->
            {hibernationState, State}
    end.

build_replica_index(BucketConfig) ->
    [{replicaIndex, proplists:get_value(replica_index, BucketConfig, true)} ||
        ns_bucket:can_have_views(BucketConfig)].

build_controller(Id, Controller) ->
    bucket_info_cache:build_pools_uri(["buckets", Id, "controller",
                                       Controller]).

build_controllers(Id, BucketConfig) ->
    [{compactAll, build_controller(Id, "compactBucket")},
     {compactDB, build_controller(Id, "compactDatabases")},
     {purgeDeletes, build_controller(Id, "unsafePurgeBucket")},
     {startRecovery, build_controller(Id, "startRecovery")} |
     [{flush, build_controller(Id, "doFlush")} ||
         proplists:get_value(flush_enabled, BucketConfig, false)]].

build_bucket_stats(for_ui, Id, Ctx) ->
    Config = menelaus_web_node:get_config(Ctx),
    Snapshot = menelaus_web_node:get_snapshot(Ctx),

    StorageTotals = [{Key, {StoragePList}}
                     || {Key, StoragePList} <-
                            ns_storage_conf:cluster_storage_info(Config,
                                                                 Snapshot)],

    [{storageTotals, {StorageTotals}} | menelaus_stats:basic_stats(Id,
                                                                   Snapshot)];
build_bucket_stats(_, Id, Ctx) ->
    Snapshot = menelaus_web_node:get_snapshot(Ctx),
    menelaus_stats:basic_stats(Id, Snapshot).

build_dynamic_bucket_info(streaming, _Id, BucketConfig, _) ->
    case ns_bucket:storage_mode(BucketConfig) of
        magma -> build_continuous_backup_info(BucketConfig);
        _ -> []
    end;

build_dynamic_bucket_info(InfoLevel, Id, BucketConfig, Ctx) ->
    [[{replicaNumber, ns_bucket:num_replicas(BucketConfig)},
      {threadsNumber, proplists:get_value(num_threads, BucketConfig,
                                          ?NUM_WORKER_THREADS)},
      {quota, {[{ram, ns_bucket:ram_quota(BucketConfig)},
                {rawRAM, ns_bucket:raw_ram_quota(BucketConfig)}]}},
      {basicStats, {build_bucket_stats(InfoLevel, Id, Ctx)}},
      {evictionPolicy, build_eviction_policy(BucketConfig)},
      {durabilityMinLevel, build_durability_min_level(BucketConfig)},
      build_magma_bucket_info(BucketConfig),
      {conflictResolutionType,
       ns_bucket:conflict_resolution_type(BucketConfig)},
      {workloadPatternDefault, ns_bucket:workload_pattern_default(BucketConfig)}],
     case cluster_compat_mode:is_enterprise() of
         true ->
             [{maxTTL, proplists:get_value(max_ttl, BucketConfig, 0)},
              {compressionMode,
               proplists:get_value(compression_mode, BucketConfig, off)}];
         false ->
             []
     end,
     case cluster_compat_mode:is_cluster_79() andalso
          ns_bucket:bucket_type(BucketConfig) =:= membase of
         true ->
             [{expiryPagerSleepTime,
               ns_bucket:get_expiry_pager_sleep_time(BucketConfig)},
              {memoryLowWatermark,
               ns_bucket:get_memory_low_watermark(BucketConfig)},
              {memoryHighWatermark,
               ns_bucket:get_memory_high_watermark(BucketConfig)},
              {durabilityImpossibleFallback,
               build_durability_impossible_fallback(BucketConfig)},
              {warmupBehavior, ns_bucket:warmup_behavior(BucketConfig)},
              {invalidHlcStrategy,
               ns_bucket:get_invalid_hlc_strategy(BucketConfig)},
              {hlcMaxFutureThreshold,
               ns_bucket:get_hlc_max_future_threshold(BucketConfig)},
              {dcpConnectionsBetweenNodes,
               ns_bucket:get_num_dcp_connections(BucketConfig)},
              {dcpBackfillIdleProtectionEnabled,
               ns_bucket:get_dcp_backfill_idle_protection_enabled(
                 BucketConfig)},
              {dcpBackfillIdleLimitSeconds,
               ns_bucket:get_dcp_backfill_idle_limit_seconds(BucketConfig)},
              {dcpBackfillIdleDiskThreshold,
               ns_bucket:get_dcp_backfill_idle_disk_threshold(BucketConfig)}] ++
                 case ns_bucket:is_persistent(BucketConfig) of
                     true ->
                         [{accessScannerEnabled,
                           ns_bucket:get_access_scanner_enabled(BucketConfig)}];
                     false ->
                         []
                 end;
         false ->
             []
     end,
     case ns_bucket:drift_thresholds(BucketConfig) of
         undefined ->
             [];
         {DriftAheadThreshold, DriftBehindThreshold} ->
             [{driftAheadThresholdMs, DriftAheadThreshold},
              {driftBehindThresholdMs, DriftBehindThreshold}]
     end,
     case cluster_compat_mode:is_cluster_79() of
         true ->
            Snapshot = menelaus_web_node:get_snapshot(Ctx),
            BucketUUID = ns_bucket:uuid(Id, Snapshot),
            [{encryptionAtRestInfo,
              menelaus_web_encr_at_rest:build_bucket_encr_at_rest_info(
                BucketUUID, BucketConfig)}];
         false ->
            []
     end].

build_encryption_at_rest_bucket_info(BucketConfig) ->
    case cluster_compat_mode:is_cluster_79() of
        true ->
            [{encryptionAtRestKeyId,
              proplists:get_value(encryption_secret_id, BucketConfig,
                                  ?SECRET_ID_NOT_SET)},
             {encryptionAtRestDekRotationInterval,
              proplists:get_value(encryption_dek_rotation_interval,
                                  BucketConfig,
                                  ?DEFAULT_DEK_ROTATION_INTERVAL_S)},
             {encryptionAtRestDekLifetime,
              proplists:get_value(encryption_dek_lifetime,
                                  BucketConfig,
                                  ?DEFAULT_DEK_LIFETIME_S)}];
        false ->
            []
    end.

build_continuous_backup_info(BucketConfig) ->
    case cluster_compat_mode:is_cluster_79() of
        true ->
            [{continuousBackupEnabled,
              ns_bucket:get_continuous_backup_enabled(BucketConfig)},
             {continuousBackupInterval,
              ns_bucket:get_continuous_backup_interval(BucketConfig)},
             {continuousBackupLocation,
              list_to_binary(ns_bucket:get_continuous_backup_location(
                               BucketConfig))}];
        false ->
            []
    end.

build_magma_bucket_info(BucketConfig) ->
    case ns_bucket:storage_mode(BucketConfig) of
        magma ->
            lists:flatten(
              [{storageQuotaPercentage,
                proplists:get_value(storage_quota_percentage,
                                    BucketConfig,
                                    ?MAGMA_STORAGE_QUOTA_PERCENTAGE)},
               case cluster_compat_mode:is_cluster_72() of
                   false -> [];
                   true ->
                       [{historyRetentionSeconds,
                         ns_bucket:history_retention_seconds(BucketConfig)},
                        {historyRetentionBytes,
                         ns_bucket:history_retention_bytes(BucketConfig)},
                        {historyRetentionCollectionDefault,
                         ns_bucket:history_retention_collection_default(
                           BucketConfig)},
                        {magmaKeyTreeDataBlockSize,
                         ns_bucket:magma_key_tree_data_blocksize(BucketConfig)},
                        {magmaSeqTreeDataBlockSize,
                         ns_bucket:magma_seq_tree_data_blocksize(BucketConfig)}]
               end,
               build_continuous_backup_info(BucketConfig),
               case config_profile:search({magma, can_set_max_shards}, false) of
                   true ->
                       {magmaMaxShards,
                        proplists:get_value(magma_max_shards, BucketConfig,
                                            ?DEFAULT_MAGMA_SHARDS)};
                   false -> []
               end,
               case proplists:get_value(magma_fusion_logstore_uri,
                                        BucketConfig) of
                   undefined ->
                       [];
                   Value ->
                       {?FUSION_LOGSTORE_URI, list_to_binary(Value)}
               end]);
        _ ->
            []
    end.

handle_sasl_buckets_streaming(_PoolId, Req) ->
    LocalAddr = menelaus_util:local_addr(Req),

    F = fun (_, _) ->
                List = [build_sasl_bucket_info({Id, BucketConfig}, LocalAddr) ||
                           {Id, BucketConfig} <- ns_bucket:get_buckets()],
                {just_write, {[{buckets, List}]}}
        end,
    handle_streaming(F, Req).

build_sasl_bucket_nodes(BucketConfig, LocalAddr) ->
    {nodes,
     [{[{hostname, menelaus_web_node:build_node_hostname(
                     ns_config:latest(), N, LocalAddr)},
        {ports, {[{direct,
                   service_ports:get_port(
                     memcached_port, ns_config:latest(), N)}]}}]} ||
         N <- ns_bucket:get_servers(BucketConfig)]}.

build_sasl_bucket_info({Id, BucketConfig}, LocalAddr) ->
    {lists:flatten(
       [bucket_info_cache:build_name_and_locator(Id, BucketConfig),
        bucket_info_cache:build_vbucket_map(LocalAddr, BucketConfig),
        build_sasl_bucket_nodes(BucketConfig, LocalAddr)])}.

build_streaming_info(Id, Req, LocalAddr, UpdateID) ->
    ns_server_stats:notify_counter(<<"build_streaming_info">>),
    BucketInfo = menelaus_web_cache:lookup_or_compute_with_expiration(
                   {build_bucket_info, Id, LocalAddr},
                   fun () ->
                           Ctx = menelaus_web_node:get_context(
                                   {ip, LocalAddr}, [Id], false, stable),
                           Snapshot = menelaus_web_node:get_snapshot(Ctx),
                           case ns_bucket:bucket_exists(Id, Snapshot) of
                               true ->
                                   [Info] = build_buckets_info(Req, [Id], Ctx,
                                                               streaming),
                                   {Info, 1000, UpdateID};
                               false ->
                                   {error, bucket_not_present}
                           end
                   end,
                   fun (_Key, _Value, OldUpdateID) ->
                           case {OldUpdateID, UpdateID} of
                               {_, undefined} ->
                                   false;
                               {undefined, _} ->
                                   true;
                               {OldID, ID} ->
                                   ID > OldID
                           end
                   end),
    case BucketInfo of
        {error, bucket_not_present} ->
            exit(normal);
        _ ->
            BucketInfo
    end.

handle_bucket_info_streaming(_PoolId, Id, Req) ->
    LocalAddr = menelaus_util:local_addr(Req),
    handle_streaming(
      fun(_Stability, UpdateID) ->
              {just_write,
               build_streaming_info(Id, Req, LocalAddr, UpdateID)}
      end, Req).

handle_bucket_delete(_PoolId, BucketId, Req) ->
    menelaus_web_rbac:assert_no_users_upgrade(),

    ?log_debug("Received request to delete bucket \"~s\"; will attempt to delete", [BucketId]),
    case ns_orchestrator:delete_bucket(BucketId) of
        ok ->
            ns_audit:delete_bucket(Req, BucketId),
            ?MENELAUS_WEB_LOG(?BUCKET_DELETED, "Deleted bucket \"~s\"~n", [BucketId]),
            reply(Req, 200);
        {exit, {not_found, _}, _} ->
            ns_server_stats:notify_counter({<<"rest_request_failure">>,
                                            [{type, bucket_delete},
                                             {code, 404}]}),
            reply_text(Req, "The bucket to be deleted was not found.\r\n", 404);
        Err ->
            {Body, Code} =
                case Err of
                    {shutdown_failed, _} ->
                        {{[{'_',
                           <<"Bucket deletion not yet complete, but will "
                             "continue.\r\n">>}]}, 500};
                    shutdown_incomplete ->
                        {{[{'_',
                           <<"Bucket shutdown interrupted by "
                             "auto-failover">>}]}, 500};
                    Other ->
                        case menelaus_web_cluster:busy_reply(
                               "delete bucket", Other) of
                            {ErrCode, Msg} ->
                                {{[{'_', iolist_to_binary(Msg)}]}, ErrCode};
                            undefined ->
                                exit(Other)
                        end
                end,
            ns_server_stats:notify_counter({<<"rest_request_failure">>,
                                            [{type, bucket_delete},
                                             {code, Code}]}),
            reply_json(Req, Body, Code)
    end.

respond_bucket_created(Req, PoolId, BucketId) ->
    reply(Req, 202, [{"Location", concat_url_path(["pools", PoolId, "buckets", BucketId])}]).

-record(bv_ctx, {
          validate_only,
          ignore_warnings,
          new,
          bucket_name,
          bucket_uuid,
          bucket_config,
          all_buckets,
          kv_nodes,
          max_replicas,
          cluster_storage_totals,
          cluster_version,
          is_enterprise,
          is_developer_preview}).

init_bucket_validation_context(IsNew, BucketName, Req) ->
    ValidateOnly =
        proplists:get_value("just_validate",
                            mochiweb_request:parse_qs(Req)) =:= "1",
    IgnoreWarnings =
        proplists:get_value("ignore_warnings",
                            mochiweb_request:parse_qs(Req)) =:= "1",

    Config = ns_config:get(),
    Snapshot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(all, _, [props, uuid]),
           ns_cluster_membership:fetch_snapshot(_)], #{ns_config => Config}),

    KvNodes = ns_cluster_membership:service_active_nodes(Snapshot, kv),
    ServerGroups = ns_cluster_membership:server_groups(Snapshot),

    BucketUUID =
        case IsNew of
            true -> ns_bucket:uuid(BucketName, Snapshot);
            false -> not_present
        end,
    init_bucket_validation_context(
      IsNew, BucketName, BucketUUID,
      ns_bucket:get_buckets(Snapshot),
      KvNodes, ServerGroups,
      ns_storage_conf:cluster_storage_info(Config, Snapshot),
      ValidateOnly, IgnoreWarnings,
      cluster_compat_mode:get_compat_version(),
      cluster_compat_mode:is_enterprise(),
      cluster_compat_mode:is_developer_preview()).

init_bucket_validation_context(IsNew, BucketName, BucketUUID, AllBuckets,
                               KvNodes, ServerGroups,
                               ClusterStorageTotals,
                               ValidateOnly, IgnoreWarnings,
                               ClusterVersion, IsEnterprise,
                               IsDeveloperPreview) ->
    KvServerGroups =
        ns_cluster_membership:get_nodes_server_groups(KvNodes, ServerGroups),
    NumKvNodes = length(KvNodes),

    %% Maximum number of replicas that we'll be able to place in the current
    %% cluster configuration.
    MaxReplicas =
        case ns_cluster_membership:rack_aware(KvServerGroups) of
            true ->
                ns_cluster_membership:get_max_replicas(NumKvNodes,
                                                       KvServerGroups);
            false ->
                NumKvNodes - 1
        end,

    BucketConfig =
        case lists:keyfind(BucketName, 1, AllBuckets) of
            false ->
                false;
            {_, V} ->
                V
        end,
    #bv_ctx{
       validate_only = ValidateOnly,
       ignore_warnings = IgnoreWarnings,
       new = IsNew,
       bucket_name = BucketName,
       bucket_uuid = BucketUUID,
       all_buckets = AllBuckets,
       kv_nodes = KvNodes,
       max_replicas = MaxReplicas,
       bucket_config = BucketConfig,
       cluster_storage_totals = ClusterStorageTotals,
       cluster_version = ClusterVersion,
       is_enterprise = IsEnterprise,
       is_developer_preview = IsDeveloperPreview
      }.

is_storage_mode_migration(_IsNewBucket = true, _BucketConfig, _Params) ->
    false;
is_storage_mode_migration(_IsNewBucket, BucketConfig, Params) ->
    NewStorageMode =
        case proplists:get_value("storageBackend", Params) of
            "couchstore" ->
                couchstore;
            "magma" ->
                magma;
            _ ->
                %% If an incorrect storageBackend is passed, we'll report
                %% an error via parse_validate_storage_mode. Set the
                %% NewStorageMode to undefined so that we can return false,
                %% below.
                undefined
        end,
    OldStorageMode = ns_bucket:storage_mode(BucketConfig),

    %% Unfortunately the way the current code is written it is possible to
    %% update an ephemeral bucket with 'storageBackend' param set to
    %% "couchstore" or "magma" (See comments in parse_validate_storage_mode)
    %% - we would want to ignore such an update and therefore, make stronger
    %% checks to allow only couchstore -> magma and magma -> couchstore
    %% migration.
    case {OldStorageMode, NewStorageMode} of
        {magma, couchstore} -> true;
        {couchstore, magma} -> true;
        _ -> false
    end.

format_error_response(Errors, JSONSummaries) ->
    {[{errors, {Errors}} |
      [{summaries, {JSONSummaries}} || JSONSummaries =/= undefined]]}.

handle_bucket_update(_PoolId, BucketId, Req) ->
    menelaus_web_rbac:assert_no_users_upgrade(),
    Params = mochiweb_request:parse_post(Req),
    handle_bucket_update_inner(BucketId, Req, Params, 32).

handle_bucket_update_inner(_BucketId, _Req, _Params, 0) ->
    exit(bucket_update_loop);
handle_bucket_update_inner(BucketId, Req, Params, Limit) ->
    Ctx = init_bucket_validation_context(false, BucketId, Req),
    case {Ctx#bv_ctx.validate_only, Ctx#bv_ctx.ignore_warnings,
          parse_bucket_params(Ctx, Params)} of
        {_, _, {errors, Errors, JSONSummaries}} ->
            reply_json(Req, format_error_response(Errors, JSONSummaries), 400);
        {false, _, {ok, ParsedProps, _}} ->
            BucketType = proplists:get_value(bucketType, ParsedProps),
            UpdatedProps = ns_bucket:extract_bucket_props(ParsedProps),
            case update_bucket(Ctx, BucketId, BucketType, UpdatedProps, Req) of
                retry ->
                    handle_bucket_update_inner(BucketId, Req, Params,
                                               Limit - 1);
                Response ->
                    Response
             end;

        {true, true, {ok, _, JSONSummaries}} ->
            reply_json(Req, format_error_response([], JSONSummaries), 200);
        {true, false, {ok, ParsedProps, JSONSummaries}} ->
            FinalErrors = perform_warnings_validation(Ctx, ParsedProps, []),
            reply_json(Req, format_error_response(FinalErrors, JSONSummaries),
                       case FinalErrors of
                           [] -> 202;
                           _ -> 400
                       end)
    end.

update_props_with_cas(_NodeCasVals, [] = _Map, _Props) ->
    {error, max_cas_vbucket_retrieval_no_map};
update_props_with_cas(NodeCasVals, Map, Props) ->
    try
        CasValues =
            lists:map(
              fun({VBucket, [ActiveNode | _Rest]}) ->
                      VBucketCas = proplists:get_value(ActiveNode, NodeCasVals),
                      {ok, [{"max_cas", Value}]} = dict:find(VBucket,
                                                             VBucketCas),
                      Value
              end, misc:enumerate(Map, 0)),
        {ok, [{vbuckets_max_cas, CasValues} | Props]}
    catch
        T:E ->
            ?log_error("Failed to retrieve max_cas: ~p", [{T,E}]),
            {error, max_cas_vbucket_retrieval}
    end.

storage_mode_migration_error(in_progress) ->
    {"Cannot update bucket while storage mode is being migrated.", 503};
storage_mode_migration_error(janitor_not_run) ->
    {"Cannot migrate storage mode before janitor has run for the bucket.", 503};
storage_mode_migration_error(history_retention_enabled_on_bucket) ->
    {"Cannot migrate storage mode. history_retention enabled on bucket.", 400};
storage_mode_migration_error(history_retention_enabled_on_collections) ->
    {"Cannot migrate storage mode. history_retention enabled on collections.",
     400}.

reply_storage_mode_migration_error(Req, Error) ->
    {Reply, Code} = storage_mode_migration_error(Error),
    reply_text(Req, Reply, Code).

maybe_update_cas_props(BucketId, BucketConfig, UpdatedProps, true = _CcvEn) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    {NodeResp, NodeErrors, DownNodes} =
        misc:rpc_multicall_with_plist_result(
          Servers, ns_memcached, get_vbucket_details_stats,
          [BucketId, ["max_cas"]], ?CAS_GET_TIMEOUT),

    case NodeErrors =:= [] andalso DownNodes =:= [] of
        true ->
            Map = proplists:get_value(map, BucketConfig, []),
            NodeCasVals = [{Node, Dict} || {Node, {ok, Dict}} <- NodeResp],
            update_props_with_cas(NodeCasVals, Map, UpdatedProps);
        false ->
            ?log_warning("Some nodes didn't return max_cas values: ~n~p",
                         [{NodeErrors, DownNodes}]),
            {error, node_failures}
    end;
maybe_update_cas_props(_, _, UpdatedProps, false = _CcEn) ->
    {ok, UpdatedProps}.

update_via_orchestrator(Req, BucketId, StorageMode, BucketType, UpdatedProps) ->
    update_via_orchestrator(Req, BucketId, StorageMode, BucketType,
                            UpdatedProps, true).

update_via_orchestrator(Req, BucketId, StorageMode, BucketType, UpdatedProps,
                        CanRetry) ->
    case ns_orchestrator:update_bucket(BucketType, StorageMode,
                                       BucketId, UpdatedProps) of
        ok ->
            ns_audit:modify_bucket(Req, BucketId, BucketType, UpdatedProps),
            DisplayBucketType = ns_bucket:display_type(BucketType,
                                                       StorageMode),
            ale:info(?USER_LOGGER,
                     "Updated bucket \"~s\" (of type ~s) properties:~n~p",
                     [BucketId, DisplayBucketType, UpdatedProps]),

            PropsRequiringJanitorRun = [dcp_connections_between_nodes],
            ShouldWaitForJanitor =
                lists:any(
                  fun(Prop) ->
                          proplists:is_defined(Prop, UpdatedProps)
                  end, PropsRequiringJanitorRun),
            case ShouldWaitForJanitor of
                false -> ok;
                true ->
                    ns_orchestrator:ensure_janitor_run({bucket, BucketId},
                                                       5000)
            end,
            reply(Req, 200);
        {error, {need_more_space, Zones}} ->
            reply_text(Req, need_more_space_error(Zones), 400);
        {error, cc_versioning_already_enabled} ->
            reply_text(Req, "Cross cluster versioning already enabled", 409);
        {error, {storage_mode_migration, janitor_not_run}} when CanRetry ->
            ns_orchestrator:ensure_janitor_run({bucket, BucketId}, 5000),
            update_via_orchestrator(Req, BucketId, StorageMode, BucketType,
                                    UpdatedProps, false);
        {error, {storage_mode_migration, Error}} ->
            reply_storage_mode_migration_error(Req, Error);
        {error, secret_not_found} ->
            reply_text(Req, "Encryption key does not exist", 400);
        {error, secret_not_allowed} ->
            reply_text(Req, "Encryption key can't encrypt this bucket", 400);
        {exit, {not_found, _}, _} ->
            %% if this happens then our validation raced, so repeat everything
            retry;
        Other ->
            case menelaus_web_cluster:busy_reply("update bucket", Other) of
                {Code, Msg} ->
                    reply_text(Req, Msg, Code);
                undefined ->
                    exit(Other)
            end
    end.

update_bucket(Ctx, BucketId, BucketType, UpdatedProps, Req) ->
    #bv_ctx{bucket_config = BucketConfig} = Ctx,
    StorageMode = ns_bucket:storage_mode(BucketConfig),
    CcvEn = proplists:get_value(cross_cluster_versioning_enabled,
                                UpdatedProps, false),

    case maybe_update_cas_props(BucketId, BucketConfig, UpdatedProps, CcvEn) of
        {ok, UpdateProps1} ->
            update_via_orchestrator(Req, BucketId, StorageMode, BucketType,
                                    UpdateProps1);
        {error, max_cas_vbucket_retrieval_no_map} ->
            reply_text(Req, "Unable to retrieve max_cas due to no vBucket map",
                            503);
        {error, max_cas_vbucket_retrieval} ->
            reply_text(Req, "Unable to retrieve max_cas for all vbuckets",
                            503);
        {error, node_failures} ->
            reply_text(Req, "Failed to retrieve max_cas because unable to "
                            "reach all kv nodes", 503)
    end.

maybe_cleanup_old_buckets() ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            ok;
        false ->
            true = ns_node_disco:nodes_wanted() =:= [node()],
            ns_storage_conf:delete_unused_buckets_db_files()
    end.

need_more_space_error(Zones) ->
    iolist_to_binary(
      io_lib:format("Need more space in availability zones ~p.", [Zones])).

do_bucket_create(Req, Name, ParsedProps) ->
    BucketType = proplists:get_value(bucketType, ParsedProps),
    StorageMode = proplists:get_value(storage_mode, ParsedProps, undefined),
    BucketProps = ns_bucket:extract_bucket_props(ParsedProps),
    maybe_cleanup_old_buckets(),
    case ns_orchestrator:create_bucket(BucketType, Name, BucketProps) of
        ok ->
            ns_audit:create_bucket(Req, Name, BucketType, BucketProps),
            DisplayBucketType = ns_bucket:display_type(BucketType, StorageMode),
            ?MENELAUS_WEB_LOG(?BUCKET_CREATED, "Created bucket \"~s\" of type: ~s~n~p",
                              [Name, DisplayBucketType, BucketProps]),
            ok;
        {error, {already_exists, _}} ->
            {errors, 400,
             [{name, <<"Bucket with given name already exists">>}]};
        {error, {still_exists, _}} ->
            {errors, 500, [{'_', <<"Bucket with given name still exists">>}]};
        {error, {need_more_space, Zones}} ->
            {errors, 400, [{'_', need_more_space_error(Zones)}]};
        {error, {incorrect_parameters, Error}} ->
            {errors, 400, [{'_', list_to_binary(Error)}]};
        {error, {kek_not_found, _}} ->
            {errors, 400, [{encryptionAtRestKeyId,
                            <<"Encryption key does not exist">>}]};
        {error, secret_not_found} ->
            {errors, 400, [{encryptionAtRestKeyId,
                            <<"Encryption key does not exist">>}]};
        {error, secret_not_allowed} ->
            {errors, 400, [{encryptionAtRestKeyId,
                            <<"Encryption key can't encrypt this bucket">>}]};
        Other ->
            case menelaus_web_cluster:busy_reply("create bucket", Other) of
                {Code, Msg} ->
                    {errors, Code, [{"_", iolist_to_binary(Msg)}]};
                undefined ->
                    exit(Other)
            end
    end.

do_bucket_create(Req, Name, Params, Ctx) ->
    MaxBuckets = ns_bucket:get_max_buckets(),
    case length(Ctx#bv_ctx.all_buckets) >= MaxBuckets of
        true ->
            case MaxBuckets == ns_bucket:get_max_buckets_supported() of
                true ->
                    {{[{'_',
                        iolist_to_binary(
                          io_lib:format(
                            "Cannot create more than ~w buckets",
                            [MaxBuckets]))}]}, 400};
                false ->
                    {{[{'_',
                        iolist_to_binary(
                          io_lib:format(
                            "Cannot create more than ~w buckets due to "
                            "insufficient cpu cores. Either increase the "
                            "resource minimum or the number of cores on "
                            "all kv nodes.",
                            [MaxBuckets]))}]}, 400}
            end;
        false ->
            case {Ctx#bv_ctx.validate_only, Ctx#bv_ctx.ignore_warnings,
                  parse_bucket_params(Ctx, Params)} of
                {_, _, {errors, Errors, JSONSummaries}} ->
                    {format_error_response(Errors, JSONSummaries), 400};
                {false, _, {ok, ParsedProps, JSONSummaries}} ->
                    case do_bucket_create(Req, Name, ParsedProps) of
                        ok -> ok;
                        {errors, Code, Errors} ->
                            ?log_debug("Failed to create bucket '~s' with "
                                       "code: ~p error(s): ~p",
                                       [Name, Code, Errors]),
                            {format_error_response(Errors, JSONSummaries),
                             Code}
                    end;
                {true, true, {ok, _, JSONSummaries}} ->
                    {format_error_response([], JSONSummaries), 200};
                {true, false, {ok, ParsedProps, JSONSummaries}} ->
                    FinalErrors =
                        perform_warnings_validation(Ctx, ParsedProps, []),
                    {format_error_response(FinalErrors, JSONSummaries),
                     case FinalErrors of
                         [] -> 200;
                         _ -> 400
                     end}
            end
    end.

handle_bucket_create(PoolId, Req) ->
    menelaus_web_rbac:assert_no_users_upgrade(),
    Params = mochiweb_request:parse_post(Req),
    Name = proplists:get_value("name", Params),
    Ctx = init_bucket_validation_context(true, Name, Req),

    case do_bucket_create(Req, Name, Params, Ctx) of
        ok ->
            respond_bucket_created(Req, PoolId, Name);
        {Struct, Code} ->
            ns_server_stats:notify_counter({<<"rest_request_failure">>,
                                            [{type, bucket_create},
                                             {code, Code}]}),
            reply_json(Req, Struct, Code)
    end.

assert_pause_resume_api_enabled() ->
    menelaus_util:assert_config_profile_flag(enable_pause_resume).

check_remote_path("s3://" ++ _REST = _RemotePath) ->
    ok.

validate_remote_path(Name, State) ->
    validator:validate(
      fun (Value) ->
              try check_remote_path(Value)
              catch _:_ -> {error, "Invalid remote path"}
              end
      end, Name, State).

validators_start_hibernation() ->
    [validator:required(bucket, _), validator:string(bucket, _),
     validator:required(remote_path, _), validator:string(remote_path, _),
     validate_remote_path(remote_path, _),
     validator:required(blob_storage_region, _),
     validator:string(blob_storage_region, _),
     %% rate_limit is expressed in bytes/sec.
     validator:required(rate_limit, _),
     validator:integer(rate_limit, 1024, 250 * ?MIB, _),
     validator:no_duplicates(_),
     validator:unsupported(_)].

validators_stop_hibernation() ->
    [validator:required(bucket, _), validator:string(bucket, _),
     validator:no_duplicates(_),
     validator:unsupported(_)].

handle_hibernation_response(Req, ok = _Response) ->
    menelaus_util:reply_json(Req, [], 200);
handle_hibernation_response(Req, {need_more_space, Zones} = _Response) ->
    reply_json(Req, {[{error, need_more_space_error(Zones)}]}, 503);
handle_hibernation_response(Req, Response) ->
    reply_json(Req, {[{error, Response}]}, 400).

process_req(Req, HandlerFunc, Validators) ->
    validator:handle(fun (Params) ->
                             handle_hibernation_response(Req,
                                                         HandlerFunc(Params))
                     end, Req, json, Validators()).

handle_hibernation_request(Req, Func, Validators) ->
    assert_pause_resume_api_enabled(),
    process_req(Req, Func, Validators).

handle_start_hibernation(Req, StartFunc) ->
    handle_hibernation_request(
      Req, fun(Params) ->
                   Bucket = proplists:get_value(bucket, Params),
                   RemotePath = proplists:get_value(remote_path, Params),
                   BlobStorageRegion =
                       proplists:get_value(blob_storage_region, Params),
                   RateLimit =
                       proplists:get_value(rate_limit, Params),
                   StartFunc(#bucket_hibernation_op_args{
                               bucket = Bucket,
                               remote_path = RemotePath,
                               blob_storage_region = BlobStorageRegion,
                               rate_limit = RateLimit})
           end, fun validators_start_hibernation/0).

handle_start_pause(Req) ->
    handle_start_hibernation(Req,
                             fun ns_orchestrator:start_pause_bucket/1).
handle_start_resume(Req) ->
    handle_start_hibernation(
      Req, fun(Args) ->
                   case hibernation_utils:get_metadata_from_s3(Args) of
                       {ok, Metadata} ->
                           ns_orchestrator:start_resume_bucket(Args, Metadata);
                       {error, Error} ->
                           Error
                   end
           end).

handle_stop_hibernation(Req, StopFunc) ->
    handle_hibernation_request(
      Req, fun(Params) ->
                   Bucket = proplists:get_value(bucket, Params),
                   StopFunc(Bucket)
           end, fun validators_stop_hibernation/0).

handle_stop_pause(Req) ->
    handle_stop_hibernation(Req, fun ns_orchestrator:stop_pause_bucket/1).

handle_stop_resume(Req) ->
    handle_stop_hibernation(Req, fun ns_orchestrator:stop_resume_bucket/1).

perform_warnings_validation(Ctx, ParsedProps, Errors) ->
    Errors ++
        num_replicas_warnings_validation(Ctx, proplists:get_value(num_replicas, ParsedProps)).

num_replicas_warnings_validation(_Ctx, undefined) ->
    [];
num_replicas_warnings_validation(Ctx, NReplicas) ->
    Warnings =
        if
            NReplicas > Ctx#bv_ctx.max_replicas ->
                ["you do not have enough data servers or "
                 "server groups to support this number of replicas"];
            true ->
                []
        end ++
        case get_existing_bucket_config(num_replicas, Ctx) of
            undefined -> [];
            NReplicas -> [];
            _ -> ["changing replica number may require rebalance"]
        end,
    Msg = case Warnings of
              [] ->
                  [];
              [A] ->
                  A;
              [B, C] ->
                  B ++ " and " ++ C
          end,
    case Msg of
        [] ->
            [];
        _ ->
            [{replicaNumber, ?l2b("Warning: " ++ Msg ++ ".")}]
    end.

get_existing_bucket_config(Param,
                           #bv_ctx{new = false, bucket_config = BucketConfig})
  when BucketConfig =/= false ->
    proplists:get_value(Param, BucketConfig);
get_existing_bucket_config(_, _) ->
    undefined.

handle_bucket_flush(_PoolId, Id, Req) ->
    XDCRDocs = goxdcr_rest:find_all_replication_docs(),
    case lists:any(
           fun (PList) ->
                   erlang:binary_to_list(proplists:get_value(source, PList)) =:= Id
           end, XDCRDocs) of
        false ->
            do_handle_bucket_flush(Id, Req);
        true ->
            reply_json(Req, {[{'_',
                               <<"Cannot flush buckets with outgoing XDCR">>}]},
                       503)
    end.

do_handle_bucket_flush(BucketName, Req) ->
    case ns_orchestrator:flush_bucket(BucketName) of
        ok ->
            ns_audit:flush_bucket(Req, BucketName),
            reply(Req, 200);
        bucket_not_found ->
            reply(Req, 404);
        flush_disabled ->
            reply_json(Req, {[{'_',
                               <<"Flush is disabled for the bucket">>}]}, 400);
        {flush_wait_failed, _, _} ->
            reply_json(Req, {[{'_',
                               <<"Flush failed or timed out">>}]}, 504);
        Other ->
            case menelaus_web_cluster:busy_reply("flush bucket", Other) of
                {Code, Msg} ->
                    reply_json(Req, {[{'_', Msg}]}, Code);
                undefined ->
                    exit(Other)
            end
    end.


-record(ram_summary, {
          total,                                % total cluster quota
          other_buckets,
          per_node,                             % per node quota of this bucket
          nodes_count,                          % node count of this bucket
          this_alloc,
          this_used,                            % part of this bucket which is used already
          free}).                               % total - other_buckets - this_alloc.
                                                % So it's: Amount of cluster quota available for allocation

-record(hdd_summary, {
          total,                                % total cluster disk space
          other_data,                           % disk space used by something other than our data
          other_buckets,                        % space used for other buckets
          this_used,                            % space already used by this bucket
          free}).                               % total - other_data - other_buckets - this_alloc
                                                % So it's kind of: Amount of cluster disk space available of allocation,
                                                % but with a number of 'but's.

parse_bucket_params(Ctx, Params) ->
    RV = parse_bucket_params_without_warnings(Ctx, Params),
    case {Ctx#bv_ctx.ignore_warnings, RV} of
        {_, {ok, _, _} = X} -> X;
        {false, {errors, Errors, Summaries, OKs}} ->
            {errors, perform_warnings_validation(Ctx, OKs, Errors), Summaries};
        {true, {errors, Errors, Summaries, _}} ->
            {errors, Errors, Summaries}
    end.

parse_bucket_params_without_warnings(Ctx, Params0) ->
    SkipEncryptionKeyTest =
        proplists:get_value("skipEncryptionKeyTest", Params0) =:= "1",
    Params = proplists:delete("skipEncryptionKeyTest", Params0),
    {OKs, Errors} = basic_bucket_params_screening(Ctx ,Params),
    IsNew = Ctx#bv_ctx.new,
    CurrentBucket = proplists:get_value(currentBucket, OKs),

    {RAMErrors, JSONSummaries} =
        process_ram_and_storage(Ctx, CurrentBucket, OKs),

    EKErrors = test_encryption_keys(Ctx, OKs, SkipEncryptionKeyTest),

    case RAMErrors ++ Errors ++ EKErrors ++
        validate_bucket_type(CurrentBucket, IsNew, OKs) of
        [] ->
            {ok, OKs, JSONSummaries};
        TotalErrors ->
            {errors, TotalErrors, JSONSummaries, OKs}
    end.

test_encryption_keys(_, _, true) ->
    [];
test_encryption_keys(#bv_ctx{validate_only = true}, _OKs, _) ->
    %% Testing of encryption keys can be slow, so we don't want to do it
    %% unless we are actually saving the bucket.
    [];
test_encryption_keys(#bv_ctx{bucket_config = BucketConfig} = Ctx, OKs, _) ->
    IdToTest = case proplists:get_value(encryption_secret_id, OKs) of
                   undefined -> undefined;
                   ?SECRET_ID_NOT_SET -> undefined;
                   SecretId when BucketConfig =:= false -> SecretId;
                   SecretId ->
                       case proplists:get_value(encryption_secret_id,
                                                BucketConfig) of
                           % same secret id, no need to test
                           SecretId -> undefined;
                           _ -> SecretId
                       end
               end,
    case IdToTest of
        undefined -> [];
        _ ->
            Nodes = ns_node_disco:only_live_nodes(get_nodes(Ctx)),
            case cb_cluster_secrets:test_existing_secret(IdToTest, Nodes) of
                ok -> [];
                {error, Error} ->
                    ErrorMsg = menelaus_web_secrets:format_error(Error),
                    [{encryptionAtRestKeyId, list_to_binary(ErrorMsg)}]
            end
    end.

validate_bucket_type(undefined, _IsNew, _Props) ->
    [];
validate_bucket_type(_, true, _Props) ->
    [];
validate_bucket_type(CurrentBucket, false, Props) ->
    CurrentType = ns_bucket:bucket_type(CurrentBucket),
    case proplists:get_value(bucketType, Props) of
        undefined ->
            [];
        CurrentType ->
            [];
        _ ->
            [{bucketType, <<"Cannot change bucket type.">>}]
    end.

process_ram_and_storage(Ctx, CurrentBucket, ParsedProps) ->
    PropsToCheck = case CurrentBucket of
                       undefined ->
                           ParsedProps;
                       _ ->
                           CurrentBucket
                   end,
    process_ram_and_storage(Ctx, CurrentBucket, ParsedProps,
                            proplists:is_defined(width, PropsToCheck)).

process_ram_and_storage(_Ctx, _CurrentBucket, _ParsedProps, true) ->
    {[], undefined};
process_ram_and_storage(Ctx, CurrentBucket, ParsedProps, false) ->
    ClusterStorageTotals = Ctx#bv_ctx.cluster_storage_totals,

    Props = case proplists:is_defined(ram_quota, ParsedProps) of
                false ->
                    [{ram_quota, 0} | ParsedProps];
                true ->
                    ParsedProps
            end,

    RAMSummary =
        interpret_ram_quota(Ctx, CurrentBucket, Props, ClusterStorageTotals),

    HDDSummary =
        interpret_hdd_quota(CurrentBucket, Props, ClusterStorageTotals, Ctx),

    JSONSummaries = [{ramSummary, {ram_summary_to_proplist(RAMSummary)}},
                     {hddSummary, {hdd_summary_to_proplist(HDDSummary)}}],

    {validate_ram(RAMSummary), JSONSummaries}.

validate_ram(#ram_summary{free = Free}) when Free < 0 ->
    [{ramQuota, <<"RAM quota specified is too large to be provisioned into "
                  "this cluster.">>}];
validate_ram(#ram_summary{this_alloc = Alloc, this_used = Used}) ->
    %% All buckets should have the same quota, but only since 7.6 has
    %% memcached supported a graceful quota reduction to values below the
    %% current RAM usage. As such, we should keep the existing check against
    %% RAM usage for mixed mode clusters as setting a quota below RAM usage
    %% on any pre-7.6 memcached will result in a period of temporary
    %% failures while memory is reduced.
    case cluster_compat_mode:is_cluster_76() of
        false when Alloc < Used ->
            [{ramQuota, <<"RAM quota cannot be set below current usage.">>}];
        _ ->
            []
    end.

additional_bucket_params_validation(Params, Ctx) ->
    BypassAddnlEncrChecks =
        menelaus_web_encr_at_rest:bypass_encr_cfg_restrictions(),
    lists:append([maybe_validate_replicas_and_durability(Params, Ctx),
                  validate_magma_ram_quota(Params, Ctx),
                  validate_watermarks(Params, Ctx),
                  validate_encr_lifetime_and_rotation_intrvl(
                    Params, Ctx, BypassAddnlEncrChecks)]).

maybe_validate_replicas_and_durability(Params, Ctx) ->
    %% MB-63888: When we fail over a node we set servers in the Bucket config
    %% to the post-failover servers. We're using that config whenever we call
    %% get_nodes(Ctx) and the bucket already exists. If we have configured
    %% durability_min_level in the past, have failed over enough nodes that only
    %% one KV node remains, and attempt to update any other bucket config then
    %% we would fail with a not enough servers error. This isn't great, so we're
    %% going to skip this check unless the user tries to change
    %% durability_min_level or num_replicas.
    DurabilityLevelParams = proplists:get_value(durability_min_level, Params),
    NumReplicasParams = proplists:get_value(num_replicas, Params),

    DurabilityLevelConfigured = get_existing_bucket_config(durability_min_level,
                                                           Ctx),
    NumReplicasConfigured = get_existing_bucket_config(num_replicas, Ctx),
    case (DurabilityLevelParams =:= undefined orelse
          DurabilityLevelParams =:= DurabilityLevelConfigured) andalso
         (NumReplicasParams =:= undefined orelse
          NumReplicasParams =:= NumReplicasConfigured) of
        true -> [];
        false ->
            validate_replicas_and_durability(Params, Ctx)
    end.

validate_replicas_and_durability(Params, Ctx) ->
    NumReplicas = get_value_from_parms_or_bucket(num_replicas, Params, Ctx),
    DurabilityLevel = get_value_from_parms_or_bucket(durability_min_level,
                                                     Params, Ctx),
    Nodes = get_nodes(Ctx),
    NodesCount = length(Nodes),
    case {NumReplicas, DurabilityLevel, NodesCount} of
        {0, _, _} -> [];
        {_, none, _} -> [];
        {3, _, _} -> [{durability_min_level,
                       <<"Durability minimum level cannot be specified with "
                         "3 replicas">>}];
        %% memcached bucket
        {undefined, undefined, _} -> [];
        {_, _, 1} -> [{durability_min_level,
                       <<"You do not have enough data servers to support this "
                         "durability level">>}];
        {_, _, _} -> []
    end ++
        case get_existing_bucket_config(num_replicas, Ctx) of
            undefined ->
                [];
            NumReplicas ->
                [];
            OldNumReplicas ->
                case guardrail_monitor:check_num_replicas_change(
                       OldNumReplicas, NumReplicas, Nodes) of
                    ok ->
                        [];
                    {error, Error} when is_binary(Error) ->
                        [{num_replicas, Error}]
                end
        end.

validate_magma_ram_quota(Params, Ctx) ->
    StorageMode = get_value_from_parms_or_bucket(storage_mode, Params, Ctx),
    RamQuota = get_value_from_parms_or_bucket(ram_quota, Params, Ctx),
    NumVBuckets = get_value_from_parms_or_bucket(num_vbuckets, Params, Ctx),

    %% We fetch the magma min memory quota from the config profile, then
    %% possibly override it with the value in ns_config, as this is the way
    %% QE currently sets the value for testing purposes
    DefaultMemoryQuota =
        case NumVBuckets =:= ?MAX_NUM_VBUCKETS of
            true ->
                ?DEFAULT_MAGMA_MIN_MEMORY_QUOTA_1024_VBS;
            false ->
                ?DEFAULT_MAGMA_MIN_MEMORY_QUOTA_128_VBS
        end,

    DefaultMagmaMinMemoryQuota =
        config_profile:get_value({magma, min_memory_quota},
                                 DefaultMemoryQuota),
    MagmaMinMemoryQuota =
        ns_config:read_key_fast(magma_min_memory_quota,
                                DefaultMagmaMinMemoryQuota),

    case {StorageMode, RamQuota} of
        {magma, RamQuota}
          when RamQuota < MagmaMinMemoryQuota * 1024 * 1024 ->
            RamQ = list_to_binary(integer_to_list(MagmaMinMemoryQuota)),
            [{ramQuota,
              <<"Ram quota for magma must be at least ", RamQ/binary,
                " MiB">>}];
        {_, _} ->
            []
    end.

%% Validate the relationship between the low and high watermarks.
validate_watermarks(Params, Ctx) ->
    validate_high_low_values(Params, Ctx,
                             memory_low_watermark,
                             memoryLowWatermark,
                             memory_high_watermark,
                             memoryHighWatermark,
                             less_than).

validate_lifetime_with_rotation_intrvl(undefined, _CurrRotIntrvl, _MaxDeks) ->
    [];
validate_lifetime_with_rotation_intrvl(0 = _ParamLifeTime, 0 = _CurrRotIntrvl,
                                       _MaxDeks) ->
    [];
validate_lifetime_with_rotation_intrvl(0 = _ParamLifeTime, _CurrRotIntrvl,
                                       _MaxDeks) ->
    [{encryptionAtRestDekLifetime,
      <<"DEK lifetime can't be set to 0 if DEK rotation interval is not "
        "currently 0">>}];
validate_lifetime_with_rotation_intrvl(_ParamLifeTime, 0 = _CurrRotIntrvl,
                                       _MaxDeks) ->
    [{encryptionAtRestDekLifetime,
      <<"DEK lifetime must be set to 0 if DEK rotation interval is "
        "currently 0">>}];
validate_lifetime_with_rotation_intrvl(ParamLifeTime, CurrRotIntrvl, _MaxDeks)
  when ParamLifeTime < CurrRotIntrvl + ?DEK_LIFETIME_ROTATION_MARGIN_SEC ->
    Err =
        io_lib:format("DEK lifetime must be at least ~p seconds more than the "
                      "current DEK rotation interval value of ~p",
                      [?DEK_LIFETIME_ROTATION_MARGIN_SEC, CurrRotIntrvl]),
    [{encryptionAtRestDekLifetime, list_to_binary(Err)}];
validate_lifetime_with_rotation_intrvl(ParamLifeTime, CurrRotIntrvl, MaxDeks)
  when ParamLifeTime > MaxDeks * CurrRotIntrvl ->
    Err = io_lib:format("Must be less than dekRotationInterval * max DEKs (~b)",
                        [MaxDeks]),
    [{encryptionAtRestDekLifetime, list_to_binary(Err)}];
validate_lifetime_with_rotation_intrvl(_ParamLifeTime, _CurrRotIntrvl,
                                       _MaxDeks) ->
    [].

validate_rotation_intrvl_with_lifetime(undefined, _CurrLifeTime, _MaxDeks) ->
    [];
validate_rotation_intrvl_with_lifetime(0 = _ParamRotIntrvl, 0 = _CurrLifeTime,
                                       _MaxDeks) ->
    [];
validate_rotation_intrvl_with_lifetime(_ParamRotIntrvl, 0 = _CurrLifeTime,
                                       _MaxDeks) ->
    [{encryptionAtRestDekRotationInterval,
      <<"DEK rotation interval must be set to 0 if DEK lifetime is "
        "currently 0">>}];
validate_rotation_intrvl_with_lifetime(0 = _ParamRotIntrvl, _CurrLifeTime,
                                       _MaxDeks) ->
    [{encryptionAtRestDekRotationInterval,
      <<"DEK rotation interval can't be set to 0 if DEK lifetime is not "
        "currently 0">>}];
validate_rotation_intrvl_with_lifetime(ParamRotIntrvl, CurrLifeTime, _MaxDeks)
  when CurrLifeTime < ParamRotIntrvl + ?DEK_LIFETIME_ROTATION_MARGIN_SEC ->
    Err =
        io_lib:format("DEK rotation interval must be at least ~p seconds less "
                      "than the current DEK lifetime value of ~p",
                      [?DEK_LIFETIME_ROTATION_MARGIN_SEC,
                       CurrLifeTime]),
    [{encryptionAtRestDekRotationInterval, list_to_binary(Err)}];
validate_rotation_intrvl_with_lifetime(ParamRotIntrvl, CurrLifeTime, MaxDeks)
  when CurrLifeTime > MaxDeks * ParamRotIntrvl ->
    Err = io_lib:format("Must be greater than dekLifetime / max DEKs (~b)",
                        [MaxDeks]),
    [{encryptionAtRestDekRotationInterval, list_to_binary(Err)}];
validate_rotation_intrvl_with_lifetime(_ParamRotIntrvl, _CurrLifeTime,
                                       _MaxDeks) ->
    [].

validate_encr_lifetime_and_rotation_intrvl(_, _, true = _Bypass) ->
    [];
validate_encr_lifetime_and_rotation_intrvl(Params, Ctx, false = _Bypass) ->
    ParamLifeTime =
        proplists:get_value(encryption_dek_lifetime, Params),
    ParamRotIntrvl =
        proplists:get_value(encryption_dek_rotation_interval, Params),

    CurrLifeTime =
        case get_value_from_parms_or_bucket(encryption_dek_lifetime,
                                            Params, Ctx) of
            undefined ->
                ?DEFAULT_DEK_LIFETIME_S;
            LifeTimeVal ->
                LifeTimeVal
        end,
    CurrRotIntrvl =
        case get_value_from_parms_or_bucket(encryption_dek_rotation_interval,
                                            Params, Ctx) of
            undefined ->
                ?DEFAULT_DEK_ROTATION_INTERVAL_S;
            RotIntrvlVal ->
                RotIntrvlVal
        end,
    DekKind = case Ctx#bv_ctx.bucket_uuid of
                  not_present -> {bucketDek, <<>>};
                  UUID -> {bucketDek, UUID}
              end,
    MaxDeks = cb_cluster_secrets:max_dek_num(DekKind),
    validate_lifetime_with_rotation_intrvl(ParamLifeTime, CurrRotIntrvl,
                                           MaxDeks) ++
        validate_rotation_intrvl_with_lifetime(ParamRotIntrvl, CurrLifeTime,
                                               MaxDeks).

validate_high_low_values(Params, Ctx, LowParam, LowParamExtName,
                         HighParam, HighParamExtName, Check) ->
    Low = get_value_from_parms_or_bucket(LowParam, Params, Ctx),
    High = get_value_from_parms_or_bucket(HighParam, Params, Ctx),
    case {Low, High} of
        {undefined, undefined} ->
            [];
        {undefined, _} ->
            %% Low param was found to have an error during parsing and
            %% validation. But the high param is valid so we get into
            %% this relationship validation code. The error is already
            %% queued up to be returned to the user.
            [];
        {_, undefined} ->
            %% Same as above except the high param is valid and the
            %% low param has an error queued up to be returned to the
            %% user.
            [];
        {Low, High} when Check =:= less_than andalso Low >= High ->
            Msg = io_lib:format("~p (~p) must be less than ~p (~p)",
                                [LowParamExtName, Low, HighParamExtName, High]),
            [{LowParamExtName,
              list_to_binary(Msg)}];
        {_, _} ->
            []
    end.

%% Get the value from the params. If it wasn't specified and this isn't
%% a bucket creation then get the existing value from the bucket config.
get_value_from_parms_or_bucket(Key, Params,
                               #bv_ctx{bucket_config = BucketConfig,
                                       new = IsNew}) ->
    case proplists:get_value(Key, Params) of
        undefined ->
            case IsNew of
                true -> undefined;
                false -> proplists:get_value(Key, BucketConfig, undefined)
            end;
        Value -> Value
    end.

basic_bucket_params_screening(#bv_ctx{bucket_config = false, new = false}, _Params) ->
    {[], [{name, <<"Bucket with given name doesn't exist">>}]};
basic_bucket_params_screening(Ctx, Params) ->
    CommonParams = validate_common_params(Ctx, Params),
    TypeSpecificParams =
        validate_bucket_type_specific_params(CommonParams, Params, Ctx),
    Candidates = CommonParams ++ TypeSpecificParams,
    assert_candidates(Candidates),
    %% Basic parameter checking has been done. Take the non-error key/values
    %% and do additional checking (e.g. relationships between different
    %% keys).
    OKs = [{K, V} || {ok, K, V} <- Candidates],
    Errors =  [{K, V} || {error, K, V} <- Candidates],
    AdditionalErrors = additional_bucket_params_validation(OKs, Ctx),
    {OKs, Errors ++ AdditionalErrors}.

validate_common_params(#bv_ctx{bucket_name = BucketName,
                               bucket_config = BucketConfig, new = IsNew,
                               all_buckets = AllBuckets}, Params) ->
    Is76 = cluster_compat_mode:is_cluster_76(),
    IsEnterprise = cluster_compat_mode:is_enterprise(),

    [{ok, name, BucketName},
     parse_validate_flush_enabled(Params, IsNew),
     parse_validate_cross_cluster_versioning_enabled(Params, IsNew,
                                                     Is76, IsEnterprise,
                                                     BucketConfig),
     parse_validate_version_pruning_window(Params, IsNew,
                                           Is76, IsEnterprise),
     validate_bucket_name(IsNew, BucketConfig, BucketName, AllBuckets),
     parse_validate_ram_quota(Params, BucketConfig)].

validate_bucket_placer_params(Params, IsNew, BucketConfig) ->
    case bucket_placer:is_enabled() of
        true ->
            [parse_validate_bucket_placer_param(width, weight, 1,
                                                Params, IsNew, BucketConfig),
             parse_validate_bucket_placer_param(weight, width, 0,
                                                Params, IsNew, BucketConfig)];
        false ->
            []
    end.

validate_bucket_type_specific_params(CommonParams, Params,
                                     #bv_ctx{new = IsNew,
                                             bucket_name = Name,
                                             bucket_config = BucketConfig,
                                             cluster_version = Version,
                                             is_enterprise = IsEnterprise}) ->
    BucketType = get_bucket_type(IsNew, BucketConfig, Params),

    case BucketType of
        memcached ->
            %% Remove in major release after Morpheus.
            [{error, bucketType,
              <<"memcached buckets are no longer supported">>}];
        membase ->
            validate_membase_bucket_params(CommonParams, Params, Name, IsNew,
                                           BucketConfig, Version, IsEnterprise);
        _ ->
            validate_unknown_bucket_params(Params)
    end.

validate_membase_bucket_params(CommonParams, Params, Name,
                               IsNew, BucketConfig, Version, IsEnterprise) ->
    AllowStorageLimit = config_profile:get_bool(enable_storage_limits),
    AllowThrottleLimit = config_profile:get_bool(enable_throttle_limits),
    ReplicasNumResult = validate_replicas_number(Params, IsNew),
    IsStorageModeMigration = is_storage_mode_migration(
                               IsNew, BucketConfig, Params),
    Is79 = cluster_compat_mode:is_version_79(Version),
    IsPersistent = is_ephemeral(Params, BucketConfig, IsNew) =:= false,

    HistRetSecs = parse_validate_history_retention_seconds(
                    Params, BucketConfig, IsNew, Version, IsEnterprise,
                    IsStorageModeMigration),
    BucketParams =
        [{ok, bucketType, membase},
         ReplicasNumResult,
         parse_validate_max_magma_shards(Params, BucketConfig, Version, IsNew),
         parse_validate_replica_index(Params, ReplicasNumResult, IsNew),
         parse_validate_num_vbuckets(Params, BucketConfig, IsNew, IsPersistent),
         parse_validate_threads_number(Params, IsNew),
         parse_validate_eviction_policy(
           Params, BucketConfig, IsNew, IsStorageModeMigration),
         quota_size_error(CommonParams, Name),
         parse_validate_storage_mode(Params, BucketConfig, IsNew, Version,
                                     IsEnterprise, IsStorageModeMigration,
                                     config_profile:is_serverless()),
         parse_validate_durability_min_level(Params, BucketConfig, IsNew),
         parse_validate_durability_impossible_fallback(Params, IsNew,
                                                       Is79),
         parse_validate_warmup_behavior(Params, IsNew, Is79),
         parse_validate_access_scanner_enabled(Params, IsNew, Is79,
                                               IsPersistent),
         parse_validate_expiry_pager_sleep_time(Params, IsNew, Is79),
         parse_validate_memory_low_watermark(Params, IsNew, Is79),
         parse_validate_memory_high_watermark(Params, IsNew, Is79),
         parse_validate_continuous_backup_enabled(Params, BucketConfig, IsNew,
                                                  Is79,
                                                  IsStorageModeMigration),
         parse_validate_continuous_backup_interval(Params, BucketConfig, IsNew,
                                                   Is79,
                                                   IsStorageModeMigration),
         parse_validate_continuous_backup_location(Params, BucketConfig, IsNew,
                                                   Is79,
                                                   IsStorageModeMigration),
         parse_validate_invalid_hlc_strategy(Params, IsNew, Is79),
         parse_validate_hlc_max_future_threshold(Params, IsNew, Is79),
         parse_validate_storage_quota_percentage(
           Params, BucketConfig, IsNew, IsEnterprise,
           IsStorageModeMigration),
         parse_validate_max_ttl(Params, BucketConfig, IsNew, IsEnterprise),
         parse_validate_bucket_rank(Params, IsNew),
         parse_validate_compression_mode(Params, BucketConfig, IsNew,
                                         IsEnterprise),
         HistRetSecs,
         parse_validate_history_retention_bytes(
           Params, BucketConfig, IsNew, Version, IsEnterprise,
           IsStorageModeMigration),
         parse_validate_history_retention_collection_default(
           Params, BucketConfig, IsNew, Version, IsEnterprise,
           IsStorageModeMigration),
         parse_validate_magma_key_tree_data_blocksize(Params, BucketConfig,
                                                      Version, IsNew,
                                                      IsEnterprise,
                                                      IsStorageModeMigration),
         parse_validate_magma_seq_tree_data_blocksize(Params, BucketConfig,
                                                      Version, IsNew,
                                                      IsEnterprise,
                                                      IsStorageModeMigration),
         parse_validate_dcp_connections_between_nodes(Params, IsNew, Is79,
                                                      IsEnterprise),
         parse_validate_fusion_logstore_uri(
           Params, IsNew, Is79, IsEnterprise),
         parse_validate_dcp_backfill_idle_protection_enabled(Params,
                                                             BucketConfig,
                                                             IsNew,
                                                             Is79),
         parse_validate_dcp_backfill_idle_limit_seconds(Params, IsNew,
                                                        Is79),
         parse_validate_dcp_backfill_idle_disk_threshold(Params, IsNew,
                                                         Is79),
         parse_validate_workload_pattern_default(Params)
        | validate_bucket_auto_compaction_settings(Params)] ++
        parse_validate_limits(
          Params, BucketConfig, IsNew, AllowStorageLimit,
          fun menelaus_web_settings:get_storage_limit_attributes/0) ++
        parse_validate_limits(
          Params, BucketConfig, IsNew, AllowThrottleLimit,
          fun menelaus_web_settings:get_throttle_limit_attributes/0) ++
        validate_bucket_encryption_at_rest_settings(Name, Params, Version,
                                                    IsEnterprise, IsPersistent),

    validate_bucket_purge_interval(Params, BucketConfig, IsNew) ++
        get_conflict_resolution_type_and_thresholds(
          Params, HistRetSecs, BucketConfig, IsNew) ++
        validate_bucket_placer_params(Params, IsNew, BucketConfig) ++
        BucketParams.

parse_validate_hlc_max_future_threshold(Params, _IsNew, false = _Is79) ->
    parse_validate_param_not_supported(
      "hlcMaxFutureThreshold", Params,
      fun not_supported_until_79_error/1);
parse_validate_hlc_max_future_threshold(Params, IsNew, true = _Is79) ->
    parse_validate_numeric_param(Params, hlcMaxFutureThreshold,
                                 hlc_max_future_threshold, IsNew).

parse_validate_invalid_hlc_strategy(Params, _IsNew, false = _Is79) ->
    parse_validate_param_not_supported(
      "invalidHlcStrategy", Params,
      fun not_supported_until_79_error/1);
parse_validate_invalid_hlc_strategy(Params, IsNew, _Is79) ->
    parse_validate_one_of(Params, invalidHlcStrategy, invalid_hlc_strategy,
                          IsNew, ["error", "ignore", "replace"]).

parse_validate_one_of(Params, Param, ConfigKey, IsNew, ValidValues) ->
    Value = proplists:get_value(atom_to_list(Param), Params),
    case {Value, IsNew} of
        {undefined, true} ->
            %% The value wasn't supplied and we're creating a bucket:
            %% use the default value.
            {ok, ConfigKey, ns_bucket:attribute_default(ConfigKey)};
        {undefined, false} ->
            %% The value wasn't supplied and we're modifying a bucket:
            %% don't complain since the value was either specified or a
            %% default used when the bucket was created.
            ignore;
        {_, _} ->
            validate_one_of(Value, Param, ConfigKey, ValidValues)
    end.

validate_one_of(Value, Param, ConfigKey, ValidValues) ->
    case lists:member(Value, ValidValues) of
        true ->
            {ok, ConfigKey, list_to_atom(Value)};
        false ->
            AtomValues = lists:map(fun(V) -> list_to_atom(V) end, ValidValues),
            Msg = list_to_binary(
                    io_lib:format("Must be one of ~0p", [AtomValues])),
            {error, Param, Msg}
    end.

parse_validate_continuous_backup_enabled(Params, BucketConfig, IsNew,
                                         Is79, IsStorageModeMigration) ->
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_continuous_backup_enabled_inner(Params, IsNew, Is79,
                                                   IsMagma).

parse_validate_continuous_backup_enabled_inner(Params, _IsNew, _Is79,
                                               false = _IsMagma) ->
    parse_validate_param_not_supported(
      "continuousBackupEnabled", Params, fun only_supported_on_magma/1);
parse_validate_continuous_backup_enabled_inner(Params, _IsNew,
                                               false = _Is79, _IsMagma) ->
    parse_validate_param_not_supported(
      "continuousBackupEnabled", Params,
      fun not_supported_until_79_error/1);
parse_validate_continuous_backup_enabled_inner(Params, IsNew,
                                               true = _Is79,
                                               true = _IsMagma) ->
    Result = menelaus_util:parse_validate_boolean_field(
               "continuousBackupEnabled", '_', Params),
    process_boolean_param_validation(continuousBackupEnabled,
                                     continuous_backup_enabled, Result, IsNew).

parse_validate_continuous_backup_interval(Params, BucketConfig, IsNew,
                                          Is79, IsStorageModeMigration) ->
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_continuous_backup_interval_inner(Params, IsNew, Is79,
                                                    IsMagma).

parse_validate_continuous_backup_interval_inner(Params, _IsNew, _Is79,
                                                false = _IsMagma) ->
    parse_validate_param_not_supported(
      "continuousBackupInterval", Params, fun only_supported_on_magma/1);
parse_validate_continuous_backup_interval_inner(Params, _IsNew,
                                                false = _Is79,
                                                _IsMagma) ->
    parse_validate_param_not_supported(
      "continuousBackupInterval", Params,
      fun not_supported_until_79_error/1);
parse_validate_continuous_backup_interval_inner(Params, IsNew,
                                                true = _Is79,
                                                true = _IsMagma) ->
    parse_validate_numeric_param(Params, continuousBackupInterval,
                                 continuous_backup_interval, IsNew).

parse_validate_continuous_backup_location(Params, BucketConfig, IsNew,
                                          Is79, IsStorageModeMigration) ->
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_continuous_backup_location_inner(Params, IsNew, Is79,
                                                    IsMagma).

parse_validate_continuous_backup_location_inner(Params, _IsNew, _Is79,
                                                false = _IsMagma) ->
    parse_validate_param_not_supported(
      "continuousBackupLocation", Params, fun only_supported_on_magma/1);
parse_validate_continuous_backup_location_inner(Params, _IsNew,
                                                false = _Is79,
                                                _IsMagma) ->
    parse_validate_param_not_supported(
      "continuousBackupLocation", Params,
      fun not_supported_until_79_error/1);
parse_validate_continuous_backup_location_inner(Params, IsNew,
                                                true = _Is79,
                                                true = _IsMagma) ->
    parse_validate_path_or_uri(Params, continuousBackupLocation,
                               continuous_backup_location, IsNew).

parse_validate_path_or_uri(Params, Param, ConfigKey, IsNew) ->
    Value = proplists:get_value(atom_to_list(Param), Params),
    case {Value, IsNew} of
        {undefined, true} ->
            %% The value wasn't supplied and we're creating a bucket:
            %% use the default value.
            {ok, ConfigKey, ns_bucket:attribute_default(ConfigKey)};
        {undefined, false} ->
            %% The value wasn't supplied and we're modifying a bucket:
            %% don't complain since the value was either specified or a
            %% default used when the bucket was created.
            ignore;
        {_, _} ->
            validate_path_or_uri(Value, Param, ConfigKey)
    end.

validate_path_or_uri(Value, Param, ConfigKey) ->
    case is_valid_cloud_uri(Value) orelse is_writable_dir(Value) of
        true ->
            {ok, ConfigKey, Value};
        false ->
            {error, Param, <<"Must be a valid path or uri writable "
                             "by 'couchbase' user">>}
    end.

is_valid_cloud_uri(URI) ->
    misc:is_valid_uri(URI, ["s3", "az", "gs"]).

is_writable_dir(Dir) ->
    case misc:is_absolute_path(Dir) of
        true ->
            case misc:ensure_writable_dirs([Dir]) of
                ok ->
                    true;
                {error, _} ->
                    false
            end;
        false ->
            false
    end.

validate_unknown_bucket_params(Params) ->
    [{error, bucketType, <<"invalid bucket type">>}
     | validate_bucket_auto_compaction_settings(Params)].

parse_validate_flush_enabled(Params, IsNew) ->
    validate_with_missing(proplists:get_value("flushEnabled", Params),
                          "0", IsNew, fun parse_validate_flush_enabled/1).

validate_bucket_name(_IsNew, _BucketConfig, [] = _BucketName, _AllBuckets) ->
    {error, name, <<"Bucket name cannot be empty">>};
validate_bucket_name(_IsNew, _BucketConfig, undefined = _BucketName, _AllBuckets) ->
    {error, name, <<"Bucket name needs to be specified">>};
validate_bucket_name(_IsNew, _BucketConfig, BucketName, _AllBuckets)
  when length(BucketName) > ?MAX_BUCKET_NAME_LEN ->
    {error, name, ?l2b(io_lib:format("Bucket name cannot exceed ~p characters",
                                     [?MAX_BUCKET_NAME_LEN]))};
validate_bucket_name(true = _IsNew, _BucketConfig, BucketName, AllBuckets) ->
    case ns_bucket:is_valid_bucket_name(BucketName) of
        {error, invalid} ->
            {error, name,
             <<"Bucket name can only contain characters in range A-Z, a-z, 0-9 "
               "as well as underscore, period, dash & percent. Consult the documentation.">>};
        {error, reserved} ->
            {error, name, <<"This name is reserved for the internal use.">>};
        {error, starts_with_dot} ->
            {error, name, <<"Bucket name cannot start with dot.">>};
        _ ->
            %% we have to check for conflict here because we were looking
            %% for BucketConfig using case sensetive search (in basic_bucket_params_screening/4)
            %% but we do not allow buckets with the same names in a different register
            case ns_bucket:name_conflict(
                   BucketName, [N || {N, _} <- AllBuckets]) of
                false ->
                    ignore;
                _ ->
                    {error, name, <<"Bucket with given name already exists">>}
            end
    end;
validate_bucket_name(false = _IsNew, BucketConfig, _BucketName, _AllBuckets) ->
    true = (BucketConfig =/= false),
    {ok, currentBucket, BucketConfig}.

get_bucket_type(false = _IsNew, BucketConfig, _Params)
  when is_list(BucketConfig) ->
    ns_bucket:bucket_type(BucketConfig);
get_bucket_type(_IsNew, _BucketConfig, Params) ->
    case proplists:get_value("bucketType", Params) of
        %% Remove in major release after Morpheus.
        "memcached" -> memcached;
        "membase" -> membase;
        "couchbase" -> membase;
        "ephemeral" -> membase;
        undefined -> membase;
        _ -> invalid
    end.

quota_size_error(CommonParams, Name) ->
    case lists:keyfind(ram_quota, 2, CommonParams) of
        {ok, ram_quota, RAMQuota} ->
            NumCollections = collections:num_collections(Name, direct),
            MinQuotaForCollections =
                case {guardrail_monitor:get(collections_per_quota),
                      NumCollections} of
                    {undefined, _} ->
                        %% Guardrail disabled
                        0;
                    {0, _} ->
                        %% Guardrail disabled
                        0;
                    {_, undefined} ->
                        %% No collections so we don't need to check
                        0;
                    {ColPerQ, NumCollections} when ColPerQ > 0 ->
                        NumCollections / ColPerQ
                end,
            MinQuota = misc:get_env_default(membase_min_ram_quota, 100),
            MinQuotaBin = list_to_binary(integer_to_list(MinQuota)),
            Msg = <<"RAM quota cannot be less than ", MinQuotaBin/binary,
                    " MiB">>,
            if
                RAMQuota < MinQuota * ?MIB ->
                    {error, ramQuota, Msg};
                RAMQuota < MinQuotaForCollections * ?MIB ->
                    {error, ramQuota,
                     list_to_binary(
                       io_lib:format(
                         "RAM quota cannot be less than ~.1f MiB, to "
                         "support ~b collections", [MinQuotaForCollections,
                                                    NumCollections]))};
                true ->
                    ignore
            end;
        _ ->
            ignore
    end.

validate_bucket_purge_interval(Params, _BucketConfig, true = IsNew) ->
    BucketType = proplists:get_value("bucketType", Params, "membase"),
    parse_validate_bucket_purge_interval(Params, BucketType, IsNew);
validate_bucket_purge_interval(Params, BucketConfig, false = IsNew) ->
    BucketType = ns_bucket:external_bucket_type(BucketConfig),
    parse_validate_bucket_purge_interval(Params, atom_to_list(BucketType), IsNew).

parse_validate_max_magma_shards(Params, BucketConfig, _Version, false) ->
    Request = proplists:get_value("magmaMaxShards", Params),
    Current = case proplists:get_value(magma_max_shards, BucketConfig) of
                  Num when is_number(Num) ->
                      integer_to_list(Num);
                  _ ->
                      undefined
              end,
    case Request =:= Current of
        true ->
            ignore;
        false ->
            {error, magmaMaxShards,
             <<"Number of maximum magma shards cannot be modified after bucket creation">>}
    end;
parse_validate_max_magma_shards(Params, _BucketConfig, Version, true) ->
    case proplists:is_defined("magmaMaxShards", Params) of
        true ->
            case config_profile:search({magma, can_set_max_shards}, false) of
                false ->
                    {error, magmaMaxShards,
                     <<"Cannot set maximum magma shards in this configuration profile">>};
                true ->
                    case proplists:get_value("storageBackend", Params) =:= "magma" of
                        false ->
                            {error, magmaMaxShards,
                             <<"Cannot set maximum magma shards on non-magma storage backend">>};
                        true ->
                            case cluster_compat_mode:is_version_76(Version) of
                                false ->
                                    {error, magmaMaxShards,
                                     <<"Not allowed until entire cluster is upgraded to 7.6">>};
                                true ->
                                    parse_validate_max_magma_shards_inner(Params)
                            end
                    end
            end;
        false ->
            ignore
    end.

parse_validate_max_magma_shards_inner(Params) ->
    case proplists:get_value("bucketType", Params) =:= "ephemeral" of
        true ->
            {error, magmaMaxShards, <<"Not supported for ephemeral buckets">>};
        false ->
            RangeMsg = erlang:list_to_binary(
                         io_lib:format("Must be an integer between ~p and ~p",
                                       [?MIN_MAGMA_SHARDS, ?MAX_MAGMA_SHARDS])),
            case proplists:get_value("magmaMaxShards", Params) of
                N when is_list(N), length(N) > 0 ->
                    case (catch list_to_integer(N)) of
                        Num when is_integer(Num), Num >= ?MIN_MAGMA_SHARDS,
                                 Num =< ?MAX_MAGMA_SHARDS ->
                            {ok, magma_max_shards, Num};
                        _ ->
                            {error, magmaMaxShards, RangeMsg}
                    end;
                _ ->
                    {error, magmaMaxShards, RangeMsg}
            end
    end.

parse_validate_bucket_purge_interval(Params, "couchbase", IsNew) ->
    parse_validate_bucket_purge_interval(Params, "membase", IsNew);
parse_validate_bucket_purge_interval(Params, "membase", _IsNew) ->
    case menelaus_util:parse_validate_boolean_field("autoCompactionDefined", '_', Params) of
        [] -> [];
        [{error, _F, _V}] = Error -> Error;
        [{ok, _, false}] -> [{ok, purge_interval, undefined}];
        [{ok, _, true}] ->
            menelaus_web_autocompaction:parse_validate_purge_interval(Params,
                                                                      membase)
    end;
parse_validate_bucket_purge_interval(Params, "ephemeral", IsNew) ->
    case proplists:is_defined("autoCompactionDefined", Params) of
        true ->
            [{error, autoCompactionDefined,
              <<"autoCompactionDefined must not be set for ephemeral buckets">>}];
        false ->
            Val = menelaus_web_autocompaction:parse_validate_purge_interval(
                    Params, ephemeral),
            case Val =:= [] andalso IsNew =:= true of
                true ->
                    [{ok, purge_interval, ?DEFAULT_EPHEMERAL_PURGE_INTERVAL_DAYS}];
                false ->
                    Val
            end
    end.

validate_bucket_auto_compaction_settings(Params) ->
    case parse_validate_bucket_auto_compaction_settings(Params) of
        nothing ->
            [];
        false ->
            [{ok, autocompaction, false}];
        {errors, Errors} ->
            [{error, F, M} || {F, M} <- Errors];
        {ok, ACSettings} ->
            [{ok, autocompaction, ACSettings}]
    end.

parse_validate_bucket_auto_compaction_settings(Params) ->
    case menelaus_util:parse_validate_boolean_field("autoCompactionDefined", '_', Params) of
        [] -> nothing;
        [{error, F, V}] -> {errors, [{F, V}]};
        [{ok, _, false}] -> false;
        [{ok, _, true}] ->
            case menelaus_web_autocompaction:parse_validate_settings(Params, false) of
                {ok, AllFields, _} ->
                    {ok, AllFields};
                Error ->
                    Error
            end
    end.

validate_bucket_encryption_at_rest_settings(Name, Params, Version, IsEnterprise,
                                            IsPersistent) ->
    Allowed = cluster_compat_mode:is_version_79(Version),
    case parse_validate_encryption_secret_id(Name, Params) of
        [{ok, encryption_secret_id, Id}] when not IsEnterprise,
                                              Id /= ?SECRET_ID_NOT_SET ->
            [{error, encryptionAtRestKeyId,
              <<"Encryption At-Rest is not allowed in community edition">>}];
        [{ok, encryption_secret_id, Id}] when not Allowed,
                                              Id /= ?SECRET_ID_NOT_SET ->
            [{error, encryptionAtRestKeyId,
              <<"Encryption At-Rest is not allowed until the entire cluster "
                "is upgraded to 7.9">>}];
        [{ok, encryption_secret_id, Id}] when not IsPersistent,
                                              Id /= ?SECRET_ID_NOT_SET ->
            [{error, encryptionAtRestKeyId,
              <<"Encryption At-Rest is not supported for ephemeral buckets">>}];
        RV -> RV
    end ++
    parse_validate_encryption_rotation_interval(Params) ++
    parse_validate_encryption_dek_lifetime(Params).

parse_validate_encryption_secret_id(BucketName, Params) ->
    maybe
        [IdStr] ?= proplists:get_all_values("encryptionAtRestKeyId", Params),
        {ok, Id} ?= menelaus_util:parse_validate_number(IdStr, -1, undefined),
        %% It is validated later in transaction, but we have to validate it
        %% here as well to see this error when '?validate=1' is used
        ok ?= ns_bucket:validate_encryption_secret(Id, BucketName, direct),
        %% Secret existance is checked in bucket create/update transaction
        [{ok, encryption_secret_id, Id}]
    else
        {error, secret_not_found} ->
            [{error, encryptionAtRestKeyId,
              <<"Encryption key does not exist">>}];
        {error, secret_not_allowed} ->
            [{error, encryptionAtRestKeyId,
              <<"Encryption key can't encrypt this bucket">>}];
        [] ->
            [];
        [_ | _] ->
            [{error, encryptionAtRestKeyId, <<"too many values">>}];
        E when E == too_small; E == too_large; E == invalid ->
            [{error, encryptionAtRestKeyId, <<"invalid secret id">>}]
    end.

parse_validate_encryption_rotation_interval(Params) ->
    Min = menelaus_web_encr_at_rest:min_dek_rotation_interval_in_sec(),
    maybe
        [ValStr] ?= proplists:get_all_values(
                      "encryptionAtRestDekRotationInterval",
                      Params),
        {ok, Val} ?= menelaus_util:parse_validate_number(ValStr, 0, undefined),
        ok ?= case Val of
                  0 -> ok;
                  _ ->
                      case Min =< Val of
                          true -> ok;
                          false -> too_small
                      end
              end,
        [{ok, encryption_dek_rotation_interval, Val}]
    else
        [] ->
            [];
        [_ | _] ->
            [{error, encryptionAtRestDekRotationInterval,
              <<"too many values">>}];
        E when E == too_large; E == invalid ->
            [{error, encryptionAtRestDekRotationInterval,
              <<"invalid interval">>}];
        too_small ->
            [{error, encryptionAtRestDekRotationInterval,
              menelaus_web_encr_at_rest:dek_interval_error(Min)}]
    end.

parse_validate_encryption_dek_lifetime(Params) ->
    Min = menelaus_web_encr_at_rest:min_dek_lifetime_in_sec(),
    maybe
        [ValStr] ?= proplists:get_all_values(
                      "encryptionAtRestDekLifetime",
                      Params),
        {ok, Val} ?= menelaus_util:parse_validate_number(ValStr, 0, undefined),
        ok ?= case Val of
                  0 -> ok;
                  _ ->
                      case Min =< Val of
                          true -> ok;
                          false -> too_small
                      end
              end,
        [{ok, encryption_dek_lifetime, Val}]
    else
        [] ->
            [];
        [_ | _] ->
            [{error, encryptionAtRestDekLifetime,
              <<"too many values">>}];
        E when E == too_large; E == invalid ->
            [{error, encryptionAtRestDekLifetime,
              <<"invalid interval">>}];
        too_small ->
            [{error, encryptionAtRestDekLifetime,
              menelaus_web_encr_at_rest:dek_interval_error(Min)}]
    end.

validate_replicas_number(Params, IsNew) ->
    ValueInParams = proplists:get_value("replicaNumber", Params),
    ValueToCheck =
        case {IsNew, ValueInParams} of
            {true, undefined} ->
                %% When creating a bucket and number of replicas isn't
                %% specified, provide the default that will be used so
                %% validation against min_replicas_count can be done.
                %% Otherwise 'ignore' is used and validation doesn't occur.
                integer_to_list(?DEFAULT_MEMBASE_NUM_REPLICAS);
            {_, _} ->
                ValueInParams
        end,
    validate_with_missing(
      ValueToCheck,
      %% replicaNumber doesn't have
      %% default. Has to be given for
      %% creates, but may be omitted for
      %% updates. Later is for backwards
      %% compat, the former is from earlier
      %% code and stricter requirements is
      %% IMO ok to keep.
      undefined,
      IsNew,
      fun parse_validate_replicas_number/1).

is_ephemeral(Params, _BucketConfig, true = _IsNew) ->
    case proplists:get_value("bucketType", Params, "membase") of
        "membase" -> false;
        "couchbase" -> false;
        "ephemeral" -> true
    end;
is_ephemeral(_Params, BucketConfig, false = _IsNew) ->
    ns_bucket:is_ephemeral_bucket(BucketConfig).

%% The 'bucketType' parameter of the bucket create REST API will be set to
%% 'ephemeral' by user. As this type, in many ways, is similar in functionality
%% to 'membase' buckets we have decided to not store this as a new bucket type
%% atom but instead use 'membase' as type and rely on another config parameter
%% called 'storage_mode' to distinguish between membase and ephemeral buckets.
%% Ideally we should store this as a new bucket type but the bucket_type is
%% used/checked at multiple places and would need changes in all those places.
%% Hence the above described approach.
parse_validate_storage_mode(Params, _BucketConfig, true = _IsNew,
                            _Version, IsEnterprise,
                            false = _IsStorageModeMigration,
                            _ = _IsServerless) ->
    case proplists:get_value("bucketType", Params, "membase") of
        "membase" ->
            get_storage_mode_based_on_storage_backend(Params, IsEnterprise);
        "couchbase" ->
            get_storage_mode_based_on_storage_backend(Params, IsEnterprise);
        "ephemeral" ->
            {ok, storage_mode, ephemeral}
    end;
parse_validate_storage_mode(_Params, BucketConfig, false = _IsNew,
                            _Version, _IsEnterprise,
                            false = _IsStorageModeMigration,
                            _ = _IsServerless) ->
    {ok, storage_mode, ns_bucket:storage_mode(BucketConfig)};
parse_validate_storage_mode(_Params, _BucketConfig, false = _IsNew,
                            _Version, false = _IsEnterprise,
                            true = _IsStorageModeMigration,
                            _ = _IsServerless) ->
    {error, storageBackend, <<"Storage mode migration is allowed only on "
                              "enterprise edition">>};
parse_validate_storage_mode(_Params, _BucketConfig, false = _IsNew,
                            _Version, true = _IsEnterprise,
                            true = _IsStorageModeMigration,
                            true = _IsServerless) ->
    {error, storageBackend, <<"Storage mode migration is not allowed in "
                              "serverless config profile">>};
parse_validate_storage_mode(Params, _BucketConfig, false = _IsNew, Version,
                            true = _IsEnterprise,
                            true = _IsStorageModeMigration,
                            false = _IsServerless) ->
    case cluster_compat_mode:is_version_76(Version) of
        false ->
            {error, storageBackend,
             <<"Storage mode migration is not allowed until the entire cluster "
               "is upgraded to 7.6">>};
        true ->
            StorageBackend = proplists:get_value("storageBackend", Params),
            case do_get_storage_mode_based_on_storage_backend(StorageBackend) of
                {ok, storage_mode, Mode} ->
                    {ok, storage_mode, Mode};
                Error ->
                    Error
            end
    end.

parse_validate_durability_min_level(Params, BucketConfig, IsNew) ->
    IsEphemeral = is_ephemeral(Params, BucketConfig, IsNew),
    Level = proplists:get_value("durabilityMinLevel", Params),
    do_parse_validate_durability_min_level(IsEphemeral, Level, IsNew).

do_parse_validate_durability_min_level(false = _IsEphemeral, Level, IsNew) ->
    validate_with_missing(Level, "none", IsNew,
      fun parse_validate_membase_durability_min_level/1);
do_parse_validate_durability_min_level(true = _IsEphemeral, Level, IsNew) ->
    validate_with_missing(Level, "none", IsNew,
      fun parse_validate_ephemeral_durability_min_level/1).

parse_validate_membase_durability_min_level("none") ->
    {ok, durability_min_level, none};
parse_validate_membase_durability_min_level("majority") ->
    {ok, durability_min_level, majority};
parse_validate_membase_durability_min_level("majorityAndPersistActive") ->
    {ok, durability_min_level, majorityAndPersistActive};
parse_validate_membase_durability_min_level("persistToMajority") ->
    {ok, durability_min_level, persistToMajority};
parse_validate_membase_durability_min_level(_Other) ->
    {error, durability_min_level,
     <<"Durability minimum level must be one of 'none', 'majority', "
       "'majorityAndPersistActive', or 'persistToMajority'">>}.

parse_validate_ephemeral_durability_min_level("none") ->
    {ok, durability_min_level, none};
parse_validate_ephemeral_durability_min_level("majority") ->
    {ok, durability_min_level, majority};
parse_validate_ephemeral_durability_min_level(_Other) ->
    {error, durability_min_level,
     <<"Durability minimum level must be either 'none' or 'majority' for "
       "ephemeral buckets">>}.

parse_validate_durability_impossible_fallback(Params, _IsNew,
                                              false = _Is79) ->
    parse_validate_param_not_supported(
      "durabilityImpossibleFallback", Params,
      fun not_supported_until_79_error/1);
parse_validate_durability_impossible_fallback(Params, IsNew, _Is79) ->
    Mode = proplists:get_value("durabilityImpossibleFallback", Params),
    validate_with_missing(Mode, "disabled", IsNew,
                          fun parse_validate_durability_impossible_fallback/1).

parse_validate_durability_impossible_fallback("disabled") ->
    {ok, durability_impossible_fallback, disabled};
parse_validate_durability_impossible_fallback("fallbackToActiveAck") ->
    {ok, durability_impossible_fallback, fallback_to_master_ack};
parse_validate_durability_impossible_fallback(_) ->
    {error, durability_impossible_fallback,
     <<"Durability impossible fallback must be either 'disabled' or "
       "'fallbackToActiveAck'">>}.

parse_validate_warmup_behavior(Params, _IsNew, false = _Is79) ->
    parse_validate_param_not_supported(
      "warmupBehavior", Params,
      fun not_supported_until_79_error/1);
parse_validate_warmup_behavior(Params, IsNew, _Is79) ->
    Behavior = proplists:get_value("warmupBehavior", Params),
    validate_with_missing(Behavior, "background", IsNew,
                          fun parse_validate_warmup_behavior/1).

parse_validate_warmup_behavior("background") ->
    {ok, warmup_behavior, background};
parse_validate_warmup_behavior("blocking") ->
    {ok, warmup_behavior, blocking};
parse_validate_warmup_behavior("none") ->
    {ok, warmup_behavior, none};
parse_validate_warmup_behavior(_) ->
    {error, warmup_behavior,
     <<"Warmup behavior must be either 'background' or 'blocking' "
       "or 'none'">>}.

-spec value_not_in_range_error(Param, Value, Min, Max) -> Result when
      Param :: atom(),
      Value :: string(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer(),
      Result :: {error, atom(), bitstring()}.
value_not_in_range_error(Param, Value, Min, Max) ->
    NumericValue = list_to_integer(Value),
    {error, Param,
     list_to_binary(
       io_lib:format(
         "The value of ~p (~p) must be in the range ~p to ~p inclusive",
         [Param, NumericValue, Min, Max]))}.

value_not_numeric_error(Param, Value) ->
    {error, Param,
     list_to_binary(io_lib:format(
                      "The value of ~p (~s) must be a non-negative integer",
                      [Param, Value]))}.

value_not_boolean_error(Param) ->
    {error, Param,
     list_to_binary(io_lib:format("~p must be true or false",
                                  [Param]))}.

cross_cluster_versioning_not_supported_error(Param) ->
    {error, Param,
     <<"Cross Cluster Versioning is not supported until cluster is fully "
       "7.6">>}.

version_pruning_not_supported_error(Param) ->
    {error, Param,
     <<"Version pruning is not supported until cluster is fully 7.6">>}.

parse_validate_param_not_supported(Key, Params, ErrorFun) ->
    case proplists:is_defined(Key, Params) of
        true ->
            ErrorFun(Key);
        false ->
            ignore
    end.

not_supported_until_79_error(Param) ->
    {error, Param,
     <<"Argument is not supported until cluster is fully 7.9">>}.

not_supported_for_ephemeral_buckets(Param) ->
    {error, Param,
     <<"Argument is not supported for ephemeral buckets">>}.

only_supported_on_magma(Param) ->
    {error, Param,
     <<"Argument is only supported for magma buckets">>}.

%% Parameter parsing and validation when not in enterprise mode.
parse_validate_param_not_enterprise(Key, Params) ->
    parse_validate_param_not_supported(
      Key, Params,
      fun (Param) ->
              {error, Param,
               list_to_binary(io_lib:format("~p can only be set in Enterprise "
                                            "edition", [Param]))}
      end).

parse_validate_create_only(Key, Params) ->
    parse_validate_param_not_supported(
      Key, Params,
      fun (Param) ->
              {error, Param,
               list_to_binary(
                 io_lib:format(
                   "~0p allowed only during bucket creation", [Param]))}
      end).

parse_validate_cross_cluster_versioning_enabled(Params, _IsNew, _Allow,
                                                false = _IsEnterprise,
                                                _BucketConfig) ->
    parse_validate_param_not_enterprise("enableCrossClusterVersioning", Params);
parse_validate_cross_cluster_versioning_enabled(Params, _IsNew, false = _Allow,
                                                _IsEnterprise, _BucketConfig) ->
    parse_validate_param_not_supported(
      "enableCrossClusterVersioning", Params,
      fun cross_cluster_versioning_not_supported_error/1);
parse_validate_cross_cluster_versioning_enabled(Params, true = IsNew, _Allow,
                                                _IsEnterprise, _BucketConfig) ->
    case validate_cross_cluster_versioning_enabled(Params, IsNew) of
        {ok, _Key, true} ->
            {error, enableCrossClusterVersioning,
             <<"Cross Cluster Versioning cannot be enabled on bucket "
               "create">>};
        Res ->
            Res
    end;
parse_validate_cross_cluster_versioning_enabled(Params, IsNew, _Allow,
                                                _IsEnterprise, BucketConfig) ->
    Current = proplists:get_value(cross_cluster_versioning_enabled,
                                  BucketConfig),
    case validate_cross_cluster_versioning_enabled(Params, IsNew) of
        {ok, cross_cluster_versioning_enabled, false} when Current ->
            {error, enableCrossClusterVersioning,
             <<"Cross Cluster Versioning cannot be disabled once it has been "
               "enabled">>};
        {ok, cross_cluster_versioning_enabled, true} when Current ->
            {error, enableCrossClusterVersioning,
             <<"Cross Cluster Versioning is already enabled">>};
        Other ->
            Other
    end.

process_boolean_param_validation(Param, Key, Result, IsNew) ->
    process_boolean_param_validation(Param, Key, Result, IsNew,
                                     fun ns_bucket:attribute_default/1).

process_boolean_param_validation(Param, Key, Result, IsNew, DefaultFun) ->
    case {Result, IsNew} of
        {[], true} ->
            %% The value wasn't supplied and we're creating a bucket:
            %% use the default value.
            {ok, Key, DefaultFun(Key)};
        {[], false} ->
            %% The value wasn't supplied and we're modifying a bucket:
            %% don't complain since the value was either specified or a
            %% default used when the bucket was created.
            ignore;
        {[{ok, _, Value}], _} ->
            {ok, Key, Value};
        {[{error, _, _ErrorMsg}], _} ->
            value_not_boolean_error(Param)
    end.

validate_cross_cluster_versioning_enabled(Params, IsNew) ->
    Param = "enableCrossClusterVersioning",
    Result = menelaus_util:parse_validate_boolean_field(Param, '_', Params),
    process_boolean_param_validation(
      Param, cross_cluster_versioning_enabled, Result, IsNew).

validate_vp_window_hrs(_Param, undefined = _Val, Key, true = _IsNew) ->
    {ok, Key, ns_bucket:attribute_default(Key)};
validate_vp_window_hrs(_Param, undefined = _Val, _Key, false = _IsNew) ->
    ignore;
validate_vp_window_hrs(Param, InputVal, Key, _IsNew) ->
    Min = ns_bucket:attribute_min(Key),
    Max = ns_bucket:attribute_max(Key),
    case menelaus_util:parse_validate_number(InputVal, Min, Max) of
        {ok, Value} ->
            {ok, Key, Value};
        _Error ->
            Msg = io_lib:format("~p must be an integer between ~p and ~p",
                                [Param, Min, Max]),
            {error, Param, list_to_binary(Msg)}
    end.


parse_validate_version_pruning_window(Params, _IsNew, _Allow,
                                      false = _IsEnterprise) ->
    parse_validate_param_not_enterprise("versionPruningWindowHrs", Params);
parse_validate_version_pruning_window(Params, _IsNew, false = _Allow,
                                      _IsEnterprise) ->
    parse_validate_param_not_supported(
      "versionPruningWindowHrs", Params,
      fun version_pruning_not_supported_error/1);
parse_validate_version_pruning_window(Params, IsNew, _Allow,
                                      _IsEnterprise) ->
    Param = "versionPruningWindowHrs",
    Val = proplists:get_value(Param, Params),
    validate_vp_window_hrs(Param, Val, version_pruning_window_hrs, IsNew).

parse_validate_numeric_param(Params, Param, ConfigKey, IsNew) ->
    Value = proplists:get_value(atom_to_list(Param), Params),
    case {Value, IsNew} of
        {undefined, true} ->
            %% The value wasn't supplied and we're creating a bucket:
            %% use the default value.
            {ok, ConfigKey, ns_bucket:attribute_default(ConfigKey)};
        {undefined, false} ->
            %% The value wasn't supplied and we're modifying a bucket:
            %% don't complain since the value was either specified or a
            %% default used when the bucket was created.
            ignore;
        {_, _} ->
            validate_numeric_param(Value, Param, ConfigKey)
    end.

%% Validates defined numeric parameters.
validate_numeric_param(Value, Param, ConfigKey) ->
    Min = ns_bucket:attribute_min(ConfigKey),
    Max = ns_bucket:attribute_max(ConfigKey),
    Result = menelaus_util:parse_validate_number(Value, Min, Max),
    case Result of
        {ok, X} ->
            {ok, ConfigKey, X};
        invalid ->
            value_not_numeric_error(Param, Value);
        too_small ->
            value_not_in_range_error(Param, Value, Min, Max);
        too_large ->
            value_not_in_range_error(Param, Value, Min, Max)
    end.

parse_validate_access_scanner_enabled(Params, _IsNew, false = _Is79,
                                      _IsPersistent) ->
    parse_validate_param_not_supported(
      "accessScannerEnabled", Params, fun not_supported_until_79_error/1);
parse_validate_access_scanner_enabled(Params, _IsNew, true = _Is79,
                                      false = _IsPersistent) ->
    parse_validate_param_not_supported(
      "accessScannerEnabled", Params,
      fun not_supported_for_ephemeral_buckets/1);
parse_validate_access_scanner_enabled(Params, IsNew, true = _Is79,
                                      true = _IsPersistent) ->
    Result = menelaus_util:parse_validate_boolean_field("accessScannerEnabled",
                                                        '_', Params),
    process_boolean_param_validation(accessScannerEnabled,
                                     access_scanner_enabled, Result, IsNew).

parse_validate_expiry_pager_sleep_time(Params, _IsNew, false = _Is79) ->
    parse_validate_param_not_supported(
      "expiryPagerSleepTime", Params, fun not_supported_until_79_error/1);
parse_validate_expiry_pager_sleep_time(Params, IsNew, true = _Is79) ->
    parse_validate_numeric_param(Params, expiryPagerSleepTime,
                                 expiry_pager_sleep_time, IsNew).

parse_validate_memory_low_watermark(Params, _IsNew, false = _Is79) ->
    parse_validate_param_not_supported(
      "memoryLowWatermark", Params, fun not_supported_until_79_error/1);
parse_validate_memory_low_watermark(Params, IsNew, true = _Is79) ->
    parse_validate_numeric_param(Params, memoryLowWatermark,
                                 memory_low_watermark, IsNew).

parse_validate_memory_high_watermark(Params, _IsNew, false = _Is79) ->
    parse_validate_param_not_supported(
      "memoryHighWatermark", Params, fun not_supported_until_79_error/1);
parse_validate_memory_high_watermark(Params, IsNew, true = _Is79) ->
    parse_validate_numeric_param(Params, memoryHighWatermark,
                                 memory_high_watermark, IsNew).

get_storage_mode_based_on_storage_backend(Params, IsEnterprise) ->
    DefaultStorageBackend =
        case IsEnterprise andalso cluster_compat_mode:is_cluster_79() of
            true ->
                "magma";
            false ->
                "couchstore"
        end,
    StorageBackend = proplists:get_value("storageBackend", Params,
                                         DefaultStorageBackend),
    do_get_storage_mode_based_on_storage_backend(
      StorageBackend, IsEnterprise).

do_get_storage_mode_based_on_storage_backend("magma", false) ->
    {error, storageBackend,
     <<"Magma is supported in enterprise edition only">>};
do_get_storage_mode_based_on_storage_backend(StorageBackend, _IsEnterprise) ->
    do_get_storage_mode_based_on_storage_backend(StorageBackend).

do_get_storage_mode_based_on_storage_backend(StorageBackend) ->
    case StorageBackend of
        "couchstore" ->
            {ok, storage_mode, couchstore};
        "magma" ->
            {ok, storage_mode, magma};
        _ ->
            {error, storage_mode,
             <<"storage backend must be couchstore or magma">>}
    end.

get_conflict_resolution_type_and_thresholds(Params, HistRetSecs, BucketConfig,
                                            true = IsNew) ->
    ConResType = case proplists:get_value("conflictResolutionType", Params) of
                     undefined ->
                         {ok, conflict_resolution_type, seqno};
                     Value ->
                         parse_validate_conflict_resolution_type(Value)
                 end,
    [ConResType |
     get_drift_thresholds(ConResType, Params, HistRetSecs, BucketConfig,
                          IsNew)];
get_conflict_resolution_type_and_thresholds(Params, HistRetSecs, BCfg,
                                            false = IsNew) ->
    case proplists:get_value("conflictResolutionType", Params) of
        undefined ->
            get_drift_thresholds(ns_bucket:conflict_resolution_type(BCfg),
                                 Params, HistRetSecs, BCfg, IsNew);
        _Any ->
            [{error, conflictResolutionType,
              <<"Conflict resolution type not allowed in update bucket">>}]
    end.

get_drift_thresholds(ConResType, Params, HistRetSecs, BCfg, IsNew) ->
    case drift_thresholds_needed(ConResType, HistRetSecs, BCfg, IsNew) of
        true ->
            %% Only assign default values when drift thresholds are first
            %% needed, either on bucket create with conflict_resolution_type lww
            %% or when retention_history_seconds is set non-zero
            IsDriftThresholdNew = BCfg == false orelse
                not proplists:is_defined(drift_ahead_threshold_ms, BCfg),
            [get_drift_ahead_threshold(Params, IsDriftThresholdNew),
             get_drift_behind_threshold(Params, IsDriftThresholdNew)];
        false -> []
    end.

drift_thresholds_needed(ConResType, HistRetSecs, _BCfg, true) ->
    case {ConResType, HistRetSecs} of
        {{ok, _, lww}, _} ->
            true;
        {_, {ok, _, undefined}} ->
            false;
        {_, {ok, _, Value}} when Value > 0 ->
            true;
        {{ok, _, seqno}, _} ->
            false;
        {{ok, _, custom}, _} ->
            false;
        {{error, _, _}, _} ->
            false
    end;
drift_thresholds_needed(ConResType, HistRetSecs, BCfg, false) ->
    case ConResType of
        lww -> true;
        _ ->
            HistRetSecsValue =
                case HistRetSecs of
                    {ok, _, Num} -> Num;
                    ignore -> ns_bucket:history_retention_seconds(BCfg);
                    {error, _, _} -> ns_bucket:history_retention_seconds(BCfg)
                end,
            case HistRetSecsValue of
                undefined -> false;
                V when V > 0 -> true;
                _ -> false
            end
    end.

assert_candidates(Candidates) ->
    %% this is to validate that Candidates elements have specific
    %% structure
    [case E of
         %% ok-s are used to keep correctly parsed/validated params
         {ok, _, _} -> [];
         %% error-s hold errors
         {error, _, _} -> [];
         %% ignore-s are used to "do nothing"
         ignore -> []
     end || E <- Candidates].

get_drift_ahead_threshold(Params, IsNew) ->
    validate_with_missing(proplists:get_value("driftAheadThresholdMs", Params),
                          "5000",
                          IsNew,
                          fun parse_validate_drift_ahead_threshold/1).

get_drift_behind_threshold(Params, IsNew) ->
    validate_with_missing(proplists:get_value("driftBehindThresholdMs", Params),
                          "5000",
                          IsNew,
                          fun parse_validate_drift_behind_threshold/1).

-define(PRAM(K, KO), {KO, V#ram_summary.K}).
ram_summary_to_proplist(V) ->
    [?PRAM(total, total),
     ?PRAM(other_buckets, otherBuckets),
     ?PRAM(nodes_count, nodesCount),
     ?PRAM(per_node, perNodeMegs),
     ?PRAM(this_alloc, thisAlloc),
     ?PRAM(this_used, thisUsed),
     ?PRAM(free, free)].

interpret_ram_quota(Ctx, CurrentBucket, ParsedProps, ClusterStorageTotals) ->
    RAMQuota = proplists:get_value(ram_quota, ParsedProps),
    NodesCount = length(get_nodes(Ctx)),
    ParsedQuota = RAMQuota * NodesCount,
    PerNode = RAMQuota div ?MIB,
    ClusterTotals = proplists:get_value(ram, ClusterStorageTotals),

    OtherBuckets = proplists:get_value(quotaUsedPerNode, ClusterTotals) * NodesCount
        - case CurrentBucket of
              [_|_] ->
                  ns_bucket:ram_quota(CurrentBucket);
              _ ->
                  0
          end,
    ThisUsed = case CurrentBucket of
                   [_|_] ->
                       menelaus_stats:bucket_ram_usage(
                         proplists:get_value(name, ParsedProps));
                   _ -> 0
               end,
    Total = proplists:get_value(quotaTotalPerNode, ClusterTotals) * NodesCount,
    #ram_summary{total = Total,
                 other_buckets = OtherBuckets,
                 nodes_count = NodesCount,
                 per_node = PerNode,
                 this_alloc = ParsedQuota,
                 this_used = ThisUsed,
                 free = Total - OtherBuckets - ParsedQuota}.

get_nodes(#bv_ctx{kv_nodes = KvNodes, bucket_config = BucketConfig}) ->
    case BucketConfig of
        false ->
            KvNodes;
        _ ->
            ns_bucket:get_expected_servers(BucketConfig)
    end.

-define(PHDD(K, KO), {KO, V#hdd_summary.K}).
hdd_summary_to_proplist(V) ->
    [?PHDD(total, total),
     ?PHDD(other_data, otherData),
     ?PHDD(other_buckets, otherBuckets),
     ?PHDD(this_used, thisUsed),
     ?PHDD(free, free)].

interpret_hdd_quota(CurrentBucket, ParsedProps, ClusterStorageTotals, Ctx) ->
    ClusterTotals = proplists:get_value(hdd, ClusterStorageTotals),
    UsedByUs = get_hdd_used_by_us(Ctx),
    OtherData = proplists:get_value(used, ClusterTotals) - UsedByUs,
    ThisUsed = get_hdd_used_by_this_bucket(CurrentBucket, ParsedProps),
    OtherBuckets = UsedByUs - ThisUsed,
    Total = proplists:get_value(total, ClusterTotals),
    #hdd_summary{total = Total,
                 other_data = OtherData,
                 other_buckets = OtherBuckets,
                 this_used = ThisUsed,
                 free = Total - OtherData - OtherBuckets}.

get_hdd_used_by_us(Ctx) ->
    {hdd, HDDStats} = lists:keyfind(hdd, 1, Ctx#bv_ctx.cluster_storage_totals),
    {usedByData, V} = lists:keyfind(usedByData, 1, HDDStats),
    V.

get_hdd_used_by_this_bucket([_|_] = _CurrentBucket, Props) ->
    menelaus_stats:bucket_disk_usage(
      proplists:get_value(name, Props));
get_hdd_used_by_this_bucket(_ = _CurrentBucket, _Props) ->
    0.

validate_with_missing(GivenValue, DefaultValue, UseDefault, Fn) ->
    case Fn(GivenValue) of
        {error, _, _} = Error ->
            %% Parameter validation functions return error when GivenValue is
            %% undefined or was set to an invalid value. If the user did not
            %% pass any value for the parameter (given value is undefined)
            %% during bucket create and DefaultValue is available then use it.
            %% If this is not bucket create or if it's storage mode migration
            %% and if DefaultValue is not available then ignore the error. If
            %% the user passed some invalid value during either bucket create
            %% or edit then return error to the user.
            case GivenValue of
                undefined ->
                    case UseDefault andalso DefaultValue =/= undefined of
                        true ->
                            {ok, _, _} = Fn(DefaultValue);
                        false ->
                            ignore
                    end;
                _Other ->
                    Error
            end;
        {ok, _, _} = RV -> RV
    end.

parse_validate_replicas_number(NumReplicas) ->
    MinReplicas = ns_bucket:get_min_replicas(),
    case menelaus_util:parse_validate_number(NumReplicas, MinReplicas,
                                             ?MAX_NUM_REPLICAS) of
        invalid ->
            {error, replicaNumber, <<"The replica number must be specified and must be a non-negative integer.">>};
        too_small ->
            Msg = io_lib:format("Replica number must be equal to or greater "
                                "than ~p", [MinReplicas]),
            {error, replicaNumber, iolist_to_binary(Msg)};
        too_large ->
            Msg = io_lib:format("Replica number larger than ~p is not "
                                "supported.", [?MAX_NUM_REPLICAS]),
            {error, replicaNumber, iolist_to_binary(Msg)};
        {ok, X} -> {ok, num_replicas, X}
    end.

parse_validate_replica_index(Params, ReplicasNum, true = _IsNew) ->
    case proplists:get_value("bucketType", Params) =:= "ephemeral" of
        true ->
            case proplists:is_defined("replicaIndex", Params) of
                true ->
                    {error, replicaIndex, <<"replicaIndex not supported for ephemeral buckets">>};
                false ->
                    ignore
            end;
        false ->
            parse_validate_replica_index(
              proplists:get_value("replicaIndex", Params,
                                  replicas_num_default(ReplicasNum)))
    end;
parse_validate_replica_index(_Params, _ReplicasNum, false = _IsNew) ->
    ignore.

replicas_num_default({ok, num_replicas, 0}) ->
    "0";
replicas_num_default(_) ->
    "1".

parse_validate_replica_index("0") -> {ok, replica_index, false};
parse_validate_replica_index("1") -> {ok, replica_index, true};
parse_validate_replica_index(_ReplicaValue) -> {error, replicaIndex, <<"replicaIndex can only be 1 or 0">>}.

parse_validate_compression_mode(Params, BucketConfig, IsNew, IsEnterprise) ->
    CompMode = proplists:get_value("compressionMode", Params),
    do_parse_validate_compression_mode(
      IsEnterprise, CompMode, BucketConfig, IsNew).

do_parse_validate_compression_mode(false, undefined, _BucketCfg, _IsNew) ->
    {ok, compression_mode, off};
do_parse_validate_compression_mode(false, _CompMode, _BucketCfg, _IsNew) ->
    {error, compressionMode,
     <<"Compression mode is supported in enterprise edition only">>};
do_parse_validate_compression_mode(true, CompMode, BucketCfg, IsNew) ->
    DefaultVal = case IsNew of
                     true -> passive;
                     false -> proplists:get_value(compression_mode, BucketCfg)
                 end,
    validate_with_missing(CompMode, atom_to_list(DefaultVal), IsNew,
                          fun parse_compression_mode/1).

parse_compression_mode(V) when V =:= "off"; V =:= "passive"; V =:= "active" ->
    {ok, compression_mode, list_to_atom(V)};
parse_compression_mode(_) ->
    {error, compressionMode,
     <<"compressionMode can be set to 'off', 'passive' or 'active'">>}.

parse_validate_bucket_rank(Params, IsNew) ->
    parse_validate_rank_inner(cluster_compat_mode:is_cluster_76(),
                              proplists:get_value("rank", Params), IsNew).

parse_validate_rank_inner(true, undefined, true = _IsNew) ->
    {ok, rank, ?DEFAULT_BUCKET_RANK};
parse_validate_rank_inner(true, undefined, false = _IsNew) ->
    ignore;
parse_validate_rank_inner(true, Value, _IsNew) ->
    parse_validate_rank_inner(Value);
parse_validate_rank_inner(false, undefined, _IsNew) ->
    ignore;
parse_validate_rank_inner(false, _Value, _IsNew) ->
    {error, rank,
     <<"Bucket rank cannot be set until the cluster is fully "
       "upgraded to 7.6.">>}.

parse_validate_rank_inner(Rank) ->
    case menelaus_util:parse_validate_number(Rank, ?MIN_BUCKET_RANK,
                                             ?MAX_BUCKET_RANK) of
        {ok, V} ->
            {ok, rank, V};
        _Error ->
            RankErr =
                io_lib:format("Rank must be in the range ~p-~p. Got '~p'"
                              " instead.",
                              [?MIN_BUCKET_RANK, ?MAX_BUCKET_RANK, Rank]),
            {error, rank, list_to_binary(RankErr)}
    end.

parse_validate_max_ttl(Params, BucketConfig, IsNew, IsEnterprise) ->
    MaxTTL = proplists:get_value("maxTTL", Params),
    parse_validate_max_ttl_inner(IsEnterprise, MaxTTL, BucketConfig, IsNew).

parse_validate_max_ttl_inner(false, undefined, _BucketCfg, _IsNew) ->
    {ok, max_ttl, 0};
parse_validate_max_ttl_inner(false, _MaxTTL, _BucketCfg, _IsNew) ->
    {error, maxTTL, <<"Max TTL is supported in enterprise edition only">>};
parse_validate_max_ttl_inner(true, MaxTTL, BucketCfg, IsNew) ->
    DefaultVal = case IsNew of
                     true -> "0";
                     false -> proplists:get_value(max_ttl, BucketCfg)
                 end,
    validate_with_missing(MaxTTL, DefaultVal, IsNew, fun do_parse_validate_max_ttl/1).

do_parse_validate_max_ttl(Val) ->
    case menelaus_util:parse_validate_number(Val, 0, ?MAX_32BIT_SIGNED_INT) of
        {ok, X} ->
            {ok, max_ttl, X};
        _Error ->
            Msg = io_lib:format("Max TTL must be an integer between 0 and ~p", [?MAX_32BIT_SIGNED_INT]),
            {error, maxTTL, list_to_binary(Msg)}
    end.

is_magma(Params, _BucketCfg, true = _IsNew, false = _IsStorageModeMigration) ->
    proplists:get_value("storageBackend", Params, "couchstore") =:= "magma";
is_magma(Params, _BucketCfg, false = _IsNew, true = _IsStorageModeMigration) ->
    proplists:get_value("storageBackend", Params, "couchstore") =:= "magma";
is_magma(_Params, BucketCfg, false = _IsNew, false = _IsStorageModeMigration) ->
    ns_bucket:storage_mode(BucketCfg) =:= magma.

parse_validate_storage_quota_percentage(Params, BucketConfig, IsNew,
                                        IsEnterprise, IsStorageModeMigration) ->
    Percent = proplists:get_value("storageQuotaPercentage", Params),
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_storage_quota_percentage_inner(IsEnterprise,
                                                  Percent, BucketConfig, IsNew,
                                                  IsMagma,
                                                  IsStorageModeMigration).

parse_validate_storage_quota_percentage_inner(false = _IsEnterprise,
                                              undefined = _Percent, _BucketCfg,
                                              _IsNew, _IsMagma,
                                              _IsStorageModeMigration) ->
    %% Community edition but percent/ratio wasn't specified
    ignore;
parse_validate_storage_quota_percentage_inner(false = _IsEnterprise,
                                           _Percent, _BucketCfg, _IsNew,
                                           _IsMagma, _IsStorageModeMigration) ->
    {error, storageQuotaPercentage,
     <<"Storage Quota Percentage is supported in enterprise edition only">>};
parse_validate_storage_quota_percentage_inner(true = _IsEnterprise,
                                              undefined,
                                              _BucketCfg, _IsNew,
                                              false = _IsMagma,
                                              _IsStorageModeMigration) ->
    %% Not a magma bucket and percent wasn't specified
    ignore;
parse_validate_storage_quota_percentage_inner(true = _IsEnterprise,
                                              _Percent,
                                              _BucketCfg, _IsNew,
                                              false = _IsMagma,
                                              _IsStorageModeMigration) ->
    {error, storageQuotaPercentage,
     <<"Storage Quota Percentage is only used with Magma">>};
parse_validate_storage_quota_percentage_inner(true = _IsEnterprise,
                                              Percent,
                                              BucketCfg, IsNew,
                                              true = _IsMagma,
                                              IsStorageModeMigration) ->
    %% If the storage mode for a bucket is being migrated
    %% storage_quota_percentage wouldn't have been set in BucketConfig and
    %% therefore pick ?MAGMA_STORAGE_QUOTA_PERCENTAGE if it's being migrated.
    UseDefault = IsNew orelse IsStorageModeMigration,
    DefaultVal = case UseDefault of
                     true -> integer_to_list(?MAGMA_STORAGE_QUOTA_PERCENTAGE);
                     false -> proplists:get_value(storage_quota_percentage,
                                                  BucketCfg)
                 end,
    validate_with_missing(Percent, DefaultVal, UseDefault,
                          fun do_parse_validate_storage_quota_percentage/1).

do_parse_validate_storage_quota_percentage(Val) ->
    case menelaus_util:parse_validate_number(Val,
                                             ?MIN_MAGMA_STORAGE_QUOTA_PERCENTAGE,
                                             ?MAX_MAGMA_STORAGE_QUOTA_PERCENTAGE) of
        {ok, X} ->
            {ok, storage_quota_percentage, X};
        _Error ->
            Msg = io_lib:format("Storage Quota Percentage must be between ~p "
                                "and ~p, inclusive",
                                [?MIN_MAGMA_STORAGE_QUOTA_PERCENTAGE,
                                 ?MAX_MAGMA_STORAGE_QUOTA_PERCENTAGE]),
            {error, storageQuotaPercentage, iolist_to_binary(Msg)}
    end.

parse_validate_limits(Params, BucketConfig, IsNew, IsEnabled, AttrsFunc) ->
    LimitFunc = ?cut(proplists:get_value(_, Params)),
    [do_parse_validate_limit(Param, Key, LimitFunc(atom_to_list(Param)),
                             BucketConfig, Default, Min, Max,
                             IsNew, IsEnabled) ||
        {Param, Key, Default, Min, Max} <- AttrsFunc(),
        proplists:is_defined(atom_to_list(Param), Params)].

do_parse_validate_limit(_Param, _InternalName, undefined, _BucketConfig,
                        _Default, _Min, _Max, _IsNew, false = _IsEnabled) ->
    ignore;
do_parse_validate_limit(Param, _InternalName, _Limit, _BucketConfig,
                        _Default, _Min, _Max, _IsNew, false = _IsEnabled) ->
    Msg = io_lib:format("~p is not supported with this config profile",
                        [Param]),
    {error, Param, list_to_binary(Msg)};
do_parse_validate_limit(Param, InternalName, Limit, BucketConfig,
                        Default, Min, Max, IsNew, _IsEnabled) ->
    GlobalDefault = ns_config:read_key_fast(InternalName, Default),
    DefaultLimitInt =
        case IsNew of
            true -> GlobalDefault;
            false -> proplists:get_value(InternalName,
                                         BucketConfig,
                                         GlobalDefault)
        end,
    DefaultLimit = integer_to_list(DefaultLimitInt),
    Fun = ?cut(do_validate_limit(atom_to_list(Param), InternalName, _, Min,
                                 Max)),
    validate_with_missing(Limit, DefaultLimit, IsNew, Fun).

do_validate_limit(Param, InternalName, Val, Min, Max) ->
    case menelaus_util:parse_validate_number(Val, Min, Max) of
        {ok, X} ->
            {ok, InternalName, X};
        _Error ->
            Msg = io_lib:format("~p must be an integer between ~p and ~p",
                                [Param, Min, Max]),
            {error, Param, list_to_binary(Msg)}
    end.

parse_validate_num_vbuckets(Params, BucketConfig, IsNew, IsPersistent) ->
    NumVBs = proplists:get_value("numVBuckets", Params),
    IsEnabled = ns_bucket:allow_variable_num_vbuckets(),
    do_parse_validate_num_vbuckets(NumVBs, BucketConfig, Params, IsNew,
                                   IsEnabled, IsPersistent).

do_parse_validate_num_vbuckets(undefined, _BucketConfig, _Params,
                               false = _IsNew, _IsEnabled, _IsPersistent) ->
    ignore;
do_parse_validate_num_vbuckets(NumVBs, BucketConfig, _Params, false = _IsNew,
                               _IsEnabled, _IsPersistent) ->
    CurVal = integer_to_list(proplists:get_value(num_vbuckets, BucketConfig)),
    case NumVBs =:= CurVal of
        true ->
            ignore;
        false ->
            {error, numVBuckets,
             <<"Number of vbuckets cannot be modified">>}
    end;
do_parse_validate_num_vbuckets(NumVBs, _BucketConfig, Params, true = _IsNew,
                               false = _IsEnabled, _IsPersistent)
  when NumVBs =/= undefined ->
    StorageBackend = proplists:get_value("storageBackend", Params, "magma"),
    %% Specifying variable number of vbuckets is not enabled. But, for magma
    %% buckets we allow specifying 128 or 1024; for couchstore 1024.

    case menelaus_util:parse_validate_number(NumVBs, ?DEFAULT_VBUCKETS_MAGMA,
                                             ?MAX_NUM_VBUCKETS) of
        {ok, N} when StorageBackend =:= "magma" andalso
                     (N =:= ?DEFAULT_VBUCKETS_MAGMA orelse
                      N =:= ?MAX_NUM_VBUCKETS) ->
            {ok, num_vbuckets, N};
        {ok, N} when StorageBackend =:= "couchstore" andalso
                     N =:= ?DEFAULT_VBUCKETS_COUCHSTORE ->
            {ok, num_vbuckets, N};
        {ok, _N} when StorageBackend =/= "magma" andalso
                     StorageBackend =/= "couchstore" ->
            %% Invalid value will be caught during parse/validation of
            %% StorageBackend.
            ignore;
        _ ->
            Msg = io_lib:format("Number of vbuckets must be ~p or ~p "
                                "(magma) or ~p (couchstore)",
                                [?DEFAULT_VBUCKETS_MAGMA, ?MAX_NUM_VBUCKETS,
                                 ?DEFAULT_VBUCKETS_COUCHSTORE]),
            {error, numVBuckets, list_to_binary(Msg)}
    end;
do_parse_validate_num_vbuckets(NumVBs, _BucketConfig, Params, true = _IsNew,
                               _IsEnabled, IsPersistent) ->
    case NumVBs of
        undefined ->
            StorageBackend = proplists:get_value("storageBackend", Params,
                                                 "magma"),
            case {IsPersistent, StorageBackend} of
                {false, _} ->
                    {ok, num_vbuckets,
                     ns_bucket:get_default_num_vbuckets(ephemeral)};
                {true, "magma"} ->
                    {ok, num_vbuckets,
                     ns_bucket:get_default_num_vbuckets(magma)};
                {true, "couchstore"} ->
                    {ok, num_vbuckets,
                     ns_bucket:get_default_num_vbuckets(couchstore)};
                _ ->
                    %% Invalid value will be caught during parse/validation
                    %% of storageBackend.
                    ignore
            end;
        _ ->
            validate_num_vbuckets(NumVBs)
    end.
validate_num_vbuckets(Val) ->
    case menelaus_util:parse_validate_number(Val, ?MIN_NUM_VBUCKETS,
                                             ?MAX_NUM_VBUCKETS) of
        {ok, X} ->
            {ok, num_vbuckets, X};
        _Error ->
            Msg = io_lib:format("Number of vbuckets must be an integer "
                                "between ~p and ~p",
                                [?MIN_NUM_VBUCKETS, ?MAX_NUM_VBUCKETS]),
            {error, numVBuckets, list_to_binary(Msg)}
    end.

%% Helper function that remaps the user key to the internal key if the config
%% parameter is valid.
%%
%% The REST API has bucket parameters in camelCase, but parameters are stored
%% internally and passed to memcached in snake_case. This difference
%% frustratingly means that we must map the user (camelCase) key to the internal
%% (snake_case) key if the parameter is valid, otherwise we should return the
%% user (camelCase) key in any error messages. This function allows us to deal
%% with the user (camelCase) key throughout the parsing of the validation, then
%% map to the internal key when we've validated the parameter passed by the
%% user (if appropriate). This should allow for the consolidation of all of the
%% keys into the same parsing function which allows us to avoid passing user
%% (camelCase) and internal (snake_case) keys down all of the validation call
%% stacks.
remap_user_key_to_internal_key_if_valid(Result, InternalKey) ->
    case Result of
        {ok, _UserKey, Val} ->
            {ok, InternalKey, Val};
        Error ->
            Error
    end.

parse_validate_history_retention_seconds(Params, BucketConfig, IsNew, Version,
                                         IsEnterprise,
                                         IsStorageModeMigration) ->
    UserKey = historyRetentionSeconds,
    HistoryRetentionValue = proplists:get_value(atom_to_list(UserKey), Params),
    Ret = parse_validate_history_param_numeric(
        UserKey, HistoryRetentionValue, Params, BucketConfig, IsNew, Version,
        IsEnterprise, IsStorageModeMigration,
        integer_to_list(?HISTORY_RETENTION_SECONDS_DEFAULT),
        0),
    remap_user_key_to_internal_key_if_valid(Ret, history_retention_seconds).

parse_validate_history_retention_bytes(Params, BucketConfig, IsNew,
                                       Version, IsEnterprise,
                                       IsStorageModeMigration) ->
    UserKey = historyRetentionBytes,
    HistoryRetentionValue = proplists:get_value(atom_to_list(UserKey), Params),
    Ret = parse_validate_history_param_numeric(
        UserKey, HistoryRetentionValue, Params, BucketConfig, IsNew, Version,
        IsEnterprise, IsStorageModeMigration,
        integer_to_list(?HISTORY_RETENTION_BYTES_DEFAULT),
        ?HISTORY_RETENTION_BYTES_MIN),
    remap_user_key_to_internal_key_if_valid(Ret, history_retention_bytes).

parse_validate_history_retention_collection_default(
  Params, BucketConfig, IsNew, Version, IsEnterprise, IsStorageModeMigration) ->
    UserKey = historyRetentionCollectionDefault,
    HistoryRetentionValue = proplists:get_value(atom_to_list(UserKey), Params),
    Ret = parse_validate_history_param_bool(
        UserKey, HistoryRetentionValue, Params, BucketConfig, IsNew, Version,
        IsEnterprise, IsStorageModeMigration,
        atom_to_list(?HISTORY_RETENTION_COLLECTION_DEFAULT_DEFAULT)),
    remap_user_key_to_internal_key_if_valid(
        Ret, history_retention_collection_default).

parse_validate_history_param_numeric(Key, Value, Params, BucketConfig, IsNew,
                                     Version, IsEnterprise,
                                     IsStorageModeMigration, DefaultVal,
                                     MinVal) ->
    IsCompat = cluster_compat_mode:is_version_72(Version),
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_history_param_inner(
        Key, Value, IsEnterprise, IsCompat, IsNew, IsMagma,
        fun (Val, New) ->
            validate_with_missing(
                Val, DefaultVal, New,
                fun (V) ->
                    %% 0 (Off), which is the default, is a special case for all
                    %% of the history parameters, don't enforce min value.
                    MinToUse = case V =:= DefaultVal of
                                   true -> 0;
                                   false -> MinVal
                               end,
                    do_parse_validate_history_retention_numeric(
                        Key, V, MinToUse, ?MAX_64BIT_UNSIGNED_INT)
                end)
        end).

parse_validate_history_param_bool(Key, Value, Params, BucketConfig, IsNew,
                                  Version, IsEnterprise, IsStorageModeMigration,
                                  DefaultVal) ->
    IsCompat = cluster_compat_mode:is_version_72(Version),
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_history_param_inner(
        Key, Value, IsEnterprise, IsCompat, IsNew, IsMagma,
        fun (Val, New) ->
            validate_with_missing(
                Val, DefaultVal, New,
                fun (V) ->
                    do_parse_validate_history_retention_bool(Key, V)
                end)
        end).

parse_validate_history_param_inner(_Key, undefined = _Value,
                                   false = _IsEnterprise, _IsCompat, _IsNew,
                                   _IsMagma, _ValidatorFn) ->
    %% Value wasn't specified and not enterprise
    ignore;
parse_validate_history_param_inner(_Key, undefined = _Value, _IsEnterprise,
                                   false = _IsCompat, _IsNew, _IsMagma,
                                   _ValidatorFn) ->
    %% Value wasn't specified and not 7.2
    ignore;
parse_validate_history_param_inner(_Key, undefined = _Value, _IsEnterprise,
                                   _IsCompat, _IsNew, false = _IsMagma,
                                   _ValidatorFn) ->
    %% Value wasn't specified and not magma
    ignore;
parse_validate_history_param_inner(Key, _Value, false = _IsEnterprise,
                                   _IsCompat, _IsNew, _IsMagma, _ValidatorFn) ->
    {error, Key,
        <<"History Retention is supported in enterprise edition only">>};
parse_validate_history_param_inner(Key, _Value, _IsEnterprise,
                                   false = _IsCompat, _IsNew, _IsMagma,
                                   _ValidatorFn) ->
    {error, Key,
        <<"History Retention cannot be set until the cluster is fully 7.2">>};
parse_validate_history_param_inner(Key, _Value, true = _IsEnterprise,
                                   true = _IsCompat, _IsNew, false = _IsMagma,
                                   _ValidatorFn) ->
    {error, Key,
        <<"History Retention can only used with Magma">>};
parse_validate_history_param_inner(_Key, Value, true = _IsEnterprise,
                                   true = _IsCompat, IsNew, true = _IsMagma,
                                   ValidatorFn) ->
    ValidatorFn(Value, IsNew).

do_parse_validate_history_retention_numeric(Key, Val, Min, Max) ->
    case menelaus_util:parse_validate_number(Val, Min, Max) of
        {ok, X} ->
            {ok, Key, X};
        _Error ->
            Msg = io_lib:format("Value must be an integer between ~p and ~p,"
                                " inclusive",
                                [Min, Max]),
            {error, Key, iolist_to_binary(Msg)}
    end.

do_parse_validate_history_retention_bool(Key, Val) ->
    case menelaus_util:parse_validate_boolean(Val) of
        {ok, X} ->
            {ok, Key, X};
        _Error ->
            {error, Key, <<"Value must be true or false">>}
    end.

parse_validate_magma_key_tree_data_blocksize(Params, BucketConfig, Version,
                                             IsNew, IsEnterprise,
                                             IsStorageModeMigration) ->
    UserKey = magmaKeyTreeDataBlockSize,
    MagmaDataBlockSize = proplists:get_value(atom_to_list(UserKey), Params),
    Ret = parse_validate_magma_data_blocksize(
            UserKey, MagmaDataBlockSize, Params, BucketConfig, IsNew, Version,
            IsEnterprise, IsStorageModeMigration,
            integer_to_list(?MAGMA_KEY_TREE_DATA_BLOCKSIZE),
            ?MIN_MAGMA_KEY_TREE_DATA_BLOCKSIZE,
            ?MAX_MAGMA_KEY_TREE_DATA_BLOCKSIZE),
    remap_user_key_to_internal_key_if_valid(Ret, magma_key_tree_data_blocksize).

parse_validate_magma_seq_tree_data_blocksize(Params, BucketConfig, Version,
                                             IsNew, IsEnterprise,
                                             IsStorageModeMigration) ->
    UserKey = magmaSeqTreeDataBlockSize,
    MagmaDataBlockSize = proplists:get_value(atom_to_list(UserKey), Params),
    Ret = parse_validate_magma_data_blocksize(
            UserKey, MagmaDataBlockSize, Params, BucketConfig, IsNew, Version,
            IsEnterprise, IsStorageModeMigration,
            integer_to_list(?MAGMA_SEQ_TREE_DATA_BLOCKSIZE),
            ?MIN_MAGMA_SEQ_TREE_DATA_BLOCKSIZE,
            ?MAX_MAGMA_SEQ_TREE_DATA_BLOCKSIZE),
    remap_user_key_to_internal_key_if_valid(Ret, magma_seq_tree_data_blocksize).

parse_validate_magma_data_blocksize(Key, Value, Params, BucketConfig, IsNew,
                                    Version, IsEnterprise,
                                    IsStorageModeMigration, DefaultVal, MinVal,
                                    MaxVal) ->
    IsCompat = cluster_compat_mode:is_version_72(Version),
    IsMagma = is_magma(Params, BucketConfig, IsNew, IsStorageModeMigration),
    parse_validate_magma_data_blocksize_inner(
      Key, Value, IsEnterprise, IsCompat, IsNew, IsMagma,
      fun (Val, New) ->
              validate_with_missing(
                Val, DefaultVal, New,
                fun (V) ->
                        do_parse_validate_magma_data_blocksize(Key, V, MinVal,
                                                               MaxVal)
                end)
      end).

do_parse_validate_magma_data_blocksize(Key, Val, Min, Max) ->
    case menelaus_util:parse_validate_number(Val, Min, Max) of
        {ok, X} ->
            {ok, Key, X};
        _Error ->
            Msg = io_lib:format("Value must be an integer between ~p and ~p, "
                                "inclusive", [Min, Max]),
            {error, Key, iolist_to_binary(Msg)}
    end.

parse_validate_magma_data_blocksize_inner(_Key, undefined = _Value,
                                          false = _IsEnterprise, _IsCompat,
                                          _IsNew, _IsMagma, _ValidatorFn) ->
    %% Value wasn't specified and not enterprise
    ignore;
parse_validate_magma_data_blocksize_inner(_Key, undefined = _Value,
                                          _IsEnterprise, false = _IsCompat,
                                          _IsNew, _IsMagma, _ValidatorFn) ->
    %% Value wasn't specified and not 7.2
    ignore;
parse_validate_magma_data_blocksize_inner(_Key, undefined = _Value,
                                          _IsEnterprise, _IsCompat, _IsNew,
                                          false = _IsMagma, _ValidatorFn) ->
    %% Value wasn't specified and not magma
    ignore;
parse_validate_magma_data_blocksize_inner(Key, _Value, false = _IsEnterprise,
                                          _IsCompat, _IsNew, _IsMagma,
                                          _ValidatorFn) ->
    {error, Key,
     <<"Magma data blocksize is supported in enterprise edition only">>};
parse_validate_magma_data_blocksize_inner(Key, _Value, _IsEnterprise,
                                          false = _IsCompat, _IsNew, _IsMagma,
                                          _ValidatorFn) ->
    {error, Key,
     <<"Magma data blocksize cannot be set until the cluster is fully "
       "running 7.2">>};
parse_validate_magma_data_blocksize_inner(Key, _Value, true = _IsEnterprise,
                                          true = _IsCompat, _IsNew,
                                          false = _IsMagma, _ValidatorFn) ->
    {error, Key,
     <<"Magma data blocksize can only be used with Magma">>};
parse_validate_magma_data_blocksize_inner(_Key, Value, true = _IsEnterprise,
                                          true = _IsCompat, IsNew,
                                          true = _IsMagma, ValidatorFn) ->
    ValidatorFn(Value, IsNew).

parse_validate_dcp_connections_between_nodes(Params, _IsNew, _Is79,
                                             false = _IsEnterprise) ->
    parse_validate_param_not_enterprise("dcpConnectionsBetweenNodes", Params);
parse_validate_dcp_connections_between_nodes(Params, _IsNew,
                                             false = _Is79,
                                             _IsEnterprise) ->
    parse_validate_param_not_supported(
      "dcpConnectionsBetweenNodes", Params,
      fun not_supported_until_79_error/1);
parse_validate_dcp_connections_between_nodes(Params, IsNew, _Is79,
                                             _IsEnterprise) ->
    parse_validate_numeric_param(Params, dcpConnectionsBetweenNodes,
                                 dcp_connections_between_nodes, IsNew).

parse_validate_dcp_backfill_idle_protection_enabled(Params, _BCfg, _IsNew,
                                                    false = _Is79) ->
    parse_validate_param_not_supported(
      "dcpBackfillIdleProtectionEnabled",
      Params,
      fun not_supported_until_79_error/1);
parse_validate_dcp_backfill_idle_protection_enabled(Params, BCfg, IsNew,
                                                    true = _Is79) ->
    Key = "dcpBackfillIdleProtectionEnabled",
    Result = menelaus_util:parse_validate_boolean_field(Key, '_', Params),

    DefaultFun =
        fun(_) ->
                %% We can't use the ns_bucket function to get the default
                %% because we might not have a bucket config yet.
                not is_ephemeral(Params, BCfg, IsNew)
        end,

    process_boolean_param_validation(Key, dcp_backfill_idle_protection_enabled,
                                     Result, IsNew,
                                     DefaultFun).

parse_validate_dcp_backfill_idle_limit_seconds(Params, _IsNew,
                                               false = _Is79) ->
    parse_validate_param_not_supported(
      "dcpBackfillIdleLimitSeconds", Params,
      fun not_supported_until_79_error/1);
parse_validate_dcp_backfill_idle_limit_seconds(Params, IsNew, _Is79) ->
    parse_validate_numeric_param(Params, dcpBackfillIdleLimitSeconds,
                                 dcp_backfill_idle_limit_seconds, IsNew).

parse_validate_dcp_backfill_idle_disk_threshold(Params, _IsNew,
                                                false = _Is79) ->
    parse_validate_param_not_supported(
      "dcpBackfillIdleDiskThreshold", Params,
      fun not_supported_until_79_error/1);
parse_validate_dcp_backfill_idle_disk_threshold(Params, IsNew, _Is79) ->
    parse_validate_numeric_param(Params, dcpBackfillIdleDiskThreshold,
                                 dcp_backfill_idle_disk_threshold, IsNew).

parse_validate_threads_number(Params, IsNew) ->
    validate_with_missing(proplists:get_value("threadsNumber", Params),
                          "3", IsNew, fun parse_validate_threads_number/1).

parse_validate_flush_enabled("0") -> {ok, flush_enabled, false};
parse_validate_flush_enabled("1") -> {ok, flush_enabled, true};
parse_validate_flush_enabled(_ReplicaValue) -> {error, flushEnabled, <<"flushEnabled can only be 1 or 0">>}.

parse_validate_threads_number(NumThreads) ->
    case menelaus_util:parse_validate_number(NumThreads,
                                             ?MIN_NUM_WORKER_THREADS,
                                             ?MAX_NUM_WORKER_THREADS) of
        invalid ->
            Msg = io_lib:format("The number of threads must be an integer "
                                "between ~p and ~p, inclusive",
                                [?MIN_NUM_WORKER_THREADS,
                                 ?MAX_NUM_WORKER_THREADS]),
            {error, threadsNumber, iolist_to_binary(Msg)};
        too_small ->
            Msg = io_lib:format("The number of threads can't be less than ~p",
                                [?MIN_NUM_WORKER_THREADS]),
            {error, threadsNumber, iolist_to_binary(Msg)};
        too_large ->
            Msg = io_lib:format("The number of threads can't be greater "
                                "than ~p", [?MAX_NUM_WORKER_THREADS]),
            {error, threadsNumber, iolist_to_binary(Msg)};
        {ok, X} ->
            {ok, num_threads, X}
    end.

parse_validate_eviction_policy(Params, BCfg, IsNew, IsStorageModeMigration) ->
    IsEphemeral = is_ephemeral(Params, BCfg, IsNew),
    IsMagma = is_magma(Params, BCfg, IsNew, IsStorageModeMigration),
    do_parse_validate_eviction_policy(Params, BCfg, IsEphemeral, IsNew,
                                      IsMagma).

do_parse_validate_eviction_policy(Params, _BCfg, false = _IsEphemeral, IsNew,
                                  IsMagma) ->
    Default = case IsMagma of
                  false ->
                      "valueOnly";
                  true ->
                      "fullEviction"
              end,
    validate_with_missing(proplists:get_value("evictionPolicy", Params),
                          Default, IsNew,
                          fun parse_validate_membase_eviction_policy/1);
do_parse_validate_eviction_policy(Params, _BCfg, true = _IsEphemeral,
                                  IsNew, _IsMagma) ->
    validate_with_missing(proplists:get_value("evictionPolicy", Params),
                          "noEviction", IsNew,
                          fun parse_validate_ephemeral_eviction_policy/1).

parse_validate_membase_eviction_policy("valueOnly") ->
    {ok, eviction_policy, value_only};
parse_validate_membase_eviction_policy("fullEviction") ->
    {ok, eviction_policy, full_eviction};
parse_validate_membase_eviction_policy(_Other) ->
    {error, evictionPolicy,
     <<"Eviction policy must be either 'valueOnly' or 'fullEviction' for couchbase buckets">>}.

parse_validate_ephemeral_eviction_policy("noEviction") ->
    {ok, eviction_policy, no_eviction};
parse_validate_ephemeral_eviction_policy("nruEviction") ->
    {ok, eviction_policy, nru_eviction};
parse_validate_ephemeral_eviction_policy(_Other) ->
    {error, evictionPolicy,
     <<"Eviction policy must be either 'noEviction' or 'nruEviction' for ephemeral buckets">>}.

parse_validate_drift_ahead_threshold(Threshold) ->
    case menelaus_util:parse_validate_number(Threshold, 100, undefined) of
        invalid ->
            {error, driftAheadThresholdMs,
             <<"The drift ahead threshold must be an integer not less than 100ms">>};
        too_small ->
            {error, driftAheadThresholdMs,
             <<"The drift ahead threshold can't be less than 100ms">>};
        {ok, X} ->
            {ok, drift_ahead_threshold_ms, X}
    end.

parse_validate_drift_behind_threshold(Threshold) ->
    case menelaus_util:parse_validate_number(Threshold,
                                             ?MIN_DRIFT_BEHIND_THRESHOLD,
                                             undefined) of
        invalid ->
            Msg = io_lib:format("The drift behind threshold must be an "
                                "integer not less than ~pms",
                                [?MIN_DRIFT_BEHIND_THRESHOLD]),
            {error, driftBehindThresholdMs, iolist_to_binary(Msg)};
        too_small ->
            Msg = io_lib:format("The drift behind threshold can't be less "
                                "than ~pms", [?MIN_DRIFT_BEHIND_THRESHOLD]),
            {error, driftBehindThresholdMs, iolist_to_binary(Msg)};
        {ok, X} ->
            {ok, drift_behind_threshold_ms, X}
    end.

parse_validate_ram_quota(Params, BucketConfig) ->
    RamQuota = case proplists:get_value("ramQuota", Params) of
                   undefined ->
                       %% Provide backward compatibility.
                       proplists:get_value("ramQuotaMB", Params);
                   Ram ->
                       Ram
               end,
    do_parse_validate_ram_quota(RamQuota, BucketConfig).

do_parse_validate_ram_quota(undefined, BucketConfig) when BucketConfig =/= false ->
    {ok, ram_quota, ns_bucket:raw_ram_quota(BucketConfig)};
do_parse_validate_ram_quota(Value, _BucketConfig) ->
    case menelaus_util:parse_validate_number(Value, 0, undefined) of
        invalid ->
            {error, ramQuota,
             <<"The RAM Quota must be specified and must be a positive integer.">>};
        too_small ->
            {error, ramQuota, <<"The RAM Quota cannot be negative.">>};
        {ok, X} ->
            {ok, ram_quota, X * ?MIB}
    end.

parse_validate_bucket_placer_param(What, Other, LowerLimit, Params, IsNew,
                                   BucketConfig) ->
    Err = ?cut(iolist_to_binary(io_lib:format(_, _))),

    case proplists:get_value(atom_to_list(What), Params) of
        undefined ->
            case {IsNew, proplists:get_value(atom_to_list(Other), Params)} of
                {true, Val} when Val =/= undefined ->
                    {error, What, Err("~p must be specified", [What])};
                {false, _} ->
                    case proplists:get_value(What, BucketConfig) of
                        undefined ->
                            ignore;
                        V ->
                            {ok, What, V}
                    end;
                _ ->
                    ignore
            end;
        Value ->
            case IsNew orelse
                proplists:get_value(What, BucketConfig) =/= undefined of
                false ->
                    {error, What, Err("~p cannot be updated since it was not "
                                      "specified during the bucket creation",
                                      [What])};
                true ->
                    case menelaus_util:parse_validate_number(Value, LowerLimit,
                                                             undefined) of
                        invalid ->
                            {error, What, Err("~p must be integer", [What])};
                        too_small ->
                            {error, What, Err("~p must be ~p or more",
                                              [What, LowerLimit])};
                        {ok, X} ->
                            {ok, What, X}
                    end
            end
    end.

parse_validate_conflict_resolution_type("seqno") ->
    {ok, conflict_resolution_type, seqno};
parse_validate_conflict_resolution_type("lww") ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            {ok, conflict_resolution_type, lww};
        false ->
            {error, conflictResolutionType,
             <<"Conflict resolution type 'lww' is supported only in enterprise edition">>}
    end;
parse_validate_conflict_resolution_type("custom") ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            case cluster_compat_mode:is_developer_preview() of
                true ->
                    {ok, conflict_resolution_type, custom};
                false ->
                    {error, conflictResolutionType,
                     <<"Conflict resolution type 'custom' is supported only "
                       "with developer preview enabled">>}
            end;
        false ->
            {error, conflictResolutionType,
             <<"Conflict resolution type 'custom' is supported only in "
               "enterprise edition">>}
    end;
parse_validate_conflict_resolution_type(_Other) ->
    {error, conflictResolutionType,
     <<"Conflict resolution type must be 'seqno' or 'lww' or 'custom'">>}.

parse_validate_fusion_logstore_uri(
  Params, _IsNew, _Is79, _IsEnterprise = false) ->
    parse_validate_param_not_enterprise(?FUSION_LOGSTORE_URI, Params);
parse_validate_fusion_logstore_uri(
  Params, _IsNew, _Is79 = false, _IsEnterprise) ->
    parse_validate_param_not_supported(
      ?FUSION_LOGSTORE_URI, Params, fun not_supported_until_79_error/1);
parse_validate_fusion_logstore_uri(
  Params, _IsNew = false, _Is79, _IsEnterprise) ->
    parse_validate_create_only(?FUSION_LOGSTORE_URI, Params);
parse_validate_fusion_logstore_uri(
  Params, _IsNew = true, _Is79 = true, _IsEnterprise = true) ->
    IsMagma = is_magma(Params, undefined, true, false),
    case IsMagma of
        false ->
            parse_validate_param_not_supported(
              ?FUSION_LOGSTORE_URI, Params, fun only_supported_on_magma/1);
        true ->
            case proplists:get_value(?FUSION_LOGSTORE_URI, Params) of
                undefined ->
                    ignore;
                Value ->
                    case misc:is_valid_uri(Value, ["s3", "local"]) of
                        true ->
                            {ok, magma_fusion_logstore_uri, Value};
                        false ->
                            {error, ?FUSION_LOGSTORE_URI,
                             <<"Must be a valid uri">>}
                    end
            end
    end.

%% We are not validating any compat mode here, we need to support this change in
%% a maintenance release due to a memcached behaviour change.
parse_validate_workload_pattern_default(Params) ->
    Value = proplists:get_value("workloadPatternDefault", Params, undefined),
    case Value of
        "readHeavy" ->
            {ok, workload_pattern_default, read_heavy};
        "writeHeavy" ->
            {ok, workload_pattern_default, write_heavy};
        "mixed" ->
            {ok, workload_pattern_default, mixed};
        undefined ->
            ignore;
        _ ->
            {error, workloadPatternDefault,
             <<"Workload pattern default must be 'readHeavy', 'writeHeavy' or "
               "'mixed'">>}
    end.

handle_compact_bucket(_PoolId, Bucket, Req) ->
    ok = compaction_api:force_compact_bucket(Bucket),
    reply(Req, 200).

handle_purge_compact_bucket(_PoolId, Bucket, Req) ->
    ok = compaction_api:force_purge_compact_bucket(Bucket),
    reply(Req, 200).

handle_cancel_bucket_compaction(_PoolId, Bucket, Req) ->
    ok = compaction_api:cancel_forced_bucket_compaction(Bucket),
    reply(Req, 200).

handle_compact_databases(_PoolId, Bucket, Req) ->
    ok = compaction_api:force_compact_db_files(Bucket),
    reply(Req, 200).

handle_cancel_databases_compaction(_PoolId, Bucket, Req) ->
    ok = compaction_api:cancel_forced_db_compaction(Bucket),
    reply(Req, 200).

handle_compact_view(_PoolId, Bucket, DDocId, Req) ->
    ok = compaction_api:force_compact_view(Bucket, DDocId),
    reply(Req, 200).

handle_cancel_view_compaction(_PoolId, Bucket, DDocId, Req) ->
    ok = compaction_api:cancel_forced_view_compaction(Bucket, DDocId),
    reply(Req, 200).

handle_ddocs_list(PoolId, BucketName, Req) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    Nodes = ns_bucket:get_view_nodes(BucketConfig),
    case run_on_node({?MODULE, get_ddocs_list, [PoolId, BucketName]},
                     Nodes, Req) of
        {ok, DDocs} -> reply_json(Req, DDocs);
        {error, nonodes} -> reply_json(Req, {[{error, no_ddocs_service}]}, 400)
    end.

run_on_node({M, F, A}, Nodes, Req) ->
    case lists:member(node(), Nodes) of
        true -> {ok, erlang:apply(M, F, A)};
        _ when Nodes == [] -> {error, nonodes};
        _ ->
            Node = menelaus_util:choose_node_consistently(Req, Nodes),
            case rpc:call(Node, M, F, A) of
                {badrpc, _} = Error ->
                    ?log_error("RPC to node ~p (~p:~p) failed: ~p",
                               [Node, M, F, Error]),
                    {error, Error};
                Docs -> {ok, Docs}
            end
    end.

%% The function might be rpc'ed beginning from 6.5
get_ddocs_list(PoolId, Bucket) ->
    DDocs = capi_utils:sort_by_doc_id(capi_utils:full_live_ddocs(Bucket)),
    RV =
        [begin
             Id = capi_utils:extract_doc_id(Doc),
             {[{doc, capi_utils:couch_doc_to_json(Doc, parsed)},
               {controllers,
                {[{compact,
                   bin_concat_path(["pools", PoolId, "buckets", Bucket, "ddocs",
                                    Id, "controller", "compactView"])},
                  {setUpdateMinChanges,
                   bin_concat_path(["pools", PoolId, "buckets", Bucket,
                                    "ddocs", Id, "controller",
                                    "setUpdateMinChanges"])}]}}]}
         end || Doc <- DDocs],
    {[{rows, RV}]}.

handle_set_ddoc_update_min_changes(_PoolId, Bucket, DDocIdStr, Req) ->
    DDocId = list_to_binary(DDocIdStr),

    case ns_couchdb_api:get_doc(Bucket, DDocId) of
        {ok, #doc{body={Body}} = DDoc} ->
            {Options0} = proplists:get_value(<<"options">>, Body, {[]}),
            Params = mochiweb_request:parse_post(Req),

            {Options1, Errors} =
                lists:foldl(
                  fun (Key, {AccOptions, AccErrors}) ->
                          BinKey = list_to_binary(Key),
                          %% just unset the option
                          AccOptions1 = lists:keydelete(BinKey, 1, AccOptions),

                          case proplists:get_value(Key, Params) of
                              undefined ->
                                  {AccOptions1, AccErrors};
                              Value ->
                                  case menelaus_util:parse_validate_number(
                                         Value, 0, undefined) of
                                      {ok, Parsed} ->
                                          AccOptions2 =
                                              [{BinKey, Parsed} | AccOptions1],
                                          {AccOptions2, AccErrors};
                                      Error ->
                                          Msg = io_lib:format(
                                                  "Invalid ~s: ~p",
                                                  [Key, Error]),
                                          AccErrors1 =
                                              [{Key, iolist_to_binary(Msg)}],
                                          {AccOptions, AccErrors1}
                                  end
                          end
                  end, {Options0, []},
                  ["updateMinChanges", "replicaUpdateMinChanges"]),

            case Errors of
                [] ->
                    complete_update_ddoc_options(Req, Bucket, DDoc, Options1);
                _ ->
                    reply_json(Req, {Errors}, 400)
            end;
        {not_found, _} ->
            reply_json(Req, {[{'_', <<"Design document not found">>}]}, 400)
    end.

complete_update_ddoc_options(Req, Bucket, #doc{body={Body0}}= DDoc, Options0) ->
    Options = {Options0},
    NewBody0 = [{<<"options">>, Options} |
                lists:keydelete(<<"options">>, 1, Body0)],

    NewBody = {NewBody0},
    NewDDoc = DDoc#doc{body=NewBody},
    ok = ns_couchdb_api:update_doc(Bucket, NewDDoc),
    reply_json(Req, Options).

handle_local_random_key(Bucket, Scope, Collection, Req) ->
    menelaus_web_collections:assert_api_available(Bucket),
    do_handle_local_random_key(
      Bucket,
      menelaus_web_crud:assert_collection_uid(Bucket, Scope, Collection),
      Req).

handle_local_random_key(Bucket, Req) ->
    CollectionUid = menelaus_web_crud:assert_default_collection_uid(Bucket),
    do_handle_local_random_key(Bucket, CollectionUid, Req).

do_handle_local_random_key(Bucket, CollectionUId, Req) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    Nodes = ns_bucket:get_servers(BucketConfig),

    Args = [X || X <- [Bucket, CollectionUId],
                 X =/= undefined],
    {ok, Res} = run_on_node({ns_memcached, get_random_key, Args},
                            Nodes, Req),
    case Res of
        {ok, Key} ->
            reply_json(Req, {[{ok, true},
                              {key, Key}]});
        {memcached_error, key_enoent, _} ->
            ?log_debug("No keys were found for bucket ~p. "
                       "Fallback to all docs approach.", [Bucket]),
            reply_json(Req, {[{ok, false},
                              {error, <<"fallback_to_all_docs">>}]}, 404);
        {memcached_error, Status, Msg} ->
            ?log_error("Unable to retrieve random key for bucket ~p. "
                       "Memcached returned error ~p. ~p",
                       [Bucket, Status, Msg]),
            reply_json(Req, {[{ok, false}]}, 404)
    end.

build_terse_bucket_info(BucketName) ->
    case bucket_info_cache:terse_bucket_info(BucketName) of
        {ok, _, _, V} -> V;
        %% NOTE: {auth_bucket for this route handles 404 for us albeit
        %% harmlessly racefully
        not_present ->
            %% Bucket disappeared from under us
            {error, not_present};
        {T, E, Stack} ->
            erlang:raise(T, E, Stack)
    end.

serve_short_bucket_info(BucketName, Req) ->
    case build_terse_bucket_info(BucketName) of
        {error, not_present} ->
            menelaus_util:reply_json(Req, <<"Bucket not found">>, 404);
        V ->
            menelaus_util:reply_ok(Req, "application/json", V)
    end.

serve_streaming_short_bucket_info(BucketName, Req) ->
    handle_streaming(
      fun (_, _UpdateID) ->
              case build_terse_bucket_info(BucketName) of
                  {error, not_present} ->
                      exit(normal);
                  V ->
                      {just_write, {write, V}}
              end
      end, Req).


-ifdef(TEST).
%% for test
basic_bucket_params_screening(IsNew, Name, Params, AllBuckets) ->
    basic_bucket_params_screening(IsNew, Name, Params, AllBuckets,
                                  [node1, node2]).

basic_bucket_params_screening(IsNew, Name, Params, AllBuckets, KvNodes) ->
    basic_bucket_params_screening(IsNew, Name, Params, AllBuckets, KvNodes,
                                  true).

basic_bucket_params_screening(IsNew, Name, Params, AllBuckets,
                              KvNodes, IsEnterprise) ->
    Version = cluster_compat_mode:supported_compat_version(),
    Groups = [[{uuid, N},
               {name, N},
               {nodes, [N]}] || N <- KvNodes],
    BucketUUID =
        case IsNew of
            true -> <<"7a3e8e249d8a2f9dabd757ec4dfcbc03">>;
            false -> not_present
        end,
    Ctx = init_bucket_validation_context(IsNew, Name, BucketUUID, AllBuckets,
                                         KvNodes, Groups, [],
                                         false, false,
                                         Version, IsEnterprise,
                                         %% Change when developer_preview
                                         %% defaults to false
                                         true),
    basic_bucket_params_screening(Ctx, Params).

basic_bucket_params_screening_setup() ->
    Modules = [config_profile, ns_config, cluster_compat_mode, collections,
               ns_bucket],
    meck:new(Modules, [passthrough]),
    meck:expect(config_profile, search,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    meck:expect(ns_config, read_key_fast,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(ns_config, search,
                fun (couchbase_num_vbuckets_default) ->
                        %% This value is what the tests are using.
                        {value, 16}
                end),
    meck:expect(ns_config, search_node_with_default,
                fun (_, Default) -> Default end),
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> true end),
    meck:expect(cluster_compat_mode, is_cluster_79,
                fun () -> true end),
    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> true end),
    meck:expect(ns_config, search_node_with_default,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(collections, num_collections,
                fun(_Name, direct) -> 0 end),
    meck:expect(ns_bucket, validate_encryption_secret,
                fun(_Id, _BucketName, direct) -> ok end),

    %% Return mecked modules for teardown to unload
    Modules.

basic_bucket_params_screening_t() ->
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    AllBuckets = [{"mcd",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, [node1, node2]},
                    {ram_quota, 100 * ?MIB}]},
                  {"default",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, [node1, node2]},
                    {ram_quota, 512 * ?MIB}]},
                  {"third",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {storage_mode, couchstore},
                    {num_replicas, 1},
                    {servers, [node1, node2]},
                    %% Used for test to verify this cannot be disabled
                    {cross_cluster_versioning_enabled, true},
                    {ram_quota, 768 * ?MIB}]},
                  {"fourth",
                   [{type, membase},
                    {storage_mode, couchstore},
                    {num_vbuckets, 16},
                    {num_replicas, 3},
                    {servers, [node1, node2]},
                    {ram_quota, 100 * ?MIB}]},
                  {"fifth",
                   [{type, membase},
                    {storage_mode, couchstore},
                    {num_vbuckets, 16},
                    {num_replicas, 0},
                    {servers, [node1]},
                    {ram_quota, 300 * ?MIB}]}],

    %% it is possible to create bucket with ok params
    {OK1, E1} = basic_bucket_params_screening(true, "mcd",
                                              [{"bucketType", "membase"},
                                               {"storageBackend", "couchstore"},
                                               {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                              tl(AllBuckets)),
    [] = E1,
    %% missing fields have their defaults set
    true = proplists:is_defined(num_threads, OK1),
    true = proplists:is_defined(eviction_policy, OK1),
    true = proplists:is_defined(replica_index, OK1),

    % Not enterprise/magma/7.2.0+ so history not set
    ?assertNot(proplists:is_defined(history_retention_seconds, OK1)),
    ?assertNot(proplists:is_defined(history_retention_bytes, OK1)),
    ?assertNot(proplists:is_defined(history_retention_collection_default, OK1)),

    % Only supported on magma
    ?assertNot(proplists:is_defined(continuousBackupEnabled, OK1)),
    ?assertNot(proplists:is_defined(continuousBackupInterval, OK1)),
    ?assertNot(proplists:is_defined(continuousBackupLocation, OK1)),

    %% it is not possible to create bucket with duplicate name
    {_OK2, E2} = basic_bucket_params_screening(true, "mcd",
                                               [{"bucketType", "membase"},
                                                {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    true = lists:member(name, proplists:get_keys(E2)), % mcd is already present

    %% it is not possible to update missing bucket. And specific format of errors
    {OK3, E3} = basic_bucket_params_screening(false, "missing",
                                              [{"bucketType", "membase"},
                                               {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                              AllBuckets),
    [] = OK3,
    [name] = proplists:get_keys(E3),

    %% it is not possible to update missing bucket. And specific format of errors
    {OK4, E4} = basic_bucket_params_screening(false, "missing",
                                              [],
                                              AllBuckets),
    [] = OK4,
    [name] = proplists:get_keys(E4),

    %% it is not possible to update missing bucket. And specific format of errors
    {OK5, E5} = basic_bucket_params_screening(false, "missing",
                                              [{"ramQuota", "222"}],
                                              AllBuckets),
    [] = OK5,
    [name] = proplists:get_keys(E5),

    %% it is possible to update only some fields
    {OK6, E6} = basic_bucket_params_screening(false, "third",
                                              [{"bucketType", "membase"},
                                               {"replicaNumber", "2"}],
                                              AllBuckets),
    {num_replicas, 2} = lists:keyfind(num_replicas, 1, OK6),
    [] = E6,
    ?assertEqual(false, lists:keyfind(num_threads, 1, OK6)),
    ?assertEqual(false, lists:keyfind(eviction_policy, 1, OK6)),
    ?assertEqual(false, lists:keyfind(replica_index, 1, OK6)),

    {_OK8, E8} = basic_bucket_params_screening(true, undefined,
                                               [{"bucketType", "membase"},
                                                {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    ?assertEqual([{name, <<"Bucket name needs to be specified">>}], E8),

    {_OK9, E9} = basic_bucket_params_screening(false, undefined,
                                               [{"bucketType", "membase"},
                                                {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    ?assertEqual([{name, <<"Bucket with given name doesn't exist">>}], E9),

    %% it is not possible to create bucket with duplicate name in different register
    {_OK10, E10} = basic_bucket_params_screening(true, "Mcd",
                                                 [{"bucketType", "membase"},
                                                  {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                                 AllBuckets),
    ?assertEqual([{name, <<"Bucket with given name already exists">>}], E10),

    %% it is not possible to create bucket with name longer than 100 characters
    {_OK11, E11} = basic_bucket_params_screening(true, "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901",
                                                 [{"bucketType", "membase"},
                                                  {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                                 AllBuckets),
    ?assertEqual([{name, ?l2b(io_lib:format("Bucket name cannot exceed ~p characters",
                                            [?MAX_BUCKET_NAME_LEN]))}], E11),

    %% it is possible to update optional fields
    {OK12, E12} = basic_bucket_params_screening(false, "third",
                                                [{"bucketType", "membase"},
                                                 {"threadsNumber", "8"},
                                                 {"evictionPolicy", "fullEviction"}],
                                                AllBuckets),
    [] = E12,
    ?assertEqual(8, proplists:get_value(num_threads, OK12)),
    ?assertEqual(full_eviction, proplists:get_value(eviction_policy, OK12)),

    %% it is not possible to CREATE a bucket or UPDATE an existing bucket
    %% with 3 replicas and durability level that isn't none.
    DurabilityLevels = ["majority", "majorityAndPersistActive",
                        "persistToMajority"],
    lists:map(
      fun (Level) ->
              %% Create
              {_OK14a, E14a} = basic_bucket_params_screening(
                                 true, "ReplicaDurability",
                                 [{"bucketType", "membase"},
                                  {"ramQuota", "400"},
                                  {"replicaNumber", "3"},
                                  {"durabilityMinLevel", Level}],
                                 AllBuckets),
              ?assertEqual([{durability_min_level,
                             <<"Durability minimum level cannot be specified "
                               "with 3 replicas">>}],
                           E14a),

              %% Update
              {_OK14b, E14b} = basic_bucket_params_screening(
                                 false, "fourth",
                                 [{"durabilityMinLevel", Level}],
                                 AllBuckets),
              ?assertEqual([{durability_min_level,
                             <<"Durability minimum level cannot be specified "
                               "with 3 replicas">>}],
                           E14b)
      end, DurabilityLevels),

    %% it is possible to create a bucket with 3 replicas when durability is
    %% none.
    lists:map(
      fun (BucketType) ->
              {_OK15, E15} = basic_bucket_params_screening(
                               true, "ReplicaDurability",
                               [{"bucketType", BucketType},
                                {"ramQuota", "400"},
                                {"replicaNumber", "3"},
                                {"durabilityMinLevel", "none"}],
                               AllBuckets),
              [] = E15
      end, ["membase", "ephemeral"]),

    %% is is not possible to create an ephemeral bucket with 3 replicas and
    %% durability level that isn't none.
    {_OK16, E16} = basic_bucket_params_screening(
                     true, "ReplicaDurability",
                     [{"bucketType", "ephemeral"},
                      {"ramQuota", "400"},
                      {"replicaNumber", "3"},
                      {"durabilityMinLevel", "majority"}],
                     AllBuckets),
    ?assertEqual([{durability_min_level,
                   <<"Durability minimum level cannot be specified "
                     "with 3 replicas">>}],
                 E16),

    %% it is possible to crete a bucket using the deprecated ramQuotaMB
    %% (to ensure backwards compatibility).
    {OK17, E17} = basic_bucket_params_screening(
                   true, "Bucket17", [{"bucketType", "membase"},
                    {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
                   AllBuckets),
    [] = E17,
    true = proplists:is_defined(ram_quota, OK17),

    %% it is not possible to create or update a bucket with 1 and 2 replicas
    %% and durability level that isn't none if there is one active kv node
    MajorityDurabilityLevelReplicas = [{D, R} || D <- DurabilityLevels, R <- ["1", "2"]],

    lists:map(
      fun ({DurabilityLevel, ReplicaNumber}) ->
              %% Create
              {_OK18a, E18a} = basic_bucket_params_screening(
                                 true, "ReplicaDurability",
                                 [{"bucketType", "membase"},
                                  {"storageBackend", "couchstore"},
                                  {"ramQuota", "400"},
                                  {"replicaNumber", ReplicaNumber},
                                  {"durabilityMinLevel", DurabilityLevel}],
                                 AllBuckets,
                                 [node1]),
              ?assertEqual([{durability_min_level,
                             <<"You do not have enough data servers to "
                               "support this durability level">>}],
                           E18a),

              %% Update
              {_OK18b, E18b} = basic_bucket_params_screening(
                                 false, "fifth",
                                 [{"replicaNumber", ReplicaNumber},
                                  {"durabilityMinLevel", DurabilityLevel}],
                                 AllBuckets,
                                 [node1]),
              ?assertEqual([{durability_min_level,
                             <<"You do not have enough data servers to "
                               "support this durability level">>}],
                           E18b)
      end, MajorityDurabilityLevelReplicas),

    %% it is possible to create a bucket with 1 and 2 replicas and
    %% durability level that isn't none if there is more than one active kv node
    lists:map(
      fun ({DurabilityLevel, ReplicaNumber}) ->
              {_OK19, E19} = basic_bucket_params_screening(
                               true, "ReplicaDurability",
                               [{"bucketType", "membase"},
                                {"ramQuota", "400"},
                                {"replicaNumber", ReplicaNumber},
                                {"durabilityMinLevel", DurabilityLevel}],
                               AllBuckets,
                               [node1, node2]),
              [] = E19
      end, MajorityDurabilityLevelReplicas),

    {_OK20, E20} = basic_bucket_params_screening(
                     true,
                     "HistoryNotEnterpriseMagma",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"storageBackend", "magma"},
                      {"historyRetentionSeconds", "10"},
                      {"historyRetentionBytes", "10"},
                      {"historyRetentionCollectionDefault", "true"},
                      {"magmaKeyTreeDataBlockSize", "10"},
                      {"magmaSeqTreeDataBlockSize", "10"}],
                     AllBuckets,
                     [node1],
                     false),
    ?assertEqual([{storageBackend,
                   <<"Magma is supported in enterprise edition only">>},
                  {historyRetentionSeconds,
                   <<"History Retention is supported in enterprise edition "
                     "only">>},
                  {historyRetentionBytes,
                   <<"History Retention is supported in enterprise edition "
                      "only">>},
                  {historyRetentionCollectionDefault,
                   <<"History Retention is supported in enterprise edition "
                     "only">>},
                  {magmaKeyTreeDataBlockSize,
                   <<"Magma data blocksize is supported in enterprise edition "
                     "only">>},
                  {magmaSeqTreeDataBlockSize,
                   <<"Magma data blocksize is supported in enterprise edition "
                     "only">>}],
                 E20),

    {_OK21, E21} = basic_bucket_params_screening(
                     true,
                     "HistoryEnterpriseNotMagma",
                     [{"bucketType", "membase"},
                      {"ramQuota", "400"},
                      {"historyRetentionSeconds", "10"},
                      {"historyRetentionBytes", "10"},
                      {"historyRetentionCollectionDefault", "true"},
                      {"magmaKeyTreeDataBlockSize", "10"},
                      {"magmaSeqTreeDataBlockSize", "10"}],
                     AllBuckets,
                     [node1]),
    ?assertEqual([{historyRetentionSeconds,
                   <<"History Retention can only used with Magma">>},
                  {historyRetentionBytes,
                   <<"History Retention can only used with Magma">>},
                  {historyRetentionCollectionDefault,
                   <<"History Retention can only used with Magma">>},
                  {magmaKeyTreeDataBlockSize,
                   <<"Magma data blocksize can only be used with Magma">>},
                  {magmaSeqTreeDataBlockSize,
                   <<"Magma data blocksize can only be used with Magma">>}],
                 E21),

    meck:expect(cluster_compat_mode, supported_compat_version,
        fun() ->
            ?VERSION_71
        end),

    {_OK22, E22} = basic_bucket_params_screening(
                     true,
                     "HistoryEnterpriseMagma7.1",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"storageBackend", "magma"},
                      {"historyRetentionSeconds", "10"},
                      {"historyRetentionBytes", "10"},
                      {"historyRetentionCollectionDefault", "true"},
                      {"magmaKeyTreeDataBlockSize", "10"},
                      {"magmaSeqTreeDataBlockSize", "10"}],
                     AllBuckets,
                     [node1]),
    ?assertEqual([{historyRetentionSeconds,
                    <<"History Retention cannot be set until the cluster is "
                       "fully 7.2">>},
                  {historyRetentionBytes,
                    <<"History Retention cannot be set until the cluster is "
                      "fully 7.2">>},
                  {historyRetentionCollectionDefault,
                    <<"History Retention cannot be set until the cluster is "
                      "fully 7.2">>},
                  {magmaKeyTreeDataBlockSize,
                   <<"Magma data blocksize cannot be set until the cluster is "
                     "fully running 7.2">>},
                  {magmaSeqTreeDataBlockSize,
                   <<"Magma data blocksize cannot be set until the cluster is "
                     "fully running 7.2">>}],
                 E22),

    %% put back the compat_mode to 7.6
    meck:expect(cluster_compat_mode, supported_compat_version,
                fun() ->
                        ?VERSION_76
                end),

    {_OK23, E23} = basic_bucket_params_screening(
        true,
        "HistoryEnterpriseMagma7.1",
        [{"bucketType", "membase"},
            {"ramQuota", "1024"},
            {"storageBackend", "magma"},
            {"historyRetentionSeconds", "-1"},
            {"historyRetentionBytes", "-1"},
            {"historyRetentionCollectionDefault", "-1"},
            {"magmaKeyTreeDataBlockSize", "-1"},
            {"magmaSeqTreeDataBlockSize", "-1"}],
        AllBuckets,
        [node1]),
    ?assertEqual([{historyRetentionSeconds,
                  <<"Value must be an integer between 0 and "
                    "18446744073709551615, inclusive">>},
                  {historyRetentionBytes,
                   <<"Value must be an integer between 2147483648 and "
                    "18446744073709551615, inclusive">>},
                  {historyRetentionCollectionDefault,
                   <<"Value must be true or false">>},
                  {magmaKeyTreeDataBlockSize,
                   <<"Value must be an integer between 4096 and 131072, "
                    "inclusive">>},
                  {magmaSeqTreeDataBlockSize,
                   <<"Value must be an integer between 4096 and 131072, "
                    "inclusive">>}],
        E23),

    {_OK24, E24} = basic_bucket_params_screening(
        true,
        "HistoryEnterpriseMagma7.1",
        [{"bucketType", "membase"},
            {"ramQuota", "1024"},
            {"storageBackend", "magma"},
            {"historyRetentionSeconds", "18446744073709551616"},
            {"historyRetentionBytes", "18446744073709551616"},
            {"historyRetentionCollectionDefault", "-1"}],
        AllBuckets,
        [node1]),
    ?assertEqual(
        [{historyRetentionSeconds,
            <<"Value must be an integer between 0 and "
            "18446744073709551615, inclusive">>},
        {historyRetentionBytes,
            <<"Value must be an integer between 2147483648 and "
            "18446744073709551615, inclusive">>},
        {historyRetentionCollectionDefault,
            <<"Value must be true or false">>}],
        E24),

    {OK25, E25} = basic_bucket_params_screening(
                     true,
                     "HistoryEnterpriseMagma",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"storageBackend", "magma"},
                      {"historyRetentionSeconds", "10"},
                      {"historyRetentionBytes", "2147483648"},
                      {"historyRetentionCollectionDefault", "true"},
                      {"magmaKeyTreeDataBlockSize", "4096"},
                      {"magmaSeqTreeDataBlockSize", "4096"}],
                     AllBuckets,
                     [node1]),
    ?assertEqual([], E25),
    ?assert(lists:any(fun (Elem) ->
                          Elem =:= {history_retention_seconds, 10}
                      end, OK25)),
    ?assert(lists:any(fun (Elem) ->
                          Elem =:= {history_retention_bytes, 2147483648}
                      end, OK25)),
    ?assert(lists:any(fun (Elem) ->
        Elem =:= {history_retention_collection_default, true}
                      end, OK25)),
    ?assert(lists:any(fun (Elem) ->
                          Elem =:= {magma_key_tree_data_blocksize, 4096}
                      end, OK25)),
    ?assert(lists:any(fun (Elem) ->
                          Elem =:= {magma_seq_tree_data_blocksize, 4096}
                      end, OK25)),

    %% Test defaults for history when we are enterprise/magma/7.2.0+
    {OK26, E26} = basic_bucket_params_screening(
        true,
        "mcd",
        [{"bucketType", "membase"},
         {"ramQuota", "1024"},
         {"replicaNumber", "2"},
         {"storageBackend", "magma"}],
        tl(AllBuckets)),
    ?assertEqual([], E26),

    ?assertEqual({history_retention_seconds, 0},
                  proplists:lookup(history_retention_seconds, OK26)),
    ?assertEqual({history_retention_bytes, 0},
                  proplists:lookup(history_retention_bytes, OK26)),
    ?assertEqual({history_retention_collection_default, true},
                  proplists:lookup(history_retention_collection_default, OK26)),
    ?assertEqual({magma_key_tree_data_blocksize, 4096},
                 proplists:lookup(magma_key_tree_data_blocksize, OK26)),
    ?assertEqual({magma_seq_tree_data_blocksize, 4096},
                 proplists:lookup(magma_seq_tree_data_blocksize, OK26)),

    %% Cannot create a bucket with replicaNumber less than the minimum.
    meck:expect(ns_config, read_key_fast,
                fun (min_replicas_count, _) ->
                        2;
                    (_, Default) ->
                        Default
                end),

    {_OK27, E27} = basic_bucket_params_screening(
        true,
        "bucket27",
        [{"bucketType", "membase"},
         {"ramQuota", "1024"},
         {"replicaNumber", "1"},
         {"storageBackend", "magma"}],
        AllBuckets),
    ?assertEqual([{replicaNumber,
                   <<"Replica number must be equal to or greater than 2">>}],
                 E27),

    %% Cannot create a bucket using the default replicaNumber when it is
    %% less than the minimum.
    {_OK27_1, E27_1} = basic_bucket_params_screening(
        true,
        "bucket27_1",
        [{"bucketType", "membase"},
         {"ramQuota", "1024"},
         {"storageBackend", "magma"}],
        AllBuckets),
    ?assertEqual([{replicaNumber,
                   <<"Replica number must be equal to or greater than 2">>}],
                 E27_1),

    %% Cannot update a bucket's replicaNumber to be less than the minimum.
    {_OK28, E28} = basic_bucket_params_screening(
                     false, "third",
                     [{"replicaNumber", "1"}],
                     AllBuckets),
    ?assertEqual([{replicaNumber,
                   <<"Replica number must be equal to or greater than 2">>}],
                 E28),

    %% Can create a bucket with replicaNumber equal to the minimum.
    {OK29, E29} = basic_bucket_params_screening(
        true,
        "bucket29",
        [{"bucketType", "membase"},
         {"ramQuota", "1024"},
         {"replicaNumber", "2"},
         {"storageBackend", "magma"}],
        AllBuckets),
    ?assertEqual([], E29),
    ?assertEqual({num_replicas, 2}, proplists:lookup(num_replicas, OK29)),

    %% Reset to default value to not affect downstream tests.
    meck:expect(ns_config, read_key_fast,
                fun (min_replicas_count, _) ->
                        0;
                    (_, Default) ->
                        Default
                end),

    %% Cannot create a bucket with enableCrossClusterVersioning specified
    {_OK30, E30} = basic_bucket_params_screening(
                     true,
                     "bucket30",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"enableCrossClusterVersioning", "true"}],
                     AllBuckets),
    ?assertEqual([{enableCrossClusterVersioning,
                   <<"Cross Cluster Versioning cannot be enabled on bucket "
                     "create">>}],
                 E30),

    %% Cannot disable enableCrossClusterVersioning once it has been enabled.
    {_OK32, E32} = basic_bucket_params_screening(
                     false,
                     "third",
                     [{"enableCrossClusterVersioning", "false"}],
                     AllBuckets),
    ?assertEqual([{enableCrossClusterVersioning,
                   <<"Cross Cluster Versioning cannot be disabled once "
                     "it has been enabled">>}],
                 E32),

    %% put back the compat_mode to our current version
    meck:expect(cluster_compat_mode, supported_compat_version,
                fun() ->
                        ?LATEST_VERSION_NUM
                end),

    %% Cannot specify a non-true/false value.
    {_OK33, E33} = basic_bucket_params_screening(
                     true,
                     "bucket33",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"accessScannerEnabled", "neitherTrueNorFalse"}],
                     AllBuckets),
    ?assertEqual([{accessScannerEnabled,
                   <<"accessScannerEnabled must be true or false">>}], E33),

    %% Cannot specify for ephemeral buckets
    {_OK34, E34} = basic_bucket_params_screening(
                     true,
                     "bucket34",
                     [{"bucketType", "ephemeral"},
                      {"ramQuota", "1024"},
                      {"accessScannerEnabled", "true"}],
                     AllBuckets),
    ?assertEqual([{"accessScannerEnabled",
                   <<"Argument is not supported for ephemeral buckets">>}],
                 E34),

    %% Specify invalid values. This isn't intended to be exhaustive. It
    %% tests the parsing/validation of each item.
    {_OK35, E35} = basic_bucket_params_screening(
                     true,
                     "bucket35",
                     [{"bucketType", "membase"},
                      {"storageBackend", "magma"},
                      {"ramQuota", "1024"},
                      {"expiryPagerSleepTime", "-1"},
                      {"memoryLowWatermark", "-888"},
                      {"memoryHighWatermark", "333"},
                      {"continuousBackupEnabled", "hello"},
                      {"continuousBackupInterval", "1"},
                      {"continuousBackupLocation", "yahoo://storage/blob"},
                      {"invalidHlcStrategy", "badvalue"}],
                     AllBuckets),
    ?assertEqual([{expiryPagerSleepTime,
                   <<"The value of expiryPagerSleepTime (-1) must be in the "
                     "range 0 to 18446744073709551615 inclusive">>},
                  {memoryLowWatermark,
                   <<"The value of memoryLowWatermark (-888) must be in the "
                     "range 50 to 89 inclusive">>},
                  {memoryHighWatermark,
                   <<"The value of memoryHighWatermark (333) must be in the "
                     "range 51 to 90 inclusive">>},
                  {continuousBackupEnabled,
                   <<"continuousBackupEnabled must be true or false">>},
                  {continuousBackupInterval,
                   <<"The value of continuousBackupInterval (1) must be "
                     "in the range 2 to 2147483647 inclusive">>},
                  {continuousBackupLocation,
                   <<"Must be a valid path or uri writable by "
                     "'couchbase' user">>},
                  {invalidHlcStrategy,
                   <<"Must be one of [error,ignore,replace]">>}],
                 E35),

    %% Test related values.
    {_OK36, E36} = basic_bucket_params_screening(
                     true,
                     "bucket36",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"memoryLowWatermark", "88"},
                      {"memoryHighWatermark", "77"}],
                     AllBuckets),
    ?assertEqual([{memoryLowWatermark,
                   <<"memoryLowWatermark (88) must be less than "
                     "memoryHighWatermark (77)">>}],
                 E36),

    %% Specify valid values. This isn't intended to be exhaustive. It
    %% tests the parsing/validation of each item.
    {OK37, E37} = basic_bucket_params_screening(
                    true,
                    "bucket37",
                     [{"bucketType", "membase"},
                      {"storageBackend", "magma"},
                      {"ramQuota", "1024"},
                      {"accessScannerEnabled", "false"},
                      {"expiryPagerSleepTime", "12345"},
                      {"memoryLowWatermark", "68"},
                      {"memoryHighWatermark", "70"},
                      {"continuousBackupEnabled", "true"},
                      {"continuousBackupInterval", "123"},
                      {"continuousBackupLocation", "s3://hello/world"}],
                    AllBuckets),
    ?assertEqual([], E37),
    ?assertEqual(false, proplists:get_value(access_scanner_enabled, OK37)),
    ?assertEqual(12345, proplists:get_value(expiry_pager_sleep_time, OK37)),
    ?assertEqual(68, proplists:get_value(memory_low_watermark, OK37)),
    ?assertEqual(70, proplists:get_value(memory_high_watermark, OK37)),
    ?assertEqual(true, proplists:get_value(continuous_backup_enabled, OK37)),
    ?assertEqual(123, proplists:get_value(continuous_backup_interval, OK37)),
    ?assertEqual("s3://hello/world",
                 proplists:get_value(continuous_backup_location, OK37)),

    %% Back to default action
    meck:expect(ns_config, read_key_fast,
                fun (_, Default) ->
                        Default
                end),

    %% Parsing encryption at rest params
    {OK38, E38} = basic_bucket_params_screening(
                    true,
                    "bucket38",
                    [{"bucketType", "membase"},
                     {"ramQuota", "1024"},
                     {"encryptionAtRestKeyId", "1"},
                     {"encryptionAtRestDekRotationInterval", "604800"},
                     {"encryptionAtRestDekLifetime", "2592000"}],
                    AllBuckets),

    ?assertEqual([], E38),
    ?assertEqual(1, proplists:get_value(encryption_secret_id, OK38)),
    ?assertEqual(604800,
                 proplists:get_value(encryption_dek_rotation_interval, OK38)),
    ?assertEqual(2592000,
                 proplists:get_value(encryption_dek_lifetime, OK38)),

    %% Invalid encryption at rest
    {_OK39, E39} = basic_bucket_params_screening(
                     true,
                     "bucket39",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"encryptionAtRestKeyId", "-3"},
                      {"encryptionAtRestDekRotationInterval", "bad"},
                      {"encryptionAtRestDekLifetime", "bad"}],
                     AllBuckets),

    ?assertEqual(<<"invalid secret id">>,
                 proplists:get_value(encryptionAtRestKeyId, E39)),
    ?assertEqual(<<"invalid interval">>,
                 proplists:get_value(encryptionAtRestDekRotationInterval, E39)),
    ?assertEqual(<<"invalid interval">>,
                 proplists:get_value(encryptionAtRestDekLifetime, E39)),

    %% Default encryption at rest params
    {OK40, E40} = basic_bucket_params_screening(
                    true,
                    "bucket40",
                    [{"bucketType", "membase"},
                     {"ramQuota", "1024"}],
                    AllBuckets),

    ?assertEqual([], E40),
    ?assertEqual([], proplists:get_all_values(encryption_secret_id, OK40)),
    ?assertEqual([], proplists:get_all_values(encryption_dek_rotation_interval,
                                              OK40)),
    ?assertEqual([], proplists:get_all_values(encryption_dek_lifetime, OK40)),

    %% Default bucket is magma...and only 100MB ram is needed.
    {OK41, _E41} = basic_bucket_params_screening(
                     true,
                     "bucket41",
                     [{"bucketType", "membase"},
                      {"ramQuota", "100"}],
                     AllBuckets),
    ?assertEqual(magma, proplists:get_value(storage_mode, OK41)),

    %% Number of vbuckets for magma must be 128 or 1024
    {_OK42, E42} = basic_bucket_params_screening(
                     true,
                     "bucket42",
                     [{"bucketType", "membase"},
                      {"numVBuckets", "777"},
                      {"storageBackend", "magma"},
                      {"ramQuota", "100"}],
                     AllBuckets),
    ?assertEqual([{numVBuckets,
                   <<"Number of vbuckets must be 128 or 1024 (magma) or "
                     "1024 (couchstore)">>}],
                 E42),

    %% magma with 1024 vbuckets requires 1GB ram
    {_OK43, E43} = basic_bucket_params_screening(
                     true,
                     "bucket43",
                     [{"bucketType", "membase"},
                      {"numVBuckets", "1024"},
                      {"storageBackend", "magma"},
                      {"ramQuota", "100"}],
                     AllBuckets),
    ?assertEqual([{ramQuota,
                   <<"Ram quota for magma must be at least 1024 MiB">>}],
                 E43),

    %% Number of vbuckets for couchstore must be 1024
    {_OK44, E44} = basic_bucket_params_screening(
                     true,
                     "bucket44",
                     [{"bucketType", "membase"},
                      {"numVBuckets", "333"},
                      {"storageBackend", "couchstore"},
                      {"ramQuota", "100"}],
                     AllBuckets),
    ?assertEqual([{numVBuckets,
                   <<"Number of vbuckets must be 128 or 1024 (magma) or "
                     "1024 (couchstore)">>}],
                 E44),

    {_OK45, []} = basic_bucket_params_screening(
                    true, "bucket45",
                    [{"bucketType", "membase"},
                     {"ramQuota", "100"},
                     {"durabilityImpossibleFallback", "disabled"},
                     {"warmupBehavior", "background"}],
                    AllBuckets),

    {_OK46, []} = basic_bucket_params_screening(
                    true, "bucket46",
                    [{"bucketType", "membase"},
                     {"ramQuota", "100"},
                     {"durabilityImpossibleFallback", "fallbackToActiveAck"},
                     {"warmupBehavior", "blocking"}],
                    AllBuckets),

    {_OK47, []} = basic_bucket_params_screening(
                    true, "bucket46",
                    [{"bucketType", "membase"},
                     {"ramQuota", "100"},
                     {"warmupBehavior", "none"}],
                    AllBuckets),

    {_OK48, E48} = basic_bucket_params_screening(
                     true, "bucket48",
                     [{"bucketType", "membase"},
                      {"ramQuota", "100"},
                      {"durabilityImpossibleFallback", "badValue"},
                      {"warmupBehavior", "badValue"}],
                     AllBuckets),
    ?assertEqual([{durability_impossible_fallback,
                   <<"Durability impossible fallback must be either 'disabled' "
                     "or 'fallbackToActiveAck'">>},
                  {warmup_behavior,
                   <<"Warmup behavior must be either 'background' or "
                     "'blocking' or 'none'">>}],
                 E48),

    %% Reset this so "real" default is used.
    meck:expect(ns_config, search,
                fun (couchbase_num_vbuckets_default) -> false end),

    %% Verify default number of vbuckets for an ephemeral bucket.
    {OK49, []} = basic_bucket_params_screening(
                   true, "bucket49",
                   [{"bucketType", "ephemeral"},
                    {"ramQuota", "100"}],
                   AllBuckets),
    ?assertEqual(?DEFAULT_VBUCKETS_EPHEMERAL,
                 proplists:get_value(num_vbuckets, OK49)),
    %% and put it back
    meck:expect(ns_config, search,
                fun (couchbase_num_vbuckets_default) -> {value, 16} end),

    lists:foreach(
      fun (InvalidHlcArg) ->
              {OK50, []} = basic_bucket_params_screening(
                             true, "bucket50",
                             [{"bucketType", "membase"},
                              {"ramQuota", "100"},
                              {"invalidHlcStrategy", InvalidHlcArg}],
                             AllBuckets),
              ?assertEqual(list_to_atom(InvalidHlcArg),
                           proplists:get_value(invalid_hlc_strategy, OK50))
      end, ["error", "ignore", "replace"]),

    {_OK51, []} = basic_bucket_params_screening(
                    true, "bucket51",
                    [{"bucketType", "membase"},
                     {"ramQuota", "100"},
                     {"dcpConnectionsBetweenNodes", "1"}],
                    AllBuckets),

    {_OK52, E52} = basic_bucket_params_screening(
                     true, "bucket52",
                     [{"bucketType", "membase"},
                      {"ramQuota", "100"},
                      {"dcpConnectionsBetweenNodes", "0"}],
                     AllBuckets),

    ?assertEqual(
       [{dcpConnectionsBetweenNodes,
         <<"The value of dcpConnectionsBetweenNodes (0) must be in the range 1 "
           "to 64 inclusive">>}], E52),

    {_OK53, E53} = basic_bucket_params_screening(
                     true, "bucket53",
                     [{"bucketType", "membase"},
                      {"ramQuota", "100"},
                      {"dcpConnectionsBetweenNodes", "not_an_int"}],
                     AllBuckets),

    ?assertEqual(
       [{dcpConnectionsBetweenNodes,
         <<"The value of dcpConnectionsBetweenNodes (not_an_int) must be a "
           "non-negative integer">>}], E53),

    {_OK54, E54} = basic_bucket_params_screening(
                     true, "bucket54",
                     [{"bucketType", "membase"},
                      {"ramQuota", "100"},
                      {"hlcMaxFutureThreshold", "5"}],
                     AllBuckets),
    ?assertEqual(
       [{hlcMaxFutureThreshold,
         <<"The value of hlcMaxFutureThreshold (5) must be in the range "
           "10 to 2147483647 inclusive">>}], E54),

    {_OK55, []} = basic_bucket_params_screening(
                    true, "bucket55",
                    [{"bucketType", "membase"},
                     {"ramQuota", "100"},
                     {"dcpBackfillIdleProtectionEnabled", "true"}],
                    AllBuckets),

    {_OK56, E56} = basic_bucket_params_screening(
                     true, "bucket56",
                     [{"bucketType", "membase"},
                      {"ramQuota", "100"},
                      {"dcpBackfillIdleProtectionEnabled", "not_a_boolean"}],
                     AllBuckets),

    ?assertEqual(
       [{"dcpBackfillIdleProtectionEnabled",
         <<"\"dcpBackfillIdleProtectionEnabled\" must be true or false">>}],
       E56),

    {OK57, []} = basic_bucket_params_screening(
                   true, "bucket57",
                   [{"bucketType", "membase"},
                    {"ramQuota", "100"},
                    {"storageBackend", "couchstore"}],
                   AllBuckets),
    ?assertEqual(
       true,
       proplists:get_value(dcp_backfill_idle_protection_enabled, OK57)),

    {OK58, []} = basic_bucket_params_screening(
                   true, "bucket58",
                   [{"bucketType", "ephemeral"},
                    {"ramQuota", "100"}],
                   AllBuckets),
    ?assertEqual(
       false,
       proplists:get_value(dcp_backfill_idle_protection_enabled, OK58)),

    {_OK59, E59} = basic_bucket_params_screening(
                     true,
                     "bucket59",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"workloadPatternDefault", "readHeavy"}],
                     AllBuckets),
    ?assertEqual([], E59),

    {_OK60, E60} = basic_bucket_params_screening(
                     true, "bucket60",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"dcpBackfillIdleLimitSeconds", "-1"},
                      {"dcpBackfillIdleDiskThreshold", "101"}],
                     AllBuckets),
    ?assertEqual(
       [{dcpBackfillIdleLimitSeconds,
         <<"The value of dcpBackfillIdleLimitSeconds (-1) must be in the "
           "range 0 to 18446744073709551615 inclusive">>},
        {dcpBackfillIdleDiskThreshold,
         <<"The value of dcpBackfillIdleDiskThreshold (101) must be in the "
           "range 0 to 100 inclusive">>}], E60).

basic_bucket_params_screening_test_() ->
    {setup,
     fun basic_bucket_params_screening_setup/0,
     fun meck:unload/1,
     fun basic_bucket_params_screening_t/0}.


basic_parse_validate_bucket_auto_compaction_settings_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> true end),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, get,
                fun () -> [] end),
    meck:expect(ns_config, search_node_with_default,
                fun (_, Default) ->
                        Default
                end),
    meck:new(config_profile, [passthrough]),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    meck:expect(config_profile, search,
                fun (_, Default) ->
                        Default
                end),
    meck:new(chronicle_kv, [passthrough]),
    meck:expect(chronicle_kv, get,
                fun (_, _) ->
                        {ok,
                         {[{database_fragmentation_threshold, {30, undefined}},
                           {view_fragmentation_threshold, {30, undefined}},
                           {magma_fragmentation_percentage, 50}],
                          {<<"f663189bff34bd2523ee5ff25480d845">>, 4}}}
                end),
    Value0 = parse_validate_bucket_auto_compaction_settings([{"not_autoCompactionDefined", "false"},
                                                             {"databaseFragmentationThreshold[percentage]", "10"},
                                                             {"viewFragmentationThreshold[percentage]", "20"},
                                                             {"parallelDBAndViewCompaction", "false"},
                                                             {"allowedTimePeriod[fromHour]", "0"},
                                                             {"allowedTimePeriod[fromMinute]", "1"},
                                                             {"allowedTimePeriod[toHour]", "2"},
                                                             {"allowedTimePeriod[toMinute]", "3"},
                                                             {"allowedTimePeriod[abortOutside]", "false"}]),
    ?assertMatch(nothing, Value0),
    Value1 = parse_validate_bucket_auto_compaction_settings([{"autoCompactionDefined", "false"},
                                                             {"databaseFragmentationThreshold[percentage]", "10"},
                                                             {"viewFragmentationThreshold[percentage]", "20"},
                                                             {"parallelDBAndViewCompaction", "false"},
                                                             {"allowedTimePeriod[fromHour]", "0"},
                                                             {"allowedTimePeriod[fromMinute]", "1"},
                                                             {"allowedTimePeriod[toHour]", "2"},
                                                             {"allowedTimePeriod[toMinute]", "3"},
                                                             {"allowedTimePeriod[abortOutside]", "false"}]),
    ?assertMatch(false, Value1),
    {ok, Stuff0} = parse_validate_bucket_auto_compaction_settings([{"autoCompactionDefined", "true"},
                                                                   {"databaseFragmentationThreshold[percentage]", "10"},
                                                                   {"viewFragmentationThreshold[percentage]", "20"},
                                                                   {"parallelDBAndViewCompaction", "false"},
                                                                   {"allowedTimePeriod[fromHour]", "0"},
                                                                   {"allowedTimePeriod[fromMinute]", "1"},
                                                                   {"allowedTimePeriod[toHour]", "2"},
                                                                   {"allowedTimePeriod[toMinute]", "3"},
                                                                   {"allowedTimePeriod[abortOutside]", "false"}]),
    Stuff1 = lists:sort(Stuff0),
    ?assertEqual([{allowed_time_period, [{from_hour, 0},
                                         {to_hour, 2},
                                         {from_minute, 1},
                                         {to_minute, 3},
                                         {abort_outside, false}]},
                  {database_fragmentation_threshold, {10, undefined}},
                  {parallel_db_and_view_compaction, false},
                  {view_fragmentation_threshold, {20, undefined}}],
                 Stuff1),
    meck:unload(cluster_compat_mode),
    meck:unload(ns_config),
    meck:unload(config_profile),
    meck:unload(chronicle_kv),
    ok.

combinations(N, Choices) ->
    combinations(N, Choices, [[]]).

combinations(0, _Choices, List) ->
    List;
combinations(N, Choices, List) ->
    combinations(N - 1, Choices,
                 lists:flatmap(fun (C) -> [[C | E] || E <- List] end, Choices)).

parse_validate_fusion_logstore_uri_test_() ->
    Combinations = combinations(5, [true, false]),
    {foreach, fun () -> ok end, fun (_) -> ok end,
     lists:map(
       fun ([IsNew, Is79, IsEnterprise, IsMagma, IsValid]) ->
               {lists:flatten(
                  io_lib:format(
                    "IsNew=~p, Is79=~p, IsEnterprise=~p, IsMagma=~p, "
                    "IsValid=~p",
                    [IsNew, Is79, IsEnterprise, IsMagma, IsValid])),
                fun () ->
                        Uri = case IsValid of
                                  true -> "s3://something";
                                  false  -> "something"
                              end,
                        BackendParam =
                            case IsMagma of
                                true -> [{"storageBackend", "magma"}];
                                false  -> []
                            end,
                        Params = [{"bucketType", "membase"},
                                  {?FUSION_LOGSTORE_URI, Uri}] ++ BackendParam,
                        Resp = parse_validate_fusion_logstore_uri(
                                 Params, IsNew, Is79, IsEnterprise),
                        ExpectedErrors =
                            lists:flatten(
                              [[<<"Must be a valid uri">> || not IsValid],
                               [<<"Argument is only supported for magma "
                                  "buckets">> || not IsMagma],
                               [<<"\"fusionLogstoreURI\" can only be set in "
                                  "Enterprise edition">> || not IsEnterprise],
                               [<<"Argument is not supported until cluster is "
                                  "fully 7.9">> || not Is79],
                               [<<"\"fusionLogstoreURI\" allowed only during "
                                  "bucket creation">> || not IsNew]]),
                        case ExpectedErrors of
                            [] ->
                                ?assertEqual(
                                   {ok, magma_fusion_logstore_uri, Uri}, Resp);
                            _ ->
                                ?assertMatch({error, ?FUSION_LOGSTORE_URI, _},
                                             Resp),
                                {error, ?FUSION_LOGSTORE_URI, E} = Resp,
                                ?assert(lists:member(E, ExpectedErrors))
                        end
                end}
       end, Combinations)}.

parse_validate_max_magma_shards_test() ->
    meck:new(config_profile, [passthrough]),
    meck:expect(config_profile, search,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    Params = [{"bucketType", "membase"},
              {"ramQuota", "400"},
              {"replicaNumber", "3"},
              {"durabilityMinLevel", "majority"},
              {"magmaMaxShards", "101"}],
    BucketConfig = [],
    Version = ?VERSION_76,

    Resp = parse_validate_max_magma_shards(Params, BucketConfig, Version, true),
    ?assertEqual(Resp,
                 {error, magmaMaxShards,
                  <<"Cannot set maximum magma shards in this configuration profile">>}),

    Resp2 = parse_validate_max_magma_shards(Params, BucketConfig, Version, false),
    ?assertEqual(Resp2,
                 {error, magmaMaxShards,
                  <<"Number of maximum magma shards cannot be modified after bucket creation">>}),

    meck:expect(config_profile, search,
                fun (_, _) ->
                        true
                end),
    Params2 = [{"bucketType", "membase"},
              {"ramQuota", "400"},
              {"replicaNumber", "3"},
              {"durabilityMinLevel", "majority"},
              {"magmaMaxShards", "10000"}],
    Resp3 = parse_validate_max_magma_shards(Params2, BucketConfig, Version, true),
    ?assertEqual(Resp3,
                 {error,magmaMaxShards,
                  <<"Cannot set maximum magma shards on non-magma storage backend">>}),

    Msg = erlang:list_to_binary(
            io_lib:format("Must be an integer between ~p and ~p",
                          [?MIN_MAGMA_SHARDS, ?MAX_MAGMA_SHARDS])),
    Params4 = [{"bucketType", "membase"},
               {"ramQuota", "400"},
               {"replicaNumber", "3"},
               {"durabilityMinLevel", "majority"},
               {"magmaMaxShards", "10000"},
               {"storageBackend", "magma"}],
    Resp4 = parse_validate_max_magma_shards(Params4, BucketConfig, Version, true),
    ?assertEqual(Resp4, {error, magmaMaxShards, Msg}),

    Params5 = [{"bucketType", "membase"},
               {"ramQuota", "400"},
               {"replicaNumber", "3"},
               {"durabilityMinLevel", "majority"},
               {"magmaMaxShards", "100"},
               {"storageBackend", "magma"}],
    Resp5 = parse_validate_max_magma_shards(Params5, BucketConfig, Version, true),
    ?assertEqual(Resp5, {ok, magma_max_shards, 100}),

    meck:unload(config_profile),
    ok.

validate_ram_used_76_test() ->
    meck:new(cluster_compat_mode),
    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> true end),

    ?assertEqual([],
        validate_ram(#ram_summary{this_alloc = 1, this_used = 2})),

    ?assertEqual([],
        validate_ram(#ram_summary{this_alloc = 2, this_used = 2})),

    ?assertEqual([],
        validate_ram(#ram_summary{this_alloc = 3, this_used = 2})),

    meck:unload(cluster_compat_mode).

validate_ram_used_pre_76_test() ->
    meck:new(cluster_compat_mode),
    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> false end),

    ?assertEqual(
        [{ramQuota, <<"RAM quota cannot be set below current usage.">>}],
        validate_ram(#ram_summary{this_alloc = 1, this_used = 2})),

    ?assertEqual([],
        validate_ram(#ram_summary{this_alloc = 2, this_used = 2})),

    ?assertEqual([],
        validate_ram(#ram_summary{this_alloc = 3, this_used = 2})),

    meck:unload(cluster_compat_mode).

validate_ram_quota_before_server_list_populated_test() ->
    meck:new(menelaus_stats),
    %% We aren't interested in this
    meck:expect(menelaus_stats,
                bucket_ram_usage, fun(_) -> 0 end),

    meck:new(ns_cluster_membership, [passthrough]),
    meck:expect(ns_cluster_membership, service_active_nodes,
                fun (_) -> [node1] end),

    %% BucketConfig before the server list has been populated, Ram quota: 1MiB
    BucketConfig = [{ram_quota, ?MIB}, {servers, []}],

    %% Bucket is currently using the entire quota, 1MiB
    ClusterStorageTotals = [{ram, [{quotaUsedPerNode, ?MIB},
                                   {quotaTotalPerNode, ?MIB}]}],
    Ctx = #bv_ctx{
             bucket_config=BucketConfig,
             %% Pretend to have 1 node in kv_nodes
             kv_nodes = [node1],
             cluster_storage_totals=ClusterStorageTotals
            },

    %% Attempt to increase ram_quota to 2MiB
    ParsedProps = [{ram_quota, 2*?MIB}],

    Summary = interpret_ram_quota(Ctx, BucketConfig, ParsedProps,
                                  ClusterStorageTotals),

    ?assertEqual(#ram_summary{
                    % Total cluster quota is still 1MiB
                    total=?MIB,
                    % There are no other buckets
                    other_buckets=0,
                    % New per node quota in MiB
                    per_node=2,
                    % Node count is still 1
                    nodes_count=1,
                    % We are trying to allocate 2MiB
                    this_alloc=2*?MIB,
                    % this_used is irrelevant
                    this_used=0,
                    % total - other - new quota = free
                    % 1MiB  - 0     - 2MiB      = -1MiB
                    free=-?MIB},
                 Summary),

    meck:unload(ns_cluster_membership),
    meck:unload(menelaus_stats).

validate_dura_min_level_before_server_list_populated_test() ->
    config_profile:load_default_profile_for_test(),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config,
                read_key_fast, fun(_, Default) -> Default end),
    meck:expect(ns_config, search,
                fun (couchbase_num_vbuckets_default) -> false end),
    meck:expect(ns_config, search_node_with_default,
                fun (_, D) -> D end),
    %% BucketConfig before the server list has been populated
    BucketConfig = [{servers, []},
                    {desired_servers, [node1]},
                    {num_replicas, 1}],
    Ctx = #bv_ctx{
             bucket_config=BucketConfig,
             %% Pretend to have 2 nodes in kv_nodes
             kv_nodes = [node1, node2],
             new = false
            },
    %% Attempt to increase durability_min_level to majority
    Params = [{durability_min_level, majority}],
    Errors = additional_bucket_params_validation(Params, Ctx),
    ?assertEqual([{durability_min_level,
                   <<"You do not have enough data servers to support this "
                     "durability level">>}],
                 Errors),
    config_profile:unload_profile_for_test(),
    meck:unload(ns_config).

validate_dura_min_level_change_when_only_one_kv_node_active_test() ->
    config_profile:load_default_profile_for_test(),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config,
                read_key_fast, fun(_, Default) -> Default end),
    meck:expect(ns_config, search_node_with_default,
                fun (_, D) -> D end),
    BucketConfig = [{servers, [node1]},
                    {desired_servers, [node1]},
                    {num_replicas, 1},
                    {durability_min_level, majority}],
    Ctx = #bv_ctx{
             bucket_config=BucketConfig,
             new = false
            },

    %% Attempt to increase durability_min_level to persistToMajority,
    %% should not work, not enough replicas
    Params0 = [{durability_min_level, persistToMajority}],
    Errors0 = additional_bucket_params_validation(Params0, Ctx),
    ?assertEqual(
       [{durability_min_level,
         <<"You do not have enough data servers to support this durability "
           "level">>}],
       Errors0),

    %% Attempt to set durability_min_level to none, should be fine
    Params1 = [{durability_min_level, none}],
    Errors1 = additional_bucket_params_validation(Params1, Ctx),
    ?assertEqual([], Errors1),

    %% We should be able to change things like ram_quota
    Params2 = [{ram_quota, 4 * ?MIB}],
    Errors2 = additional_bucket_params_validation(Params2, Ctx),
    ?assertEqual([], Errors2),

    %% We should be able to change things like replicas too, provided we change
    %% it to 0.
    Params3 = [{num_replicas, 0}],
    Errors3 = additional_bucket_params_validation(Params3, Ctx),
    ?assertEqual([], Errors3),

    %% But if we use an invalid value for num_replicas that should fail
    Params4 = [{num_replicas, 2}],
    Errors4 = additional_bucket_params_validation(Params4, Ctx),
    ?assertEqual(
       [{durability_min_level,
         <<"You do not have enough data servers to support this durability "
           "level">>}], Errors4),

    Params5 = [{num_replicas, 3}],
    Errors5 = additional_bucket_params_validation(Params5, Ctx),
    ?assertEqual(
       [{durability_min_level,
         <<"Durability minimum level cannot be specified with 3 replicas">>}],
       Errors5),

    %% We can specify values and skip the check if they are the same
    Params6 = [{num_replicas, 1}],
    Errors6 = additional_bucket_params_validation(Params6, Ctx),
    ?assertEqual([], Errors6),

    Params7 = [{durability_min_level, majority}],
    Errors7 = additional_bucket_params_validation(Params7, Ctx),
    ?assertEqual([], Errors7),

    meck:unload(ns_config),
    config_profile:unload_profile_for_test().

get_conflict_resolution_type_and_thresholds_test() ->
    config_profile:load_default_profile_for_test(),
    meck:new(cluster_compat_mode),
    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> false end),

    %% When not specified, conflict_resolution_type gets defaulted to seqno
    ParamsNoOp = [],
    ParsedNoOp = get_conflict_resolution_type_and_thresholds(
                   ParamsNoOp, ignore, false, true),
    ?assertEqual([{ok, conflict_resolution_type, seqno}], ParsedNoOp),

    %% Can't set to lww if not enterprise
    ParamsNotEnterprise = [{"conflictResolutionType", "lww"}],
    ParsedNotEnterprise = get_conflict_resolution_type_and_thresholds(
                            ParamsNotEnterprise, ignore, false, true),
    ?assertEqual([{error, conflictResolutionType,
                   <<"Conflict resolution type 'lww' is supported only in "
                     "enterprise edition">>}], ParsedNotEnterprise),

    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> true end),

    %% Can set to lww when enterprise and drift thresholds get default values
    ParamsEnterprise = [{"conflictResolutionType", "lww"}],
    ParsedEnterprise = get_conflict_resolution_type_and_thresholds(
                         ParamsEnterprise, ignore, false, true),
    ?assertEqual([{ok, conflict_resolution_type, lww},
                  {ok, drift_ahead_threshold_ms, 5000},
                  {ok, drift_behind_threshold_ms, 5000}], ParsedEnterprise),

    %% Drift behind still gets default value when drift ahead specified for lww
    ParamsWithDriftAhead = [{"conflictResolutionType", "lww"},
                            {"driftAheadThresholdMs", "1000"}],
    ParsedWithDriftAhead = get_conflict_resolution_type_and_thresholds(
                             ParamsWithDriftAhead, ignore, false, true),
    ?assertEqual([{ok, conflict_resolution_type, lww},
                  {ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 5000}], ParsedWithDriftAhead),

    %% Both drift thresholds are parsed when lww
    ParamsWithBoth = [{"conflictResolutionType", "lww"},
                      {"driftAheadThresholdMs", "1000"},
                      {"driftBehindThresholdMs", "1000"}],
    ParsedWithBoth = get_conflict_resolution_type_and_thresholds(
                       ParamsWithBoth, ignore, false, true),
    ?assertEqual([{ok, conflict_resolution_type, lww},
                  {ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 1000}], ParsedWithBoth),

    %% Drift thresholds are parsed when updating an lww bucket
    ParamsUpdateDriftLWW = [{"driftAheadThresholdMs", "1000"},
                            {"driftBehindThresholdMs", "1000"}],
    BucketConfigUpdateDriftLWW = [{conflict_resolution_type, lww}],
    ParsedUpdateDriftLWW = get_conflict_resolution_type_and_thresholds(
                             ParamsUpdateDriftLWW, ignore,
                             BucketConfigUpdateDriftLWW, false),
    ?assertEqual([{ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 1000}], ParsedUpdateDriftLWW),

    %% Drift thresholds get default values when history_retention_seconds > 0
    ParamsCreateWithHRS = [],
    HRSCreateWithHRS = {ok, history_retention_seconds, 10},
    ParsedCreateWithHRS = get_conflict_resolution_type_and_thresholds(
                            ParamsCreateWithHRS, HRSCreateWithHRS, false, true),
    ?assertEqual([{ok, conflict_resolution_type, seqno},
                  {ok, drift_ahead_threshold_ms, 5000},
                  {ok, drift_behind_threshold_ms, 5000}],
                 ParsedCreateWithHRS),

    %% Drift thresholds get parsed when HRS (history_retention_seconds) > 0
    ParamsCreateWithHRSAndDrift = [{"driftAheadThresholdMs", "1000"},
                                   {"driftBehindThresholdMs", "1000"}],
    HRSCreateWithHrsAndDrift = {ok, history_retention_seconds, 10},
    ParsedCreateWithHRSAndDrift = get_conflict_resolution_type_and_thresholds(
                                    ParamsCreateWithHRSAndDrift,
                                    HRSCreateWithHrsAndDrift,
                                    false, true),
    ?assertEqual([{ok, conflict_resolution_type, seqno},
                  {ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 1000}],
                 ParsedCreateWithHRSAndDrift),

    %% Drift thresholds get default values when HRS is updated to > 0
    ParamsUpdateHRS = [],
    HRSUpdateHRS = {ok, history_retention_seconds, 10},
    ParsedUpdateHRS = get_conflict_resolution_type_and_thresholds(
                        ParamsUpdateHRS, HRSUpdateHRS, [], false),
    ?assertEqual([{ok, drift_ahead_threshold_ms, 5000},
                  {ok, drift_behind_threshold_ms, 5000}],
                 ParsedUpdateHRS),

    %% Drift thresholds get parsed when HRS is updated from undefined to > 0
    ParamsUpdateHRSAndDrift = [{"driftAheadThresholdMs", "1000"},
                               {"driftBehindThresholdMs", "1000"}],
    HRSUpdateHRSAndDrift = {ok, history_retention_seconds, 10},
    ParsedUpdateHRSAndDrift = get_conflict_resolution_type_and_thresholds(
                                ParamsUpdateHRSAndDrift, HRSUpdateHRSAndDrift,
                                [], false),
    ?assertEqual([{ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 1000}],
                 ParsedUpdateHRSAndDrift),

    %% Drift thresholds get parsed when HRS is updated from 0 to > 0
    ParamsEnableDrift = [{"driftAheadThresholdMs", "1000"},
                         {"driftBehindThresholdMs", "1000"}],
    HRSEnableDrift = {ok, history_retention_seconds, 10},
    BucketConfigEnableDrift = [{history_retention_seconds, 0}],
    ParsedEnableDrift = get_conflict_resolution_type_and_thresholds(
                          ParamsEnableDrift, HRSEnableDrift,
                          BucketConfigEnableDrift, false),
    ?assertEqual([{ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 1000}], ParsedEnableDrift),

    %% Drift thresholds don't get reset when HRS is updated back to > 0
    BucketConfigDontResetDrift = [{drift_ahead_threshold_ms, 1000},
                                  {drift_behind_threshold_ms, 1000}],
    ParsedDontResetDrift = get_conflict_resolution_type_and_thresholds(
                             [], {ok, history_retention_seconds, 10},
                             BucketConfigDontResetDrift, false),
    ?assertEqual([ignore, ignore],
                 ParsedDontResetDrift),

    %% Drift thresholds get parsed when HRS is already > 0
    ParamsUpdateDriftHRS = [{"driftAheadThresholdMs", "1000"},
                            {"driftBehindThresholdMs", "1000"}],
    BucketConfigUpdateDriftHRS = [{history_retention_seconds, 10}],
    ParsedUpdateDriftHRS = get_conflict_resolution_type_and_thresholds(
                             ParamsUpdateDriftHRS, ignore,
                             BucketConfigUpdateDriftHRS, false),
    ?assertEqual([{ok, drift_ahead_threshold_ms, 1000},
                  {ok, drift_behind_threshold_ms, 1000}], ParsedUpdateDriftHRS),

    %% Drift thresholds don't get default values when HRS is set to 0
    HRSDontEnableDrift1 = {ok, history_retention_seconds, 0},
    ParsedDontEnableDrift1 = get_conflict_resolution_type_and_thresholds(
                               [], HRSDontEnableDrift1, false, true),
    ?assertEqual([{ok,conflict_resolution_type, seqno}],
                 ParsedDontEnableDrift1),

    %% Drift thresholds don't get default values when HRS is updated to 0
    HRSDontEnableDrift2 = {ok, history_retention_seconds, 0},
    ParsedDontEnableDrift2 = get_conflict_resolution_type_and_thresholds(
                               [], HRSDontEnableDrift2, [], false),
    ?assertEqual([], ParsedDontEnableDrift2),

    %% Drift thresholds don't get default values when HRS is already 0
    BucketConfigDontEnableDrift3 = [{history_retention_seconds, 0}],
    ParsedDontEnableDrift3 = get_conflict_resolution_type_and_thresholds(
                               [], ignore, BucketConfigDontEnableDrift3, false),
    ?assertEqual([], ParsedDontEnableDrift3),

    meck:unload(cluster_compat_mode),
    config_profile:unload_profile_for_test().

maybe_update_cas_props_test() ->
    meck:new(misc, [passthrough]),
    meck:expect(misc, rpc_multicall_with_plist_result,
        fun(_, _, _, _, _) ->
            NodeARsp = [{0,[{"max_cas","0"}]},
                        {1,[{"max_cas","1"}]},
                        {2,[{"max_cas","100"}]},
                        {3,[{"max_cas","101"}]}],
            NodeBRsp = [{2,[{"max_cas","3"}]},
                        {3,[{"max_cas","4"}]},
                        {4,[{"max_cas","102"}]},
                        {5,[{"max_cas","103"}]}],
            NodeCRsp = [{4,[{"max_cas","5"}]},
                        {5,[{"max_cas","6"}]},
                        {1,[{"max_cas","104"}]},
                        {2,[{"max_cas","105"}]}],

            Res = [{a, {ok, dict:from_list(NodeARsp)}},
                   {b, {ok, dict:from_list(NodeBRsp)}},
                   {c, {ok, dict:from_list(NodeCRsp)}}],
            {Res, [], []}
        end),

    %% The expected max_cas values are the values for each active vBucket
    %% from the map
    ExpectedCas = {vbuckets_max_cas, ["0","1","3","4","5","6"]},

    BCfg1 = [{map, [[a, b, c],[a, b, c],[b, c, d],
                    [b, c, e],[c, b, a], [c, b, a]]}],

    %% Adding just a stubbed out field so it can be rechecked later for
    %% existence
    StubProps = [{a, aValStub}],

    Res = maybe_update_cas_props("B1", BCfg1, StubProps, true),
    ExpectedRes =  {ok, [ExpectedCas | StubProps]},
    ?assertEqual(ExpectedRes, Res),

    meck:expect(misc, rpc_multicall_with_plist_result,
        fun(_, _, _, _, _) ->
            {[{a, {ok, stub_resp}}], [{b, reach_error}], [c]}
        end),
    ?assertEqual({error, node_failures},
                 maybe_update_cas_props("B1", BCfg1, StubProps, true)),
    meck:unload(misc).

build_dynamic_bucket_info_test_setup(Version, IsEnterprise) ->
    meck:new(ns_config, [passthrough]),
    config_profile:load_default_profile_for_test(),
    meck:expect(ns_config, search,
        fun(_, cluster_compat_version, _) ->
            Version
        end),

    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, get_compat_version, fun() -> Version end),
    meck:expect(cluster_compat_mode, is_enterprise, fun() -> IsEnterprise end),

    meck:new(chronicle_compat, [passthrough]),
    meck:expect(chronicle_compat, get,
        fun(cluster_compat_version, _) ->
            Version
        end),

    meck:new(ns_bucket, [passthrough]),
    meck:new(menelaus_web_node, [passthrough]),
    meck:expect(menelaus_web_node, get_snapshot,
        fun(_) ->
            []
        end),
    meck:new(menelaus_stats, [passthrough]),
    meck:expect(menelaus_stats, basic_stats,
        fun(_, _) ->
            []
        end).

build_dynamic_bucket_info_test_teardown() ->
    meck:unload().

%% Test the output of build_dynamic_bucket_info. Aspirationally this would test
%% the entire output of the function, but for now it just tests a subset of it.
build_dynamic_bucket_info_test(Version, IsMagma) ->
    BucketConfigBase = [{type, membase},
                        {num_replicas, 1},
                        {ram_quota, 1024},
                        {servers, ["a"]},
                        {num_thread, 3}],
    BucketConfig = BucketConfigBase ++
                   case IsMagma of
                       true -> [{storage_mode, magma}];
                       false -> [{storage_mode, couchstore}]
                   end,

    meck:expect(ns_bucket, get_bucket, fun("Bucket") -> {ok, BucketConfig} end),

    BucketInfo = build_dynamic_bucket_info([], "Bucket", BucketConfig, []),
    ?assertNotEqual([], BucketInfo),

    case IsMagma of
        false ->
            % No couchstore specific bucket conf, nothing to check here
            ok;
        true ->
            % The output from build_dynamic_bucket_info isn't particularly nice,
            % various elements are lists that are unnamed, such as the magma
            % bucket configuration that is output. We have to get a little
            % creative in parsing the output to find the magma bucket config as
            % a result.
            MainBucketConf =
                lists:last(
                    lists:filter(
                        fun (List) when is_list(List) ->
                                proplists:is_defined(replicaNumber, List);
                            (_) -> false
                        end,
                        BucketInfo)),

            MagmaBucketConf =
                lists:last(
                    lists:filter(
                        fun (List) when is_list(List) ->
                                proplists:is_defined(storageQuotaPercentage,
                                                     List);
                            (_) -> false
                        end,
                        MainBucketConf)),

            ExpectedConfBase = [{storageQuotaPercentage, 50}],
            ExpectedConf = ExpectedConfBase ++
                           case Version of
                               ?VERSION_71 ->
                                   [];
                               ?VERSION_72 ->
                                   [{historyRetentionSeconds,
                                     ?HISTORY_RETENTION_SECONDS_DEFAULT},
                                    {historyRetentionBytes,
                                     ?HISTORY_RETENTION_BYTES_DEFAULT},
                                    {historyRetentionCollectionDefault,
                                     ?HISTORY_RETENTION_COLLECTION_DEFAULT_DEFAULT},
                                    {magmaKeyTreeDataBlockSize,
                                     ?MAGMA_KEY_TREE_DATA_BLOCKSIZE},
                                    {magmaSeqTreeDataBlockSize,
                                     ?MAGMA_SEQ_TREE_DATA_BLOCKSIZE}]
                           end,

            ?assertEqual(ExpectedConf, MagmaBucketConf)
    end.

build_dynamic_bucket_info_test_() ->
    Tests = [{?VERSION_71, false, false},
             {?VERSION_71, true, false},
             {?VERSION_71, true, true},
             {?VERSION_72, false, false},
             {?VERSION_72, true, false},
             {?VERSION_72, true, true}],

    TestFun = fun({Version, IsEnterprise, IsMagma}, _R) ->
                      {lists:flatten(io_lib:format(
                                       "Version ~p, Enterprise ~p, Magma ~p",
                                       [Version, IsEnterprise, IsMagma])),
                       ?cut(build_dynamic_bucket_info_test(Version, IsMagma))}
              end,

    {foreachx,
        fun ({Version, IsEnterprise, _IsMagma}) ->
            build_dynamic_bucket_info_test_setup(Version, IsEnterprise)
        end,
        fun (_X, _R) ->
            build_dynamic_bucket_info_test_teardown()
        end,
        [{Test, TestFun} || Test <- Tests]}.

storage_mode_migration_meck_modules() ->
    [ns_config, config_profile, cluster_compat_mode, collections].

storage_mode_migration_meck_setup(Version) ->
    meck:new(storage_mode_migration_meck_modules(), [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(ns_config, search_node_with_default,
                fun (_, D) -> D end),

    meck:expect(ns_config, search,
                fun (couchbase_num_vbuckets_default) ->
                        false
                end),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    meck:expect(config_profile, get_value,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(cluster_compat_mode, supported_compat_version,
                fun () ->
                        Version
                end),
    meck:expect(cluster_compat_mode, is_cluster_79,
                fun () ->
                        true
                end),
    meck:expect(collections, num_collections,
                fun (_Name, direct) -> 0 end).

storage_mode_migration_cluster_compat_test(Version, CurrentStorageMode,
                                           NewStorageMode) ->
    Params = [{"storageBackend", NewStorageMode}],
    Bucket = [{"foo",
               [{type, membase},
                {num_vbuckets, 1024},
                {servers, [node1, node2]},
                {ram_quota, 1024 * ?MIB},
                {storage_mode, CurrentStorageMode}]}],
    {Oks, Errors} = basic_bucket_params_screening(
                      false, "foo", Params, Bucket),
    case Version of
        ?VERSION_72 ->
            ?assertEqual([{storageBackend,
                           <<"Storage mode migration is not allowed "
                             "until the entire cluster is "
                             "upgraded to 7.6">>}], Errors);
        ?VERSION_76 ->
            ?assert(proplists:get_value(storage_mode, Oks) =:=
                    list_to_atom(NewStorageMode))
    end.

storage_mode_migration_cluster_compat_test_() ->
    %% TestArg: {ClusterVersion, CurrentStorageMode, NewStorageMode}.
    TestArgs = [{?VERSION_72, couchstore, "magma"},
                {?VERSION_72, magma, "couchstore"},
                {?VERSION_76, couchstore, "magma"},
                {?VERSION_76, magma, "couchstore"}],

    TestFun =
        fun ({Version, CurrentStorageMode, NewStorageMode}, _R) ->
                fun () ->
                        {lists:flatten(
                           io_lib:format("Bucket migration from ~p to ~p."
                                         " Cluster Version: ~p",
                                         [CurrentStorageMode,
                                          NewStorageMode, Version])),
                         ?cut(storage_mode_migration_cluster_compat_test(
                               Version, CurrentStorageMode, NewStorageMode))}
                end
        end,

    {foreachx,
     fun ({Version, _CurrentStorageMode, _NewStorageMode}) ->
             storage_mode_migration_meck_setup(Version)
     end,
     fun (_X, _R) ->
             meck:unload(storage_mode_migration_meck_modules())
     end,
     [{TestArg, TestFun} || TestArg <- TestArgs]}.

storage_mode_migration_ram_quota_test() ->
    storage_mode_migration_meck_setup(?VERSION_76),
    %% bucket rank functions use cluster_compat_mode:is_cluster_76/0
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () ->
                        true
                end),
    meck:expect(cluster_compat_mode, is_enterprise, fun () -> true end),
    Params = [{"storageBackend", "magma"}],
    BaseBucketConfig =
        [{type, membase},
         {num_vbuckets, 1024},
         {servers, [node1, node2]},
         {storage_mode, couchstore}],
    Buckets = [{"foo", BaseBucketConfig ++ [{ram_quota, 100 * ?MIB}]}],

    CheckRamQuotaFun =
        fun (Oks, Expected) ->
                lists:any(fun (KV) ->
                                  KV =:= Expected
                          end, Oks)
        end,

    {_Oks, Errors} = basic_bucket_params_screening(false, "foo", Params,
                                                   Buckets),
    ?assertEqual(Errors,
                 [{ramQuota,
                   <<"Ram quota for magma must be at least 1024 MiB">>}]),

    BucketConfig = BaseBucketConfig ++ [{ram_quota, 1024 * ?MIB}],
    Buckets1 = [{"foo", BucketConfig}],
    {_Oks1, Errors1} = basic_bucket_params_screening(false, "foo", Params,
                                                     Buckets1),
    ?assertEqual(Errors1, []),

    Params1 = [{"storageBackend", "magma"},
               {"ramQuota", "1024"}],
    {Oks2, Error2} = basic_bucket_params_screening(false, "foo", Params1,
                                                   Buckets),
    ?assertEqual(Error2, []),
    ?assert(CheckRamQuotaFun(Oks2, {ram_quota, 1024 * ?MIB})),

    BucketConfig1 = BaseBucketConfig -- [{storage_mode, couchstore}] ++
                        [{storage_mode, magma}, {ram_quota, 1024 * ?MIB}],
    Params2 = [{"storageBackend", "couchstore"},
               {"ramQuota", "100"}],
    Buckets2 = [{"foo", BucketConfig1}],
    {Oks3, Errors3} = basic_bucket_params_screening(false, "foo", Params2,
                                                    Buckets2),
    ?assertEqual(Errors3, []),
    ?assert(CheckRamQuotaFun(Oks3, {ram_quota, 100 * ?MIB})),
    meck:unload(storage_mode_migration_meck_modules()).

storage_mode_migration_validate_attributes(
  {AttributeParseFun, Params, Prop, {Updated, NewValue}}) ->
    Config = [{"foo",
               [{type, membase},
                {num_vbuckets, 1024},
                {servers, [node1, node2]},
                {ram_quota, 1024 * ?MIB},
                {storage_mode, couchstore}] ++ [Prop || Prop =/= none]}],
    Res = AttributeParseFun(Params, Config),
    case Updated of
        true ->
            {ok, _K, V} = Res,
            ?assertEqual(V, NewValue);
        false ->
            ?assertEqual(ignore, Res)
    end.

storage_mode_migration_validate_attributes_test() ->
    meck:new(ns_bucket, [passthrough]),
    meck:expect(ns_bucket, is_ephemeral_bucket,
                fun (_) ->
                        false
                end),
    config_profile:load_default_profile_for_test(),
    BaseParams = [{"storageBackend", "magma"}],
    %% TestArg: {AttributeParseFun, UpdateParams, CurrentProp,
    %%           {Updated, NewValue}}
    TestArgs = [{?cut(parse_validate_eviction_policy(_, _, false, true)),
                 BaseParams ++ [{"evictionPolicy", "valueOnly"}],
                 {eviction_policy, full_eviction},
                 {true, value_only}},
                {?cut(parse_validate_storage_quota_percentage(
                        _, _, false, true, true)),
                 BaseParams ++ [{"storageQuotaPercentage", "25"}],
                 none,
                 {true, 25}},
                {?cut(parse_validate_storage_quota_percentage(
                        _, _, false, true, true)),
                 BaseParams,
                 none,
                 {true, ?MAGMA_STORAGE_QUOTA_PERCENTAGE}}],

    lists:foreach(
      fun (TestArg) ->
              storage_mode_migration_validate_attributes(TestArg)
      end, TestArgs),

    config_profile:unload_profile_for_test(),
    meck:unload(ns_bucket).

parse_validate_storage_mode_setup() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> true end),
    meck:expect(cluster_compat_mode, is_cluster_79,
                fun () -> true end),
    config_profile:load_default_profile_for_test().

parse_validate_storage_mode_test__(
  {{OldStorageMode, NewStorageMode, IsNewBucket, Version,
    IsEnterprise, IsStorageModeMigration, IsServerless}, ExpectedResult}) ->
    Params =
        [{"bucketType", "couchbase"}] ++
        case NewStorageMode of
            undefined -> [];
            _ -> [{"storageBackend", NewStorageMode}]
        end,

    BucketConfig = case OldStorageMode of
                       undefined -> undefined;
                       _ -> [{type, membase},
                             {storage_mode, list_to_atom(OldStorageMode)}]
                   end,

    Res = parse_validate_storage_mode(
            Params, BucketConfig, IsNewBucket, Version,
            IsEnterprise, IsStorageModeMigration, IsServerless),

    ExpectedStorageMode =
        case NewStorageMode of
            undefined ->
                OldStorageMode;
            _ ->
                NewStorageMode
        end,

    case ExpectedResult of
        error ->
            ?assertEqual(element(1, Res), error);
        ok ->
            ?assertEqual(Res, {ok, storage_mode,
                               list_to_atom(ExpectedStorageMode)})
    end.

parse_validate_storage_mode_teardown() ->
    meck:unload(cluster_compat_mode),
    config_profile:unload_profile_for_test().

parse_validate_storage_mode_test_() ->
    %% TestArgs: {{OldStorageMode, NewStorageMode,
    %%             IsNewBucket, Version, IsEnterprise, IsStorageModeMigration,
    %%             IsServerless},
    %%            ExpectedResult}.
    TestArgs =
        [%% New bucket creates.
         {{undefined, "magma", true, ?VERSION_71, true, false, false},
          ok},
         {{undefined, "magma", true, ?VERSION_71, false, false, false},
          error},
         {{undefined, "magma", true, ?VERSION_76, true,
           false, false}, ok},
         {{undefined, "magma", true, ?VERSION_76, false,
           false, false}, error},
         {{undefined, "couchstore", true, ?VERSION_71, true,
           false, false}, ok},
         {{undefined, "couchstore", true, ?VERSION_71, false,
           false, false}, ok},
         {{undefined, "couchstore", true, ?VERSION_76, true,
           false, false}, ok},
         {{undefined, "couchstore", true, ?VERSION_76, false,
           false, false}, ok},
         %% Storage mode migration.
         {{"magma", "couchstore", false, ?VERSION_71, true, true, false},
          error},
         {{"magma", "couchstore", false, ?VERSION_71, false, true, false},
          error},
         {{"magma", "couchstore", false, ?VERSION_76, true,
           true, false}, ok},
         {{"magma", "couchstore", false, ?VERSION_76, false,
           true, false}, error},
         {{"couchstore", "magma", false, ?VERSION_71, true, true, false},
          error},
         {{"couchstore", "magma", false, ?VERSION_71, false,
           true, false}, error},
         {{"couchstore", "magma", false, ?VERSION_76, true,
           true, false}, ok},
         {{"couchstore", "magma", false, ?VERSION_76, true,
           true, false}, ok},
         {{"couchstore", "magma", false, ?VERSION_76, false,
           true, true}, error},
         %% Couchstore bucket updates.
         {{"couchstore", undefined, false, ?VERSION_71, true,
           false, false}, ok},
         {{"couchstore", undefined, false, ?VERSION_71, false,
           false, false}, ok},
         {{"couchstore", undefined, false, ?VERSION_76, true,
           false, false}, ok},
         {{"couchstore", undefined, false, ?VERSION_76, false,
           false, false}, ok},
         %% Magma bucket updates.
         {{"magma", undefined, false, ?VERSION_76, true,
           false, false}, ok}],

    TestFun =
        fun ({{OldStorageMode, NewStorageMode, IsNewBucket, Version,
               IsEnterprise, IsStorageModeMigration, IsServerless},
              ExpectedResult} = Arg) ->
                {lists:flatten(io_lib:format(
                   "OldStorageMode - ~p, NewStorageMode - ~p, IsNewBucket - ~p"
                   " Version - ~p, IsEnterprise - ~p, "
                   "IsStorageModeMigration - ~p, IsServerless - ~p, "
                   "ExpectedResult - ~p",
                   [OldStorageMode, NewStorageMode, IsNewBucket, Version,
                    IsEnterprise, IsStorageModeMigration, IsServerless,
                    ExpectedResult])),
                 fun () ->
                         parse_validate_storage_mode_test__(Arg)
                 end}
        end,

    {foreach,
     fun () -> parse_validate_storage_mode_setup() end,
     fun (_X) -> parse_validate_storage_mode_teardown() end,
     [TestFun(TestArg) || TestArg <- TestArgs]}.

rank_params_screening_test() ->
    %% These tests ensure that all the different cases are handled when we are
    %% fully upgraded to 7.6.
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> true end),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    Params = [{"bucketType", "couchbase"}, {"rank", "0"}],
    IsNew = true,
    ?assertEqual(parse_validate_bucket_rank(Params, IsNew), {ok, rank, 0}),

    Params2 = [{"bucketType", "couchbase"}],
    IsNew2 = false,
    ?assertEqual(parse_validate_bucket_rank(Params2, IsNew2), ignore),

    Params3 = [{"bucketType", "couchbase"}, {"rank", "10"}],
    IsNew3 = true,
    ?assertEqual(parse_validate_bucket_rank(Params3, IsNew3), {ok, rank, 10}),

    Params4 = [{"bucketType", "couchbase"}, {"rank", "100"}],
    IsNew4 = false,
    ?assertEqual(parse_validate_bucket_rank(Params4, IsNew4), {ok, rank, 100}),

    %% This case tests rank=undefined,IsNew=false,Is76=true. This allows
    %% older nodes to join newer ones without issue.
    NoRankParams = [{"bucketType", "couchbase"}],
    NotNew = false,
    ?assertEqual(parse_validate_bucket_rank(NoRankParams, NotNew), ignore),

    %% these tests ensure we return correct value when we are NOT in 7.6
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> false end),
    %% Is76=false, rank=0, IsNew=true
    Params5 = [{"bucketType", "couchbase"}, {"rank", "0"}],
    IsNew5 = true,
    ?assertEqual(parse_validate_bucket_rank(Params5, IsNew5),
                 {error, rank,
                  <<"Bucket rank cannot be set until the cluster is fully "
                    "upgraded to 7.6.">>}),

    %% Is76=false, rank=10, IsNew=false
    Params6 = [{"bucketType", "couchbase"}, {"rank", "10"}],
    IsNew6 = false,
    ?assertEqual(parse_validate_bucket_rank(Params6, IsNew6),
                 {error, rank,
                  <<"Bucket rank cannot be set until the cluster is fully "
                    "upgraded to 7.6.">>}),

    %% Is76=false, rank=undefined, IsNew=false
    ?assertEqual(parse_validate_bucket_rank(NoRankParams, NotNew), ignore),
    config_profile:unload_profile_for_test(),
    meck:unload(cluster_compat_mode).

test_num_replicas_guardrail_validation(#{disk_usage := DiskUsage,
                                         old_num_replicas := OldNumReplicas,
                                         new_num_replicas := NewNumReplicas}) ->
    meck:expect(ns_cluster_membership, node_active_services,
                fun (_Node) -> [kv] end),
    meck:expect(rpc, call,
                fun (_Node, guardrail_monitor, get_disk_data_for_service, [kv],
                     _Timeout) ->
                        case DiskUsage of
                            error -> {error, no_dbdir};
                            _ -> {ok, {"/", 1, DiskUsage}}
                        end
                end),
    Servers = [node1, node2],
    {New, BucketConfig} =
        case OldNumReplicas of
            undefined -> {true, false};
            _ -> {false,
                  [{num_replicas, OldNumReplicas},
                   {servers, Servers}]}
        end,
    Ctx = #bv_ctx{
             bucket_config=BucketConfig,
             kv_nodes = Servers,
             new = New},
    Params = [{num_replicas, NewNumReplicas},
              {durability_min_level, none}],
    additional_bucket_params_validation(Params, Ctx).

num_replicas_guardrail_validation_test_() ->
    ExpectedError1 = [{num_replicas,
                       <<"The following data node(s) have insufficient disk "
                         "space to safely increase the number of replicas: "
                         "node1, node2">>}],
    ExpectedError2 = [{num_replicas,
                       <<"Couldn't determine safety of increasing number of "
                         "replicas as there were errors getting disk usage on "
                         "the following nodes: node1, node2">>}],
    {setup,
     fun () ->
             %% We need unstick, so that we can meck rpc
             config_profile:load_default_profile_for_test(),
             meck:new([rpc], [passthrough, unstick]),
             meck:expect(ns_config, read_key_fast,
                         fun(resource_management, _) ->
                                 [{disk_usage,
                                   [{enabled, true},
                                    {maximum, 90}]}];
                            (_, Default) -> Default end),
             meck:expect(ns_config, search,
                         fun (couchbase_num_vbuckets_default) -> false end),
             meck:expect(ns_config, search_node_with_default,
                         fun ({cb_cluster_secrets, _}, D) -> D end),

             meck:expect(ns_storage_conf, this_node_dbdir,
                         fun () -> {ok, ""} end),
             meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                         fun ([Stats], _) -> {ok, Stats};
                             ([], _) -> none end),
             meck:expect(ns_config, get_timeout,
                         fun (_, Default) -> Default end)
     end,
     fun (_) ->
             meck:unload()
     end,
     [{"num_replicas can't be increased after disk usage guardrail hit",
       ?_assertEqual(ExpectedError1,
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => 91,
                         old_num_replicas => 1,
                         new_num_replicas => 2}))},
      {"num_replicas can't be increased when disk usage is giving an error",
       ?_assertEqual(ExpectedError2,
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => error,
                         old_num_replicas => 1,
                         new_num_replicas => 2}))},
      {"num_replicas can be increased if disk usage guardrail not hit",
       ?_assertEqual([],
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => 90,
                         old_num_replicas => 1,
                         new_num_replicas => 2}))},
      {"num_replicas can be decreased after disk usage guardrail hit",
       ?_assertEqual([],
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => 91,
                         old_num_replicas => 2,
                         new_num_replicas => 1}))},
      {"num_replicas can be set unchanged after disk usage guardrail hit",
       ?_assertEqual([],
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => 91,
                         old_num_replicas => 1,
                         new_num_replicas => 1}))},
      {"num_replicas can be decreased if disk usage guardrail not hit",
       ?_assertEqual([],
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => 90,
                         old_num_replicas => 2,
                         new_num_replicas => 1}))},
      {"num_replicas can be set unchanged if disk usage guardrail not hit",
       ?_assertEqual([],
                     test_num_replicas_guardrail_validation(
                       #{disk_usage => 90,
                         old_num_replicas => 1,
                         new_num_replicas => 1}))}]}.

parse_validate_ephemeral_eviction_policy_test() ->
    ParamsEphemeral = [{"bucketType", "ephemeral"}],
    ParamsNoEviction = ParamsEphemeral ++ [{"evictionPolicy", "noEviction"}],
    ParamsNruEviction = ParamsEphemeral ++ [{"evictionPolicy", "nruEviction"}],
    %% valueOnly is not valid for Ephemeral.
    ParamsInvalid = ParamsEphemeral ++ [{"evictionPolicy", "valueOnly"}],
    NewBucket = true,
    IsEphemeral = true,
    IsMagma = false,
    ?assertEqual({ok, eviction_policy, no_eviction},
                 do_parse_validate_eviction_policy(
                    ParamsNoEviction, [], IsEphemeral,
                    NewBucket, IsMagma)),
    ?assertEqual({ok, eviction_policy, nru_eviction},
                 do_parse_validate_eviction_policy(
                   ParamsNruEviction, [], IsEphemeral,
                   NewBucket, IsMagma)),
    ?assertEqual({ok, eviction_policy, no_eviction},
                 do_parse_validate_eviction_policy(
                   ParamsNoEviction, [], IsEphemeral,
                   not NewBucket, IsMagma)),
    ?assertEqual({ok, eviction_policy, nru_eviction},
                 do_parse_validate_eviction_policy(
                   ParamsNruEviction, [], IsEphemeral,
                   not NewBucket, IsMagma)),
    %% Check invalid eviction policy.
    ?assertEqual(
        {error, evictionPolicy,
         <<"Eviction policy must be either 'noEviction' or 'nruEviction' "
           "for ephemeral buckets">>},
        do_parse_validate_eviction_policy(
            ParamsInvalid, [], IsEphemeral, not NewBucket, IsMagma)),
    %% Check default eviction policy for new buckets.
    ?assertEqual({ok, eviction_policy, no_eviction},
                 do_parse_validate_eviction_policy(
                   ParamsEphemeral, [], IsEphemeral,
                   NewBucket, IsMagma)).
-endif.
