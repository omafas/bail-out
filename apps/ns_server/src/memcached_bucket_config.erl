%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc configuration parameters to create bucket and rules for updating
%%      these parameters in case if they change

-module(memcached_bucket_config).

-include("ns_common.hrl").
-include("ns_bucket.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("cb_cluster_secrets.hrl").

-define(MCD_DISABLED_ENCRYPTION_KEY_ID, <<"">>).
-define(MAGMA_FUSION_AUTH_TOKEN, "chronicle_auth_token").

-record(cfg, {type, name, config, snapshot, engine_config, params}).

-export([get/1,
         get_bucket_config/1,
         ensure/3,
         start_params/4,
         ensure_collections/2,
         get_current_collections_uid/1,
         format_mcd_keys/2]).

params(membase, BucketName, BucketConfig, MemQuota, UUID, DBSubDir) ->
    {DriftAheadThreshold, DriftBehindThreshold} =
        case ns_bucket:drift_thresholds(BucketConfig) of
            undefined ->
                {undefined, undefined};
            {A, B} ->
                {misc:msecs_to_usecs(A), misc:msecs_to_usecs(B)}
        end,

    [{"max_size", [{reload, flush}], MemQuota},
     {"dbname", [restart], DBSubDir},
     {"backend", [], ns_bucket:node_kv_backend_type(BucketConfig)},
     {"couch_bucket", [], BucketName},
     {"max_vbuckets", [], proplists:get_value(num_vbuckets, BucketConfig)},
     {"alog_path", [], filename:join(DBSubDir, "access.log")},
     {"data_traffic_enabled", [], false},
     {"max_num_workers", maybe_restart(),
      proplists:get_value(num_threads, BucketConfig, ?NUM_WORKER_THREADS)},
     {"uuid", [], UUID},
     {"conflict_resolution_type", [],
      ns_bucket:conflict_resolution_type(BucketConfig)},
     {"bucket_type", [], ns_bucket:kv_bucket_type(BucketConfig)},
     {"durability_min_level", [{reload, flush}],
      ns_bucket:durability_min_level(BucketConfig)},
     {"durability_impossible_fallback", [{reload, flush}],
      ns_bucket:durability_impossible_fallback(BucketConfig)},
     {"access_scanner_enabled", [{reload, flush}],
      ns_bucket:get_access_scanner_enabled(BucketConfig)},
     {"exp_pager_stime", [{reload, flush}],
      ns_bucket:get_expiry_pager_sleep_time(BucketConfig)},
     {"mem_low_wat_percent", [{reload, flush}],
      get_memory_watermark(low, BucketConfig)},
     {"mem_high_wat_percent", [{reload, flush}],
      get_memory_watermark(high, BucketConfig)},
     {"warmup_behavior", [{reload, flush}],
      ns_bucket:warmup_behavior(BucketConfig)},
     {"continuous_backup_enabled", [{reload, flush}],
      ns_bucket:get_continuous_backup_enabled(BucketConfig)},
     {"continuous_backup_interval", [{reload, flush}],
      get_continuous_backup_interval(BucketConfig)},
     {"hlc_drift_ahead_threshold_us", [no_param, {reload, vbucket}],
      DriftAheadThreshold},
     {"hlc_drift_behind_threshold_us", [no_param, {reload, vbucket}],
      DriftBehindThreshold},
     {"hlc_invalid_strategy", [{reload, vbucket}],
      get_invalid_hlc_strategy(BucketConfig)},
     {"hlc_max_future_threshold_us", [{reload, vbucket}],
      get_hlc_max_future_threshold(BucketConfig)},
     {"workload_pattern_default", [{reload, flush}],
      ns_bucket:workload_pattern_default(BucketConfig)},
     {"item_eviction_policy", maybe_restart(),
      get_eviction_policy(true, BucketConfig)},
     {"ephemeral_full_policy", [{reload, flush}],
      get_eviction_policy(false, BucketConfig)},
     {"ephemeral_metadata_purge_age", [{reload, flush}],
      ephemeral_metadata_purge_age(BucketConfig)},
     {"persistent_metadata_purge_age", [{reload, flush}],
      persistent_metadata_purge_age(BucketName, BucketConfig)},
     {"max_ttl", [{reload, flush}], proplists:get_value(max_ttl, BucketConfig)},
     {"ht_locks", [], proplists:get_value(
                        ht_locks, BucketConfig,
                        misc:getenv_int("MEMBASE_HT_LOCKS",
                                        ?MEMBASE_HT_LOCKS))},
     {"ht_size", [],
      proplists:get_value(ht_size, BucketConfig,
                          misc:getenv_int("MEMBASE_HT_SIZE", undefined))},
     {"compression_mode", [{reload, flush}],
      proplists:get_value(compression_mode, BucketConfig)},
     {"max_num_shards", [],
      ns_bucket:magma_max_shards(BucketConfig, ?DEFAULT_MAGMA_SHARDS)}] ++
        case cluster_compat_mode:is_cluster_phoenix() of
            false -> [];
            true ->
                [{"dcp_backfill_idle_protection_enabled", [{reload, dcp}],
                  ns_bucket:get_dcp_backfill_idle_protection_enabled(
                    BucketConfig)},
                 {"dcp_backfill_idle_limit_seconds", [{reload, dcp}],
                  ns_bucket:get_dcp_backfill_idle_limit_seconds(BucketConfig)},
                 {"dcp_backfill_idle_disk_threshold", [{reload, dcp}],
                  ns_bucket:get_dcp_backfill_idle_disk_threshold(BucketConfig)}]
        end
        ++ get_magma_bucket_config(BucketConfig);

params(memcached, _BucketName, _BucketConfig, MemQuota, UUID, _DBSubDir) ->
    [{"cache_size", [], MemQuota},
     {"uuid", [], UUID}].

maybe_restart() ->
    case ns_config:read_key_fast(dont_reload_bucket_on_cfg_change, false) of
        false ->
            [restart];
        true ->
            []
    end.

get_continuous_backup_interval(BucketConfig) ->
    case ns_bucket:get_continuous_backup_interval(BucketConfig) of
        undefined ->
            undefined;
        Minutes ->
            Minutes * 60
    end.

get_eviction_policy(Persistent, BucketConfig) ->
    case ns_bucket:is_persistent(BucketConfig) of
        Persistent ->
            case ns_bucket:node_eviction_policy(BucketConfig) of
                nru_eviction ->
                    auto_delete;
                no_eviction ->
                    fail_new_data;
                Other ->
                    Other
            end;
        _ ->
            undefined
    end.

ephemeral_metadata_purge_age(BucketConfig) ->
    case ns_bucket:is_persistent(BucketConfig) of
        true ->
            undefined;
        false ->
            %% Purge interval is accepted in # of days but the ep-engine
            %% needs it to be expressed in seconds.
            Val = proplists:get_value(purge_interval, BucketConfig,
                                      ?DEFAULT_EPHEMERAL_PURGE_INTERVAL_DAYS),
            erlang:round(Val * 24 * 3600)
    end.

persistent_metadata_purge_age(BucketName, BucketConfig) ->
    case ns_bucket:is_persistent(BucketConfig) of
        false ->
            undefined;
        true ->
            %% Purge interval is accepted in # of days but the ep-engine
            %% needs it to be expressed in seconds.  For persistent buckets
            %% the global auto-compaction purge interval may be used.
            PI = compaction_api:get_purge_interval(list_to_binary(BucketName)),
            erlang:round(PI * 24 * 3600)
    end.


get_invalid_hlc_strategy(BucketConfig) ->
    case cluster_compat_mode:is_cluster_phoenix() of
        false ->
            undefined;
        true ->
            ns_bucket:get_invalid_hlc_strategy(BucketConfig)
    end.

get_hlc_max_future_threshold(BucketConfig) ->
    case cluster_compat_mode:is_cluster_phoenix() of
        false ->
            undefined;
        true ->
            Seconds = ns_bucket:get_hlc_max_future_threshold(BucketConfig),
            misc:secs_to_usecs(Seconds)
    end.

get_memory_watermark(Type, BucketConfig) ->
    Watermark = case Type of
                    low ->
                        ns_bucket:get_memory_low_watermark(BucketConfig);
                    high ->
                        ns_bucket:get_memory_high_watermark(BucketConfig)
                end,
    case Watermark of
        undefined ->
            undefined;
        _ ->
            Watermark / 100
    end.

get(BucketName) ->
    Snapshot = ns_bucket:get_snapshot(BucketName),

    case ns_bucket:get_bucket(BucketName, Snapshot) of
        {ok, BucketConfig} ->
            BucketType = proplists:get_value(type, BucketConfig),

            MemQuota = proplists:get_value(ram_quota, BucketConfig),
            UUID = ns_bucket:uuid(BucketName, Snapshot),
            DBSubDir = ns_storage_conf:this_node_bucket_dbdir(BucketName,
                                                              Snapshot),

            Params = params(BucketType, BucketName, BucketConfig, MemQuota,
                            UUID, DBSubDir),

            Engines = ns_config:search_node_prop(ns_config:latest(),
                                                 memcached, engines),
            EngineConfig = proplists:get_value(BucketType, Engines),

            #cfg{type = BucketType, name = BucketName, config = BucketConfig,
                 snapshot = Snapshot, params = Params,
                 engine_config = EngineConfig};
        not_present ->
            {error, not_present}
    end.

query_stats(Sock) ->
    case mc_binary:quick_stats(
           Sock, <<>>,
           fun (<<"ep_", Name/binary>>, V, Dict) ->
                   dict:store(binary_to_list(Name), V, Dict);
               (_, _, Dict) ->
                   Dict
           end, dict:new()) of
        {ok, Stats} ->
            {ok, Stats};
        {memcached_error, not_supported, undefined} ->
            %% This is the method the memcached team has told us to use
            %% to identify a config-only bucket.
            {error, config_only_bucket}
    end.

value_to_string(V) when is_binary(V) ->
    binary_to_list(V);
value_to_string(V) when is_integer(V) ->
    integer_to_list(V);
value_to_string(V) when is_float(V) ->
    float_to_list(V, [{decimals, 2}, compact]);
value_to_string(V) when is_atom(V) ->
    atom_to_list(V);
value_to_string(V) when is_list(V) ->
    V.

value_to_binary(undefined) ->
    undefined;
value_to_binary(V) when is_binary(V) ->
    V;
value_to_binary(V) ->
    list_to_binary(value_to_string(V)).

has_changed(_BucketName, _Name, undefined, _Dict) ->
    false;
has_changed(BucketName, Name, Value, Dict) ->
    case dict:find(Name, Dict) of
        {ok, Value} ->
            false;
        {ok, OldValue} ->
            ?log_info(
               "Detected change of parameter ~s for bucket ~s from ~s to ~s",
               [Name, BucketName, OldValue, Value]),
            true;
        error ->
            ?log_info(
               "Detected change of parameter ~s for bucket ~s to ~s",
               [Name, BucketName, Value]),
            true
    end.

maybe_update_param(_Sock, _Stats, _BucketName,
                   {?MAGMA_FUSION_AUTH_TOKEN, _Props, custom}, undefined) ->
    ok;
maybe_update_param(Sock, _Stats, BucketName,
                   {?MAGMA_FUSION_AUTH_TOKEN, _Props, custom}, JWT) ->
    {_, Payload} = jose_jwt:peek_payload(JWT),
    ?log_info("Push JWT ~p to bucket ~s", [Payload, BucketName]),
    ok = mc_client_binary:set_chronicle_auth_token(Sock, JWT);
maybe_update_param(Sock, Stats, BucketName, {Name, Props, Value}, _JWT) ->
    case proplists:get_value(reload, Props) of
        undefined ->
            ok;
        ReloadType ->
            BinValue = value_to_binary(Value),
            case has_changed(BucketName, Name, BinValue, Stats) of
                true ->
                    ?log_info("Changing parameter ~s of bucket ~s to ~s",
                              [Name, BucketName, BinValue]),
                    ok = mc_client_binary:set_engine_param(
                           Sock, list_to_binary(Name), BinValue, ReloadType);
                false ->
                    ok
            end
    end.

ensure(Sock, #cfg{type = membase, name = BucketName, params = Params}, JWT) ->
    case query_stats(Sock) of
        {ok, Stats} ->
            Restart =
                lists:any(
                  fun ({Name, Props, Value}) ->
                          lists:member(restart, Props) andalso
                              has_changed(BucketName, Name,
                                          value_to_binary(Value), Stats)
                  end, Params),
            case Restart of
                true ->
                    restart;
                false ->
                    lists:foreach(
                      maybe_update_param(Sock, Stats, BucketName, _, JWT),
                      Params)
            end;
        Error ->
            Error
    end;
ensure(Sock, #cfg{type = memcached}, undefined) ->
    %% TODO: change max size of memcached bucket also
    %% Make sure it's a memcached bucket
    {ok, present} = mc_binary:quick_stats(
                      Sock, <<>>,
                      fun (<<"evictions">>, _, _) ->
                              present;
                          (_, _, CD) ->
                              CD
                      end, not_present),
    ok.

format_mcd_keys(ActiveDek, Deks) ->
    format_mcd_keys(ActiveDek, Deks, fun (K) -> K end).
format_mcd_keys(ActiveDek, Deks, Sanitizer) ->
    DeksJsonMcd = lists:filtermap(fun (D) -> format_mcd_key(D, Sanitizer) end,
                                  Deks),
    ActiveKeyMcd = case ActiveDek of
                       undefined -> ?MCD_DISABLED_ENCRYPTION_KEY_ID;
                       #{id := ActiveId} -> ActiveId
                   end,
    {[{keys, DeksJsonMcd}, {active, ActiveKeyMcd}]}.

format_mcd_key(#{id := _Id, type := error}, _) ->
    false;
format_mcd_key(#{id := Id, type := 'raw-aes-gcm', info := #{key := KeyFun}},
               Sanitizer) ->
    Encoded = Sanitizer(base64:encode(KeyFun())),
    {true, {[{id, Id}, {cipher, <<"AES-256-GCM">>}, {key, Encoded}]}}.

get_current_collections_uid(Sock) ->
    case mc_client_binary:get_collections_manifest(Sock) of
        {memcached_error, no_coll_manifest, _} ->
            undefined;
        {ok, Bin} ->
            {Json} = ejson:decode(Bin),
            proplists:get_value(<<"uid">>, Json)
    end.

need_update_collections_manifest(Sock, BucketName, Snapshot) ->
    case collections:uid(BucketName, Snapshot) of
        undefined ->
            false;
        Uid ->
            case get_current_collections_uid(Sock) of
                Uid ->
                    false;
                Other ->
                    {true, Other, Uid}
            end
    end.

ensure_collections(Sock, #cfg{name = BucketName, snapshot = Snapshot}) ->
    case need_update_collections_manifest(Sock, BucketName, Snapshot) of
        false ->
            ok;
        {true, Prev, Next} ->
            Manifest = collections:manifest_json_for_memcached(BucketName,
                                                               Snapshot),
            ?log_debug(
               "Applying collection manifest to bucket ~p due to id change from"
               " ~p to ~p.", [BucketName, Prev, Next]),
            ok = mc_client_binary:set_collections_manifest(
                   Sock, ejson:encode(Manifest)),
            gen_event:notify(buckets_events,
                             {set_collections_manifest,
                              ns_bucket:uuid(BucketName, Snapshot),
                              collections:convert_uid_from_memcached(Next)})
    end.

start_params(#cfg{name=BucketName,
                  config = BucketConfig,
                  snapshot = Snapshot,
                  params = Params,
                  engine_config = EngineConfig}, ActiveDek, Deks, JWT) ->
    Engine = proplists:get_value(engine, EngineConfig),

    StaticConfigString =
        proplists:get_value(
          static_config_string, BucketConfig,
          proplists:get_value(static_config_string, EngineConfig)),
    ExtraConfigString =
        proplists:get_value(
          extra_config_string, BucketConfig,
          proplists:get_value(extra_config_string, EngineConfig, "")),

    DynamicParams =
        lists:filtermap(
          fun ({_Name, _Props, undefined}) ->
                  false;
              ({Name, Props, Value}) ->
                  case lists:member(no_param, Props) of
                      true ->
                          false;
                      false ->
                          {true, Name ++ "=" ++ value_to_string(Value)}
                  end
          end, Params),

    PrepareCfgString =
        fun (ForLogging) ->
                Sanitizer =
                    case ForLogging of
                        true ->
                            fun (_) -> <<"<sanitized>">> end;
                        false ->
                            fun (S) -> S end
                    end,
                EncodedDeks = ejson:encode(format_mcd_keys(ActiveDek,
                                                           Deks, Sanitizer)),
                DeksConfigString = "encryption=" ++ binary_to_list(EncodedDeks),

                JWTConfigString =
                    case JWT of
                        undefined ->
                            "";
                        _ ->
                            JWTParam =
                                case ForLogging of
                                    true ->
                                        {_, Payload} =
                                            jose_jwt:peek_payload(JWT),
                                        lists:flatten(
                                          io_lib:format("~p", [Payload]));
                                    false ->
                                        binary_to_list(JWT)
                                end,
                            ?MAGMA_FUSION_AUTH_TOKEN ++ "=" ++ JWTParam
                    end,
                CollectionManifestString =
                    case ForLogging of
                        true ->
                            %% The collection manifest can potentially be very
                            %% large, so we'll just log the uid
                            CollectionManifestShort =
                                io_lib:format(
                                  "<base64-encoding of manifest ~s>",
                                  [collections:uid(BucketName, Snapshot)]),
                            "collection_manifest=" ++ CollectionManifestShort;
                        false ->
                            CollectionManifestJson =
                                collections:manifest_json_for_memcached(
                                  BucketName, Snapshot),
                            %% Note, we base64url encode the collection manifest
                            %% to avoid needing to to make sure we escape any
                            %% ";" / "=". At the time of writing, these
                            %% characters are not believed to be possible to
                            %% appear in the manifest, but we're just making
                            %% sure we don't get a bug slip through if that
                            %% changed in future.
                            CollectionManifestEncoded =
                                base64:encode(
                                  ejson:encode(CollectionManifestJson),
                                  #{padding => false,
                                    mode => urlsafe}),
                            "collection_manifest=" ++ CollectionManifestEncoded
                    end,

                ExtraParams = [P || P <- [StaticConfigString, ExtraConfigString,
                                          DeksConfigString, JWTConfigString,
                                          CollectionManifestString],
                                    P =/= ""],
                string:join(DynamicParams ++ ExtraParams, ";")
        end,

    {Engine, PrepareCfgString(false), PrepareCfgString(true)}.

get_bucket_config(#cfg{config = BucketConfig}) ->
    BucketConfig.

get_magma_bucket_config(BucketConfig) ->
    StorageMode =
        ns_bucket:node_storage_mode(BucketConfig),

    case StorageMode of
        magma ->
            [{"magma_fragmentation_percentage", [{reload, flush}],
              ns_bucket:node_magma_fragmentation_percentage(BucketConfig)},
             %% The internal name, known by memcached, is a ratio so do the
             %% conversion.
             {"magma_mem_quota_ratio", [{reload, flush}],
              proplists:get_value(storage_quota_percentage, BucketConfig,
                                  ?MAGMA_STORAGE_QUOTA_PERCENTAGE) / 100},
             {"history_retention_seconds", [{reload, flush}],
              ns_bucket:history_retention_seconds(BucketConfig)},
             {"history_retention_bytes", [{reload, flush}],
              ns_bucket:history_retention_bytes(BucketConfig)},
             {"magma_key_tree_data_block_size", [{reload, flush}],
              ns_bucket:magma_key_tree_data_blocksize(BucketConfig)},
             {"magma_seq_tree_data_block_size", [{reload, flush}],
              ns_bucket:magma_seq_tree_data_blocksize(BucketConfig)}] ++
                get_fusion_bucket_config(BucketConfig);
        _ ->
            []
    end.

get_fusion_bucket_config(BucketConfig) ->
    case ns_bucket:is_fusion(BucketConfig) of
        true ->
            [{"magma_fusion_logstore_uri", [],
              ns_bucket:magma_fusion_logstore_uri(BucketConfig)},
             {"magma_fusion_metadatastore_uri", [{reload, flush}],
              ns_bucket:magma_fusion_metadatastore_uri(BucketConfig)},
             {?MAGMA_FUSION_AUTH_TOKEN, [{reload, flush}, no_param], custom}];
        false ->
            []
    end.
