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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MCD_DISABLED_ENCRYPTION_KEY_ID, <<"">>).
-define(MAGMA_FUSION_AUTH_TOKEN, "chronicle_auth_token").

-record(cfg, {type, name, config, snapshot, engine_config, params}).

-export([get/2,
         get_bucket_config/1,
         ensure/3,
         start_params/4,
         get_validation_config_string/3,
         remap_config_names/2,
         ensure_collections/2,
         get_current_collections_uid/1,
         format_mcd_keys/2]).

params_without_extras(membase, BucketName, BucketConfig, MemQuota, UUID,
                      DBSubDir) ->
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
     %% Compat version carries the cluster compat version. It is available
     %% from Totoro. We always set it on the local node, to allow the data
     %% service to apply compatibility settings suitable for lower versions.
     {"compat_version", [{reload, config}], get_compat_version_string()},
     {"alog_path", [], persistent_alog_path(BucketConfig, DBSubDir)},
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
        case cluster_compat_mode:is_cluster_79() of
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
        ++ get_magma_bucket_config(BucketConfig).

params(membase, BucketName, BucketConfig, MemQuota, UUID, DBSubDir) ->
    params_without_extras(membase, BucketName, BucketConfig, MemQuota, UUID,
                          DBSubDir)
        ++ get_extra_params(BucketConfig);
params(memcached, _BucketName, _BucketConfig, MemQuota, UUID, _DBSubDir) ->
    [{"cache_size", [], MemQuota},
     {"uuid", [], UUID}].

%% @doc Get the compat version string.
get_compat_version_string() ->
    [Major, Minor] = cluster_compat_mode:get_compat_version(),
    io_lib:format("~B.~B", [Major, Minor]).

%% @doc Parse an extra param from the bucket config extra_params.
-spec parse_extra_param(binary(), any()) -> {string(), list(), any()}.
parse_extra_param(Key, Value) ->
    {binary_to_list(Key), [extra], Value}.

%% @doc Get the extra params for the bucket config.
-spec get_extra_params(list()) -> list().
get_extra_params(BucketConfig) ->
    %% Convert extra params to the {Key, Props, Value} format.
    lists:map(fun ({Key, Value}) ->
        parse_extra_param(Key, Value)
              end, proplists:get_value(extra_params, BucketConfig, [])).

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

persistent_alog_path(BucketConfig, DBSubDir) ->
    case ns_bucket:is_persistent(BucketConfig) of
        true ->
            filename:join(DBSubDir, "access.log");
        false ->
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
    case cluster_compat_mode:is_cluster_79() of
        false ->
            undefined;
        true ->
            ns_bucket:get_invalid_hlc_strategy(BucketConfig)
    end.

get_hlc_max_future_threshold(BucketConfig) ->
    case cluster_compat_mode:is_cluster_79() of
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

get(BucketName, Snapshot) ->
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

%% @doc Get the components of a bucket config string which would be used if the
%% requested extra param changes were applied.
%%
%% The RequestedChanges is a proplist of {Key, Value} pairs.
%%
%% The RequestedChanges cannot override BucketConfig params other than
%% extra_params. Any attempt to do so will be logged.
%%
%% Note that the JWT and DEKs are not included in the returned config string.
-spec get_validation_config_string_internal(string(), list(), list()) ->
          {list(), list(), list()}.
get_validation_config_string_internal(BucketName, BucketConfig,
                                      RequestedChanges) ->
    BucketType = proplists:get_value(type, BucketConfig),
    MemQuota = proplists:get_value(ram_quota, BucketConfig),
    ExistingParams = params_without_extras(BucketType, "BucketName",
                                           BucketConfig, MemQuota, "UUID",
                                           "DBSubDir"),
    ExistingParamsKeys = proplists:get_keys(ExistingParams),
    ExistingExtraParams = get_extra_params(BucketConfig),

    %% Extra params we will try to update or set.
    %% We cannot override built-in params.
    {AllowExtraParams, DisallowExtraParams} =
        lists:partition(
          fun ({Key, _}) ->
                  not lists:member(Key, ExistingParamsKeys)
          end, RequestedChanges),

    %% This covers cases in which we attempt to modify a memcached parameter
    %% that we support via the REST API by using the memcached name rather than
    %% the rest API name - e.g. an attempt to change max_vbuckets which is
    %% called num_vbuckets internally is caught by this check.
    case DisallowExtraParams of
        [] -> ok;
        _ ->
            ?log_warning("Attempted to change built-in params ~p via extra "
                         "params. This is not allowed", [DisallowExtraParams])
    end,

    %% Compute params that will not be overridden by ChangedParams.
    UnchangedBuiltInParams =
        [T || {K, _Props, _Value} = T <- ExistingParams,
              not proplists:is_defined(K, AllowExtraParams)],

    %% Identify extra params that conflict with built-in params
    ErroneousExtraParams =
        [T || {K, _Props, _Value} = T <- ExistingExtraParams,
              proplists:is_defined(K, ExistingParams)],

    case ErroneousExtraParams of
        [] -> ok;
        _  -> ?log_warning("Found erroneous extra params ~p when generating"
                           " memcached bucket config string for bucket ~p",
                           [ErroneousExtraParams, BucketName])
    end,

    %% Unchanged extra params, no conflicts, not overridden
    UnchangedExtraParams =
        [T || {K, _Props, _Value} = T <- ExistingExtraParams,
              not proplists:is_defined(K, AllowExtraParams),
              not proplists:is_defined(K, ExistingParams)],

    UnchangedParams = UnchangedBuiltInParams ++ UnchangedExtraParams,

    %% We don't use build_params_string here because the extra params do not
    %% have the props field.
    JoinedAllowExtraParams = lists:map(fun ({Key, Value}) ->
                                               join_key_value(Key, Value)
                                       end, AllowExtraParams),
    %% Return the list of keys we applied.
    AllowedKeys = [K || {K, _} <- AllowExtraParams],
    {build_params_string(UnchangedParams), JoinedAllowExtraParams, AllowedKeys}.

%% @doc Get the bucket config string which would be used if the updated params
%% were applied. Note that the JWT and DEKs are not included in the returned
%% config string.
%% Returns the new bucket config string and the keys that were accepted.
%% Only extra params are allowed to be overriden. Other parameter changes are
%% not allowed and will be ignored.
-spec get_validation_config_string(string(), list(), list()) -> {list(), list()}.
get_validation_config_string(BucketName, BucketConfig, RequestedChanges) ->
    {Unchanged, Changed, Accepted} =
        get_validation_config_string_internal(BucketName,
                                              BucketConfig,
                                              RequestedChanges),
    {join_params(Unchanged ++ Changed), Accepted}.

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
    Integer = trunc(V),
    case V == Integer of
        true -> integer_to_list(Integer);
        false ->
            float_to_list(V, [{decimals, 2}, compact])
    end;
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

%% @doc Update the props of an extra param from the validation result.
%% This is used to determine if a restart is required for the extra params.
-spec update_props_from_validation_result(list(), map()) -> list().
update_props_from_validation_result(Props, ValidationResult) ->
    ExtraProps = case memcached_bucket_config_validation:requires_restart(
        ValidationResult) of
        true ->
            [restart];
        false ->
            []
    end,
    %% Reload type config can be used with all parameters.
    Props ++ ExtraProps ++ [{reload, config}].

%% @doc Populate the extra params with props from the validation result.
%% This is used to determine if a restart is required for the extra params.
populate_extra_params_props_internal(_Sock, membase, _Params, []) ->
    [];
populate_extra_params_props_internal(Sock, membase, Params, ExtraParams) ->
    BucketConfigString = join_params(build_params_string(Params)),
    {ok, ValidationMap} = mc_client_binary:validate_bucket_config(
                            Sock, <<"ep.so">>, BucketConfigString),
    lists:map(
      fun ({Name, Props, Value}) ->
              case maps:find(list_to_binary(Name), ValidationMap) of
                  {ok, ValidationResult} ->
                      {Name,
                       update_props_from_validation_result(Props,
                                                           ValidationResult),
                       Value};
                  error ->
                      %% Param not in validation map, but was specified -
                      %% implementation error.
                      erlang:exit(unknown_param)
              end
      end, ExtraParams).

%% @doc Populate the extra params with props from the validation result.
%% This is used to determine if a restart is required for the extra params.
%% If no extra params are present, no additional validation call is made.
populate_extra_params_props(Sock, membase, Params) ->
    {ExtraParams, NotExtraParams} = lists:partition(
                                      fun ({_Name, Props, _Value}) ->
                                              lists:member(extra, Props)
                                      end,
                                      Params),
    populate_extra_params_props_internal(Sock, membase, Params, ExtraParams)
        ++ NotExtraParams.

ensure(Sock, #cfg{type = membase, name = BucketName, params = Params}, JWT) ->
    case query_stats(Sock) of
        {ok, Stats} ->
            ChangedParams0 =
                lists:filter(
                  fun ({Name, _Props, Value}) ->
                          has_changed(BucketName, Name,
                                      value_to_binary(Value), Stats)
                  end, Params),

            %% If any of the changed params are extra params, we need to
            %% populate them, by calling validate_bucket_config. This is because
            %% the extra params do not have props filled in. This will tell us
            %% if a restart is required for the extra params.
            %% We only do this if there are changed extra params as a
            %% performance optimization.
            ChangedParams =
                case lists:any(
                       fun ({_, Props, _}) ->
                               lists:member(extra, Props)
                       end, ChangedParams0) of
                    true ->
                        PopulatedParams = populate_extra_params_props(
                                            Sock, membase, Params),
                        %% Filter down to only the changed params.
                        lists:filter(
                          fun ({Name, _Props, _Value}) ->
                                  proplists:is_defined(Name, ChangedParams0)
                          end, PopulatedParams);
                    false ->
                        ChangedParams0
                end,

            Restart =
                lists:any(
                  fun ({_, Props, _}) ->
                          lists:member(restart, Props)
                  end, ChangedParams),
            case Restart of
                true ->
                    restart;
                false ->
                    lists:foreach(
                      maybe_update_param(Sock, Stats, BucketName, _, JWT),
                      ChangedParams)
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

format_mcd_keys(ActiveDekId, Deks) ->
    format_mcd_keys(ActiveDekId, Deks, fun (K) -> K end).
format_mcd_keys(ActiveDekId, Deks, Sanitizer) ->
    DeksJsonMcd = lists:filtermap(fun (D) -> format_mcd_key(D, Sanitizer) end,
                                  Deks),
    ActiveKeyMcd = case ActiveDekId of
                       undefined -> ?MCD_DISABLED_ENCRYPTION_KEY_ID;
                       _ -> ActiveDekId
                   end,
    {[{keys, DeksJsonMcd}, {active, ActiveKeyMcd}]}.

format_mcd_key(?DEK_ERROR_PATTERN(_, _), _) ->
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

%% @doc Join a key and value into a string.
-spec join_key_value(string(), any()) -> string().
join_key_value(Key, Value) ->
    Key ++ "=" ++ value_to_string(Value).

%% @doc Join a list of params into config string.
join_params(Params) ->
    string:join(Params, ";").

%% @doc Filter out params that are not set and join pairs into strings.
-spec build_params_string(list()) -> list().
build_params_string(Params) ->
    lists:filtermap(
          fun ({_Name, _Props, undefined}) ->
                  false;
              ({Name, Props, Value}) ->
                  case lists:member(no_param, Props) of
                      true ->
                          false;
                      false ->
                          {true, join_key_value(Name, Value)}
                  end
          end, Params).

start_params(#cfg{name=BucketName,
                  config = BucketConfig,
                  snapshot = Snapshot,
                  params = Params,
                  engine_config = EngineConfig}, ActiveDekId, Deks, JWT) ->
    Engine = proplists:get_value(engine, EngineConfig),

    StaticConfigString =
        proplists:get_value(
          static_config_string, BucketConfig,
          proplists:get_value(static_config_string, EngineConfig)),
    ExtraConfigString =
        proplists:get_value(
          extra_config_string, BucketConfig,
          proplists:get_value(extra_config_string, EngineConfig, "")),

    DynamicParams = build_params_string(Params),

    PrepareCfgString =
        fun (ForLogging) ->
                Sanitizer =
                    case ForLogging of
                        true ->
                            fun (_) -> <<"<sanitized>">> end;
                        false ->
                            fun (S) -> S end
                    end,
                EncodedDeks = ejson:encode(format_mcd_keys(ActiveDekId,
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
                                        "Bearer " ++ binary_to_list(JWT)
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
                join_params(DynamicParams ++ ExtraParams)
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
            [{"magma_fusion_logstore_uri", [{reload, flush}],
              fusion_uploaders:get_log_store_uri()},
             {"magma_fusion_metadatastore_uri", [{reload, flush}],
              fusion_uploaders:get_metadata_store_uri()},
             {?MAGMA_FUSION_AUTH_TOKEN, [{reload, flush}, no_param], custom}];
        false ->
            []
    end.

map_name(Name, for_kv) when is_binary(Name) ->
    map_name(binary_to_list(Name), for_kv);
map_name(Name, for_ui) when is_binary(Name) ->
    map_name(binary_to_list(Name), for_ui);
map_name(Name, for_kv) when is_list(Name) ->
    snake_case(Name);
map_name(Name, for_ui) when is_list(Name) ->
    camel_case(Name);
%% When we have a non-string input, pass it through as is.
%% This will be values which are not strings (e.g. numbers or booleans).
map_name(Name, _) ->
    {ok, Name}.

%% This is an artifact of poor naming in the past and the remapping of KVs
%% snake_case config to camelCase for the REST API. These parameters are not
%% really camelCase due to the special capital handling, and so they must
%% be handled specially as we need to validate that we are not overriding any
%% parameters that ns_server should handle (built-in params).
special_mapping_cases("maxTTL") ->
    {ok, "max_ttl"};
special_mapping_cases("numVBuckets") ->
    {ok, "max_vbuckets"};
special_mapping_cases("ramQuotaMB") ->
    {ok, "ram_quota"};
special_mapping_cases("parallelDBAndViewCompaction") ->
    {ok, "parallel_db_and_view_compaction"};
special_mapping_cases("databaseFragmentationThreshold[percentage]") ->
    {ok, "db_fragmentation_percentage"};
special_mapping_cases("databaseFragmentationThreshold[size]") ->
    {ok, "db_fragmentation_size"};
special_mapping_cases("viewFragmentationThreshold[percentage]") ->
    {ok, "view_fragmentation_percentage"};
special_mapping_cases("viewFragmentationThreshold[size]") ->
    {ok, "view_fragmentation_size"};
special_mapping_cases("indexFragmentationThreshold[percentage]") ->
    {ok, "index_fragmentation_percentage"};
special_mapping_cases("indexFragmentationThreshold[size]") ->
    {ok, "index_fragmentation_size"};
%% Allowed Time Period is parsed strangely, the value is a complex object
%% comprised of all of these fields. We just need to remap to the key, that
%% should be enough to ignore this in the validation outside of
%% menelaus_web_autocompaction.erl.
special_mapping_cases("allowedTimePeriod[fromHour]") ->
    {ok, "allowed_time_period"};
special_mapping_cases("allowedTimePeriod[fromMinute]") ->
    {ok, "allowed_time_period"};
special_mapping_cases("allowedTimePeriod[toHour]") ->
    {ok, "allowed_time_period"};
special_mapping_cases("allowedTimePeriod[toMinute]") ->
    {ok, "allowed_time_period"};
special_mapping_cases("allowedTimePeriod[abortOutside]") ->
    {ok, "allowed_time_period"};
special_mapping_cases(_) ->
    not_found.

%% @doc Remap the config names for the UI or KV.
%% This is used to convert the config names to the names that memcached
%% supports. Any names which cannot be converted to the target case are ignored.
-spec remap_config_names(list() | map(), for_kv | for_ui) -> {list(), list()}.
remap_config_names(Params, Mode) when is_list(Params) ->
    lists:foldr(
      fun({K, V}, {OKAcc, BadAcc}) ->
              Result = case special_mapping_cases(K) of
                           not_found -> map_name(K, Mode);
                           SpecialRemap -> SpecialRemap
                       end,

              case Result of
                  {ok, Remapped} ->
                      {[{Remapped, V} | OKAcc], BadAcc};
                  {error, _} ->
                      {OKAcc, [{K, V} | BadAcc]}
              end
      end, {[], []}, Params);
remap_config_names(Params, Mode) when is_map(Params) ->
    List = proplists:from_map(Params),
    remap_config_names(List, Mode).


%% @doc Pattern to match words that are parts of a variable name.
word_pattern() ->
     %% Uppercase letter followed by lowercase letters or numbers
     "[A-Z][a-z0-9]+" ++
     %% or
     "|" ++
     %% Lowercase letters or numbers
     "[a-z0-9]+".

%% @doc Pattern to match a name.
%% Expects snake_case, camelCase or PascalCase.
%%
%% Note: To ensure deterministic word splitting, every word-segment starting
%% with an uppercase letter must be followed by at least one lowercase letter or
%% number.
%%
%% This means acronyms must be formatted as "XmlParser" or "UserId" instead of
%% "XMLParser" or "UserID".
%%
%% Allows underscores between words.
%% Does not allow:
%% - underscores at the beginning or end of the name
%% - consecutive underscores
name_pattern() ->
    %% Matches a name composed of words optionally separated by single
    %% underscores. Underscores cannot be at the beginning or end of the name.
    PrefixPattern = "^(" ++ word_pattern() ++ ")+",
    SuffixPattern = "(_?(" ++ word_pattern() ++ "))*$",

    PrefixPattern ++ SuffixPattern.

-spec is_name(Name :: string()) -> boolean().
is_name(Name) ->
    case re:run(Name, name_pattern(), [{capture, none}]) of
        match ->
            true;
        nomatch ->
            false
    end.

ensure_name(Name) ->
    case is_name(Name) of
        true ->
            ok;
        false ->
            {error, lists:flatten(io_lib:format("Invalid name: `~p`", [Name]))}
    end.

%% @doc Returns a list of words found in the input name.
%% Words can be separated by an '_' (snake_case), or a capital letter
%% (e.g. camelCase). Other separators are not supported.
-spec words(Name :: string()) -> {ok, [string()]} | {error, string()}.
words(Name) ->
    case ensure_name(Name) of
        ok ->
            {match, Matches} =
                re:run(Name, word_pattern(), [global, {capture, all, list}]),
            {ok, lists:append(Matches)};
        Error ->
            Error
    end.

%% Expects a valid name (see is_name/1).
-spec snake_case(Name :: string()) -> {ok, string()} | {error, string()}.
snake_case(Name) ->
    case words(Name) of
        {ok, Words} ->
            LowercasedWords = lists:map(fun string:lowercase/1, Words),
            {ok, lists:append(lists:join("_", LowercasedWords))};
        Error ->
            Error
    end.

%% Expects a valid name (see is_name/1).
-spec camel_case(Name :: string()) -> {ok, string()} | {error, string()}.
camel_case(Name) ->
    case words(Name) of
        {ok, [FirstWord | RestWords]} ->
            %% All words except the first one should be titlecased
            First = string:lowercase(FirstWord),
            Rest = [string:titlecase(Word) || Word <- RestWords],
            {ok, lists:append([First | Rest])};
        Error ->
            Error
    end.

-ifdef(TEST).
words_test() ->
    ?assertEqual({ok, ["foo"]}, words("foo")),
    ?assertEqual({ok, ["Foo"]}, words("Foo")),
    ?assertEqual({ok, ["Foo", "Bar"]}, words("FooBar")),
    ?assertEqual({ok, ["max", "Vbuckets"]}, words("maxVbuckets")),
    ?assertEqual({ok, ["ipv4"]}, words("ipv4")),
    ?assertEqual({ok, ["sha512"]}, words("sha512")),

    ?assertMatch({error, _}, words("foo_")),
    ?assertMatch({error, _}, words("_foo")),
    ?assertMatch({error, _}, words("_foo_")),
    ?assertMatch({error, _}, words("foo__bar")),
    ?assertMatch({error, _}, words("FOO")),
    ?assertMatch({error, _}, words("FooBAR")),
    ?assertMatch({error, _}, words("FooBARBaz")),
    ?assertMatch({error, _}, words("maxTTL")),
    ?assertMatch({error, _}, words("SHA512")).

is_name_test() ->
    ?assert(is_name("foo")),
    ?assert(is_name("Foo")),
    ?assert(is_name("fooBar")),
    ?assert(is_name("FooBar")),
    ?assert(is_name("maxVbuckets")),
    ?assert(is_name("ipv4")),
    ?assert(is_name("sha512")),

    ?assert(is_name("foo_bar")),
    ?assert(is_name("foo_bar_baz")),
    ?assert(is_name("max_vbuckets")),
    ?assert(is_name("sha_512")),

    ?assertNot(is_name("FOO")),
    ?assertNot(is_name("fooBAR")),
    ?assertNot(is_name("fooBARBaz")),
    ?assertNot(is_name("maxTTL")),
    ?assertNot(is_name("SHA512")),
    ?assertNot(is_name("foo_BAR_baz")),
    ?assertNot(is_name("max_TTL")),

    ?assertNot(is_name("foo_")),
    ?assertNot(is_name("_foo")),
    ?assertNot(is_name("_foo_")),
    ?assertNot(is_name("foo__bar")).

snake_case_test() ->
    ?assertEqual({ok, "foo"}, snake_case("foo")),
    ?assertEqual({ok, "foo"}, snake_case("Foo")),
    ?assertEqual({ok, "foo_bar"}, snake_case("foo_bar")),
    ?assertEqual({ok, "foo_bar"}, snake_case("FooBar")),
    ?assertEqual({error, "Invalid name: `\"FOOBar\"`"}, snake_case("FOOBar")),
    ?assertEqual({error, "Invalid name: `\"FooBARBaz\"`"},
                 snake_case("FooBARBaz")),
    ?assertEqual({ok, "max_vbuckets"}, snake_case("maxVbuckets")),
    ?assertEqual({error,"Invalid name: `\"SHA512\"`"}, snake_case("SHA512")),
    ?assertEqual({ok, "sha512"}, snake_case("Sha512")),
    ?assertEqual({error,"Invalid name: `\"HLCDriftAheadThresholdUS\"`"},
                 snake_case("HLCDriftAheadThresholdUS")),
    ?assertEqual({ok, "hlc_drift_ahead_threshold_us"},
                 snake_case("HlcDriftAheadThresholdUs")),

    ?assertEqual({error, "Invalid name: `[]`"}, snake_case("")),
    ?assertEqual({error, "Invalid name: `\"with space\"`"},
                 snake_case("with space")),
    ?assertEqual({error, "Invalid name: `\"with multiple spaces\"`"},
                 snake_case("with multiple spaces")),
    ?assertEqual({error, "Invalid name: `\"_not_a_name\"`"},
                 snake_case("_not_a_name")),
    ?assertEqual({error, "Invalid name: `\"special_chars@\"`"},
                 snake_case("special_chars@")),

    ?assertEqual({ok, "magma_max_level0_ttl"}, snake_case("magmaMaxLevel0Ttl")).

camel_case_test() ->
    ?assertEqual({ok, "foo"}, camel_case("foo")),
    ?assertEqual({ok, "foo"}, camel_case("Foo")),
    ?assertEqual({ok, "fooBar"}, camel_case("foo_bar")),
    ?assertEqual({ok, "fooBar"}, camel_case("FooBar")),
    ?assertEqual({error, "Invalid name: `\"FOOBar\"`"}, camel_case("FOOBar")),

    ?assertEqual({ok, "maxVbuckets"}, camel_case("max_vbuckets")),
    ?assertEqual({ok, "somethingWwwSomething"},
                 camel_case("something_www_something")),

    ?assertEqual({error, "Invalid name: `[]`"},
                 camel_case("")),
    ?assertEqual({error, "Invalid name: `\"with space\"`"},
                 camel_case("with space")),
    ?assertEqual({error, "Invalid name: `\"with multiple spaces\"`"},
                 camel_case("with multiple spaces")),
    ?assertEqual({error, "Invalid name: `\"_not_a_name\"`"},
                 camel_case("_not_a_name")),
    ?assertEqual({error, "Invalid name: `\"specialChars@\"`"},
                 camel_case("specialChars@")),

    ?assertEqual({ok, "magmaMaxLevel0Ttl"},
                 camel_case("magma_max_level_0_ttl")).


get_validation_config_string_internal_test() ->
    fake_ns_config:setup(),
    fake_chronicle_kv:setup(),

    fake_ns_config:setup_cluster_compat_version(?LATEST_VERSION_NUM),
    fake_chronicle_kv:setup_cluster_compat_version(?LATEST_VERSION_NUM),

    BucketConfig = [
                    {type, membase},
                    {ram_quota, 1},
                    {extra_params, [
                                    {<<"connection_manager_interval">>, "1"},
                                    {<<"item_compressor_interval">>, "1"}
                                   ]}
                   ],

    {Unchanged, Changed, Accepted} =
        get_validation_config_string_internal(
            "bucket", BucketConfig, [{"connection_manager_interval", "2"}]),

    ?assertEqual(true,
                 lists:member("item_compressor_interval=1", Unchanged)),
    ?assertEqual(false,
                 lists:member("connection_manager_interval=1", Unchanged)),
    ?assertEqual(["connection_manager_interval=2"], Changed),
    ?assertEqual(["connection_manager_interval"], Accepted),

    fake_chronicle_kv:teardown(),
    fake_ns_config:teardown().

-endif.
