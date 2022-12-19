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
-include("cut.hrl").

-define(DEFAULT_MAGMA_MIN_MEMORY_QUOTA, 1024).

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
         create_bucket/3,
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

may_expose_bucket_auth(Name, Req) ->
    case menelaus_auth:get_token(Req) of
        undefined ->
            case cluster_compat_mode:is_cluster_71() of
                false ->
                    %% The bucket password permission was removed in 7.1
                    %% so would only come into play when running mixed versions
                    %% with pre-7.1 nodes.
                    menelaus_auth:has_permission({[{bucket, Name}, password],
                                                  read}, Req);
                true ->
                    false
            end;
        _ ->
            false
    end.

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
    F = menelaus_web_node:build_nodes_info_fun(Ctx),
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
    add_couch_api_base_loop(Nodes, BucketName, BucketUUID, LocalAddr, F,
                            Dict, [], []).


add_couch_api_base_loop([], _BucketName, _BucketUUID, _LocalAddr, _F, _Dict, CAPINodes, NonCAPINodes) ->
    CAPINodes ++ NonCAPINodes;
add_couch_api_base_loop([Node | RestNodes],
                        BucketName, BucketUUID, LocalAddr, F, Dict, CAPINodes, NonCAPINodes) ->
    {struct, KV} = F(Node, BucketName),
    case dict:find(Node, Dict) of
        {ok, V} when V =/= [] ->
            %% note this is generally always expected, but let's play safe just in case
            S = {struct, add_couch_api_base(BucketName, BucketUUID, KV, Node, LocalAddr)},
            add_couch_api_base_loop(RestNodes, BucketName, BucketUUID,
                                    LocalAddr, F, Dict, [S | CAPINodes], NonCAPINodes);
        _ ->
            S = {struct, KV},
            add_couch_api_base_loop(RestNodes, BucketName, BucketUUID,
                                    LocalAddr, F, Dict, CAPINodes, [S | NonCAPINodes])
    end.

add_couch_api_base(BucketName, BucketUUID, KV, Node, LocalAddr) ->
    NodesKeysList = [{Node, couchApiBase}, {{ssl, Node}, couchApiBaseHTTPS}],

    lists:foldl(fun({N, Key}, KVAcc) ->
                        case capi_utils:capi_bucket_url_bin(N, BucketName,
                                                            BucketUUID, LocalAddr) of
                            undefined ->
                                KVAcc;
                            Url ->
                                {ok, BCfg} = ns_bucket:get_bucket(BucketName),
                                case ns_bucket:bucket_type(BCfg) of
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

build_buckets_info(Req, Buckets, Ctx, InfoLevel) ->
    SkipMap = InfoLevel =/= streaming andalso
        proplists:get_value(
          "skipMap", mochiweb_request:parse_qs(Req)) =:= "true",
    [build_bucket_info(BucketName, Ctx, InfoLevel,
                      may_expose_bucket_auth(BucketName, Req), SkipMap) ||
        BucketName <- Buckets].

build_bucket_info(Id, Ctx, InfoLevel, MayExposeAuth, SkipMap) ->
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
        {nodes,
         menelaus_util:strip_json_struct(
           build_bucket_nodes_info(Id, BucketUUID, BucketConfig, Ctx))},
        {stats,
         {[{uri, bucket_info_cache:build_pools_uri(["buckets", Id, "stats"])},
           {directoryURI,
            bucket_info_cache:build_pools_uri(["buckets", Id, "stats",
                                               "Directory"])},
           {nodeStatsListURI,
            bucket_info_cache:build_pools_uri(["buckets", Id, "nodes"])}]}},
        build_authType(BucketConfig),
        build_auto_compaction_info(BucketConfig),
        build_purge_interval_info(BucketConfig),
        build_replica_index(BucketConfig),
        build_dynamic_bucket_info(InfoLevel, Id, BucketConfig, Ctx),
        [build_sasl_password(BucketConfig) || MayExposeAuth]])}.

build_authType(BucketConfig) ->
    case cluster_compat_mode:is_cluster_71() of
        false ->
            [{authType, misc:expect_prop_value(auth_type, BucketConfig)}];
        true ->
            %% Needed by XDCR on versions prior to 7.0. This must remain
            %% until there are no supported pre-7.0 versions that can
            %% replicate to us.
            [{authType, sasl}]
    end.

build_sasl_password(BucketConfig) ->
    case cluster_compat_mode:is_cluster_71() of
        true ->
            [];
        false ->
            case cluster_compat_mode:is_cluster_70() of
                true ->
                    {saslPassword, <<>>};
                false ->
                    {saslPassword,
                     list_to_binary(proplists:get_value(sasl_password,
                                                        BucketConfig, ""))}
            end
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

    [{storageTotals, {StorageTotals}} | menelaus_stats:basic_stats(Id)];
build_bucket_stats(_, Id, _) ->
    menelaus_stats:basic_stats(Id).

build_dynamic_bucket_info(streaming, _Id, _BucketConfig, _) ->
    [];
build_dynamic_bucket_info(InfoLevel, Id, BucketConfig, Ctx) ->
    [[{replicaNumber, ns_bucket:num_replicas(BucketConfig)},
      {threadsNumber, proplists:get_value(num_threads, BucketConfig,
                                          ?NUM_WORKER_THREADS)},
      {quota, {[{ram, ns_bucket:ram_quota(BucketConfig)},
                {rawRAM, ns_bucket:raw_ram_quota(BucketConfig)}]}},
      {basicStats, {build_bucket_stats(InfoLevel, Id, Ctx)}},
      {evictionPolicy, build_eviction_policy(BucketConfig)},
      {durabilityMinLevel, build_durability_min_level(BucketConfig)},
      build_pitr_dynamic_bucket_info(BucketConfig),
      build_magma_bucket_info(BucketConfig),
      {conflictResolutionType,
       ns_bucket:conflict_resolution_type(BucketConfig)}],
     case cluster_compat_mode:is_enterprise() of
         true ->
             [{maxTTL, proplists:get_value(max_ttl, BucketConfig, 0)},
              {compressionMode,
               proplists:get_value(compression_mode, BucketConfig, off)}];
         false ->
             []
     end,
     case ns_bucket:drift_thresholds(BucketConfig) of
         undefined ->
             [];
         {DriftAheadThreshold, DriftBehindThreshold} ->
             [{driftAheadThresholdMs, DriftAheadThreshold},
              {driftBehindThresholdMs, DriftBehindThreshold}]
     end].

build_pitr_dynamic_bucket_info(BucketConfig) ->
    case ns_bucket:bucket_type(BucketConfig) of
        memcached ->
            %% memcached buckets don't support pitr.
            [];
        _ ->
            [{pitrEnabled, ns_bucket:pitr_enabled(BucketConfig)},
             {pitrGranularity, ns_bucket:pitr_granularity(BucketConfig)},
             {pitrMaxHistoryAge, ns_bucket:pitr_max_history_age(BucketConfig)}]
    end.

build_magma_bucket_info(BucketConfig) ->
    case ns_bucket:storage_mode(BucketConfig) of
        magma ->
            [{storageQuotaPercentage,
              proplists:get_value(storage_quota_percentage, BucketConfig,
                                  ?MAGMA_STORAGE_QUOTA_PERCENTAGE)}];
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
    case ns_bucket:bucket_exists(Id, direct) of
        true ->
            menelaus_web_cache:lookup_or_compute_with_expiration(
              {build_bucket_info, Id, LocalAddr},
              fun () ->
                      Ctx = menelaus_web_node:get_context(
                              {ip, LocalAddr}, [Id], false, stable),
                      [Info] = build_buckets_info(Req, [Id], Ctx, streaming),
                      {Info, 1000, UpdateID}
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
              end);
        false ->
            exit(normal)
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
        rebalance_running ->
            reply_json(Req, {struct, [{'_', <<"Cannot delete buckets during rebalance.\r\n">>}]}, 503);
        in_recovery ->
            reply_json(Req, {struct, [{'_', <<"Cannot delete buckets when cluster is in recovery mode.\r\n">>}]}, 503);
        {shutdown_failed, _} ->
            reply_json(Req, {struct, [{'_', <<"Bucket deletion not yet complete, but will continue.\r\n">>}]}, 500);
        {exit, {not_found, _}, _} ->
            reply_text(Req, "The bucket to be deleted was not found.\r\n", 404)
    end.

respond_bucket_created(Req, PoolId, BucketId) ->
    reply(Req, 202, [{"Location", concat_url_path(["pools", PoolId, "buckets", BucketId])}]).

-record(bv_ctx, {
          validate_only,
          ignore_warnings,
          new,
          bucket_name,
          bucket_config,
          all_buckets,
          kv_nodes,
          max_replicas,
          cluster_storage_totals,
          cluster_version,
          is_enterprise,
          is_developer_preview}).

init_bucket_validation_context(IsNew, BucketName, Req) ->
    ValidateOnly = (proplists:get_value("just_validate", mochiweb_request:parse_qs(Req)) =:= "1"),
    IgnoreWarnings = (proplists:get_value("ignore_warnings", mochiweb_request:parse_qs(Req)) =:= "1"),
    init_bucket_validation_context(IsNew, BucketName, ValidateOnly, IgnoreWarnings).

init_bucket_validation_context(IsNew, BucketName, ValidateOnly,
                               IgnoreWarnings) ->
    Config = ns_config:get(),
    Snapshot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(all, _),
           ns_cluster_membership:fetch_snapshot(_)], #{ns_config => Config}),

    KvNodes = ns_cluster_membership:service_active_nodes(Snapshot, kv),
    ServerGroups = ns_cluster_membership:server_groups(Snapshot),

    init_bucket_validation_context(
      IsNew, BucketName,
      ns_bucket:get_buckets(Snapshot),
      KvNodes, ServerGroups,
      ns_storage_conf:cluster_storage_info(Config, Snapshot),
      ValidateOnly, IgnoreWarnings,
      cluster_compat_mode:get_compat_version(),
      cluster_compat_mode:is_enterprise(),
      cluster_compat_mode:is_developer_preview()).

init_bucket_validation_context(IsNew, BucketName, AllBuckets,
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
       all_buckets = AllBuckets,
       kv_nodes = KvNodes,
       max_replicas = MaxReplicas,
       bucket_config = BucketConfig,
       cluster_storage_totals = ClusterStorageTotals,
       cluster_version = ClusterVersion,
       is_enterprise = IsEnterprise,
       is_developer_preview = IsDeveloperPreview
      }.

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
            RV = {struct, [{errors, {struct, Errors}},
                           {summaries, {struct, JSONSummaries}}]},
            reply_json(Req, RV, 400);
        {false, _, {ok, ParsedProps, _}} ->
            BucketType = proplists:get_value(bucketType, ParsedProps),
            StorageMode = proplists:get_value(storage_mode, ParsedProps,
                                              undefined),
            UpdatedProps = ns_bucket:extract_bucket_props(ParsedProps),
            case ns_orchestrator:update_bucket(BucketType, StorageMode,
                                               BucketId, UpdatedProps) of
                ok ->
                    ns_audit:modify_bucket(Req, BucketId, BucketType, UpdatedProps),
                    DisplayBucketType = ns_bucket:display_type(BucketType,
                                                               StorageMode),
                    ale:info(?USER_LOGGER, "Updated bucket \"~s\" (of type ~s) properties:~n~p",
                             [BucketId, DisplayBucketType, UpdatedProps]),
                    reply(Req, 200);
                rebalance_running ->
                    reply_text(Req,
                               "Cannot update bucket "
                               "while rebalance is running.", 503);
                in_recovery ->
                    reply_text(Req,
                               "Cannot update bucket "
                               "while recovery is in progress.", 503);
                {exit, {not_found, _}, _} ->
                    %% if this happens then our validation raced, so repeat everything
                    handle_bucket_update_inner(BucketId, Req, Params, Limit-1)
            end;
        {true, true, {ok, _, JSONSummaries}} ->
            reply_json(Req, {struct, [{errors, {struct, []}},
                                      {summaries, {struct, JSONSummaries}}]}, 200);
        {true, false, {ok, ParsedProps, JSONSummaries}} ->
            FinalErrors = perform_warnings_validation(Ctx, ParsedProps, []),
            reply_json(Req, {struct, [{errors, {struct, FinalErrors}},
                                      {summaries, {struct, JSONSummaries}}]},
                       case FinalErrors of
                           [] -> 202;
                           _ -> 400
                       end)
    end.

maybe_cleanup_old_buckets() ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            ok;
        false ->
            true = ns_node_disco:nodes_wanted() =:= [node()],
            ns_storage_conf:delete_unused_buckets_db_files()
    end.

create_bucket(Req, Name, Params) ->
    Ctx = init_bucket_validation_context(true, Name, false, false),
    do_bucket_create(Req, Name, Params, Ctx).

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
            {errors, [{name, <<"Bucket with given name already exists">>}]};
        {error, {still_exists, _}} ->
            {errors_500, [{'_', <<"Bucket with given name still exists">>}]};
        {error, {invalid_name, _}} ->
            {errors, [{name, <<"Name is invalid.">>}]};
        rebalance_running ->
            {errors_500, [{'_', <<"Cannot create buckets during rebalance">>}]};
        in_recovery ->
            {errors_500, [{'_', <<"Cannot create buckets when cluster is in recovery mode">>}]}
    end.

do_bucket_create(Req, Name, Params, Ctx) ->
    MaxBuckets = ns_bucket:get_max_buckets(),
    case length(Ctx#bv_ctx.all_buckets) >= MaxBuckets of
        true ->
            {{struct, [{'_', iolist_to_binary(io_lib:format("Cannot create more than ~w buckets", [MaxBuckets]))}]}, 400};
        false ->
            case {Ctx#bv_ctx.validate_only, Ctx#bv_ctx.ignore_warnings,
                  parse_bucket_params(Ctx, Params)} of
                {_, _, {errors, Errors, JSONSummaries}} ->
                    {{struct, [{errors, {struct, Errors}},
                               {summaries, {struct, JSONSummaries}}]}, 400};
                {false, _, {ok, ParsedProps, _}} ->
                    case do_bucket_create(Req, Name, ParsedProps) of
                        ok -> ok;
                        {errors, Errors} ->
                            ?log_debug("Failed to create bucket '~s' with 40X error(s): ~p",
                                       [Name, Errors]),
                            {{struct, Errors}, 400};
                        {errors_500, Errors} ->
                            ?log_debug("Failed to create bucket '~s' with 50X error(s): ~p",
                                       [Name, Errors]),
                            {{struct, Errors}, 503}
                    end;
                {true, true, {ok, _, JSONSummaries}} ->
                    {{struct, [{errors, {struct, []}},
                               {summaries, {struct, JSONSummaries}}]}, 200};
                {true, false, {ok, ParsedProps, JSONSummaries}} ->
                    FinalErrors = perform_warnings_validation(Ctx, ParsedProps, []),
                    {{struct, [{errors, {struct, FinalErrors}},
                               {summaries, {struct, JSONSummaries}}]},
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
            reply_json(Req, Struct, Code)
    end.

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
        case {Ctx#bv_ctx.new, Ctx#bv_ctx.bucket_config} of
            {true, _} ->
                [];
            {_, false} ->
                [];
            {false, BucketConfig} ->
                case ns_bucket:num_replicas(BucketConfig) of
                    NReplicas ->
                        [];
                    _ ->
                        ["changing replica number may require rebalance"]
                end
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

handle_bucket_flush(_PoolId, Id, Req) ->
    XDCRDocs = goxdcr_rest:find_all_replication_docs(),
    case lists:any(
           fun (PList) ->
                   erlang:binary_to_list(proplists:get_value(source, PList)) =:= Id
           end, XDCRDocs) of
        false ->
            do_handle_bucket_flush(Id, Req);
        true ->
            reply_json(Req, {struct, [{'_', <<"Cannot flush buckets with outgoing XDCR">>}]}, 503)
    end.

do_handle_bucket_flush(BucketName, Req) ->
    case ns_orchestrator:flush_bucket(BucketName) of
        ok ->
            ns_audit:flush_bucket(Req, BucketName),
            reply(Req, 200);
        rebalance_running ->
            reply_json(Req, {struct, [{'_', <<"Cannot flush buckets during rebalance">>}]}, 503);
        in_recovery ->
            reply_json(Req, {struct, [{'_', <<"Cannot flush buckets when cluster is in recovery mode">>}]}, 503);
        bucket_not_found ->
            reply(Req, 404);
        flush_disabled ->
            reply_json(Req, {struct, [{'_', <<"Flush is disabled for the bucket">>}]}, 400);
        _ ->
            reply_json(Req, {struct, [{'_', <<"Flush failed with unexpected error. Check server logs for details.">>}]}, 500)
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

parse_bucket_params_without_warnings(Ctx, Params) ->
    {OKs, Errors} = basic_bucket_params_screening(Ctx ,Params),
    ClusterStorageTotals = Ctx#bv_ctx.cluster_storage_totals,
    IsNew = Ctx#bv_ctx.new,
    CurrentBucket = proplists:get_value(currentBucket, OKs),
    HasRAMQuota = lists:keyfind(ram_quota, 1, OKs) =/= false,
    RAMSummary = if
                     HasRAMQuota ->
                         interpret_ram_quota(Ctx, CurrentBucket, OKs,
                                             ClusterStorageTotals);
                     true ->
                         interpret_ram_quota(Ctx, CurrentBucket,
                                             [{ram_quota, 0} | OKs],
                                             ClusterStorageTotals)
                 end,
    HDDSummary = interpret_hdd_quota(CurrentBucket, OKs, ClusterStorageTotals, Ctx),
    JSONSummaries = [{ramSummary, {struct, ram_summary_to_proplist(RAMSummary)}},
                     {hddSummary, {struct, hdd_summary_to_proplist(HDDSummary)}}],
    Errors2 = case {CurrentBucket, IsNew} of
                  {undefined, _} -> Errors;
                  {_, true} -> Errors;
                  {_, false} ->
                      case {proplists:get_value(bucketType, OKs),
                            ns_bucket:bucket_type(CurrentBucket)} of
                          {undefined, _} -> Errors;
                          {NewType, NewType} -> Errors;
                          {_NewType, _OldType} ->
                              [{bucketType, <<"Cannot change bucket type.">>}
                               | Errors]
                      end
              end,
    RAMErrors =
        if
            RAMSummary#ram_summary.free < 0 ->
                [{ramQuota, <<"RAM quota specified is too large to be provisioned into this cluster.">>}];
            RAMSummary#ram_summary.this_alloc < RAMSummary#ram_summary.this_used ->
                [{ramQuota, <<"RAM quota cannot be set below current usage.">>}];
            true ->
                []
        end,
    TotalErrors = RAMErrors ++ Errors2,
    if
        TotalErrors =:= [] ->
            {ok, OKs, JSONSummaries};
        true ->
            {errors, TotalErrors, JSONSummaries, OKs}
    end.

additional_bucket_params_validation(Params, Ctx) ->
    NumReplicas = get_value_from_parms_or_bucket(num_replicas, Params, Ctx),
    DurabilityLevel = get_value_from_parms_or_bucket(durability_min_level,
                                                     Params, Ctx),
    NodesCount = length(get_nodes(Ctx)),
    Err1 = case {NumReplicas, DurabilityLevel, NodesCount} of
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
           end,

    StorageMode = get_value_from_parms_or_bucket(storage_mode, Params, Ctx),
    RamQuota = get_value_from_parms_or_bucket(ram_quota, Params, Ctx),
    MagmaMinMemoryQuota =
        ns_config:read_key_fast(magma_min_memory_quota,
                                ?DEFAULT_MAGMA_MIN_MEMORY_QUOTA),

    Err2 = case {StorageMode, RamQuota} of
               {magma, RamQuota}
                 when RamQuota < MagmaMinMemoryQuota * 1024 * 1024 ->
                   RamQ = list_to_binary(integer_to_list(MagmaMinMemoryQuota)),
                   [{ramQuota,
                     <<"Ram quota for magma must be at least ", RamQ/binary,
                       " MiB">>}];
               {_, _} ->
                   []
           end,

    Err1 ++ Err2.

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
    [{ok, name, BucketName},
     parse_validate_flush_enabled(Params, IsNew),
     validate_bucket_name(IsNew, BucketConfig, BucketName, AllBuckets),
     parse_validate_ram_quota(Params, BucketConfig),
     parse_validate_other_buckets_ram_quota(Params)].

validate_bucket_type_specific_params(CommonParams, Params,
                                     #bv_ctx{new = IsNew,
                                             bucket_config = BucketConfig,
                                             cluster_version = Version,
                                             is_enterprise = IsEnterprise,
                                             is_developer_preview =
                                                 IsDeveloperPreview}) ->
    BucketType = get_bucket_type(IsNew, BucketConfig, Params),

    case BucketType of
        memcached ->
            validate_memcached_bucket_params(CommonParams, Params, IsNew,
                                             BucketConfig);
        membase ->
            validate_membase_bucket_params(CommonParams, Params, IsNew,
                                           BucketConfig, Version, IsEnterprise,
                                           IsDeveloperPreview);
        _ ->
            validate_unknown_bucket_params(Params)
    end.

validate_memcached_params(Params) ->
    case proplists:get_value("replicaNumber", Params) of
        undefined ->
            ignore;
        _ ->
            {error, replicaNumber,
             <<"replicaNumber is not valid for memcached buckets">>}
    end.

validate_memcached_bucket_params(CommonParams, Params, IsNew, BucketConfig) ->
    [{ok, bucketType, memcached},
     validate_memcached_params(Params),
     quota_size_error(CommonParams, memcached, IsNew, BucketConfig)].

validate_membase_bucket_params(CommonParams, Params,
                               IsNew, BucketConfig, Version, IsEnterprise,
                               IsDeveloperPreview) ->
    ReplicasNumResult = validate_replicas_number(Params, IsNew),
    HistRetSecs = parse_validate_history_retention_seconds(
        Params, BucketConfig, IsNew, Version, IsEnterprise),
    BucketParams =
        [{ok, bucketType, membase},
         ReplicasNumResult,
         parse_validate_replica_index(Params, ReplicasNumResult, IsNew),
         parse_validate_threads_number(Params, IsNew),
         parse_validate_eviction_policy(Params, BucketConfig, IsNew),
         quota_size_error(CommonParams, membase, IsNew, BucketConfig),
         parse_validate_storage_mode(Params, BucketConfig, IsNew, Version,
                                     IsEnterprise),
         parse_validate_durability_min_level(Params, BucketConfig, IsNew,
                                             Version),
         parse_validate_pitr_enabled(Params, IsNew, IsDeveloperPreview),
         parse_validate_pitr_granularity(Params, IsNew, IsDeveloperPreview),
         parse_validate_pitr_max_history_age(Params, IsNew, IsDeveloperPreview),
         parse_validate_storage_quota_percentage(Params, BucketConfig, IsNew, Version,
                                                 IsEnterprise),
         parse_validate_max_ttl(Params, BucketConfig, IsNew, IsEnterprise),
         parse_validate_compression_mode(Params, BucketConfig, IsNew,
                                         IsEnterprise),
         HistRetSecs,
         parse_validate_history_retention_bytes(Params, BucketConfig,
                                                IsNew, Version, IsEnterprise)
         | validate_bucket_auto_compaction_settings(Params)],

    validate_bucket_purge_interval(Params, BucketConfig, IsNew) ++
        get_conflict_resolution_type_and_thresholds(Params, HistRetSecs,
                                                    BucketConfig, IsNew) ++
        BucketParams.

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
            case ns_bucket:name_conflict(BucketName, AllBuckets) of
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
        "memcached" -> memcached;
        "membase" -> membase;
        "couchbase" -> membase;
        "ephemeral" -> membase;
        undefined -> membase;
        _ -> invalid
    end.

quota_size_error(CommonParams, BucketType, IsNew, BucketConfig) ->
    case lists:keyfind(ram_quota, 2, CommonParams) of
        {ok, ram_quota, RAMQuota} ->
            {MinQuota, Msg}
                = case BucketType of
                      membase ->
                          Q = misc:get_env_default(membase_min_ram_quota, 100),
                          Qv = list_to_binary(integer_to_list(Q)),
                          {Q, <<"RAM quota cannot be less than ", Qv/binary,
                                " MiB">>};
                      memcached ->
                          Q = misc:get_env_default(memcached_min_ram_quota, 64),
                          Qv = list_to_binary(integer_to_list(Q)),
                          {Q, <<"RAM quota cannot be less than ", Qv/binary,
                                " MiB">>}
                  end,
            if
                RAMQuota < MinQuota * ?MIB ->
                    {error, ramQuota, Msg};
                IsNew =/= true andalso BucketConfig =/= false andalso BucketType =:= memcached ->
                    case ns_bucket:raw_ram_quota(BucketConfig) of
                        RAMQuota -> ignore;
                        _ ->
                            {error, ramQuota, <<"cannot change quota of memcached buckets">>}
                    end;
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

validate_replicas_number(Params, IsNew) ->
    validate_with_missing(
      proplists:get_value("replicaNumber", Params),
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
parse_validate_storage_mode(Params, _BucketConfig, true = _IsNew, Version,
                            IsEnterprise) ->
    case proplists:get_value("bucketType", Params, "membase") of
        "membase" ->
            get_storage_mode_based_on_storage_backend(Params, Version,
                                                      IsEnterprise);
        "couchbase" ->
            get_storage_mode_based_on_storage_backend(Params, Version,
                                                      IsEnterprise);
        "ephemeral" ->
            {ok, storage_mode, ephemeral}
    end;
parse_validate_storage_mode(_Params, BucketConfig, false = _IsNew, _Version,
                            _IsEnterprise)->
    {ok, storage_mode, ns_bucket:storage_mode(BucketConfig)}.

parse_validate_durability_min_level(Params, BucketConfig, IsNew, Version) ->
    IsEphemeral = is_ephemeral(Params, BucketConfig, IsNew),
    Level = proplists:get_value("durabilityMinLevel", Params),
    IsCompat = cluster_compat_mode:is_version_66(Version),
    do_parse_validate_durability_min_level(IsEphemeral, Level, IsNew, IsCompat).

do_parse_validate_durability_min_level(_IsEphemeral, Level, _IsNew,
                                       false = _IsCompat)
  when Level =/= undefined ->
    {error, durability_min_level,
     <<"Durability minimum level cannot be set until cluster is fully 6.6">>};
do_parse_validate_durability_min_level(false = _IsEphemeral, Level, IsNew,
                                       _IsCompat) ->
    validate_with_missing(Level, "none", IsNew,
      fun parse_validate_membase_durability_min_level/1);
do_parse_validate_durability_min_level(true = _IsEphemeral, Level, IsNew,
                                       _IsCompat) ->
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

%% Point-in-time Recovery (PITR) parameter parsing and validation.

pitr_not_developer_preview_error(Param) ->
    {error, Param,
     <<"Point in time recovery is only supported in developer preview mode">>}.

%% PITR parameter parsing and validation when not in developer preview mode.
%% Specifying PITR params in production builds is not allowed.
parse_validate_pitr_param_not_dev_preview(Key, Params) ->
    case proplists:is_defined(Key, Params) of
        true ->
            pitr_not_developer_preview_error(Key);
        false ->
            ignore
    end.

parse_validate_pitr_enabled(Params, _IsNew, false = _IsDeveloperPreview) ->
    parse_validate_pitr_param_not_dev_preview("pitrEnabled", Params);
parse_validate_pitr_enabled(Params, IsNew, true = _IsDeveloperPreview) ->
    Result = menelaus_util:parse_validate_boolean_field("pitrEnabled",
                                                        '_', Params),
    case {Result, IsNew} of
        {[], true} ->
            %% The value wasn't supplied and we're creating a bucket:
            %% use the default value.
            {ok, pitr_enabled, false};
        {[], false} ->
            %% The value wasn't supplied and we're modifying a bucket:
            %% don't complain since the value was either specified or a
            %% default used when the bucket was created.
            ignore;
        {[{ok, _, Value}], _} ->
            {ok, pitr_enabled, Value};
        {[{error, _, _ErrorMsg}], _} ->
            value_not_boolean_error(pitrEnabled)
    end.

parse_validate_pitr_granularity(Params, _IsNew, false = _IsDeveloperPreview) ->
    parse_validate_pitr_param_not_dev_preview("pitrGranularity", Params);
parse_validate_pitr_granularity(Params, IsNew, true = _IsDeveloperPreview) ->
    parse_validate_pitr_numeric_param(Params, pitrGranularity,
                                      pitr_granularity, IsNew).

parse_validate_pitr_max_history_age(Params, _IsNew,
                                    false = _IsDeveloperPreview) ->
    parse_validate_pitr_param_not_dev_preview("pitrMaxHistoryAge", Params);
parse_validate_pitr_max_history_age(Params, IsNew,
                                    true = _IsDeveloperPreview) ->
    parse_validate_pitr_numeric_param(Params, pitrMaxHistoryAge,
                                      pitr_max_history_age, IsNew).

parse_validate_pitr_numeric_param(Params, Param, ConfigKey, IsNew) ->
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
            validate_pitr_numeric_param(Value, Param, ConfigKey)
    end.

%% Validates defined numeric parameters.
validate_pitr_numeric_param(Value, Param, ConfigKey) ->
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

get_storage_mode_based_on_storage_backend(Params, Version, IsEnterprise) ->
    StorageBackend = proplists:get_value("storageBackend", Params,
                                         "couchstore"),
    do_get_storage_mode_based_on_storage_backend(
      StorageBackend, IsEnterprise,
      cluster_compat_mode:is_version_71(Version)).

do_get_storage_mode_based_on_storage_backend("magma", false, _Is71) ->
    {error, storageBackend,
     <<"Magma is supported in enterprise edition only">>};
do_get_storage_mode_based_on_storage_backend("magma", true, false) ->
    {error, storageBackend,
     <<"Not allowed until entire cluster is upgraded to 7.1">>};
do_get_storage_mode_based_on_storage_backend(StorageBackend, _IsEnterprise,
                                             _Is71) ->
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
    OtherBucketsRAMQuota = proplists:get_value(other_buckets_ram_quota, ParsedProps, 0),
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
          end + OtherBucketsRAMQuota * NodesCount,
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
            ns_bucket:get_servers(BucketConfig)
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

validate_with_missing(GivenValue, DefaultValue, IsNew, Fn) ->
    case Fn(GivenValue) of
        {error, _, _} = Error ->
            %% Parameter validation functions return error when GivenValue
            %% is undefined or was set to an invalid value.
            %% If the user did not pass any value for the parameter
            %% (given value is undefined) during bucket create and DefaultValue is
            %% available then use it. If this is not bucket create or if
            %% DefaultValue is not available then ignore the error.
            %% If the user passed some invalid value during either bucket create or
            %% edit then return error to the user.
            case GivenValue of
                undefined ->
                    case IsNew andalso DefaultValue =/= undefined of
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
    case menelaus_util:parse_validate_number(NumReplicas, 0,
                                             ?MAX_NUM_REPLICAS) of
        invalid ->
            {error, replicaNumber, <<"The replica number must be specified and must be a non-negative integer.">>};
        too_small ->
            {error, replicaNumber, <<"The replica number cannot be negative.">>};
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
    case menelaus_util:parse_validate_number(Val, 0, ?MC_MAXINT) of
        {ok, X} ->
            {ok, max_ttl, X};
        _Error ->
            Msg = io_lib:format("Max TTL must be an integer between 0 and ~p", [?MC_MAXINT]),
            {error, maxTTL, list_to_binary(Msg)}
    end.

is_magma(Params, _BucketCfg, true = _IsNew) ->
    proplists:get_value("storageBackend", Params, "couchstore") =:= "magma";
is_magma(_Params, BucketCfg, false = _IsNew) ->
    ns_bucket:storage_mode(BucketCfg) =:= magma.

parse_validate_storage_quota_percentage(Params, BucketConfig, IsNew, Version,
                                        IsEnterprise) ->
    Percent = proplists:get_value("storageQuotaPercentage", Params),
    IsCompat = cluster_compat_mode:is_version_71(Version),
    IsMagma = is_magma(Params, BucketConfig, IsNew),
    parse_validate_storage_quota_percentage_inner(IsEnterprise, IsCompat,
                                                  Percent, BucketConfig, IsNew,
                                                  IsMagma).

parse_validate_storage_quota_percentage_inner(false = _IsEnterprise, _IsCompat,
                                              undefined = _Percent, _BucketCfg,
                                              _IsNew, _IsMagma) ->
    %% Community edition but percent/ratio wasn't specified
    ignore;
parse_validate_storage_quota_percentage_inner(_IsEnterprise, false = _IsCompat,
                                              undefined = _Percent, _BucketCfg,
                                              _IsNew, _IsMagma) ->
    %% Not cluster compatible but percent/ratio wasn't specified
    ignore;
parse_validate_storage_quota_percentage_inner(false = _IsEnterprise, _IsCompat,
                                           _Percent, _BucketCfg, _IsNew,
                                           _IsMagma) ->
    {error, storageQuotaPercentage,
     <<"Storage Quota Percentage is supported in enterprise edition only">>};
parse_validate_storage_quota_percentage_inner(_IsEnterprise, false = _IsCompat,
                                              _Percent, _BucketCfg, _IsNew,
                                              _IsMagma) ->
    {error, storageQuotaPercentage,
     <<"Storage Quota Percentage cannot be set until the cluster is fully "
       "7.1">>};
parse_validate_storage_quota_percentage_inner(true = _IsEnterprise,
                                              true = _IsCompat, undefined,
                                              _BucketCfg, _IsNew,
                                              false = _IsMagma) ->
    %% Not a magma bucket and percent wasn't specified
    ignore;
parse_validate_storage_quota_percentage_inner(true = _IsEnterprise,
                                              true = _IsCompat, _Percent,
                                              _BucketCfg, _IsNew,
                                              false = _IsMagma) ->
    {error, storageQuotaPercentage,
     <<"Storage Quota Percentage is only used with Magma">>};
parse_validate_storage_quota_percentage_inner(true = _IsEnterprise,
                                              true = _IsCompat, Percent,
                                              BucketCfg, IsNew,
                                              true = _IsMagma) ->
    DefaultVal = case IsNew of
                     true -> integer_to_list(?MAGMA_STORAGE_QUOTA_PERCENTAGE);
                     false -> proplists:get_value(storage_quota_percentage,
                                                  BucketCfg)
                 end,
    validate_with_missing(Percent, DefaultVal, IsNew,
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

parse_validate_history_retention_seconds(Params, BucketConfig, IsNew, Version,
                                         IsEnterprise) ->
    HistoryRetentionValue =
        proplists:get_value("historyRetentionSeconds", Params),
    parse_validate_history_param(history_retention_seconds,
                                 HistoryRetentionValue, Params, BucketConfig,
                                 IsNew, Version, IsEnterprise).

parse_validate_history_retention_bytes(Params, BucketConfig, IsNew,
                                       Version, IsEnterprise) ->
    HistoryRetentionValue =
        proplists:get_value("historyRetentionBytes", Params),
    parse_validate_history_param(history_retention_bytes,
                                 HistoryRetentionValue, Params, BucketConfig,
                                 IsNew, Version, IsEnterprise).

parse_validate_history_param(Key, Value, Params, BucketConfig, IsNew, Version,
                                     IsEnterprise) ->
    IsCompat = cluster_compat_mode:is_version_72(Version),
    IsMagma = is_magma(Params, BucketConfig, IsNew),
    parse_validate_history_param_inner(Key, Value, IsEnterprise, IsCompat,
                                       IsNew, IsMagma).

parse_validate_history_param_inner(_Key, undefined = _Value, _IsEnterprise,
                                   _IsCompat, _IsNew, _IsMagma) ->
    %% Value wasn't specified
    ignore;
parse_validate_history_param_inner(Key, _Value, false = _IsEnterprise,
                                   _IsCompat, _IsNew, _IsMagma) ->
    {error, Key,
        <<"History Retention is supported in enterprise edition only">>};
parse_validate_history_param_inner(Key, _Value, _IsEnterprise,
                                   false = _IsCompat, _IsNew, _IsMagma) ->
    {error, Key,
        <<"History Retention cannot be set until the cluster is fully 7.2">>};
parse_validate_history_param_inner(Key, _Value, true = _IsEnterprise,
                                   true = _IsCompat, _IsNew, false = _IsMagma) ->
    {error, Key,
        <<"History Retention can only used with Magma">>};
parse_validate_history_param_inner(Key, Value, true = _IsEnterprise,
                                   true = _IsCompat, IsNew, true = _IsMagma) ->
    %% Default to undefined (to avoid setting the parameter and defer to
    %% default memcached has set.
    DefaultVal = undefined,
    validate_with_missing(Value, DefaultVal, IsNew,
        fun (V) ->
            do_parse_validate_history_retention_seconds(Key, V)
        end).

do_parse_validate_history_retention_seconds(Key, Val) ->
    case menelaus_util:parse_validate_number(Val, 0, undefined) of
        {ok, X} ->
            {ok, Key, X};
        _Error ->
            {error, Key, <<"Value must be greater than or equal to 0">>}
    end.

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

parse_validate_eviction_policy(Params, BCfg, IsNew) ->
    IsEphemeral = is_ephemeral(Params, BCfg, IsNew),
    IsMagma = is_magma(Params, BCfg, IsNew),
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
                                  true = IsNew, _IsMagma) ->
    validate_with_missing(proplists:get_value("evictionPolicy", Params),
                          "noEviction", IsNew,
                          fun parse_validate_ephemeral_eviction_policy/1);
do_parse_validate_eviction_policy(Params, BCfg, true = _IsEphemeral,
                                  false = _IsNew, _IsMagma) ->
    case proplists:get_value("evictionPolicy", Params) of
        undefined ->
            ignore;
        Val ->
            case build_eviction_policy(BCfg) =:= list_to_binary(Val) of
                true ->
                    ignore;
                false ->
                    {error, evictionPolicy,
                     <<"Eviction policy cannot be updated for ephemeral buckets">>}
            end
    end.

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

parse_validate_other_buckets_ram_quota(Params) ->
    do_parse_validate_other_buckets_ram_quota(
      proplists:get_value("otherBucketsRamQuotaMB", Params)).

do_parse_validate_other_buckets_ram_quota(undefined) ->
    {ok, other_buckets_ram_quota, 0};
do_parse_validate_other_buckets_ram_quota(Value) ->
    case menelaus_util:parse_validate_number(Value, 0, undefined) of
        {ok, X} ->
            {ok, other_buckets_ram_quota, X * ?MIB};
        _ ->
            {error, otherBucketsRamQuotaMB,
             <<"The other buckets RAM Quota must be a positive integer.">>}
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
                    reply_json(Req, {struct, Errors}, 400)
            end;
        {not_found, _} ->
            reply_json(Req, {struct, [{'_',
                                       <<"Design document not found">>}]}, 400)
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
    Nodes = ns_cluster_membership:service_active_nodes(kv),
    Args = [X || X <- [Bucket, CollectionUId],
                 X =/= undefined],
    {ok, Res} = run_on_node({ns_memcached, get_random_key, Args},
                            Nodes, Req),
    case Res of
        {ok, Key} ->
            reply_json(Req, {struct,
                             [{ok, true},
                              {key, Key}]});
        {memcached_error, key_enoent, _} ->
            ?log_debug("No keys were found for bucket ~p. Fallback to all docs approach.", [Bucket]),
            reply_json(Req, {struct,
                             [{ok, false},
                              {error, <<"fallback_to_all_docs">>}]}, 404);
        {memcached_error, Status, Msg} ->
            ?log_error("Unable to retrieve random key for bucket ~p. Memcached returned error ~p. ~p",
                       [Bucket, Status, Msg]),
            reply_json(Req, {struct,
                             [{ok, false}]}, 404)
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
    Ctx = init_bucket_validation_context(IsNew, Name, AllBuckets,
                                         KvNodes, Groups, [],
                                         false, false,
                                         Version, IsEnterprise,
                                         %% Change when developer_preview
                                         %% defaults to false
                                         true),
    basic_bucket_params_screening(Ctx, Params).

basic_bucket_params_screening_test() ->
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (_, Default) ->
                        Default
                end),
    AllBuckets = [{"mcd",
                   [{type, memcached},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, [node1, node2]},
                    {ram_quota, 76 * ?MIB}]},
                  {"default",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, [node1, node2]},
                    {ram_quota, 512 * ?MIB}]},
                  {"third",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, [node1, node2]},
                    {ram_quota, 768 * ?MIB}]},
                  {"fourth",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 3},
                    {servers, [node1, node2]},
                    {ram_quota, 100 * ?MIB}]},
                  {"fifth",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 0},
                    {servers, [node1]},
                    {ram_quota, 300 * ?MIB}]}],

    %% it is possible to create bucket with ok params
    {OK1, E1} = basic_bucket_params_screening(true, "mcd",
                                              [{"bucketType", "membase"},
                                               {"ramQuota", "400"}, {"replicaNumber", "2"}],
                                              tl(AllBuckets)),
    [] = E1,
    %% missing fields have their defaults set
    true = proplists:is_defined(num_threads, OK1),
    true = proplists:is_defined(eviction_policy, OK1),
    true = proplists:is_defined(replica_index, OK1),

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

    %% its not possible to update memcached bucket ram quota
    {_OK7, E7} = basic_bucket_params_screening(false, "mcd",
                                               [{"bucketType", "membase"},
                                                {"ramQuota", "1024"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    ?assertEqual(true, lists:member(ramQuota, proplists:get_keys(E7))),

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
                      {"historyRetentionBytes", "10"}],
                     AllBuckets,
                     [node1],
                     false),
    ?assertEqual([{storageBackend,
                   <<"Magma is supported in enterprise edition only">>},
                  {history_retention_seconds,
                   <<"History Retention is supported in enterprise edition "
                     "only">>},
                  {history_retention_bytes,
                   <<"History Retention is supported in enterprise edition "
                      "only">>}],
                 E20),

    {_OK21, E21} = basic_bucket_params_screening(
                     true,
                     "HistoryEnterpriseNotMagma",
                     [{"bucketType", "membase"},
                      {"ramQuota", "400"},
                      {"historyRetentionSeconds", "10"},
                      {"historyRetentionBytes", "10"}],
                     AllBuckets,
                     [node1]),
    ?assertEqual([{history_retention_seconds,
                   <<"History Retention can only used with Magma">>},
                  {history_retention_bytes,
                   <<"History Retention can only used with Magma">>}],
                 E21),

    meck:new(cluster_compat_mode, [passthrough]),
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
                      {"historyRetentionBytes", "10"}],
                     AllBuckets,
                     [node1]),
    ?assertEqual([{history_retention_seconds,
                    <<"History Retention cannot be set until the cluster is "
                       "fully 7.2">>},
                  {history_retention_bytes,
                    <<"History Retention cannot be set until the cluster is "
                      "fully 7.2">>}],
                 E22),

    meck:unload(cluster_compat_mode),

    {_OK23, E23} = basic_bucket_params_screening(
        true,
        "HistoryEnterpriseMagma7.1",
        [{"bucketType", "membase"},
            {"ramQuota", "1024"},
            {"storageBackend", "magma"},
            {"historyRetentionSeconds", "-1"},
            {"historyRetentionBytes", "-1"}],
        AllBuckets,
        [node1]),
    ?assertEqual([{history_retention_seconds,
                   <<"Value must be greater than or equal to 0">>},
                  {history_retention_bytes,
                   <<"Value must be greater than or equal to 0">>}],
        E23),

    {OK24, E24} = basic_bucket_params_screening(
                     true,
                     "HistoryEnterpriseMagma",
                     [{"bucketType", "membase"},
                      {"ramQuota", "1024"},
                      {"storageBackend", "magma"},
                      {"historyRetentionSeconds", "10"},
                      {"historyRetentionBytes", "10"}],
                     AllBuckets,
                     [node1]),
    ?assertEqual([], E24),
    ?assert(lists:any(fun (Elem) ->
                          Elem =:= {history_retention_seconds, 10}
                      end, OK24)),
    ?assert(lists:any(fun (Elem) ->
                          Elem =:= {history_retention_bytes, 10}
                      end, OK24)),

    meck:unload(ns_config),

    ok.

basic_parse_validate_bucket_auto_compaction_settings_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_cluster_71,
                fun () -> true end),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, get,
                fun () -> [] end),
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
    meck:unload(chronicle_kv),
    ok.

parse_validate_pitr_max_history_age_test() ->
    %% "Constants" used to make parse_validate_pitr_numeric_param() calls in
    %% this test clearer.
    IsNewTrue = true,
    IsNewFalse = false,

    LegitParams = [{"pitrMaxHistoryAge", "100"}],

    %% For these legitimate params tests, the value of IsNew shouldn't matter.

    %% sub-test: legitimate params, IsNew true
    Result1 = parse_validate_pitr_numeric_param(
                LegitParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewTrue),
    Expected1 = {ok, pitr_max_history_age, 100},
    ?assertEqual(Expected1, Result1),

    %% sub-test: legitimate params, IsNew false
    Result2 = parse_validate_pitr_numeric_param(
                LegitParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewFalse),
    Expected2 = {ok, pitr_max_history_age, 100},
    ?assertEqual(Expected2, Result2),

    NonNumericParams = [{"pitrMaxHistoryAge", "foo"}],

    %% sub-test: non-numeric params, IsNew true
    Result3 = parse_validate_pitr_numeric_param(
                NonNumericParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewTrue),
    Expected3 = value_not_numeric_error(pitrMaxHistoryAge, "foo"),
    ?assertEqual(Expected3, Result3),

    %% sub-test: non-numeric params, IsNew false
    Result4 = parse_validate_pitr_numeric_param(
                NonNumericParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewFalse),
    Expected4 = value_not_numeric_error(pitrMaxHistoryAge, "foo"),
    ?assertEqual(Expected4, Result4),

    TooSmallParams = [{"pitrMaxHistoryAge", "0"}],

    %% sub-test: too small params, IsNew true
    Result5 = parse_validate_pitr_numeric_param(
                TooSmallParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewTrue),
    Expected5 = value_not_in_range_error(
                  pitrMaxHistoryAge, "0",
                  ns_bucket:attribute_min(pitr_max_history_age),
                  ns_bucket:attribute_max(pitr_max_history_age)),
    ?assertEqual(Expected5, Result5),

    %% sub-test: too small params, IsNew false
    Result6 = parse_validate_pitr_numeric_param(
                TooSmallParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewFalse),
    Expected6 = value_not_in_range_error(
                  pitrMaxHistoryAge, "0",
                  ns_bucket:attribute_min(pitr_max_history_age),
                  ns_bucket:attribute_max(pitr_max_history_age)),
    ?assertEqual(Expected6, Result6),

    TooBigParams = [{"pitrMaxHistoryAge", "172801"}],

    %% sub-test: too big params, IsNew true
    Result7 = parse_validate_pitr_numeric_param(
                TooBigParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewTrue),
    Expected7 = value_not_in_range_error(
                  pitrMaxHistoryAge, "172801",
                  ns_bucket:attribute_min(pitr_max_history_age),
                  ns_bucket:attribute_max(pitr_max_history_age)),
    ?assertEqual(Expected7, Result7),

    %% sub-test: too big params, IsNew false
    Result8 = parse_validate_pitr_numeric_param(
                TooBigParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewFalse),
    Expected8 = value_not_in_range_error(
                  pitrMaxHistoryAge, "172801",
                  ns_bucket:attribute_min(pitr_max_history_age),
                  ns_bucket:attribute_max(pitr_max_history_age)),
    ?assertEqual(Expected8, Result8),

    MissingParams = [],

    %% sub-test: missing params, IsNew true
    %% The result should be the default value.
    Result9 = parse_validate_pitr_numeric_param(
                MissingParams,
                pitrMaxHistoryAge,
                pitr_max_history_age,
                IsNewTrue),
    Expected9 = {ok, pitr_max_history_age, 86400},
    ?assertEqual(Expected9, Result9),

    %% sub-test: missing params, IsNew false.
    %% The missing parameters should be ignored.
    Result10 = parse_validate_pitr_numeric_param(
                 MissingParams,
                 pitrMaxHistoryAge,
                 pitr_max_history_age,
                 IsNewFalse),
    Expected10 = ignore,
    ?assertEqual(Expected10, Result10).

get_conflict_resolution_type_and_thresholds_test() ->
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

    meck:unload(cluster_compat_mode).
-endif.
