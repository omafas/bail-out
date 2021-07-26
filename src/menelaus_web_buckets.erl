%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2020 Couchbase, Inc.
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
%% @doc handlers for bucket related REST API's

-module(menelaus_web_buckets).

-author('NorthScale <info@northscale.com>').

-include("menelaus_web.hrl").
-include("ns_common.hrl").
-include("couch_db.hrl").
-include("ns_stats.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([checking_bucket_uuid/3,
         handle_bucket_list/1,
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
         handle_local_random_key/3,
         build_bucket_capabilities/1,
         external_bucket_type/1,
         display_type/2,
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

checking_bucket_uuid(Req, BucketConfig, Body) ->
    ReqUUID0 = proplists:get_value("bucket_uuid", mochiweb_request:parse_qs(Req)),
    case ReqUUID0 =/= undefined of
        true ->
            ReqUUID = list_to_binary(ReqUUID0),
            BucketUUID = ns_bucket:bucket_uuid(BucketConfig),

            case BucketUUID =:= ReqUUID of
                true ->
                    Body();
                false ->
                    reply_text(Req, "Bucket uuid does not match the requested.\r\n", 404)
            end;
        false ->
            Body()
    end.

may_expose_bucket_auth(Name, Req) ->
    case menelaus_auth:get_token(Req) of
        undefined ->
            menelaus_auth:has_permission({[{bucket, Name}, password], read}, Req);
        _ ->
            false
    end.

handle_bucket_list(Req) ->
    BucketNamesUnsorted =
        menelaus_auth:get_accessible_buckets(fun (BucketName) ->
                                                     {[{bucket, BucketName}, settings], read}
                                             end, Req),

    BucketNames = lists:sort(fun (A,B) -> A =< B end, BucketNamesUnsorted),

    LocalAddr = menelaus_util:local_addr(Req),
    InfoLevel = case proplists:get_value("basic_stats", mochiweb_request:parse_qs(Req)) of
                    undefined -> normal;
                    _ -> for_ui
                end,
    SkipMap = proplists:get_value("skipMap", mochiweb_request:parse_qs(Req)) =:= "true",
    BucketsInfo = [build_bucket_info(Name, undefined, InfoLevel, LocalAddr,
                                     may_expose_bucket_auth(Name, Req), SkipMap)
                   || Name <- BucketNames],
    reply_json(Req, BucketsInfo).

handle_bucket_info(_PoolId, Id, Req) ->
    InfoLevel = case proplists:get_value("basic_stats", mochiweb_request:parse_qs(Req)) of
                    undefined -> normal;
                    _ -> for_ui
                end,
    SkipMap = proplists:get_value("skipMap", mochiweb_request:parse_qs(Req)) =:= "true",
    reply_json(Req, build_bucket_info(Id, undefined, InfoLevel,
                                      menelaus_util:local_addr(Req),
                                      may_expose_bucket_auth(Id, Req), SkipMap)).

build_bucket_node_infos(BucketName, BucketConfig, InfoLevel0, LocalAddr) ->
    {InfoLevel, Stability} = convert_info_level(InfoLevel0),
    %% Only list nodes this bucket is mapped to
    F = menelaus_web_node:build_nodes_info_fun(false, InfoLevel, Stability, LocalAddr),
    Nodes = ns_bucket:get_servers(BucketConfig),
    %% NOTE: there's potential inconsistency here between BucketConfig
    %% and (potentially more up-to-date) vbuckets dict. Given that
    %% nodes list is mostly informational I find it ok.
    Dict = case vbucket_map_mirror:node_vbuckets_dict(BucketName) of
               {ok, DV} -> DV;
               {error, not_present} -> dict:new();
               {error, no_map} -> dict:new()
           end,
    BucketUUID = ns_bucket:bucket_uuid(BucketConfig),
    add_couch_api_base_loop(Nodes, BucketName, BucketUUID, LocalAddr, F, Dict, [], []).


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

%% Used while building the bucket info. This transforms the internal
%% representation of bucket types to externally known bucket types.
%% Ideally the 'display_type' function should suffice here but there
%% is too much reliance on the atom membase by other modules (ex: xdcr).
external_bucket_type(BucketConfig) ->
    external_bucket_type(ns_bucket:bucket_type(BucketConfig), BucketConfig).

external_bucket_type(memcached = _Type, _) ->
    memcached;
external_bucket_type(membase = _Type, BucketConfig) ->
    case ns_bucket:storage_mode(BucketConfig) of
        couchstore ->
            membase;
        ephemeral ->
            ephemeral
    end.

build_auto_compaction_info(BucketConfig, couchstore) ->
    ACSettings = case proplists:get_value(autocompaction, BucketConfig) of
                     undefined -> false;
                     false -> false;
                     ACSettingsX -> ACSettingsX
                 end,

    case ACSettings of
        false ->
            [{autoCompactionSettings, false}];
        _ ->
            [{autoCompactionSettings,
              menelaus_web_autocompaction:build_bucket_settings(ACSettings)}]
    end;
build_auto_compaction_info(_BucketConfig, ephemeral) ->
    [];
build_auto_compaction_info(_BucketConfig, undefined) ->
    %% When the bucket type is memcached.
    [{autoCompactionSettings, false}].

build_purge_interval_info(BucketConfig, couchstore) ->
    case proplists:get_value(autocompaction, BucketConfig, false) of
        false ->
            [];
        _Val ->
            PInterval = case proplists:get_value(purge_interval, BucketConfig) of
                            undefined -> compaction_api:get_purge_interval(global);
                            PI -> PI
                        end,
            [{purgeInterval, PInterval}]
    end;
build_purge_interval_info(BucketConfig, ephemeral) ->
    [{purgeInterval, proplists:get_value(purge_interval, BucketConfig)}];
build_purge_interval_info(_BucketConfig, undefined) ->
    %% When the bucket type is memcached.
    [].

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

build_bucket_info(Id, undefined, InfoLevel, LocalAddr, MayExposeAuth,
                  SkipMap) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Id),
    build_bucket_info(Id, BucketConfig, InfoLevel, LocalAddr, MayExposeAuth,
                      SkipMap);
build_bucket_info(Id, BucketConfig, InfoLevel, LocalAddr, MayExposeAuth,
                  SkipMap) ->
    Nodes = build_bucket_node_infos(Id, BucketConfig, InfoLevel, LocalAddr),
    StatsUri = bin_concat_path(["pools", "default", "buckets", Id, "stats"]),
    StatsDirectoryUri = iolist_to_binary([StatsUri, <<"Directory">>]),
    NodeStatsListURI = bin_concat_path(["pools", "default", "buckets", Id, "nodes"]),
    BucketCaps = build_bucket_capabilities(BucketConfig),

    BucketUUID = ns_bucket:bucket_uuid(BucketConfig),
    QSProps = [{"bucket_uuid", BucketUUID}],

    BuildUUIDURI = fun (Segments) ->
                           bin_concat_path(Segments, QSProps)
                   end,

    EvictionPolicy = build_eviction_policy(BucketConfig),
    ConflictResolutionType = ns_bucket:conflict_resolution_type(BucketConfig),
    DurabilityMinLevel = build_durability_min_level(BucketConfig),

    Suffix = case InfoLevel of
                 streaming ->
                     BucketCaps;
                 _ ->
                     BasicStats0 = menelaus_stats:basic_stats(Id),

                     BasicStats = case InfoLevel of
                                      for_ui ->
                                          StorageTotals = [{Key, {struct, StoragePList}}
                                                           || {Key, StoragePList} <- ns_storage_conf:cluster_storage_info()],

                                          [{storageTotals, {struct, StorageTotals}} | BasicStats0];
                                      _ -> BasicStats0
                                  end,

                     BucketParams =
                         [{replicaNumber, ns_bucket:num_replicas(BucketConfig)},
                          {threadsNumber, proplists:get_value(num_threads, BucketConfig, 3)},
                          {quota, {struct, [{ram, ns_bucket:ram_quota(BucketConfig)},
                                            {rawRAM, ns_bucket:raw_ram_quota(BucketConfig)}]}},
                          {basicStats, {struct, BasicStats}},
                          {evictionPolicy, EvictionPolicy},
                          {durabilityMinLevel, DurabilityMinLevel},
                          {conflictResolutionType, ConflictResolutionType}
                          | BucketCaps],

                     BucketParams1 =
                         case cluster_compat_mode:is_enterprise() andalso
                             cluster_compat_mode:is_cluster_55() of
                             true ->
                                 CMode = proplists:get_value(compression_mode,
                                                             BucketConfig, off),
                                 [{maxTTL, proplists:get_value(max_ttl,
                                                               BucketConfig, 0)},
                                  {compressionMode, CMode} |
                                  BucketParams];
                             false ->
                                 BucketParams
                         end,

                     case ns_bucket:drift_thresholds(BucketConfig) of
                         undefined ->
                             BucketParams1;
                         {DriftAheadThreshold, DriftBehindThreshold} ->
                             [{driftAheadThresholdMs, DriftAheadThreshold},
                              {driftBehindThresholdMs, DriftBehindThreshold}
                              | BucketParams1]
                     end
             end,
    BucketType = ns_bucket:bucket_type(BucketConfig),
    %% Only list nodes this bucket is mapped to
    %% Leave vBucketServerMap key out for memcached buckets; this is
    %% how Enyim decides to use ketama
    Suffix1 = case BucketType of
                  membase ->
                      case SkipMap of
                          false ->
                              [{vBucketServerMap, ns_bucket:json_map_from_config(
                                                    LocalAddr, BucketConfig)} |
                               Suffix];
                          _ ->
                              Suffix
                      end;
                  memcached ->
                      Suffix
              end,

    StorageMode = ns_bucket:storage_mode(BucketConfig),
    ACInfo = build_auto_compaction_info(BucketConfig, StorageMode),
    PIInfo = build_purge_interval_info(BucketConfig, StorageMode),
    Suffix2 = ACInfo ++ PIInfo ++ Suffix1 ,

    Suffix3 = case StorageMode of
                  couchstore ->
                      DDocsURI = bin_concat_path(["pools", "default", "buckets",
                                                  Id, "ddocs"]),
                      [{ddocs, {struct, [{uri, DDocsURI}]}},
                       {replicaIndex, proplists:get_value(replica_index, BucketConfig, true)}
                       | Suffix2];
                  _ ->
                      Suffix2
              end,

    FlushEnabled = proplists:get_value(flush_enabled, BucketConfig, false),
    MaybeFlushController =
        case FlushEnabled of
            true ->
                [{flush, bin_concat_path(["pools", "default",
                                          "buckets", Id, "controller", "doFlush"])}];
            false ->
                []
        end,

    Suffix4 = case MayExposeAuth of
                  true ->
                      [{saslPassword,
                       list_to_binary(proplists:get_value(sasl_password, BucketConfig, ""))} |
                       Suffix3];
                  false ->
                      Suffix3
              end,

    {struct, [{name, list_to_binary(Id)},
              {uuid, BucketUUID},
              {bucketType, external_bucket_type(BucketType, BucketConfig)},
              {authType, misc:expect_prop_value(auth_type, BucketConfig)},
              {uri, BuildUUIDURI(["pools", "default", "buckets", Id])},
              {streamingUri, BuildUUIDURI(["pools", "default", "bucketsStreaming", Id])},
              {localRandomKeyUri, bin_concat_path(["pools", "default",
                                                   "buckets", Id, "localRandomKey"])},
              {controllers,
               {struct,
                MaybeFlushController ++
                    [{compactAll, bin_concat_path(["pools", "default",
                                                   "buckets", Id, "controller", "compactBucket"])},
                     {compactDB, bin_concat_path(["pools", "default",
                                                  "buckets", Id, "controller", "compactDatabases"])},
                     {purgeDeletes, bin_concat_path(["pools", "default",
                                                     "buckets", Id, "controller", "unsafePurgeBucket"])},
                     {startRecovery, bin_concat_path(["pools", "default",
                                                      "buckets", Id, "controller", "startRecovery"])}]}},
              {nodes, Nodes},
              {stats, {struct, [{uri, StatsUri},
                                {directoryURI, StatsDirectoryUri},
                                {nodeStatsListURI, NodeStatsListURI}]}},
              {nodeLocator, ns_bucket:node_locator(BucketConfig)}
              | Suffix4]}.

build_bucket_capabilities(BucketConfig) ->
    Caps =
        case ns_bucket:bucket_type(BucketConfig) of
            membase ->
                Conditional =
                    [{collections, collections:enabled(BucketConfig)},
                     {durableWrite, cluster_compat_mode:is_cluster_65()},
                     {tombstonedUserXAttrs,
                      cluster_compat_mode:is_cluster_66()},
                     {couchapi,
                      ns_bucket:storage_mode(BucketConfig) =:= couchstore}],

                [C || {C, true} <- Conditional] ++
                    [dcp, cbhello, touch, cccp, xdcrCheckpointing, nodesExt,
                     xattr];
            memcached ->
                [cbhello, nodesExt]
        end,

    [{bucketCapabilitiesVer, ''},
     {bucketCapabilities, Caps}].

handle_sasl_buckets_streaming(_PoolId, Req) ->
    LocalAddr = menelaus_util:local_addr(Req),

    F = fun (_, _) ->
                Config = ns_config:get(),
                SASLBuckets = lists:filter(
                                fun ({_, BucketInfo}) ->
                                        ns_bucket:auth_type(BucketInfo) =:= sasl
                                end, ns_bucket:get_buckets(Config)),
                List =
                    lists:map(
                      fun ({Name, BucketInfo}) ->
                              BucketNodes =
                                  [begin
                                       Hostname =
                                           list_to_binary(
                                             menelaus_web_node:build_node_hostname(Config, N, LocalAddr)),
                                       DirectPort =
                                           service_ports:get_port(
                                             memcached_port, Config, N),
                                       {struct, [{hostname, Hostname},
                                                 {ports,
                                                  {struct,
                                                   [{direct, DirectPort}]}}]}
                                   end ||
                                      N <- ns_bucket:get_servers(BucketInfo)],
                              VBM = case ns_bucket:bucket_type(BucketInfo) of
                                        membase ->
                                            [{vBucketServerMap,
                                              ns_bucket:json_map_from_config(
                                                LocalAddr, BucketInfo)}];
                                        memcached ->
                                            []
                                    end,
                              {struct,
                               [{name, list_to_binary(Name)},
                                {nodeLocator,
                                 ns_bucket:node_locator(BucketInfo)},
                                {saslPassword,
                                 list_to_binary(
                                   proplists:get_value(sasl_password,
                                                       BucketInfo, ""))},
                                {nodes, BucketNodes} | VBM]}
                      end, SASLBuckets),
                {just_write, {struct, [{buckets, List}]}}
        end,
    handle_streaming(F, Req).

handle_bucket_info_streaming(_PoolId, Id, Req) ->
    LocalAddr = menelaus_util:local_addr(Req),
    F = fun(_Stability, _UpdateID) ->
                case ns_bucket:get_bucket(Id) of
                    {ok, BucketConfig} ->
                        Info = build_bucket_info(
                                 Id, BucketConfig, streaming, LocalAddr,
                                 may_expose_bucket_auth(Id, Req), false),
                        {just_write, Info};
                    not_present ->
                        exit(normal)
                end
        end,
    handle_streaming(F, Req).

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

%% returns pprop list with only props useful for ns_bucket
extract_bucket_props(Props) ->
    [X || X <-
              [lists:keyfind(Y, 1, Props) ||
                  Y <- [num_replicas, replica_index, ram_quota, auth_type,
                        durability_min_level,
                        sasl_password, moxi_port, autocompaction,
                        purge_interval, flush_enabled, num_threads,
                        eviction_policy, conflict_resolution_type,
                        drift_ahead_threshold_ms, drift_behind_threshold_ms,
                        storage_mode, max_ttl, compression_mode]],
          X =/= false].

-record(bv_ctx, {
          validate_only,
          ignore_warnings,
          new,
          bucket_name,
          bucket_config,
          all_buckets,
          cluster_storage_totals,
          cluster_version,
          is_enterprise}).

init_bucket_validation_context(IsNew, BucketName, Req) ->
    ValidateOnly = (proplists:get_value("just_validate", mochiweb_request:parse_qs(Req)) =:= "1"),
    IgnoreWarnings = (proplists:get_value("ignore_warnings", mochiweb_request:parse_qs(Req)) =:= "1"),
    init_bucket_validation_context(IsNew, BucketName, ValidateOnly, IgnoreWarnings).

init_bucket_validation_context(IsNew, BucketName, ValidateOnly, IgnoreWarnings) ->
    init_bucket_validation_context(IsNew, BucketName,
                                   ns_bucket:get_buckets(),
                                   extended_cluster_storage_info(),
                                   ValidateOnly, IgnoreWarnings,
                                   cluster_compat_mode:get_compat_version(),
                                   cluster_compat_mode:is_enterprise()).

init_bucket_validation_context(IsNew, BucketName, AllBuckets, ClusterStorageTotals,
                               ValidateOnly, IgnoreWarnings,
                               ClusterVersion, IsEnterprise) ->
    {BucketConfig, ExtendedTotals} =
        case lists:keyfind(BucketName, 1, AllBuckets) of
            false -> {false, ClusterStorageTotals};
            {_, V} ->
                case ns_bucket:get_servers(V) of
                    [] ->
                        {V, ClusterStorageTotals};
                    Servers ->
                        ServersCount = length(Servers),
                        {V, lists:keyreplace(nodesCount, 1, ClusterStorageTotals, {nodesCount, ServersCount})}
                end
        end,
    #bv_ctx{
       validate_only = ValidateOnly,
       ignore_warnings = IgnoreWarnings,
       new = IsNew,
       bucket_name = BucketName,
       all_buckets = AllBuckets,
       bucket_config = BucketConfig,
       cluster_storage_totals = ExtendedTotals,
       cluster_version = ClusterVersion,
       is_enterprise = IsEnterprise
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
            UpdatedProps = extract_bucket_props(ParsedProps),
            case ns_orchestrator:update_bucket(BucketType, StorageMode,
                                               BucketId, UpdatedProps) of
                ok ->
                    ns_audit:modify_bucket(Req, BucketId, BucketType, UpdatedProps),
                    DisplayBucketType = display_type(BucketType, StorageMode),
                    ale:info(?USER_LOGGER, "Updated bucket \"~s\" (of type ~s) properties:~n~p",
                             [BucketId, DisplayBucketType,
                              lists:keydelete(sasl_password, 1, UpdatedProps)]),
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
    BucketProps = extract_bucket_props(ParsedProps),
    maybe_cleanup_old_buckets(),
    case ns_orchestrator:create_bucket(BucketType, Name, BucketProps) of
        ok ->
            ns_audit:create_bucket(Req, Name, BucketType, BucketProps),
            DisplayBucketType = display_type(BucketType, StorageMode),
            ?MENELAUS_WEB_LOG(?BUCKET_CREATED, "Created bucket \"~s\" of type: ~s~n~p",
                              [Name, DisplayBucketType, lists:keydelete(sasl_password, 1, BucketProps)]),
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
                            {{struct, Errors}, 400};
                        {errors_500, Errors} ->
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
    ActiveCount = length(ns_cluster_membership:service_active_nodes(kv)),
    Warnings =
        if
            ActiveCount =< NReplicas ->
                ["you do not have enough data servers to support this number of replicas"];
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

%% Default bucket type is now couchbase and not membase. Ideally, we should
%% change the default bucket type atom to couchbase but the bucket type membase
%% is used/checked at multiple locations. For similar reasons, the ephemeral
%% bucket type also gets stored as 'membase' and to differentiate between the
%% couchbase and ephemeral buckets we store an extra parameter called
%% 'storage_mode'. So to fix the log message to display the correct bucket type
%% we use both type and storage_mode parameters of the bucket config.
display_type(membase = _Type, couchstore = _StorageMode) ->
    couchbase;
display_type(membase = _Type, ephemeral = _StorageMode) ->
    ephemeral;
display_type(Type, _) ->
    Type.

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

do_handle_bucket_flush(Id, Req) ->
    case ns_orchestrator:flush_bucket(Id) of
        ok ->
            ns_audit:flush_bucket(Req, Id),
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
                         interpret_ram_quota(CurrentBucket, OKs,
                                             ClusterStorageTotals);
                     true ->
                         interpret_ram_quota(CurrentBucket,
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
                [{ramQuotaMB, <<"RAM quota specified is too large to be provisioned into this cluster.">>}];
            RAMSummary#ram_summary.this_alloc < RAMSummary#ram_summary.this_used ->
                [{ramQuotaMB, <<"RAM quota cannot be set below current usage.">>}];
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

basic_bucket_params_screening(#bv_ctx{bucket_config = false, new = false}, _Params) ->
    {[], [{name, <<"Bucket with given name doesn't exist">>}]};
basic_bucket_params_screening(Ctx, Params) ->
    CommonParams = validate_common_params(Ctx, Params),
    TypeSpecificParams =
        validate_bucket_type_specific_params(CommonParams, Params, Ctx),
    Candidates = CommonParams ++ TypeSpecificParams,
    assert_candidates(Candidates),
    {[{K,V} || {ok, K, V} <- Candidates],
     [{K,V} || {error, K, V} <- Candidates]}.

validate_common_params(#bv_ctx{bucket_name = BucketName,
                               bucket_config = BucketConfig, new = IsNew,
                               all_buckets = AllBuckets}, Params) ->
    [{ok, name, BucketName},
     parse_validate_flush_enabled(Params, IsNew),
     validate_bucket_name(IsNew, BucketConfig, BucketName, AllBuckets),
     parse_validate_ram_quota(Params, BucketConfig),
     parse_validate_other_buckets_ram_quota(Params),
     validate_moxi_port(Params)].

validate_bucket_type_specific_params(CommonParams, Params,
                                     #bv_ctx{new = IsNew,
                                             bucket_config = BucketConfig,
                                             cluster_version = Version,
                                             is_enterprise = IsEnterprise}) ->
    BucketType = get_bucket_type(IsNew, BucketConfig, Params),

    case BucketType of
        memcached ->
            validate_memcached_bucket_params(CommonParams, Params, IsNew,
                                             BucketConfig);
        membase ->
            validate_membase_bucket_params(CommonParams, Params, IsNew,
                                           BucketConfig, Version, IsEnterprise);
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
                               IsNew, BucketConfig, Version, IsEnterprise) ->
    ReplicasNumResult = validate_replicas_number(Params, IsNew),
    BucketParams =
        [{ok, bucketType, membase},
         ReplicasNumResult,
         parse_validate_replica_index(Params, ReplicasNumResult, IsNew),
         parse_validate_threads_number(Params, IsNew),
         parse_validate_eviction_policy(Params, BucketConfig, IsNew),
         quota_size_error(CommonParams, membase, IsNew, BucketConfig),
         get_storage_mode(Params, BucketConfig, IsNew),
         parse_validate_durability_min_level(Params, BucketConfig, IsNew,
                                             Version),
         parse_validate_max_ttl(Params, BucketConfig,
                                IsNew, Version, IsEnterprise),
         parse_validate_compression_mode(Params, BucketConfig,
                                         IsNew, Version, IsEnterprise)
         | validate_bucket_auto_compaction_settings(Params)],

    validate_bucket_purge_interval(Params, BucketConfig, IsNew) ++
        get_conflict_resolution_type_and_thresholds(Params, BucketConfig, IsNew) ++
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

validate_moxi_port(Params) ->
    do_validate_moxi_port(proplists:get_value("proxyPort", Params)).

do_validate_moxi_port(undefined) ->
    ignore;
do_validate_moxi_port("none") ->
    %% needed for pre-6.5 clusters only
    {ok, moxi_port, undefined};
do_validate_moxi_port(_) ->
    {error, proxyPort,
     <<"Setting proxy port is not allowed on this version of the cluster">>}.

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
        {ok, ram_quota, RAMQuotaMB} ->
            {MinQuota, Msg}
                = case BucketType of
                      membase ->
                          Q = misc:get_env_default(membase_min_ram_quota, 100),
                          Qv = list_to_binary(integer_to_list(Q)),
                          {Q, <<"RAM quota cannot be less than ", Qv/binary, " MB">>};
                      memcached ->
                          Q = misc:get_env_default(memcached_min_ram_quota, 64),
                          Qv = list_to_binary(integer_to_list(Q)),
                          {Q, <<"RAM quota cannot be less than ", Qv/binary, " MB">>}
                  end,
            if
                RAMQuotaMB < MinQuota * ?MIB ->
                    {error, ramQuotaMB, Msg};
                IsNew =/= true andalso BucketConfig =/= false andalso BucketType =:= memcached ->
                    case ns_bucket:raw_ram_quota(BucketConfig) of
                        RAMQuotaMB -> ignore;
                        _ ->
                            {error, ramQuotaMB, <<"cannot change quota of memcached buckets">>}
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
    BucketType = external_bucket_type(BucketConfig),
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
get_storage_mode(Params, BucketConfig, IsNew) ->
    case is_ephemeral(Params, BucketConfig, IsNew) of
        true ->
            {ok, storage_mode, ephemeral};
        false ->
            {ok, storage_mode, couchstore}
    end.

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

get_conflict_resolution_type_and_thresholds(Params, _BucketConfig, true = IsNew) ->
    case proplists:get_value("conflictResolutionType", Params) of
        undefined ->
            [{ok, conflict_resolution_type, seqno}];
        Value ->
            ConResType = parse_validate_conflict_resolution_type(Value),
            case ConResType of
                {ok, _, lww} ->
                    [ConResType,
                     get_drift_ahead_threshold(Params, IsNew),
                     get_drift_behind_threshold(Params, IsNew)];
                _ ->
                    [ConResType]
            end
    end;
get_conflict_resolution_type_and_thresholds(Params, BucketConfig, false = IsNew) ->
    case proplists:get_value("conflictResolutionType", Params) of
        undefined ->
            case ns_bucket:conflict_resolution_type(BucketConfig) of
                lww ->
                    [get_drift_ahead_threshold(Params, IsNew),
                     get_drift_behind_threshold(Params, IsNew)];
                seqno ->
                    []
            end;
        _Any ->
            [{error, conflictResolutionType,
              <<"Conflict resolution type not allowed in update bucket">>}]
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

interpret_ram_quota(CurrentBucket, ParsedProps, ClusterStorageTotals) ->
    RAMQuota = proplists:get_value(ram_quota, ParsedProps),
    OtherBucketsRAMQuota = proplists:get_value(other_buckets_ram_quota, ParsedProps, 0),
    NodesCount = proplists:get_value(nodesCount, ClusterStorageTotals),
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
    case menelaus_util:parse_validate_number(NumReplicas, 0, 3) of
        invalid ->
            {error, replicaNumber, <<"The replica number must be specified and must be a non-negative integer.">>};
        too_small ->
            {error, replicaNumber, <<"The replica number cannot be negative.">>};
        too_large ->
            {error, replicaNumber, <<"Replica number larger than 3 is not supported.">>};
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

parse_validate_compression_mode(Params, BucketConfig, IsNew,
                                Version, IsEnterprise) ->
    CompMode = proplists:get_value("compressionMode", Params),
    do_parse_validate_compression_mode(
      IsEnterprise,
      cluster_compat_mode:is_version_55(Version),
      CompMode, BucketConfig, IsNew).

do_parse_validate_compression_mode(false, _, undefined, _BucketCfg, _IsNew) ->
    {ok, compression_mode, off};
do_parse_validate_compression_mode(_, false, undefined, _BucketCfg, _IsNew) ->
    ignore;
do_parse_validate_compression_mode(false, _, _CompMode, _BucketCfg, _IsNew) ->
    {error, compressionMode,
     <<"Compression mode is supported in enterprise edition only">>};
do_parse_validate_compression_mode(_, false, _CompMode, _BucketCfg, _IsNew) ->
    {error, compressionMode,
     <<"Compression mode can not be set until the cluster is fully 5.5">>};
do_parse_validate_compression_mode(true, true, CompMode, BucketCfg, IsNew) ->
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

parse_validate_max_ttl(Params, BucketConfig, IsNew, Version, IsEnterprise) ->
    MaxTTL = proplists:get_value("maxTTL", Params),
    parse_validate_max_ttl_inner(IsEnterprise,
                                 cluster_compat_mode:is_version_55(Version),
                                 MaxTTL, BucketConfig, IsNew).

parse_validate_max_ttl_inner(false, _, undefined, _BucketCfg, _IsNew) ->
    {ok, max_ttl, 0};
parse_validate_max_ttl_inner(_, false, undefined, _BucketCfg, _IsNew) ->
    ignore;
parse_validate_max_ttl_inner(false, _, _MaxTTL, _BucketCfg, _IsNew) ->
    {error, maxTTL, <<"Max TTL is supported in enterprise edition only">>};
parse_validate_max_ttl_inner(_, false, _MaxTTL, _BucketCfg, _IsNew) ->
    {error, maxTTL, <<"Max TTL can not be set until the cluster is fully 5.5">>};
parse_validate_max_ttl_inner(true, true, MaxTTL, BucketCfg, IsNew) ->
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

parse_validate_threads_number(Params, IsNew) ->
    validate_with_missing(proplists:get_value("threadsNumber", Params),
                          "3", IsNew, fun parse_validate_threads_number/1).

parse_validate_flush_enabled("0") -> {ok, flush_enabled, false};
parse_validate_flush_enabled("1") -> {ok, flush_enabled, true};
parse_validate_flush_enabled(_ReplicaValue) -> {error, flushEnabled, <<"flushEnabled can only be 1 or 0">>}.

parse_validate_threads_number(NumThreads) ->
    case menelaus_util:parse_validate_number(NumThreads, 2, 8) of
        invalid ->
            {error, threadsNumber,
             <<"The number of threads must be an integer between 2 and 8">>};
        too_small ->
            {error, threadsNumber,
             <<"The number of threads can't be less than 2">>};
        too_large ->
            {error, threadsNumber,
             <<"The number of threads can't be greater than 8">>};
        {ok, X} ->
            {ok, num_threads, X}
    end.

parse_validate_eviction_policy(Params, BCfg, IsNew) ->
    IsEphemeral = is_ephemeral(Params, BCfg, IsNew),
    do_parse_validate_eviction_policy(Params, BCfg, IsEphemeral, IsNew).

do_parse_validate_eviction_policy(Params, _BCfg, false = _IsEphemeral, IsNew) ->
    validate_with_missing(proplists:get_value("evictionPolicy", Params),
                          "valueOnly", IsNew,
                          fun parse_validate_membase_eviction_policy/1);
do_parse_validate_eviction_policy(Params, _BCfg, true = _IsEphemeral,
                                  true = IsNew) ->
    validate_with_missing(proplists:get_value("evictionPolicy", Params),
                          "noEviction", IsNew,
                          fun parse_validate_ephemeral_eviction_policy/1);
do_parse_validate_eviction_policy(Params, BCfg, true = _IsEphemeral,
                                  false = _IsNew) ->
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
    case menelaus_util:parse_validate_number(Threshold, 100, undefined) of
        invalid ->
            {error, driftBehindThresholdMs,
             <<"The drift behind threshold must be an integer not less than 100ms">>};
        too_small ->
            {error, driftBehindThresholdMs,
             <<"The drift behind threshold can't be less than 100ms">>};
        {ok, X} ->
            {ok, drift_behind_threshold_ms, X}
    end.

parse_validate_ram_quota(Params, BucketConfig) ->
    do_parse_validate_ram_quota(proplists:get_value("ramQuotaMB", Params),
                                BucketConfig).

do_parse_validate_ram_quota(undefined, BucketConfig) when BucketConfig =/= false ->
    {ok, ram_quota, ns_bucket:raw_ram_quota(BucketConfig)};
do_parse_validate_ram_quota(Value, _BucketConfig) ->
    case menelaus_util:parse_validate_number(Value, 0, undefined) of
        invalid ->
            {error, ramQuotaMB,
             <<"The RAM Quota must be specified and must be a positive integer.">>};
        too_small ->
            {error, ramQuotaMB, <<"The RAM Quota cannot be negative.">>};
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
parse_validate_conflict_resolution_type(_Other) ->
    {error, conflictResolutionType,
     <<"Conflict resolution type must be 'seqno' or 'lww'">>}.

extended_cluster_storage_info() ->
    [{nodesCount, length(ns_cluster_membership:service_active_nodes(kv))}
     | ns_storage_conf:cluster_storage_info()].


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
    Nodes = ns_bucket:bucket_view_nodes(BucketName),
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
            case cluster_compat_mode:is_cluster_65() of
                true ->
                    Node = menelaus_util:choose_node_consistently(Req, Nodes),
                    case rpc:call(Node, M, F, A) of
                        {badrpc, _} = Error -> {error, Error};
                        Docs -> {ok, Docs}
                    end;
                false -> {error, nonodes}
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

handle_local_random_key(_PoolId, Bucket, Req) ->
    Nodes = ns_cluster_membership:service_active_nodes(kv),
    {ok, Res} = run_on_node({ns_memcached, get_random_key, [Bucket]},
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

convert_info_level(streaming) ->
    {normal, stable};
convert_info_level(InfoLevel) ->
    {InfoLevel, unstable}.

build_terse_bucket_info(BucketName) ->
    case bucket_info_cache:terse_bucket_info(BucketName) of
        {ok, _, V} -> V;
        %% NOTE: {auth_bucket for this route handles 404 for us albeit
        %% harmlessly racefully
        {T, E, Stack} ->
            erlang:raise(T, E, Stack)
    end.

serve_short_bucket_info(BucketName, Req) ->
    V = build_terse_bucket_info(BucketName),
    menelaus_util:reply_ok(Req, "application/json", V).

serve_streaming_short_bucket_info(BucketName, Req) ->
    handle_streaming(
      fun (_, _UpdateID) ->
              V = build_terse_bucket_info(BucketName),
              {just_write, {write, V}}
      end, Req).


-ifdef(TEST).
%% for test
basic_bucket_params_screening(IsNew, Name, Params, AllBuckets) ->
    Version = cluster_compat_mode:supported_compat_version(),
    Ctx = init_bucket_validation_context(IsNew, Name, AllBuckets, undefined,
                                         false, false,
                                         Version, true),
    basic_bucket_params_screening(Ctx, Params).

basic_bucket_params_screening_test() ->
    AllBuckets = [{"mcd",
                   [{type, memcached},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, []},
                    {ram_quota, 76 * ?MIB},
                    {auth_type, none},
                    {moxi_port, 33333}]},
                  {"default",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, []},
                    {ram_quota, 512 * ?MIB},
                    {auth_type, sasl},
                    {sasl_password, ""}]},
                  {"third",
                   [{type, membase},
                    {num_vbuckets, 16},
                    {num_replicas, 1},
                    {servers, []},
                    {ram_quota, 768 * ?MIB},
                    {auth_type, sasl},
                    {sasl_password, "asdasd"}]}],
    %% it is possible to create bucket with ok params
    {OK1, E1} = basic_bucket_params_screening(true, "mcd",
                                              [{"bucketType", "membase"},
                                               {"authType", "sasl"}, {"saslPassword", ""},
                                               {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
                                              tl(AllBuckets)),
    [] = E1,
    %% missing fields have their defaults set
    true = proplists:is_defined(num_threads, OK1),
    true = proplists:is_defined(eviction_policy, OK1),
    true = proplists:is_defined(replica_index, OK1),

    %% it is not possible to create bucket with duplicate name
    {_OK2, E2} = basic_bucket_params_screening(true, "mcd",
                                               [{"bucketType", "membase"},
                                                {"authType", "sasl"}, {"saslPassword", ""},
                                                {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    true = lists:member(name, proplists:get_keys(E2)), % mcd is already present

    %% it is not possible to update missing bucket. And specific format of errors
    {OK3, E3} = basic_bucket_params_screening(false, "missing",
                                              [{"bucketType", "membase"},
                                               {"authType", "sasl"}, {"saslPassword", ""},
                                               {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
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
                                              [{"authType", "some"}],
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
                                                {"authType", "sasl"}, {"saslPassword", ""},
                                                {"ramQuotaMB", "1024"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    ?assertEqual(true, lists:member(ramQuotaMB, proplists:get_keys(E7))),

    {_OK8, E8} = basic_bucket_params_screening(true, undefined,
                                               [{"bucketType", "membase"},
                                                {"authType", "sasl"}, {"saslPassword", ""},
                                                {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    ?assertEqual([{name, <<"Bucket name needs to be specified">>}], E8),

    {_OK9, E9} = basic_bucket_params_screening(false, undefined,
                                               [{"bucketType", "membase"},
                                                {"authType", "sasl"}, {"saslPassword", ""},
                                                {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
                                               AllBuckets),
    ?assertEqual([{name, <<"Bucket with given name doesn't exist">>}], E9),

    %% it is not possible to create bucket with duplicate name in different register
    {_OK10, E10} = basic_bucket_params_screening(true, "Mcd",
                                                 [{"bucketType", "membase"},
                                                  {"authType", "sasl"}, {"saslPassword", ""},
                                                  {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
                                                 AllBuckets),
    ?assertEqual([{name, <<"Bucket with given name already exists">>}], E10),

    %% it is not possible to create bucket with name longer than 100 characters
    {_OK11, E11} = basic_bucket_params_screening(true, "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901",
                                                 [{"bucketType", "membase"},
                                                  {"authType", "sasl"}, {"saslPassword", ""},
                                                  {"ramQuotaMB", "400"}, {"replicaNumber", "2"}],
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

    ok.

basic_parse_validate_bucket_auto_compaction_settings_test() ->
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
    ok.
-endif.
