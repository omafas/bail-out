%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc implementation of pools REST API's

-module(menelaus_web_pools).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_pools/1,
         check_and_handle_pool_info/2,
         handle_pool_info_streaming/2,
         handle_pool_settings_post/1,
         handle_pool_settings_post_body/3,
         handle_terse_cluster_info/1,
         get_cluster_name/0,
         pool_settings_post_validators/2,
         handle_defragmented/2]).

%% for hibernate
-export([handle_pool_info_wait_wake/4]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3,
         local_addr/1,
         encode_json/1,
         reply_ok/4,
         bin_concat_path/1,
         bin_concat_path/2,
         format_server_time/1,
         handle_streaming/2,
         reply/2]).

%% The timeout value is based on indexer's claim that the computation can
%% take around 10 minutes. This could be subject to change.
-define(DEFRAGMENTED_UTILIZATION_TIMEOUT,
        ?get_timeout(get_defragmented_utilization, 1000 * 60 * 20)).

handle_pools(Req) ->
    %% TODO RBAC for the time being let's tell the UI that the user is admin
    %% later there will be an API to test the permissions

    Enterprise = cluster_compat_mode:is_enterprise(),
    AllowedServices =
        ns_cluster_membership:supported_services(Enterprise),
    RV1 = [{isAdminCreds, true},
           {isROAdminCreds, false},
           {isEnterprise, Enterprise},
           {prodName, list_to_binary(cluster_compat_mode:prod_name())},
           {configProfile, list_to_binary(config_profile:name())},
           {allowedServices, AllowedServices},
           {isDeveloperPreview, cluster_compat_mode:is_developer_preview()},
           {packageVariant,
            menelaus_web_cache:get_static_value(package_variant)}
          | get_content_for_provisioned_system()],
    RV = RV1 ++ menelaus_web_cache:get_static_value(versions),
    reply_json(Req, {RV}).

get_content_for_provisioned_system() ->
    {Pools, Settings, UUID} =
        case ns_config_auth:is_system_provisioned() of
            true ->
                UUID1 = menelaus_web:get_uuid(),
                Pools1 = [{[{name, <<"default">>},
                            {uri, <<"/pools/default?uuid=", UUID1/binary>>},
                            {streamingUri, <<"/poolsStreaming/default?uuid=", UUID1/binary>>}]}],
                Settings1 = {[{<<"maxParallelIndexers">>,
                               <<"/settings/maxParallelIndexers?uuid=", UUID1/binary>>},
                              {<<"viewUpdateDaemon">>,
                               <<"/settings/viewUpdateDaemon?uuid=", UUID1/binary>>}]},
                {Pools1, Settings1, UUID1};
            false ->
                {[], [], []}
        end,
    [{pools, Pools}, {settings, Settings}, {uuid, UUID}].

check_and_handle_pool_info(Id, Req) ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            handle_pool_info(Id, Req);
        _ ->
            reply_json(Req, <<"unknown pool">>, 404)
    end.

handle_pool_info(Id, Req) ->
    LocalAddr = local_addr(Req),
    Query = mochiweb_request:parse_qs(Req),
    WaitChangeS = proplists:get_value("waitChange", Query),
    PassedETag = proplists:get_value("etag", Query),
    case WaitChangeS of
        undefined ->
            reply_json(Req, pool_info(Id, Req, normal, unstable,
                                      LocalAddr, undefined));
        _ ->
            WaitChange = list_to_integer(WaitChangeS),
            menelaus_event:register_watcher(self()),
            erlang:send_after(WaitChange, self(), wait_expired),
            handle_pool_info_wait(Req, Id, LocalAddr, PassedETag, undefined)
    end.

pool_info(Id, Req, InfoLevel, Stability, LocalAddr, UpdateID) ->
    {Info} = build_pool_info(Id, Req, InfoLevel, Stability,
                             LocalAddr, UpdateID),
    Buckets = proplists:get_value(bucketNames, Info),
    FilteredBuckets = menelaus_auth:filter_accessible_buckets(
                        fun ({[{bucketName, B}, _UUID]}) ->
                                {[{bucket, binary_to_list(B)}, settings], read}
                        end,
                        Buckets, Req),
    {lists:keyreplace(bucketNames, 1, Info, {bucketNames, FilteredBuckets})}.

handle_pool_info_wait(Req, Id, LocalAddr, PassedETag, UpdateID) ->
    Info = pool_info(Id, Req, for_ui, stable, LocalAddr, UpdateID),
    ETag = integer_to_list(erlang:phash2(Info)),
    if
        ETag =:= PassedETag ->
            menelaus_util:hibernate(Req, ?MODULE, handle_pool_info_wait_wake,
                                    [Req, Id, LocalAddr, PassedETag]);
        true ->
            handle_pool_info_wait_tail(Req, Id, LocalAddr, ETag, UpdateID)
    end.

handle_pool_info_wait_wake(Req, Id, LocalAddr, PassedETag) ->
    receive
        wait_expired ->
            handle_pool_info_wait_tail(Req, Id, LocalAddr, PassedETag,
                                       undefined);
        {notify_watcher, ID} ->
            timer:sleep(200), %% delay a bit to catch more notifications
            LastID = menelaus_event:flush_watcher_notifications(ID),
            handle_pool_info_wait(Req, Id, LocalAddr, PassedETag, LastID);
        _ ->
            exit(normal)
    end.

handle_pool_info_wait_tail(Req, Id, LocalAddr, ETag, UpdateID) ->
    menelaus_event:unregister_watcher(self()),
    %% consume all notifications
    LastID = menelaus_event:flush_watcher_notifications(UpdateID),
    %% and reply
    {PList} = pool_info(Id, Req, for_ui, unstable, LocalAddr, LastID),
    Info = {[{etag, list_to_binary(ETag)} | PList]},
    reply_ok(Req, "application/json", encode_json(Info),
             menelaus_auth:maybe_refresh_token(Req)),
    %% this will cause some extra latency on ui perhaps,
    %% because browsers commonly assume we'll keepalive, but
    %% keeping memory usage low is imho more important
    exit(normal).

config_version_token() ->
    {chronicle_kv:get_revision(kv), ns_config:config_version_token()}.

build_pool_info(Id, Req, normal, Stability, LocalAddr, UpdateID) ->
    InfoLevel =
        case menelaus_auth:has_permission({[admin, internal], all}, Req) of
            true ->
                case menelaus_auth:is_internal(Req) of
                    true ->
                        internal;
                    false ->
                        admin
                end;
            false ->
                normal
        end,

    %% NOTE: we limit our caching here for "normal" info
    %% level. Explicitly excluding UI (which InfoLevel = for_ui). This
    %% is because caching doesn't take into account "buckets version"
    %% which is important to deliver asap to UI (i.e. without any
    %% caching "staleness"). Same situation is with tasks version
    menelaus_web_cache:lookup_or_compute_with_expiration(
      {pool_details, InfoLevel, Stability, LocalAddr},
      fun () ->
              %% NOTE: token needs to be taken before building pool info
              Vsn = {config_version_token(), nodes(), UpdateID},
              {do_build_pool_info(Id, InfoLevel, Stability, LocalAddr),
               1000, Vsn}
      end,
      fun (_Key, _Value, {ConfigVersionToken, Nodes, OldUpdateID}) ->
              ConfigVersionToken =/= config_version_token()
                  orelse Nodes =/= nodes()
                  orelse ((UpdateID =/= OldUpdateID) andalso
                          (UpdateID =/= undefined))
      end);
build_pool_info(Id, _Req, for_ui, Stability, LocalAddr, _UpdateID) ->
    do_build_pool_info(Id, for_ui, Stability, LocalAddr).

server_groups_uri_json(GroupsV) ->
    [{serverGroupsUri, <<"/pools/default/serverGroups?v=",
                         (list_to_binary(integer_to_list(GroupsV)))/binary>>} ||
        cluster_compat_mode:is_enterprise()].

do_build_pool_info(Id, InfoLevel, Stability, LocalAddr) ->
    Ctx = menelaus_web_node:get_context({ip, LocalAddr}, false, Stability),
    Config = menelaus_web_node:get_config(Ctx),
    Snapshot = menelaus_web_node:get_snapshot(Ctx),

    UUID = menelaus_web:get_uuid(),
    Nodes = menelaus_web_node:build_nodes_info(Ctx),

    TasksURI = bin_concat_path(["pools", Id, "tasks"],
                               [{"v", ns_doctor:get_tasks_version()}]),

    {ok, IndexesVersion0} = service_index:get_indexes_version(),
    IndexesVersion = list_to_binary(integer_to_list(IndexesVersion0)),

    GroupsV = erlang:phash2(ns_cluster_membership:server_groups()),
    TrustedCertsV = erlang:phash2(ns_server_cert:trusted_CAs(props)),
    TrustedCertsVBin = integer_to_binary(TrustedCertsV),

    PropList =
        [{name, list_to_binary(Id)},
         {nodes, Nodes},
         build_buckets_info(Id, UUID, Nodes, Snapshot),
         build_uri_with_validation(remoteClusters,
                                   "/pools/default/remoteClusters", UUID),
         build_alerts(UUID),
         build_controllers(UUID),
         build_rebalance_params(Id, UUID),
         {nodeStatusesUri, <<"/nodeStatuses">>},
         build_node_services_uri(),
         {maxBucketCount, ns_bucket:get_max_buckets()},
         {maxCollectionCount, collections:get_max_supported(num_collections)},
         {maxScopeCount, collections:get_max_supported(num_scopes)},
         {minReplicasCount, ns_bucket:get_min_replicas()},
         {autoCompactionSettings,
          menelaus_web_autocompaction:build_global_settings()},
         {tasks, {[{uri, TasksURI}]}},
         {counters, {ns_cluster:counters()}},
         {indexStatusURI, <<"/indexStatus?v=", IndexesVersion/binary>>},
         {trustedCAsURI, <<"/pools/default/trustedCAs?v=",
                           TrustedCertsVBin/binary>>},
         {clusterName, list_to_binary(get_cluster_name())},
         {clusterEncryptionLevel,
          misc:get_effective_cluster_encryption_level(Config)},
         build_rebalance_status(),
         build_check_permissions_uri(Id, Snapshot),
         menelaus_web_node:build_memory_quota_info(Config),
         build_ui_params(InfoLevel, Snapshot),
         build_internal_params(InfoLevel),
         build_unstable_params(Ctx),
         server_groups_uri_json(GroupsV)],
    {lists:flatten(PropList)}.

-spec build_rebalance_status() -> proplists:proplist().
build_rebalance_status() ->
    rebalance_details(ns_orchestrator:needs_rebalance_with_detail()).

-spec rebalance_details(map()) -> proplists:proplist().
rebalance_details(Details) ->
    case Details of
        #{services := [], buckets := []} ->
            [{balanced, true}];
        #{services := Services, buckets := []} ->
            [{balanced, false},
             {servicesNeedRebalance,
              format_service_rebalance_needed(Services)}];
        #{services := [], buckets := Buckets} ->
            [{balanced, false},
             {bucketsNeedRebalance, format_bucket_rebalance_needed(Buckets)}];
        #{services := Services, buckets := Buckets} ->
            [{balanced, false},
             {servicesNeedRebalance, format_service_rebalance_needed(Services)},
             {bucketsNeedRebalance, format_bucket_rebalance_needed(Buckets)}]
    end.

format_bucket_rebalance_needed(Descriptors) ->
    format_buckets_and_services(
      fun (Buckets) ->
              [{buckets, [list_to_binary(Bucket) || Bucket <- Buckets]}]
      end, Descriptors).

format_service_rebalance_needed(Descriptors) ->
    format_buckets_and_services(
      fun (Services) ->
              [{services, Services}]
      end, Descriptors).

format_buckets_and_services(Fun, Descriptors) ->
    Grouped = maps:to_list(maps:groups_from_list(
                             element(1, _), element(2, _), Descriptors)),
    lists:map(
      fun ({Descriptor, Things}) ->
              {[{code, Descriptor},
                {description,
                 ns_rebalancer:to_human_readable_reason(Descriptor)}]
               ++ Fun(Things)}
      end, Grouped).

build_rebalance_params(Id, UUID) ->
    RebalanceStatus = case rebalance:running() of
                          true ->
                              <<"running">>;
                          _ ->
                              <<"none">>
                      end,

    [{rebalanceStatus, RebalanceStatus},
     {rebalanceProgressUri,
      bin_concat_path(["pools", Id, "rebalanceProgress"])},
     {stopRebalanceUri, build_controller_uri("stopRebalance", UUID)}].

build_internal_params(internal) ->
    try
        case ns_audit_cfg:get_uid() of
            undefined ->
                [];
            AuditUID ->
                [{auditUid, list_to_binary(AuditUID)}]
        end
    catch Error:Type ->
            %% When memcached is starting up, ns_audit_cfg will also not yet be
            %% up, so rather than crashing menelaus_web_cache and causing
            %% potentially multiple 500 errors, we should just report the error
            ?log_error("Got ~p fetching audit uid: ~p", [Type, Error]),
            []
    end;
build_internal_params(_) ->
    [].

build_ui_params(for_ui, Snapshot) ->
    [{failoverWarnings, ns_bucket:failover_warnings(Snapshot)},
     {saslauthdEnabled, cluster_compat_mode:is_saslauthd_enabled()},
     {uiSessionTimeout,
      ns_config:read_key_fast(ui_session_timeout, undefined)}];
build_ui_params(_, _) ->
    [].

build_check_permissions_uri(Id, Snapshot) ->
    Params = [{"v",
               menelaus_web_rbac:check_permissions_url_version(Snapshot)}],
    {checkPermissionsURI, bin_concat_path(["pools", Id, "checkPermissions"],
                                          Params)}.

build_node_services_uri() ->
    {_Rev, _RevEpoch, _V, NodesExtHash} =
        bucket_info_cache:build_node_services(),
    {nodeServicesUri, <<"/pools/default/nodeServices?v=",
                        NodesExtHash/binary>>}.

build_unstable_params(Ctx) ->
    case menelaus_web_node:get_stability(Ctx) of
        stable ->
            [];
        unstable ->
            Config = menelaus_web_node:get_config(Ctx),
            Snapshot = menelaus_web_node:get_snapshot(Ctx),
            StorageInfo = ns_storage_conf:cluster_storage_info(
                            Config, Snapshot),
            [{storageTotals,
              {[{Key, {StoragePList}} || {Key, StoragePList} <- StorageInfo]}}]
    end.

build_buckets_info(Id, UUID, Nodes, Snapshot) ->
    Buckets = ns_bucket:uuids(Snapshot),
    BucketsVer =
        erlang:phash2(Buckets)
        bxor erlang:phash2(
               [{proplists:get_value(hostname, KV),
                 proplists:get_value(status, KV)} || {KV} <- Nodes]),
    [{buckets, {[{uri, bin_concat_path(["pools", Id, "buckets"],
                                       [{"v", BucketsVer},
                                        {"uuid", UUID}])},
                 {terseBucketsBase, <<"/pools/default/b/">>},
                 {terseStreamingBucketsBase, <<"/pools/default/bs/">>}]}},
     {bucketNames, [{[{bucketName, list_to_binary(BucketName)},
                      {uuid, BucketUUID}]}
                    || {BucketName, BucketUUID} <- Buckets]}].

build_controller(Name, UUID) ->
    build_controller(Name, atom_to_list(Name), UUID).

build_controller(Name, Endpoint, UUID) ->
    {Name, {[{uri, build_controller_uri(Endpoint, UUID)}]}}.

build_uri_with_validation(Name, Endpoint, UUID) ->
    {Name, {[{uri, build_uri_with_uuid(Endpoint, UUID)},
             {validateURI, build_validate_uri(Endpoint)}]}}.

build_controller_uri(Endpoint, UUID) ->
    build_uri_with_uuid(["/controller/", Endpoint], UUID).

build_uri_with_uuid(Endpoint, UUID) ->
    iolist_to_binary([Endpoint, "?uuid=", UUID]).

build_validate_uri(Endpoint) ->
    iolist_to_binary([Endpoint, "?just_validate=1"]).

build_controllers(UUID) ->
    {controllers,
     {[build_controller(addNode, "addNodeV2", UUID),
       build_controller(rebalance, UUID),
       build_controller(failOver, UUID),
       build_controller(startGracefulFailover, UUID),
       build_controller(reAddNode, UUID),
       build_controller(reFailOver, UUID),
       build_controller(ejectNode, UUID),
       build_controller(setRecoveryType, UUID),
       build_uri_with_validation(setAutoCompaction,
                                 "/controller/setAutoCompaction", UUID),
       {clusterLogsCollection,
        {[{startURI, build_controller_uri("startLogsCollection", UUID)},
          {cancelURI, build_controller_uri("cancelLogsCollection", UUID)}]}},
       %% TODO Why is this such a special case?
       {replication,
        {[{createURI, build_controller_uri("createReplication", UUID)},
          {validateURI, build_validate_uri("/controller/createReplication")}]
        }}]}}.

build_alerts(UUID) ->
    {Alerts, AlertsSilenceToken} = menelaus_web_alerts_srv:fetch_alerts(),
    [{alerts, [build_one_alert(Alert) || Alert <- Alerts]},
     {alertsSilenceURL,
      iolist_to_binary([build_controller_uri("resetAlerts", UUID), "&token=",
                        AlertsSilenceToken])}].

build_one_alert({_Key, Msg, Time, DisablePopUp}) ->
    Timestamp = misc:time_to_timestamp(Time),
    %% This check is for consistent reporting of time across the cluster
    StrTime = case cluster_compat_mode:is_cluster_morpheus() of
                  true ->
                      Datetime = calendar:now_to_universal_time(Timestamp),
                      misc:utc_to_iso8601(Datetime, local);
                  false ->
                      LocalTime = calendar:now_to_local_time(Timestamp),
                      format_server_time(LocalTime)
              end,
    {[{msg, Msg}, {serverTime, StrTime},
      {disableUIPopUp, DisablePopUp}]}.

handle_pool_info_streaming(Id, Req) ->
    LocalAddr = local_addr(Req),
    F = fun(Stability, UpdateID) ->
                pool_info(Id, Req, normal, Stability, LocalAddr, UpdateID)
        end,
    handle_streaming(F, Req).

get_cluster_name() ->
    ns_config:read_key_fast(cluster_name, "").

pool_settings_post_validators(Config, Snapshot) ->
    [validator:touch(clusterName, _),
     validate_memory_quota(Config, Snapshot, _)].

validate_memory_quota(Config, Snapshot, ValidatorState) ->
    QuotaFields =
        [{memory_quota:service_to_json_name(Service), Service} ||
            Service <- memory_quota:aware_services(Config)],
    ValidationResult =
        lists:foldl(
          fun ({Key, _}, Acc) ->
                  validator:integer(Key, Acc)
          end, ValidatorState, QuotaFields),

    Quotas = lists:filtermap(
               fun ({Key, Service}) ->
                       case validator:get_value(Key, ValidationResult) of
                           undefined ->
                               false;
                           ServiceQuota ->
                               {true, {Service, ServiceQuota}}
                       end
               end, QuotaFields),

    case Quotas of
        [] ->
            ValidationResult;
        _ ->
            do_validate_memory_quota(Config, Snapshot, Quotas, ValidationResult)
    end.

do_validate_memory_quota(Config, Snapshot, Quotas, ValidatorState) ->
    Nodes = ns_cluster_membership:nodes_wanted(Snapshot),
    case ns_doctor:get_memory_data(Nodes) of
        {ok, NodesMemData} ->
            NodeInfos =
                [{Node, ns_cluster_membership:node_services(Snapshot, Node),
                  MemoryData} || {Node, MemoryData} <- NodesMemData],

            case memory_quota:check_quotas(NodeInfos,
                                           Config,
                                           Snapshot,
                                           Quotas) of
                ok ->
                    validator:return_value(quotas, Quotas, ValidatorState);
                {error, Error} ->
                    {Key, Msg} = quota_error_msg(Error),
                    validator:return_error(Key, Msg, ValidatorState)
            end;
        {error, Error} ->
            {_Key, Msg} = quota_error_msg(Error),
            menelaus_util:web_exception(500, Msg)
    end.

quota_error_msg({total_quota_too_high, Node, TotalQuota, MaxAllowed}) ->
    Msg = io_lib:format("Total quota (~bMB) exceeds the maximum allowed quota (~bMB) on node ~p",
                        [TotalQuota, MaxAllowed, Node]),
    {'_', Msg};
quota_error_msg({timeout, Nodes}) ->
    Msg =
        io_lib:format("Did not receive response in time from nodes ~p",
                      [Nodes]),
    {'_', Msg};
quota_error_msg({service_quota_too_low, Service, Quota, MinAllowed}) ->
    Details = case Service of
                  kv ->
                      " (current total buckets quota, or at least 256MB)";
                  _ ->
                      ""
              end,

    ServiceStr = ns_cluster_membership:user_friendly_service_name(Service),
    Msg = io_lib:format("The ~s service quota (~bMB) "
                        "cannot be less than ~bMB~s.",
                        [ServiceStr, Quota, MinAllowed, Details]),
    {memory_quota:service_to_json_name(Service), Msg}.

handle_pool_settings_post(Req) ->
    do_handle_pool_settings_post_loop(Req, 10).

do_handle_pool_settings_post_loop(_, 0) ->
    erlang:error(exceeded_retries);
do_handle_pool_settings_post_loop(Req, RetriesLeft) ->
    try
        do_handle_pool_settings_post(Req)
    catch
        throw:retry_needed ->
            do_handle_pool_settings_post_loop(Req, RetriesLeft - 1)
    end.

do_handle_pool_settings_post(Req) ->
    Config = ns_config:get(),
    Snapshot = chronicle_compat:get_snapshot(
                 [ns_bucket:fetch_snapshot(all, _, [props]),
                  ns_cluster_membership:fetch_snapshot(_)],
                 #{ns_config => Config}),

    validator:handle(
      fun (Params) ->
          handle_pool_settings_post_body(Req, Config, Params),
          reply(Req, 200)
      end, Req, form,
      pool_settings_post_validators(Config, Snapshot) ++
      [validator:has_params(_),
       validator:unsupported(_)]).

handle_pool_settings_post_body(Req, Config, Values) ->
    QuotaSupplied =
        case lists:keyfind(quotas, 1, Values) of
            {_, Quotas} ->
                case memory_quota:set_quotas(Config, Quotas) of
                    ok ->
                        ok;
                    retry_needed ->
                        throw(retry_needed)
                end,
                true;
            false ->
                false
        end,

    NameSupplied =
        case lists:keyfind(clusterName, 1, Values) of
            {_, ClusterName} ->
                ok = ns_config:set(cluster_name, ClusterName),
                true;
            false ->
                false
        end,

    (QuotaSupplied or NameSupplied) andalso do_audit_cluster_settings(Req).

do_audit_cluster_settings(Req) ->
    %% this is obviously raceful, but since it's just audit...
    Quotas = lists:map(
               fun (Service) ->
                       {ok, Quota} = memory_quota:get_quota(Service),
                       {Service, Quota}
               end, memory_quota:aware_services()),
    ClusterName = get_cluster_name(),
    ns_audit:cluster_settings(Req, Quotas, ClusterName).

handle_terse_cluster_info(Req) ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            Props = cluster_info_props(Req),
            validator:handle(
              fun (ReqdPropNames) ->
                      RV = get_terse_cluster_info(ReqdPropNames, Props),
                      menelaus_util:reply_json(Req, RV, 200)
              end, Req, qs, [validator:boolean(all, _) |
                             [validator:boolean(K, _) || {K, _} <- Props]]);
        false ->
            reply_json(Req, <<"unknown pool">>, 404)
    end.

cluster_info_props(Req) ->
    [{clusterUUID, fun menelaus_web:get_uuid/0},
     {autoFailover,
      fun (Cfg) ->
              AFCfg = [{K, V} || {K, V} <- auto_failover:get_cfg(Cfg),
                                 lists:member(K, [enabled, timeout])],
              {AFCfg}
      end},
     {autoReprovision, fun auto_reprovision:jsonify_cfg/0},
     {orchestrator,
      fun () ->
              case leader_registry:whereis_name(ns_orchestrator) of
                  undefined -> undefined;
                  RV -> node(RV)
              end
      end},
     {master, fun mb_master:master_node/0},
     {isBalanced, fun ns_cluster_membership:is_balanced/0},
     {quotaInfo,
      fun (Cfg) ->
              QuotaInfo = menelaus_web_node:build_memory_quota_info(Cfg),
              {QuotaInfo}
      end},
     {clusterCompatVersion,
      fun (_Cfg) ->
              [V1 | V2] = lists:map(
                            integer_to_list(_),
                            cluster_compat_mode:get_compat_version()),
              list_to_binary(V1 ++ "." ++ V2)
      end},
     {clientCertAuthState,
      fun (Cfg) ->
              CCAState = ns_ssl_services_setup:client_cert_auth_state(Cfg),
              list_to_binary(CCAState)
      end},
     {buckets, fun extract_bucket_specific_data/0},
     {nodes,
      fun () ->
              [begin
                   {Props} =
                       menelaus_web_node:build_full_node_info(Req, N),
                   glean_node_details(Props)
               end || N <- ns_node_disco:nodes_wanted()]
      end}].

get_terse_cluster_info([], Props) ->
    get_terse_cluster_info([clusterUUID,
                            orchestrator,
                            isBalanced,
                            clusterCompatVersion], Props);
get_terse_cluster_info(ReqdPropNames, Props) ->
    ReqdPropNames1 = case proplists:get_bool(all, ReqdPropNames) of
                         true -> [K || {K, _Fun} <- Props];
                         false -> ReqdPropNames
                     end,
    Config = ns_config:get(),
    RV = lists:filtermap(
           fun ({Key, Fun}) ->
                   case proplists:get_bool(Key, ReqdPropNames1) of
                       true ->
                           Val = case Fun of
                                     _ when is_function(Fun, 0) -> Fun();
                                     _ when is_function(Fun, 1) -> Fun(Config)
                                 end,
                           {true, {Key, Val}};
                       false ->
                           false
                   end
           end, Props),
    {RV}.

extract_bucket_specific_data() ->
    BktsInfo = maps:to_list(maps:groups_from_list(
            fun({_, BCfg}) -> ns_bucket:display_type(BCfg) end,
            fun({BName, BCfg}) ->
                    CommonProps = [{ramQuota, ns_bucket:raw_ram_quota(BCfg)}],
                    case ns_bucket:bucket_type(BCfg) of
                        memcached ->
                            {BName, {CommonProps}};
                        _ ->
                            NumReplicas = ns_bucket:num_replicas(BCfg),
                            EvictP = ns_bucket:eviction_policy(BCfg),
                            Props = [{numReplicas, NumReplicas},
                                     {evictionPolicy, EvictP}],
                            {BName, {Props ++ CommonProps}}
                    end
            end, ns_bucket:get_buckets())),

        {[{DT, {AllBProps}} || {DT, AllBProps} <- BktsInfo]}.

glean_node_details(NodeInfo) ->
    {SysStats} = proplists:get_value(systemStats, NodeInfo),
    SwapTotal = proplists:get_value(swap_total, SysStats),
    SwapUsed = proplists:get_value(swap_used, SysStats),

    {SC} = proplists:get_value(storage, NodeInfo),
    [{SConf0}] = proplists:get_value(hdd, SC),
    SConf =
        lists:foldl(
          fun({Name, Key}, Acc) ->
                  Val = proplists:get_value(Key, SConf0),
                  [{Name, Val} | Acc]
          end, [], [{cbasDirs, cbas_dirs},
                    {indexPath, index_path},
                    {dbPath, path}]),

    KeysOfInterest = [hostname, version, os, uptime, cpuCount,
                      memoryTotal, memoryFree, clusterMembership,
                      status, services, nodeUUID],

    {misc:proplist_keyfilter(lists:member(_, KeysOfInterest), NodeInfo) ++
         [{swapTotal, SwapTotal},
          {swapUsed, SwapUsed},
          {storageConf, {SConf}}]}.

handle_defragmented(ServiceStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_config_profile_flag(enable_bucket_placer),

    SupportedServices =
        [atom_to_list(S) ||
            S <- ns_cluster_membership:cluster_supported_services()],

    ServiceStr =/= "kv" orelse
        menelaus_util:web_exception(
          400, "KV service is not supported by this API"),

    lists:member(ServiceStr, SupportedServices) orelse
        menelaus_util:web_exception(404, "Unsupported service"),

    Service = list_to_existing_atom(ServiceStr),
    case ns_cluster_membership:pick_service_node(direct, Service) of
        undefined ->
            menelaus_util:web_exception(404, "Service not available");
        Node ->
            case rpc:call(Node, service_api, get_defragmented_utilization,
                          [Service, ?DEFRAGMENTED_UTILIZATION_TIMEOUT],
                          ?DEFRAGMENTED_UTILIZATION_TIMEOUT) of
                {badrpc, Error} ->
                    menelaus_util:web_exception(
                      500, io_lib:format("Error contacting service node: ~p",
                                         [Error]));
                {error, Error} ->
                    menelaus_util:web_exception(500, Error);
                {ok, {[{<<"Info">>, Result}]}} ->
                    menelaus_util:reply_json(Req, Result)
            end
    end.

-ifdef(TEST).
rebalance_details_test() ->
    ?assertEqual([{balanced, true}],
                 rebalance_details(#{services => [], buckets => []})),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {servicesNeedRebalance,
           [{[{code, service_not_balanced},
              {description, <<"Service needs rebalance.">>},
              {services, [kv]}]}]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [{service_not_balanced, kv}],
                             buckets => []}))),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {bucketsNeedRebalance,
           [{[{code, num_replicas_changed},
              {description,
               <<"Number of replicas for bucket has changed.">>},
              {buckets, [<<"default">>]}]}]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [],
                             buckets => [{num_replicas_changed,
                                          "default"}]}))),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {servicesNeedRebalance,
           [{[{code, service_not_balanced},
              {description, <<"Service needs rebalance.">>},
              {services, [kv, cbas]}]}]},
          {bucketsNeedRebalance,
           [{[{code, num_replicas_changed},
              {description,
               <<"Number of replicas for bucket has changed.">>},
              {buckets, [<<"default">>]}]}]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [{service_not_balanced, kv},
                                          {service_not_balanced, cbas}],
                             buckets => [{num_replicas_changed,
                                          "default"}]}))),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {servicesNeedRebalance,
           [{[{code, service_not_balanced},
              {description, <<"Service needs rebalance.">>},
              {services, [kv, cbas]}]}]},
          {bucketsNeedRebalance,
           [{[{code, num_replicas_changed},
              {description,
               <<"Number of replicas for bucket has changed.">>},
              {buckets, [<<"default">>, <<"another">>]}]}]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [{service_not_balanced, kv},
                                          {service_not_balanced, cbas}],
                             buckets => [{num_replicas_changed,
                                          "default"},
                                         {num_replicas_changed,
                                          "another"}]}))),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {servicesNeedRebalance,
           [{[{code, service_not_balanced},
              {description, <<"Service needs rebalance.">>},
              {services, [kv, cbas]}]},
            {[{code, servers_not_balanced},
              {description,
               <<"Servers of bucket are not balanced.">>},
              {services, [n1ql]}]}]},
          {bucketsNeedRebalance,
           [{[{code, num_replicas_changed},
              {description,
               <<"Number of replicas for bucket has changed.">>},
              {buckets, [<<"default">>, <<"another">>]}]}]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [{service_not_balanced, kv},
                                          {service_not_balanced, cbas},
                                          {servers_not_balanced, n1ql}],
                             buckets => [{num_replicas_changed,
                                          "default"},
                                         {num_replicas_changed,
                                          "another"}]}))),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {servicesNeedRebalance,
           [{[{code, service_not_balanced},
              {description, <<"Service needs rebalance.">>},
              {services, [kv, cbas]}]},
            {[{code, servers_not_balanced},
              {description,
               <<"Servers of bucket are not balanced.">>},
              {services, [n1ql]}]}]},
          {bucketsNeedRebalance,
           [
            {[{code, num_replicas_changed},
              {description,
               <<"Number of replicas for bucket has changed.">>},
              {buckets, [<<"default">>, <<"another">>]}]},
            {[{code, map_needs_rebalance},
              {description, <<"Bucket map needs rebalance.">>},
              {buckets, [<<"another">>]}]}
           ]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [{service_not_balanced, kv},
                                          {service_not_balanced, cbas},
                                          {servers_not_balanced, n1ql}],
                             buckets => [{num_replicas_changed,
                                          "default"},
                                         {num_replicas_changed,
                                          "another"},
                                         {map_needs_rebalance,
                                          "another"}]}))),
    ?assertEqual(
       misc:sort_kv_list(
         [{balanced, false},
          {servicesNeedRebalance,
           [{[{code, service_not_balanced},
              {description, <<"Service needs rebalance.">>},
              {services, [kv, cbas]}]},
            {[{code, servers_not_balanced},
              {description,
               <<"Servers of bucket are not balanced.">>},
              {services, [n1ql]}]}]},
          {bucketsNeedRebalance,
           [{[{code, num_replicas_changed},
              {description,
               <<"Number of replicas for bucket has changed.">>},
              {buckets, [<<"default">>, <<"another">>]}]},
            {[{code, map_needs_rebalance},
              {description, <<"Bucket map needs rebalance.">>},
              {buckets, [<<"another">>]}]},
            {[{code, servers_changed},
              {description, <<"Servers of bucket have changed.">>},
              {buckets, [<<"another_again">>]}]}]}]),
       misc:sort_kv_list(
         rebalance_details(#{services => [{service_not_balanced, kv},
                                          {service_not_balanced, cbas},
                                          {servers_not_balanced, n1ql}],
                             buckets => [{num_replicas_changed,
                                          "default"},
                                         {num_replicas_changed,
                                          "another"},
                                         {map_needs_rebalance,
                                          "another"},
                                         {servers_changed,
                                          "another_again"}]}))).

timeout_on_pools_default_post_test() ->
    meck:new(ns_cluster_membership, [passthrough]),
    meck:new(ns_doctor, [passthrough]),
    meck:new(validator, [passthrough]),

    meck:expect(ns_cluster_membership, nodes_wanted, fun(_) -> ok end),
    meck:expect(ns_doctor,
                get_memory_data,
                fun(_) ->
                        {error, {timeout, "1234"}}
                end),
    meck:expect(validator, return_error, fun(_,_,_) -> ok end),

    Msg =
        io_lib:format(
          "Did not receive response in time from nodes ~p", ["1234"]),
    ?assertThrow(
       {web_exception, 500, Msg, []},
       do_validate_memory_quota([],[], [],[])),

    meck:unload(ns_cluster_membership),
    meck:unload(ns_doctor),
    meck:unload(validator).

-endif.
