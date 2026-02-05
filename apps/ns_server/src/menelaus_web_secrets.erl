%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc rest api's for cluster secrets

-module(menelaus_web_secrets).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("cb_cluster_secrets.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(IS_WRITABLE_TIMEOUT, ?get_timeout(is_writable, 60000)).

% We should not allow unbounded timeout values to flow to go code as go
% time.duration type is in ns unit ticks of int64_t and larger value of
% timeouts in ms when converted to duration can overflow the type easily. Right
% now we cap the go code internally to error out if a timeout of larger than
% 5 minutes is specified and this define matches that exactly
-define(MAX_KMS_GO_TIMEOUT_MS, (5 * 60 * 1000)).

-export([handle_get_secrets/1,
         handle_get_secret/2,
         handle_post_secret/1,
         handle_put_secret/2,
         handle_test_post_secret/1,
         handle_test_post_secret/2,
         handle_test_put_secret/2,
         handle_delete_secret/2,
         handle_delete_historical_key/3,
         handle_rotate/2,
         format_error/1,
         format_secret_props/1]).

%% Can be called by other nodes
-export([is_writable_remote/4]).

handle_get_secrets(Req) ->
    cb_cluster_secrets:maybe_renew_secrets_usage_info(),
    Snapshot = chronicle_compat:get_snapshot(
                 [cb_cluster_secrets:fetch_snapshot_in_txn(_)], #{}),
    All = cb_cluster_secrets:get_all(Snapshot),
    FilteredSecrets = read_filter_secrets_by_permission(All, Req),
    TestResults = get_test_results_aggregated(FilteredSecrets),
    Res = lists:map(
            fun (#{id := Id} = Props) ->
                UsedBy = cb_cluster_secrets:where_is_secret_used(Id, Snapshot),
                TestRes = maps:get(Id, TestResults),
                {export_secret(Props#{used_by => UsedBy,
                                      test_results => TestRes})}
            end, FilteredSecrets),
    menelaus_util:reply_json(Req, Res).

handle_get_secret(IdStr, Req) when is_list(IdStr) ->
    menelaus_util:assert_is_enterprise(),
    cb_cluster_secrets:maybe_renew_secrets_usage_info(),
    Snapshot = chronicle_compat:get_snapshot(
                 [cb_cluster_secrets:fetch_snapshot_in_txn(_)], #{}),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:get_secret(Id, Snapshot) of
        {ok, Props} ->
            case read_filter_secrets_by_permission([Props], Req) of
                [] -> menelaus_util:reply_not_found(Req);
                [_] ->
                    UsedBy = cb_cluster_secrets:where_is_secret_used(
                              Id, Snapshot),
                    #{Id := TestRes} = get_test_results_aggregated([Props]),
                    Res = {export_secret(Props#{used_by => UsedBy,
                                                test_results => TestRes})},
                    menelaus_util:reply_json(Req, Res)
            end;
        {error, not_found} ->
            menelaus_util:reply_not_found(Req)
    end.

handle_post_secret(Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    with_validated_secret(
      fun (ToAdd, _, _) ->
          maybe
              {ok, Res} ?= cb_cluster_secrets:add_new_secret(ToAdd),
              Formatted = export_secret(Res),
              ns_audit:set_encryption_secret(Req, Formatted),
              menelaus_util:reply_json(Req, {Formatted}),
              ok
          end
      end, undefined, true, Req).

handle_put_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    Id = parse_id(IdStr),
    with_validated_secret(
      fun (Props, CurProps, Snapshot) ->
          maybe
              %% If the secret is already in use, we need to test it before
              %% saving it (because we test key when we are assigning it for
              %% encryption). Strictly speaking, the fact that it works now
              %% doesn't mean that it will work in the future, but it's better
              %% than nothing, and hopefully it will help avoiding most of the
              %% issues. Not doing it inside a save transaction because
              %% (1) testing is very slow and (2) it will still give us no
              %% guarantee that it will work in the future.
              ok ?= case cb_cluster_secrets:is_secret_used(Id, Snapshot) of
                        true -> cb_cluster_secrets:test(Props, CurProps);
                        false -> ok
                    end,
              IsSecretWritableMFA = {?MODULE, is_writable_remote,
                                      [?HIDE(Req), node()]},
              %% replace_secret will check "old usages" inside txn
              {ok, Res} ?= cb_cluster_secrets:replace_secret(
                              Id, Props, IsSecretWritableMFA),
              Formatted = export_secret(Res),
              ns_audit:set_encryption_secret(Req, Formatted),
              menelaus_util:reply_json(Req, {Formatted}),
              ok
          end
      end, Id, true, Req).

handle_test_post_secret(Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    with_validated_secret(
      fun (Params, _, _) ->
          maybe
              ok ?= cb_cluster_secrets:test(Params, undefined),
              menelaus_util:reply(Req, 200),
              ok
          end
      end, undefined, false, Req).

handle_test_post_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:get_secret(Id) of
        {ok, CurProps} ->
          maybe
              true ?= is_writable(CurProps, Req),
              Nodes = ns_node_disco:nodes_wanted(),
              ok ?= cb_cluster_secrets:test_existing_secret_props(CurProps,
                                                                  Nodes),
              menelaus_util:reply(Req, 200),
              ok
          else
              false ->
                  menelaus_util:web_exception(403, format_error(forbidden));
              {error, forbidden} ->
                  menelaus_util:web_exception(403, format_error(forbidden));
              {error, Reason} ->
                  menelaus_util:reply_global_error(Req, format_error(Reason))
          end;
        {error, not_found} ->
            menelaus_util:reply_not_found(Req)
    end.

handle_test_put_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    Id = parse_id(IdStr),
    with_validated_secret(
        fun (Params, CurProps, _) ->
            maybe
                ok ?= cb_cluster_secrets:test(Params, CurProps),
                menelaus_util:reply(Req, 200),
                ok
            end
        end, Id, false, Req).

with_validated_secret(Fun, ExistingId, NeedQuorum, Req) ->
    %% We need to fetch snapshot with read_consistency in order to deal with
    %% the following scenario:
    %% 1. User creates a bucket on a node which is not an orchestrator
    %%    (real creation happens on orchestrator)
    %% 2. Then the user tries to create a secret with bucket-encryption
    %%    usage for this bucket (also on non-orchestrator node)
    %% 3. This can fail because that node doesn't know about the bucket yet
    %%    (parsing of the bucket encryption usage will fail).

    %% At the same time we can't ask for quorum during test operations,
    %% because it will break the test call in case of no quorum
    %% (for non-test operations we don't care about the error here because the
    %% call will fail with no quorum error anyway).
    Opts = case NeedQuorum of
               true -> #{read_consistency => quorum};
               false -> #{}
           end,
    Snapshot = try
                   chronicle_compat:get_snapshot(
                     [cb_cluster_secrets:fetch_snapshot_in_txn(_)], Opts)
               catch
                   exit:timeout ->
                       menelaus_util:web_exception(503,
                                                   format_error(no_quorum))
               end,
    CurPropsRes = case ExistingId of
                      undefined -> {ok, #{}};
                      _ -> cb_cluster_secrets:get_secret(ExistingId, Snapshot)
                  end,

    case CurPropsRes of
        {ok, CurProps} ->
            validator:handle(
              fun (RawProps) ->
                  maybe
                      Props = import_secret(RawProps),
                      %% Note: All "usages" should be writable by current user.
                      %% This includes "new usages" (usages that are being set)
                      %% and "old usages" (usages that are being replaced)
                      %% Checking "new usages" here:
                      true ?= is_writable(Props, Req),
                      %% Fun is responsible for checking "old usages" inside txn
                      ok ?= Fun(Props, CurProps, Snapshot)
                  else
                      false ->
                          menelaus_util:web_exception(403,
                                                      format_error(forbidden));
                      {error, forbidden} ->
                          menelaus_util:web_exception(403,
                                                      format_error(forbidden));
                      {error, no_quorum} ->
                          menelaus_util:web_exception(503,
                                                      format_error(no_quorum));
                      {error, Reason} ->
                          menelaus_util:reply_global_error(Req,
                                                           format_error(Reason))
                  end
              end, Req, json, secret_validators(CurProps, Snapshot));
        {error, not_found} ->
            menelaus_util:reply_not_found(Req)
    end.

%% Note: CurProps can only be used for static fields validation here.
%% Any field that can be modified and needs to use CurProps should be
%% checked in transaction in cb_cluster_secret:replace_secret_internal.
secret_validators(CurProps, Snapshot) ->
    [validator:trimmed_string(name, _),
     validator:required(name, _),
     validator:validate(
       fun ("") -> {error, "Must not be empty"};
           (Str) ->
               Id = maps:get(id, CurProps, ?SECRET_ID_NOT_SET),
               case cb_cluster_secrets:is_name_unique(Id, Str, Snapshot) of
                   true -> ok;
                   %% Checking it here and inside transaction later
                   %% Check here is needed mostly to make it user friendly in UI
                   false -> {error, "Must be unique"}
               end
       end, name, _),
     validator:one_of(type, [?CB_MANAGED_KEY_TYPE, ?AWSKMS_KEY_TYPE,
                             ?GCPKMS_KEY_TYPE, ?AZUREKMS_KEY_TYPE,
                             ?HASHIKMS_KEY_TYPE, ?KMIP_KEY_TYPE], _),
     validator:convert(type, binary_to_atom(_, latin1), _),
     validator:required(type, _),
     validate_key_usage(usage, Snapshot, _),
     validator:required(usage, _),
     validate_secrets_data(data, CurProps, Snapshot, _),
     validator:required(data, _),
     validator:unsupported(_)].

enforce_static_field_validator(Name, CurValue, State) ->
    validator:validate(fun (NewValue) when NewValue == CurValue -> ok;
                            (_) -> {error, "The field can't be changed"}
                       end, Name, State).

handle_delete_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    Id = parse_id(IdStr),
    IsSecretWritableMFA = {?MODULE, is_writable_remote, [?HIDE(Req), node()]},
    case cb_cluster_secrets:delete_secret(Id, IsSecretWritableMFA) of
        {ok, Name} ->
            ns_audit:delete_encryption_secret(Req, Id, Name),
            menelaus_util:reply(Req, 200);
        {error, forbidden} ->
            menelaus_util:web_exception(403, format_error(forbidden));
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, no_quorum} ->
            menelaus_util:web_exception(503, format_error(no_quorum));
        {error, Reason} ->
            menelaus_util:reply_global_error(Req, format_error(Reason))
    end.

handle_delete_historical_key(IdStr, HistKeyIdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    Id = parse_id(IdStr),
    HistKeyId = list_to_binary(HistKeyIdStr),
    IsSecretWritableMFA = {?MODULE, is_writable_remote, [?HIDE(Req), node()]},
    case cb_cluster_secrets:delete_historical_key(Id,
                                                  HistKeyId,
                                                  IsSecretWritableMFA) of
        {ok, Name} ->
            ns_audit:delete_historical_encryption_key(Req, Id, Name, HistKeyId),
            menelaus_util:reply(Req, 200);
        {error, forbidden} ->
            menelaus_util:web_exception(403, format_error(forbidden));
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, no_quorum} ->
            menelaus_util:web_exception(503, format_error(no_quorum));
        {error, Reason} -> menelaus_util:reply_global_error(Req, format_error(Reason))
    end.

handle_rotate(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_79(),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:rotate(Id) of
        {ok, Name} ->
            ns_audit:rotate_encryption_secret(Req, Id, Name),
            menelaus_util:reply(Req, 200);
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, no_quorum} ->
            menelaus_util:web_exception(503, format_error(no_quorum));
        {error, not_supported} ->
            Msg = iolist_to_binary(format_error(not_supported)),
            menelaus_util:reply(Req, Msg, 501, []);
        {error, Reason} ->
            Msg = iolist_to_binary(format_error(Reason)),
            menelaus_util:reply(Req, Msg, 500, [])
    end.

keys_remap() ->
    #{creation_time => creationDateTime,
      rotation_interval_in_days => rotationIntervalInDays,
      next_rotation_time => nextRotationTime,
      last_rotation_time => lastRotationTime,
      auto_rotation => autoRotation,
      can_be_cached => canBeCached,
      key_arn => keyARN,
      key_url => keyURL,
      credentials_chain => credentialsChain,
      encryption_algorithm => encryptionAlgorithm,
      credentials_file => credentialsFile,
      config_file => configFile,
      use_imds => useIMDS,
      encrypt_with => encryptWith,
      encrypt_secret_id => encryptWithKeyId,
      stored_ids => storedKeyIds,
      key_path => keyPath,
      key_resource_id => keyResourceId,
      cert_path => certPath,
      key_passphrase => keyPassphrase,
      ca_selection => caSelection,
      encryption_approach => encryptionApproach,
      active_key => activeKey,
      hist_keys => historicalKeys,
      kmip_id => kmipId,
      key_material => keyMaterial,
      req_timeout_ms => reqTimeoutMs,
      used_by => usedBy,
      test_results => testResults}.

keys_to_json(Term) ->
    transform_keys(keys_remap(), Term).

keys_from_json(Term) ->
    transform_keys(#{V => K || K := V <- keys_remap()}, Term).

transform_keys(Map, Term) ->
    generic:transformt(fun ({K, V}) -> {maps:get(K, Map, K), V};
                           (#{} = M) ->
                               maps:from_list(
                                 lists:map(fun ({K, V}) ->
                                               {maps:get(K, Map, K), V}
                                           end, maps:to_list(M)));
                           (T) -> T
                       end, Term).

import_secret(Props) ->
    #{data := Data} = Map = maps:from_list(keys_from_json(Props)),
    Map#{data => maps:from_list(Data)}.

export_secret(#{type := DataType} = Props) ->
    keys_to_json(
      maps:to_list(
        maps:map(
          fun (id, Id) ->
                  Id;
              (name, Name) ->
                  iolist_to_binary(Name);
              (creation_time, DateTime) ->
                  format_datetime(DateTime);
              (type, T) ->
                  T;
              (usage, UList) ->
                  lists:filtermap(fun usage_to_json/1, UList);
              (data, D) when DataType == ?CB_MANAGED_KEY_TYPE ->
                  {format_cb_managed_key_data(D)};
              (data, D) when DataType == ?AWSKMS_KEY_TYPE ->
                  {format_aws_key_data(D)};
              (data, D) when DataType == ?GCPKMS_KEY_TYPE ->
                  {format_gcp_key_data(D)};
              (data, D) when DataType == ?AZUREKMS_KEY_TYPE ->
                  {format_azure_key_data(D)};
              (data, D) when DataType == ?HASHIKMS_KEY_TYPE ->
                  {format_hashi_key_data(D)};
              (data, D) when DataType == ?KMIP_KEY_TYPE ->
                  {format_kmip_key_data(D)};
              (used_by, UsedBy) ->
                  format_secrets_used_by_list_to_json(UsedBy);
              (test_results, TestResults) ->
                  format_test_results_to_json(TestResults)
          end, Props))).

format_test_results_to_json(#{status := Status,
                              datetime := UpdateDateTime,
                              missing_nodes := MissingNodes,
                              error_nodes := ErrorNodes,
                              success_nodes := SuccessNodes}) ->
    StatusJson = case Status of
                     ok -> <<"ok">>;
                     unknown -> <<"unknown">>;
                     {error, _} -> <<"error">>
                 end,
    OptionalDateTime =
        case UpdateDateTime of
            undefined -> [];
            _ -> [{<<"datetime">>, format_datetime(UpdateDateTime)}]
        end,
    BuildHostname = menelaus_web_node:build_node_hostname(
                      ns_config:latest(), _, misc:localhost()),
    Description =
        case Status of
            {error, Reason} ->
                iolist_to_binary(format_error(Reason));
            unknown when length(MissingNodes) > 0 ->
                <<"Missing test results for some nodes">>;
            unknown ->
                <<"No test results available yet">>;
            ok ->
                <<"Test passed">>
        end,
    {[{<<"status">>, StatusJson},
      {<<"description">>, Description},
      {<<"missingNodes">>, [BuildHostname(N) || N <- MissingNodes]},
      {<<"errorNodes">>, [BuildHostname(N) || N <- ErrorNodes]},
      {<<"successNodes">>, [BuildHostname(N) || N <- SuccessNodes]}] ++
     OptionalDateTime}.

format_secrets_used_by_list_to_json(UsedBy) ->
    Kinds = maps:get(by_config, UsedBy, []) ++ maps:get(by_deks, UsedBy, []),
    Secrets = maps:get(by_secrets, UsedBy, []),
    %% There should be no other fields in UsedBy
    0 = maps:size(maps:without([by_config, by_deks, by_secrets], UsedBy)),

    Kind2Usage =
        fun (K) ->
            {succ, U} = cb_deks:call_dek_callback(get_required_usage, K, []),
            U
        end,

    MakeRes = fun (U, D) -> {[{<<"usage">>, U}, {<<"description">>, D}]} end,

    FormatKind = fun (Kind) ->
                     Usage = Kind2Usage(Kind),
                     DescrBin = iolist_to_binary(usage_to_string(Usage)),
                     case usage_to_json(Usage) of
                         {true, UsageBin} ->
                             {true, MakeRes(UsageBin, DescrBin)};
                         false ->
                             false
                     end
                 end,

    FormatSecret = fun (S) ->
                       {true, UsageBin} = usage_to_json(secrets_encryption),
                       DescrBin = iolist_to_binary("key \"" ++ S ++ "\""),
                       MakeRes(UsageBin, DescrBin)
                   end,

    lists:filtermap(FormatKind, lists:uniq(Kinds)) ++
    lists:map(FormatSecret, lists:uniq(Secrets)).

usage_to_json({bucket_encryption, <<"*">>}) ->
    {true, <<"bucket-encryption">>};
usage_to_json({bucket_encryption, BucketUUID}) ->
    case ns_bucket:uuid2bucket(BucketUUID) of
        {ok, BucketName} ->
            {true, iolist_to_binary([<<"bucket-encryption-">>, BucketName])};
        {error, not_found} ->
            false
    end;
usage_to_json(config_encryption) ->
    {true, <<"config-encryption">>};
usage_to_json(secrets_encryption) ->
    {true, <<"KEK-encryption">>};
usage_to_json(audit_encryption) ->
    {true, <<"audit-encryption">>};
usage_to_json(log_encryption) ->
    {true, <<"log-encryption">>}.

usage_to_string(config_encryption) -> "configuration";
usage_to_string(log_encryption) -> "logs";
usage_to_string(audit_encryption) -> "audits";
usage_to_string(secrets_encryption) -> "encryption keys";
usage_to_string({bucket_encryption, <<"*">>}) -> "all buckets";
usage_to_string({bucket_encryption, BucketUUID}) ->
    case ns_bucket:uuid2bucket(BucketUUID) of
        {ok, BucketName} -> "bucket \"" ++ BucketName ++ "\"";
        {error, not_found} -> "unknown bucket"
    end.

format_cb_managed_key_data(Props) ->
    ActiveKeyId = maps:get(active_key_id, Props),
    maps:to_list(
      maps:map(
        fun (auto_rotation, B) ->
                B;
            (can_be_cached, B) ->
                B;
            (rotation_interval_in_days, Interval) ->
                Interval;
            (next_rotation_time, DateTime) ->
                format_datetime(DateTime);
            (last_rotation_time, DateTime) ->
                format_datetime(DateTime);
            (encrypt_with, E) ->
                E;
            (encrypt_secret_id, SId) ->
                SId;
            (keys, Keys) ->
                lists:map(
                  fun (KeyProps) ->
                      {format_key(KeyProps, ActiveKeyId)}
                  end, Keys)
        end, maps:remove(active_key_id, Props))).

format_aws_key_data(Props) ->
    maps:to_list(
      maps:map(
        fun (key_arn, U) -> iolist_to_binary(U);
            (region, R) -> iolist_to_binary(R);
            (credentials_file, F) -> iolist_to_binary(F);
            (config_file, F) -> iolist_to_binary(F);
            (profile, P) -> iolist_to_binary(P);
            (use_imds, U) -> U;
            (req_timeout_ms, R) -> R;
            (last_rotation_time, DT) -> format_datetime(DT);
            (stored_ids, StoredIds) ->
                [{[{id, Id}, {creation_time, format_datetime(CT)}]}
                 || #{id := Id, creation_time := CT} <- StoredIds]
        end, Props)).

format_gcp_key_data(Props) ->
    maps:to_list(
      maps:map(
        fun (key_resource_id, U) -> iolist_to_binary(U);
            (credentials_file, F) -> iolist_to_binary(F);
            (req_timeout_ms, R) -> R;
            (last_rotation_time, DT) -> format_datetime(DT);
            (stored_ids, StoredIds) ->
                [{[{id, Id}, {creation_time, format_datetime(CT)}]}
                 || #{id := Id, creation_time := CT} <- StoredIds]
        end, Props)).

format_azure_key_data(Props) ->
    maps:to_list(
      maps:map(
        fun (key_url, U) -> iolist_to_binary(U);
            (encryption_algorithm, Algorithm) -> iolist_to_binary(Algorithm);
            (req_timeout_ms, R) -> R;
            (last_rotation_time, DT) -> format_datetime(DT);
            (credentials_chain, C) -> iolist_to_binary(string:join(C, ","));
            (stored_ids, StoredIds) ->
                [{[{id, Id}, {creation_time, format_datetime(CT)}]}
                 || #{id := Id, creation_time := CT} <- StoredIds]
        end, Props)).

format_hashi_key_data(Props) ->
    maps:to_list(
      maps:map(
        fun (key_url, U) -> iolist_to_binary(U);
            (req_timeout_ms, R) -> R;
            (key_path, F) -> iolist_to_binary(F);
            (cert_path, F) -> iolist_to_binary(F);
            (key_passphrase, _) -> <<"******">>;
            (encrypt_with, E) -> E;
            (encrypt_secret_id, SId) -> SId;
            (ca_selection, use_sys_ca) -> <<"useSysCa">>;
            (ca_selection, use_cb_ca) -> <<"useCbCa">>;
            (ca_selection, use_sys_and_cb_ca) -> <<"useSysAndCbCa">>;
            (ca_selection, skip_server_cert_verification) ->
                <<"skipServerCertVerification">>;
            (last_rotation_time, DT) -> format_datetime(DT);
            (stored_ids, StoredIds) ->
                [{[{id, Id}, {creation_time, format_datetime(CT)}]}
                    || #{id := Id, creation_time := CT} <- StoredIds]
        end, Props)).

format_kmip_key_data(Props) ->
    maps:to_list(
      maps:map(
        fun (host, U) -> iolist_to_binary(U);
            (port, R) -> R;
            (req_timeout_ms, R) -> R;
            (key_path, F) -> iolist_to_binary(F);
            (cert_path, F) -> iolist_to_binary(F);
            (key_passphrase, _) -> <<"******">>;
            (active_key, K) -> format_kmip_key(K);
            (hist_keys, L) -> [format_kmip_key(K) || K <- L];
            (encrypt_with, E) -> E;
            (encrypt_secret_id, SId) -> SId;
            (ca_selection, use_sys_ca) -> <<"useSysCa">>;
            (ca_selection, use_cb_ca) -> <<"useCbCa">>;
            (ca_selection, use_sys_and_cb_ca) -> <<"useSysAndCbCa">>;
            (ca_selection, skip_server_cert_verification) ->
                <<"skipServerCertVerification">>;
            (encryption_approach, use_get) -> <<"useGet">>;
            (encryption_approach, use_encrypt_decrypt) ->
                <<"useEncryptDecrypt">>
        end, Props)).

format_kmip_key(#{id := Id, kmip_id := KmipId, creation_time := CT}) ->
    {[{id, Id}, {kmip_id, KmipId}, {creation_time, format_datetime(CT)}]}.

format_key(Props, ActiveKeyId) ->
    lists:flatmap(fun ({id, Id}) ->
                          [{id, Id}, {active, Id == ActiveKeyId}];
                      ({creation_time, DateTime}) ->
                          [{creation_time, format_datetime(DateTime)}];
                      ({active, Active}) ->
                          [{active, Active}];
                      ({key_material, #{data := _Binary}}) ->
                          [{key_material, <<"******">>}]
                  end, maps:to_list(Props)).

format_datetime(DateTime) ->
    misc:utc_to_iso8601(DateTime, local).

validate_key_usage(Name, Snapshot, State) ->
    validator:string_array(
      Name,
      fun (Str) ->
          case iolist_to_binary(Str) of
              <<"bucket-encryption">> ->
                  {value, {bucket_encryption, <<"*">>}};
              <<"bucket-encryption-", N/binary>> when size(N) > 0 ->
                  case ns_bucket:uuid(binary_to_list(N), Snapshot) of
                      not_present ->
                          {error, io_lib:format("Bucket ~s not found", [N])};
                      BucketUUID when is_binary(BucketUUID) ->
                          {value, {bucket_encryption, BucketUUID}}
                  end;
              <<"KEK-encryption">> ->
                  {value, secrets_encryption};
              <<"config-encryption">> ->
                  {value, config_encryption};
              <<"audit-encryption">> ->
                  {value, audit_encryption};
              <<"log-encryption">> ->
                  {value, log_encryption};
              _ ->
                  {error, "unknown usage"}
          end
      end, false, State).

%% Note: CurSecretProps can only be used for static fields validation here.
%% Any field that can be modified and needs to use CurProps should be
%% checked in transaction in cb_cluster_secret:replace_secret_internal.
validate_secrets_data(Name, CurSecretProps, Snapshot, State) ->
    Type = validator:get_value(type, State),
    CurType = maps:get(type, CurSecretProps, Type),
    case Type == CurType of
        true ->
            Validators =
                case Type of
                    ?CB_MANAGED_KEY_TYPE ->
                        cb_managed_key_validators(CurSecretProps, Snapshot);
                    ?AWSKMS_KEY_TYPE ->
                        awskms_key_validators(CurSecretProps);
                    ?GCPKMS_KEY_TYPE ->
                        gcpkms_key_validators(CurSecretProps);
                    ?AZUREKMS_KEY_TYPE ->
                        azurekms_key_validators(CurSecretProps);
                    ?HASHIKMS_KEY_TYPE ->
                        hashikms_key_validators(CurSecretProps, Snapshot);
                    ?KMIP_KEY_TYPE ->
                        kmip_key_validators(CurSecretProps, Snapshot);
                    _ -> []
                end,
            validator:decoded_json(
              Name,
              Validators ++ [validator:unsupported(_)],
              State);
        %% We can't call data validators because they assume
        %% the type of CurSecretProps
        false ->
            enforce_static_field_validator(type, CurType, State)
    end.

%% Note: CurSecretProps can only be used for static fields validation here.
%% Any field that can be modified and needs to use CurProps should be
%% checked in transaction in cb_cluster_secret:replace_secret_internal.
cb_managed_key_validators(CurSecretProps, Snapshot) ->
    [validator:boolean(canBeCached, _),
     validator:default(canBeCached, true, _),
     validator:boolean(autoRotation, _),
     validator:default(autoRotation, true, _),
     validator:range(rotationIntervalInDays, 1, max_uint64, _),
     validate_iso8601_datetime(nextRotationTime, _),
     validate_datetime_in_the_future(nextRotationTime, _),
     mandatory_rotation_fields(_),
     validator:validate(fun (_) -> {error, "read only"} end, keys, _),
     validator:one_of(encryptWith, ["nodeSecretManager", "encryptionKey"], _),
     validator:convert(encryptWith, binary_to_atom(_, latin1), _),
     validate_encrypt_with(encryptWith, Snapshot, _),
     validator:default(encryptWith, nodeSecretManager, _),
     validator:integer(encryptWithKeyId, -1, max_uint64, _),
     validate_encrypt_secret_id(encryptWithKeyId, CurSecretProps, _)].

validate_encrypt_with(Name, Snapshot, State) ->
    validator:validate(
      fun (encryptionKey) ->
              case validator:get_value(encryptWithKeyId, State) of
                  undefined ->
                      {error, "encryptWithKeyId must be set when "
                              "'encryptionKey' is used"};
                  _ -> ok
              end;
          (nodeSecretManager) ->
              case cb_crypto:get_encryption_method(config_encryption, cluster,
                                                   Snapshot) of
                  {ok, disabled} ->
                      {error, format_error(config_encryption_disabled)};
                  {ok, _} ->
                      ok
              end
      end, Name, State).

validate_encrypt_secret_id(Name, CurSecretProps, State) ->
    CurId = case CurSecretProps of
                #{id := Id} -> Id;
                #{} when map_size(CurSecretProps) == 0 -> undefined
            end,
    validator:validate_relative(
      fun (?SECRET_ID_NOT_SET, nodeSecretManager) ->
              ok;
          (_, nodeSecretManager) ->
              {error, "can't be set when encryptWith is nodeSecretManager"};
          (EId, encryptionKey) when EId == CurId, CurId =/= undefined ->
              {error, "key can't encrypt itself"};
          (_EId, encryptionKey) ->
              ok
      end, Name, encryptWith, State).

%% Note: CurSecretProps can only be used for static fields validation here.
%% Any field that can be modified and needs to use CurProps should be
%% checked in transaction in cb_cluster_secret:replace_secret_internal.
awskms_key_validators(CurSecretProps) ->
    [validator:string(keyARN, _),
     validate_awskm_arn(keyARN, _),
     validator:required(keyARN, _),
     validator:string(region, _),
     validator:default(region, "", _),
     validator:boolean(useIMDS, _),
     validator:default(useIMDS, false, _),
     validator:string(credentialsFile, _),
     validator:default(credentialsFile, "", _),
     validate_optional_file(credentialsFile, _),
     validator:string(configFile, _),
     validator:default(configFile, "", _),
     validate_optional_file(configFile, _),
     validator:integer(reqTimeoutMs, 1000, ?MAX_KMS_GO_TIMEOUT_MS, _),
     validator:default(reqTimeoutMs, 30000, _),
     validator:validate(fun (_) -> {error, "read only"} end, storedKeyIds, _),
     validator:string(profile, _),
     validator:default(profile, "", _)] ++
    case CurSecretProps of
        #{data := #{key_arn := KeyArn, region := Region}} ->
            [enforce_static_field_validator(keyARN, KeyArn, _),
             enforce_static_field_validator(region, Region, _)];
        #{} when map_size(CurSecretProps) == 0 ->
            []
    end.

validate_awskm_arn(Name, State) ->
    validator:validate(
      fun ("TEST_AWS_KEY_ARN") ->
              ok;
          ("TEST_AWS_BAD_KEY_ARN") ->
              ok;
          (Arn) ->
              case string:split(Arn, ":", all) of
                  ["arn", _Partition, "kms", _Region, _Acc, "key/" ++ _Id] ->
                      ok;
                  ["arn", _Partition, "kms", _Region, _Acc, "alias/" ++ _A] ->
                      ok;
                  _ ->
                      {error, "Invalid AWS Key ARN"}
              end
      end, Name, State).

validate_optional_file(Name, State) ->
    validator:validate(
      fun (Path) ->
          case string:trim(Path, both) of
              "" ->
                  {value, ""};
              F ->
                  case filelib:is_regular(F) of
                      true ->
                          ok;
                      false ->
                          {error, "The value must be a valid file"}
                  end
          end
      end, Name, State).

validate_azure_credentials_chain(State) ->
    Allowed = ["environment", "workloadIdentity", "managedIdentity", "azureCli",
               "azureDeveloperCli", "defaultCredentialsChain"],

    State1 =
        validator:token_list(credentialsChain, ",",
                             fun (Value) ->
                                     case lists:member(string:trim(Value),
                                                       Allowed) of
                                         true -> {value, string:trim(Value)};
                                         false -> {error, "Invalid value"}
                                     end
                             end, State),

    validator:validate(
        fun(["defaultCredentialsChain"]) ->
                ok;
            (ValueList) ->
                case {lists:member("defaultCredentialsChain", ValueList),
                      length(ValueList) =/= length(lists:usort(ValueList))} of
                    {true, _} ->
                        {error, "defaultCredentialsChain can not be mixed with"
                                 " other chain options"};
                    {_, true} ->
                        {error, "chain must not contain duplicates"};
                    {false, false} ->
                        ok
                end
        end, credentialsChain, State1).


gcpkms_key_validators(CurSecretProps) ->
    [validator:string(keyResourceId, _),
     validator:required(keyResourceId, _),
     validator:string(credentialsFile, _),
     validator:default(credentialsFile, "", _),
     validator:integer(reqTimeoutMs, 1000, ?MAX_KMS_GO_TIMEOUT_MS, _),
     validator:default(reqTimeoutMs, 30000, _),
     validator:validate(fun (_) -> {error, "read only"} end, storedKeyIds, _)
    ] ++
    case CurSecretProps of
        #{data := #{key_resource_id := KeyResourceId}} ->
            [enforce_static_field_validator(keyResourceId, KeyResourceId, _)];
        #{} when map_size(CurSecretProps) == 0 ->
            []
    end.

azurekms_key_validators(CurSecretProps) ->
    [validator:string(keyURL, _),
     validator:validate(fun validate_azure_key/1, keyURL, _),
     validator:required(keyURL, _),
     validator:one_of(encryptionAlgorithm, ["A128CBC", "A128CBCPAD", "A128GCM",
                                            "A128KW", "A192CBC", "A192CBCPAD",
                                            "A192GCM", "A192KW", "A256CBC",
                                            "A256CBCPAD", "A256GCM", "A256KW",
                                            "CKMAESKEYWRAP", "CKMAESKEYWRAPPAD",
                                            "RSA15", "RSAOAEP",
                                            "RSAOAEP256"], _),
     validator:required(encryptionAlgorithm, _),
     validator:required(credentialsChain, _),
     validator:non_empty_string(credentialsChain, _),
     fun validate_azure_credentials_chain/1,
     validator:integer(reqTimeoutMs, 1000, ?MAX_KMS_GO_TIMEOUT_MS, _),
     validator:default(reqTimeoutMs, 30000, _),
     validator:validate(fun (_) -> {error, "read only"} end, storedKeyIds, _)
    ] ++
    case CurSecretProps of
        #{data := #{key_url := KeyURL,
                    encryption_algorithm := Algorithm}} ->
            [enforce_static_field_validator(keyURL, KeyURL, _),
             enforce_static_field_validator(encryptionAlgorithm, Algorithm, _)];
        #{} when map_size(CurSecretProps) == 0 ->
            []
    end.

hashikms_key_validators(CurSecretProps, Snapshot) ->
    [validator:string(keyURL, _),
     validator:validate(fun validate_hashi_key/1, keyURL, _),
     validator:required(keyURL, _) ,
     validator:integer(reqTimeoutMs, 1000, ?MAX_KMS_GO_TIMEOUT_MS, _),
     validator:default(reqTimeoutMs, 30000, _),
     validator:string(keyPath, _),
     validator:required(keyPath, _),
     validator:string(certPath, _),
     validator:required(certPath, _),
     validator:non_empty_string(keyPassphrase, _),
     validator:convert(keyPassphrase, iolist_to_binary(_), _),
     validator:validate(fun (P) -> {value, ?HIDE(P)} end, keyPassphrase, _),
     validator:one_of(caSelection,["useSysCa",
                                   "useCbCa",
                                   "useSysAndCbCa",
                                   "skipServerCertVerification"], _),
     validator:convert(caSelection,
                      fun (<<"useSysCa">>) -> use_sys_ca;
                          (<<"useCbCa">>) -> use_cb_ca;
                          (<<"useSysAndCbCa">>) -> use_sys_and_cb_ca;
                          (<<"skipServerCertVerification">>) ->
                              skip_server_cert_verification
                      end, _),
     validator:default(caSelection, use_cb_ca, _),
     validator:one_of(encryptWith, ["nodeSecretManager", "encryptionKey"], _),
     validator:convert(encryptWith, binary_to_atom(_, latin1), _),
     validate_encrypt_with(encryptWith, Snapshot, _),
     validator:default(encryptWith, nodeSecretManager, _),
     validator:integer(encryptWithKeyId, -1, max_uint64, _),
     validate_encrypt_secret_id(encryptWithKeyId, CurSecretProps, _)] ++
     case CurSecretProps of
         #{data := #{key_url := KeyUrl}} ->
             [enforce_static_field_validator(keyURL, KeyUrl, _)];
         #{} when map_size(CurSecretProps) == 0 ->
             [validator:required(keyPassphrase, _)]
     end.

kmip_key_validators(CurSecretProps, Snapshot) ->
    [validator:string(host, _),
     validator:required(host, _),
     validator:integer(port, 1, 65535, _),
     validator:required(port, _),
     validator:integer(reqTimeoutMs, 1000, ?MAX_KMS_GO_TIMEOUT_MS, _),
     validator:default(reqTimeoutMs, 30000, _),
     validator:string(keyPath, _),
     validator:required(keyPath, _),
     validator:string(certPath, _),
     validator:required(certPath, _),
     validator:string(keyPassphrase, _),
     validator:convert(keyPassphrase, iolist_to_binary(_), _),
     validator:validate(fun (P) -> {value, ?HIDE(P)} end, keyPassphrase, _),
     validator:one_of(caSelection,["useSysCa",
                                   "useCbCa",
                                   "useSysAndCbCa",
                                   "skipServerCertVerification"], _),
     validator:convert(caSelection,
                      fun (<<"useSysCa">>) -> use_sys_ca;
                          (<<"useCbCa">>) -> use_cb_ca;
                          (<<"useSysAndCbCa">>) -> use_sys_and_cb_ca;
                          (<<"skipServerCertVerification">>) ->
                              skip_server_cert_verification
                      end, _),
     validator:default(caSelection, use_cb_ca, _),
     validator:one_of(encryptionApproach, ["useGet",
                                           "useEncryptDecrypt"], _),
     validator:convert(encryptionApproach,
                       fun (<<"useGet">>) -> use_get;
                           (<<"useEncryptDecrypt">>) -> use_encrypt_decrypt
                       end, _),
     validator:default(encryptionApproach, use_get, _),
     validator:decoded_json(activeKey,
                            [validator:string(kmipId, _),
                             validator:required(kmipId, _),
                             validator:convert(kmipId, iolist_to_binary(_), _),
                             validator:unsupported(_)], _),
     validator:required(activeKey, _),
     validator:validate(fun (P) -> {value, maps:from_list(P)} end,
                        activeKey, _),
     validator:validate(fun (_) -> {error, "read only"} end, historicalKeys, _),
     validator:one_of(encryptWith, ["nodeSecretManager", "encryptionKey"], _),
     validator:convert(encryptWith, binary_to_atom(_, latin1), _),
     validate_encrypt_with(encryptWith, Snapshot, _),
     validator:default(encryptWith, nodeSecretManager, _),
     validator:integer(encryptWithKeyId, -1, max_uint64, _),
     validate_encrypt_secret_id(encryptWithKeyId, CurSecretProps, _)] ++
        [validator:required(keyPassphrase, _) ||
            map_size(CurSecretProps) == 0].

mandatory_rotation_fields(State) ->
    case validator:get_value(autoRotation, State) of
        true ->
            functools:chain(State,
                            [validator:required(nextRotationTime, _),
                             validator:required(rotationIntervalInDays, _)]);
        false ->
            State
    end.

validate_hashi_key("TEST_HASHI_KEY_URL")  ->
    ok;
validate_hashi_key(KeyUrlStr) ->
    Scheme =
        fun (Url) when Url =:= <<"https">>; Url =:= <<"http">> ->
                valid;
            (_S) ->
                {error, "must be http or https url only"}
        end,

    ParseUrlFn =
        fun() ->
            case misc:parse_url(KeyUrlStr, [{scheme_validation_fun, Scheme},
                                            {return, string}]) of
                {ok, _} = Result ->
                    Result;
                {error, E} ->
                    {error, io_lib:format("failed to parse url: ~p", [E])}
            end
        end,

    ValidateHost =
        fun ([]) ->
                {error, "a host must be specified in the URL"};
            (Host) when is_list(Host) ->
                ok
        end,

    ValidateKeyPath =
        fun(<<"/", Rest/binary>>) when byte_size(Rest) > 0 ->
                ok;
            (_) ->
                {error, "path must have /<keyName>"}
        end,

    maybe
        {ok, #{host := Host, path := Path}} ?= ParseUrlFn(),

        ok ?= ValidateHost(Host),

        ok ?= ValidateKeyPath(list_to_binary(Path))
    end.

validate_azure_key("TEST_AZURE_KEY_URL") ->
    ok;
validate_azure_key(KeyUrlStr) ->
    %% The allowed domains are unlikely to change, but we allow them to be
    %% changed via /settings/security just in case a new domain needs to be
    %% supported in the future. For complete correctness, we should do this
    %% validation as a chronicle transactions once /settings/security values
    %% are moved from ns_config to chronicle.
    AllowedDomains = ns_config:search(ns_config:latest(), azure_allowed_domains,
                                      ?DEFAULT_AZURE_ALLOWED_DOMAINS),

    Scheme =
        fun (<<"https">>) -> valid;
            (_S) -> {error, "must be https url only"}
        end,

    ValidateDomain =
        fun (Domain) ->
            case lists:member(Domain, AllowedDomains) of
                true ->
                    ok;
                false ->
                    {error, io_lib:format("domain not allowed: ~p", [Domain])}
            end
         end,

    ValidateHost =
        fun ([]) ->
                {error, "no host found in azure key URL"};
            (Host) when is_list(Host)->
                case string:split(Host, ".", all) of
                    [VaultOrHsmName, _ | _] when VaultOrHsmName =:= [] ->
                        {error, "no vault or managedhsm name found in domain"};
                    [_VaultOrHsmName, H | T] ->
                        ValidateDomain(string:join([H | T], "."));
                    _ ->
                        {error,
                         "format of host must be <vaultOrHsmName>.<domain>"}
                end
        end,

    ValidateKeyPath =
        fun(<<"/keys/", Rest/binary>>) ->
                case binary:match(Rest, <<"/">>) of
                    nomatch ->
                        ok;
                    _Match ->
                        {error, "azure key must be a base identifier"}

                end;
            (_) ->
                {error, "path must have /keys"}
        end,

    ParseUrlFn =
        fun() ->
            case misc:parse_url(KeyUrlStr, [{scheme_validation_fun, Scheme},
                                            {return, string}]) of
                {ok, _} = Result ->
                    Result;
                {error, E} ->
                    {error, io_lib:format("failed to parse url: ~p", [E])}
            end
        end,

    maybe
        {ok, #{host := Host, path := Path}} ?= ParseUrlFn(),

        ok ?= ValidateHost(Host),

        ok ?= ValidateKeyPath(list_to_binary(Path))
    end.


validate_iso8601_datetime(Name, State) ->
    validator:validate(
      fun (S) ->
          try
              {value, iso8601:parse(S)}
          catch
              _:_ ->
                  {error, "invalid ISO 8601 time"}
          end
      end, Name, State).

validate_datetime_in_the_future(Name, State) ->
    validator:validate(
      fun (DT) ->
          case DT > calendar:universal_time() of
              true -> ok;
              false -> {error, "must be in the future"}
          end
      end, Name, State).

usage_extra_permissions({bucket_encryption, <<"*">>}, write, _Snapshot) ->
    %% Those who can create a bucket should be able to create a secret to
    %% encrypt that bucket
    [{[buckets], create}];
usage_extra_permissions({bucket_encryption, <<"*">>}, read, _Snapshot) ->
    %% Those who can view bucket list should be able to view the secrets
    %% that can encrypt buckets
    [{[{bucket, any}, settings], read}];

usage_extra_permissions({bucket_encryption, BucketUUID}, write, Snapshot) ->
    %% Those who can modify bucket settings should be able to create a secret
    %% that encrypts that specific bucket
    maybe
        {ok, B} ?= ns_bucket:uuid2bucket(BucketUUID, Snapshot),
        [{[{bucket, B}, settings], write}]
    else
        {error, not_found} ->
            []
    end;
usage_extra_permissions({bucket_encryption, BucketUUID}, read, Snapshot) ->
    %% Those who can read bucket settings should be able to see secrets that
    %% can encrypt that specific bucket
    maybe
        {ok, B} ?= ns_bucket:uuid2bucket(BucketUUID, Snapshot),
        [{[{bucket, B}, settings], read}]
    else
        {error, not_found} ->
            []
    end;

usage_extra_permissions(Usage, _PermType, _Snapshot)
                            when Usage =:= secrets_encryption;
                                 Usage =:= config_encryption;
                                 Usage =:= audit_encryption;
                                 Usage =:= log_encryption ->
    [].

%% These permissions give access to all secrets (all usages)
%% For some usages, there are also relaxed permissions (e.g. bucket admins
%% should be able to read/write encryption keys that are meant to encrypt their
%% buckets)
-define(usage_read_perm, {[admin, security], read}).
-define(usage_write_perm, {[admin, security], write}).

is_usage_allowed(Usage, PermType, Req, Snapshot) ->
    FullAccessPerm = case PermType of
                         read -> ?usage_read_perm;
                         write -> ?usage_write_perm
                     end,
    AllowedPerms = [FullAccessPerm | usage_extra_permissions(Usage, PermType,
                                                             Snapshot)],
    lists:any(menelaus_auth:has_permission(_, Req), AllowedPerms).

read_filter_secrets_by_permission(Secrets, Req) ->
    lists:filter(is_readable(_, Req), Secrets).

is_readable(#{usage := Usages}, Req) ->
    Snapshot = ns_bucket:get_snapshot(all, [uuid]),
    ExistingUsages = only_existing_usages(Usages, Snapshot),
    menelaus_auth:has_permission(?usage_read_perm, Req) orelse
       lists:any(is_usage_allowed(_, read, Req, Snapshot), ExistingUsages).

is_writable(Secret, Req) ->
    is_writable(Secret, Req, ns_bucket:get_snapshot(all, [uuid])).

is_writable(#{usage := Usages}, Req, Snapshot) ->
    ExistingUsages = only_existing_usages(Usages, Snapshot),
    menelaus_auth:has_permission(?usage_write_perm, Req) orelse
        (ExistingUsages =/= [] andalso
            lists:all(is_usage_allowed(_, write, Req, Snapshot), ExistingUsages)).

only_existing_usages(Usages, Snapshot) ->
    ExistingUUIDs = [U || {_, U} <- ns_bucket:uuids(Snapshot)],
    lists:filter(fun ({bucket_encryption, <<"*">>}) ->
                        true;
                     ({bucket_encryption, UUID}) ->
                        lists:member(UUID, ExistingUUIDs);
                     (_) ->
                        true
                end, Usages).

parse_id(Str) when is_list(Str) ->
    try list_to_integer(Str) of
        N -> N
    catch
        _:_ ->
            menelaus_util:web_exception(404, menelaus_util:reply_text_404())
    end.

-spec format_error(term()) -> iolist().
format_error({encrypt_id, not_found}) ->
    "Encryption key does not exist";
format_error({encrypt_id, not_allowed}) ->
    "Encryption key not allowed";
format_error({usage, in_use}) ->
    "Can't modify usage as this key is in use";
format_error(name_not_unique) ->
    "Name is not unique";
format_error(config_encryption_disabled) ->
    "Can't use master password for encryption because "
    "config encryption is disabled";
format_error({encrypt_key_error, Msg}) when is_list(Msg) ->
    lists:flatten(io_lib:format("Key encryption failed: ~s", [Msg]));
format_error({decrypt_key_error, Msg}) when is_list(Msg) ->
    lists:flatten(io_lib:format("Key decryption failed: ~s", [Msg]));
format_error({used_by, UsedByList}) ->
    Formatted = format_secrets_used_by_list(UsedByList),
    lists:flatten(io_lib:format("Can't be removed because ~s", [Formatted]));
format_error({cycle, _}) ->
    "Circular dependency between keys";
format_error(no_quorum) ->
    "Operation temporarily cannot be performed possibly due to loss of quorum";
format_error({store_key_error_test, Msg}) ->
    %% This error is returned when secret params test fails
    Msg;
format_error(active_key) ->
    "Can't delete active key";
format_error({unsafe, Reason}) ->
    "Operation is unsafe. " ++ format_error(Reason);
format_error({deks_sync_failed, Errors}) ->
    BuildHostname = menelaus_web_node:build_node_hostname(
                      ns_config:latest(), _, misc:localhost()),
    "Failed to synchronize data encryption keys on " ++
    lists:join(", ", [BuildHostname(Node) || {Node, _} <- Errors]);
format_error(deks_issues) ->
    "Some data encryption keys are outdated or not available";
format_error(missing_nodes) ->
    "Some nodes are missing";
format_error(node_errors) ->
    "Failed to get data encryption keys (DEKs) information from some nodes";
format_error(not_supported) ->
    "Operation not supported";
format_error(timeout) ->
    "Operation timed out";
format_error(no_connection_to_node) ->
    "No connection to node";
format_error({invalid_key_settings, Msg}) ->
    "Unable to perform encryption/decryption with provided key, "
    "check encryption key settings (" ++ Msg ++ ")";
format_error({failed_to_encrypt_or_decrypt_key, Msg}) ->
    "Unable to encrypt (or decrypt) this key. Please verify the configuration "
    "of the encryption key used to protect this key (" ++ Msg ++ ")";
format_error(decrypted_data_mismatch) ->
    "Decrypted data does not match original data that was encrypted";
format_error({test_failed_for_some_nodes, Errors}) ->
    %% Sorting just to show more relevant errors first
    %% no_connection_to_node, timeout, exception are not relevant to
    %% encryption, so we move them to the end
    SortedErrors = lists:sort(fun ({_, no_connection_to_node}, {_, _}) -> false;
                                  ({_, _}, {_, no_connection_to_node}) -> true;
                                  ({_, timeout}, {_, _}) -> false;
                                  ({_, _}, {_, timeout}) -> true;
                                  ({_, exception}, {_, _}) -> false;
                                  ({_, _}, {_, exception}) -> true;
                                  ({_, R1}, {_, R2}) -> R1 =< R2
                               end, Errors),
    [{_, Reason} | _] = SortedErrors,
    BuildHostname = menelaus_web_node:build_node_hostname(
                      ns_config:latest(), _, misc:localhost()),
    "Encryption key test failed on " ++
    lists:join(", ", [BuildHostname(Node) || {Node, _} <- Errors]) ++
    case length(lists:usort([R || {_, R} <- SortedErrors])) > 1 of
        true ->
            ". First error: " ++ format_error(Reason);
        false ->
            ": " ++ format_error(Reason)
    end;
format_error(retry) ->
    "Please try again later";
format_error(forbidden) ->
    "Forbidden";
format_error({key_test_alert, Reason, Name, Node, DateTime}) ->
    DateTimeStr = iso8601:format(DateTime),
    ReasonStr = format_error(Reason),
    Host = misc:extract_node_address(Node),
    lists:flatten(
      io_lib:format(
        "Encryption-at-Rest key validation event at ~s: FAILED on node \"~s\" "
        "for key \"~s\". Error details: \"~s\".",
        [DateTimeStr, Host, Name, ReasonStr]));
format_error({invalid_dek_file_name, Path}) ->
    io_lib:format("Invalid DEK file name: ~s", [Path]);
format_error({dek_decode_error, Msg}) ->
    Msg;
format_error({read_key_file_error, ErrorMsg}) ->
    io_lib:format("Failed to read DEK file: ~s", [ErrorMsg]);
format_error(kind_not_found) ->
    "Key type does not exist on this node";
format_error(Reason) ->
    lists:flatten(io_lib:format("~p", [Reason])).


format_secrets_used_by_list(UsedByMap) ->
    format_secrets_used_by_list(UsedByMap, direct).

format_secrets_used_by_list(UsedByMap, Snapshot) ->
    UsedByCfg = maps:get(by_config, UsedByMap, []),
    Secrets = maps:get(by_secrets, UsedByMap, []),
    UsedByDeks = maps:get(by_deks, UsedByMap, []),
    Joined = fun (L) -> lists:join(", ", ["\"" ++ E ++ "\"" || E <- L]) end,
    FormatUsages =
        fun (Usages) ->
                {BucketsUUIDs, Other} =
                    misc:partitionmap(
                      fun ({bucket_encryption, BUUID}) -> {left, BUUID};
                          (K) -> {right, K}
                      end, Usages),
                FormattedUsages = lists:map(fun usage_to_string/1, Other),
                AllBuckets = maps:from_list(
                               [{U, N} || {N, U} <- ns_bucket:uuids(Snapshot)]),
                Buckets = lists:map(fun (B) ->
                                            maps:get(B, AllBuckets,
                                                     "deleted (id: " ++
                                                     binary_to_list(B) ++
                                                     ")")
                                    end, BucketsUUIDs),
                Buckets2 = Joined(Buckets),
                BucketsStr =
                    case length(Buckets) of
                        0 -> [];
                        1 -> ["bucket " ++ Buckets2];
                        _ -> ["buckets " ++ Buckets2]
                    end,
                FormattedUsages ++ BucketsStr
        end,
    Kind2Usage =
        fun (K) ->
            {succ, U} = cb_deks:call_dek_callback(get_required_usage, K, []),
            U
        end,
    UsagesUsedByCfg = lists:uniq(lists:map(Kind2Usage, UsedByCfg)),
    UsagesUsedByDeks = lists:uniq(lists:map(Kind2Usage, UsedByDeks)),
    SecretsStrs = case length(Secrets) of
                      0 -> [];
                      1 -> ["key " ++ Joined(Secrets)];
                      _ -> ["keys " ++ Joined(Secrets)]
                  end,
    Strings1 = FormatUsages(UsagesUsedByCfg) ++ SecretsStrs,
    Strings2 = FormatUsages(UsagesUsedByDeks -- UsagesUsedByCfg),

    case {Strings1, Strings2} of
        {_, []} ->
            "this key is configured to encrypt " ++
            lists:join(", ", Strings1);
        {[], _} ->
            "this key still encrypts some data in " ++
            lists:join(", ", Strings2);
        {_ , _} ->
            "this key is configured to encrypt " ++
            lists:join(", ", Strings1) ++
            "; it also still encrypts some data in " ++
            lists:join(", ", Strings2)
    end.

format_secret_props(Props) ->
    export_secret(Props).

%% Not using menelaus_util:assert_is_79() because it returns text
%% instead of "global error" json, which is needed for the UI to show
%% the error in a proper way
assert_is_79() ->
    case cluster_compat_mode:is_cluster_79() of
        true ->
            ok;
        false ->
            menelaus_util:global_error_exception(
              400,
              <<"Not supported until cluster is fully 7.9">>)
    end.

is_writable_remote(ReqHidden, Node, Secret, Snapshot) when Node =:= node() ->
    is_writable(Secret, ?UNHIDE(ReqHidden), Snapshot);
is_writable_remote(ReqHidden, Node, Secret, Snapshot) ->
    erpc:call(Node, ?MODULE, is_writable_remote, [ReqHidden, Node, Secret, Snapshot],
              ?IS_WRITABLE_TIMEOUT).

get_test_results_aggregated(Secrets) ->
    NodesInfo = ns_doctor:get_nodes(),
    Nodes = ns_cluster_membership:nodes_wanted(),
    SecretIds = [Id || #{id := Id} <- Secrets],

    Initial = #{ Id => #{status => ok,
                         datetime => undefined,
                         missing_nodes => [],
                         error_nodes => [],
                         success_nodes => []} || Id <- SecretIds },
    Default = #{ Id => #{status => unknown,
                         datetime => undefined} || Id <- SecretIds },
    lists:foldl(fun (N, Acc) ->
                    TestResults =
                        maybe
                            {ok, Info} ?= dict:find(N, NodesInfo),
                            false ?= proplists:get_bool(stale, Info),
                            case proplists:get_value(encryption_keys_tests,
                                                     Info) of
                                %% Ignore, because not supported (old version)
                                undefined -> #{};
                                R -> maps:merge(Default, R)
                            end
                        else
                            %% Info for node is missing
                            error -> Default;
                            %% Info for node is stale
                            true -> Default
                        end,
                    %% NodesInfo may contain results for secrets that are not
                    %% in the list of secrets, so we need to filter them out
                    Filtered = maps:filter(
                                 fun (K, _) -> maps:is_key(K, Default) end,
                                 TestResults),
                    maps:merge_with(?cut(merge_test_res(_2, _3, N)),
                                    Acc, Filtered)
                end, Initial, Nodes).

merge_test_res(#{status := CurStatus, datetime := CurDT,
                 missing_nodes := CurMN, error_nodes := CurEN,
                 success_nodes := CurSN} = Cur,
               #{status := NewStatus, datetime := NewDT},
               Node) ->
    Status = case {CurStatus, NewStatus} of
                 {ok, ok} -> ok;
                 {ok, unknown} -> unknown;
                 {ok, {error, _}} -> NewStatus;
                 {unknown, ok} -> unknown;
                 {unknown, unknown} -> unknown;
                 {unknown, {error, _}} -> NewStatus;
                 {{error, _}, ok} -> CurStatus;
                 {{error, _}, unknown} -> CurStatus;
                 {{error, _}, {error, _}} when NewDT > CurDT -> NewStatus;
                 {{error, _}, {error, _}} -> CurStatus
             end,
    Cur#{status => Status,
         datetime => max(CurDT, NewDT),
         missing_nodes => case NewStatus of
                              unknown -> [Node | CurMN];
                              _ -> CurMN
                          end,
         error_nodes => case NewStatus of
                            {error, _} -> [Node | CurEN];
                            _ -> CurEN
                        end,
         success_nodes => case NewStatus of
                              ok -> [Node | CurSN];
                              _ -> CurSN
                          end}.

-ifdef(TEST).

format_secrets_used_by_list_test() ->
    Snapshot = #{bucket_names => {["b1", "b2"], 1},
                 {bucket, "b1", uuid} => {<<"b1-uuid">>, 2},
                 {bucket, "b2", uuid} => {<<"b2-uuid">>, 3}},
    All = cb_deks:dek_cluster_kinds_list(Snapshot),
    Secrets = ["s1", "s2"],
    F = ?cut(lists:flatten(format_secrets_used_by_list(_, Snapshot))),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audits, buckets \"b1\", \"b2\"",
                 F(#{by_deks => All, by_config => All, by_secrets => []})),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audits, buckets \"b1\", \"b2\"",
                 F(#{by_deks => [], by_config => All, by_secrets => []})),
    ?assertEqual("this key still encrypts some data in configuration, logs, "
                 "audits, buckets \"b1\", \"b2\"",
                 F(#{by_deks => All, by_config => [], by_secrets => []})),
    ?assertEqual("this key is configured to encrypt keys \"s1\", \"s2\"",
                 F(#{by_deks => [], by_config => [], by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audits, buckets \"b1\", \"b2\", keys \"s1\", \"s2\"",
                 F(#{by_deks => [], by_config => All, by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audits, buckets \"b1\", \"b2\", keys \"s1\", \"s2\"",
                 F(#{by_deks => All, by_config => All, by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration; it also "
                 "still encrypts some data in logs, audits, "
                 "buckets \"b1\", \"b2\"",
                 F(#{by_deks => All, by_config => [configDek],
                     by_secrets => []})),
    ?assertEqual("this key is configured to encrypt configuration, "
                 "bucket \"b2\", keys \"s1\", \"s2\"; "
                 "it also still encrypts some data in bucket \"b1\"",
                 F(#{by_deks => [configDek, {bucketDek, <<"b1-uuid">>}],
                     by_config => [{bucketDek, <<"b2-uuid">>}, configDek],
                     by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration, "
                 "bucket \"b2\", key \"s1\"; "
                 "it also still encrypts some data in bucket \"b1\"",
                 F(#{by_deks => [configDek, {bucketDek, <<"b1-uuid">>}],
                     by_config => [{bucketDek, <<"b2-uuid">>}, configDek],
                     by_secrets => ["s1"]})).

validate_azure_key_t() ->
    {error, Error0} =
        validate_azure_key("http://bla.me.com/keys/testKey"),
    ?assertEqual("failed to parse url: \"must be https url only\"",
        lists:flatten(Error0)),

    {error, Error1} =
        validate_azure_key("https://bla.me.com/keys/testKey"),
    ?assertEqual("domain not allowed: \"me.com\"",
                 lists:flatten(Error1)),

    {error, Error2} =
        validate_azure_key("https:///keys/testKey"),
    ?assertEqual("no host found in azure key URL",
                 lists:flatten(Error2)),

    {error, Error3} =
        validate_azure_key("https://me/keys/testKey"),
    ?assertEqual("format of host must be <vaultOrHsmName>.<domain>",
                 lists:flatten(Error3)),

    {error, Error4} =
        validate_azure_key("https://.domain.com/keys/testKey"),
    ?assertEqual("no vault or managedhsm name found in domain",
        lists:flatten(Error4)),

    {error, Error5} =
        validate_azure_key("https://name.vault.azure.net/testKey"),
    ?assertEqual("path must have /keys",
        lists:flatten(Error5)),

    {error, Error6} =
        validate_azure_key("https://name.vault.azure.net/keys/testKey/rev"),
    ?assertEqual("azure key must be a base identifier",
        lists:flatten(Error6)),

    ok = validate_azure_key("https://name.vault.azure.net/keys/testKey"),

    {error, Error7} =
        validate_azure_key("not valid url"),
    ?assertEqual("failed to parse url: invalid_uri",
                 lists:flatten(Error7)).

validate_azure_key_test() ->
    try
        meck:new(ns_config, [passthrough]),
        meck:expect(ns_config, search,
                    fun(_, azure_allowed_domains, D) ->
                        D
                     end),
        validate_azure_key_t()
    after
        meck:unload(ns_config)
    end.

validate_hashi_key_test() ->
    {error, Error0} =
        validate_hashi_key("invalid://bla.me.com/keys/testKey"),
    ?assertEqual("failed to parse url: \"must be http or https url only\"",
        lists:flatten(Error0)),

    {error, Error1} =
        validate_hashi_key("https:///keys/testKey"),
    ?assertEqual("a host must be specified in the URL",
                 lists:flatten(Error1)),

    {error, Error2} =
        validate_hashi_key("https://dome.domain"),
    ?assertEqual("path must have /<keyName>",
        lists:flatten(Error2)),

    {error, Error3} =
        validate_hashi_key("https://dome.domain/"),
    ?assertEqual("path must have /<keyName>",
        lists:flatten(Error3)),

    ok = validate_hashi_key("https://dome.domain/key").

-endif.
