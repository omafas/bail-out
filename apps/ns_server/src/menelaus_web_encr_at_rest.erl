%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_web_encr_at_rest).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("cb_cluster_secrets.hrl").

-export([handle_get/2, handle_post/2, get_settings/1, handle_drop_keys/2,
         handle_bucket_drop_keys/2, build_bucket_encr_at_rest_info/2,
         format_encr_at_rest_info/1, handle_force_encr/2,
         handle_bucket_force_encr/2]).

encr_method(Param, SecretIdName, EncrType) ->
    AllowedInMixedClusters = case EncrType of
                                 config_encryption -> true;
                                 _ -> cluster_compat_mode:is_cluster_morpheus()
                             end,
    {Param,
     #{cfg_key => [EncrType, encryption],
       type => {encryption_method, AllowedInMixedClusters},
       depends_on =>
           #{SecretIdName => fun (secret, ?SECRET_ID_NOT_SET) ->
                                     {error, "encryptionKeyId must be set "
                                             "when encryptionMethod is set "
                                             "to encryptionKey"};
                                 (_, _) ->
                                     %% Intentionally not checking the case
                                     %% where encryptionMethod is set to
                                     %% nodeSecretManager or disabled because
                                     %% secretId can be just automatically
                                     %% set to ?SECRET_ID_NOT_SET in that case
                                     ok
                             end}}}.

encr_secret_id(Param, EncrMethodName, EncrType) ->
    {Param,
     #{cfg_key => [EncrType, secret_id],
       type => secret_id,
       depends_on =>
           #{EncrMethodName => fun (?SECRET_ID_NOT_SET, secret) ->
                                       {error, "encryptionKeyId must be set "
                                               "when encryptionMethod is set "
                                               "to encryptionKey"};
                                   (_Id, secret) ->
                                       ok;
                                   (Id, _) when Id /= ?SECRET_ID_NOT_SET ->
                                       {error, "encryptionKeyId must not be "
                                               "set when encryptionMethod is "
                                               "set to nodeSecretManager or "
                                               "disabled"};
                                   (_, _) ->
                                       ok
                               end}}}.

encr_dek_lifetime(Param, RotIntervalName, EncrType) ->
    DependsOnFn =
        case ns_config:read_key_fast(test_bypass_encr_cfg_restrictions,
                                     false) of
            true ->
                fun (_LifeTime, _RotIntrvl) -> ok end;
            _ ->
                fun (0 = _LifeTime, _RotIntrvl) ->
                        ok;
                    (_LifeTime, 0 = _RotIntrvl) ->
                        {error,  "dekLifetime must be set to 0, if "
                                 "dekRotationInterval is currently 0"};
                    (LifeTime, RotIntrvl)
                      when LifeTime <
                           RotIntrvl + ?DEK_LIFETIME_ROTATION_MARGIN_SEC ->
                        Err = io_lib:format("dekLifetime must be a least ~p "
                                            "seconds more than the current "
                                            "dekRotationInterval value of ~p",
                                            [?DEK_LIFETIME_ROTATION_MARGIN_SEC,
                                             RotIntrvl]),
                        {error, Err};
                    (_, _) ->
                        ok
                end
        end,
    {Param,
     #{cfg_key => [EncrType, dek_lifetime_in_sec],
       type => {int, 0, max_uint64},
       depends_on => #{RotIntervalName => DependsOnFn}}}.

encr_dek_rotate_intrvl(Param, LifetimeName, EncrType) ->
    DependsOnFn =
        case ns_config:read_key_fast(test_bypass_encr_cfg_restrictions,
                                     false) of
            true ->
                fun (_RotIntrvl, _LifeTime) -> ok end;
            _ ->
                fun (_RotIntrvl, 0 = _LifeTime) ->
                        ok;
                    (0 = _RotIntrvl, _LifeTime) ->
                        {error, "dekRotationInterval can't be set to 0 "
                                "if dekLifetime is not currently 0"};
                    (RotIntrvl, LifeTime)
                      when LifeTime <
                           RotIntrvl + ?DEK_LIFETIME_ROTATION_MARGIN_SEC ->
                        Err = io_lib:format("dekRotationInterval must be a "
                                            "least ~p seconds less than the "
                                            "current dekLifetime value of ~p",
                                            [?DEK_LIFETIME_ROTATION_MARGIN_SEC,
                                             LifeTime]),
                        {error, Err};
                    (_, _) ->
                        ok
                end
        end,

    {Param,
     #{cfg_key => [EncrType, dek_rotation_interval_in_sec],
       type => {int, 0, max_uint64},
       depends_on => #{LifetimeName => DependsOnFn}}}.

encr_deks_drop_date(Param, EncrType) ->
    {Param,
     #{cfg_key => [EncrType, dek_drop_datetime],
       type => {read_only, {optional, datetime_iso8601}}}}.

encr_force_encr_date(Param, EncrType) ->
    {Param,
     #{cfg_key => [EncrType, force_encryption_datetime],
       type => {read_only, {optional, datetime_iso8601}}}}.

encr_info(Param, EncrType) ->
    {Param, #{cfg_key => [EncrType, info],
              type => {read_only, encr_info}}}.

skip_test(Param, EncrType) ->
    {Param,
     #{cfg_key => [EncrType, skip_encryption_key_test],
       type => bool}}.

params() ->
    [encr_method("config.encryptionMethod", "config.encryptionKeyId",
                 config_encryption),
     encr_method("log.encryptionMethod", "log.encryptionKeyId",
                 log_encryption),
     encr_method("audit.encryptionMethod", "audit.encryptionKeyId",
                 audit_encryption),

     encr_secret_id("config.encryptionKeyId", "config.encryptionMethod",
                    config_encryption),
     encr_secret_id("log.encryptionKeyId", "log.encryptionMethod",
                    log_encryption),
     encr_secret_id("audit.encryptionKeyId", "audit.encryptionMethod",
                    audit_encryption),

     encr_dek_lifetime("config.dekLifetime",
                       "config.dekRotationInterval", config_encryption),
     encr_dek_lifetime("log.dekLifetime",
                       "log.dekRotationInterval", log_encryption),
     encr_dek_lifetime("audit.dekLifetime",
                       "audit.dekRotationInterval", audit_encryption),

     encr_dek_rotate_intrvl("config.dekRotationInterval",
                            "config.dekLifetime", config_encryption),
     encr_dek_rotate_intrvl("log.dekRotationInterval",
                            "log.dekLifetime", log_encryption),
     encr_dek_rotate_intrvl("audit.dekRotationInterval",
                            "audit.dekLifetime", audit_encryption),

     encr_deks_drop_date("config.dekLastDropDate", config_encryption),
     encr_deks_drop_date("log.dekLastDropDate", log_encryption),
     encr_deks_drop_date("audit.dekLastDropDate", audit_encryption),

     encr_force_encr_date("config.lastForceEncryptionDate", config_encryption),
     encr_force_encr_date("log.lastForceEncryptionDate", log_encryption),
     encr_force_encr_date("audit.lastForceEncryptionDate", audit_encryption),

     encr_info("config.info", config_encryption),
     encr_info("log.info", log_encryption),
     encr_info("audit.info", audit_encryption),

     skip_test("config.skipEncryptionKeyTest", config_encryption),
     skip_test("log.skipEncryptionKeyTest", log_encryption),
     skip_test("audit.skipEncryptionKeyTest", audit_encryption)].

type_spec(secret_id) ->
    ValidatorFun = fun (?SECRET_ID_NOT_SET) -> ok;
                       (Id) ->
                           case cb_cluster_secrets:get_secret(Id) of
                               {ok, _} -> ok;
                               {error, _} -> {error, "Key does not exist"}
                           end
                   end,
    #{validators => [int, ?cut(validator:validate(ValidatorFun, _1, _2))],
      formatter => int};
type_spec({encryption_method, AllowedInMixedClusters}) ->
    #{validators => [{one_of, string,
                      ["disabled", "nodeSecretManager", "encryptionKey"]},
                     ?cut(validator:convert(_1, fun ("disabled") ->
                                                        disabled;
                                                    ("nodeSecretManager") ->
                                                        encryption_service;
                                                    ("encryptionKey") ->
                                                        secret
                                                end, _2)),
                     ?cut(validator:validate(
                            fun (disabled) -> ok;
                                (_V) when AllowedInMixedClusters -> ok;
                                (_V) -> {error, "Not supported until cluster "
                                                "is fully Morpheus"}
                            end, _1, _2))],
      formatter => fun (encryption_service) -> {value, <<"nodeSecretManager">>};
                       (disabled) -> {value, <<"disabled">>};
                       (secret) -> {value, <<"encryptionKey">>}
                   end};
type_spec(encr_info) ->
    #{validators => [not_supported],
      formatter => fun (undefined) -> ignore;
                       (Info) -> {value, format_encr_at_rest_info(Info)}
                   end}.

handle_get(Path, Req) ->
    Settings = get_settings(direct),
    NodesInfo = ns_doctor:get_nodes(),
    Nodes = ns_cluster_membership:nodes_wanted(),
    List = lists:map(
             fun ({K, V}) ->
                 {K, [{info, aggregated_EAR_info(K, NodesInfo, Nodes)} | V]}
             end, settings_to_list(Settings)),
    menelaus_web_settings2:handle_get(Path, params(), fun type_spec/1, List,
                                      Req).

settings_to_list(Settings) ->
    maps:to_list(maps:map(fun (_K, V) -> maps:to_list(V) end, Settings)).

handle_post(Path, Req) ->
    menelaus_util:assert_is_enterprise(),
    AlreadyDefined = chronicle_compat:get(direct,
                                          ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY,
                                          #{default => #{}}),
    menelaus_web_settings2:handle_post(
      fun (Params, Req2) ->
          NewSettings = maps:map(fun (_, V) -> maps:from_list(V) end,
                                 maps:groups_from_list(
                                   fun ({[K1, _K2], _V}) -> K1 end,
                                   fun ({[_K1, K2], V}) -> {K2, V} end,
                                   Params)),
          NewSettings2 =
              maps:map(fun (_, #{encryption := disabled} = P) ->
                               P#{secret_id => ?SECRET_ID_NOT_SET};
                           (_, #{encryption := encryption_service} = P) ->
                               P#{secret_id => ?SECRET_ID_NOT_SET};
                           (_, P) -> P
                       end, NewSettings),

          test_encryption_keys(NewSettings2),

          NewSettings3 = maps:map(fun (_, S) ->
                                      maps:remove(skip_encryption_key_test, S)
                                  end, NewSettings2),

          RV = chronicle_kv:transaction(
                 kv, [?CHRONICLE_SECRETS_KEY,
                      ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY],
                 fun (Snapshot) ->
                     CurrentSettings = get_settings(Snapshot),
                     MergedNewSettings = get_settings(Snapshot, NewSettings3),
                     ToApply = apply_auto_fields(
                                 Snapshot, MergedNewSettings),
                     case validate_all_settings_txn(maps:to_list(ToApply),
                                                    Snapshot) of
                         ok ->
                             {commit,
                              [{set,
                                ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY,
                                ToApply}],
                              {ToApply, CurrentSettings}};
                         {error, _} = Error ->
                             {abort, Error}
                     end
                 end),
         case RV of
             {ok, _, {SettingsApplied, PrevSettings}} ->
                 log_and_audit_settings(Req2, SettingsApplied, PrevSettings),
                 cb_cluster_secrets:sync_with_all_node_monitors(),
                 handle_get(Path, Req2);
             {error, Msg} ->
                 menelaus_util:reply_global_error(Req2, Msg)
         end
      end, Path, params(), fun type_spec/1,
      settings_to_list(AlreadyDefined), settings_to_list(defaults()), Req).

handle_bucket_drop_keys(Bucket, Req) ->
    handle_bucket_drop_keys(Bucket, dek_drop_datetime, dropKeysDate, Req).

handle_bucket_force_encr(Bucket, Req) ->
    handle_bucket_drop_keys(Bucket, force_encryption_datetime,
                            forceEncryptionDate, Req).

handle_bucket_drop_keys(Bucket, DropDeksTimeKey, AuditKey, Req) ->
    menelaus_util:assert_is_enterprise(),
    handle_set_drop_time(
      fun (Time) ->
          Key = ns_bucket:sub_key(Bucket, encr_at_rest),
          Res = cb_cluster_secrets:chronicle_transaction(
                  [ns_bucket:root(), ns_bucket:sub_key(Bucket, props), Key],
                  fun (Snapshot) ->
                      case ns_bucket:bucket_exists(Bucket, Snapshot) of
                          true ->
                              CurVal = chronicle_compat:get(Snapshot, Key,
                                                            #{default => #{}}),
                              NewVal = CurVal#{DropDeksTimeKey => Time},
                              {commit, [{set, Key, NewVal}]};
                          false ->
                              menelaus_util:web_exception(
                                404, "Requested resource not found.\r\n")
                      end
                  end),
          case Res of
              ok ->
                  AuditProps = [{dekType, "bucket"},
                                {bucketName, Bucket},
                                {AuditKey, iso8601:format(Time)}],
                  ns_audit:encryption_at_rest_drop_deks(Req, AuditProps),
                  ok;
              {error, no_quorum = R} ->
                  menelaus_util:web_exception(
                    503, menelaus_web_secrets:format_error(R))
          end
      end, AuditKey, Req).

build_bucket_encr_at_rest_info(BucketUUID, BucketConfig) ->
    NodesInfo = ns_doctor:get_nodes(),
    Nodes = ns_bucket:get_servers(BucketConfig),
    I = aggregated_EAR_info({bucket_encryption, BucketUUID}, NodesInfo, Nodes),
    format_encr_at_rest_info(I).

format_encr_at_rest_info(Info) ->
    {lists:map(fun ({data_status, partially_encrypted}) ->
                       {dataStatus, partiallyEncrypted};
                   ({data_status, S}) ->
                       {dataStatus, S};
                   ({issues, L}) ->
                       {issues, cb_cluster_secrets:format_dek_issues(L)};
                   ({dek_num, V}) ->
                       {dekNumber, V};
                   ({oldest_dek_datetime, D}) ->
                       {oldestDekCreationDatetime,
                        misc:utc_to_iso8601(D, local)}
                end, Info)}.

handle_drop_keys(TypeName, Req) ->
    handle_drop_keys(TypeName, dek_drop_datetime, dropKeysDate, Req).

handle_force_encr(TypeName, Req) ->
    handle_drop_keys(TypeName, force_encryption_datetime,
                     forceEncryptionDate, Req).

handle_drop_keys(TypeName, DropDeksTimeKey, AuditKey, Req) ->
    menelaus_util:assert_is_enterprise(),
    handle_set_drop_time(
      fun (Time) ->
          TypeKey =
              case TypeName of
                  "config" -> config_encryption;
                  "log" -> log_encryption;
                  "audit" -> audit_encryption;
                  _ -> menelaus_util:web_exception(404, "not found")
              end,
          Res = cb_cluster_secrets:chronicle_transaction(
                  [?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY],
                  fun (Snapshot) ->
                      AllSettings = chronicle_compat:get(
                                      Snapshot,
                                      ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY,
                                      #{default => #{}}),
                      SubSettings = maps:get(TypeKey, AllSettings, #{}),
                      NewSubSetting = SubSettings#{DropDeksTimeKey =>
                                                   {set, Time}},
                      NewSettings = AllSettings#{TypeKey => NewSubSetting},
                      {commit,
                       [{set, ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY,
                        NewSettings}]}
                  end),
          case Res of
              ok ->
                  AuditProps = [{dekType, TypeName},
                                {AuditKey, iso8601:format(Time)}],
                  ns_audit:encryption_at_rest_drop_deks(Req, AuditProps),
                  ok;
              {error, no_quorum = R} ->
                  menelaus_util:web_exception(
                    503, menelaus_web_secrets:format_error(R))
          end
      end, AuditKey, Req).

handle_set_drop_time(SetTimeFun, AuditKey, Req) ->
    %% We need to sync with all node monitors to make sure that all nodes
    %% have generated DEKs that they have to be generated by this moment.
    %% This is needed to make sure dek drop time doesn't match the time of
    %% of any DEK that was triggered by user (if "DEK generation time" ==
    %% "deks drop time", we interpret it as "DEK generation happened after
    %% the drop time").
    %%
    %% In order to avoid that we:
    %% 1. sync with all node monitors to make sure they finished their work
    %% 2. sleep for 1 second to make sure current timestamp doesn't match
    %%    the time of any DEK that was triggered during the same second
    %%    (we use datetime format with has 1 second resolution).
    %%
    %% This is not a perfect solution, but it's the best we can do for now.
    %% Primary scenario that is being addressed here is the following:
    %% 1. Call drop deks
    %% 2. Disable encryption
    %% 3. Call drop deks again
    %% If #1 and #3 happens in the same second, we will have some data left
    %% encrypted after #3 (because deks that we generated because of #1 will be
    %% treated as those that were generated after #3).
    %%
    %% Also note that we are assuming that cluster nodes have their clocks
    %% synchronized. If one node is significantly ahead of others (1 second
    %% or more), this mechanism will not work as expected.
    cb_cluster_secrets:sync_with_all_node_monitors(),
    timer:sleep(1000),
    validator:handle(
      fun (ParsedList) ->
          Time = case proplists:get_value(datetime, ParsedList) of
                     undefined -> calendar:universal_time();
                     DT -> DT
                 end,
          ok = SetTimeFun(Time),
          Reply = {[{AuditKey, iso8601:format(Time)}]},
          menelaus_util:reply_json(Req, Reply)
      end, Req, form, [validator:iso_8601_parsed(datetime, _),
                       validator:unsupported(_)]).

validate_all_settings_txn([], _Snapshot) -> ok;
validate_all_settings_txn([{Usage, Cfg} | Tail], Snapshot) ->
    maybe
        ok ?= validate_sec_settings(Usage, Cfg, Snapshot),
        ok ?= validate_no_unencrypted_secrets(Usage, Cfg, Snapshot),
        validate_all_settings_txn(Tail, Snapshot)
    end.

get_settings(Snapshot) -> get_settings(Snapshot, #{}).
get_settings(Snapshot, ExtraSettings) ->
    Merge = fun (Settings1, Settings2) ->
                maps:merge_with(fun (_K, V1, V2) -> maps:merge(V1, V2) end,
                                Settings1, Settings2)
            end,
    Settings = chronicle_compat:get(Snapshot,
                                    ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY,
                                    #{default => #{}}),
    Merge(Merge(defaults(), Settings), ExtraSettings).

defaults() ->
    IsEnterprise = cluster_compat_mode:is_enterprise(),
    #{config_encryption => #{encryption => case IsEnterprise of
                                               true -> encryption_service;
                                               false -> disabled
                                           end,
                             secret_id => ?SECRET_ID_NOT_SET,
                             dek_lifetime_in_sec => 365*60*60*24,
                             dek_rotation_interval_in_sec => 30*60*60*24,
                             dek_drop_datetime => {not_set, ""},
                             force_encryption_datetime => {not_set, ""}},
      log_encryption => #{encryption => disabled,
                          secret_id => ?SECRET_ID_NOT_SET,
                          dek_lifetime_in_sec => 365*60*60*24,
                          dek_rotation_interval_in_sec => 30*60*60*24,
                          dek_drop_datetime => {not_set, ""},
                          force_encryption_datetime => {not_set, ""}},
      audit_encryption => #{encryption => disabled,
                            secret_id => ?SECRET_ID_NOT_SET,
                            dek_lifetime_in_sec => 365*60*60*24,
                            dek_rotation_interval_in_sec => 30*60*60*24,
                            dek_drop_datetime => {not_set, ""},
                            force_encryption_datetime => {not_set, ""}}}.

validate_no_unencrypted_secrets(config_encryption,
                                #{encryption := disabled,
                                  secret_id := ?SECRET_ID_NOT_SET}, Snapshot) ->
    Secrets = lists:filter(
                fun cb_cluster_secrets:is_encrypted_by_secret_manager/1,
                cb_cluster_secrets:get_all(Snapshot)),
    Names = lists:map(fun (#{name := Name}) -> Name end, Secrets),
    case Names of
        [] -> ok;
        [_ | _] ->
            NamesStr = lists:join(", ", Names),
            {error, "Encryption can't be disabled because it will leave "
                    "some cluster secrets unencrypted: " ++ NamesStr}
    end;
validate_no_unencrypted_secrets(_, #{}, _Snapshot) ->
    ok.

validate_sec_settings(_, #{encryption := disabled,
                           secret_id := ?SECRET_ID_NOT_SET}, _) ->
    ok;
validate_sec_settings(_, #{encryption := disabled,
                           secret_id := _}, _) ->
    {error, "Key id must not be set when encryption is disabled"};
validate_sec_settings(_, #{encryption := encryption_service,
                           secret_id := ?SECRET_ID_NOT_SET}, _) ->
    ok;
validate_sec_settings(_, #{encryption := encryption_service,
                           secret_id := _}, _) ->
    {error, "Key id must not be set when encryption_service is used"};
validate_sec_settings(_, #{encryption := secret,
                        secret_id := ?SECRET_ID_NOT_SET}, _) ->
    {error, "Key id must be set"};
validate_sec_settings(Name, #{encryption := secret,
                              secret_id := Id}, Snapshot) ->
    case cb_cluster_secrets:is_allowed_usage_for_secret(Id, Name, Snapshot) of
        ok -> ok;
        {error, not_found} -> {error, "Key not found"};
        {error, not_allowed} -> {error, "Key not allowed"}
    end.

apply_auto_fields(Snapshot, NewSettings) ->
    CurSettings = get_settings(Snapshot),
    IsEnabled = fun (S) -> maps:get(encryption, S) == disabled end,
    maps:fold(
      fun (T, V1, Acc) ->
          #{T := V2} = Acc,
          case IsEnabled(V1) /= IsEnabled(V2) of
              true -> Acc#{T => V2#{encryption_last_toggle_datetime =>
                                        calendar:universal_time()}};
              false -> Acc
          end
      end, NewSettings, CurSettings).

aggregated_EAR_info(_Type, _NodesInfo, []) ->
    [];
aggregated_EAR_info(Type, NodesInfo, Nodes) ->
    maps:to_list(
      lists:foldl(
        fun (N, Acc) ->
            Info = extract_node_EAR_info(Type, NodesInfo, N),
            InfoMap = maps:from_list(Info),
            case Acc of
                undefined -> InfoMap;
                _ -> cb_cluster_secrets:merge_dek_infos(Acc, InfoMap)
            end
        end, undefined, Nodes)).

extract_node_EAR_info(Type, NodesInfo, Node) ->
    maybe
        {ok, NodeInfo} ?= dict:find(Node, NodesInfo),
        case cb_cluster_secrets:node_supports_encryption_at_rest(NodeInfo) of
            false ->
                [{issues, []}];
            _ ->
                EARInfo = proplists:get_value(encryption_at_rest_info,
                                              NodeInfo, []),
                proplists:get_value(Type, EARInfo,
                                    [{issues, [{node_info, pending}]}])
        end
    else
        error ->
            [{issues, [{node_info, pending}]}]
    end.

log_and_audit_settings(Req, NewSettings, OldSettings) ->
    Prepare =
        fun (S) ->
            L = maps:to_list(maps:map(fun (_, V) -> maps:to_list(V) end, S)),
            menelaus_web_settings2:prepare_json([], params(),
                                                fun type_spec/1, L)
        end,
    {NewProps} = Prepare(NewSettings),
    {OldProps} = Prepare(OldSettings),
    event_log:add_log(encr_at_rest_cfg_changed, [{new_settings, {NewProps}},
                                                 {old_settings, {OldProps}}]),
    ns_audit:encryption_at_rest_settings(Req, NewProps).

test_encryption_keys(SettingsToTest) ->
    %% Check if the encryption key being set is working.
    %% It doesn't guarantee that the key will continue working (as the key can
    %% be removed or changed on remote system such as KMIP), but it will catch
    %% most of the issues.
    CurSettings = get_settings(direct),
    NewSettings = get_settings(direct, SettingsToTest),
    maps:foreach(
      fun (_Type, #{encryption := disabled}) -> ok;
          (_Type, #{encryption := encryption_service}) -> ok;
          (_Type, #{skip_encryption_key_test := true}) -> ok;
          (Type, #{encryption := secret, secret_id := SecretId}) ->
              case maps:get(Type, CurSettings) of
                  #{encryption := secret, secret_id := SecretId} ->
                      %% Nothing changes
                      ok;
                  _ ->
                      %% Encryption key is being changed
                      Nodes = ns_node_disco:nodes_actual(),
                      case cb_cluster_secrets:test_existing_secret(SecretId,
                                                                   Nodes) of
                          ok -> ok;
                          {error, Error} ->
                              Msg = menelaus_web_secrets:format_error(Error),
                              menelaus_util:global_error_exception(
                                400, iolist_to_binary(Msg))
                      end
              end
      end, NewSettings).