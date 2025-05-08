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

-export([handle_get_secrets/1,
         handle_get_secret/2,
         handle_post_secret/1,
         handle_put_secret/2,
         handle_test_post_secret/1,
         handle_test_put_secret/2,
         handle_delete_secret/2,
         handle_delete_historical_key/3,
         handle_rotate/2,
         format_error/1,
         format_secret_props/1]).

handle_get_secrets(Req) ->
    All = cb_cluster_secrets:get_all(),
    FilteredSecrets = read_filter_secrets_by_permission(All, Req),
    Res = lists:map(
            fun (Props) ->
                {export_secret(Props)}
            end, FilteredSecrets),
    menelaus_util:reply_json(Req, Res).

handle_get_secret(IdStr, Req) when is_list(IdStr) ->
    menelaus_util:assert_is_enterprise(),
    case cb_cluster_secrets:get_secret(parse_id(IdStr)) of
        {ok, Props} ->
            case read_filter_secrets_by_permission([Props], Req) of
                [] -> menelaus_util:reply_not_found(Req);
                [_] ->
                    Res = {export_secret(Props)},
                    menelaus_util:reply_json(Req, Res)
            end;
        {error, not_found} ->
            menelaus_util:reply_not_found(Req)
    end.

handle_post_secret(Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    with_validated_secret(
      fun (ToAdd) ->
          maybe
              {ok, Res} ?= cb_cluster_secrets:add_new_secret(ToAdd),
              Formatted = export_secret(Res),
              ns_audit:set_encryption_secret(Req, Formatted),
              menelaus_util:reply_json(Req, {Formatted}),
              ok
          end
      end, #{}, Req).

handle_put_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:get_secret(Id) of
        {ok, CurProps} ->
            with_validated_secret(
              fun (Props) ->
                  maybe
                      %% replace_secret will check "old usages" inside txn
                      {ok, Res} ?= cb_cluster_secrets:replace_secret(
                                     Id, Props, is_writable(_, Req)),
                      Formatted = export_secret(Res),
                      ns_audit:set_encryption_secret(Req, Formatted),
                      menelaus_util:reply_json(Req, {Formatted}),
                      ok
                  end
              end, CurProps, Req);
        {error, not_found} ->
            %% We don't want PUT to create secrets because we generate id's
            menelaus_util:reply_not_found(Req)
    end.

handle_test_post_secret(Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    with_validated_secret(
      fun (Params) ->
          maybe
              ok ?= cb_cluster_secrets:test(Params, undefined),
              menelaus_util:reply(Req, 200),
              ok
          end
      end, #{}, Req).

handle_test_put_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:get_secret(Id) of
        {ok, CurProps} ->
            with_validated_secret(
              fun (Params) ->
                  maybe
                      ok ?= cb_cluster_secrets:test(Params, CurProps),
                      menelaus_util:reply(Req, 200),
                      ok
                  end
              end, CurProps, Req);
        {error, not_found} ->
            menelaus_util:reply_not_found(Req)
    end.

with_validated_secret(Fun, CurProps, Req) ->
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
              ok ?= Fun(Props)
          else
              false ->
                  menelaus_util:web_exception(403, "Forbidden");
              {error, forbidden} ->
                  menelaus_util:web_exception(403, "Forbidden");
              {error, no_quorum} ->
                  menelaus_util:web_exception(503, format_error(no_quorum));
              {error, Reason} ->
                  menelaus_util:reply_global_error(Req, format_error(Reason))
          end
      end, Req, json, secret_validators(CurProps)).

%% Note: CurProps can only be used for static fields validation here.
%% Any field that can be modified and needs to use CurProps should be
%% checked in transaction in cb_cluster_secret:replace_secret_internal.
secret_validators(CurProps) ->
    [validator:trimmed_string(name, _),
     validator:required(name, _),
     validator:validate(
       fun ("") -> {error, "Must not not be empty"};
           (Str) ->
               Id = maps:get(id, CurProps, ?SECRET_ID_NOT_SET),
               case cb_cluster_secrets:is_name_unique(Id, Str, direct) of
                   true -> ok;
                   %% Checking it here and inside transaction later
                   %% Check here is needed mostly to make it user friendly in UI
                   false -> {error, "Must be unique"}
               end
       end, name, _),
     validator:one_of(type, [?GENERATED_KEY_TYPE, ?AWSKMS_KEY_TYPE,
                             ?KMIP_KEY_TYPE], _),
     validator:convert(type, binary_to_atom(_, latin1), _),
     validator:required(type, _),
     validate_key_usage(usage, _),
     validator:required(usage, _),
     validate_secrets_data(data, CurProps, _),
     validator:required(data, _),
     validator:unsupported(_)].

enforce_static_field_validator(Name, CurValue, State) ->
    validator:validate(fun (NewValue) when NewValue == CurValue -> ok;
                            (_) -> {error, "the field can't be changed"}
                       end, Name, State).

handle_delete_secret(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:delete_secret(Id, is_writable(_, Req)) of
        {ok, Name} ->
            ns_audit:delete_encryption_secret(Req, Id, Name),
            menelaus_util:reply(Req, 200);
        {error, forbidden} ->
            menelaus_util:web_exception(403, "Forbidden");
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, no_quorum} ->
            menelaus_util:web_exception(503, format_error(no_quorum));
        {error, Reason} ->
            menelaus_util:reply_global_error(Req, format_error(Reason))
    end.

handle_delete_historical_key(IdStr, HistKeyIdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    Id = parse_id(IdStr),
    HistKeyId = list_to_binary(HistKeyIdStr),
    case cb_cluster_secrets:delete_historical_key(Id,
                                                  HistKeyId,
                                                  is_writable(_, Req)) of
        {ok, Name} ->
            ns_audit:delete_historical_encryption_key(Req, Id, Name, HistKeyId),
            menelaus_util:reply(Req, 200);
        {error, forbidden} ->
            menelaus_util:web_exception(403, "Forbidden");
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, no_quorum} ->
            menelaus_util:web_exception(503, format_error(no_quorum));
        {error, Reason} -> menelaus_util:reply_global_error(Req, format_error(Reason))
    end.

handle_rotate(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_is_morpheus(),
    Id = parse_id(IdStr),
    case cb_cluster_secrets:rotate(Id) of
        {ok, Name} ->
            ns_audit:rotate_encryption_secret(Req, Id, Name),
            menelaus_util:reply(Req, 200);
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, no_quorum} ->
            menelaus_util:web_exception(503, format_error(no_quorum));
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
      credentials_file => credentialsFile,
      config_file => configFile,
      use_imds => useIMDS,
      encrypt_with => encryptWith,
      encrypt_secret_id => encryptWithKeyId,
      stored_ids => storedKeyIds,
      key_path => keyPath,
      cert_path => certPath,
      key_passphrase => keyPassphrase,
      ca_selection => caSelection,
      encryption_approach => encryptionApproach,
      active_key => activeKey,
      hist_keys => historicalKeys,
      kmip_id => kmipId,
      key_material => keyMaterial,
      req_timeout_ms => reqTimeoutMs}.

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
                  lists:map(
                    fun ({bucket_encryption, "*"}) ->
                            <<"bucket-encryption">>;
                        ({bucket_encryption, BucketName}) ->
                            iolist_to_binary([<<"bucket-encryption-">>,
                                              BucketName]);
                        (config_encryption) ->
                            <<"config-encryption">>;
                        (secrets_encryption) ->
                            <<"KEK-encryption">>;
                        (audit_encryption) ->
                            <<"audit-encryption">>;
                        (log_encryption) ->
                            <<"log-encryption">>
                    end, UList);
              (data, D) when DataType == ?GENERATED_KEY_TYPE ->
                  {format_auto_generated_key_data(D)};
              (data, D) when DataType == ?AWSKMS_KEY_TYPE ->
                  {format_aws_key_data(D)};
              (data, D) when DataType == ?KMIP_KEY_TYPE ->
                  {format_kmip_key_data(D)}
          end, Props))).

format_auto_generated_key_data(Props) ->
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

validate_key_usage(Name, State) ->
    validator:string_array(
      Name,
      fun (Str) ->
          case iolist_to_binary(Str) of
              <<"bucket-encryption">> ->
                  {value, {bucket_encryption, "*"}};
              <<"bucket-encryption-", N/binary>> when size(N) > 0 ->
                  {value, {bucket_encryption, binary_to_list(N)}};
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
validate_secrets_data(Name, CurSecretProps, State) ->
    Type = validator:get_value(type, State),
    CurType = maps:get(type, CurSecretProps, Type),
    case Type == CurType of
        true ->
            Validators =
                case Type of
                    ?GENERATED_KEY_TYPE ->
                        generated_key_validators(CurSecretProps);
                    ?AWSKMS_KEY_TYPE ->
                        awskms_key_validators(CurSecretProps);
                    ?KMIP_KEY_TYPE ->
                        kmip_key_validators(CurSecretProps);
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
generated_key_validators(CurSecretProps) ->
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
     validate_encrypt_with(encryptWith, _),
     validator:default(encryptWith, nodeSecretManager, _),
     validator:integer(encryptWithKeyId, -1, max_uint64, _),
     validate_encrypt_secret_id(encryptWithKeyId, CurSecretProps, _)].

validate_encrypt_with(Name, State) ->
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
                                                   direct) of
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

kmip_key_validators(CurSecretProps) ->
    [validator:string(host, _),
     validator:required(host, _),
     validator:integer(port, 1, 65535, _),
     validator:required(port, _),
     validator:integer(reqTimeoutMs, 1000, 5 * 60 * 1000, _),
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
     validate_encrypt_with(encryptWith, _),
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

is_usage_allowed({bucket_encryption, "*"}, write, Req) ->
    %% Those who can create a bucket should be able to create a secret to
    %% encrypt that bucket
    menelaus_auth:has_permission({[buckets], create}, Req) orelse
    menelaus_auth:has_permission({[admin, security], write}, Req);
is_usage_allowed({bucket_encryption, "*"}, read, Req) ->
    %% Those who can view bucket list should be able to view the secrets
    %% that can encrypt buckets
    menelaus_auth:has_permission({[{bucket, any}, settings], read}, Req) orelse
    menelaus_auth:has_permission({[admin, security], read}, Req);

is_usage_allowed({bucket_encryption, B}, write, Req) ->
    %% Those who can modify bucket settings should be able to create a secret
    %% that encrypts that specific bucket
    menelaus_auth:has_permission({[{bucket, B}, settings], write}, Req) orelse
    menelaus_auth:has_permission({[admin, security], write}, Req);
is_usage_allowed({bucket_encryption, B}, read, Req) ->
    %% Those who can read bucket settings should be able to see secrets that
    %% can encrypt that specific bucket
    menelaus_auth:has_permission({[{bucket, B}, settings], read}, Req) orelse
    menelaus_auth:has_permission({[admin, security], read}, Req);

is_usage_allowed(secrets_encryption, write, Req) ->
    menelaus_auth:has_permission({[admin, security], write}, Req);
is_usage_allowed(secrets_encryption, read, Req) ->
    menelaus_auth:has_permission({[admin, security], read}, Req);

is_usage_allowed(config_encryption, write, Req) ->
    menelaus_auth:has_permission({[admin, security], write}, Req);
is_usage_allowed(config_encryption, read, Req) ->
    menelaus_auth:has_permission({[admin, security], read}, Req);

is_usage_allowed(audit_encryption, write, Req) ->
    menelaus_auth:has_permission({[admin, security], write}, Req);
is_usage_allowed(audit_encryption, read, Req) ->
    menelaus_auth:has_permission({[admin, security], read}, Req);

is_usage_allowed(log_encryption, write, Req) ->
    menelaus_auth:has_permission({[admin, security], write}, Req);
is_usage_allowed(log_encryption, read, Req) ->
    menelaus_auth:has_permission({[admin, security], read}, Req).

read_filter_secrets_by_permission(Secrets, Req) ->
    lists:filter(is_readable(_, Req), Secrets).

is_readable(#{usage := Usages}, Req) when Usages /= [] ->
    lists:any(is_usage_allowed(_, read, Req), Usages).

is_writable(#{usage := Usages}, Req) when Usages /= [] ->
    lists:all(is_usage_allowed(_, write, Req), Usages).

parse_id(Str) when is_list(Str) ->
    try list_to_integer(Str) of
        N -> N
    catch
        _:_ ->
            menelaus_util:web_exception(404, menelaus_util:reply_text_404())
    end.

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
format_error({store_key_error, Msg}) ->
    %% This error is returned when secret params test fails
    Msg;
format_error(active_key) ->
    "Can't delete active key";
format_error({unsafe, Reason}) ->
    "Operation is unsafe. " ++ format_error(Reason);
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
format_error({invalid_encryption_secret, Msg}) ->
    "Unable to perform encryption/decryption with provided key, "
    "check encryption key settings (" ++ Msg ++ ")";
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

format_error(Reason) ->
    lists:flatten(io_lib:format("~p", [Reason])).

format_secrets_used_by_list(UsedByMap) ->
    UsedByCfg = maps:get(by_config, UsedByMap, []),
    Secrets = maps:get(by_secrets, UsedByMap, []),
    UsedByDeks = maps:get(by_deks, UsedByMap, []),
    Joined = fun (L) -> lists:join(", ", ["\"" ++ E ++ "\"" || E <- L]) end,
    FormatUsages =
        fun (Usages) ->
                {Buckets, Other} = misc:partitionmap(
                                     fun ({bucket_encryption, B}) -> {left, B};
                                         (K) -> {right, K}
                                     end, Usages),
                FormattedUsages = lists:map(fun (config_encryption) ->
                                                    "configuration";
                                                (log_encryption) ->
                                                    "logs";
                                                (audit_encryption) ->
                                                    "audit"
                                            end, Other),
                Buckets2 = Joined(Buckets),
                BucketsStr =
                    case length(Buckets) of
                        0 -> [];
                        1 -> ["bucket " ++ Buckets2];
                        _ -> ["buckets " ++ Buckets2]
                    end,
                FormattedUsages ++ BucketsStr
        end,
    Kind2Usage = ?cut(maps:get(required_usage, cb_deks:dek_config(_))),
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

%% Not using menelaus_util:assert_is_morpheus() because it returns text
%% instead of "global error" json, which is needed for the UI to show
%% the error in a proper way
assert_is_morpheus() ->
    case cluster_compat_mode:is_cluster_morpheus() of
        true ->
            ok;
        false ->
            menelaus_util:global_error_exception(
              400,
              <<"Not supported until cluster is fully Morpheus">>)
    end.

-ifdef(TEST).

format_secrets_used_by_list_test() ->
    All = cb_deks:dek_cluster_kinds_list(#{bucket_names => {["b1", "b2"], 1}}),
    Secrets = ["s1", "s2"],
    F = ?cut(lists:flatten(format_secrets_used_by_list(_))),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audit, buckets \"b1\", \"b2\"",
                 F(#{by_deks => All, by_config => All, by_secrets => []})),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audit, buckets \"b1\", \"b2\"",
                 F(#{by_deks => [], by_config => All, by_secrets => []})),
    ?assertEqual("this key still encrypts some data in configuration, logs, "
                 "audit, buckets \"b1\", \"b2\"",
                 F(#{by_deks => All, by_config => [], by_secrets => []})),
    ?assertEqual("this key is configured to encrypt keys \"s1\", \"s2\"",
                 F(#{by_deks => [], by_config => [], by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audit, buckets \"b1\", \"b2\", keys \"s1\", \"s2\"",
                 F(#{by_deks => [], by_config => All, by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration, logs, "
                 "audit, buckets \"b1\", \"b2\", keys \"s1\", \"s2\"",
                 F(#{by_deks => All, by_config => All, by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration; it also "
                 "still encrypts some data in logs, audit, "
                 "buckets \"b1\", \"b2\"",
                 F(#{by_deks => All, by_config => [configDek],
                     by_secrets => []})),
    ?assertEqual("this key is configured to encrypt configuration, "
                 "bucket \"b2\", keys \"s1\", \"s2\"; "
                 "it also still encrypts some data in bucket \"b1\"",
                 F(#{by_deks => [configDek, {bucketDek, "b1"}],
                     by_config => [{bucketDek, "b2"}, configDek],
                     by_secrets => Secrets})),
    ?assertEqual("this key is configured to encrypt configuration, "
                 "bucket \"b2\", key \"s1\"; "
                 "it also still encrypts some data in bucket \"b1\"",
                 F(#{by_deks => [configDek, {bucketDek, "b1"}],
                     by_config => [{bucketDek, "b2"}, configDek],
                     by_secrets => ["s1"]})).

-endif.
