%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_kmip_ear_key).

-behaviour(cb_cluster_secrets).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").

-export([prepare_new_props/3,
         modify_props/3,
         sanitize_props/2,
         persist/3,
         generate_key/2,
         set_new_active_key_in_props/3,
         historical_keys_to_remove_from_props/2,
         get_next_rotation_time_from_props/2,
         maybe_update_next_rotation_time_in_props/3,
         remove_historical_key_from_props/3,
         test_props/3,
         is_encrypted_by_secret_manager/2,
         get_active_key_id_from_props/2,
         get_all_key_ids_from_props/2,
         get_key_ids_that_encrypt_props/2,
         get_secret_ids_that_encrypt_props/2,
         get_props_encryption_method/2,
         maybe_reencrypt_props/4]).

-export_type([secret_props/0]).

-type secret_props() :: #{host := string(),
                          port := 1..65535,
                          req_timeout_ms := integer(),
                          key_path := string(),
                          cert_path := string(),
                          key_passphrase := cb_cluster_secrets:sensitive_data(),
                          ca_selection := use_sys_ca | use_cb_ca |
                                          use_sys_and_cb_ca |
                                          skip_server_cert_verification,
                          encryption_approach := use_get | use_encrypt_decrypt,
                          active_key := kmip_key(),
                          hist_keys := [kmip_key()],
                          encrypt_with := nodeSecretManager |
                                          encryptionKey,
                          encrypt_secret_id := cb_cluster_secrets:secret_id() |
                                               ?SECRET_ID_NOT_SET}.

-type kmip_key() :: #{id := cb_cluster_secrets:key_id(),
                      kmip_id := binary(),
                      creation_time := calendar:datetime()}.

-spec prepare_new_props(calendar:datetime(), map(), list()) ->
          secret_props().
prepare_new_props(CreationTime, PropsFromValidator, _) ->
    #{active_key := AK, key_passphrase := HiddenKP} = PropsFromValidator,
    KP = #{type => sensitive,
           data => ?UNHIDE(HiddenKP),
           encrypted_by => undefined},
    PropsFromValidator#{active_key => AK#{id => cb_cluster_secrets:new_key_id(),
                             creation_time => CreationTime},
                        key_passphrase => KP,
                        hist_keys => []}.

-spec modify_props(secret_props(), map(), list()) -> secret_props().
modify_props(OldProps, PropsFromValidator, _) ->
    #{active_key := OldActive,
      hist_keys := HistKeys,
      key_passphrase := OldPassphrase} = OldProps,
    #{active_key := Active} = PropsFromValidator,
    #{id := OldActiveId,
        kmip_id := OldKmipId,
        creation_time := OldCT} = OldActive,
    #{kmip_id := NewKmipId} = Active,
    {NewFound, HistKeysCleaned} =
        misc:partitionmap(fun (#{kmip_id := I} = E)
                                                when I == NewKmipId ->
                                  {left, E};
                              (#{} = E) ->
                                  {right, E}
                          end, HistKeys),

    {NewActive, NewHistKeys} =
        case {NewKmipId == OldKmipId, NewFound} of
            {true, _} ->
                %% New active is not actually new
                %% Copy id and creation date from existing active key
                {Active#{id => OldActiveId, creation_time => OldCT},
                    HistKeys};
            {false, []} ->
                %% We have absolutely new active key
                %% Generate new uuid for it and set now as its creation
                %% time
                {Active#{id => cb_cluster_secrets:new_key_id(),
                         creation_time => erlang:universaltime()},
                    [OldActive | HistKeys]};
            {false, [PrevNew]} ->
                %% New active key doesn't match current active key,
                %% but it is present in history keys.
                %% Restore the existing key as active by copying its id
                %% and creation date and remove it from the list of
                %% history keys)
                ExistingKeyId = maps:get(id, PrevNew),
                ExistingKeyCT = maps:get(creation_time, PrevNew),
                {Active#{id => ExistingKeyId,
                            creation_time => ExistingKeyCT},
                    [OldActive | HistKeysCleaned]}
        end,
    KP =
        case maps:find(key_passphrase, PropsFromValidator) of
            error ->
                OldPassphrase;
            {ok, NewHiddenPass} ->
                #{type => sensitive,
                    data => ?UNHIDE(NewHiddenPass),
                    encrypted_by => undefined}
        end,
    PropsFromValidator#{active_key => NewActive,
                        hist_keys => NewHistKeys,
                        key_passphrase => KP}.

-spec sanitize_props(secret_props(), list()) -> secret_props().
sanitize_props(#{key_passphrase := D} = Props, _) ->
    Props#{key_passphrase => cb_cluster_secrets:sanitize_sensitive_data(D)}.

-spec persist(secret_props(), binary(), list()) -> ok | {error, _}.
persist(Props, ExtraAD, _) ->
    ensure_kmip_kek_on_disk(Props, ExtraAD, false).

-spec generate_key(calendar:datetime(), list()) -> {error, not_supported}.
generate_key(_CreationTime, _) ->
    {error, not_supported}.

-spec set_new_active_key_in_props(kmip_key(), secret_props(), list()) ->
          no_return().
set_new_active_key_in_props(_Key, _Props, _) ->
    erlang:error(internal_error).

-spec historical_keys_to_remove_from_props(secret_props(), list()) -> [].
historical_keys_to_remove_from_props(#{}, _) ->
    [].

-spec get_next_rotation_time_from_props(secret_props(), list()) -> undefined.
get_next_rotation_time_from_props(#{}, _) ->
    undefined.

-spec maybe_update_next_rotation_time_in_props(secret_props(),
                                                calendar:datetime(), list()) ->
          {error, not_supported}.
maybe_update_next_rotation_time_in_props(_Props, _CurTime, _) ->
    {error, not_supported}.

-spec remove_historical_key_from_props(secret_props(),
                                       cb_cluster_secrets:key_id(), list()) ->
          {ok, secret_props()} | {error, active_key | not_found}.
remove_historical_key_from_props(Props, KeyId, _) ->
    #{active_key := #{id := ActiveId}, hist_keys := HistKeys} = Props,
    case ActiveId of
        KeyId -> {error, active_key};
        _ ->
            NewHistKeys = lists:filter(fun(#{id := Id}) -> Id =/= KeyId end,
                                       HistKeys),
            case length(NewHistKeys) =:= length(HistKeys) of
                true -> {error, not_found};
                false -> {ok, Props#{hist_keys => NewHistKeys}}
            end
    end.

-spec test_props(secret_props(), binary(), list()) -> ok | {error, _}.
test_props(Props, ExtraAD, _) ->
    %% Only testing active kmip id (the one that is being set basically)
    PropsWithoutHistKeys = Props#{hist_keys => []},
    maybe
        {ok, Encrypted} ?= ensure_props_encrypted(PropsWithoutHistKeys,
                                                  ExtraAD, direct),
        ok ?= ensure_kmip_kek_on_disk(Encrypted, ExtraAD, true)
    else
        {error, [{_, Reason}]} -> {error, Reason};
        {error, _} = E -> E
    end.

-spec is_encrypted_by_secret_manager(secret_props(), list()) -> boolean().
is_encrypted_by_secret_manager(#{encrypt_with := nodeSecretManager}, _) ->
    true;
is_encrypted_by_secret_manager(#{}, _) ->
    false.

-spec get_active_key_id_from_props(secret_props(), list()) ->
          {ok, cb_cluster_secrets:key_id()}.
get_active_key_id_from_props(#{active_key := #{id := Id}}, _) ->
    {ok, Id}.

-spec get_all_key_ids_from_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
get_all_key_ids_from_props(#{active_key := AK, hist_keys := Keys}, _) ->
    lists:map(fun (#{id := Id}) -> Id end, [AK | Keys]).

-spec get_key_ids_that_encrypt_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
get_key_ids_that_encrypt_props(#{key_passphrase := KP}, _) ->
    case KP of
        #{encrypted_by := {_, KekId}} -> [KekId];
        #{encrypted_by := undefined} -> []
    end.

-spec get_secret_ids_that_encrypt_props(secret_props(), list()) ->
          [cb_cluster_secrets:secret_id()].
get_secret_ids_that_encrypt_props(#{key_passphrase := KP} = Props, _) ->
    L = case Props of
            #{encrypt_with := encryptionKey, encrypt_secret_id := Id} -> [Id];
            #{} -> []
        end ++
        case KP of
            #{encrypted_by := {Id, _}} -> [Id];
            #{encrypted_by := undefined} -> []
        end,
    lists:uniq(L).

-spec get_props_encryption_method(secret_props(), list()) ->
          cb_deks:encryption_method().
get_props_encryption_method(#{encrypt_with := encryptionKey,
                              encrypt_secret_id := Id}, _) ->
    {secret, Id};
get_props_encryption_method(#{encrypt_with := nodeSecretManager}, _) ->
    encryption_service.

-spec maybe_reencrypt_props(secret_props(),
                            cb_cluster_secrets:get_active_id_fun(),
                            binary(), list()) ->
          {ok, secret_props()} | no_change | {error, _}.
maybe_reencrypt_props(Props, GetActiveId, ExtraAD, _) ->
    Pass = maps:get(key_passphrase, Props),
    EncryptBy = maps:get(encrypt_with, Props, undefined),
    SecretId = maps:get(encrypt_secret_id, Props, undefined),

    case cb_cluster_secrets:maybe_reencrypt_data(
           Pass, ExtraAD, EncryptBy, SecretId, GetActiveId) of
        {ok, NewPass} -> {ok, Props#{key_passphrase => NewPass}};
        no_change -> no_change;
        {error, E} -> {error, E}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

ensure_kmip_kek_on_disk(#{active_key := ActiveKey,
                          hist_keys := OtherKeys,
                          key_passphrase := Pass} = Props,
                        ExtraAD,
                        TestOnly) ->
    {DecryptRes, KekId} =
        case Pass of
            #{type := sensitive, data := D, encrypted_by := undefined} ->
                {{ok, D}, undefined};
            #{type := encrypted, data := ED, encrypted_by := {_, KId}} ->
                R = encryption_service:decrypt_key(ED, ExtraAD, KId),
                {R, KId}
        end,
    case DecryptRes of
        {ok, PassData} ->
            Common = maps:with([host, port, req_timeout_ms, key_path,
                                cert_path, ca_selection,
                                encryption_approach], Props),
            Res = lists:map(
                    fun (#{id := Id, kmip_id := KmipId,
                           creation_time := CreationTime}) ->
                        Params = Common#{kmip_id => KmipId,
                                         key_passphrase => PassData},
                        {Id, encryption_service:store_kmip_key(
                               Id, Params, KekId, CreationTime, TestOnly)}
                    end, [ActiveKey | OtherKeys]),
            misc:many_to_one_result(Res);
        {error, _} = E ->
            E
    end.
ensure_props_encrypted(Props, ExtraAD, Snapshot) ->
    GetActiveId = cb_cluster_secrets:get_active_key_id(_, Snapshot),
    case maybe_reencrypt_props(Props, GetActiveId, ExtraAD, []) of
        {ok, Encrypted} -> {ok, Encrypted};
        no_change -> {ok, Props};
        {error, _} = Error -> Error
    end.