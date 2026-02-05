%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_managed_ear_key).

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

-type secret_props() ::
          #{can_be_cached := boolean(),
            auto_rotation := boolean(),
            rotation_interval_in_days := pos_integer(),
            next_rotation_time := calendar:datetime(),
            last_rotation_time := calendar:datetime(),
            active_key_id := cb_cluster_secrets:key_id(),
            keys := [kek_props()],
            encrypt_with := nodeSecretManager | encryptionKey,
            encrypt_secret_id := cb_cluster_secrets:secret_id() |
                                ?SECRET_ID_NOT_SET}.
-type kek_props() :: #{id := cb_cluster_secrets:key_id(),
                       creation_time := calendar:datetime(),
                       key_material := cb_cluster_secrets:sensitive_data()}.

-spec prepare_new_props(calendar:datetime(), map(), list()) -> secret_props().
prepare_new_props(CurrentTime, PropsFromValidator, _) ->
    %% Creating new cb_managed key
    {ok, #{id := KekId}} = {ok, KeyProps} = generate_key(CurrentTime, []),
    functools:chain(PropsFromValidator, [set_keys_in_props(_, [KeyProps]),
                                         set_active_key_in_props(_, KekId)]).

-spec modify_props(secret_props(), map(), list()) -> secret_props().
modify_props(OldProps, PropsFromValidator, _) ->
    #{active_key_id := OldActiveId, keys := Keys} = OldProps,
    LastRotationTime = maps:get(last_rotation_time, OldProps, undefined),
    functools:chain(
        PropsFromValidator,
        [set_keys_in_props(_, Keys),
         set_active_key_in_props(_, OldActiveId),
         set_last_rotation_time_in_props(_, LastRotationTime)]).

-spec sanitize_props(secret_props(), list()) -> secret_props().
sanitize_props(#{keys := Keys} = Props, _) ->
    NewKeys = lists:map(
                fun (#{key_material := K} = Key) ->
                    Sanitized = cb_cluster_secrets:sanitize_sensitive_data(K),
                    Key#{key_material => Sanitized}
                end, Keys),
    Props#{keys => NewKeys}.

-spec persist(secret_props(), binary(), list()) -> ok | {error, _}.
persist(Props, ExtraAD, _) ->
    ensure_cb_managed_keks_on_disk(Props, ExtraAD, false).

-spec generate_key(calendar:datetime(), list()) -> {ok, kek_props()}.
generate_key(CreationDateTime, _) ->
    Key = cb_cluster_secrets:generate_raw_key(?ENVELOP_CIPHER),
    {ok, #{id => cb_cluster_secrets:new_key_id(),
           creation_time => CreationDateTime,
           key_material => #{type => sensitive,
                             data => Key,
                             encrypted_by => undefined}}}.

-spec set_new_active_key_in_props(kek_props(), secret_props(), list()) ->
          secret_props().
set_new_active_key_in_props(#{id := KekId} = Key, #{keys := CurKeks} = Props, _) ->
    Time = calendar:universal_time(),
    functools:chain(
      Props,
      [set_keys_in_props(_, [Key | CurKeks]),
       set_active_key_in_props(_, KekId),
       set_last_rotation_time_in_props(_, Time)]).

-spec historical_keys_to_remove_from_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
historical_keys_to_remove_from_props(#{}, _) ->
    [].

-spec get_next_rotation_time_from_props(secret_props(), list()) ->
          calendar:datetime() | undefined.
get_next_rotation_time_from_props(#{auto_rotation := true,
                                    next_rotation_time := Next}, _) ->
    Next;
get_next_rotation_time_from_props(#{}, _) ->
    undefined.

-spec maybe_update_next_rotation_time_in_props(secret_props(),
                                                calendar:datetime(), list()) ->
          {ok, secret_props()} | no_change.
maybe_update_next_rotation_time_in_props(Props, CurTime, _) ->
    case Props of
        #{auto_rotation := true,
          next_rotation_time := NextTime,
          rotation_interval_in_days := IntervalInD} when CurTime >= NextTime ->
            NextTimeS = calendar:datetime_to_gregorian_seconds(NextTime),
            CurTimeS = calendar:datetime_to_gregorian_seconds(CurTime),
            IntervalS = IntervalInD * ?SECS_IN_DAY,
            %% How many intervals to skip
            %% This is needed for the case when the system was down for a long
            %% time, and it's been more than 1 rotation interval since last
            %% rotation. In other words, NewNextTime must be in future.
            N = (CurTimeS - NextTimeS) div IntervalS + 1,
            NewNextTimeS = NextTimeS + N * IntervalS,
            NewNextTime = calendar:gregorian_seconds_to_datetime(NewNextTimeS),
            {ok, Props#{next_rotation_time => NewNextTime}};
        #{} ->
            no_change
    end.

-spec remove_historical_key_from_props(secret_props(),
                                       cb_cluster_secrets:key_id(), list()) ->
          {ok, secret_props()} | {error, active_key | not_found}.
remove_historical_key_from_props(#{keys := Keys,
                                   active_key_id := ActiveId} = Props,
                                   KeyId, _) ->
    case ActiveId of
        KeyId -> {error, active_key};
        _ ->
            NewKeys = lists:filter(fun(#{id := Id}) -> Id =/= KeyId end, Keys),
            case length(NewKeys) =:= length(Keys) of
                true -> {error, not_found};
                false -> {ok, Props#{keys => NewKeys}}
            end
    end.

-spec test_props(secret_props(), binary(), list()) -> ok | {error, _}.
test_props(#{keys := Keys} = Props, ExtraAD, _) ->
    {ok, ActiveKeyId} = get_active_key_id_from_props(Props, []),
    FilteredKeys = lists:filter(fun (#{id := Id}) -> Id == ActiveKeyId end,
                                Keys),
    PropsWithoutHistKeys = Props#{keys => FilteredKeys},
    maybe
        {ok, Encrypted} ?= ensure_props_encrypted(PropsWithoutHistKeys,
                                                  ExtraAD, direct),
        ok ?= ensure_cb_managed_keks_on_disk(Encrypted, ExtraAD, true)
    else
        {error, [{_, Reason}]} -> {error, Reason};
        {error, _} = E -> E
    end.

-spec is_encrypted_by_secret_manager(secret_props(), list()) -> boolean().
is_encrypted_by_secret_manager(#{encrypt_with := nodeSecretManager}, _) ->
    true;
is_encrypted_by_secret_manager(#{keys := Keys}, _) ->
    lists:any(fun (#{key_material := #{encrypted_by := EB}}) ->
                  EB == undefined
              end, Keys);
is_encrypted_by_secret_manager(#{}, _) ->
    false.

-spec get_active_key_id_from_props(secret_props(), list()) ->
          {ok, cb_cluster_secrets:key_id()} | {error, _}.
get_active_key_id_from_props(#{active_key_id := Id}, _) ->
    {ok, Id}.

-spec get_all_key_ids_from_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
get_all_key_ids_from_props(#{keys := Keys}, _) ->
    lists:map(fun (#{id := Id}) -> Id end, Keys).

-spec get_key_ids_that_encrypt_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
get_key_ids_that_encrypt_props(#{keys := Keys}, _) ->
    lists:filtermap(fun (#{key_material := #{encrypted_by := {_, KekId}}}) ->
                            {true, KekId};
                        (#{key_material := #{encrypted_by := undefined}}) ->
                            false
                    end, Keys).

-spec get_secret_ids_that_encrypt_props(secret_props(), list()) ->
          [cb_cluster_secrets:secret_id()].
get_secret_ids_that_encrypt_props(#{keys := Keys} = Props, _) ->
    L = case Props of
            #{encrypt_with := encryptionKey, encrypt_secret_id := Id} -> [Id];
            #{} -> []
        end ++
        lists:filtermap(
          fun (#{key_material := #{encrypted_by := {Id, _}}}) -> {true, Id};
              (#{key_material := #{encrypted_by := undefined}}) -> false
          end, Keys),
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
maybe_reencrypt_props(#{keys := Keys} = Props, GetActiveId, ExtraAD, _) ->
    case maybe_reencrypt_keks(Keys, Props, GetActiveId, ExtraAD) of
        {ok, NewKeks} -> {ok, Props#{keys => NewKeks}};
        no_change -> no_change;
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_keys_in_props(Props, Keys) ->
    Props#{keys => Keys}.

set_active_key_in_props(Props, KeyId) ->
    Props#{active_key_id => KeyId}.

set_last_rotation_time_in_props(Props, undefined) ->
    Props;

set_last_rotation_time_in_props(Props, CurUTCTime) ->
    Props#{last_rotation_time => CurUTCTime}.

ensure_cb_managed_keks_on_disk(#{keys := Keys,
                                 can_be_cached := CanBeCached},
                                 ExtraAD, TestOnly) ->
    Res = lists:map(fun (#{id := Id} = K) ->
                        {Id, ensure_kek_on_disk(K, ExtraAD,
                                                CanBeCached, TestOnly)}
                    end, Keys),
    misc:many_to_one_result(Res).

ensure_kek_on_disk(#{id := Id,
                     key_material := #{type := sensitive, data := Key,
                                       encrypted_by := undefined},
                     creation_time := CreationTime},
                   _, CanBeCached, TestOnly) ->
    encryption_service:store_kek(Id, Key, undefined, CreationTime, CanBeCached,
                                 TestOnly);
ensure_kek_on_disk(#{id := Id,
                     key_material := #{type := encrypted, data := EncryptedKey,
                                       encrypted_by := {_ESecretId, EKekId}},
                     creation_time := CreationTime} = KeyProps, ExtraAD,
                   CanBeCached, TestOnly) ->
    AD = cb_managed_key_ad(ExtraAD, KeyProps),
    maybe
        {ok, Key} ?= encryption_service:decrypt_key(EncryptedKey, AD, EKekId),
        encryption_service:store_kek(Id, Key, EKekId, CreationTime,
                                     CanBeCached, TestOnly)
    end.

cb_managed_key_ad(ExtraAD, #{id := Id, creation_time := CT}) ->
    CTISO = iso8601:format(CT),
    iolist_to_binary([ExtraAD, Id, CTISO]).

maybe_reencrypt_keks(Keys, Props, GetActiveId, ExtraAD) ->
    try
        EncryptBy = maps:get(encrypt_with, Props, undefined),
        SecretId = maps:get(encrypt_secret_id, Props, undefined),

        RV = lists:mapfoldl(
               fun (#{key_material := KeyData} = Key,Acc) ->
                   AD = cb_managed_key_ad(ExtraAD, Key),
                   case cb_cluster_secrets:maybe_reencrypt_data(
                          KeyData, AD, EncryptBy, SecretId, GetActiveId) of
                       no_change ->
                           {Key, Acc};
                       {ok, NewKeyData} ->
                           {Key#{key_material => NewKeyData}, changed};
                       {error, _} = E ->
                           throw(E)
                   end
               end, no_change, Keys),
        case RV of
            {NewKeyList, changed} -> {ok, NewKeyList};
            {_, no_change} -> no_change
        end
    catch
        throw:{error, _} = Error -> Error
    end.

ensure_props_encrypted(Props, ExtraAD, Snapshot) ->
    GetActiveId = cb_cluster_secrets:get_active_key_id(_, Snapshot),
    case maybe_reencrypt_props(Props, GetActiveId, ExtraAD, []) of
        {ok, Encrypted} -> {ok, Encrypted};
        no_change -> {ok, Props};
        {error, _} = Error -> Error
    end.