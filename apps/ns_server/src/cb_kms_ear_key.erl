%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_kms_ear_key).

-behavior(cb_cluster_secrets).

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

-export_type([key_props/0]).

-type secret_props() :: #{stored_ids := [key_props()],
                          last_rotation_time := calendar:datetime()}.

-type key_props() :: #{id := cb_cluster_secrets:key_id(),
                       creation_time := calendar:datetime()}.

-spec prepare_new_props(calendar:datetime(), map(), list()) -> secret_props().
prepare_new_props(CreationTime, PropsFromValidator, _) ->
    {ok, Key} = generate_key(CreationTime, []),
    PropsFromValidator#{stored_ids => [Key]}.

-spec modify_props(secret_props(), map(), list()) -> secret_props().
modify_props(OldProps, PropsFromValidator, _) ->
    #{stored_ids := StoredIds} = OldProps,
    LastRotationTime = maps:get(last_rotation_time, OldProps, undefined),
    set_last_rotation_time_in_props(
        PropsFromValidator#{stored_ids => StoredIds},
        LastRotationTime).

-spec sanitize_props(secret_props(), list()) -> secret_props().
sanitize_props(Props, _) ->
    Props.

-spec generate_key(calendar:datetime(), list()) -> {ok, key_props()}.
generate_key(CreationTime, _) ->
    {ok, #{id => cb_cluster_secrets:new_key_id(),
           creation_time => CreationTime}}.

-spec persist(secret_props(), binary(), list()) -> ok | {error, _}.
persist(Props, ExtraAD, [Submodule]) ->
    Submodule:persist(Props, ExtraAD, []).

-spec set_new_active_key_in_props(key_props(), secret_props(), list()) ->
          secret_props().
set_new_active_key_in_props(Key, #{stored_ids := StoredIds} = Props, _) ->
    Time = calendar:universal_time(),
    NewStoredIds = [Key | StoredIds],
    set_last_rotation_time_in_props(Props#{stored_ids => NewStoredIds}, Time).

-spec historical_keys_to_remove_from_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
historical_keys_to_remove_from_props(#{stored_ids := StoredIds}, _) ->
    [Id || #{id := Id} <- tl(StoredIds)].

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
    #{stored_ids := [#{id := ActiveId} | _] = StoredIds} = Props,
    case ActiveId of
        KeyId -> {error, active_key};
        _ ->
            NewStoredIds = lists:filter(fun(#{id := Id}) -> Id =/= KeyId end,
                                        StoredIds),
            case length(NewStoredIds) =:= length(StoredIds) of
                true -> {error, not_found};
                false -> {ok, Props#{stored_ids => NewStoredIds}}
            end
    end.

-spec test_props(secret_props(), binary(), list()) -> ok | {error, _}.
test_props(Props, ExtraAD, [Submodule]) ->
    Submodule:test_props(Props, ExtraAD, []).

-spec is_encrypted_by_secret_manager(secret_props(), list()) -> false.
is_encrypted_by_secret_manager(#{}, _) ->
    false.

-spec get_active_key_id_from_props(secret_props(), list()) ->
          {ok, cb_cluster_secrets:key_id()}.
get_active_key_id_from_props(#{stored_ids := [#{id := Id} | _]}, _) ->
    {ok, Id}.

-spec get_all_key_ids_from_props(secret_props(), list()) ->
          [cb_cluster_secrets:key_id()].
get_all_key_ids_from_props(#{stored_ids := StoredIds}, _) ->
    lists:map(fun (#{id := Id}) -> Id end, StoredIds).

-spec get_key_ids_that_encrypt_props(secret_props(), list()) -> [].
get_key_ids_that_encrypt_props(#{}, _) ->
    [].

-spec get_secret_ids_that_encrypt_props(secret_props(), list()) -> [].
get_secret_ids_that_encrypt_props(#{}, _) ->
    [].

-spec get_props_encryption_method(secret_props(), list()) -> disabled.
get_props_encryption_method(#{}, _) ->
    disabled.

-spec maybe_reencrypt_props(secret_props(),
                            cb_cluster_secrets:get_active_id_fun(),
                            binary(), list()) -> no_change.
maybe_reencrypt_props(_Props, _GetActiveId, _ExtraAD, _) ->
    no_change.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_last_rotation_time_in_props(Props, undefined) ->
    Props;
set_last_rotation_time_in_props(Props, CurUTCTime) ->
    Props#{last_rotation_time => CurUTCTime}.