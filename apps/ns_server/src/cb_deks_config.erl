%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks_config).

-behaviour(cb_deks).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").

-export([get_encryption_method/3,
         update_deks/2,
         get_required_usage/1,
         get_deks_lifetime/2,
         get_deks_rotation_interval/2,
         get_drop_deks_timestamp/2,
         get_force_encryption_timestamp/2,
         get_dek_ids_in_use/2,
         initiate_drop_deks/3,
         synchronize_deks/2,
         fetch_chronicle_keys_in_txn/2]).

-spec get_encryption_method(cb_deks:dek_kind(), cluster | node,
                            cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, cb_deks:encryption_method()} | {error, not_found}.
get_encryption_method(_Kind, Scope, Snapshot) ->
    cb_crypto:get_encryption_method(config_encryption, Scope, Snapshot).

-spec update_deks(cb_deks:dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) -> ok | {error, _}.
update_deks(_Kind, _Snapshot) ->
    force_config_encryption_keys().

-spec get_required_usage(cb_deks:dek_kind()) -> cb_cluster_secrets:secret_usage().
get_required_usage(_Kind) ->
    config_encryption.

-spec get_deks_lifetime(cb_deks:dek_kind(),
                        cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_lifetime(_Kind, Snapshot) ->
    cb_crypto:get_dek_kind_lifetime(config_encryption, Snapshot).

-spec get_deks_rotation_interval(cb_deks:dek_kind(),
                                 cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_rotation_interval(_Kind, Snapshot) ->
    cb_crypto:get_dek_rotation_interval(config_encryption, Snapshot).

-spec get_drop_deks_timestamp(cb_deks:dek_kind(),
                              cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_drop_deks_timestamp(_Kind, Snapshot) ->
    cb_crypto:get_drop_keys_timestamp(config_encryption, Snapshot).

-spec get_force_encryption_timestamp(cb_deks:dek_kind(),
                                    cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_force_encryption_timestamp(_Kind, Snapshot) ->
    cb_crypto:get_force_encryption_timestamp(config_encryption, Snapshot).

-spec get_dek_ids_in_use(cb_deks:dek_kind(),
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, [cb_deks:dek_id()]} | {error, _}.
get_dek_ids_in_use(_Kind, _Snapshot) ->
    get_config_dek_ids_in_use().

-spec initiate_drop_deks(cb_deks:dek_kind(), [cb_deks:dek_id()],
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, done | started} | {error, not_found | retry | _}.
initiate_drop_deks(_Kind, DekIdsToDrop, _Snapshot) ->
    maybe
        ok ?= force_config_encryption_keys(),
        {ok, DekIdsInUse} ?= get_config_dek_ids_in_use(),
        StillInUse = [Id || Id <- DekIdsInUse, lists:member(Id, DekIdsToDrop)],
        case StillInUse of
            [] -> {ok, done};
            [_ | _] -> {error, {still_in_use, StillInUse}}
        end
    end.

-spec synchronize_deks(cb_deks:dek_kind(),
                      cb_cluster_secrets:chronicle_snapshot()) ->
          ok | {error, _}.
synchronize_deks(_Kind, _Snapshot) ->
    ok.

-spec fetch_chronicle_keys_in_txn(cb_deks:dek_kind(), Txn :: term()) ->
          cb_cluster_secrets:chronicle_snapshot().
fetch_chronicle_keys_in_txn(_Kind, Txn) ->
    chronicle_compat:txn_get_many([?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY], Txn).

force_config_encryption_keys() ->
    maybe
        %% How it works:
        %%  1. memcached_config_mgr pushes new keys to memcached and saves the
        %%     DekSnapshot in persistent_term memcached_native_encryption_deks.
        %%     This persistent_term determines DEKs that memcached knows about.
        %%     Only these DEKs can be used for encryption of files that are
        %%     to be read by memcached
        %%  2. memcached_config_mgr reloads memcached.json encrypted by the new
        %%     dek
        %%  3. password and permissions files get reencrypted on disk
        %%     (sync_reload) with the DekSnapshot taken from
        %%     memcached_native_encryption_deks
        %%  4. all historical keys in memcached_native_encryption_deks get
        %%     dropped, because old deks are not used anywhere
        ok ?= memcached_config_mgr:push_config_encryption_key(true),
        ok ?= memcached_passwords:sync_reload(),
        ok ?= memcached_permissions:sync_reload(),
        ok ?= ns_audit_cfg:maybe_apply_new_keys(),
        ok ?= memcached_config_mgr:drop_historical_deks(),
        ok ?= ns_config:resave(),
        ok ?= menelaus_users:apply_keys_and_resave(),
        ok ?= menelaus_local_auth:resave(),
        ok ?= simple_store:resave(?XDCR_CHECKPOINT_STORE),
        ok ?= chronicle_local:maybe_apply_new_keys(),
        ok ?= ns_ssl_services_setup:resave_encrypted_files(),
        ok ?= encryption_service:remove_old_integrity_tokens(
                [kek | cb_deks:dek_kinds_list_existing_on_node(direct)]),
        ok
    end.

get_config_dek_ids_in_use() ->
    maybe
        {ok, Ids1} ?= memcached_config_mgr:get_key_ids_in_use(),
        {ok, Ids2} ?= memcached_passwords:get_key_ids_in_use(),
        {ok, Ids3} ?= memcached_permissions:get_key_ids_in_use(),
        {ok, Ids4} ?= ns_config:get_key_ids_in_use(),
        {ok, Ids5} ?= menelaus_users:get_key_ids_in_use(),
        {ok, Ids6} ?= menelaus_local_auth:get_key_ids_in_use(),
        {ok, Ids7} ?= simple_store:get_key_ids_in_use(?XDCR_CHECKPOINT_STORE),
        {ok, Ids8} ?= chronicle_local:get_encryption_dek_ids(),
        {ok, Ids9} ?= ns_ssl_services_setup:get_key_ids_in_use(),
        {ok, Ids10} ?= encryption_service:get_key_ids_in_use(),
        {ok, Ids11} ?= ns_audit_cfg:get_key_ids_in_use(),
        {ok, lists:map(fun (undefined) -> ?NULL_DEK;
                           (Id) -> Id
                       end, lists:uniq(Ids1 ++ Ids2 ++ Ids3 ++ Ids4 ++ Ids5 ++
                                       Ids6 ++ Ids7 ++ Ids8 ++ Ids9 ++ Ids10 ++
                                       Ids11))}
    end.
