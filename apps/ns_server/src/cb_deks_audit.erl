%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks_audit).

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
    cb_crypto:get_encryption_method(audit_encryption, Scope, Snapshot).

-spec update_deks(cb_deks:dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) -> ok | {error, _}.
update_deks(auditDek = Kind, _Snapshot) ->
    maybe
        {ok, DeksSnapshot} ?= cb_crypto:fetch_deks_snapshot(Kind),
        ok ?= cb_crypto:active_key_ok(DeksSnapshot),
        ns_memcached:set_active_dek("@audit", DeksSnapshot)
    end.

-spec get_required_usage(cb_deks:dek_kind()) ->
          cb_cluster_secrets:secret_usage().
get_required_usage(_Kind) ->
    audit_encryption.

-spec get_deks_lifetime(cb_deks:dek_kind(),
                        cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_lifetime(_Kind, Snapshot) ->
    cb_crypto:get_dek_kind_lifetime(audit_encryption, Snapshot).

-spec get_deks_rotation_interval(cb_deks:dek_kind(),
                                 cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_rotation_interval(_Kind, Snapshot) ->
      cb_crypto:get_dek_rotation_interval(audit_encryption, Snapshot).

-spec get_drop_deks_timestamp(cb_deks:dek_kind(),
                              cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_drop_deks_timestamp(_Kind, Snapshot) ->
      cb_crypto:get_drop_keys_timestamp(audit_encryption, Snapshot).

-spec get_force_encryption_timestamp(cb_deks:dek_kind(),
                                    cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_force_encryption_timestamp(_Kind, Snapshot) ->
    cb_crypto:get_force_encryption_timestamp(audit_encryption, Snapshot).

-spec get_dek_ids_in_use(cb_deks:dek_kind(),
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, [cb_deks:dek_id()]} | {error, _}.
get_dek_ids_in_use(_Kind, _Snapshot) ->
    ns_memcached:get_dek_ids_in_use("@audit").

-spec initiate_drop_deks(cb_deks:dek_kind(), [cb_deks:dek_id()],
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, done | started} | {error, not_found | retry | _}.
initiate_drop_deks(Kind, DekIdsToDrop, _Snapshot) ->
    cb_deks_log:try_drop_dek_work(
      fun() ->
              ns_memcached:prune_log_or_audit_encr_keys("@audit", DekIdsToDrop)
      end, Kind).

-spec synchronize_deks(cb_deks:dek_kind(),
                      cb_cluster_secrets:chronicle_snapshot()) ->
          ok | {error, _}.
%% Audit DEKs are not stored externally, so synchronization is not needed.
synchronize_deks(_Kind, _Snapshot) ->
    ok.

-spec fetch_chronicle_keys_in_txn(cb_deks:dek_kind(), Txn :: term()) ->
          cb_cluster_secrets:chronicle_snapshot().
fetch_chronicle_keys_in_txn(_Kind, Txn) ->
    chronicle_compat:txn_get_many([?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY], Txn).
