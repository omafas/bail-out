%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_cluster_secrets).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("cb_cluster_secrets.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MASTER_MONITOR_NAME, {via, leader_registry, cb_cluster_secrets_master}).
-define(RETRY_TIME, ?get_param(retry_time, 10000)).
-define(SYNC_TIMEOUT, ?get_timeout(sync, 60000)).
-define(NODE_PROC, node_monitor_process).
-define(MASTER_PROC, master_monitor_process).
-define(DEK_COUNTERS_UPDATE_TIMEOUT, ?get_timeout(counters_update, 30000)).
-define(SYNCHRONIZE_DEKS_TIMEOUT, ?get_timeout(synchronize_deks, 60000)).
-define(REMOVE_HISTORICAL_KEYS_INTERVAL,
        ?get_param(remove_historical_keys_interval, 60*60*1000)).
-define(DEK_TIMER_RETRY_TIME_S, ?get_param(dek_retry_interval, 60)).
-define(DEK_DROP_RETRY_TIME_S(Kind),
        ?get_param({dek_removal_min_interval, Kind}, 60*60*3)).
-define(DROP_EXCESSIVE_DEKS, ?get_param(drop_excessive_deks, true)).

%% DEK GC interval is slightly less than DEK_INFO_UPDATE_INVERVAL_S to
%% ensure that DEK GC can be triggered before DEK info update (on average).
-define(MIN_DEK_GC_INTERVAL_S, ?get_param(min_dek_gc_interval, 590)).
-define(DEK_INFO_UPDATE_INVERVAL_S, ?get_param(dek_info_update_interval, 600)).
-define(DEK_COUNTERS_RENEW_INTERVAL_S,
        ?get_param(dek_counters_renew_interval, 60)).

-define(MIN_TIMER_INTERVAL, ?get_param(min_timer_interval, 30000)).
-define(TEST_SECRET_TIMEOUT, ?get_param(secret_test_timeout, 30000)).
-define(RPC_TIMEOUT, ?get_timeout(rpc_timeout, 5000)).

-ifndef(TEST).
-define(MAX_RECHECK_ROTATION_INTERVAL, ?get_param(rotation_recheck_interval,
                                                  ?SECS_IN_DAY*1000)).
-else.
-define(MAX_RECHECK_ROTATION_INTERVAL, ?SECS_IN_DAY*1000).
-endif.

-callback prepare_new_props(CreationTime :: calendar:datetime(),
                            ValidatedProps :: map(),
                            ExtraArgs :: list()) -> secret_props_data().
-callback modify_props(CurProps :: secret_props_data(),
                       ValidatedProps :: map(),
                       ExtraArgs :: list()) -> secret_props_data().
-callback sanitize_props(secret_props_data(),
                         ExtraArgs :: list()) -> secret_props_data().
-callback persist(secret_props_data(),
                  ExtraAD :: binary(), ExtraArgs :: list()) ->
    ok | {error, _}.
-callback generate_key(calendar:datetime(), ExtraArgs :: list()) ->
    {ok, AbstractKey :: term()} | {error, _}.
-callback set_new_active_key_in_props(AbstractKey :: term(),
                                      secret_props_data(),
                                      ExtraArgs :: list()) ->
    secret_props_data().
-callback historical_keys_to_remove_from_props(secret_props_data(),
                                               ExtraArgs :: list()) ->
    [key_id()].
-callback get_next_rotation_time_from_props(secret_props_data(),
                                            ExtraArgs :: list()) ->
    calendar:datetime() | undefined.
-callback maybe_update_next_rotation_time_in_props(
            secret_props_data(), CurTime :: calendar:datetime(),
            ExtraArgs :: list()) ->
    {ok, secret_props_data()} | no_change | {error, not_supported}.
-callback remove_historical_key_from_props(secret_props_data(),
                                           KeyId :: key_id(),
                                           ExtraArgs :: list()) ->
    {ok, secret_props_data()} | {error, _}.
-callback test_props(secret_props_data(), ExtraAD :: binary(),
                     ExtraArgs :: list()) ->
    ok | {error, _}.
-callback is_encrypted_by_secret_manager(secret_props_data(),
                                         ExtraArgs :: list()) -> boolean().
-callback get_active_key_id_from_props(secret_props_data(),
                                       ExtraArgs :: list()) ->
    {ok, key_id()} | {error, _}.
-callback get_all_key_ids_from_props(secret_props_data(),
                                     ExtraArgs :: list()) -> [key_id()].
-callback get_key_ids_that_encrypt_props(secret_props_data(),
                                         ExtraArgs :: list()) -> [key_id()].
-callback get_secret_ids_that_encrypt_props(secret_props_data(),
                                            ExtraArgs :: list()) ->
    [secret_id()].
-callback get_props_encryption_method(secret_props_data(),
                                      ExtraArgs :: list()) ->
    cb_deks:encryption_method().
-callback maybe_reencrypt_props(secret_props_data(),
                                get_active_id_fun(),
                                ExtraAD :: binary(),
                                ExtraArgs :: list()) ->
    {ok, secret_props_data()} | no_change | {error, _}.

-define(STALE_INFO_MARGIN_S, 30).

%% API
-export([start_link_node_monitor/0,
         start_link_master_monitor/0,
         add_new_secret/1,
         replace_secret/3,
         delete_secret/2,
         delete_historical_key/3,
         get_all/0,
         get_all/1,
         where_is_secret_used/2,
         maybe_renew_secrets_usage_info/0,
         get_secret/1,
         get_secret/2,
         get_active_key_id/1,
         get_active_key_id/2,
         rotate/1,
         test/2,
         test_secret_props/1,
         test_existing_secret/2,
         test_existing_secret_props/2,
         get_secret_by_kek_id_map/1,
         ensure_can_encrypt_dek_kind/3,
         is_allowed_usage_for_secret/3,
         is_encrypted_by_secret_manager/1,
         generate_raw_key/1,
         sync_with_all_node_monitors/0,
         new_key_id/0,
         is_valid_key_id/1,
         dek_drop_complete/2,
         is_name_unique/3,
         sanitize_chronicle_cfg/1,
         merge_dek_infos/2,
         format_dek_issues/1,
         chronicle_transaction/2,
         get_node_deks_info_quickly/0,
         destroy_deks/2,
         diag_info/0,
         reencrypt_deks/0,
         node_supports_encryption_at_rest/1,
         max_local_dek_num/1,
         fetch_snapshot_in_txn/1,
         recalculate_deks_info/0,
         is_secret_used/2,
         import_dek_files/3,
         sanitize_sensitive_data/1,
         maybe_reencrypt_data/5,
         get_latest_test_results/0,
         alert_keys_added_in_totoro/0,
         alert_keys_default/0,
         alert_keys_all/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2, terminate/2]).

%% Can be called by other nodes:
-export([add_new_secret_internal/1,
         replace_secret_internal/3,
         rotate_internal/1,
         test_internal/2,
         sync_with_node_monitor/0,
         delete_secret_internal/2,
         delete_historical_key_internal/3,
         get_node_deks_info/0,
         maybe_renew_secrets_usage_info_internal/0,
         synchronize_deks_local/1]).

-record(state, {proc_type :: ?NODE_PROC | ?MASTER_PROC,
                jobs :: [node_job()] | [master_job()],
                timers_trigger_ts = #{} :: #{atom() := integer()},
                timers = #{retry_jobs => undefined,
                           rotate_keks => undefined,
                           remove_historical_keys => undefined,
                           dek_cleanup => undefined,
                           rotate_deks => undefined,
                           dek_info_update => undefined,
                           remove_retired_keys => undefined,
                           test_secrets => undefined}
                         :: #{atom() := {reference(), integer()} | undefined},
                deks_info = undefined :: #{cb_deks:dek_kind() := deks_info()} |
                                         undefined,
                kek_hashes_on_disk = #{} :: #{secret_id() := integer()}}).

-export_type([secret_id/0, key_id/0, chronicle_snapshot/0, secret_usage/0,
              dek_encryption_counters/0, sensitive_data/0,
              get_active_id_fun/0]).

-type secret_props_data() :: cb_managed_ear_key:secret_props() |
                             cb_aws_kms_ear_key:secret_props() |
                             cb_gcp_kms_ear_key:secret_props() |
                             cb_azure_kms_ear_key:secret_props() |
                             cb_hashi_ear_key:secret_props() |
                             cb_kmip_ear_key:secret_props().
-type secret_props() ::
    #{id := secret_id(),
      name := string(),
      creation_time := calendar:datetime(),
      type := secret_type(),
      usage := [secret_usage()],
      data := secret_props_data()}.
-type secret_type() :: ?CB_MANAGED_KEY_TYPE | ?AWSKMS_KEY_TYPE |
                       ?GCPKMS_KEY_TYPE | ?AZUREKMS_KEY_TYPE |
                       ?HASHIKMS_KEY_TYPE | ?KMIP_KEY_TYPE.
-type secret_usage() :: {bucket_encryption, BucketUUID :: binary()} |
                         secrets_encryption | cb_crypto:encryption_type().
-type sensitive_data() :: #{type := sensitive | encrypted,
                            data := binary(),
                            encrypted_by := undefined |
                                            {secret_id(), key_id()}}.
-type secret_id() :: non_neg_integer().
-type key_id() :: uuid().
-type chronicle_snapshot() :: direct | map().
-type uuid() :: binary(). %% uuid as binary string
-type node_job() :: garbage_collect_keks |
                    ensure_all_keks_on_disk |
                    cleanup_alerts |
                    {dek_job(), cb_deks:dek_kind()}.

-type dek_job() :: maybe_update_deks | garbage_collect_deks |
                   maybe_reencrypt_deks | reread_bad_deks.

-type master_job() :: maybe_reencrypt_secrets | maybe_reset_deks_counters |
                      maybe_remove_historical_keys.

-type bad_encrypt_id() :: {encrypt_id, not_allowed | not_found}.
-type bad_usage_change() :: {usage, in_use}.
-type inconsistent_graph() :: {cycle, [secret_id()]} |
                              {unknown_id, secret_id()}.

-type secret_in_use() :: {used_by, used_by()}.

-type used_by() :: #{by_config := [cb_deks:dek_kind()],
                     by_secrets := [secret_id()],
                     by_deks := [cb_deks:dek_kind()]}.

-type deks_info() :: #{active_id := cb_deks:dek_id() | undefined,
                       deks := [cb_deks:dek()],
                       is_enabled := boolean(),
                       deks_being_dropped := sets:set(cb_deks:dek_id() |
                                                      ?NULL_DEK),
                       has_unencrypted_data := undefined | boolean(),
                       last_deks_gc_datetime := undefined | calendar:datetime(),
                       last_drop_timestamp := undefined | non_neg_integer(),
                       statuses := #{node_job() := ok | retry | {error, _}},
                       prev_deks_hash := integer() | undefined}.

-type external_dek_info() :: #{data_status := dek_info_data_status(),
                               issues := [dek_issue()],
                               deks => [cb_deks:dek_meta()],
                               dek_num => non_neg_integer(),
                               oldest_dek_datetime => calendar:datetime()}.

-type dek_info_data_status() :: encrypted | partially_encrypted |
                                unencrypted | unknown.

-type dek_issue() :: {dek_job() | proc_communication | node_info,
                      pending | failed}.

-type dek_encryption_counters() ::
          #{{secret, secret_id()} | encryption_service :=
            #{cb_deks:dek_kind() := {non_neg_integer(), Rev :: integer()}}}.

-type get_active_id_fun() :: fun((secret_id()) -> {ok, key_id()} | {error, _}).

%%%===================================================================
%%% API
%%%===================================================================

%% Starts on each cluster node
-spec start_link_node_monitor() -> {ok, pid()}.
start_link_node_monitor() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [?NODE_PROC], []).

%% Starts on the master node only
-spec start_link_master_monitor() -> {ok, pid()}.
start_link_master_monitor() ->
    misc:start_singleton(gen_server, start_link,
                         [?MASTER_MONITOR_NAME, ?MODULE, [?MASTER_PROC], []]).

-spec get_all() -> [secret_props()].
get_all() -> get_all(direct).

-spec get_all(chronicle_snapshot()) -> [secret_props()].
get_all(Snapshot) ->
    chronicle_compat:get(Snapshot, ?CHRONICLE_SECRETS_KEY, #{default => []}).

-spec where_is_secret_used(secret_id(), chronicle_snapshot()) -> used_by().
where_is_secret_used(Id, Snapshot) ->
    %% Places where this secret is used directly in encryption configuration
    EncryptionConfigUsages =
        lists:filtermap(
          fun (Kind) ->
               case cb_deks:call_dek_callback(get_encryption_method, Kind,
                                              [cluster, Snapshot]) of
                  {succ, {ok, {secret, Id}}} ->
                      {true, Kind};
                  {succ, {ok, _}} ->
                      false;
                  {succ, {error, not_found}} ->
                      false
              end
          end, cb_deks:dek_cluster_kinds_list(Snapshot)),
    %% Places where this secret is used for encryption of other secrets
    Secrets = get_secrets_used_by_secret_id(Id, Snapshot),
    %% Places where this secret is used to encrypt deks (such deks can exist
    %% even if encryption is disabled for this entity)
    Deks = get_dek_kinds_used_by_secret_id(Id, Snapshot),
    SecretNames =
        lists:map(fun (SId) ->
                      {ok, #{name := SName}} = get_secret(SId, Snapshot),
                      SName
                  end, Secrets),

    #{by_config => EncryptionConfigUsages,
      by_secrets => SecretNames,
      by_deks => Deks}.

-spec maybe_renew_secrets_usage_info() -> ok | {error, _}.
maybe_renew_secrets_usage_info() ->
    maybe
        true ?= should_renew_secrets_usage_info(),
        ok ?= execute_on_master(
                {?MODULE, maybe_renew_secrets_usage_info_internal, []}),
        %% Make sure we have received the updated DEKs counters
        ok ?= chronicle_kv:sync(kv, ?SYNC_TIMEOUT)
    else
        false -> ok;
        ok -> ok;
        {error, _} = Error ->
            ?log_error("Failed to renew secrets usage info: ~0p", [Error]),
            Error
    end.

-spec maybe_renew_secrets_usage_info_internal() -> ok.
maybe_renew_secrets_usage_info_internal() ->
    case should_renew_secrets_usage_info() of
        true -> maybe_reset_deks_counters();
        false -> ok
    end.

-spec get_secret(secret_id()) -> {ok, secret_props()} | {error, not_found}.
get_secret(SecretId) -> get_secret(SecretId, direct).

-spec get_secret(secret_id(), chronicle_snapshot()) ->
                                    {ok, secret_props()} | {error, not_found}.
get_secret(SecretId, Snapshot) when is_integer(SecretId) ->
    SearchFun = fun (#{id := Id}) -> SecretId == Id end,
    case lists:search(SearchFun, get_all(Snapshot)) of
        {value, Props} ->
            {ok, Props};
        false ->
            {error, not_found}
    end.

-spec add_new_secret(secret_props()) ->
          {ok, secret_props()} |
          {error, not_supported |
                  bad_encrypt_id() |
                  bad_usage_change() |
                  inconsistent_graph() |
                  encryption_service:stored_key_error() |
                  no_quorum}.
add_new_secret(Props) ->
    execute_on_master({?MODULE, add_new_secret_internal, [Props]}).

-spec add_new_secret_internal(secret_props()) ->
          {ok, secret_props()} |
          {error, not_supported |
                  bad_encrypt_id() |
                  bad_usage_change() |
                  inconsistent_graph() |
                  encryption_service:stored_key_error() |
                  no_quorum}.
add_new_secret_internal(Props) ->
    CurrentDateTime = erlang:universaltime(),
    PropsWTime = Props#{creation_time => CurrentDateTime},
    RV = chronicle_compat_txn(
           fun (Txn) ->
               Snapshot = fetch_snapshot_in_txn(Txn),
               CurList = get_all(Snapshot),
               NextId = chronicle_compat:get(Snapshot, ?CHRONICLE_NEXT_ID_KEY,
                                             #{default => 0}),
               PropsWId = PropsWTime#{id => NextId},
               maybe
                   Prepared = prepare_new_secret(PropsWId),
                   {ok, FinalProps} ?= ensure_secret_encrypted_txn(Prepared,
                                                                   Snapshot),
                   NewList = [FinalProps | CurList],
                   ok ?= validate_secret_in_txn(FinalProps, #{}, Snapshot),
                   ok ?= validate_secrets_consistency(NewList),
                   {commit, [{set, ?CHRONICLE_SECRETS_KEY, NewList},
                             {set, ?CHRONICLE_NEXT_ID_KEY, NextId + 1}],
                    FinalProps}
               else
                   {error, Reason} -> {abort, {error, Reason}}
               end
            end, #{read_consistency => quorum}),
    case RV of
        {ok, #{id := Id, name := Name} = Res} ->
            ResJson = menelaus_web_secrets:format_secret_props(Res),
            event_log:add_log(encryption_key_created,
                              [{encryption_key_id, Id},
                               {encryption_key_name, iolist_to_binary(Name)},
                               {settings, {ResJson}}]),
            sync_with_all_node_monitors(),
            {ok, Res};
        {error, _} = Error -> Error
    end.

-spec replace_secret(secret_id(), map(),
                     {M :: atom(), F :: atom(), A :: [term()]}) ->
          {ok, secret_props()} |
          {error, not_found | bad_encrypt_id() | bad_usage_change() |
                  forbidden | inconsistent_graph() |
                  encryption_service:stored_key_error() | bad_encrypt_id() |
                  no_quorum}.
replace_secret(Id, NewProps, IsSecretWritableFun) ->
    execute_on_master({?MODULE, replace_secret_internal,
                       [Id, NewProps, IsSecretWritableFun]}).

-spec replace_secret_internal(secret_id(), map(),
                              {M :: atom(), F :: atom(), A :: [term()]}) ->
          {ok, secret_props()} |
          {error, not_found | bad_encrypt_id() | bad_usage_change() |
                  forbidden | inconsistent_graph() |
                  encryption_service:stored_key_error() | bad_encrypt_id() |
                  no_quorum}.
replace_secret_internal(Id, NewProps, IsSecretWritableMFA) ->
    %% Make sure we have most recent information about which secrets are in use
    %% This is needed for verification of 'usage' modification
    maybe_reset_deks_counters(),
    Res =
        chronicle_compat_txn(
          fun (Txn) ->
              maybe
                  Snapshot = fetch_snapshot_in_txn(Txn),
                  {ok, OldProps} ?= get_secret(Id, Snapshot),
                  true ?= call_is_writable_mfa(IsSecretWritableMFA,
                                               [OldProps, Snapshot]),
                  Props = copy_static_props(OldProps, NewProps),
                  CurList = get_all(Snapshot),
                  {ok, FinalProps} ?= ensure_secret_encrypted_txn(Props,
                                                                  Snapshot),
                  NewList = replace_secret_in_list(FinalProps, CurList),
                  ok ?= validate_secret_in_txn(FinalProps, OldProps, Snapshot),
                  ok ?= validate_secrets_consistency(NewList),
                  {commit, [{set, ?CHRONICLE_SECRETS_KEY, NewList}],
                   {FinalProps, OldProps}}
              else
                  false ->
                      {abort, {error, forbidden}};
                  {error, _} = Err ->
                      {abort, Err}
              end
          end, #{read_consistency => quorum}),
    case Res of
        {ok, {#{name := Name} = ResProps, PrevProps}} ->
            ResPropsJson = menelaus_web_secrets:format_secret_props(ResProps),
            PrevPropsJson = menelaus_web_secrets:format_secret_props(PrevProps),
            event_log:add_log(encryption_key_changed,
                              [{encryption_key_id, Id},
                               {encryption_key_name, iolist_to_binary(Name)},
                               {old_settings, {PrevPropsJson}},
                               {new_settings, {ResPropsJson}}]),
            %% In order to make sure all keys are reencrypted by the time when
            %% the call is finished
            sync_with_all_node_monitors(),
            maybe_reencrypt_secrets(),
            {ok, ResProps};
        {error, _} = Error -> Error
    end.

-spec delete_secret(secret_id(), {M :: atom(), F :: atom(), A :: [term()]}) ->
          {ok, string()} | {error, not_found | secret_in_use() | forbidden |
                                   inconsistent_graph() | no_quorum}.
delete_secret(Id, IsSecretWritableFun) ->
    execute_on_master({?MODULE, delete_secret_internal,
                       [Id, IsSecretWritableFun]}).

-spec delete_secret_internal(secret_id(),
                             {M :: atom(), F :: atom(), A :: [term()]}) ->
          {ok, string()} | {error, not_found | secret_in_use() | forbidden |
                                   inconsistent_graph() | no_quorum}.
delete_secret_internal(Id, IsSecretWritableMFA) ->
    %% Make sure we have most recent information about which secrets are in use
    maybe_reset_deks_counters(),
    RV = chronicle_compat_txn(
           fun (Txn) ->
               maybe
                   Snapshot = fetch_snapshot_in_txn(Txn),
                   {ok, #{id := Id,
                          name := Name} = Props} ?= get_secret(Id, Snapshot),
                   true ?= call_is_writable_mfa(IsSecretWritableMFA,
                                                [Props, Snapshot]),
                   ok ?= can_delete_secret(Props, Snapshot),
                   CurSecrets = get_all(Snapshot),
                   NewSecrets = lists:filter(
                                  fun (#{id := Id2}) -> Id2 /= Id end,
                                  CurSecrets),
                   true = (length(NewSecrets) + 1 == length(CurSecrets)),
                   ok ?= validate_secrets_consistency(NewSecrets),
                   {commit, [{set, ?CHRONICLE_SECRETS_KEY, NewSecrets}], Name}
               else
                   false -> {abort, {error, forbidden}};
                   {error, _} = Error -> {abort, Error}
               end
           end),
    case RV of
        {ok, Name} ->
            event_log:add_log(encryption_key_deleted,
                              [{encryption_key_id, Id},
                               {encryption_key_name, iolist_to_binary(Name)}]),
            sync_with_all_node_monitors(),
            {ok, Name};
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_historical_key(secret_id(), key_id(),
                            {M :: atom(), F :: atom(), A :: [term()]}) ->
          {ok, string()} | {error, not_found | secret_in_use() | forbidden |
                                   inconsistent_graph() | no_quorum}.
delete_historical_key(SecretId, HistKeyId, IsSecretWritableFun) ->
    execute_on_master({?MODULE, delete_historical_key_internal,
                       [SecretId, HistKeyId, IsSecretWritableFun]}).

-spec delete_historical_key_internal(
        secret_id(), key_id(),
        {M :: atom(), F :: atom(), A :: [term()]}) ->
          {ok, string()} |
          {error, not_found | {unsafe, secret_in_use() | forbidden |
                                       inconsistent_graph() | no_quorum |
                                       active_key |
                                       {deks_sync_failed, [{node(), term()}]}}}.
delete_historical_key_internal(SecretId, HistKeyId, IsSecretWritableMFA) ->
    %% It is important to get the counters before we start dek info aggregation
    Snapshot = chronicle_compat:get_snapshot(
                 [fun fetch_snapshot_in_txn/1], #{}),
    {_, CountersRev} = get_dek_counters(Snapshot),
    maybe
        {ok, #{name := SName}} ?= get_secret(SecretId, Snapshot),
        {ok, AllNodesDekInfo} ?= get_all_node_deks_info(),
        IsSecretWritableFun = fun (Props, Snapshot2) ->
                                  call_is_writable_mfa(IsSecretWritableMFA,
                                                       [Props, Snapshot2])
                              end,
        ok ?= hd(delete_historical_keys_internal([{SecretId, SName, HistKeyId}],
                                                 IsSecretWritableFun,
                                                 AllNodesDekInfo,
                                                 CountersRev, Snapshot, false)),
        sync_with_all_node_monitors(),
        {ok, SName}
    else
        {error, not_found} ->
            {error, not_found};
        {error, Reason} ->
            {error, {unsafe, Reason}}
    end.

delete_historical_keys_internal(KeysToRemove, IsSecretWritableFun,
                                AllNodesDekInfo, CountersRev, Snapshot,
                                IsAutomatic) ->
    KindsToSync = cb_deks:dek_cluster_kinds_list(Snapshot),
    case synchronize_deks_on_all_nodes(KindsToSync) of
        ok ->
            lists:map(
                fun ({SecretId, SecretName, KeyId}) ->
                    delete_historical_key_without_sync(
                      SecretId, SecretName, KeyId, IsSecretWritableFun,
                      AllNodesDekInfo, CountersRev, IsAutomatic)
                end, KeysToRemove);
        {error, SyncError} ->
            lists:map(
              fun ({SecretId, SecretName, KeyId}) ->
                  log_unsucc_hist_key_removal(SecretId, SecretName, KeyId,
                                              SyncError, IsAutomatic),
                  {error, SyncError}
              end, KeysToRemove)
    end.

%% Delete historical key without calling synchronize_deks
delete_historical_key_without_sync(SecretId, SecretName, HistKeyId,
                                   IsSecretWritableFun, AllNodesDekInfo,
                                   CountersRev, IsAutomatic) ->
    maybe
        not_in_use ?= check_key_id_usage(HistKeyId, AllNodesDekInfo),
        ok ?= chronicle_compat_txn(
                fun (Txn) ->
                    Snapshot = fetch_snapshot_in_txn(Txn),
                    %% When we modify deks, we increment counters in
                    %% chronicle. If counter's revision has changed,
                    %% it means no changes were made to deks since
                    %% previous get_dek_counters() call, and our checks
                    %% against counters are still valid.
                    {_, NewCountersRev} = get_dek_counters(Snapshot),
                    case NewCountersRev == CountersRev of
                        true ->
                            delete_historical_key_txn(
                                SecretId,
                                HistKeyId,
                                IsSecretWritableFun,
                                Snapshot);
                        false ->
                            {abort, {error, retry}}
                    end
                end),
        event_log:add_log(
            historical_encryption_key_deleted,
            [{encryption_key_id, SecretId},
             {encryption_key_name, iolist_to_binary(SecretName)},
             {historical_key_UUID, HistKeyId}]),
        ?log_debug("Removed historical key ~p for secret ~p",
                   [HistKeyId, SecretId]),
        ok
    else
        {in_use, DekKinds} ->
            ?log_error("Failed to remove historical key ~p for secret ~p: "
                       "in use by DEK kinds ~p",
                       [HistKeyId, SecretId, DekKinds]),
            {error, {used_by, #{by_deks => DekKinds}}};
        {error, not_found} ->
            ?log_debug("Skipping historical key ~p for secret ~p "
                       "because it was not found", [HistKeyId, SecretId]),
            {error, not_found};
        {error, Reason} = Error ->
            ?log_error("Failed to remove historical key ~p for secret ~p: ~p",
                       [HistKeyId, SecretId, Reason]),
            log_unsucc_hist_key_removal(SecretId, SecretName, HistKeyId,
                                        Reason, IsAutomatic),
            Error
    end.

%% Cipher should have type crypto:cipher() but it is not exported
-spec generate_raw_key(Cipher :: atom()) -> binary().
generate_raw_key(Cipher) ->
    #{key_length := Length} = crypto:cipher_info(Cipher),
    crypto:strong_rand_bytes(Length).

-spec rotate(secret_id()) -> {ok, string()} |
                             {error, not_found | bad_encrypt_id() |
                                     inconsistent_graph() | not_supported |
                                     no_quorum}.
rotate(Id) ->
    execute_on_master({?MODULE, rotate_internal, [Id]}).

-spec rotate_internal(secret_id()) -> {ok, string()} |
                                      {error, not_found |
                                              bad_encrypt_id() |
                                              inconsistent_graph() |
                                              not_supported |
                                              no_quorum}.
rotate_internal(Id) ->
    case rotate_secret_by_id(Id, false) of
        {ok, Name} ->
            %% In order to make sure all keys are reencrypted by
            %% the time when the call is finished
            sync_with_all_node_monitors(),
            maybe_reencrypt_secrets(),
            {ok, Name};
        {error, Reason} ->
            {error, Reason}
    end.

-spec test(secret_props(), secret_props() | undefined) -> ok | {error, _}.
test(ParamsToTest, CurProps) ->
    execute_on_master({?MODULE, test_internal, [ParamsToTest, CurProps]}).

-spec test_internal(secret_props(), secret_props() | undefined) ->
          ok | {error, _}.
test_internal(Props, CurProps) ->
    PropsWTime = Props#{creation_time => erlang:universaltime()},
    PropsWId = PropsWTime#{id => 999999999},
    Prepared =
        case CurProps of
            undefined -> %% This is a test during key creation
                prepare_new_secret(PropsWId);
            _ -> %% This is a test during key editing
                copy_static_props(CurProps, Props)
        end,

    NodesToTest = nodes_with_encryption_at_rest(ns_node_disco:nodes_wanted()),
    Res = erpc:multicall(NodesToTest, ?MODULE, test_secret_props, [Prepared],
                         ?TEST_SECRET_TIMEOUT),
    handle_erpc_key_test_result(Res, NodesToTest).

%% This function can be called by other nodes. Those nodes can be older than
%% this node so this function should be backward compatible.
-spec test_secret_props(secret_props()) -> ok | {error, _}.
test_secret_props(#{type := T, id := SecretId, data := Data} = Props) ->
    ?log_debug("Testing ~p secret ~p", [T, SecretId]),
    call_module_by_type(T, test_props, [Data, secret_ad(Props)]).

test_existing_secret(SecretId, Nodes) ->
    case get_secret(SecretId, direct) of
        {ok, SecretProps} ->
            test_existing_secret_props(SecretProps, Nodes);
        {error, Error} ->
            {error, Error}
    end.

test_existing_secret_props(SecretProps, Nodes) ->
    case get_active_key_id_from_secret(SecretProps) of
        {ok, KeyId} ->
            Res = erpc:multicall(Nodes, encryption_service, test_existing_key,
                                 [KeyId], ?TEST_SECRET_TIMEOUT),
            handle_erpc_key_test_result(Res, Nodes);
        {error, Error} ->
            {error, Error}
    end.

-spec get_active_key_id(secret_id()) -> {ok, key_id()} |
                                        {error, not_found | not_supported}.
get_active_key_id(SecretId) ->
    get_active_key_id(SecretId, direct).

-spec get_active_key_id(secret_id(), chronicle_snapshot()) ->
                                            {ok, key_id()} |
                                            {error, not_found | not_supported}.
get_active_key_id(SecretId, Snapshot) ->
    maybe
        {ok, SecretProps} ?= get_secret(SecretId, Snapshot),
        {ok, _} ?= get_active_key_id_from_secret(SecretProps)
    else
        {error, _} = Err -> Err
    end.

-spec sync_with_node_monitor() -> ok.
sync_with_node_monitor() ->
    %% Mostly needed to make sure local cb_cluster_secret has pushed all new
    %% keys to disk before we try using them.
    %% chronicle_kv:sync() makes sure we have the latest chronicle data
    %% chronicle_compat_events:sync() makes sure all notifications has been sent
    %% sync([node()]) makes sure local cb_cluster_secret has handled that
    %% notification
    ok = chronicle_kv:sync(kv, ?SYNC_TIMEOUT),
    chronicle_compat_events:sync(),
    gen_server:call(?MODULE, sync, ?SYNC_TIMEOUT).

-spec ensure_can_encrypt_dek_kind(secret_id(), cb_deks:dek_kind(),
                                  chronicle_snapshot()) ->
          ok | {error, not_allowed | not_found}.
ensure_can_encrypt_dek_kind(SecretId, DekKind, Snapshot) ->
    maybe
        {ok, SecretProps} ?= get_secret(SecretId, Snapshot),
        true ?= can_secret_props_encrypt_dek_kind(SecretProps, DekKind),
        ok
    else
        false -> {error, not_allowed};
        {error, not_found} -> {error, not_found}
    end.

-spec is_allowed_usage_for_secret(secret_id(), secret_usage(),
                                  chronicle_snapshot()) ->
          ok | {error, not_allowed | not_found}.
is_allowed_usage_for_secret(SecretId, Usage, Snapshot) ->
    maybe
        {ok, #{usage := AllowedUsages}} ?= get_secret(SecretId, Snapshot),
        true ?= is_allowed([Usage], AllowedUsages),
        ok
    else
        false -> {error, not_allowed};
        {error, not_found} -> {error, not_found}
    end.

-spec is_encrypted_by_secret_manager(secret_props()) -> boolean().
is_encrypted_by_secret_manager(#{type := T, data := Data}) ->
    call_module_by_type(T, is_encrypted_by_secret_manager, [Data]).

-spec get_secret_by_kek_id_map(chronicle_snapshot()) ->
                                                    #{key_id() := secret_id()}.
get_secret_by_kek_id_map(Snapshot) ->
    maps:from_list(lists:flatmap(
                     fun (#{id := Id} = S) ->
                         [{KeyId, Id} ||  KeyId <- get_all_keys_from_props(S)]
                     end, get_all(Snapshot))).

-spec get_node_deks_info_quickly() ->
          #{cb_deks:dek_kind() := external_dek_info()}.
get_node_deks_info_quickly() ->
    %% Using ets here to avoid calling gen_server:call() and make sure
    %% ns_heart can get the latest status as quick as possible.
    %% This gen_server can be busy with other stuff, especially in situations
    %% like quorum loss (chronicle transactions time out in this case).
    %% We don't want ns_heart to wait multiple seconds in such cases
    try ets:lookup(?MODULE, deks_info) of
        [] ->
            dummy_deks_info(unknown, [{proc_communication, pending}]);
        [{_, {Timestamp, Info}}] ->
            Now = erlang:monotonic_time(second),
            Deadline = Timestamp + ?DEK_INFO_UPDATE_INVERVAL_S +
                       ?STALE_INFO_MARGIN_S, %% We need some margin because
                                             %% the info collection takes
                                             %% some time.
            case Deadline >= Now of
                true ->
                    Info;
                false ->
                    dummy_deks_info(unknown, [{proc_communication, pending}])
            end
    catch
        error:badarg ->
            %% ets table is not created yet, or the process is restarting
            dummy_deks_info(unknown, [{proc_communication, pending}])
    end.

-spec get_node_deks_info() -> #{cb_deks:dek_kind() := external_dek_info()}.
get_node_deks_info() ->
    try
        gen_server:call(?MODULE, get_node_deks_info,
                        ?DEK_COUNTERS_UPDATE_TIMEOUT)
    catch
        _:E ->
            case cluster_compat_mode:is_enterprise() of
                true ->
                    ?log_error("Failed to get node deks_info: ~p", [E]),
                    dummy_deks_info(unknown, [{proc_communication, failed}]);
                false ->
                    dummy_deks_info(unencrypted, [])
            end
    end.

-spec recalculate_deks_info() -> ok.
recalculate_deks_info() ->
    ?MODULE ! calculate_dek_info,
    ok.

-spec new_key_id() -> key_id().
new_key_id() ->
    Id = misc:uuid_v4(),
    true = is_valid_key_id(Id),
    Id.

-spec is_valid_key_id(binary()) -> boolean().
is_valid_key_id(Bin) -> misc:is_valid_v4uuid(Bin).

%% This function is called when the DEK drop is complete by one of the entities
%% that uses DEKs (the drop can still be in progress for other entities).
-spec dek_drop_complete(cb_deks:dek_kind(), ok | {error, any()}) -> ok.
dek_drop_complete(DekKind, Rv) ->
    ?MODULE ! {dek_drop_complete, DekKind, Rv},
    ok.

-spec is_name_unique(secret_id(), string(), chronicle_snapshot()) -> boolean().
is_name_unique(Id, Name, Snapshot) ->
    lists:all(fun (#{id := Id2}) when Id == Id2 -> true;
                  (#{name := Name2}) -> Name /= Name2
              end, get_all(Snapshot)).

-spec sanitize_chronicle_cfg([secret_props()]) -> [term()].
sanitize_chronicle_cfg(Value) ->
    lists:map(fun sanitize_secret/1, Value).

-spec merge_dek_infos(external_dek_info(), external_dek_info()) ->
          external_dek_info().
merge_dek_infos(M1, M2) ->
    MergeIssues = fun (Issues1, Issues2) ->
                      maps:to_list(
                        maps:merge_with(fun (_, failed, _) -> failed;
                                            (_, _, failed) -> failed;
                                            (_, pending, pending) -> pending
                                        end,
                                        maps:from_list(Issues1),
                                        maps:from_list(Issues2)))
                  end,
    maps:merge_with(
      fun (data_status, unknown, _) -> unknown;
          (data_status, _, unknown) -> unknown;
          (data_status, encrypted, encrypted) -> encrypted;
          (data_status, unencrypted, unencrypted) -> unencrypted;
          (data_status, _, _) -> partially_encrypted;
          (issues, A, B) -> MergeIssues(A, B);
          (dek_num, A, B) -> A + B;
          (deks, A, B) -> A ++ B;
          (oldest_dek_datetime, A, B) -> min(A, B)
      end, M1, M2).

-spec format_dek_issues([dek_issue()]) -> [binary()].
format_dek_issues(List) ->
    lists:map(fun ({maybe_update_deks, pending}) ->
                      <<"keys update pending">>;
                  ({maybe_update_deks, failed}) ->
                      <<"keys update failed">>;
                  ({reread_bad_deks, pending}) ->
                      <<"keys read pending">>;
                  ({reread_bad_deks, failed}) ->
                      <<"keys read failed">>;
                  ({garbage_collect_deks, pending}) ->
                      <<"keys garbage collection pending">>;
                  ({garbage_collect_deks, failed}) ->
                      <<"keys garbage collection failed">>;
                  ({maybe_reencrypt_deks, pending}) ->
                      <<"keys reencryption pending">>;
                  ({maybe_reencrypt_deks, failed}) ->
                      <<"keys reencryption failed">>;
                  ({node_info, pending}) ->
                      <<"information missing for some nodes">>;
                  ({proc_communication, pending}) ->
                      <<"encryption information not available yet">>;
                  ({proc_communication, failed}) ->
                      <<"encryption manager does not respond">>
              end, List).

-spec destroy_deks(cb_deks:dek_kind(), fun()) -> ok.
destroy_deks(DekKind, ContFun) ->
    case gen_server:call(?MODULE, {destroy_deks, DekKind, ContFun}, 300000) of
        {res, Res} ->
            Res;
        {exception, {C, E, ST}} ->
            erlang:raise(C, E, ST)
    end.

-spec diag_info() -> iolist().
diag_info() ->
    [diag_info_helper("cb_cluster_secrets node", whereis(cb_cluster_secrets)),
     <<"\n\n">>,
     case leader_registry:whereis_name(cb_cluster_secrets_master) of
         undefined ->
             "cb_cluster_secrets master process is not running";
         Pid when node() == node(Pid) ->
             diag_info_helper("cb_cluster_secrets master", Pid);
         Pid ->
             io_lib:format("cb_cluster_secrets master process is running on ~p",
                           [node(Pid)])
     end,
     <<"\n">>].

%% Can be called only when cb_cluster_secrets is not running!
-spec reencrypt_deks() -> ok | {error, [term()]}.
reencrypt_deks() ->
    %% This check doesn't guarantee that cb_cluster_secrets is not running
    %% during execution of init_deks(), but it should help catching the obvious
    %% cases, when this function is called with cb_cluster_secrets running in
    %% parallel.
    case whereis(?MODULE) of
        undefined ->
            {_KekPushHashes, _Deks, Errors} = init_deks(),
            case Errors of
                [] -> ok;
                _ -> {error, Errors}
            end;
        _ ->
            {error, cb_cluster_secrets_is_running}
    end.

-spec node_supports_encryption_at_rest([{atom(), term()}]) ->
          boolean() | no_info.
node_supports_encryption_at_rest(NodeInfo) ->
    case proplists:get_value(supported_compat_version, NodeInfo) of
        undefined ->
            no_info;
        SupportedVersion ->
            cluster_compat_mode:is_version_79(SupportedVersion)
    end.

nodes_with_encryption_at_rest(Nodes) ->
    lists:filter(fun (N) ->
                     NodeInfo = ns_doctor:get_node(N),
                     case node_supports_encryption_at_rest(NodeInfo) of
                         no_info -> true;
                         true -> true;
                         false -> false
                     end
                 end, Nodes).

-spec max_local_dek_num(cb_deks:dek_kind()) -> pos_integer().
max_local_dek_num(Kind) ->
    Default = case Kind of
                  {bucketDek, _} -> ?get_param({max_dek_num, bucketDek}, 50);
                  _ -> 50
              end,
    ?get_param({max_dek_num, Kind}, Default).

-spec max_total_dek_num(cb_deks:dek_kind()) -> pos_integer().
max_total_dek_num(Kind) ->
    Default =
        case Kind of
            {bucketDek, _} -> ?get_param({max_total_dek_num, bucketDek}, 1000);
            _ -> 1000
        end,
    ?get_param({max_total_dek_num, Kind}, Default).

-spec is_secret_used(secret_id(), chronicle_snapshot()) -> boolean().
is_secret_used(Id, Snapshot) ->
    case get_secret(Id, Snapshot) of
        {ok, SecretProps} ->
            case can_delete_secret(SecretProps, Snapshot) of
                ok -> false;
                {error, {used_by, _}} -> true
            end;
        {error, not_found} ->
            false
    end.

-spec import_dek_files(cb_deks:dek_kind(), [file:filename()], timeout()) ->
          ok | {error, term()}.
import_dek_files(DekKind, Paths, Timeout) ->
    gen_server:call(?MODULE, {import_dek_files, DekKind, Paths}, Timeout).

-spec sanitize_sensitive_data(sensitive_data()) -> sensitive_data().
sanitize_sensitive_data(#{type := sensitive} = Data) ->
    Data#{data => chronicle_kv_log:masked()};
sanitize_sensitive_data(#{type := encrypted} = Data) ->
    Data.

-spec get_latest_test_results() ->
          #{secret_id() := #{status := ok | {error, term()},
                             datetime := undefined | calendar:datetime()}}.
get_latest_test_results() ->
    try ets:lookup(?MODULE, secrets_test_results) of
        [] ->
            #{};
        [{_, {InfoDateTime, Info}}] ->
            Now = calendar:universal_time(),
            TestInterval = get_secrets_test_interval_s(),
            Deadline = misc:datetime_add(InfoDateTime,
                                         TestInterval + ?STALE_INFO_MARGIN_S),
            case Deadline >= Now of
                true ->
                    maps:map(fun (_Id, {_Name, TestResult}) ->
                                #{status => TestResult,
                                datetime => InfoDateTime}
                            end, Info);
                false ->
                    #{}
            end
    catch
        error:badarg -> #{}
    end.

%% This should only be updated with additions/removals when removing the
%% associated config upgrade function from menelaus_alert.
%% See the comment attached to menelaus_alert:alert_keys_default/0 for more info
-spec alert_keys_default() -> [atom()].
alert_keys_default() ->
    [].

%% These keys should be moved to alert_keys_default/0 when totoro is the lowest
%% supported release
-spec alert_keys_added_in_totoro() -> [atom()].
alert_keys_added_in_totoro () ->
    [encr_at_rest_key_test_failed].

%% Returns a list of all alerts that might send out an email notification.
-spec alert_keys_all() -> [atom()].
alert_keys_all() ->
    alert_keys_default() ++ alert_keys_added_in_totoro().

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Type]) ->
    ?log_debug("cb_cluster_secrets init ~p started", [Type]),
    Self = self(),
    EventFilter =
        fun (?CHRONICLE_SECRETS_KEY = K) -> {true, {config_change, K}};
            (secrets_test_interval_s = K) -> {true, {config_change, K}};
            (Key) ->
                case Type == ?NODE_PROC andalso
                     cb_deks:dek_chronicle_keys_filter(Key) of
                    false -> false;
                    ignore -> false;
                    {dek_settings_updated, _} = R -> {true, R};
                    check_for_deleted_keys = R -> {true, R}
                end
        end,
    chronicle_compat_events:subscribe(
      fun (Key) ->
          case EventFilter(Key) of
              false -> ok;
              {true, M} -> Self ! M
          end
      end),
    Jobs = case Type of
               ?MASTER_PROC ->
                   [maybe_reencrypt_secrets,
                    maybe_reset_deks_counters,
                    maybe_remove_historical_keys];
               ?NODE_PROC ->
                    ets:new(?MODULE, [named_table, protected, set,
                                      {keypos, 1}]),
                    Kinds = cb_deks:dek_cluster_kinds_list(),
                    lists:flatmap(fun (K) ->
                                      [{reread_bad_deks, K},
                                       {maybe_update_deks, K},
                                       {garbage_collect_deks, K}]
                                  end, Kinds) ++
                   [garbage_collect_keks,
                    ensure_all_keks_on_disk,
                    cleanup_alerts] ++
                   [{maybe_reencrypt_deks, K} || K <- Kinds]
           end,

    %% If we are starting after joininig the cluster (or after leaving cluster),
    %% it means that secrets configuration in chronicle has magically changed
    %% while this process was down.
    %% For example, KEKs on disk do not exist in configuration any more,
    %% and they will be garbage collected by the garbage_collect_keks job.
    %% This can be a problem because there could be DEKs that are encrypted with
    %% those KEKs. In order to smoothly migrate from old configuration, it is
    %% important to run reencryption of DEKs before garbage collecting KEKs.
    %% It is also important to not garbage collect KEKs until reencryption of
    %% DEKs finishes successfully.
    %% Note that this means that this process can't start until the reecnryption
    %% is finished.

    {ok, maybe_read_deks(#state{proc_type = Type, jobs = Jobs}),
     {continue, init}}.

handle_continue(init, State) ->
    NewState = functools:chain(State,
                               [run_jobs(_),
                                restart_dek_rotation_timer(_),
                                restart_dek_cleanup_timer(_),
                                restart_rotation_timer(_),
                                restart_dek_info_update_timer(true, _),
                                restart_remove_retired_timer(_),
                                restart_test_secrets_timer(true, _)]),
    ?log_debug("cb_cluster_secrets init ~p finished", [State#state.proc_type]),
    {noreply, NewState}.

handle_call({call, {M, F, A} = MFA}, _From,
            #state{proc_type = ?MASTER_PROC} = State) ->
    try
        ?log_debug("Calling ~p", [MFA]),
        {reply, {succ, erlang:apply(M, F, A)}, restart_rotation_timer(State)}
    catch
        C:E:ST ->
            ?log_warning("Call ~p failed: ~p:~p~n~p", [MFA, C, E, ST]),
            {reply, {exception, {C, E, ST}}, restart_rotation_timer(State)}
    end;

handle_call(sync, _From, #state{proc_type = ?NODE_PROC} = State) ->
    {reply, ok, State};

handle_call(get_node_deks_info, _From,
            #state{proc_type = ?NODE_PROC} = State) ->
    {Res, NewState} = calculate_dek_info(State),
    {reply, Res, restart_dek_info_update_timer(false, NewState)};

handle_call({destroy_deks, DekKind, ContFun}, _From,
            #state{proc_type = ?NODE_PROC, deks_info = Deks} = State) ->
    Continuation = fun () ->
                      try
                          {res, ContFun()}
                      catch
                          C:E:ST ->
                              ?log_error("Continuation failed: ~p:~p~n~p",
                                         [C, E, ST]),
                              {exception, {C, E, ST}}
                      end
                   end,
    case maps:find(DekKind, Deks) of
        {ok, _} ->
            Res = Continuation(),
            case Res of
                {res, ok} ->
                    ?log_info("DEK ~p destroy requested", [DekKind]),
                    {reply, Res,
                     add_and_run_jobs_async([{maybe_update_deks, DekKind}],
                                            destroy_dek_info(DekKind, State))};
                {R, _} when R == res; R == exception ->
                    ?log_error("DEK ~p destroy ignored: continuation error",
                               [DekKind]),
                    {reply, Res, State}
            end;
        error ->
            ?log_debug("DEK ~p destroy ignored (does not exist)", [DekKind]),
            {reply, Continuation(), State}
    end;

handle_call(diag, _From, State) ->
    {reply, diag(State), State};

handle_call({import_dek_files, DekKind, Paths}, _From,
            #state{proc_type = ?NODE_PROC} = State) ->
    ?log_info("~p DEK files import started:~n~p", [DekKind, Paths]),
    {Res, NewState} = import_dek_files_impl(DekKind, Paths, State),
    ?log_info("~p DEK files import finished: ~0p", [DekKind, Res]),
    {reply, Res, NewState};

handle_call(Request, _From, State) ->
    ?log_warning("Unhandled call: ~p", [Request]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?log_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info({config_change, secrets_test_interval_s} = Msg,
            #state{proc_type = ?NODE_PROC} = State) ->
    ?log_debug("Secrets test interval has changed..."),
    misc:flush(Msg),
    NewState =
        case get_secrets_test_interval_s() of
            0 -> stop_timer(test_secrets, State);
            T -> apply_timer_interval(test_secrets, T * 1000, State)
        end,
    {noreply, NewState};

handle_info({config_change, ?CHRONICLE_SECRETS_KEY} = Msg,
            #state{proc_type = ?NODE_PROC, deks_info = Deks} = State) ->
    ?log_debug("Secrets in chronicle have changed..."),
    misc:flush(Msg),
    Kinds = maps:keys(Deks),
    NewJobs = [garbage_collect_keks,    %% Removal of cb_managed keks and AWS
               ensure_all_keks_on_disk, %% Adding keks + AWS key change
               cleanup_alerts] ++       %% Remove alerts for removed secrets
              [{maybe_reencrypt_deks, K} || K <- Kinds], %% Keks rotation
    {noreply, add_and_run_jobs(NewJobs, State)};

handle_info({config_change, ?CHRONICLE_SECRETS_KEY} = Msg,
            #state{proc_type = ?MASTER_PROC} = State) ->
    ?log_debug("Secrets in chronicle have changed..."),
    misc:flush(Msg),
    NewJobs = [maybe_reencrypt_secrets,       %% Modififcation of encryptWith or
               maybe_remove_historical_keys], %% rotation of secret that
                                              %% encrypts other secrets
    {noreply, add_and_run_jobs(NewJobs, State)};

handle_info({config_change, _}, State) ->
    {noreply, State};

handle_info({dek_settings_updated, KindList} = Msg,
            #state{proc_type = ?NODE_PROC} = State) ->
    ?log_debug("Dek settings updated for ~p", [KindList]),
    misc:flush(Msg),
    NewState = functools:chain(
                 State,
                 [add_and_run_jobs(
                    [{maybe_update_deks, Kind} || Kind <- KindList] ++
                    [{maybe_reencrypt_deks, Kind} || Kind <- KindList],
                    _),
                  %% We should restart these timers because rotation settings
                  %% can change
                  restart_dek_rotation_timer(_),
                  restart_dek_cleanup_timer(_)]),
    {noreply, NewState};

handle_info(check_for_deleted_keys, #state{proc_type = ?NODE_PROC,
                                           deks_info = Deks} = State) ->
    ?log_debug("Checking for deleted keys"),
    ExistingKinds = maps:keys(Deks),
    LatestKinds = cb_deks:dek_cluster_kinds_list(),
    ToCheck = ExistingKinds -- LatestKinds,
    case ToCheck of
        [] -> {noreply, State};
        _ -> handle_info({dek_settings_updated, ToCheck}, State)
    end;

handle_info(run_jobs, #state{proc_type = ProcType} = State) ->
    ?log_debug("[~p] Running jobs", [ProcType]),
    misc:flush(run_jobs),
    {noreply, run_jobs(State)};

handle_info({timer, Name}, #state{proc_type = ProcType,
                                  timers_trigger_ts = TriggerTs,
                                  timers = Timers} = State) ->
    misc:flush({timer, Name}),
    ?log_debug("[~p] Handling timer ~p", [ProcType, Name]),
    CurTs = erlang:monotonic_time(millisecond),
    NewState = State#state{timers_trigger_ts = TriggerTs#{Name => CurTs},
                           timers = Timers#{Name => undefined}},
    {noreply, handle_timer(Name, NewState)};

handle_info({dek_drop_complete, Kind, Rv} = Msg,
            #state{proc_type = ?NODE_PROC} = State) ->
    case Rv of
        ok ->
            ?log_debug("Dek drop complete: ~p", [Kind]);
        {error, E} ->
            %% We log warning but still proceed with garbage collection because
            %% it is possible some DEKs may have been freed even if there
            %% were some errors
            ?log_warning("Dek drop for kind (~p) complete with error: ~p",
                         [Kind, E])
    end,
    misc:flush(Msg),
    self() ! calculate_dek_info,
    %% Restart dek cleanup timer to check if we need to drop excessive DEKs
    %% In case if this is a finish of null DEK drop, we might end up with no
    %% real changes in deks, and hence no restart of dek cleanup timer is called
    {noreply, restart_dek_cleanup_timer(
                add_and_run_jobs([{garbage_collect_deks, Kind}], State))};

handle_info(calculate_dek_info, #state{proc_type = ?NODE_PROC} = State) ->
    ?log_debug("DEK info update"),
    {_Res, NewState} = calculate_dek_info(State),
    {noreply, restart_dek_info_update_timer(false, NewState)};

handle_info(Info, State) ->
    ?log_warning("Unhandled info: ~p", [Info]),
    {noreply, State}.

handle_timer(retry_jobs, #state{} = State) ->
    run_jobs(State);

handle_timer(rotate_keks, #state{proc_type = ?MASTER_PROC} = State) ->
    CurTime = calendar:universal_time(),
    %% Intentionally update next_rotation time first, and run rotations after.
    %% Reason: in case of a crash during rotation we don't want to retry.
    %% Rotation generates keys. If we get stuck in a loop, we can generate
    %% too many keys which can lead to unpredictable results.
    case update_secrets(update_next_rotation_time(CurTime, _)) of
        {ok, IdsToRotate} ->
            lists:foreach(
              fun (Id) ->
                  try
                      {ok, _Name} = rotate_secret_by_id(Id, true)
                  catch
                      C:E:ST ->
                          ?log_error("Secret #~p rotation crashed: ~p:~p~n~p",
                                     [Id, C, E, ST])
                  end
              end, IdsToRotate);
        {error, _} ->
            %% we will retry
            ok
    end,
    restart_rotation_timer(State);

handle_timer(dek_cleanup, #state{proc_type = ?NODE_PROC,
                                 deks_info = DeksInfo} = State) ->
    DeksToDropFun =
        fun (Kind, StateAcc) ->
            case deks_to_drop(Kind, StateAcc) of
                [] -> {[], StateAcc};
                [_|_] ->
                    %% It is possible that these deks are already not being used
                    %% so try garbage collecting them first (as it is a
                    %% cheaper thing to do), and only if it doesn't help,
                    %% perform the drop keys precedure (which is expensive for
                    %% buckets)
                    Snapshot = deks_config_snapshot(Kind),
                    NewStateAcc = maybe_garbage_collect_deks(Kind, false,
                                                             Snapshot,
                                                             StateAcc),
                    {deks_to_drop(Kind, NewStateAcc), NewStateAcc}
            end
        end,
    NewState =
        maps:fold(
          fun (Kind, _KindDeks, StateAcc) ->
              {ToDrop, NewStateAcc} = DeksToDropFun(Kind, StateAcc),
              initiate_deks_drop(Kind, ToDrop, NewStateAcc)
          end, State, DeksInfo),
    restart_dek_cleanup_timer(NewState);

handle_timer(rotate_deks, #state{proc_type = ?NODE_PROC,
                                 deks_info = Deks} = State) ->
    CurDT = calendar:universal_time(),
    NewJobs = maps:fold(fun (Kind, KindDeks, Acc) ->
                            Snapshot = deks_config_snapshot(Kind),
                            case dek_rotation_needed(Kind, KindDeks, CurDT,
                                                     Snapshot) of
                                true ->
                                    ?log_debug("Dek rotation needed for ~p",
                                               [Kind]),
                                    [{maybe_update_deks, Kind} | Acc];
                                false -> Acc
                            end
                        end, [], Deks),
    restart_dek_rotation_timer(add_and_run_jobs(NewJobs, State));

handle_timer(dek_info_update, #state{proc_type = ?NODE_PROC} = State) ->
    {_Res, NewState} = calculate_dek_info(State),
    restart_dek_info_update_timer(false, NewState);

handle_timer(remove_retired_keys, #state{proc_type = ?NODE_PROC} = State) ->
    encryption_service:cleanup_retired_keys(),
    restart_remove_retired_timer(State);

handle_timer(remove_historical_keys,
             #state{proc_type = ?MASTER_PROC} = State) ->
    {ok, NewState} = maybe_remove_historical_keys(State),
    NewState;

handle_timer(test_secrets, #state{proc_type = ?NODE_PROC} = State) ->
    run_periodic_test_for_secrets(),
    restart_test_secrets_timer(false, State);

handle_timer(Name, State) ->
    ?log_warning("Unhandled timer: ~p", [Name]),
    State.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec module_by_type(secret_type()) -> {module(), ExtraArg::list()}.
module_by_type(?CB_MANAGED_KEY_TYPE) ->
    {cb_managed_ear_key, []};
module_by_type(?AWSKMS_KEY_TYPE) ->
    {cb_kms_ear_key, [cb_aws_kms_ear_key]};
module_by_type(?GCPKMS_KEY_TYPE) ->
    {cb_kms_ear_key, [cb_gcp_kms_ear_key]};
module_by_type(?AZUREKMS_KEY_TYPE) ->
    {cb_kms_ear_key, [cb_azure_kms_ear_key]};
module_by_type(?HASHIKMS_KEY_TYPE) ->
    {cb_hashi_ear_key, []};
module_by_type(?KMIP_KEY_TYPE) ->
    {cb_kmip_ear_key, []}.

-spec call_module_by_type(secret_type(), atom(), [term()]) -> term().
call_module_by_type(Type, Function, Args) ->
    {Module, ExtraArgs} = module_by_type(Type),
    erlang:apply(Module, Function, Args ++ [ExtraArgs]).

-spec rotate_secret_by_id(secret_id(), boolean()) ->
          {ok, string()} |
          {error, not_found | bad_encrypt_id() |
                  inconsistent_graph() | not_supported | no_quorum}.
rotate_secret_by_id(Id, IsAutomatic) ->
    ?log_info("Rotating secret #~b", [Id]),
    case get_secret(Id) of
        {ok, #{name := Name} = SecretProps} ->
            try test_and_rotate_secret(SecretProps) of
                ok ->
                    log_succ_kek_rotation(Id, Name, IsAutomatic),
                    {ok, Name};
                {error, Reason} ->
                    log_unsucc_kek_rotation(Id, Name, Reason, IsAutomatic),
                    {error, Reason}
            catch
                C:E:ST ->
                    log_unsucc_kek_rotation(Id, Name, exception, IsAutomatic),
                    erlang:raise(C, E, ST)
            end;
        {error, Reason} ->
            ?log_error("Secret #~p rotation failed: ~p", [Id, Reason]),
            {error, Reason}
    end.

test_and_rotate_secret(SecretProps) ->
    %% If the secret is broken, we should not start rotation.
    %% Rotation is asynchronous: we create new key id and then let the system
    %% apply that new key (re-encrypt corresponding keys with that new key).
    %% If the secret is broken completely (say it uses AWS key that is removed),
    %% the rotation will simply generate garbage in config and on disk.
    %% Note that this check doesn't guarantee that there will be no issues
    %% with re-encryption, but it helps to catch most of the issues (e.g. when
    %% the reason of the problem is secret's settings).
    Nodes = ns_node_disco:nodes_actual(),
    maybe
        ok ?= test_existing_secret_props(SecretProps, Nodes),
        rotate_secret(SecretProps)
    end.

-spec rotate_secret(secret_props()) -> ok | {error, not_found |
                                                    bad_encrypt_id() |
                                                    inconsistent_graph() |
                                                    not_supported |
                                                    no_quorum}.
rotate_secret(#{id := Id, type := Type}) ->
    maybe
        {ok, NewKey} ?= call_module_by_type(Type, generate_key,
                                            [erlang:universaltime()]),
        ok ?= add_active_key(Id, NewKey, _UpdateRotationTime = true),
        ok
    else
        {error, Reason} ->
            ?log_error("Secret #~p rotation failed: ~p", [Id, Reason]),
            {error, Reason}
    end.

-spec copy_static_props(secret_props(), secret_props()) -> secret_props().
%% Copies properties that secret can never change
copy_static_props(#{type := Type, id := Id,
                    creation_time := CreationDT,
                    data := OldData},
                  #{type := Type, data := NewData} = NewSecretProps) ->
    UpdatedData = call_module_by_type(Type, modify_props, [OldData, NewData]),
    NewSecretProps#{id => Id, creation_time => CreationDT, data => UpdatedData}.

-spec replace_secret_in_list(secret_props(), [secret_props()]) ->
                                                      [secret_props()] | false.
replace_secret_in_list(NewProps, List) ->
    Id = maps:get(id, NewProps),
    ReplaceFun = fun Replace([], _Acc) -> false;
                     Replace([Next | Rest], Acc) ->
                         case maps:get(id, Next) of
                             Id -> lists:reverse([NewProps | Acc], Rest);
                             _ -> Replace(Rest, [Next | Acc])
                         end
                 end,
    ReplaceFun(List, []).

-spec add_active_key(secret_id(), term(), boolean()) ->
                        ok | {error, not_found | inconsistent_graph() |
                                     encryption_service:stored_key_error() |
                                     bad_encrypt_id() | no_quorum}.
add_active_key(Id, Key, true = _UpdateRotationTime) ->
    chronicle_transaction(
      [?CHRONICLE_SECRETS_KEY],
      fun (Snapshot) ->
          maybe
              {ok, #{type := Type,
                     data := SecretData} = SecretProps} ?=
                  get_secret(Id, Snapshot),
              UpdatedData = call_module_by_type(Type,
                                                set_new_active_key_in_props,
                                                [Key, SecretData]),
              Updated = SecretProps#{data => UpdatedData},
              {ok, FinalProps} ?= ensure_secret_encrypted_txn(Updated,
                                                              Snapshot),
              NewList = replace_secret_in_list(FinalProps,
                                               get_all(Snapshot)),
              true = is_list(NewList),
              ok ?= validate_secrets_consistency(NewList),
              {commit, [{set, ?CHRONICLE_SECRETS_KEY, NewList}]}
          else
              {error, _} = Error ->
                  {abort, Error}
          end
      end).

-spec ensure_all_keks_on_disk(#state{}) ->
          {ok, #state{}} | {error, #state{}, list()}.
ensure_all_keks_on_disk(State) ->
    ensure_all_keks_on_disk(State, direct).

-spec ensure_all_keks_on_disk(#state{}, chronicle_snapshot()) ->
          {ok, #state{}} | {error, #state{}, list()}.
ensure_all_keks_on_disk(#state{kek_hashes_on_disk = Vsns} = State, Snapshot) ->
    {RV, NewVsns} = persist_keks(Vsns, Snapshot),
    NewState = State#state{kek_hashes_on_disk = NewVsns},
    case RV of
        ok -> {ok, NewState};
        {error, Reason} -> {error, NewState, Reason}
    end.

-spec cleanup_alerts() -> ok.
cleanup_alerts() ->
    AllIdsSet = sets:from_list([Id || #{id := Id} <- get_all()]),
    menelaus_web_alerts_srv:filter_alerts(
      fun ({encr_at_rest_key_test_failed, Id}) ->
              sets:is_element(Id, AllIdsSet);
          (_) ->
              true
      end).

-spec persist_keks(Hashes, Snapshot) ->
          {ok, Hashes} |
          {{error, term()}, Hashes} when Hashes :: #{secret_id() => integer()},
                                         Snapshot :: chronicle_snapshot().
persist_keks(Hashes, Snapshot) ->
    Write = fun (#{type := T, id := SecretId, data := D} = S) ->
                    ?log_debug("Ensure all keys are on disk for secret ~p ",
                               [SecretId]),
                    call_module_by_type(T, persist, [D, secret_ad(S)])
            end,

    {ok, AllSecrets} = topologically_sorted_secrets(get_all(Snapshot)),

    {RV, NewHashes} = lists:mapfoldl(
                        fun (#{id := Id, name := Name} = S, Acc) ->
                            Old = maps:get(Id, Acc, undefined),
                            case erlang:phash2(S, ?MAX_PHASH2_RANGE) of
                                Old -> {{Id, ok}, Acc};
                                New ->
                                   create_encryption_key_stats(Name),
                                   case Write(S) of
                                       ok -> {{Id, ok}, Acc#{Id => New}};
                                       {error, _} = E -> {{Id, E}, Acc}
                                   end
                            end
                        end, Hashes, AllSecrets),

    IdsDoNotExist = maps:keys(NewHashes) -- proplists:get_keys(RV),
    {misc:many_to_one_result(RV), maps:without(IdsDoNotExist, NewHashes)}.

-spec garbage_collect_keks() -> ok.
garbage_collect_keks() ->
    AllKekIds = all_kek_ids(),
    ?log_debug("keks gc: All existing keks: ~p", [AllKekIds]),
    case encryption_service:garbage_collect_keks(AllKekIds) of
        ok -> %% some keks were retired
            garbage_collect_key_stats(),
            ok;
        {error, _} = Error ->
            Error;
        no_change ->
            ok
    end.

-spec synchronize_deks_on_all_nodes([cb_deks:dek_kind()]) ->
          ok | {error, {deks_sync_failed, [{node(), term()}]}}.
synchronize_deks_on_all_nodes(AffectedKinds) ->
    AllNodes = ns_node_disco:nodes_wanted(),
    Res = erpc:multicall(AllNodes, ?MODULE,
                            synchronize_deks_local,
                            [AffectedKinds],
                            ?SYNCHRONIZE_DEKS_TIMEOUT),
    AllErrors =
        lists:filtermap(
          fun ({_, {ok, ok}}) -> false;
              ({N, {ok, E}}) -> {true, {N, E}};
              ({N, {error, {exception, undef,
                            [{cb_cluster_secrets,
                              synchronize_deks_local, _, _}]}}}) ->
                  %% Pre-totoro nodes don't have synchronize_deks_local
                  %% We can safely ignore them here, because pre-totoro nodes
                  %% don't use encryption-at-rest for services and they don't
                  %% support file-based rebalance.
                  try erpc:call(N, cluster_compat_mode,
                                supported_compat_version, [], ?RPC_TIMEOUT) of
                      SupportedVsn ->
                          cluster_compat_mode:is_version_totoro(SupportedVsn)
                  catch Class:Reason ->
                          ?log_error("exception (~p:~p) when calling "
                                     "supported_compat_version on node ~p",
                                     [Class, Reason, N]),
                          true
                  end;
              ({_N, _E}) -> true
          end, lists:zip(AllNodes, Res)),
    case AllErrors of
        [] ->
            ok;
        _ ->
            ?log_error("synchronize_deks failed on some nodes: ~p",
                        [AllErrors]),
            {error, {deks_sync_failed, AllErrors}}
    end.

%% This function can be called remotely
-spec synchronize_deks_local([cb_deks:dek_kind()]) -> ok | {error, _}.
synchronize_deks_local([]) ->
    ok;
synchronize_deks_local([Kind | Rest]) ->
    Snapshot = deks_config_snapshot(Kind),
    case cb_deks:call_dek_callback(synchronize_deks, Kind, [Snapshot]) of
        {succ, ok} ->
            synchronize_deks_local(Rest);
        {succ, {error, Reason}} ->
            ?log_error("synchronize_deks failed for ~p: ~p", [Kind, Reason]),
            {error, {synchronize_failed, Kind, Reason}};
        {except, {C, E, _ST}} ->
            %% Error is logged by cb_deks:call_dek_callback
            {error, {synchronize_crashed, Kind, C, E}}
    end.

-spec all_kek_ids() -> [key_id()].
all_kek_ids() ->
    lists:flatmap(get_all_keys_from_props(_), get_all()).

-spec prepare_new_secret(secret_props()) -> secret_props().
prepare_new_secret(#{type := T, data := Data, creation_time := CT} = Props) ->
    NewData = call_module_by_type(T, prepare_new_props, [CT, Data]),
    Props#{data => NewData}.

-spec reread_bad_deks(cb_deks:dek_kind(), #state{}) ->
          {ok, #state{}} | {error, #state{}, term()}.
reread_bad_deks(Kind, #state{deks_info = DeksInfo} = State) ->
    case maps:find(Kind, DeksInfo) of
        {ok, #{deks := DekIds} = KindDeksInfo} ->
            {UpdatedDeks, AnyNewDeks} =
                lists:mapfoldl(
                  fun (#{id := _, type := 'raw-aes-gcm'} = K, Acc) -> {K, Acc};
                      (?DEK_ERROR_PATTERN(Id, _), Acc) ->
                          case cb_deks:read(Kind, [Id]) of
                              [?DEK_ERROR_PATTERN(_, _) = K] -> {K, Acc};
                              [K] -> {K, true}
                          end
                  end, false, DekIds),
            NewDeksInfo = DeksInfo#{Kind => KindDeksInfo#{deks => UpdatedDeks}},
            State2 = State#state{deks_info = NewDeksInfo},
            State3 = case AnyNewDeks of
                         true ->
                             {ok, _} = cb_crypto:reset_dek_cache(Kind),
                             functools:chain(
                               State2,
                               [on_deks_update(Kind, _),
                                add_jobs([{maybe_update_deks, Kind},
                                          {maybe_reencrypt_deks, Kind}], _)]);
                         false ->
                             State2
                     end,
            case lists:any(fun (?DEK_ERROR_PATTERN(_, _)) -> true;
                               (_) -> false
                           end, UpdatedDeks) of
                true ->
                    {error, State3, read_failed};
                false ->
                    {ok, State3}
            end;
        error ->
            {ok, State}
    end.

-spec maybe_update_deks(cb_deks:dek_kind(), #state{}) ->
          {ok, #state{}} | {error, #state{}, term()}.
maybe_update_deks(Kind, OldState) ->
    Snapshot = deks_config_snapshot(Kind),
    case cb_deks:call_dek_callback(get_encryption_method, Kind,
                                   [node, Snapshot]) of
        {succ, {ok, EncrMethod}} ->
            %% Read DEKs if we don't have them yet
            State = #state{deks_info = AllDeks} =
                create_dek_info_if_does_not_exist(Kind, OldState),

            #{Kind := #{active_id := ActiveId,
                        is_enabled := WasEnabled,
                        deks := Deks} = KindDeks} = AllDeks,

            CurDT = calendar:universal_time(),
            ShouldRotate = dek_rotation_needed(Kind, KindDeks, CurDT, Snapshot),

            %% Check current encryption settings and push actual active key to
            %% dek users
            case WasEnabled of

                %% On disk it is enabled but in config it is disabled:
                true when EncrMethod == disabled ->
                    ?log_info("Disabling encryption for ~p", [Kind]),
                    NewState = set_active(Kind, ActiveId, false, State),
                    ok = maybe_rotate_integrity_tokens(Kind, undefined,
                                                       NewState),
                    call_set_active_cb(Kind, Snapshot, NewState);

                %% It is enabled on disk and in config:
                true when not ShouldRotate ->
                    %% We should push it even when nothing changes in order to
                    %% handle the scenario when we crash between
                    %% set_active and SetActiveCB
                    call_set_active_cb(Kind, Snapshot, State);

                %% It is disabled on disk and in config:
                false when EncrMethod == disabled ->
                    %% We should push it even when nothing changes in order to
                    %% handle the scenario when we crash between
                    %% set_active and SetActiveCB
                    call_set_active_cb(Kind, Snapshot, State);

                %% On disk it is disabled but in config it is enabled
                %% and we already have a dek
                false when is_binary(ActiveId) and not ShouldRotate ->
                    ?log_info("Enabling encryption for ~p (no rotation needed)",
                              [Kind]),
                    NewState = set_active(Kind, ActiveId, true, State),
                    ok = maybe_rotate_integrity_tokens(Kind, ActiveId,
                                                       NewState),
                    call_set_active_cb(Kind, Snapshot, NewState);

                %% On disk it is disabled but in config it is enabled
                %% or rotation is needed
                V when (V == false) orelse ShouldRotate ->
                    case V of
                        false ->
                            ?log_info("Enabling encryption for ~p "
                                      "(new dek needed)", [Kind]);
                        _ -> ok
                    end,
                    %% There is no active dek currently, but encryption is on,
                    %% we should generate a new dek
                    case generate_new_dek(Kind, Deks, EncrMethod, Snapshot) of
                        {ok, DekId} ->
                            NewState = set_active(Kind, DekId, true, State),
                            ok = maybe_rotate_integrity_tokens(Kind, DekId,
                                                               NewState),
                            call_set_active_cb(Kind, Snapshot, NewState);
                        %% Too many DEKs and encryption is being enabled
                        %% We could not create new DEK, but should still
                        %% enable the encryption
                        %% Note that ActiveId can't be undefined because
                        %% we know there are too many deks.
                        {error, too_many_deks} when V == false ->
                            true = is_binary(ActiveId),
                            NewState = set_active(Kind, ActiveId, true, State),
                            case call_set_active_cb(Kind, Snapshot, NewState) of
                                {ok, NewState2} ->
                                    {error, NewState2, too_many_deks};
                                {error, NewState2, Reason} ->
                                    {error, NewState2, Reason}
                            end;
                        %% This just a dek rotation attempt. No need to call
                        %% set_active because nothing changes.
                        {error, too_many_deks} ->
                            NewState = maybe_garbage_collect_deks(Kind, false,
                                                                  Snapshot,
                                                                  State),
                            %% Returning error to make sure we show error
                            %% in UI
                            {error, NewState, too_many_deks};
                        {error, Reason} ->
                            {error, State, Reason}
                    end
            end;
        {succ, {error, not_found}} ->
            %% This entity doesn't exist anymore
            %% Note that bucket can exist globally, but can be missing at this
            %% specific node.
            ?log_debug("DEK kind ~p doesn't seem to exist. Forgetting about it",
                       [Kind]),
            {ok, destroy_dek_info(Kind, OldState)}
    end.

-spec maybe_garbage_collect_deks(cb_deks:dek_kind(), boolean(),
                                 chronicle_snapshot(), #state{}) ->
          #state{}.
maybe_garbage_collect_deks(Kind, Force, Snapshot,
                           #state{deks_info = DeksInfo} = State) ->
    ShouldRun =
        case maps:find(Kind, DeksInfo) of
            {ok, #{last_deks_gc_datetime := undefined}} ->
                true;
            {ok, #{last_deks_gc_datetime := DT}} ->
                %% The goal is to not call it too often
                Deadline = misc:datetime_add(DT, ?MIN_DEK_GC_INTERVAL_S),
                calendar:universal_time() > Deadline;
            error ->
                false
        end,
    case ShouldRun orelse Force of
        true ->
            case garbage_collect_deks(Kind, Force, Snapshot, State) of
                {ok, NewState} -> NewState;
                {error, NewState, Error} ->
                    case Error of
                        retry ->
                            ?log_debug("~p DEK garbage collection returned "
                                       "retry", [Kind]);
                        _ ->
                            ?log_error("~p DEK garbage collection failed: ~p",
                                       [Kind, Error])
                    end,
                    add_jobs([{garbage_collect_deks, Kind}], NewState)
            end;
        false ->
            State
    end.

%% Remove DEKs that are not being used anymore
%% Also update has_unencrypted_data in state
-spec garbage_collect_deks(cb_deks:dek_kind(), boolean(), chronicle_snapshot(),
                           #state{}) ->
          {ok, #state{}} | {error, #state{}, term()}.
garbage_collect_deks(Kind, Force, Snapshot,
                     #state{deks_info = DeksInfo} = State) ->
    ?log_debug("Garbage collecting ~p DEKs", [Kind]),
    case maps:find(Kind, DeksInfo) of
        %% Note: we can't skip this phase even when we don't have deks
        %% (or have only one dek), because we need to update
        %% "has_unencrypted_data" info anyway
        {ok, _KindDeks} ->
            case cb_deks:call_dek_callback(get_dek_ids_in_use, Kind, [Snapshot],
                                           #{verbose => true}) of
                {succ, {ok, IdList}} ->
                    handle_new_dek_ids_in_use(Kind, IdList, Force, State);
                {succ, {error, not_found}} ->
                    %% The entity that uses deks does not exist.
                    %% Ignoring it here because we assume that deks will
                    %% be removed by maybe_update_deks
                    %% It is possible that bucket exists on disk, but not in
                    %% memcached. In this case it is important to not remove
                    %% any deks here. It is not an error either.
                    {ok, State};
                {succ, {error, Reason}} ->
                    {error, State, Reason};
                {except, {_, E, _}} ->
                    {error, State, E}
            end;
        error ->
            {ok, State}
    end.

-spec handle_new_dek_ids_in_use(cb_deks:dek_kind(),
                                [cb_deks:dek_id() | ?NULL_DEK],
                                boolean(), #state{}) ->
          {ok, #state{}} | {error, #state{}, term()}.
handle_new_dek_ids_in_use(Kind, CurrInUseIDs, Force,
                          #state{deks_info = DeksInfo} = State) ->
    %% Note that we assume Kind exists
    #{Kind := #{has_unencrypted_data := HasUnencryptedData,
                statuses := Statuses,
                deks_being_dropped := IdsBeingDroppedSet} = KindDeks} =
                    DeksInfo,
    UniqCurrInUseIDs = lists:uniq(CurrInUseIDs),
    N = length(lists:delete(?NULL_DEK, UniqCurrInUseIDs)),
    notify_kind_gauge(<<"encr_at_rest_deks_in_use">>, Kind, N,
                      #{expiration_s => ?MIN_DEK_GC_INTERVAL_S}),
    UpdateStatus = case maps:get(maybe_update_deks, Statuses, undefined) of
                       undefined -> ok;
                       %% We can't generate new dek because there are too many
                       %% deks. Obviously this error should not stop the
                       %% garbage collection. Otherwise we will never get out
                       %% of too_many_deks error.
                       {error, too_many_deks} -> ok;
                       S -> S
                   end,
    UpdatedIdsBeingDropped =
        %% To prevent dropping ?NULL_DEK again and again
        %% Otherwise if we don't retire any real deks, we will never
        %% remove ?NULL_DEK from deks_being_dropped
        case lists:member(?NULL_DEK, UniqCurrInUseIDs) of
            true -> IdsBeingDroppedSet;
            false -> sets:del_element(?NULL_DEK, IdsBeingDroppedSet)
        end,
    NewHasUnencryptedData = lists:member(?NULL_DEK, UniqCurrInUseIDs),
    NewKindDeks = KindDeks#{has_unencrypted_data => NewHasUnencryptedData,
                            deks_being_dropped => UpdatedIdsBeingDropped},
    (NewHasUnencryptedData == HasUnencryptedData) orelse
        (self() ! calculate_dek_info),
    case (UpdateStatus == ok) orelse Force of
        true ->
            NewKindDeks2 = NewKindDeks#{last_deks_gc_datetime =>
                                            calendar:universal_time()},
            NewState = State#state{deks_info = DeksInfo#{
                                                   Kind => NewKindDeks2}},
            CleanedIdList = lists:delete(?NULL_DEK, UniqCurrInUseIDs),
            {ok, retire_unused_deks(Kind, CleanedIdList, NewState)};
        false ->
            %% UpdateStatus is not ok. This means update of deks
            %% finished unsuccesfully, so we don't really know if
            %% update_deks has actually finished.
            %% It is hypothetically possible that we receive error,
            %% but update_deks is still working (e.g. in
            %% memcached). In this case it is possible that we remove
            %% the keys that are being pushed.
            NewState = State#state{deks_info = DeksInfo#{
                                                   Kind => NewKindDeks}},
            ?log_debug("Skipping ~p deks retiring because update "
                       "status is ~p", [Kind, UpdateStatus]),
            {error, NewState, retry}
    end.

-spec retire_unused_deks(cb_deks:dek_kind(), [cb_deks:dek_id()], #state{}) ->
          #state{}.
retire_unused_deks(Kind, DekIdsInUse, #state{deks_info = DeksInfo} = State) ->
    #{Kind := #{active_id := ActiveId,
                is_enabled := IsEnabled,
                deks := Deks} = KindDeks} = DeksInfo,
    NewDeks =
        lists:filter(
          fun (#{id := Id}) ->
              case lists:member(Id, DekIdsInUse) of
                  true ->
                      true;
                  false when Id == ActiveId, IsEnabled ->
                      %% We should not remove active key
                      ?log_error("Attempt to remove active dek ~p for ~p "
                                 "while encryption is on", [ActiveId, Kind]),
                      true;
                  false ->
                      false
              end
          end, Deks),

    case length(NewDeks) == length(Deks) of
        true -> State;
        false ->
            {NewKindDeks, NewDekIdsInUse} =
                case lists:member(ActiveId, DekIdsInUse) of
                    false when not IsEnabled ->
                        {KindDeks#{deks => NewDeks, active_id => undefined},
                         DekIdsInUse};
                    false ->
                        %% Don't let encryption service remove active dek
                        {KindDeks#{deks => NewDeks}, [ActiveId | DekIdsInUse]};
                    true ->
                        {KindDeks#{deks => NewDeks}, DekIdsInUse}
                end,
            NewState = State#state{deks_info = DeksInfo#{Kind => NewKindDeks}},
            write_deks_cfg_file(NewState),
            %% It doesn't make sense to fail this job if file removal fails
            %% because when retried the job will do nothing anyway (because
            %% state doesn't have those deks)
            encryption_service:garbage_collect_keys(Kind, NewDekIdsInUse),
            {ok, _} = cb_crypto:reset_dek_cache(Kind),
            on_deks_update(Kind, NewState)
    end.

-spec call_set_active_cb(cb_deks:dek_kind(), chronicle_snapshot(), #state{}) ->
          {ok, #state{}} | {error, #state{}, term()}.
call_set_active_cb(Kind, Snapshot, #state{deks_info = DeksInfo} = State) ->
    #{Kind := #{active_id := ActiveId,
                deks := Keys,
                is_enabled := IsEnabled,
                prev_deks_hash := PrevHash} = KindDeks} = DeksInfo,
    NewActiveKey =
        case IsEnabled of
            true ->
                {value, ActiveKey} = lists:search(fun (#{id := Id}) ->
                                                        Id == ActiveId
                                                  end, Keys),
                ActiveKey;
            false -> undefined
        end,
    NewHash = deks_hash(IsEnabled, ActiveId, Keys),
    case NewActiveKey of
        ?DEK_ERROR_PATTERN(_, _) ->
            {error, State, active_key_not_available};
        _ when NewHash == PrevHash ->
            ?log_debug("No changes in ~p deks, skipping calling "
                       "update_deks", [Kind]),
            {ok, State};
        _ ->
            case cb_crypto:reset_dek_cache(Kind,
                                           should_update_cache(NewHash, _)) of
                {ok, _} ->
                    case cb_deks:call_dek_callback(update_deks, Kind,
                                                   [Snapshot],
                                                   #{verbose => true}) of
                        {succ, ok} ->
                            NewKindDeks = KindDeks#{prev_deks_hash => NewHash},
                            NewDeksInfo = DeksInfo#{Kind => NewKindDeks},
                            NewState = State#state{deks_info = NewDeksInfo},
                            {ok, maybe_garbage_collect_deks(Kind, true,
                                                            Snapshot,
                                                            NewState)};
                        {succ, {error, Reason}} ->
                            {error, State, Reason};
                        {except, {_, E, _}} ->
                            {error, State, E}
                    end;
                {error, Reason} ->
                    {error, State, Reason}
            end
    end.

deks_hash(IsEnabled, ActiveId, Keys) ->
    %% Note: Here we assume that id and type uniquely identify the key, so
    %% we are not using the key material itself.
    %% Type is needed to identify the keys that are not actually available
    %% (e.g. read errors).
    Ids = lists:map(fun (#{id := Id, type := T}) -> {Id, T} end, Keys),
    erlang:phash2({IsEnabled, ActiveId, lists:sort(Ids)}, ?MAX_PHASH2_RANGE).

should_update_cache(NewHash, CachedDEKSnapshot) ->
    {ActiveKeyId, AllKeys} = cb_crypto:get_all_deks(CachedDEKSnapshot),

    OldHash = deks_hash(ActiveKeyId =/= undefined, ActiveKeyId, AllKeys),

    NewHash =/= OldHash.

-spec maybe_rotate_integrity_tokens(cb_deks:dek_kind(),
                                    cb_deks:dek_id() | undefined,
                                    #state{}) -> ok.
maybe_rotate_integrity_tokens(configDek, DekId, State) ->
    ok = encryption_service:maybe_rotate_integrity_tokens(DekId),
    %% Resaving deks cfg because we need to update MAC for this file
    ok = write_deks_cfg_file(State);
maybe_rotate_integrity_tokens(_Kind, _DekId, _State) ->
    ok.

dek_kind_supports_drop(_Kind) ->
    true.

-spec on_deks_update(cb_deks:dek_kind(), #state{}) -> #state{}.
on_deks_update(Kind, #state{deks_info = AllDeks} = State) ->
    case maps:find(Kind, AllDeks) of
        {ok, #{deks_being_dropped := CurDeksDroppedSet,
               deks := CurDeks,
               is_enabled := IsEnabled} = CurKindDeks} ->
            DekIds = lists:map(fun (#{id := Id}) -> Id end, CurDeks),
            DeksDroppedSet = sets:intersection(CurDeksDroppedSet,
                                               sets:from_list(DekIds,
                                                              [{version, 2}])),
            DeksDroppedSet2 = case IsEnabled of
                                  true -> DeksDroppedSet;
                                  false -> sets:del_element(?NULL_DEK,
                                                            DeksDroppedSet)
                              end,
            NewKindDeks = CurKindDeks#{deks_being_dropped => DeksDroppedSet2},
            NewAllDeks = AllDeks#{Kind => NewKindDeks},
            self() ! calculate_dek_info,
            functools:chain(State#state{deks_info = NewAllDeks},
                            [restart_dek_cleanup_timer(_),
                             restart_dek_rotation_timer(_)]);
        error ->
            State
    end.

-spec set_active(cb_deks:dek_kind(), undefined | cb_deks:dek_id(), boolean(),
                 #state{}) -> #state{}.
set_active(Kind, ActiveId, IsEnabled, #state{deks_info = DeksInfo} = State) ->
    #{Kind := #{active_id := CurActiveId, deks := CurDeks} = D} = DeksInfo,
    NewDeks =
        case ActiveId of
            undefined -> CurDeks;
            CurActiveId -> CurDeks;
            _ ->
                {ok, NewDek} = encryption_service:read_dek(Kind, ActiveId),
                %% We assume that we never add old keys, only newly generated ones
                false = lists:search(fun (#{id := Id}) ->
                                         Id == ActiveId
                                     end, CurDeks),
                [NewDek | CurDeks]
        end,
    NewD = D#{active_id => ActiveId, is_enabled => IsEnabled, deks => NewDeks},
    NewState = State#state{deks_info = DeksInfo#{Kind => NewD}},
    write_deks_cfg_file(NewState),
    on_deks_update(Kind, NewState).

-spec write_deks_cfg_file(#state{}) -> ok.
write_deks_cfg_file(#state{deks_info = DeksInfo}) ->
    Path = deks_file_path(),
    Term = maps:map(
             fun (_Kind, #{is_enabled := IsEnabled,
                           active_id := ActiveId,
                           deks := Deks}) ->
                 cb_deks_raw_utils:new_deks_file_record(
                   ActiveId, IsEnabled, [Id || #{id := Id} <- Deks])
             end, DeksInfo),
    Bin = term_to_binary(Term),

    ConfigDekInfo = case maps:find(configDek, DeksInfo) of
                        {ok, C} -> C;
                        error ->
                            #{is_enabled => false,
                              active_id => undefined,
                              deks => []}
                    end,

    {ok, MAC} = encryption_service:mac(Bin),
    MACSize = byte_size(MAC),
    ToWrite = <<MACSize:32/unsigned-integer, MAC/binary, Bin/binary>>,
    case ConfigDekInfo of
        #{is_enabled := false} ->
            ok = file:write_file(Path, ToWrite);
        #{is_enabled := true, active_id := CfgActiveId, deks := CfgDeks} ->
            {value, CfgActiveKey} = lists:search(fun (#{id := Id}) ->
                                                         Id == CfgActiveId
                                                 end, CfgDeks),
            #{type := 'raw-aes-gcm'} = CfgActiveKey,
            DS = cb_crypto:create_single_dek_snapshot(CfgActiveKey, undefined),
            ok = cb_crypto:atomic_write_file(Path, ToWrite, DS)
    end,
    ok.

-spec deks_file_path() -> string().
deks_file_path() ->
    filename:join(path_config:component_path(data, "config"),
                  ?DEK_CFG_FILENAME).

-spec maybe_read_deks(#state{}) -> #state{}.
maybe_read_deks(#state{proc_type = ?NODE_PROC,
                       deks_info = undefined} = State) ->
    {KekPushHashes, Deks, Errors} = init_deks(),

    case Errors of
        [] -> ok;
        _ -> ?log_error("Init deks errors:~n~p", [Errors])
    end,

    NewState = State#state{deks_info = Deks,
                           kek_hashes_on_disk = KekPushHashes},

    ok = garbage_collect_keks(),
    encryption_service:cleanup_retired_keys(),

    %% We should rotate here (when process is starting) because we can
    %% hypothetically crash after calling set_active() but before calling
    %% maybe_rotate_integrity_tokens() below
    case NewState of
        #state{deks_info = #{configDek := #{is_enabled := true,
                                        active_id := ActiveId}}} ->
            ok = maybe_rotate_integrity_tokens(configDek, ActiveId, NewState);
        #state{deks_info = #{configDek := #{is_enabled := false}}} ->
            ok = maybe_rotate_integrity_tokens(configDek, undefined, NewState);
        #state{} ->
            ok
    end,
    Kinds = maps:keys(Deks),
    add_jobs([{maybe_update_deks, K} || K <- Kinds], NewState);
maybe_read_deks(#state{} = State) ->
    State.

-spec init_deks() -> {#{secret_id() => integer()},
                      #{cb_deks:dek_kind() => deks_info()},
                      [term()]}.
init_deks() ->
    Snapshot = chronicle_compat:get_snapshot([fun fetch_snapshot_in_txn/1],
                                             #{}),
    Deks = read_all_deks(Snapshot),
    KekPushHashes =
        case persist_keks(#{}, Snapshot) of
            {ok, H} -> H;
            {{error, Reason}, H} ->
                %% Some Keks may have been written so we use the updated state
                %% Some Keks that can't be written may be unused so we continue
                %% to try to rencrypt deks even on a failure here
                ?log_error("Failed to write all keks to disk: ~p", [Reason]),
                H
        end,

    {ReencryptedDeksList, Errors} =
        lists:mapfoldl(
          fun ({Kind, KindDeks}, Acc) ->
              case reencrypt_deks(Kind, KindDeks, Snapshot) of
                  no_change ->
                      {{Kind, KindDeks}, Acc};
                  {changed, NewKindDeks, Errors} ->
                      {{Kind, NewKindDeks}, Errors ++ Acc};
                  {error, Error} ->
                      {{Kind, KindDeks}, [Error | Acc]}
              end
          end, [], maps:to_list(Deks)),

    ReencryptedDeks = maps:from_list(ReencryptedDeksList),
    {KekPushHashes, ReencryptedDeks, Errors}.

-spec read_all_deks(chronicle_snapshot()) ->
          #{cb_deks:dek_kind() => deks_info()}.
read_all_deks(Snapshot) ->
    GetCfgDek = encryption_service:read_dek(configDek, _),
    VerifyMac = fun encryption_service:verify_mac/2,
    {ok, Term} = cb_deks_raw_utils:read_deks_file(deks_file_path(), GetCfgDek,
                                                  VerifyMac),

    maps:filtermap(
      fun (Kind, #{is_enabled := IsEnabled,
                  active_id := ActiveId,
                  dek_ids := DekIds}) ->
          case cb_deks:call_dek_callback(get_encryption_method, Kind,
                                         [node, Snapshot]) of
              {succ, {ok, _}} ->
                  Keys = cb_deks:read(Kind, DekIds),
                  {true, new_dek_info(Kind, ActiveId, Keys,
                                      IsEnabled)};
              {succ, {error, not_found}} ->
                  false
          end
      end, Term).

-spec reread_deks(cb_deks:dek_kind(), [cb_deks:dek_id()], deks_info()) ->
          deks_info().
reread_deks(Kind, ChangedIds, #{deks := CurDeks} = KindDeks) ->
    NewDeks =
        lists:map(
            fun (#{id := DekId} = OldK) ->
                case lists:member(DekId, ChangedIds) of
                    true ->
                        {ok, NewK} = encryption_service:read_dek(Kind, DekId),
                        NewK;
                    false ->
                        OldK
                end
            end, CurDeks),
    KindDeks#{deks => NewDeks}.

-spec new_dek_info(cb_deks:dek_kind(), undefined | cb_deks:dek_id(),
                   [cb_deks:dek()], boolean()) -> deks_info().
new_dek_info(Kind, ActiveId, Keys, IsEnabled) ->
    create_kind_stats(Kind),
    InUse = lists:map(fun (#{id := Id}) -> Id end, Keys),
    encryption_service:garbage_collect_keys(Kind, InUse),
    #{active_id => ActiveId,
      deks => Keys,
      is_enabled => IsEnabled,
      deks_being_dropped => sets:new([{version, 2}]),
      last_drop_timestamp => undefined,
      has_unencrypted_data => undefined,
      last_deks_gc_datetime => undefined,
      statuses => #{},
      prev_deks_hash => undefined}.

create_dek_info_if_does_not_exist(Kind, #state{deks_info = CurDeks} = State) ->
    case maps:find(Kind, CurDeks) of
        {ok, _} -> State;
        error ->
            EmptyDeks = new_dek_info(Kind, undefined, [], false),
            self() ! calculate_dek_info,
            State#state{deks_info = CurDeks#{Kind => EmptyDeks}}
    end.

-spec destroy_dek_info(cb_deks:dek_kind(), #state{}) -> #state{}.
destroy_dek_info(Kind, #state{deks_info = DeksInfo} = State) ->
    NewState = State#state{deks_info = maps:remove(Kind, DeksInfo)},
    write_deks_cfg_file(NewState),
    self() ! calculate_dek_info,
    delete_kind_stats(Kind),
    encryption_service:garbage_collect_keys(Kind, []),
    functools:chain(NewState,
                    [restart_dek_cleanup_timer(_),
                     restart_dek_rotation_timer(_)]).

is_imported(#{info := #{imported := Imported}}) -> Imported.

-spec generate_new_dek(cb_deks:dek_kind(),
                       [cb_deks:dek()],
                       cb_deks:encryption_method(),
                       chronicle_snapshot()) ->
          {ok, cb_deks:dek_id()} | {error, _}.
generate_new_dek(Kind, CurrentDeks, EncryptionMethod, Snapshot) ->
    %% Rotation is needed but if there are too many deks already
    %% we should not generate new deks (something is wrong)
    CurrentDekNum = length([D || D <- CurrentDeks, not is_imported(D)]),
    case CurrentDekNum < max_local_dek_num(Kind) of
        true ->
            ?log_debug("Generating new ~p dek, encryption is ~p...",
                       [Kind, EncryptionMethod]),
            case cb_deks:generate_new(Kind, EncryptionMethod, Snapshot) of
                {ok, DekId} ->
                    log_succ_dek_rotation(Kind, DekId),
                    {ok, DekId};
                {error, Reason} ->
                    log_unsucc_dek_rotation(Kind, Reason),
                    {error, Reason}
            end;
        false ->
            ?log_error("Skip ~p DEK creation/rotation: "
                       "too many not imported DEKs (~p)",
                       [Kind, CurrentDekNum]),
            log_unsucc_dek_rotation(Kind, too_many_deks),
            {error, too_many_deks}
    end.

-spec maybe_reencrypt_deks(cb_deks:dek_kind(), #state{}) ->
          {ok, #state{}} | {error, #state{}, term()}.
maybe_reencrypt_deks(Kind, #state{deks_info = Deks} = State) ->
    case maps:find(Kind, Deks) of
        {ok, KindDeks} ->
            Snapshot = deks_config_snapshot(Kind),
            NewState = case ensure_all_keks_on_disk(State, Snapshot) of
                           {ok, NS} -> NS;
                           {error, NS, EnsureErrors} ->
                               ?log_error("Failed to ensure all keks on "
                                          "disk: ~p", [EnsureErrors]),
                               NS
                       end,
            case reencrypt_deks(Kind, KindDeks, Snapshot) of
                no_change -> {ok, NewState};
                {changed, NewKindDeks, Errors} ->
                    NewState2 =
                        NewState#state{deks_info = Deks#{Kind => NewKindDeks}},
                    NewState3 = on_deks_update(Kind, NewState2),
                    case Errors of
                        [] -> {ok, NewState3};
                        _ -> {error, NewState3, Errors}
                    end;
                {error, Errors} ->
                    {error, NewState, Errors}
            end;
        error ->
            {ok, State}
    end.

reencrypt_deks(Kind, #{deks := Keys} = DeksInfo, Snapshot) ->
    maybe
        {succ, {ok, EncrMethod}} ?= cb_deks:call_dek_callback(
                                      get_encryption_method,
                                      Kind,
                                      [node, Snapshot]),
        RV = cb_deks:maybe_reencrypt_deks(Kind, Keys, EncrMethod, Snapshot),
        ?log_debug("Reencrypt ~0p ~p deks with ~0p result:~n~p",
                   [Kind, length(Keys), EncrMethod, RV]),
        case RV of
            no_change ->
                no_change;
            {changed, ChangedIds, Errors} ->
                {changed, reread_deks(Kind, ChangedIds, DeksInfo), Errors};
            {error, Error} ->
                {error, Error}
        end
    else
        {succ, {error, not_found}} ->
            no_change
    end.

-spec deks_config_snapshot(cb_deks:dek_kind()) -> chronicle_snapshot().
deks_config_snapshot(Kind) ->
    FetchDekKeysFun =
        fun (Txn) ->
            cb_deks:call_dek_callback_unsafe(
              fetch_chronicle_keys_in_txn, Kind, [Txn])
        end,
    FetchOtherKeysFun = chronicle_compat:txn_get_many([?CHRONICLE_SECRETS_KEY], _),
    chronicle_compat:get_snapshot([FetchDekKeysFun, FetchOtherKeysFun], #{}).

-spec get_all_keys_from_props(secret_props()) -> [key_id()].
get_all_keys_from_props(#{type := T, data := Data}) ->
    call_module_by_type(T, get_all_key_ids_from_props, [Data]).

-spec validate_secret_in_txn(secret_props(), #{} | secret_props(),
                             chronicle_snapshot()) ->
                                            ok | {error, bad_encrypt_id() |
                                                         bad_usage_change()}.
validate_secret_in_txn(NewProps, PrevProps, Snapshot) ->
    maybe
        ok ?= validate_secrets_encryption_usage_change(NewProps, PrevProps,
                                                       Snapshot),
        ok ?= validate_dek_related_usage_change(NewProps, PrevProps, Snapshot),
        ok ?= validate_encryption_secret_id(NewProps, Snapshot),
        ok ?= validate_for_config_encryption(NewProps, Snapshot),
        ok ?= validate_name_uniqueness(NewProps, Snapshot),
        ok ?= validate_secret_usages(NewProps, Snapshot)
    end.

-spec execute_on_master({module(), atom(), [term()]}) -> term().
execute_on_master({_, _, _} = MFA) ->
    misc:wait_for_global_name(cb_cluster_secrets_master),
    case gen_server:call(?MASTER_MONITOR_NAME, {call, MFA}, 60000) of
        {succ, Res} -> Res;
        {exception, {C, E, ST}} -> erlang:raise(C, E, ST)
    end.

-spec can_delete_secret(secret_props(), chronicle_snapshot()) ->
                                            ok | {error, secret_in_use()}.
can_delete_secret(#{id := Id}, Snapshot) ->
    UsedBy = where_is_secret_used(Id, Snapshot),
    InUse = maps:fold(fun (_, Where, Acc) when is_list(Where) ->
                          Acc orelse (length(Where) > 0)
                      end, false, UsedBy),
    case InUse of
        false -> ok;
        true -> {error, {used_by, UsedBy}}
    end.

-spec get_secrets_encrypted_by_key_id(key_id(), chronicle_snapshot()) ->
          [secret_id()].
get_secrets_encrypted_by_key_id(KeyId, Snapshot) ->
    lists:filtermap(
      fun (#{id := Id} = Props) ->
          case lists:member(KeyId, get_kek_ids_that_encrypt_props(Props)) of
              true -> {true, Id};
              false -> false
          end
      end, get_all(Snapshot)).

-spec get_kek_ids_that_encrypt_props(secret_props()) -> [key_id()].
get_kek_ids_that_encrypt_props(#{type := T, data := Data}) ->
    call_module_by_type(T, get_key_ids_that_encrypt_props, [Data]).

-spec get_secrets_used_by_secret_id(secret_id(), chronicle_snapshot()) ->
                                                                [secret_id()].
get_secrets_used_by_secret_id(SecretId, Snapshot) ->
    lists:filtermap(
      fun (#{id := Id} = Props) ->
          case lists:member(SecretId, get_secrets_that_encrypt_props(Props)) of
              true -> {true, Id};
              false -> false
          end
      end, get_all(Snapshot)).

-spec get_secrets_that_encrypt_props(secret_props()) -> [secret_id()].
get_secrets_that_encrypt_props(#{type := T, data := Data}) ->
    call_module_by_type(T, get_secret_ids_that_encrypt_props, [Data]).

-spec get_dek_kinds_used_by_secret_id(secret_id(), chronicle_snapshot()) ->
                                                        [cb_deks:dek_kind()].
get_dek_kinds_used_by_secret_id(Id, Snapshot) ->
    {Map, _} = get_dek_counters(Snapshot),
    maps:keys(maps:get({secret, Id}, Map, #{})).

-spec get_active_key_id_from_secret(secret_props()) -> {ok, key_id()} |
                                                       {error, not_supported}.
get_active_key_id_from_secret(#{type := T, data := Data}) ->
    call_module_by_type(T, get_active_key_id_from_props, [Data]).

-spec maybe_reencrypt_secrets() -> ok | {error, no_quorum}.
maybe_reencrypt_secrets() ->
    RV = chronicle_transaction(
           [?CHRONICLE_SECRETS_KEY],
           fun (Snapshot) ->
               All = get_all(Snapshot),
               KeksMap =
                   maps:from_list(
                     lists:filtermap(
                       fun (#{id := Id} = P) ->
                           maybe
                               {ok, KekId} ?= get_active_key_id_from_secret(P),
                               {true, {Id, KekId}}
                           else
                               {error, not_supported} ->
                                   false
                           end
                       end, All)),
               GetActiveId = fun (SId) -> {ok, maps:get(SId, KeksMap)} end,
               {NewSecretsList, {IsChanged, AllErrors}} =
                   lists:mapfoldl(
                     fun (Secret, {ChangedAcc, ErrorsAcc}) ->
                         case maybe_reencrypt_secret_txn(Secret, GetActiveId) of
                             {true, NewSecret} -> {NewSecret, {true, ErrorsAcc}};
                             false -> {Secret, {ChangedAcc, ErrorsAcc}};
                             {error, E} -> {Secret, {ChangedAcc, [E|ErrorsAcc]}}
                         end
                     end, {false, []}, All),
               case {IsChanged, AllErrors} of
                   {false, [_ | _]} -> {abort, {error, AllErrors}};
                   {false, []} -> {abort, no_change};
                   {true, _} ->
                       %% In theory reencryption should never lead to any
                       %% cycles in graph, but we still should check it
                       ok = validate_secrets_consistency(NewSecretsList),
                       {commit, [{set, ?CHRONICLE_SECRETS_KEY, NewSecretsList}],
                        AllErrors}
               end
           end),
    case RV of
        {ok, []} ->
            sync_with_node_monitor(),
            ok;
        {ok, Errors} ->
            %% Some secrets were reencrypted, but some of them returned errors
            {error, Errors};
        no_change -> ok;
        {error, _} = Error -> Error
    end.

-spec ensure_secret_encrypted_txn(secret_props(), chronicle_snapshot()) ->
          {ok, secret_props()} |
          {error, encryption_service:stored_key_error() | bad_encrypt_id()}.
ensure_secret_encrypted_txn(Props, Snapshot) ->
    GetActiveId = get_active_key_id(_, Snapshot),
    case maybe_reencrypt_secret_txn(Props, GetActiveId) of
        {true, NewProps} -> {ok, NewProps};
        false -> {ok, Props};
        {error, _} = Error -> Error
    end.

-spec maybe_reencrypt_secret_txn(secret_props(),
                                 fun ((secret_id()) -> key_id())) ->
          false | {true, secret_props()} |
          {error, encryption_service:stored_key_error() | bad_encrypt_id()}.
maybe_reencrypt_secret_txn(#{type := T, data := Data} = Secret, GetActiveId) ->
    case call_module_by_type(T, maybe_reencrypt_props,
                             [Data, GetActiveId, secret_ad(Secret)]) of
        {ok, NewData} -> {true, Secret#{data => NewData}};
        no_change -> false;
        {error, _} = Error -> Error
    end.

-spec secret_ad(secret_props()) -> binary().
secret_ad(#{id := Id, type := T, creation_time := CT}) ->
    CTISO = iso8601:format(CT),
    iolist_to_binary([integer_to_binary(Id), atom_to_binary(T), CTISO]).

-spec maybe_reencrypt_data(sensitive_data(),
                           binary(),
                           nodeSecretManager | encryptionKey,
                           undefined | secret_id(),
                           fun ((secret_id()) -> key_id())) ->
          {ok, sensitive_data()} |
          no_change |
          {error, encryption_service:stored_key_error() | bad_encrypt_id()}.
maybe_reencrypt_data(Data, AD, EncryptBy, SecretId, GetActiveId) ->
    case EncryptBy of
        nodeSecretManager -> maybe_reencrypt_data(Data, AD, undefined);
        encryptionKey ->
            case GetActiveId(SecretId) of
                {ok, KekId} ->
                    maybe_reencrypt_data(Data, AD, {SecretId, KekId});
                {error, not_found} ->
                    {error, {encrypt_id, not_found}};
                {error, not_supported} ->
                    {error, {encrypt_id, not_allowed}}
            end
    end.

-spec maybe_reencrypt_data(sensitive_data(), binary(),
                           undefined | {secret_id(), key_id()}) ->
          {ok, sensitive_data()} |
          no_change |
          {error, encryption_service:stored_key_error()}.
%% Already encrypted with correct key
maybe_reencrypt_data(#{type := encrypted, data := _Bin,
                       encrypted_by := EncryptedBy},
                     _AD,
                     EncryptedBy) ->
    no_change;
%% Encrypted with wrong key, should reencrypt
maybe_reencrypt_data(#{type := encrypted, data := Bin,
                       encrypted_by := {_SecretId, KekId}},
                     AD,
                     {NewSecretId, NewKekId}) ->
    maybe
        {ok, RawBin} ?= encryption_service:decrypt_key(Bin, AD, KekId),
        {ok, NewBin} ?= encryption_service:encrypt_key(RawBin, AD, NewKekId),
        {ok, #{type => encrypted, data => NewBin,
               encrypted_by => {NewSecretId, NewKekId}}}
    else
        {error, Error} ->
            {error, encryption_service:maybe_wrap_encryption_error(
                      Error, failed_to_encrypt_or_decrypt_key)}
    end;
%% Encrypted, but we want it to be unencrypted (encrypted by node SM actually)
maybe_reencrypt_data(#{type := encrypted, data := Bin,
                       encrypted_by := {_SecretId, KekId}},
                     AD,
                     undefined) ->
    maybe
        {ok, RawBin} ?= encryption_service:decrypt_key(Bin, AD, KekId),
        {ok, #{type => sensitive, data => RawBin, encrypted_by => undefined}}
    end;
%% Not encrypted but should be
maybe_reencrypt_data(#{type := sensitive, data := Bin,
                       encrypted_by := undefined},
                     AD,
                     {NewSecretId, NewKekId}) ->
    maybe
        {ok, NewBin} ?= encryption_service:encrypt_key(Bin, AD, NewKekId),
        {ok, #{type => encrypted, data => NewBin,
               encrypted_by => {NewSecretId, NewKekId}}}
    else
        {error, Error} ->
            {error, encryption_service:maybe_wrap_encryption_error(
                      Error, failed_to_encrypt_or_decrypt_key)}
    end;
%% Not encrypted, and that's right
maybe_reencrypt_data(#{type := sensitive, data := _Bin,
                       encrypted_by := undefined},
                     _AD,
                     undefined) ->
    no_change.

-spec add_jobs([node_job()] | [master_job()], #state{}) -> #state{}.
add_jobs(NewJobs, State) ->
    ensure_timer_started(retry_jobs, ?RETRY_TIME,
                         add_jobs_to_state(NewJobs, State)).

add_jobs_to_state(NewJobs, #state{jobs = Jobs} = State) ->
    State#state{jobs = Jobs ++ (NewJobs -- Jobs)}.

-spec add_and_run_jobs([node_job()] | [master_job()], #state{}) -> #state{}.
add_and_run_jobs(NewJobs, State) ->
    run_jobs(add_jobs_to_state(NewJobs, State)).

-spec add_and_run_jobs_async([node_job()] | [master_job()], #state{}) -> #state{}.
add_and_run_jobs_async(NewJobs, State) ->
    self() ! run_jobs,
    add_jobs_to_state(NewJobs, State).

-spec run_jobs(#state{}) -> #state{}.
run_jobs(#state{jobs = Jobs, proc_type = ProcType} = State) ->
    NewState = lists:foldl(fun run_job/2, State#state{jobs = []}, Jobs),

    misc:flush({timer, retry_jobs}),
    case NewState#state.jobs of
        [] ->
            ?log_debug("[~p] All jobs completed", [ProcType]),
            stop_timer(retry_jobs, NewState);
        [_ | _] ->
            restart_timer(retry_jobs, ?RETRY_TIME, NewState)
    end.

-spec run_job(node_job() | master_job(), #state{}) -> #state{}.
run_job(J, State) ->
    ?log_debug("Starting job: ~p", [J]),
    {Res, NewState} = normalize_job_res(do(J, State), State),
    NewState2 = update_job_status(J, Res, NewState),
    case Res of
        ok ->
            ?log_debug("Job complete: ~p", [J]),
            NewState2;
        retry ->
            ?log_debug("Job ~p returned 'retry'", [J]),
            add_jobs_to_state([J], NewState2);
        {error, Error} ->
            ?log_error("Job ~p returned error: ~p", [J, Error]),
            add_jobs_to_state([J], NewState2)
    end.

-spec normalize_job_res(Res :: term(), #state{}) -> {ok | retry | {error, _}, #state{}}.
normalize_job_res(ok, State) -> {ok, State};
normalize_job_res({ok, State}, _) -> {ok, State};
normalize_job_res({error, State, retry}, _) -> {retry, State};
normalize_job_res({error, retry}, State) -> {retry, State};
normalize_job_res({error, State, Reason}, _) -> {{error, Reason}, State};
normalize_job_res({error, Reason}, State) -> {{error, Reason}, State}.

-spec update_job_status(node_job() | master_job(),
                        ok | retry |{error, _},
                        #state{}) -> #state{}.
update_job_status({Name, Kind}, Res, #state{deks_info = DeksInfo} = State) ->
    case maps:find(Kind, DeksInfo) of
        {ok, #{statuses := S} = D} ->
            NewS = S#{Name => Res},
            (NewS == S) orelse (self() ! calculate_dek_info),
            NewDeksInfo = DeksInfo#{Kind => D#{statuses => NewS}},
            State#state{deks_info = NewDeksInfo};
        error ->
            State
    end;
update_job_status(_, _Res, #state{} = State) ->
    State.

-spec do(node_job() | master_job(), #state{}) ->
          ok | {ok, #state{}} | retry | {error, _} | {error, #state{}, _}.
do(garbage_collect_keks, _) ->
    garbage_collect_keks();
do(ensure_all_keks_on_disk, State) ->
    ensure_all_keks_on_disk(State);
do(cleanup_alerts, _) ->
    cleanup_alerts();
do(maybe_reencrypt_secrets, _) ->
    maybe_reencrypt_secrets();
do(maybe_remove_historical_keys, State) ->
    maybe_remove_historical_keys(State);
do(maybe_reset_deks_counters, _) ->
    maybe_reset_deks_counters();
do({maybe_update_deks, Kind}, State) ->
    maybe_update_deks(Kind, State);
do({reread_bad_deks, Kind}, State) ->
    reread_bad_deks(Kind, State);
do({garbage_collect_deks, Kind}, State) ->
    garbage_collect_deks(Kind, false, deks_config_snapshot(Kind), State);
do({maybe_reencrypt_deks, K}, State) ->
    maybe_reencrypt_deks(K, State).

-spec stop_timer(Name :: atom(), #state{}) -> #state{}.
stop_timer(Name, #state{timers = Timers} = State) ->
    case maps:get(Name, Timers) of
        undefined -> State;
        {Ref, _} when is_reference(Ref) ->
            erlang:cancel_timer(Ref),
            State#state{timers = Timers#{Name => undefined}}
    end.

-spec is_timer_started(Name :: atom(), #state{}) -> boolean().
is_timer_started(Name, #state{timers = Timers}) ->
    case maps:get(Name, Timers) of
        undefined -> false;
        {Ref, _} when is_reference(Ref) -> true
    end.

-spec restart_timer(Name :: atom(), Time :: non_neg_integer(), #state{}) ->
          #state{}.
restart_timer(Name, Time, State) ->
    restart_timer(Name, Time, Time, State).

%% "Time" is the time to fire next timer, "Interval" is the intended interval
%% between firings. E.g. we want next timer to fire in 10 seconds, but normally
%% it fires every 60 seconds.
%% Normally they are the same. It is not true in some cases:
%%  - when we change timer interval via config, we need to know what was the
%%    previous interval to calculate the time to fire next timer
%%  - for some timers we want to fire them immediatelly at startup, but we still
%%    need to know what the intended interval is
restart_timer(Name, Time, Interval,
              #state{timers = Timers, timers_trigger_ts = TriggerTs} = State) ->
    NewState = stop_timer(Name, State),
    NewTime =
        case maps:find(Name, TriggerTs) of
            {ok, LastTS} ->
                %% Making sure that timer doesn't fire too often
                CurTS = erlang:monotonic_time(millisecond),
                MinPossibleTime = min_timer_interval(Name) - (CurTS - LastTS),
                max(MinPossibleTime, Time);
            error -> Time
        end,
    ?log_debug("Starting ~p timer for ~b...", [Name, NewTime]),
    Ref = erlang:send_after(NewTime, self(), {timer, Name}),
    NewState#state{timers = Timers#{Name => {Ref, Interval}}}.

-spec apply_timer_interval(atom(), pos_integer(), #state{}) -> #state{}.
apply_timer_interval(Name, NewTime, #state{timers = Timers} = State)
                                        when is_integer(NewTime), NewTime > 0 ->
    case maps:get(Name, Timers) of
        undefined ->
            restart_timer(Name, NewTime, State);
        {Ref, NewTime} when is_reference(Ref) -> %% No change
            State;
        {Ref, PrevTime} when is_reference(Ref) ->
            case erlang:read_timer(Ref) of
                false -> restart_timer(Name, NewTime, State);
                TimeLeft ->
                    ShouldBeTimeLeft = max(0, TimeLeft + (NewTime - PrevTime)),
                    restart_timer(Name, ShouldBeTimeLeft, NewTime, State)
            end
    end.

-spec min_timer_interval(atom()) -> non_neg_integer().
min_timer_interval(retry_jobs) -> ?RETRY_TIME;
min_timer_interval(_Name) -> ?MIN_TIMER_INTERVAL.

-spec ensure_timer_started(Name :: atom(), Time :: non_neg_integer(),
                           #state{}) ->
          #state{}.
ensure_timer_started(Name, Time, #state{timers = Timers} = State) ->
    case maps:get(Name, Timers) of
        undefined -> restart_timer(Name, Time, State);
        {Ref, _} when is_reference(Ref) -> State
    end.

restart_dek_info_update_timer(IsFirstCall,
                              #state{proc_type = ?NODE_PROC} = State) ->
    Interval = ?DEK_INFO_UPDATE_INVERVAL_S * 1000,
    Time = case IsFirstCall of
               true -> 0;
               false -> Interval
           end,
    restart_timer(dek_info_update, Time, Interval, State);
restart_dek_info_update_timer(_, #state{proc_type = ?MASTER_PROC} = State) ->
    State.

-spec restart_rotation_timer(#state{}) -> #state{}.
restart_rotation_timer(#state{proc_type = ?NODE_PROC} = State) ->
    State;
restart_rotation_timer(#state{proc_type = ?MASTER_PROC} = State) ->
    CurDateTime = calendar:universal_time(),
    Time = calculate_next_rotation_time(CurDateTime, get_all()),
    ?log_debug("Starting rotation timer for ~b...", [Time]),
    restart_timer(rotate_keks, Time, State).

-spec ensure_remove_historical_keys_timer(#state{}) -> #state{}.
ensure_remove_historical_keys_timer(#state{proc_type = ?NODE_PROC} = State) ->
    State;
ensure_remove_historical_keys_timer(#state{proc_type = ?MASTER_PROC} = State) ->
    HistoricalKeysToRemove = historical_keys_to_remove(direct),
    case HistoricalKeysToRemove of
        [] ->
            stop_timer(remove_historical_keys, State);
        [_ | _] ->
            ensure_timer_started(remove_historical_keys,
                                 ?REMOVE_HISTORICAL_KEYS_INTERVAL, State)
    end.

-spec calculate_next_rotation_time(calendar:datetime(), [secret_props()]) ->
                                            TimeInMs :: non_neg_integer().
calculate_next_rotation_time(CurDateTime, Secrets) ->
    Times = [T || S <- Secrets, T <- [get_rotation_time(S)], T =/= undefined],
    time_to_first_event(CurDateTime, Times).

-spec get_rotation_time(secret_props()) -> calendar:datetime() | undefined.
get_rotation_time(#{type := T, data := Data}) ->
    call_module_by_type(T, get_next_rotation_time_from_props, [Data]).

-spec time_to_first_event(calendar:datetime(), [calendar:datetime()]) ->
          non_neg_integer().
time_to_first_event(_CurDateTime, []) -> ?MAX_RECHECK_ROTATION_INTERVAL;
time_to_first_event(CurDateTime, EventTimes) ->
    MinDateTime = lists:min(EventTimes),
    CurSec = calendar:datetime_to_gregorian_seconds(CurDateTime),
    MinSec = calendar:datetime_to_gregorian_seconds(MinDateTime),
    TimeRemains = max(0, (MinSec - CurSec) * 1000),
    min(?MAX_RECHECK_ROTATION_INTERVAL, TimeRemains).

-spec restart_dek_cleanup_timer(#state{}) -> #state{}.
restart_dek_cleanup_timer(#state{proc_type = ?MASTER_PROC} = State) ->
    State;
restart_dek_cleanup_timer(#state{proc_type = ?NODE_PROC,
                                 deks_info = DeksInfo} = State) ->
    CurDateTime = calendar:universal_time(),
    Time = calculate_next_dek_cleanup(CurDateTime, DeksInfo),
    restart_timer(dek_cleanup, Time, State).

-spec calculate_next_dek_cleanup(calendar:datetime(), #{}) ->
          TimeInMs :: non_neg_integer().
calculate_next_dek_cleanup(CurDateTime, DeksInfo) ->
    Times =
        maps:fold(
          fun (Kind, KindDeks, Acc) ->
              #{deks_being_dropped := IdsBeingDroppedSet,
                last_drop_timestamp := LastDropTS} = KindDeks,
              DropRetryInterval = ?DEK_DROP_RETRY_TIME_S(Kind),
              case dek_expiration_times(Kind, KindDeks) of
                  {ok, ExpirationTimes} ->
                      ?log_debug("~p DEKs expiration times: ~0p, deks already "
                                 "being dropped: ~0p (last drop time: ~0p)",
                                 [Kind, ExpirationTimes, IdsBeingDroppedSet,
                                  LastDropTS]),
                      lists:map(
                        fun ({DT, Id}) ->
                            case sets:is_element(Id, IdsBeingDroppedSet) of
                                true ->
                                    LastDropDT =
                                        calendar:gregorian_seconds_to_datetime(
                                          LastDropTS),
                                    misc:datetime_add(LastDropDT,
                                                      DropRetryInterval);
                                false ->
                                    DT
                            end
                        end, ExpirationTimes) ++ Acc;
                  {error, not_found} ->
                      %% Assume there is not such entity anymore, we just
                      %% haven't removed deks yet, ignoring them
                      Acc;
                  {error, not_supported} ->
                      Acc;
                  {error, _} ->
                      [misc:datetime_add(CurDateTime,
                                         ?DEK_TIMER_RETRY_TIME_S) | Acc]
              end
          end, [], DeksInfo) ++
        excessive_deks_drop_time(CurDateTime, DeksInfo),
    time_to_first_event(CurDateTime, Times).

excessive_deks_drop_time(CurDateTime, DeksInfo) ->
    maps:fold(fun (Kind, KindDeks, Acc) ->
                  case should_reduce_deks_num(Kind, KindDeks) of
                      true -> [CurDateTime | Acc];
                      false -> Acc
                  end
             end, [], DeksInfo).

-spec should_reduce_deks_num(cb_deks:dek_kind(), deks_info()) -> boolean().
should_reduce_deks_num(Kind, #{deks_being_dropped := AlreadyBeingDroppedSet,
                               deks := Deks}) ->
    %% If we have too many DEKs and we are not dropping anything currently,
    %% we should drop some DEKs to reduce the number of DEKs
    ?DROP_EXCESSIVE_DEKS andalso
    sets:is_empty(AlreadyBeingDroppedSet) andalso
    (length(Deks) > max_total_dek_num(Kind)).

excessive_deks_to_drop(_Kind, #{deks := []}) -> [];
excessive_deks_to_drop(Kind, #{deks := Deks} = KindDeks) ->
    case should_reduce_deks_num(Kind, KindDeks) of
        true ->
            %% find the oldest dek:
            R = lists:foldl(fun (#{info := #{creation_time := CT1}} = D1,
                                 #{info := #{creation_time := CT2}} = D2) ->
                                case CT1 < CT2 of
                                    true -> D1;
                                    false -> D2
                                end
                            end, hd(Deks), tl(Deks)),
            Id = maps:get(id, R),
            ?log_debug("Too many ~p DEKs, should drop ~p", [Kind, Id]),
            [Id];
        false ->
            []
    end.

-spec get_expired_deks(cb_deks:dek_kind(), deks_info()) ->
          [cb_deks:dek_id() | ?NULL_DEK].
get_expired_deks(Kind, DeksInfo) ->
    case dek_expiration_times(Kind, DeksInfo) of
        {ok, DekExpirationTimes} ->
            CurDateTime = calendar:universal_time(),
            lists:filtermap(fun ({ExpirationTime, Id}) ->
                                case CurDateTime >= ExpirationTime of
                                    true -> {true, Id};
                                    false -> false
                                end
                            end, DekExpirationTimes);
        {error, _} -> []
    end.

-spec dek_expiration_times(cb_deks:dek_kind(), deks_info()) ->
          {ok, [{calendar:datetime(), cb_deks:dek_id() | ?NULL_DEK}]} |
          {error, _}.
dek_expiration_times(Kind, #{deks := Deks, is_enabled := IsEnabled,
                             has_unencrypted_data := HasUnencryptedData}) ->
    Snapshot = deks_config_snapshot(Kind),
    maybe
        {_, true} ?= {drop_supported, dek_kind_supports_drop(Kind)},
        {succ, {ok, LifeTimeInSec}} ?=
            cb_deks:call_dek_callback(get_deks_lifetime, Kind, [Snapshot]),
        {succ, {ok, DropKeysTS}} ?=
            cb_deks:call_dek_callback(get_drop_deks_timestamp, Kind,
                                      [Snapshot]),
        {succ, {ok, ForceEncryptionTS}} ?=
            cb_deks:call_dek_callback(get_force_encryption_timestamp, Kind,
                                      [Snapshot]),
        RegularKeyTimes =
            lists:filtermap(
              fun (#{id := Id} = Key) ->
                  case dek_expiration_time(LifeTimeInSec, DropKeysTS, Key) of
                      {value, DT} -> {true, {DT, Id}};
                      false -> false
                  end
              end, Deks),
        EmptyKeyTimes =
            case ForceEncryptionTS =/= undefined andalso IsEnabled
                 andalso HasUnencryptedData of
                true -> [{ForceEncryptionTS, ?NULL_DEK}];
                %% This means HasUnencryptedData is undefined. We assume that
                %% bucket has unencrypted data in this case, just in case
                undefined -> [{ForceEncryptionTS, ?NULL_DEK}];
                false -> []
            end,
        {ok, RegularKeyTimes ++ EmptyKeyTimes}
    else
        {drop_supported, false} ->
            {error, not_supported};
        {succ, {error, not_found}} ->
            {error, not_found};
        {except, Err} ->
            {error, Err}
    end.

dek_expiration_time(_, _, ?DEK_ERROR_PATTERN(_, _)) -> false;
dek_expiration_time(undefined, undefined, _) -> false;
dek_expiration_time(undefined, DropKeysTS,
                    #{type := 'raw-aes-gcm',
                      info := #{creation_time := CreationTime}}) ->
    %% Note: We should not treat keys with CreationTime == DropKeysTS as expired
    %% because newly created keys will be treated as expired then
    case CreationTime < DropKeysTS of
        true -> {value, DropKeysTS};
        false -> false
    end;
dek_expiration_time(LifetimeInSec, undefined,
                    #{type := 'raw-aes-gcm',
                      info := #{creation_time := CreationTime}}) ->
    {value, misc:datetime_add(CreationTime, LifetimeInSec)};
dek_expiration_time(LifetimeInSec, DropKeysTS,
                    #{type := 'raw-aes-gcm',
                      info := #{creation_time := CreationTime}}) ->
    ExpDT = misc:datetime_add(CreationTime, LifetimeInSec),
    %% Note: We should not treat keys with CreationTime == DropKeysTS as expired
    %% because newly created keys will be treated as expired then
    case CreationTime < DropKeysTS of
        true -> {value, min(ExpDT, DropKeysTS)};
        false -> {value, ExpDT}
    end.

-spec restart_dek_rotation_timer(#state{}) -> #state{}.
restart_dek_rotation_timer(#state{proc_type = ?MASTER_PROC} = State) ->
    State;
restart_dek_rotation_timer(#state{proc_type = ?NODE_PROC,
                                  deks_info = Deks} = State) ->
    CurDT = calendar:universal_time(),
    Times =
        maps:fold(fun (Kind, KindDeks, Acc) ->
                      Snapshot = deks_config_snapshot(Kind),
                      case dek_rotation_time(Kind, KindDeks, Snapshot) of
                          {value, now} -> [CurDT | Acc];
                          {value, ExpDT} -> [ExpDT | Acc];
                          false -> Acc;
                          {error, _} ->
                              [misc:datetime_add(CurDT,
                                                 ?DEK_TIMER_RETRY_TIME_S) | Acc]
                      end
                  end, [], Deks),
    TimerTime = time_to_first_event(CurDT, Times),
    restart_timer(rotate_deks, TimerTime, State).

-spec dek_rotation_needed(cb_deks:dek_kind(), deks_info(), calendar:datetime(),
                          chronicle_snapshot()) -> boolean().
dek_rotation_needed(Kind, KindDeks, CurDT, Snapshot) ->
    case dek_rotation_time(Kind, KindDeks, Snapshot) of
        {value, now} -> true;
        {value, ExpDT} -> ExpDT =< CurDT;
        false -> false;
        {error, _} -> false
    end.

-spec dek_rotation_time(cb_deks:dek_kind(), deks_info(),
                        chronicle_snapshot()) ->
          {value, calendar:datetime() | now} | false | {error, _}.
dek_rotation_time(_Kind, #{is_enabled := false}, _Snapshot) ->
    false;
dek_rotation_time(Kind, #{is_enabled := true, active_id := ActiveId,
                          deks := Keys}, Snapshot) ->
    maybe
        {value, #{type := 'raw-aes-gcm',
                  info := #{creation_time := CDT}}} ?=
            lists:search(fun (#{id := Id}) -> Id == ActiveId end, Keys),
        %% We should remove all keys that were created before this date:
        {succ, {ok, DKTS}} ?=
            cb_deks:call_dek_callback(get_drop_deks_timestamp, Kind,
                                      [Snapshot]),
        %% This is how often we should create new deks:
        {succ, {ok, RotationInt}} ?=
            cb_deks:call_dek_callback(get_deks_rotation_interval, Kind,
                                      [Snapshot]),

        %% Note: We should not treat keys with CDT == DKTS as expired
        %% because newly created keys will be treated as expired then
        Candidates = [DKTS || DKTS /= undefined, CDT < DKTS] ++
                     [misc:datetime_add(CDT, RotationInt)
                      || RotationInt /= undefined],
        case Candidates of
            [] -> false;
            [_ | _] -> {value, lists:min(Candidates)}
        end
    else
        {value, ?DEK_ERROR_PATTERN(_, _)} ->
            ?log_error("Active ~p dek ~p is not available (read error?), "
                       "will try to generate a new one", [Kind, ActiveId]),
            {value, now};
        {succ, {error, not_found}} -> false;
        {succ, {error, R}} ->
            ?log_error("Failed to calculate next rotation time for dek ~p: ~p",
                       [Kind, R]),
            {error, R};
        {except, {_, E, _}} ->
            ?log_error("Failed to calculate next rotation time for dek ~p: ~p",
                       [Kind, E]),
            {error, E}
    end.

-spec calculate_next_remove_retired_time(calendar:datetime()) ->
          non_neg_integer().
calculate_next_remove_retired_time({{Year, Month, _Day},
                                    {_Hour, _Min, _Sec}} = Now) ->
    %% We want to remove retired keys at 12:00:00 of the first day of next month
    %% Calculate first day of next month
    {NextYear, NextMonth} =
        case Month of
            12 -> {Year + 1, 1};
            _ -> {Year, Month + 1}
        end,
    NextTime = {{NextYear, NextMonth, 1}, {12, 0, 0}},
    CurrentSecs = calendar:datetime_to_gregorian_seconds(Now),
    NextSecs = calendar:datetime_to_gregorian_seconds(NextTime),
    (NextSecs - CurrentSecs) * 1000.

-spec restart_remove_retired_timer(#state{}) -> #state{}.
restart_remove_retired_timer(#state{proc_type = ?MASTER_PROC} = State) ->
    State;
restart_remove_retired_timer(#state{proc_type = ?NODE_PROC} = State) ->
    Time = calculate_next_remove_retired_time(calendar:universal_time()),
    restart_timer(remove_retired_keys, Time, State).


validate_for_config_encryption(#{type := T, data := Data}, Snapshot) ->
    case call_module_by_type(T, get_props_encryption_method, [Data]) of
        {secret, _} -> ok;
        disabled -> ok;
        encryption_service ->
            case cb_crypto:get_encryption_method(config_encryption, cluster,
                                                 Snapshot) of
                {ok, disabled} -> {error, config_encryption_disabled};
                {ok, _} -> ok
            end
    end.

-spec validate_encryption_secret_id(secret_props(), chronicle_snapshot()) ->
                    ok | {error, bad_encrypt_id()}.
validate_encryption_secret_id(#{type := T, data := Data}, Snapshot) ->
    case call_module_by_type(T, get_props_encryption_method, [Data]) of
        {secret, Id} ->
            case secret_can_encrypt_secrets(Id, Snapshot) of
                ok -> ok;
                {error, not_found} -> {error, {encrypt_id, not_found}};
                {error, not_allowed} -> {error, {encrypt_id, not_allowed}}
            end;
        disabled -> ok;
        encryption_service -> ok
    end.

-spec secret_can_encrypt_secrets(secret_id(), chronicle_snapshot()) ->
                                        ok | {error, not_found | not_allowed}.
secret_can_encrypt_secrets(SecretId, Snapshot) ->
    case get_secret(SecretId, Snapshot) of
        {ok, #{usage := Usage}} ->
            case lists:member(secrets_encryption, Usage) of
                true -> ok;
                false -> {error, not_allowed}
            end;
        {error, not_found} -> {error, not_found}
    end.

-spec validate_secrets_encryption_usage_change(secret_props(),
                                               #{} | secret_props(),
                                               chronicle_snapshot()) ->
                                            ok | {error, bad_usage_change()}.
validate_secrets_encryption_usage_change(NewProps, PrevProps, Snapshot) ->
    validate_if_usage_removed(
      secrets_encryption, NewProps, PrevProps,
      fun (Id) ->
          case secret_encrypts_other_secrets(Id, Snapshot) of
              true -> {error, {usage, in_use}};
              false -> ok
          end
      end).

validate_dek_related_usage_change(_NewProps, PrevProps, _Snapshot)
                                                        when PrevProps == #{} ->
    ok;
validate_dek_related_usage_change(NewProps, PrevProps, Snapshot) ->
    %% Make sure that all dek users are still allowed to use this secret.
    %% For example, say this secret encrypts bucket "a" and
    %% OldProps contains {bucket_encryption, A_UUID} in the usage field.
    %% Then the following changes would be allowed:
    %%   1. [{bucket_encryption, A_UUID}] -> [{bucket_encryption, <<"*">>}].
    %%   2. [{bucket_encryption, A_UUID}] -> [{bucket_encryption, A_UUID},
    %%                                     config_encryption].
    %% And the following changes would be disallowed:
    %%   1. [{bucket_encryption, A_UUID}] -> [{bucket_encryption, B_UUID}].
    %%   2. [{bucket_encryption, A_UUID}] -> [config_encryption].
    #{id := Id} = PrevProps,
    %% Check existing deks. If this secret still encrypts any deks, we should
    %% not allow corresponding usage removal
    KindsOfExistingDeks = get_dek_kinds_used_by_secret_id(Id, Snapshot),
    DekKindRequirements =
        fun (Kind) ->
            {succ, Requirement} = cb_deks:call_dek_callback(get_required_usage,
                                                            Kind, []),
            case lists:member(Kind, KindsOfExistingDeks) of
                true -> {true, Requirement};
                false ->
                    {succ, RV} = cb_deks:call_dek_callback(
                                   get_encryption_method, Kind,
                                   [cluster, Snapshot]),
                    case {ok, {secret, Id}} == RV of
                        true -> {true, Requirement};
                        false -> false
                    end
            end
        end,
    InUseList = lists:filtermap(DekKindRequirements,
                                cb_deks:dek_cluster_kinds_list(Snapshot)),
    NewUsageList = maps:get(usage, NewProps, []),
    case is_allowed(InUseList, NewUsageList) of
        true -> ok;
        false -> {error, {usage, in_use}}
    end.

is_allowed(Requirements, ListOfAllowedUsages) ->
    lists:all(
      fun (Req) ->
          lists:any(
            fun (Usage) ->
                case Req of
                    Usage -> true;
                    {bucket_encryption, _} ->
                        Usage == {bucket_encryption, <<"*">>};
                    _ -> false
                end
            end,
            ListOfAllowedUsages)
      end, Requirements).

-spec validate_if_usage_removed(secret_usage(), secret_props(),
                                #{} | secret_props(),
                                fun((secret_id()) -> ok | {error, term()})) ->
                                            ok | {error, bad_usage_change()}.
validate_if_usage_removed(Usage, NewProps, PrevProps, Fun) ->
    PrevUsage = maps:get(usage, PrevProps, []),
    NewUsage = maps:get(usage, NewProps, []),
    case (not lists:member(Usage, NewUsage)) andalso
         (lists:member(Usage, PrevUsage)) of
        true ->
            #{id := PrevId} = PrevProps,
            Fun(PrevId);
        false ->
            ok
    end.

-spec secret_encrypts_other_secrets(secret_id(), chronicle_snapshot()) ->
                                                                    boolean().
secret_encrypts_other_secrets(Id, Snapshot) ->
    lists:any(fun (#{type := T, data := Data}) ->
                    case call_module_by_type(T, get_props_encryption_method,
                                             [Data]) of
                        {secret, EncId} -> EncId == Id;
                        disabled -> false;
                        encryption_service -> false
                    end
              end, get_all(Snapshot)).

-spec can_secret_props_encrypt_dek_kind(secret_props(), cb_deks:dek_kind()) ->
          boolean().
can_secret_props_encrypt_dek_kind(#{usage := UsageList}, DekKind) ->
    {succ, Required} = cb_deks:call_dek_callback(get_required_usage, DekKind,
                                                 []),
    is_allowed([Required], UsageList).

-spec update_secrets(
        fun((secret_props()) -> {value, secret_props()} | false)) ->
          {ok, [UpdatedIds :: secret_id()]} | {error, no_quorum}.
update_secrets(Fun) ->
    RV = chronicle_transaction(
           [?CHRONICLE_SECRETS_KEY],
           fun (Snapshot) ->
               {NewList, ChangedIds} =
                   lists:mapfoldl(fun (#{id := Id} = S, Acc) ->
                                      case Fun(S) of
                                          {value, NewS} -> {NewS, [Id | Acc]};
                                          false -> {S, Acc}
                                      end
                                  end, [], get_all(Snapshot)),
               case ChangedIds of
                   [] -> {abort, no_change};
                   _ ->
                       ok = validate_secrets_consistency(NewList),
                       {commit,
                        [{set, ?CHRONICLE_SECRETS_KEY, NewList}],
                        ChangedIds}
               end
           end),
    case RV of
        {ok, UpdatedIds} -> {ok, UpdatedIds};
        no_change -> {ok, []};
        {error, _} = Error -> Error
    end.

-spec update_next_rotation_time(calendar:datetime(), secret_props()) ->
                                                {value, secret_props()} | false.
update_next_rotation_time(CurTime, #{type := T, data := Data} = Secret) ->
    case call_module_by_type(T, maybe_update_next_rotation_time_in_props,
                             [Data, CurTime]) of
        {ok, UpdatedData} -> {value, Secret#{data => UpdatedData}};
        no_change -> false;
        {error, not_supported} -> false
    end.

-spec sync_with_all_node_monitors() -> ok | {error, [atom()]}.
sync_with_all_node_monitors() ->
    Nodes = nodes_with_encryption_at_rest(ns_node_disco:nodes_actual()),
    Res = erpc:multicall(Nodes, ?MODULE, sync_with_node_monitor, [],
                         ?SYNC_TIMEOUT),
    BadNodes = lists:filtermap(
                 fun ({_Node, {ok, _}}) ->
                         false;
                     ({Node, {Class, Exception}}) ->
                         ?log_error("Node ~p sync failed: ~p ~p",
                                    [Node, Class, Exception]),
                         {true, Node}
                 end, lists:zip(Nodes, Res)),
    case BadNodes of
        [] -> ok;
        _ ->
            ?log_error("Sync failed, bad nodes: ~p", [BadNodes]),
            {error, BadNodes}
    end.

%% Every time we start using a secret to encrypt a dek, we increment a counter
%% in chronicle. This is needed so we always have an understanding what secrets
%% are used for what (for example we need this information in order to be able
%% to remove KEKs safely).
%%
%% Currently those counters look like the following:
%% #{ {secret, 23} => #{ configDek => 14,
%%                       {bucketDek, <<"beer-sample-uuid">>} => {2, 2345334}},
%%    {secret, 26} => #{ {bucketDek, <<"travel-sample-uuid">>} => {6, 835335} }}
%%
%% This function is supposed to cleanup these counters by basically removing
%% those dek types that don't use the secret anymore.
%% It asks all the nodes for deks information that they have. Then it calculates
%% what secrets are used to encrypt those deks. Then it removes all dek types
%% from the counters map that don't use the secret anymore.
-spec maybe_reset_deks_counters() ->
          ok | {error, retry | node_errors | missing_nodes | no_quorum}.
maybe_reset_deks_counters() ->
    case get_dek_counters(direct) of
        {CounterMap, _} when CounterMap == #{} ->
            ok;
        {CounterMap, _} ->
            case get_all_node_deks_info() of
                {ok, AllNodesDekInfo} ->
                    reset_dek_counters(CounterMap, AllNodesDekInfo);
                {error, _} ->
                    {error, retry}
            end
    end.

-spec historical_keys_to_remove(chronicle_snapshot()) ->
          [{secret_id(), string(), cb_deks:dek_id()}].
historical_keys_to_remove(Snapshot) ->
    lists:flatmap(fun (#{id := SecretId, name := SecretName, type := T,
                         data := Data}) ->
                      L = call_module_by_type(
                            T, historical_keys_to_remove_from_props, [Data]),
                      [{SecretId, SecretName, Id} || Id <- L]
                  end, get_all(Snapshot)).

-spec maybe_remove_historical_keys(#state{}) -> {ok, #state{}}.
maybe_remove_historical_keys(State) ->
    %% Currently the only type of historical keys we remove automatically is
    %% AWS. AWS keys are rotated by the AWS service and it is transparent for
    %% us, which means that AWS key ARN doesn't change, and that means that
    %% our keys will basically all be the same (the only difference is their
    %% UUIDs). We generate new UUIDs to keep track which data has already been
    %% re-encrypted after each AWS key rotation. After re-encryption is
    %% complete, we can remove the historical keys as they are no longer needed.
    maybe
        {_, false} ?= {timer, is_timer_started(remove_historical_keys, State)},
        Snapshot = chronicle_compat:get_snapshot(
                     [fun fetch_snapshot_in_txn/1], #{}),
        HistoricalKeysToRemove = historical_keys_to_remove(Snapshot),
        [_ | _] ?= HistoricalKeysToRemove,
        ?log_debug("There are ~p historical keys to remove: ~0p",
                   [length(HistoricalKeysToRemove), HistoricalKeysToRemove]),
        {_, CountersRev} = get_dek_counters(Snapshot),
        {ok, AllNodesDekInfo} ?= get_all_node_deks_info(),
        %% Errors are logged in delete_historical_key_without_sync
        _ = delete_historical_keys_internal(
                    HistoricalKeysToRemove,
                    fun (_, _) -> true end,
                    AllNodesDekInfo,
                    CountersRev,
                    Snapshot,
                    true),
        {ok, ensure_remove_historical_keys_timer(State)}
    else
        {timer, true} ->
            %% Timer is already started, we should wait until it expires
            %% in order to avoid doing too many attempts
            {ok, State};
        [] ->
            {ok, State};
        {error, _Reason} ->
            {ok, ensure_remove_historical_keys_timer(State)}
    end.

-spec get_dek_counters(chronicle_snapshot()) ->
          {dek_encryption_counters(), undefined | chronicle:revision()}.
get_dek_counters(direct) ->
    case chronicle_kv:get(kv, ?CHRONICLE_DEK_COUNTERS_KEY) of
        {ok, Value} ->
            Value;
        {error, not_found} ->
            {#{}, undefined}
    end;
get_dek_counters(Snapshot) ->
    case maps:find(?CHRONICLE_DEK_COUNTERS_KEY, Snapshot) of
        {ok, Value} ->
            Value;
        error ->
            {#{}, undefined}
    end.

-spec get_all_node_deks_info() ->
          {ok, #{cb_deks:dek_kind() => [cb_deks:dek_meta()]}} |
          {error, _}.
get_all_node_deks_info() ->
    AllNodes = ns_node_disco:nodes_wanted(),
    MissingNodes = AllNodes -- ns_node_disco:nodes_actual(),
    case MissingNodes of
        [] ->
            %% Each node returns information about its deks
            %% So we can calculate which secrets are actually in use
            %% and update that information in chronicle
            Res = erpc:multicall(AllNodes, ?MODULE, get_node_deks_info,
                                    [], ?DEK_COUNTERS_UPDATE_TIMEOUT),
            {NonErrors, AllErrors} =
                misc:partitionmap(fun ({N, {ok, R}}) -> {left, {N, R}};
                                      ({N, E}) -> {right, {N, E}}
                                  end, lists:zip(AllNodes, Res)),

            Errors =
                lists:filter(
                  fun ({Node, {error, {exception, undef,
                                        [{cb_cluster_secrets,
                                          get_node_deks_info, _, _}]}}}) ->
                          NodeInfo = ns_doctor:get_node(Node),
                          case node_supports_encryption_at_rest(NodeInfo) of
                              no_info -> true;
                              true -> true;
                              false ->
                                  %% We can ignore the error because that node
                                  %% doesn't support encryption-at-rest
                                  %% so it doesn't have any DEKs
                                  false
                          end;
                      ({_N, _}) ->
                          true
                  end, AllErrors),

            %% If some deks have issues we should not remove anything
            %% until those issues are resolved
            Issues =
                lists:filtermap(
                    fun ({N, L}) ->
                        case maps:filtermap(
                               fun (_, #{issues := I}) ->
                                   case length(I) > 0 of
                                       true -> {true, I};
                                       false -> false
                                   end
                               end, L) of
                            M when map_size(M) > 0 -> {true, {N, M}};
                            _ -> false
                        end
                    end, NonErrors),
            ShouldRetry = length(Issues) > 0,

            OnlyKeys =
                lists:map(
                    fun ({_Node, FullInfo}) ->
                        maps:filtermap(fun (_, #{deks := K}) -> {true, K};
                                           (_, #{}) -> false
                                       end, FullInfo)
                    end, NonErrors),

            case {Errors, ShouldRetry} of
                {[], false} ->
                    %% Merge deks from all nodes into one map
                    {ok, lists:foldl(
                                fun (M, Acc) ->
                                    maps:merge_with(fun (_, V1, V2) ->
                                                        V1 ++ V2
                                                    end, M, Acc)
                                end, #{}, OnlyKeys)};
                {[], true} ->
                    ?log_debug("Some deks have issues.~n"
                               "Issues: ~p", [Issues]),
                    {error, deks_issues};
                {Errors, _} ->
                    ?log_error("Failed to get deks info from some nodes: ~p",
                               [Errors]),
                    {error, node_errors}
            end;
        _ ->
            ?log_debug("Skipping deks info collection because some nodes "
                       "are missing: ~p", [MissingNodes]),
            {error, missing_nodes}
    end.

-spec reset_dek_counters(dek_encryption_counters(),
                         #{cb_deks:dek_kind() => [cb_deks:dek_meta()]}) ->
          ok | {error, no_quorum}.
reset_dek_counters(OldCountersMap, ActualDeksUsageInfo) ->
    Res =
        chronicle_transaction(
          [?CHRONICLE_SECRETS_KEY, ?CHRONICLE_DEK_COUNTERS_KEY,
           ?CHRONICLE_DEK_COUNTERS_TIME_KEY],
          fun (Snapshot) ->
              reset_dek_counters_txn(OldCountersMap, ActualDeksUsageInfo,
                                     Snapshot)
          end),

    case Res of
        ok -> ok;
        {error, _} = Error -> Error
    end.

reset_dek_counters_txn(OldCountersMap, ActualDeksUsageInfo, Snapshot) ->
    KeksToSecrets = get_secret_by_kek_id_map(Snapshot),
    GetEncryptionMethod =
        fun (#{type := 'raw-aes-gcm',
               info := #{encryption_key_id := <<"encryptionService">>}}) ->
                {ok, encryption_service};
             (#{type := 'raw-aes-gcm',
               info := #{encryption_key_id := CurKekId}}) ->
                case maps:find(CurKekId, KeksToSecrets) of
                    {ok, SId} -> {ok, {secret, SId}};
                    error -> error
                end
        end,

    %% Turn #{DekKind => [Dek]} map into #{DekKind => [{secret, SecretId}]} map
    %% It represents information about which dek type uses which secret.
    SecretInfo =
        maps:map(fun (_K, Deks) ->
                     lists:filtermap(
                       fun (D) ->
                           case GetEncryptionMethod(D) of
                               {ok, Res} -> {true, Res};
                               error ->
                                   ?log_error("orphaned dek: ~p", [D]),
                                   false
                           end
                       end, Deks)
                 end, ActualDeksUsageInfo),

    DekStillUsesSecretId =
        fun (DekKind, SecretId) ->
            SecretsIdsList = maps:get(DekKind, SecretInfo, []),
            lists:member(SecretId, SecretsIdsList)
        end,

    %% Filter out those dek types that do not use that secretId anymore.
    FilterCountersForSecret =
        fun (SecretId, Map) -> %% The Map variable here represents all
                               %% dek types that still uses that secret
            maybe
                {ok, OldMap} ?= maps:find(SecretId, OldCountersMap),
                NewMap = maps:filter(
                           fun (DekKind, CounterWithRev) ->
                               DekStillUsesSecretId(DekKind, SecretId) orelse
                               %% Checking if counter has changed since before
                               %% we started deks info aggregation;
                               %% If so, that means that something started just
                               %% started using it and we should not remove it
                               %% from the map
                               (CounterWithRev /= maps:get(DekKind, OldMap,
                                                           {0, undefined}))
                           end, Map),
                case maps:size(NewMap) of
                    0 -> false;
                    _ -> {true, NewMap}
                end
            else
                %% This SecretId was missing in the first check, this means
                %% that something just started using that secret
                error -> true
            end
        end,

    {Old, _} = get_dek_counters(Snapshot),
    New = maps:filtermap(FilterCountersForSecret, Old),
    UpdateTime = calendar:universal_time(),
    case New == Old of
        true ->
            {commit,
             [{set, ?CHRONICLE_DEK_COUNTERS_TIME_KEY, UpdateTime}]};
        false ->
            {commit,
             [{set, ?CHRONICLE_DEK_COUNTERS_KEY, New},
              {set, ?CHRONICLE_DEK_COUNTERS_TIME_KEY, UpdateTime}]}
    end.

%% Fetches a snapshot in transaction with all dek related chronicle keys,
%% and all secrets related chronicle keys.
fetch_snapshot_in_txn(Txn) ->
    BucketListSnapshot = ns_bucket:fetch_snapshot(all, Txn, [uuid]),
    DeksRelatedSnapshot =
        lists:foldl(
          fun (Kind, Acc) ->
              FetchedKeys = cb_deks:call_dek_callback_unsafe(
                              fetch_chronicle_keys_in_txn, Kind, [Txn]),
              maps:merge(Acc, FetchedKeys)
          end,
          BucketListSnapshot,
          cb_deks:dek_cluster_kinds_list(BucketListSnapshot)),
    SecretsSnapshot = chronicle_compat:txn_get_many(
                        [?CHRONICLE_SECRETS_KEY,
                         ?CHRONICLE_DEK_COUNTERS_KEY,
                         ?CHRONICLE_DEK_COUNTERS_TIME_KEY,
                         ?CHRONICLE_NEXT_ID_KEY],
                        Txn),
    maps:merge(DeksRelatedSnapshot, SecretsSnapshot).

-spec deks_to_drop(cb_deks:dek_kind(), deks_info() | #state{}) ->
          [cb_deks:dek_id() | ?NULL_DEK].
deks_to_drop(Kind, #state{deks_info = DeksInfo}) ->
    case maps:find(Kind, DeksInfo) of
        {ok, KindDeks} -> deks_to_drop(Kind, KindDeks);
        error -> []
    end;
deks_to_drop(Kind, KindDeks) ->
    CurTime = calendar:universal_time(),
    NowS = calendar:datetime_to_gregorian_seconds(CurTime),
    ExpiredIds = get_expired_deks(Kind, KindDeks),
    ExcessiveIds = case ExpiredIds of %% reduce deks num if needed
                       [] -> excessive_deks_to_drop(Kind, KindDeks);
                       _ -> []
                   end,
    #{deks_being_dropped := AlreadyBeingDroppedSet,
      last_drop_timestamp := LastDropS} = KindDeks,
    DropRetryInterval = ?DEK_DROP_RETRY_TIME_S(Kind),
    LastDropTime = case LastDropS of
                       undefined -> undefined;
                       _ -> calendar:gregorian_seconds_to_datetime(LastDropS)
                   end,
    AlreadyBeingDroppedList = sets:to_list(AlreadyBeingDroppedSet),
    ?log_debug("The following ~p DEKs has expired: ~p (excessive: ~p)~n"
               "Among them DEKs that are already being dropped: ~p~n"
               "Last drop attempt time: ~p",
               [Kind, ExpiredIds, ExcessiveIds, AlreadyBeingDroppedList,
                LastDropTime]),
    %% If we have already started dropping something, we should continue
    %% even if it is not "expired" anymore.
    AllExpired = lists:usort(ExpiredIds ++ AlreadyBeingDroppedList ++
                             ExcessiveIds),
    ShouldAttemptDrop =
        case AllExpired -- AlreadyBeingDroppedList of
            [_|_] ->
                true; %% there are new deks in the list
            [] when AllExpired == [] ->
                false; %% no deks to drop
            [] when NowS > LastDropS + DropRetryInterval ->
                true; %% no new deks to drop, but it's been a while
                      %% since last drop attempt; will retry
            [] ->
                false %% no new deks to drop
        end,
    case ShouldAttemptDrop of
        true -> AllExpired;
        false -> []
    end.

-spec initiate_deks_drop(cb_deks:dek_kind(), [cb_deks:dek_id() | ?NULL_DEK],
                         #state{}) -> #state{}.
initiate_deks_drop(_Kind, [], #state{} = State) -> State;
initiate_deks_drop(Kind, IdsToDropList0,
                   #state{deks_info = DeksInfo} = State0) ->
    CurTime = calendar:universal_time(),
    IdsToDropSet0 = sets:from_list(IdsToDropList0, [{version, 2}]),
    NowS = calendar:datetime_to_gregorian_seconds(CurTime),
    #{Kind := #{deks_being_dropped := BeingDroppedSet} = KindDeks} = DeksInfo,
    IdsToDropSet1 = %% Don't let active dek to be dropped
        case DeksInfo of
            #{Kind := #{is_enabled := true, active_id := ActiveId}} ->
                case sets:is_element(ActiveId, IdsToDropSet0) of
                    true ->
                        ?log_debug("Active DEK (~p) has expired for "
                                   "~p (ignoring attempt to drop it)",
                                   [ActiveId, Kind]),
                        sets:del_element(ActiveId, IdsToDropSet0);
                    false -> IdsToDropSet0
                end;
            #{Kind := #{is_enabled := false}} ->
                IdsToDropSet0
        end,
    IdsToDropFinalSet =
        case DeksInfo of
            #{Kind := #{is_enabled := true}} ->
                %% We have at least one expired dek, and encryption is enabled,
                %% it is probably time to encrypt data that is not encrypted yet
                %% (if there is such data)
                case sets:size(IdsToDropSet1) > 0 of
                    true -> sets:add_element(?NULL_DEK, IdsToDropSet1);
                    false -> IdsToDropSet1
                end;
            #{Kind := #{is_enabled := false}} ->
                %% If encryption is disabled we should never try dropping empty
                %% dek because it beasically means "encrypt everything", which
                %% doesn't make sense in this case
                sets:del_element(?NULL_DEK, IdsToDropSet1)
        end,
    IdsToDropFinalList = sets:to_list(IdsToDropFinalSet),

    ?log_debug("Trying to drop ~p DEKs: ~0p", [Kind, IdsToDropFinalList]),

    notify_kind_counter(<<"encr_at_rest_drop_deks_events">>, Kind),

    log_expired_deks(encr_at_rest_deks_expired, Kind,
                     sets:subtract(IdsToDropSet0, BeingDroppedSet)),
    log_expired_deks(encr_at_rest_expired_deks_drop_failed, Kind,
                     BeingDroppedSet),
    case (length(IdsToDropFinalList) > 0) andalso
         cb_deks:call_dek_callback(initiate_drop_deks, Kind,
                                   [IdsToDropFinalList,
                                    deks_config_snapshot(Kind)],
                                   #{verbose => true}) of
        false ->
            %% IdsToDrop0 was not empty, but the final list is empty (we
            %% probably removed NULL_DEK or ActiveId), so we should not attempt
            %% to drop anything.
            %% We should update last_drop_timestamp to indicate that we checked
            %% it and drop is not needed (so we don't not try to drop it again
            %% immediately). Example: when we have active dek expired, we can't
            %% drop it, but we should not re-try dropping it again and again.
            %% By updating last_drop_timestamp we are making sure that
            %% ActiveId will not be treated as expired immediately.
            NewKindDeks = KindDeks#{deks_being_dropped => IdsToDropSet0,
                                    last_drop_timestamp => NowS},
            State0#state{deks_info = DeksInfo#{Kind => NewKindDeks}};
        {succ, {ok, Res}} ->
            %% Setting deks_being_dropped => IdsToDropSet0 on purpose
            %% because we want to memorize information that we tried to drop
            %% active dek, so we don't retry too often.
            NewKindDeks = KindDeks#{deks_being_dropped => IdsToDropSet0,
                                    last_drop_timestamp => NowS},
            State = State0#state{deks_info = DeksInfo#{Kind => NewKindDeks}},
            case Res of
                done ->
                    %% 'done' means that all all ids have been dropped and it is
                    %% safe to remove them
                    AllIds = [Id || #{id := Id} <- maps:get(deks, KindDeks)],
                    IdsInUse = AllIds -- IdsToDropFinalList,
                    retire_unused_deks(Kind, IdsInUse, State);
                started ->
                    State
            end;
        {succ, {error, not_found}} -> State0;
        {succ, {error, retry}} -> State0; %% compaction daemon not started yet
        {succ, {error, Reason}} ->
            log_expired_deks(encr_at_rest_expired_deks_drop_failed, Kind,
                             sets:subtract(IdsToDropFinalSet, BeingDroppedSet)),
            ?log_error("initiate_drop_deks for ~p returned error: ~p",
                       [Kind, Reason]),
            State0;
        {except, _} ->
            log_expired_deks(encr_at_rest_expired_deks_drop_failed, Kind,
                             sets:subtract(IdsToDropFinalSet, BeingDroppedSet)),
            State0
    end.

-spec validate_name_uniqueness(secret_props(), chronicle_snapshot()) ->
          ok | {error, name_not_unique}.
validate_name_uniqueness(#{id := Id, name := Name}, Snapshot) ->
    case is_name_unique(Id, Name, Snapshot) of
        true -> ok;
        false -> {error, name_not_unique}
    end.

-spec validate_secret_usages(secret_props(), chronicle_snapshot()) ->
          ok | {error, bucket_not_found}.
validate_secret_usages(#{usage := Usages}, Snapshot) ->
    %% We validated usages when we were parsing it, but since then buckets
    %% might have been deleted, so we need to validate them again in txn
    validate_usages(Usages, Snapshot).

validate_usages([], _Snapshot) -> ok;
validate_usages([Usage | Rest], Snapshot) ->
    Res =
        case Usage of
            {bucket_encryption, <<"*">>} -> ok;
            {bucket_encryption, BucketUUID} ->
                case ns_bucket:uuid2bucket(BucketUUID, Snapshot) of
                    {ok, _} -> ok;
                    {error, not_found} -> {error, bucket_not_found}
                end;
            U when U =:= config_encryption; U =:= audit_encryption;
                   U =:= log_encryption; U =:= secrets_encryption ->
                ok
        end,
    maybe
        ok ?= Res,
        validate_usages(Rest, Snapshot)
    end.

-spec validate_secrets_consistency([secret_props()]) ->
          ok | {error, inconsistent_graph()}.
validate_secrets_consistency(Secrets) ->
    %% Make sure secrets graph has no cycles and all ids that are
    %% mentioned in props are actually present
    with_secrets_in_digraph(Secrets, fun (_) -> ok end).

topologically_sorted_secrets(Secrets) ->
    with_secrets_in_digraph(Secrets, fun (G) ->
        case digraph_utils:topsort(G) of
            false -> {error, no_topological_sort};
            SortedIds ->
                SecretLookup =
                    fun (Id) ->
                            {value, Secret} = lists:search(
                                                fun (#{id := Id2}) ->
                                                    Id =:= Id2
                                                end, Secrets),
                            Secret
                    end,
                {ok, lists:map(SecretLookup, SortedIds)}
        end
    end).

with_secrets_in_digraph(Secrets, Fun) ->
    G = digraph:new([acyclic]),
    try
        lists:foreach(fun (#{id := Id}) -> digraph:add_vertex(G, Id) end,
                      Secrets),
        Res = lists:foldl(
                fun (#{}, {error, _} = Acc) -> Acc;
                    (#{id := Id} = P, ok) ->
                      lists:foldl(
                        fun (_Id2, {error, _} = Acc2) -> Acc2;
                            (Id2, ok) ->
                            case digraph:add_edge(G, Id2, Id) of
                                ['$e' | _] -> ok;
                                {error, _} = E -> E
                            end
                        end, ok, get_secrets_that_encrypt_props(P))
                end, ok, Secrets),
        case Res of
            ok -> Fun(G);
            {error, {bad_edge, Ids}} -> {error, {cycle, Ids}};
            {error, {bad_vertex, Id}} -> {error, {unknown_id, Id}}
        end
    after
        digraph:delete(G)
    end.

-spec sanitize_secret(secret_props()) -> term().
sanitize_secret(#{type := T, data := Data} = S) ->
    S#{data => call_module_by_type(T, sanitize_props, [Data])}.

-spec extract_dek_info(cb_deks:dek_kind(), #state{}) ->
          {ok, external_dek_info()} | {error, not_found}.
extract_dek_info(Kind, #state{deks_info = DeksInfo}) ->
    PreprocessKeys =
        fun (Keys) -> %% Filter out keys that we couldn't read and remove key
                      %% material for security reasons
            lists:filtermap(
              fun (#{type := 'raw-aes-gcm', info := Info} = K) ->
                      {true, K#{info => maps:remove(key, Info)}};
                  (?DEK_ERROR_PATTERN(_, _)) ->
                      false
              end, Keys)
        end,

    maybe
        {ok, #{has_unencrypted_data := HasUnencryptedData,
               statuses := Statuses,
               deks := Keys}} ?= maps:find(Kind, DeksInfo),
        Issues = maps:fold(fun (_J, ok, Acc) -> Acc;
                               (J, retry, Acc) -> [{J, pending} | Acc];
                               (J, {error, _}, Acc) -> [{J, failed} | Acc]
                           end, [], Statuses),
        CreationTime = fun (#{type := 'raw-aes-gcm',
                              info := #{creation_time := CT}}) -> CT
                       end,
        HasEncryptedData = length(Keys) > 0,
        Status = case {HasUnencryptedData, HasEncryptedData} of
                     {undefined, _} -> unknown;
                     {true, false} -> unencrypted;
                     {false, true} -> encrypted;
                     {true, true} -> partially_encrypted;
                     {false, false} -> unknown
                 end,
        PreprocessedKeys = PreprocessKeys(Keys),
        Res = #{data_status => Status,
                issues => Issues,
                deks => PreprocessedKeys,
                dek_num => length(Keys)},
        case PreprocessedKeys of
            [] -> {ok, Res};
            _ ->
                MinCreationTime = lists:min([CreationTime(D) ||
                                             D <- PreprocessedKeys]),
                {ok, Res#{oldest_dek_datetime => MinCreationTime}}
        end
    else
        error -> {error, not_found}
    end.

chronicle_transaction(Keys, Fun) ->
    try chronicle_kv:transaction(kv, Keys, Fun) of
        {ok, _Rev} -> ok;
        {ok, _Rev, Res} -> {ok, Res};
        Else -> Else
    catch
        exit:timeout ->
            ?log_error("Chronicle transaction failed with reason timeout"),
            {error, no_quorum}
    end.

chronicle_compat_txn(Fun) ->
    chronicle_compat_txn(Fun, #{}).
chronicle_compat_txn(Fun, Opts) ->
    try chronicle_compat:txn(Fun, Opts) of
        {ok, _Rev} -> ok;
        {ok, _Rev, Res} -> {ok, Res};
        Else -> Else
    catch
        exit:timeout ->
            ?log_error("Chronicle transaction failed with reason timeout"),
            {error, no_quorum}
    end.

-spec delete_historical_key_txn(secret_id(), key_id(),
                                fun((secret_props(),
                                     chronicle_snapshot()) -> boolean()),
                                chronicle_snapshot()) ->
          {commit, [{set, ?CHRONICLE_SECRETS_KEY, secret_props()}]} |
          {abort, {error, not_found | secret_in_use() | forbidden |
                          inconsistent_graph() | no_quorum | active_key}}.
delete_historical_key_txn(SecretId, HistKeyId, IsSecretWritableFun, Snapshot) ->
    maybe
        {ok, Props} ?= get_secret(SecretId, Snapshot),
        {_, true} ?= {writable, IsSecretWritableFun(Props, Snapshot)},
        SecretIds = get_secrets_encrypted_by_key_id(HistKeyId, Snapshot),
        {_, []} ?= {in_use, SecretIds},
        {ok, NewProps} ?= remove_historical_key_from_props(Props, HistKeyId),
        %% Update secret list
        CurSecrets = get_all(Snapshot),
        NewSecrets = replace_secret_in_list(NewProps, CurSecrets),
        ok ?= validate_secrets_consistency(NewSecrets),
        {commit, [{set, ?CHRONICLE_SECRETS_KEY, NewSecrets}]}
    else
        {error, _} = Error -> {abort, Error};
        {writable, false} -> {abort, {error, forbidden}};
        {in_use, Ids} -> {abort, {error, {used_by, #{by_secret => Ids}}}}
    end.

-spec check_key_id_usage(key_id(),
                         #{cb_deks:dek_kind() := [cb_deks:dek()]}) ->
          not_in_use | {in_use, [cb_deks:dek_kind()]}.
check_key_id_usage(KeyId, AllNodesDekInfo) ->
    UsedBy = maps:fold(
        fun(Kind, Deks, Acc) ->
            case lists:any(
                fun(#{info := #{encryption_key_id := K}}) -> K =:= KeyId;
                   (_) -> false
                end, Deks) of
                true -> [Kind | Acc];
                false -> Acc
            end
        end, [], AllNodesDekInfo),
    case UsedBy of
        [] -> not_in_use;
        KindList -> {in_use, KindList}
    end.

-spec remove_historical_key_from_props(secret_props(), key_id()) ->
          {ok, secret_props()} | {error, not_found | active_key}.
remove_historical_key_from_props(#{type := T, data := Data} = Props, KeyId) ->
    maybe
        {ok, NewData} ?= call_module_by_type(T,
                                             remove_historical_key_from_props,
                                             [Data, KeyId]),
        {ok, Props#{data => NewData}}
    end.

-spec calculate_dek_info(#state{}) ->
          {#{cb_deks:dek_kind() => external_dek_info()}, #state{}}.
calculate_dek_info(State) ->
    #state{deks_info = Deks} = State,
    Kinds = maps:keys(Deks),
    {Res, NewState} =
        lists:foldl(
          fun (Kind, {ResAcc, StateAcc}) ->
              %% Run gc for deks; it is usefull in case if a compaction
              %% has been run recently
              Snapshot = deks_config_snapshot(Kind),
              NewStateAcc = maybe_garbage_collect_deks(Kind, false, Snapshot,
                                                       StateAcc),
              case extract_dek_info(Kind, NewStateAcc) of
                  {ok, I} ->
                      report_data_status_stats(Kind, I),
                      {ResAcc#{Kind => I}, NewStateAcc};
                  {error, not_found} -> {ResAcc, NewStateAcc}
              end
          end, {#{}, State}, Kinds),
    ets:insert(?MODULE, {deks_info, {erlang:monotonic_time(second), Res}}),
    %% We can always flush calculate_dek_info message after
    %% calculate_dek_info() has finished because we know we just updated the
    %% info.
    %% Note calculate_dek_info() can modify the state
    %% (in garbage_collect_deks()), which means it can put another
    %% calculate_dek_info message into this process's mailbox.
    misc:flush(calculate_dek_info),
    {Res, NewState}.

-spec dummy_deks_info(dek_info_data_status(), [dek_issue()]) ->
          #{cb_deks:dek_kind() := external_dek_info()}.
dummy_deks_info(DataStatus, Issues) ->
    Kinds = cb_deks:dek_kinds_list_existing_on_node(direct),
    maps:from_list(
      lists:map(
        fun (K) ->
            {K, #{data_status => DataStatus,
                  issues => Issues}}
        end, Kinds)).

log_succ_kek_rotation(Id, Name, IsAutomatic) ->
    ale:info(?USER_LOGGER,
             "Encryption key \"~s\" (~p) has been rotated successfully",
             [Name, Id]),
    ns_server_stats:notify_counter(
      {<<"encryption_key_rotations">>, [{key_name, Name}]}),
    event_log:add_log(encryption_key_rotated,
                      [{encryption_key_id, Id},
                       {encryption_key_name, iolist_to_binary(Name)},
                       {is_automatic, IsAutomatic}]).

log_unsucc_kek_rotation(Id, Name, Reason, IsAutomatic) ->
    ale:error(?USER_LOGGER,
              "Encryption key \"~s\" (~p) rotation FAILED: \"~s\".",
              [Name, Id, menelaus_web_secrets:format_error(Reason)]),
    ns_server_stats:notify_counter({<<"encryption_key_rotation_failures">>,
                                   [{key_name, Name}]}),
    event_log:add_log(encryption_key_rotation_failed,
                      [{encryption_key_id, Id},
                       {encryption_key_name, iolist_to_binary(Name)},
                       {is_automatic, IsAutomatic},
                       {reason, format_failure_reason(Reason)}]).

log_succ_dek_rotation(Kind, NewDekId) ->
    ale:info(?USER_LOGGER, "DEK for ~s has been rotated successfully",
             [cb_deks:kind2bin(Kind, <<"unknown">>)]),
    notify_kind_counter(<<"encr_at_rest_generate_dek">>, Kind),
    event_log:add_log(encr_at_rest_dek_rotated,
                      [{kind, cb_deks:kind2bin(Kind, <<"unknown">>)},
                       {new_DEK_UUID, NewDekId}]).

log_unsucc_dek_rotation(Kind, Reason) ->
    DataTypeName = try cb_deks:kind2datatype(Kind)
                   catch error:not_found -> <<"unknown">>
                   end,
    ale:error(?USER_LOGGER, "DEK rotation failed for ~s: ~s",
              [DataTypeName, menelaus_web_secrets:format_error(Reason)]),
    notify_kind_counter(<<"encr_at_rest_generate_dek_failures">>, Kind),
    event_log:add_log(encr_at_rest_dek_rotation_failed,
                      [{kind, cb_deks:kind2bin(Kind, <<"unknown">>)},
                       {reason, format_failure_reason(Reason)}]).

log_unsucc_hist_key_removal(SecretId, SecretName, HistKeyId, Reason,
                            IsAutomatic) ->
    ale:error(?USER_LOGGER,
              "Failed to remove historical key info  (~p "
              "for secret \"~s\" (~p): ~s",
              [HistKeyId, SecretName, SecretId,
               menelaus_web_secrets:format_error(Reason)]),
    %% Notify stats only for automatic removals to avoid triggering
    %% alerts for user-initiated removals.
    %% User-initiated removals are not considered part of the key
    %% rotation process, so we should not increment rotation failures counter.
    %% Automatic removals, on the other hand, happen automatically after
    %% rotation, so they are considered part of the key rotation process, hence
    %% the increment of the rotation failures counter is justified.
    IsAutomatic andalso
        ns_server_stats:notify_counter(
          {<<"encryption_key_rotation_failures">>,
           [{key_name, SecretName}]}),
    event_log:add_log(historical_encryption_key_deletion_failed,
                      [{encryption_key_id, SecretId},
                       {encryption_key_name, iolist_to_binary(SecretName)},
                       {historical_key_UUID, HistKeyId},
                       {is_automatic, IsAutomatic},
                       {reason, format_failure_reason(Reason)}]).

log_expired_deks(Type, Kind, IdsSet) ->
    Ids = sets:to_list(sets:del_element(?NULL_DEK, IdsSet)),
    case Ids of
        [] -> ok;
        _ -> event_log:add_log(Type,
                               [{'DEK_UUIDs', Ids},
                                {kind, cb_deks:kind2bin(Kind, <<"unknown">>)}])
    end.

format_failure_reason(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason], [{chars_limit, 200}])).

-spec report_data_status_stats(cb_deks:dek_kind(), external_dek_info()) -> ok.
report_data_status_stats(Kind, #{data_status := DataStatus}) ->
    N = case DataStatus of
            unknown -> -1;
            unencrypted -> 0;
            partially_encrypted -> 0.5;
            encrypted -> 1
        end,
    try
        ns_server_stats:notify_gauge(
          {<<"encr_at_rest_data_status">>,
           [{data_type, cb_deks:kind2datatype(Kind)}]}, N)
    catch
        error:not_found ->
            ok
    end.

create_kind_stats(Kind) ->
    try cb_deks:kind2bin(Kind) of
        KindBin ->
            lists:foreach(
              fun (M) ->
                  ns_server_stats:create_counter({M, [{type, KindBin}]})
              end, all_kind_stat_names())
    catch
        error:not_found ->
            ok
    end.

delete_kind_stats(Kind) ->
    try cb_deks:kind2bin(Kind) of
        KindBin ->
            lists:foreach(
              fun (M) ->
                  ns_server_stats:delete_counter({M, [{type, KindBin}]})
              end, all_kind_stat_names())
    catch
        error:not_found ->
            ok
    end.

notify_kind_counter(Counter, Kind) ->
    try
        ns_server_stats:notify_counter(
          {Counter, [{type, cb_deks:kind2bin(Kind)}]})
    catch
        %% Bucket for this Kind does not exist (bucket already deleted but we
        %% don't know about it yet)
        error:not_found -> ok
    end.

notify_kind_gauge(Gauge, Kind, Val, Opts) ->
    try
        ns_server_stats:notify_gauge(
          {Gauge, [{type, cb_deks:kind2bin(Kind)}]}, Val, Opts)
    catch
        %% Bucket for this Kind does not exist (bucket already deleted but we
        %% don't know about it yet)
        error:not_found -> ok
    end.

all_kind_stat_names() ->
    [<<"encr_at_rest_generate_dek">>,
     <<"encr_at_rest_generate_dek_failures">>,
     <<"encr_at_rest_drop_deks_events">>,
     <<"encr_at_rest_retire_key_events">>,
     <<"encr_at_rest_retire_key_failures">>,
     <<"encr_at_rest_deks_imported">>,
     <<"encr_at_rest_deks_import_skipped">>,
     <<"encr_at_rest_deks_import_failures">>].

create_encryption_key_stats(Name) ->
    ns_server_stats:create_counter(
        {<<"encryption_key_rotation_failures">>, [{key_name, Name}]}),
    ns_server_stats:create_counter(
        {<<"encryption_key_rotations">>, [{key_name, Name}]}).

garbage_collect_key_stats() ->
    CurList = lists:map(fun (#{name := Name}) ->
                            list_to_binary(Name)
                        end, get_all()),
    GC = fun (MetricName) ->
             ns_server_stats:garbage_collect_counters(
               MetricName,
               fun (<<"key_name">>, Name) -> not lists:member(Name, CurList);
                   (_, _) -> false
               end)
         end,
    GC(<<"encryption_key_rotation_failures">>),
    GC(<<"encryption_key_rotations">>).

diag_info_helper(Name, undefined) ->
    io_lib:format("~s process is not running", [Name]);
diag_info_helper(Name, Pid) ->
     try
         gen_server:call(Pid, diag, 5000)
     catch
         exit:{noproc, _} ->
             io_lib:format("~s process is not running", [Name]);
         exit:{timeout, _} ->
             case erlang:process_info(Pid, [backtrace]) of
                 undefined ->
                     io_lib:format("~s process diag info timed out. "
                                   "Process backtrace: undefined", [Name]);
                 [{backtrace, Backtrace}] ->
                     io_lib:format("~s process diag info timed out. "
                                   "Process backtrace:~n~s",
                                   [Name, Backtrace])
             end;
        _:E ->
             io_lib:format("Failed to get diag info from ~s: ~0p", [Name, E])
     end.

diag(#state{proc_type = ?NODE_PROC} = State) ->
    [<<"Process type: node ">>, io_lib:format("(~p)", [self()]), $\n,
     diag_deks(State#state.deks_info), $\n,
     diag_timers(State#state.timers, State#state.timers_trigger_ts), $\n,
     diag_jobs(State#state.jobs), $\n,
     diag_cached_keys_list(encryption_service:cached_keys_list())];
diag(#state{proc_type = ?MASTER_PROC} = State) ->
    [<<"Process type: master ">>, io_lib:format("(~p)", [self()]), $\n,
     diag_timers(State#state.timers, State#state.timers_trigger_ts), $\n,
     diag_jobs(State#state.jobs)].

%% Helper functions for diag
diag_deks(DeksMap) ->
    Now = calendar:universal_time(),
    [<<"DEKs Info:">>, $\n,
     lists:join(
       $\n,
       lists:map(
         fun ({Kind, Info}) ->
             diag_dek_kind(Kind, Info, Now)
         end, maps:to_list(DeksMap)))].

diag_dek_kind(Kind, Info, Now) ->
    ExtractKindData =
        fun (CallbackName) ->
            case cb_deks:call_dek_callback(CallbackName, Kind, [direct]) of
                {succ, {ok, V}} -> V;
                {succ, {error, not_found}} -> not_found;
                {succ, Error} ->
                    ?log_error("Failed to get ~p for ~p: ~0p",
                               [CallbackName, Kind, Error]),
                    error;
                {except, _} -> %% error is logged by call_dek_callback
                    exception
            end
        end,
    LifeTimeInSec = ExtractKindData(get_deks_lifetime),
    RotationInterval = ExtractKindData(get_deks_rotation_interval),
    DropKeysTS = ExtractKindData(get_drop_deks_timestamp),
    ForceEncryptionTS = ExtractKindData(get_force_encryption_timestamp),
    CreationTimeDeadline =
        case is_number(LifeTimeInSec) of
            true -> misc:datetime_add(Now, -LifeTimeInSec);
            false -> undefined
        end,
    io_lib:format(
      "  ~p:\n"
      "    Enabled: ~p\n"
      "    Active DEK id: ~s\n"
      "    Has unencrypted data: ~p\n"
      "    Last on-demand DEKs drop time: ~p\n"
      "    Last GC time: ~p\n"
      "    DEKs currently being dropped: ~s\n"
      "    DEKs lifetime (sec): ~p\n"
      "    DEKs rotation interval (sec): ~p\n"
      "    DEKs drop timestamp: ~p\n"
      "    Force encryption timestamp: ~p\n"
      "    Jobs statuses: ~p\n"
      "    All DEKs (total: ~p): ~s",
      [Kind,
       maps:get(is_enabled, Info, undefined),
       format_dek_id_for_diag(maps:get(active_id, Info, undefined)),
       maps:get(has_unencrypted_data, Info, undefined),
       maps:get(last_drop_timestamp, Info, undefined),
       maps:get(last_deks_gc_datetime, Info, undefined),
       sets:fold(
           fun(DekId, FAcc) ->
               [format_dek_id_for_diag(DekId),
               " " | FAcc]
           end, [], maps:get(deks_being_dropped, Info, sets:new())),
       LifeTimeInSec,
       RotationInterval,
       DropKeysTS,
       ForceEncryptionTS,
       maps:get(statuses, Info, #{}),
       length(maps:get(deks, Info, [])),
       [["\n      ", diag_dek(Dek, CreationTimeDeadline)]
           || Dek <- maps:get(deks, Info, [])]]).

diag_dek(?DEK_ERROR_PATTERN(Id, Reason), _CreationTimeDeadline) ->
    io_lib:format("~s (ERROR)~n          ~p",
                  [format_dek_id_for_diag(Id), Reason]);
diag_dek(#{type := Type, id := Id, info := Info}, CreationTimeDeadline) ->
    io_lib:format("~s (~p)~s~n          ~s",
                  [format_dek_id_for_diag(Id), Type,
                   diag_is_dek_expired(Info, CreationTimeDeadline),
                   io_lib:print(maps:remove(key, Info), 11, 80, -1)]).

diag_is_dek_expired(_, undefined) -> "";
diag_is_dek_expired(#{creation_time := CreationTime}, CreationTimeDeadline) ->
    case CreationTime < CreationTimeDeadline of
        true -> " expired!";
        false -> ""
    end;
diag_is_dek_expired(_, _) ->
    "".

diag_timers(Timers, TimersTimestamps) ->
    CurTS = erlang:monotonic_time(millisecond),
    [<<"Timers:">>, $\n,
     lists:join(
       $\n,
       lists:map(
         fun ({Name, Timer}) ->
             LastTimeFiredStr =
                case maps:find(Name, TimersTimestamps) of
                    {ok, LastTS} ->
                        LT = CurTS - LastTS,
                        io_lib:format("last time fired ~s ago",
                                      [misc:ms_to_str(LT)]);
                    error ->
                        "never fired before"
                end,
             io_lib:format(
               "  ~p: ~s",
               [Name,
                case Timer of
                    {TimerRef, Interval} when is_reference(TimerRef) ->
                        IntervalStr = io_lib:format("~s timer",
                                                    [misc:ms_to_str(Interval)]),
                        RemainingStr =
                            case erlang:read_timer(TimerRef) of
                                false -> "expired";
                                Ms -> io_lib:format("~s remaining",
                                                    [misc:ms_to_str(Ms)])
                            end,
                        io_lib:format("active (~s, ~s, ~s)",
                                      [RemainingStr, IntervalStr,
                                       LastTimeFiredStr]);
                    T -> io_lib:format("~p (~s)", [T, LastTimeFiredStr])
                end])
         end, maps:to_list(Timers)))].

diag_jobs(Jobs) ->
    [<<"Jobs:">>, $\n, io_lib:format("~0p", [Jobs])].

diag_cached_keys_list({ok, CachedKeysList}) ->
    [<<"Cached keys list:">>, $\n,
     io_lib:format("~s", [CachedKeysList])];
diag_cached_keys_list({error, Error}) ->
    [<<"Cached keys list:">>, $\n,
     io_lib:format("Request failed: ~0p", [Error])].

format_dek_id_for_diag(Id) when is_binary(Id) ->
    Id;
format_dek_id_for_diag(Id) ->
    io_lib:format("~p", [Id]).

handle_erpc_key_test_result(Res, Nodes) ->
    Errors = lists:filtermap(
               fun ({_Node, {ok, ok}}) ->
                       false;
                   ({Node, {ok, {error, R}}}) ->
                       {true, {Node, R}};
                   ({Node, {error, {erpc, timeout}}}) ->
                       {true, {Node, timeout}};
                   ({Node, {error, {erpc, noconnection}}}) ->
                       {true, {Node, no_connection_to_node}};
                   ({Node, {Class, ExceptionReason}}) ->
                       ?log_error("Failed to test secret on ~p: ~p:~p",
                                  [Node, Class, ExceptionReason]),
                       {true, {Node, exception}}
               end, lists:zip(Nodes, Res)),

    case Errors of
        [] ->
            ok;
        _ ->
            {error, {test_failed_for_some_nodes, Errors}}
    end.

call_is_writable_mfa({M, F, A}, ExtraArgs) ->
    erlang:apply(M, F, A ++ ExtraArgs).

-spec import_dek_files_impl(cb_deks:dek_kind(), [file:filename()],
                            #state{}) ->
          {ok, #state{}} | {{error, term()}, #state{}}.
import_dek_files_impl(_Kind, [], State) ->
    {ok, State};
import_dek_files_impl(Kind, Paths, State) ->
    maybe
        Snapshot = deks_config_snapshot(Kind),
        {succ, {ok, EncrMethod}} ?= cb_deks:call_dek_callback(
                                      get_encryption_method, Kind,
                                      [node, Snapshot]),
        {Res, NewState} = import_deks_into_state(Kind, Paths, EncrMethod,
                                                 Snapshot, false, State),
        write_deks_cfg_file(NewState),
        NewState2 = on_deks_update(Kind, NewState),
        case call_set_active_cb(Kind, Snapshot, NewState2) of
            {ok, NewState3} ->
                %% Adding reread_bad_deks job for the case if we have added deks
                %% to state but failed to read some of them after saving it
                {Res, add_jobs([{reread_bad_deks, Kind}], NewState3)};
            {error, NewState3, Reason} ->
                %% We have added the DEKs to state, so we can't just forget
                %% about them now. We will continue maintaining them, and will
                %% remove them eventually because they should not be used by
                %% memcached
                ?log_error("Failed to push DEKs: ~p", [Reason]),
                NewRes = case Res of
                             ok -> {error, Reason};
                             {error, _} -> Res
                         end,
                {NewRes, add_jobs([{reread_bad_deks, Kind},
                                   {maybe_update_deks, Kind}], NewState3)}
        end
    else
        {succ, {error, not_found}} ->
            {{error, kind_not_found}, State};
        {succ, {error, Error}} ->
            {{error, Error}, State};
        {error, Error} ->
            {{error, Error}, State}
    end.

-spec import_deks_into_state(cb_deks:dek_kind(), [file:filename()],
                             cb_deks:encryption_method(), chronicle_snapshot(),
                             boolean(), #state{}) ->
          {ok, #state{}} | {{error, term()}, #state{}}.
import_deks_into_state(_Kind, [], _EncrMethod, _Snapshot, _SkipCountersInc,
                       State) ->
    {ok, State};
import_deks_into_state(Kind, [Path | Tail], EncrMethod, Snapshot,
                       SkipCountersInc, State) ->
    maybe
        {ok, {NewSkipCountersInc, NewState}} ?=
            import_dek_into_state(Kind, Path, EncrMethod, Snapshot,
                                  SkipCountersInc, State),
        import_deks_into_state(Kind, Tail, EncrMethod, Snapshot,
                               NewSkipCountersInc, NewState)
    else
        {error, Error} ->
            {{error, Error}, State}
    end.

-spec import_dek_into_state(cb_deks:dek_kind(), string(),
                            cb_deks:encryption_method(), chronicle_snapshot(),
                            boolean(), #state{}) ->
          {ok, {boolean(), #state{}}} | {error, term()}.
import_dek_into_state(Kind, Path, EncrMethod, Snapshot, SkipCountersInc,
                      OldState) ->
    maybe
        %% Checking if this DEK is already imported before reading it.
        %% Read is expensive, while it is likely that the same DEK will be
        %% imported multiple times (because all from the same node vbuckets will
        %%  normally use the same DEKs).
        #{deks := ExistingDeks} = maps:get(Kind, OldState#state.deks_info,
                                           #{deks => []}),
        {ok, DekId} ?= encryption_service:extract_dek_id(Path),
        continue ?=
            case lists:search(fun (#{id := Id}) -> Id == DekId end,
                              ExistingDeks) of
                false -> continue;
                {value, _} -> {skip, DekId}
            end,
        %% Not requesting proof validation because this dek likely comes from
        %% another node, while gosecrets can only validate proofs generated
        %% by this node.
        {ok, NewKey} ?=
            case encryption_service:read_dek_file(Path, false) of
                {ok, {Kind, #{id := DekId} = K}} ->
                    {ok, K};
                {ok, {Kind, #{id := UnexpectedDekId}}} ->
                    {error, {unexpected_dek_id, UnexpectedDekId}};
                {ok, {UnexpectedKind, _}} ->
                    {error, {unexpected_dek_kind, UnexpectedKind}};
                {error, Error} ->
                    {error, Error}
            end,

        #{info := NewKeyInfo} = NewKey,
        ToImport = NewKey#{info => NewKeyInfo#{imported => true}},
        %% Put the dek into the proper folder, where other deks of this Kind
        %% are stored.
        {ok, DekId} ?= cb_deks:save_dek(Kind, ToImport, EncrMethod,
                                        SkipCountersInc, Snapshot),
        %% Note that if import fails later, this key will need to be
        %% garbage-collected
        [NewDek] = cb_deks:read(Kind, [DekId]),
        %% Update State only if we have successfully imported the DEK.
        %% We should not create empty DEK info.
        State = create_dek_info_if_does_not_exist(Kind, OldState),
        #state{deks_info = AllDeks = #{Kind := KindDeks}} = State,
        NewKindDeks = KindDeks#{deks => [NewDek | maps:get(deks, KindDeks)]},
        notify_kind_counter(<<"encr_at_rest_deks_imported">>, Kind),
        {ok, {true, State#state{deks_info = AllDeks#{Kind => NewKindDeks}}}}
    else
        {error, Reason} ->
            notify_kind_counter(<<"encr_at_rest_deks_import_failures">>, Kind),
            {error, Reason};
        {skip, SkippedDekId} ->
            notify_kind_counter(<<"encr_at_rest_deks_import_skipped">>, Kind),
            ?log_debug("Skipping import of DEK ~p because it already "
                        "exists in state", [SkippedDekId]),
            {ok, {SkipCountersInc, OldState}}
    end.

-spec should_renew_secrets_usage_info() -> boolean().
should_renew_secrets_usage_info() ->
    case chronicle_compat:get(direct, ?CHRONICLE_DEK_COUNTERS_TIME_KEY,
                              #{default => undefined}) of
        undefined -> true;
        UpdateTime ->
            CurrentTime = calendar:universal_time(),
            Diff = calendar:datetime_to_gregorian_seconds(CurrentTime) -
                   calendar:datetime_to_gregorian_seconds(UpdateTime),
            %% Using abs to catch cases when time on master node is way
            %% ahead of the this node.
            %% We can incorrectly skip some updates in case if time is not
            %% synced, but in DEK_COUNTERS_RENEW_INTERVAL_S
            %% seconds we will update it anyway.
            %% In worst case we will always go to master which will do empty
            %% update anyway.
            abs(Diff) > ?DEK_COUNTERS_RENEW_INTERVAL_S
    end.

-spec restart_test_secrets_timer(boolean(), #state{}) -> #state{}.
restart_test_secrets_timer(_, #state{proc_type = ?MASTER_PROC} = State) ->
    State;
restart_test_secrets_timer(IsFirst, #state{proc_type = ?NODE_PROC} = State) ->
    Interval = case get_secrets_test_interval_s() of
                   0 -> undefined;
                   T -> T * 1000
               end,
    Time = case IsFirst of
               true -> 0;
               false -> Interval
           end,
    case Interval of
        undefined ->
            ?log_debug("Disabling test secrets timer"),
            stop_timer(test_secrets, State);
        _ ->
            restart_timer(test_secrets, Time, Interval, State)
    end.

-spec run_periodic_test_for_secrets() -> ok.
run_periodic_test_for_secrets() ->
    Res = lists:map(fun (#{id := Id, name := Name} = S) ->
                        {Id, {Name, test_secret_props(S)}}
                    end, get_all()),
    DateTime = calendar:universal_time(),
    lists:foreach(
      fun ({Id, {Name, {error, Reason}}}) ->
              Msg = menelaus_web_secrets:format_error(
                      {key_test_alert, Reason, Name, node(), DateTime}),
              menelaus_web_alerts_srv:global_alert(
                {encr_at_rest_key_test_failed, Id}, iolist_to_binary(Msg));
          ({_Id, {_Name, _R}}) ->
              ok
      end, Res),
    ets:insert(?MODULE, {secrets_test_results,
                         {DateTime, maps:from_list(Res)}}),
    ok.

-spec get_secrets_test_interval_s() -> non_neg_integer().
get_secrets_test_interval_s() ->
    ns_config:search_node_with_default(secrets_test_interval_s,
                                       ?SECRETS_TEST_INTERVAL_DEFAULT_S).

-ifdef(TEST).
replace_secret_in_list_test() ->
    ?assertEqual(false, replace_secret_in_list(#{id => 3, p => 5}, [])),
    ?assertEqual(false,
                 replace_secret_in_list(#{id => 3, p => 5}, [#{id => 4}])),
    ?assertEqual([#{id => 4, p => 1}, #{id => 3, p => 5}, #{id => 1}],
                 replace_secret_in_list(
                   #{id => 3, p => 5},
                   [#{id => 4, p => 1}, #{id => 3, p => 6}, #{id => 1}])).

test_secret(NextTime, Interval, AutoRotation) ->
     #{type => ?CB_MANAGED_KEY_TYPE,
       data => #{can_be_cached => false,
                 auto_rotation => AutoRotation,
                 rotation_interval_in_days => Interval,
                 next_rotation_time => NextTime}}.

calculate_next_rotation_time_test() ->
    CurTime = {{2016, 09, 30}, {16, 00, 00}},
    Secret = ?cut(test_secret(_1, 1, _2)),
    Calc = fun (List) -> calculate_next_rotation_time(CurTime, List) end,
    Min = 0,
    MinSec = 0,
    Max = ?MAX_RECHECK_ROTATION_INTERVAL,
    MaxSec = Max div 1000,
    Future = ?cut(misc:datetime_add(CurTime, _)),
    Past = ?cut(misc:datetime_add(CurTime, -(_))),
    ?assertEqual(Max, Calc([])),
    ?assertEqual(Min, Calc([Secret(CurTime, true)])),
    ?assertEqual(Max, Calc([Secret(CurTime, false)])),
    %% next rotation in future:
    ?assertEqual(Min, Calc([Secret(Future(MinSec - 1), true)])),
    ?assertEqual(Min + 1000, Calc([Secret(Future(MinSec + 1), true)])),
    ?assertEqual(Max, Calc([Secret(Future(MaxSec + 1), true)])),
    ?assertEqual(Max - 1000, Calc([Secret(Future(MaxSec - 1), true)])),
    %% next rotation in the past:
    ?assertEqual(Min, Calc([Secret(Past(1), true)])),
    ?assertEqual(Min, Calc([Secret(Past(?SECS_IN_DAY*5), true)])),

    ?assertEqual(Min + 11000,
                 Calc([Secret(Future(MinSec + 20), true),
                       Secret(Future(MinSec + 3),  false),
                       Secret(Future(MinSec + 12), true),
                       Secret(Future(MinSec + 11), true),
                       Secret(Past(1),             false),
                       Secret(Future(MinSec + 23), true)])).

update_next_rotation_time_test() ->
    CurTime = {{2016, 09, 30}, {16, 00, 00}},
    Calc = fun (NextTime, Interval, AutoRotation) ->
               S = test_secret(NextTime, Interval, AutoRotation),
               case update_next_rotation_time(CurTime, S) of
                   {value, #{data := #{next_rotation_time := NewDate}} = Res} ->
                       #{data := D} = S,
                       %% Making sure nothing but next_rotation_time has changed
                       ?assertEqual(
                         S#{data => D#{next_rotation_time => NewDate}}, Res),
                       NewDate;
                   false -> false
               end
           end,
    D = ?SECS_IN_DAY,
    Future = ?cut(misc:datetime_add(CurTime, _)),
    Past = ?cut(misc:datetime_add(CurTime, -(_))),

    ?assertEqual(false,             Calc(CurTime, 3, false)),
    ?assertEqual(false,             Calc(Past(1), 3, false)),
    ?assertEqual(false,             Calc(Future(1), 3, false)),
    ?assertEqual(Future(3 * D),     Calc(CurTime,   3, true)),
    ?assertEqual(false,             Calc(Future(1), 3, true)),
    ?assertEqual(Future(3 * D - 1), Calc(Past(1), 3, true)),
    ?assertEqual(Future(2 * D),     Calc(Past(D), 3, true)),
    ?assertEqual(Future(D),         Calc(Past(2 * D), 3, true)),
    ?assertEqual(Future(3 * D),     Calc(Past(3 * D), 3, true)),
    ?assertEqual(Future(3 * D - 1), Calc(Past(3 * D + 1), 3, true)),
    ?assertEqual(Future(1),         Calc(Past(3 * D - 1), 3, true)),
    ?assertEqual(Future(3 * D - 1), Calc(Past(12 * D + 1), 3, true)),
    ?assertEqual(Future(1),         Calc(Past(12 * D - 1), 3, true)).

calculate_next_remove_retired_time_test() ->
    TimeDiff = fun (A, B) ->
                    (calendar:datetime_to_gregorian_seconds(A) -
                     calendar:datetime_to_gregorian_seconds(B)) * 1000
               end,
    %% Test mid-month case
    MidMonth = {{2023, 6, 15}, {14, 30, 45}},
    ExpectedMidMonth = TimeDiff({{2023, 7, 1}, {12, 0, 0}}, MidMonth),
    ?assertEqual(ExpectedMidMonth,
                 calculate_next_remove_retired_time(MidMonth)),

    %% Test end of month case
    EndMonth = {{2023, 6, 30}, {23, 59, 59}},
    ExpectedEndMonth = TimeDiff({{2023, 7, 1}, {12, 0, 0}}, EndMonth),
    ?assertEqual(ExpectedEndMonth,
                 calculate_next_remove_retired_time(EndMonth)),

    %% Test start of month case
    StartMonth = {{2023, 6, 1}, {0, 0, 0}},
    ExpectedStartMonth = TimeDiff({{2023, 7, 1}, {12, 0, 0}}, StartMonth),
    ?assertEqual(ExpectedStartMonth,
                 calculate_next_remove_retired_time(StartMonth)),

    %% Test December case (year rollover)
    December = {{2023, 12, 15}, {12, 0, 0}},
    ExpectedDecember = TimeDiff({{2024, 1, 1}, {12, 0, 0}}, December),
    ?assertEqual(ExpectedDecember,
                 calculate_next_remove_retired_time(December)),

    %% Test exactly at noon on first of month
    AtNoon = {{2023, 6, 1}, {12, 0, 0}},
    ExpectedAtNoon = TimeDiff({{2023, 7, 1}, {12, 0, 0}}, AtNoon),
    ?assertEqual(ExpectedAtNoon,
                 calculate_next_remove_retired_time(AtNoon)).

topologically_sorted_secrets_test() ->
    TopS = fun (Id) ->
               #{id => Id, type => ?CB_MANAGED_KEY_TYPE,
                 data => #{encrypt_with => nodeSecretManager, keys => []}}
           end,

    SubS = fun (Id, EncryptedById) ->
               #{id => Id, type => ?CB_MANAGED_KEY_TYPE,
                 data => #{encrypt_with => encryptionKey,
                           encrypt_secret_id => EncryptedById,
                           keys => []}}
           end,

    ?assertEqual({ok, []},
                 topologically_sorted_secrets([])),
    ?assertEqual({ok, [TopS(1)]},
                 topologically_sorted_secrets([TopS(1)])),
    {ok, Res0} = topologically_sorted_secrets([TopS(1), TopS(2)]),
    io:format("Res0: ~p~n", [Res0]),
    ?assert(lists:member(Res0,
                         [[TopS(1), TopS(2)],
                          [TopS(2), TopS(1)]])),

    %%
    %%   1
    %%  / \
    %% 2   3
    %%    /
    %%   4
    %%
    G1 = [TopS(1), SubS(2, 1), SubS(3, 1), SubS(4, 3)],
    {ok, Res1} = topologically_sorted_secrets(G1),
    io:format("Res1: ~p~n", [Res1]),
    ?assert(lists:member(Res1,
                         [[TopS(1), SubS(2, 1), SubS(3, 1), SubS(4, 3)],
                          [TopS(1), SubS(3, 1), SubS(4, 3), SubS(2, 1)],
                          [TopS(1), SubS(3, 1), SubS(2, 1), SubS(4, 3)]])),

    %%
    %%   3         4
    %%    \       / \
    %%     7     2   6
    %%    /
    %%   5
    %%
    G2 = [TopS(3), SubS(7, 3), SubS(5, 7)],
    G3 = [TopS(4), SubS(2, 4), SubS(6, 4)],
    {ok, Res2} = topologically_sorted_secrets(misc:shuffle(G2 ++ G3)),
    {Res2Even, Res2Odd} = misc:partitionmap(
        fun (#{id := Id} = E) ->
            case Id rem 2 of
                0 -> {left, E};
                1 -> {right, E}
            end
        end, Res2),
    io:format("Res2even: ~p~n", [Res2Even]),
    io:format("Res2odd: ~p~n", [Res2Odd]),
    ?assert(lists:member(Res2Even,
                         [[TopS(4), SubS(6, 4), SubS(2, 4)],
                          [TopS(4), SubS(2, 4), SubS(6, 4)]])),
    ?assertEqual([TopS(3), SubS(7, 3), SubS(5, 7)], Res2Odd),

    Cycle = [SubS(1, 2), SubS(2, 3), SubS(3, 1)],
    ?assertMatch({error, {cycle, _}},
                 topologically_sorted_secrets(Cycle)).

-endif.
