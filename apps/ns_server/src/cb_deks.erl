%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([list/1,
         read/2,
         generate_new/3,
         save_dek/5,
         maybe_reencrypt_deks/4,
         dek_cluster_kinds_list/0,
         dek_cluster_kinds_list/1,
         dek_kinds_list_existing_on_node/1,
         call_dek_callback/3,
         call_dek_callback/4,
         call_dek_callback_unsafe/3,
         dek_chronicle_keys_filter/1,
         kind2bin/1,
         kind2bin/2,
         kind2datatype/1]).

-define(REENCRYPT_DELAY, ?get_param(reencrypt_delay, 1000)).

%% Return encryption method for a given data type.
%% Parameters: Chronicle snapshot that contains all keys prepared by
%%             fetch_keys_callback.
%% Must be lightweight, as it can be called often.
-callback get_encryption_method(dek_kind(), cluster | node,
                                cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, encryption_method()} | {error, not_found}.

%% Called when DEKs are updated (active key changes or historical keys are
%% added/dropped).
%% Returns ok if keys are applied successfully, {error, Reason} otherwise.
%% Must be idempotent (can be called with the same arguments multiple times).
%% Must be lightweight in cases when keys are already applied.
-callback update_deks(dek_kind(),
                      cb_cluster_secrets:chronicle_snapshot()) ->
              ok | {error, _}.

%% The usage (in cb_cluster_secrets sense) that a secret must have in order to
%% be allowed to encrypt this kind of deks.
-callback get_required_usage(dek_kind()) -> cb_cluster_secrets:secret_usage().

%% Return the lifetime of DEKs in seconds.
-callback get_deks_lifetime(dek_kind(),
                            cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, pos_integer() | undefined} | {error, not_found}.

%% Return the rotation interval of DEKs in seconds.
-callback get_deks_rotation_interval(dek_kind(),
                                     cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, undefined | pos_integer()} | {error, not_found}.

%% Return the drop timestamp.
%% All DEKs that were created before that timestamp will be dropped at that
%% timestamp (the drop procedure will be initiated at that timestamp).
-callback get_drop_deks_timestamp(dek_kind(),
                                  cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, undefined | calendar:datetime()} | {error, not_found}.

%% Return the force encryption timestamp.
%% Will be used as expiration time for NULL DEK. Which basically means
%% cb_cluster_secrets will initiate encryption for all data that is not
%% encrypted yet at that timestamp. Normally, toggling encryption should reset
%% this timestamp to undefined.
-callback get_force_encryption_timestamp(
                  dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, undefined | calendar:datetime()} | {error, not_found}.

%% Return the list of DEKs that are currently in use.
%% Must be lightweight, as it can be called relatively often.
%% Should include ?NULL_DEK in the returned list if there is unencrypted data.
-callback get_dek_ids_in_use(dek_kind(),
                             cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, [dek_id()]} | {error, _}.

%% Initiate the drop procedure for a given list of DEKs.
%% User should start getting rid of the given DEKs as soon as possible.
%% If user doesn't currently use those DEKs, it way return {ok, done}.
%% It is also ok to return {ok, done} if necessary re-encryption is quick so
%% it can be performed synchronously immediately.
%% Otherwise, it should return {ok, started} and initiate the re-encryption
%% asynchronously. When finished the user should call dek_drop_complete/2.
%% The DEK list to drop can include ?NULL_DEK, which means that all unencrypted
%% data should be encrypted.
-callback initiate_drop_deks(dek_kind(), [dek_id()],
                             cb_cluster_secrets:chronicle_snapshot()) ->
             {ok, done | started} | {error, not_found | retry | _}.

%% Synchronize DEKs that are stored externally (e.g., in S3).
%% This callback is called when a KEK is about to be removed, after
%% re-encryption of DEKs but before KEK removal. The service should ensure
%% that all DEKs that are stored externally are up to date (the latest
%% version of DEK is copied externally). If the service doesn't store any
%% data outside of the cluster, this callback can always return ok.
%% If the service can't perform the synchronization, it should return an
%% error, which will interrupt KEK removal.
-callback synchronize_deks(dek_kind(),
                           cb_cluster_secrets:chronicle_snapshot()) ->
              ok | {error, _}.

%% Return a chronicle snapshot that contains all the chronicle
%% keys where encryption settings are stored for this dek kind.
-callback fetch_chronicle_keys_in_txn(dek_kind(), Txn :: term()) ->
              cb_cluster_secrets:chronicle_snapshot().

-export_type([dek_id/0, dek/0, dek_meta/0, dek_kind/0, encryption_method/0]).

-type encryption_method() :: {secret, cb_cluster_secrets:secret_id()} |
                             encryption_service |
                             disabled.
-type dek_id() :: cb_cluster_secrets:key_id().
-type dek_kind() :: configDek | logDek | auditDek |
                    {bucketDek, BucketUUID :: binary()}.
-type good_dek(Type, Info) :: #{id := dek_id(), type := Type, info := Info}.
-type aes_dek_info() :: #{key := ?HIDDEN_DATA(binary()),
                          encryption_key_id := cb_cluster_secrets:key_id(),
                          creation_time := calendar:datetime(),
                          imported := boolean()}.
-type ext_aes_dek_info() :: #{encryption_key_id := cb_cluster_secrets:key_id(),
                              creation_time := calendar:datetime(),
                              imported := boolean()}.
-type bad_dek() :: #{id := dek_id(), type := error, reason := term()}.
-type dek() :: good_dek('raw-aes-gcm', aes_dek_info()) | bad_dek().
-type dek_meta() :: good_dek('raw-aes-gcm', ext_aes_dek_info()).

-spec list(dek_kind()) ->
    {ok, {undefined | dek_id(), [dek_id()], boolean()}} | {error, _}.
list(Kind) ->
    GetCfgDekFun = encryption_service:read_dek(configDek, _),
    VerifyMac = fun encryption_service:verify_mac/2,
    ?log_debug("Reading list of ~p deks...", [Kind]),
    case cb_deks_raw_utils:external_list(Kind, GetCfgDekFun, VerifyMac, #{}) of
        {ok, {ActiveKeyId, AllIds, IsEnabled}} ->
            ?log_debug("~p DEK read res: Active: ~0p, AllIds: ~0p, "
                       "IsEnabled: ~0p",
                       [Kind, ActiveKeyId, AllIds, IsEnabled]),
            {ok, {ActiveKeyId, AllIds, IsEnabled}};
        {error, {read_dek_cfg_file_error, {Path, Reason}}} = Error ->
            ?log_error("Failed to read dek cfg file \"~s\": ~p",
                       [Path, Reason]),
            Error
    end.

-spec read(dek_kind(), [dek_id()]) -> [dek()].
read(Kind, DekIds) ->
    ?log_debug("Reading the following ~p keys from disk:~n~p", [Kind, DekIds]),
    lists:map(
        fun (DekId) when is_binary(DekId) ->
            case encryption_service:read_dek(Kind, DekId) of
                {ok, B} -> B;
                {error, R} ->
                    ?log_error("Failed to read key ~s: ~p", [DekId, R]),
                    encryption_service:new_dek_record(DekId, error, R)
            end
        end, DekIds).

-spec generate_new(dek_kind(), {secret, Id} | encryption_service,
                   cb_cluster_secrets:chronicle_snapshot()) ->
        {ok, dek_id()} | {error, _} when Id :: cb_cluster_secrets:secret_id().
generate_new(Kind, encryption_service, _Snapshot) ->
    maybe
        ok ?= increment_dek_encryption_counter(Kind, encryption_service),
        new(Kind, <<"encryptionService">>)
    end;
generate_new(Kind, {secret, Id}, Snapshot) ->
    maybe
        ok ?= increment_dek_encryption_counter(Kind, {secret, Id}),
        {ok, KekId} ?= cb_cluster_secrets:get_active_key_id(Id, Snapshot),
        new(Kind, KekId)
    end.

-spec save_dek(dek_kind(), good_dek('raw-aes-gcm', aes_dek_info()),
               {secret, Id}, boolean(), Snapshot) ->
          {ok, dek_id()} | {error, _}
                  when Id :: cb_cluster_secrets:secret_id(),
                       Snapshot :: cb_cluster_secrets:chronicle_snapshot().
%% Not supporting encryption_service because it doesn't seem to be needed so far
save_dek(Kind, #{id := DekId, info := #{key := BinHidden,
                                        creation_time := CreateTime,
                                        imported := Imported}},
         {secret, SecretId}, SkipCounterInc, Snapshot) ->
    maybe
        %% We don't want to increment the dek encryption counter when
        %% saving multiple deks in one call.
        %% Doing many unnecessary increments increases load on chronicle
        %% and increases probability of "retries_exceeded" errors.
        ok ?= case SkipCounterInc of
                  true -> ok;
                  false -> increment_dek_encryption_counter(Kind,
                                                            {secret, SecretId})
              end,
        {ok, KekId} ?= cb_cluster_secrets:get_active_key_id(SecretId, Snapshot),
        ok ?= encryption_service:store_dek(Kind, DekId, ?UNHIDE(BinHidden),
                                           KekId, CreateTime, Imported),
        {ok, DekId}
    end.

-spec new(dek_kind(), cb_cluster_secrets:key_id()) ->
                                                    {ok, dek_id()} | {error, _}.
new(Kind, KekIdToEncrypt) ->
    Id = cb_cluster_secrets:new_key_id(),
    ?log_debug("Generating new dek (~p): ~p", [Kind, Id]),
    Bin = cb_cluster_secrets:generate_raw_key(aes_256_gcm),
    CreateTime = calendar:universal_time(),
    case encryption_service:store_dek(Kind, Id, Bin, KekIdToEncrypt,
                                      CreateTime, false) of
        ok -> {ok, Id};
        {error, Reason} ->
            ?log_error("Failed to store key ~p on disk: ~p", [Id, Reason]),
            {error, Reason}
    end.

-spec maybe_reencrypt_deks(dek_kind(), [dek()], encryption_method(),
                           cb_cluster_secrets:chronicle_snapshot()) ->
          no_change | %% nothing changed
          {changed, [dek_id()], [Error :: term()]} |
          {error, term()}.
maybe_reencrypt_deks(_Kind, [], disabled, _Snapshot) -> no_change;
maybe_reencrypt_deks(_Kind, [], _, _Snapshot) -> {changed, [], []};
maybe_reencrypt_deks(Kind, Deks, EncryptionMethod, Snapshot) ->
    TargetKekIdRes =
        case EncryptionMethod of
            {secret, SecretId} ->
                case cb_cluster_secrets:get_active_key_id(SecretId, Snapshot) of
                    {ok, KekId} ->
                        {ok, KekId};
                    {error, Reason} ->
                        ?log_error("Dek ~p uses invalid secret ~p: ~p",
                                   [Kind, SecretId, Reason]),
                        {error, {invalid_secret_id, SecretId, Reason}}
                end;
            encryption_service ->
                {ok, <<"encryptionService">>};
            disabled ->
                {ok, undefined}
        end,

    case TargetKekIdRes of
        %% Encryption is disabled, but we still have deks. We should reencrypt
        %% them with the most recent keks.
        {ok, undefined} ->
            KekSecretMap =
                cb_cluster_secrets:get_secret_by_kek_id_map(Snapshot),
            maybe_reencrypt_deks(
              Kind, Deks,
              fun (#{type := 'raw-aes-gcm',
                     info := #{encryption_key_id := CurKekId}} = Dek) ->
                  maybe
                      false ?= (CurKekId == <<"encryptionService">>),
                      {ok, CurSecId} ?= maps:find(CurKekId, KekSecretMap),
                      {ok, NewKekId} ?= cb_cluster_secrets:get_active_key_id(
                                          CurSecId, Snapshot),
                      case NewKekId == CurKekId of
                          true -> false;
                          false -> {true, {{secret, CurSecId}, NewKekId}}
                      end
                  else
                      true ->
                          false;
                      error ->
                          %% When node is leaving the cluster, configuration
                          %% gets wiped out, so all KEKs disappear.
                          %% We should simply reencrypt all DEKs with the
                          %% encryption service in this case, as this is the
                          %% only key that is available no matter what.
                          ?log_warning(
                            "Orphaned key: ~p, key ~p is missing "
                            "(will reencrypt with encryptionService) - this is "
                            "expected when node is leaving the cluster",
                            [Dek, CurKekId]),
                          {true, {encryption_service, <<"encryptionService">>}};
                      {error, Reason2} ->
                          ?log_error("Encryption secret is missing for dek: "
                                     "~p: ~p", [Dek, Reason2]),
                          {true, {encryption_service, <<"encryptionService">>}}
                  end
              end);
        {ok, TargetKekId} ->
            maybe_reencrypt_deks(
              Kind, Deks,
              fun (#{type := 'raw-aes-gcm',
                     info := #{encryption_key_id := CurKekId}}) ->
                  case CurKekId == TargetKekId of
                      true -> false;
                      false -> {true, {EncryptionMethod, TargetKekId}}
                  end
              end);
        {error, _} = Error ->
            Error
    end.

maybe_reencrypt_deks(Kind, Deks, NewEncryptionKeyFun) ->
    ?log_debug("Checking if ~p deks need reencryption. All deks "
               "must be encrypted with current active key", [Kind]),
    ToReencrypt =
        lists:filtermap(
          fun (?DEK_ERROR_PATTERN(_, _)) -> false;
              (Dek) ->
                  case NewEncryptionKeyFun(Dek) of
                      {true, V} -> {true, {Dek, V}};
                      false -> false
                  end
          end, Deks),

    case ToReencrypt of
        [] -> no_change;
        _ ->
            %% Sleep to avoid coordinated writes to chronicle
            %% When KEK is rotated, all nodes start reencryption
            %% simultaneously, which causes N chronicle transactions
            %% to be started in parallel, causing a lot of retries, and
            %% possible exceeded_retries errors
            timer:sleep(rand:uniform(?REENCRYPT_DELAY)),

            %% Group by EncMethod to increment counter only once per method
            ByEncMethod = maps:groups_from_list(
                            fun({_Dek, {EncMethod, _}}) -> EncMethod end,
                            fun({Dek, {_, NewKekId}}) -> {Dek, NewKekId} end,
                            ToReencrypt),

            {Changed, Errors} =
                lists:foldl(
                  fun ({EncMethod, DeksAndKekIds},
                          {ChangedAcc, ErrorsAcc}) ->
                      {NewChanged, NewErrors} =
                          store_deks_reencrypted(Kind, EncMethod,
                                                 DeksAndKekIds),
                      {ChangedAcc ++ NewChanged, ErrorsAcc ++ NewErrors}
                  end, {[], []}, maps:to_list(ByEncMethod)),
            {changed, Changed, Errors}
    end.

-spec store_deks_reencrypted(dek_kind(), encryption_method(),
                             [{dek(), cb_cluster_secrets:key_id()}]) ->
    {[dek_id()], [{dek_id(), term()}]}.
store_deks_reencrypted(Kind, EncMethod, DeksAndKekIds) ->
    %% Increment counter once per encryption method
    case increment_dek_encryption_counter(Kind, EncMethod) of
        ok ->
            %% Process all DEKs for this method
            misc:partitionmap(
                fun ({#{type := 'raw-aes-gcm',
                        id := DekId,
                        info := #{encryption_key_id := CurKekId,
                                  key := DekKey,
                                  creation_time := CT,
                                  imported := Imported}},
                      NewKekId}) ->
                    ?log_debug("Dek ~p is encrypted with ~p, "
                                "while correct kek is ~p (~0p), "
                                "will reencrypt",
                                [DekId, CurKekId, NewKekId,
                                EncMethod]),
                    maybe
                        ok ?= encryption_service:store_dek(
                                Kind, DekId, DekKey(), NewKekId,
                                CT, Imported),
                        {left, DekId}
                    else
                        {error, Reason} ->
                            ?log_error("Failed to reencrypt "
                                        "dek ~p: ~p",
                                        [DekId, Reason]),
                            {right, {DekId, Reason}}
                    end
                end, DeksAndKekIds);
        {error, R} ->
            ?log_error("Failed to increment dek encryption "
                        "counter for ~p deks: ~p", [Kind, R]),
            {[], lists:map(fun ({#{id := DekId}, _}) ->
                               {DekId, R}
                           end, DeksAndKekIds)}
    end.

increment_dek_encryption_counter(Kind, SecretId) ->
    MovesPerNode = menelaus_web_settings:get_rebalance_moves_per_node(),
    Retries = max(2 * length(nodes()) * MovesPerNode, 10),
    cb_cluster_secrets:chronicle_transaction_with_backoff(
      [?CHRONICLE_DEK_COUNTERS_KEY],
      fun (Snapshot) ->
           All = chronicle_compat:get(Snapshot,
                                      ?CHRONICLE_DEK_COUNTERS_KEY,
                                      #{default => #{}}),
           NewDEKCounters = increment_dek_encryption_counter(Kind, SecretId,
                                                             All),
           {commit, [{set, ?CHRONICLE_DEK_COUNTERS_KEY, NewDEKCounters}]}
      end,
      Retries).

-spec increment_dek_encryption_counter(
        dek_kind(),
        cb_cluster_secrets:secret_id(),
        cb_cluster_secrets:dek_encryption_counters()) ->
          cb_cluster_secrets:dek_encryption_counters().
increment_dek_encryption_counter(Kind, SecretId, AllCounters) ->
    SecretCounters = maps:get(SecretId, AllCounters, #{}),
    {Counter, _Rev} = maps:get(Kind, SecretCounters, {0, undefined}),
    NewRev = rand:uniform(16#FFFFFFFF),
    NewSecretCounters = SecretCounters#{Kind => {Counter + 1, NewRev}},
    AllCounters#{SecretId => NewSecretCounters}.

%% Chronicle keys that can trigger dek reencryption, enablement/disablement
%% of encryption, etc...
%% Returns a dek kind that is affected by a given chronicle key.
%% Returns false otherwise.
dek_chronicle_keys_filter(?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY) ->
    {dek_settings_updated, ?DEK_KIND_LIST_STATIC};
dek_chronicle_keys_filter(Key) ->
    case ns_bucket:sub_key_match(Key) of
        {true, Bucket, K} when K == props; K == encr_at_rest ->
            case ns_bucket:uuid(Bucket, direct) of
                not_present -> %% Deleted?
                    check_for_deleted_keys;
                UUID when is_binary(UUID) ->
                    {dek_settings_updated, [{bucketDek, UUID}]}
            end;
        {true, _Bucket, _} -> ignore;
        false ->
            MembershipKeys = ns_cluster_membership:node_membership_keys(node()),
            case lists:member(Key, MembershipKeys) of
                true ->
                    {dek_settings_updated, dek_cluster_kinds_list()};
                false ->
                    ignore
            end
    end.

call_dek_callback(CallbackName, Kind, Args) ->
    call_dek_callback(CallbackName, Kind, Args, #{}).

-spec call_dek_callback(get_encryption_method |
                        update_deks |
                        get_required_usage |
                        get_deks_lifetime |
                        get_deks_rotation_interval |
                        get_drop_deks_timestamp |
                        get_force_encryption_timestamp |
                        get_dek_ids_in_use |
                        initiate_drop_deks |
                        synchronize_deks, dek_kind(), list(),
                        #{verbose => boolean()}) ->
      {succ, term()} | {except, {atom(), term(), term()}}.
call_dek_callback(CallbackName, Kind, Args, Opts) ->
    Module = dek_user_impl(Kind),
    try erlang:apply(Module, CallbackName, [Kind | Args]) of
        RV ->
            case maps:get(verbose, Opts, false) of
                true ->
                    ?log_debug("~p for ~p returned: ~0p",
                               [CallbackName, Kind, RV]);
                false ->
                    ok
            end,
            {succ, RV}
    catch
        C:E:ST ->
            ?log_error("~p for ~p crash ~p:~p~n~p",
                       [CallbackName, Kind, C, E, ST]),
            {except, {C, E, ST}}
    end.

%% Version of call_dek_callback that doesn't catch exceptions.
%% Use-case: Chronicle uses exceptions in transaction as a control flow, so if
%% we catch it here (when calling fetch_chronicle_keys_in_txn), we break the
%% logic of transaction.
-spec call_dek_callback_unsafe(fetch_chronicle_keys_in_txn, dek_kind(),
                               list()) -> term().
call_dek_callback_unsafe(CallbackName, Kind, Args) ->
    erlang:apply(dek_user_impl(Kind), CallbackName, [Kind | Args]).

dek_user_impl(configDek) -> cb_deks_config;
dek_user_impl(logDek) -> cb_deks_log;
dek_user_impl(auditDek) -> cb_deks_audit;
dek_user_impl({bucketDek, _}) -> cb_deks_bucket.

%% Returns all possible deks kinds for this cluster.
%% The list was supposed to be static if not buckets. Buckets can be created and
%% removed in real time, so the list is dynamic because of that. Note that the
%% list doesn't depend if encryption is on or off.
dek_cluster_kinds_list() ->
    dek_cluster_kinds_list(direct).
dek_cluster_kinds_list(Snapshot) ->
    Buckets = ns_bucket:uuids(Snapshot),
    ?DEK_KIND_LIST_STATIC ++
    [{bucketDek, UUID} || {_, UUID} <- Buckets].

dek_kinds_list_existing_on_node(Snapshot) ->
    AllKinds = dek_cluster_kinds_list(Snapshot),
    lists:filter(
        fun(Kind) ->
            case call_dek_callback(get_encryption_method, Kind,
                                   [node, Snapshot]) of
                {succ, {ok, _}} -> true;
                {succ, {error, not_found}} -> false
            end
        end,
        AllKinds).

kind2bin({bucketDek, UUID}) ->
    case ns_bucket:uuid2bucket(UUID) of
        {ok, BucketName} -> iolist_to_binary(["bucketDek_", BucketName]);
        {error, not_found} -> erlang:error(not_found)
    end;
kind2bin(K) -> atom_to_binary(K).

kind2bin(K, Default) ->
    try
        kind2bin(K)
    catch
        error:not_found -> Default
    end.

kind2datatype({bucketDek, UUID}) ->
    case ns_bucket:uuid2bucket(UUID) of
        {ok, BucketName} -> iolist_to_binary(["bucket_", BucketName]);
        {error, not_found} -> erlang:error(not_found)
    end;
kind2datatype(bucketDek) -> <<"bucket_data">>;
kind2datatype(kek) -> <<"keys">>;
kind2datatype(configDek) -> <<"config">>;
kind2datatype(logDek) -> <<"logs">>;
kind2datatype(auditDek) -> <<"audit">>.

-ifdef(TEST).

kind2datatype_test() ->
    %% Make sure the test fails if we add a new kind and forget to add
    %% a clause for it in kind2datatype/1
    lists:foreach(fun (K) ->
                      ?assert(is_binary(kind2datatype(K)))
                  end, ?KEY_KIND_LIST_STATIC).

dek_user_impl_exists_test() ->
    %% Make sure the test fails if we add a new kind and forget to add
    %% a clause for it in dek_user_impl/1
    lists:foreach(fun (K) ->
                      Module = dek_user_impl(K),
                      ?assert(is_atom(Module)),
                      Module:module_info()
                  end, [{bucketDek, <<"123">>} | ?DEK_KIND_LIST_STATIC]).

-endif.
