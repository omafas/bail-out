%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(encryption_service).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").
-include_lib("ns_common/include/cut.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(wrap_error_msg(C, A, P),
        try C of
            __RES -> wrap_error_msg(__RES, A, P)
        catch _:__ERR ->
            __M = io_lib:format("exception: ~p", [__ERR], [{chars_limit, 200}]),
            wrap_error_msg({error, lists:flatten(__M)}, A, P)
        end).

-export([start_link/0,
         decrypt/1,
         encrypt/1,
         encrypt_key/3,
         decrypt_key/3,
         test_existing_key/1,
         maybe_map_not_found_key_error/1,
         change_password/1,
         get_keys_ref/0,
         rotate_data_key/0,
         maybe_clear_backup_key/1,
         get_state/0,
         os_pid/0,
         reconfigure/1,
         store_kek/5,
         store_aws_key/4,
         store_kmip_key/5,
         store_dek/5,
         read_dek/2,
         key_path/1,
         decode_key_info/1,
         garbage_collect_keks/1,
         garbage_collect_keys/2,
         cleanup_retired_keys/0,
         maybe_rotate_integrity_tokens/1,
         remove_old_integrity_tokens/1,
         get_key_ids_in_use/0,
         mac/1,
         verify_mac/2,
         revalidate_key_cache/0]).


-export_type([stored_key_error/0]).

-type stored_key_error() :: {encrypt_key_error | decrypt_key_error |
                             store_key_error | read_key_error, string()}.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(RUNNER, {cb_gosecrets_runner, ns_server:get_babysitter_node()}).
-define(RESTART_WAIT_TIMEOUT, 120000).
-define(RETIRED_KEYS_RETENTION_MONTHS,
        ?get_param(retired_keys_retention_months, 3)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

encrypt(Data) ->
    cb_gosecrets_runner:encrypt(?RUNNER, Data).

decrypt(Data) ->
    cb_gosecrets_runner:decrypt(?RUNNER, Data).

change_password(NewPassword) ->
    cb_gosecrets_runner:change_password(?RUNNER, NewPassword).

get_keys_ref() ->
    cb_gosecrets_runner:get_keys_ref(?RUNNER).

get_state() ->
    cb_gosecrets_runner:get_state(?RUNNER).

rotate_data_key() ->
    cb_gosecrets_runner:rotate_data_key(?RUNNER).

maybe_clear_backup_key(DataKey) ->
    cb_gosecrets_runner:maybe_clear_backup_key(?RUNNER, DataKey).

os_pid() ->
    cb_gosecrets_runner:os_pid(?RUNNER).

reconfigure(NewCfg) ->
    safe_call({change_config, NewCfg}, infinity).

garbage_collect_keks(InUseKeyIds) ->
    garbage_collect_keys(kek, InUseKeyIds).

store_kek(Id, Key, KekIdToEncrypt, CreationDT, CanBeCached) ->
    store_key(kek, Id, 'raw-aes-gcm', Key, KekIdToEncrypt, CreationDT,
              CanBeCached).

store_dek({bucketDek, Bucket}, Id, Key, KekIdToEncrypt, CreationDT) ->
    BucketUUID = ns_bucket:uuid(Bucket, direct),
    store_dek(bucketDek, bucket_dek_id(BucketUUID, Id), Key, KekIdToEncrypt,
              CreationDT);
store_dek(Kind, Id, Key, KekIdToEncrypt, CreationDT) ->
    store_key(Kind, Id, 'raw-aes-gcm', Key, KekIdToEncrypt, CreationDT, false).

store_aws_key(Id, Params, CreationDT, TestOnly) ->
    store_key(kek, Id, awskm, ejson:encode(format_aws_key_params(Params)),
              <<"encryptionService">>, CreationDT, false, TestOnly).

format_aws_key_params(#{key_arn := KeyArn, region := Region,
                        profile := Profile, config_file := ConfigFile,
                        credentials_file := CredsFile, use_imds := UseIMDS}) ->
    {[{keyArn, iolist_to_binary(KeyArn)},
      {region, iolist_to_binary(Region)},
      {profile, iolist_to_binary(Profile)},
      {credsFile, iolist_to_binary(CredsFile)},
      {configFile, iolist_to_binary(ConfigFile)},
      {useIMDS, UseIMDS}]}.

store_kmip_key(Id, Params, KekIdToEncrypt, CreationDT, TestOnly) ->
    store_key(kek, Id, kmip, ejson:encode(format_kmip_key_params(Params)),
              KekIdToEncrypt, CreationDT, false, TestOnly).

format_kmip_key_params(#{host := Host,
                         port := Port,
                         req_timeout_ms := ReqTimeoutMs,
                         kmip_id := KmipId,
                         key_path := KeyPath,
                         cert_path := CertPath,
                         key_passphrase := PassData,
                         ca_selection := CaSel,
                         encryption_approach := Appr}) ->
    {[{host, iolist_to_binary(Host)},
      {port, Port},
      {reqTimeoutMs, ReqTimeoutMs},
      {kmipId, KmipId},
      {keyPath, iolist_to_binary(KeyPath)},
      {certPath, iolist_to_binary(CertPath)},
      {keyPassphrase, base64:encode(PassData)},
      {caSelection, CaSel},
      {cbCaPath, iolist_to_binary(ns_ssl_services_setup:ca_file_path())},
      {encryptionApproach, Appr}]}.

read_dek(Kind, DekId) ->
    {NewId, NewKind} = case Kind of
                           {bucketDek, Bucket} ->
                               BucketUUID = ns_bucket:uuid(Bucket, direct),
                               {bucket_dek_id(BucketUUID, DekId), bucketDek};
                           _ ->
                               {DekId, Kind}
                       end,
    case ?wrap_error_msg(
           cb_gosecrets_runner:read_key(?RUNNER, NewKind, NewId),
           read_key_error, [{kind, cb_deks:kind2bin(NewKind)},
                            {key_UUID, NewId}]) of
        {ok, Json} ->
            {Props} = ejson:decode(Json),
            Res = maps:from_list(
                    lists:map(fun ({<<"type">>, <<"raw-aes-gcm">>}) -> 
                                      {type, 'raw-aes-gcm'};
                                  ({<<"info">>, InfoProps}) ->
                                      {info, decode_key_info(InfoProps)}
                              end, Props)),
            {ok, Res#{id => DekId}};
        {error, Error} ->
            {error, Error}
    end.

decode_key_info({InfoProps}) ->
    maps:from_list(
      lists:map(
        fun ({<<"key">>, B64Key}) ->
                Key = base64:decode(B64Key),
                {key, fun () -> Key end};
            ({<<"encryptionKeyId">>, KekId}) ->
                {encryption_key_id, KekId};
            ({<<"creationTime">>, CreationTimeISO}) ->
                {creation_time, iso8601:parse(CreationTimeISO)}
        end, InfoProps)).

encrypt_key(Data, AD, KekId) when is_binary(Data), is_binary(AD),
                                  is_binary(KekId) ->
    FinalAD = <<AD/binary, KekId/binary>>,
    ?wrap_error_msg(
      cb_gosecrets_runner:encrypt_with_key(?RUNNER, Data, FinalAD, kek, KekId),
      encrypt_key_error, [{key_UUID, KekId}]).

decrypt_key(Data, AD, KekId) when is_binary(Data), is_binary(AD),
                                  is_binary(KekId) ->
    FinalAD = <<AD/binary, KekId/binary>>,
    ?wrap_error_msg(
      cb_gosecrets_runner:decrypt_with_key(?RUNNER, Data, FinalAD, kek, KekId),
      decrypt_key_error, [{key_UUID, KekId}]).

%% This function can be called by other nodes
test_existing_key(KekId) when is_binary(KekId) ->
    RandomData = rand:bytes(16),
    RandomAD = rand:bytes(16),
    maybe
        {ok, EncryptedData} ?= encrypt_key(RandomData, RandomAD, KekId),
        {ok, DecryptedData} ?= decrypt_key(EncryptedData, RandomAD, KekId),
        case RandomData =:= DecryptedData of
            true -> ok;
            false -> {error, decrypted_data_mismatch}
        end
    else
        {error, Error} -> {error, maybe_map_not_found_key_error(Error)}
    end.

maybe_map_not_found_key_error({T, "no files found matching:" ++ _ = Error})
                                                when T == encrypt_key_error;
                                                     T == decrypt_key_error ->
    %% We know that if this function is called the secret actually
    %% exists, so if we get this error, it means that we have problems
    %% saving the key on disk (likely because something is wrong with secret
    %% settings). Changing the error to avoid confusion.
    {invalid_encryption_secret, Error};
maybe_map_not_found_key_error(Error) ->
    Error.

maybe_rotate_integrity_tokens(undefined) ->
    maybe_rotate_integrity_tokens(<<>>);
maybe_rotate_integrity_tokens(KeyName) when is_binary(KeyName) ->
    ?wrap_error_msg(
      cb_gosecrets_runner:rotate_integrity_tokens(?RUNNER, KeyName),
      rotate_integrity_tokens_error, [{key_UUID, KeyName}]).

remove_old_integrity_tokens(Kinds) ->
    Paths = lists:filtermap(
              fun(Kind) ->
                  case key_path(Kind) of
                      undefined -> false;
                      Path -> {true, Path}
                  end
              end, Kinds),
    ?wrap_error_msg(
      cb_gosecrets_runner:remove_old_integrity_tokens(?RUNNER, Paths),
      remove_old_integrity_tokens_error, []).

get_key_ids_in_use() ->
    case cb_gosecrets_runner:get_key_id_in_use(?RUNNER) of
        {ok, <<>>} -> {ok, [undefined]};
        {ok, KeyId} -> {ok, [KeyId]};
        {error, Error} -> {error, Error}
    end.

mac(Data) when is_binary(Data) ->
    ?wrap_error_msg(cb_gosecrets_runner:mac(?RUNNER, Data),
                    mac_calculation_error, []).

verify_mac(Mac, Data) when is_binary(Data), is_binary(Mac) ->
    ?wrap_error_msg(cb_gosecrets_runner:verify_mac(?RUNNER, Mac, Data),
                    mac_verification_error, []).

%% This function ensures that gosecrets doesn't hold any removed keys in its
%% cache.
revalidate_key_cache() ->
    ?log_debug("Validating key cache"),
    ?wrap_error_msg(cb_gosecrets_runner:revalidate_key_cache(?RUNNER),
                    validate_key_cache_error, []).

%%%===================================================================
%%% callbacks
%%%===================================================================

init([]) ->
    case recover() of
        ok ->
            EventFilter = fun (database_dir) -> true;
                              (_) -> false
                          end,
            chronicle_compat_events:notify_if_key_changes(
                                      EventFilter, update),
            case maybe_update_dek_path_in_config() of
                ok ->
                    create_encryption_service_stats(),
                    {ok, #{}};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, _} = Error -> {stop, {recover_failed, Error}}
    end.

handle_call({change_config, Cfg}, _From, State) ->
    case change_config(Cfg) of
        ok -> {reply, ok, State};
        {error, _} = Error ->
            {stop, {change_cfg_failed, Error}, Error, State}
    end;
handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(Msg, _From, State) ->
    ?log_error("unhandled call: ~p", [Msg]),
    {reply, unhandled, State}.

handle_cast(Msg, State) ->
    ?log_error("unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info(update, State) ->
    case maybe_update_dek_path_in_config() of
        ok -> {noreply, State};
        {error, Reason} -> {stop, Reason, State}
    end;

handle_info(Info, State) ->
    ?log_error("unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

change_config(NewCfg) ->
    OldCfg = ns_config:read_key_fast(ns_config_sm_key(), []),
    ?log_debug("Change config started.~nOld cfg: ~p~nNew cfg: ~p",
               [OldCfg, NewCfg]),
    MarkerPath = change_cfg_marker(),
    write_change_cfg_marker(MarkerPath, {config_change, {OldCfg, NewCfg}}),
    case change_config(NewCfg, OldCfg, MarkerPath, _CopySecrets = needed,
                       _ResetPassword = true) of
        ok ->
            misc:remove_marker(MarkerPath),
            ok;
        {error, _} = Error -> Error
    end.

%% Note: We don't need to copy secrets if we are recovering after failure
%% because if we have removed old cfg secrets, the config change is actually
%% already finished.
change_config(NewCfg, OldCfg, MarkerPath, _CopySecrets = needed,
              ResetPassword = true) ->
    case cb_gosecrets_runner:copy_secrets(?RUNNER, NewCfg) of
        {ok, <<"same">>} ->
            write_change_cfg_marker(MarkerPath, {config_change_same_secrets,
                                                 {OldCfg, NewCfg}}),
            change_config(NewCfg, OldCfg, MarkerPath, not_needed,
                          false);
        {ok, <<"copied">>} ->
            write_change_cfg_marker(MarkerPath, {config_change_copy_done,
                                                 {OldCfg, NewCfg}}),
            change_config(NewCfg, OldCfg, MarkerPath, done, ResetPassword);
        {error, _} = Error ->
            Error
    end;
%% copy_secrets doesn't support using custom passwords, because we always
%% reset it. Looks like we don't really need to support that case anyway.
%% The only scenario when we don't want to reset password is when we are
%% recovering after unsuccessful change_config attempt.
change_config(_NewCfg, _OldCfg, _MarkerPath, _CopySecrets = needed,
              _ResetPassword = false) ->
    error(not_supported);
%% Can't (and shouldn't) reset password here because password is reset during
%% copying of secrets, since no copying is done, the password should stay
%% the same
change_config(NewCfg, _OldCfg, _MarkerPath, _CopySecrets = not_needed,
              ResetPassword = false) ->
    ns_config:set(ns_config_sm_key(), NewCfg),
    case cb_gosecrets_runner:set_config(?RUNNER, NewCfg, ResetPassword) of
        ok ->
            ok;
        {error, _} = Error ->
            Error
    end;
change_config(NewCfg, OldCfg, MarkerPath, _CopySecrets = done, ResetPassword) ->
    ns_config:set(ns_config_sm_key(), NewCfg),
    case cb_gosecrets_runner:set_config(?RUNNER, NewCfg, ResetPassword) of
        ok ->
            %% If a hard error happens during removal of old secrets,
            %% it might be very hard to recover to previous config
            %% (because old secrets can be already removed and we can't
            %% copy secrets back because of that hard error).
            %% So it seems like it is safer to not return error here.
            write_change_cfg_marker(MarkerPath, {cleanup_secrets, OldCfg}),
            cb_gosecrets_runner:cleanup_secrets(?RUNNER, OldCfg),
            ok;
        {error, _} = Error ->
            Error
    end.

recover() ->
    MarkerPath = change_cfg_marker(),
    case misc:consult_marker(MarkerPath) of
        %% Actual change has not started yet, so we can just remove new secrets
        %% that we created during the config change attempt and be done
        {ok, [{config_change, {_OldCfg, NewCfg}}]} ->
            ?log_warning("Found config_change marker. Starting cleanup for new cfg: ~p",
                         [NewCfg]),
            case cb_gosecrets_runner:cleanup_secrets(?RUNNER, NewCfg) of
                ok ->
                    ?log_debug("Cleanup finished successfully");
                {error, _} = Error ->
                    %% Ignoring because there is not match we can do
                    ?log_error("Cleanup failed with error: ~p. Ignoring...",
                               [Error])
            end,
            misc:remove_marker(MarkerPath),
            ok;
        %% Copy of configs has finished, and actual config change has probably
        %% started, but old secrets are not removed yet.
        {ok, [{config_change_copy_done, {OldCfg, NewCfg}}]} ->
            ?log_warning("Found change config marker. Starting gosecrets "
                         "recover to old config~nOld cfg: ~p~nNew cfg: ~p",
                         [OldCfg, NewCfg]),
            case change_config(OldCfg, NewCfg, MarkerPath,
                               _CopySecrets = done, _ResetPassword = false) of
                ok ->
                    ?log_debug("Recover finished successfully"),
                    misc:remove_marker(MarkerPath),
                    ok;
                {error, _} = Error ->
                    ?log_error("Recover failed with reason: ~p", [Error]),
                    Error
            end;
        %% Copy of configs was not needed, and actual config change has probably
        %% started.
        {ok, [{config_change_same_secrets, {OldCfg, NewCfg}}]} ->
            ?log_warning("Found change config marker. Starting gosecrets "
                         "recover to old config~nOld cfg: ~p~nNew cfg: ~p",
                         [OldCfg, NewCfg]),
            case change_config(OldCfg, NewCfg, MarkerPath,
                               _CopySecrets = not_needed,
                               _ResetPassword = false) of
                ok ->
                    ?log_debug("Recover finished successfully"),
                    misc:remove_marker(MarkerPath),
                    ok;
                {error, _} = Error ->
                    ?log_error("Recover failed with reason: ~p", [Error]),
                    Error
            end;
        %% We have already finished new config reload. The only thing that is
        %% not done yet, is the removal of old secrets.
        {ok, [{cleanup_secrets, OldCfg}]} ->
            ?log_warning("Found cleanup marker. Starting cleanup for cfg: ~p",
                         [OldCfg]),
            case cb_gosecrets_runner:cleanup_secrets(?RUNNER, OldCfg) of
                ok ->
                    ?log_debug("Cleanup finished successfully");
                {error, _} = Error ->
                    %% Ignoring because there is not match we can do
                    ?log_error("Cleanup failed with error: ~p. Ignoring...",
                               [Error])
            end,
            misc:remove_marker(MarkerPath),
            ok;
        false ->
            ?log_debug("Config marker ~s doesn't exist", [MarkerPath]),
            ok
    end.

change_cfg_marker() ->
    filename:join(path_config:component_path(data, "config"),
                  "sm_load_config_marker").

write_change_cfg_marker(MarkerPath, Term) ->
    MarkerBody = io_lib:format("~p.", [Term]),
    misc:create_marker(MarkerPath, iolist_to_binary(MarkerBody)).

ns_config_sm_key() -> {node, node(), secret_mngmt_cfg}.

safe_call(Req, Timeout) ->
    safe_call(Req, Timeout, 100).

safe_call(Req, _Timeout, AttemptsLeft) when AttemptsLeft =< 0 ->
    ?log_error("Call ~p retries exceeded", [Req]),
    {error, call_retries_exceeded};
safe_call(Req, Timeout, AttemptsLeft) ->
    wait_for_server_start(),
    try
        gen_server:call(?MODULE, Req, Timeout)
    catch
        exit:{{change_cfg_failed, _}, _} ->
            %% The process is being restarted because it failed to change
            %% config (previous call has failed, not this one), so we should
            %% wait until it restarts, and retry
            true = wait_for_server_start(),
            safe_call(Req, Timeout, AttemptsLeft - 1);
        exit:{{recover_failed, _}, _} ->
            %% The process is being restarted because it failed to recover
            %% after unsuccessful config change.
            %% config (previous call has failed, not this one), so we should
            %% wait until it restarts, and retry
            true = wait_for_server_start(),
            safe_call(Req, Timeout, AttemptsLeft - 1)
    end.

wait_for_server_start() ->
    misc:poll_for_condition(
      fun () ->
          try
              is_process_alive(whereis(?MODULE)) andalso
              (ok == gen_server:call(?MODULE, sync))
          catch
              _:_ -> false
          end
      end, ?RESTART_WAIT_TIMEOUT, 100).

store_key(Kind, Name, Type, KeyData, EncryptionKeyId, CreationDT,
          CanBeCached) ->
    store_key(Kind, Name, Type, KeyData, EncryptionKeyId, CreationDT,
              CanBeCached, false).

store_key(Kind, Name, Type, KeyData, undefined, CreationDT, CanBeCached,
          TestOnly) ->
    store_key(Kind, Name, Type, KeyData, <<"encryptionService">>, CreationDT,
              CanBeCached, TestOnly);
store_key(Kind, Name, Type, KeyData, EncryptionKeyId,
          {{_, _, _}, {_, _, _}} = CreationDT, CanBeCached, TestOnly)
                                            when is_atom(Kind),
                                                 is_binary(Name),
                                                 is_atom(Type),
                                                 is_binary(KeyData),
                                                 is_binary(EncryptionKeyId),
                                                 is_atom(TestOnly),
                                                 is_boolean(CanBeCached) ->
    CreationDTISO = iso8601:format(CreationDT),
    KindBin = cb_deks:kind2bin(Kind),
    ErrorAtom = case TestOnly of
                    true -> store_key_error_test;
                    false -> store_key_error
                end,

    ?wrap_error_msg(
      cb_gosecrets_runner:store_key(?RUNNER, Kind, Name, Type, KeyData,
                                    EncryptionKeyId, CreationDTISO, CanBeCached,
                                    TestOnly),
      ErrorAtom, [{kind, KindBin}, {key_UUID, Name}]).

maybe_update_dek_path_in_config() ->
    case ns_storage_conf:this_node_dbdir() of
        {ok, BucketDekPath} ->
            Cfg = ns_config:read_key_fast(ns_config_sm_key(), []),
            case proplists:get_value(bucket_dek_path, Cfg) of
                BucketDekPath -> ok;
                OldBucketDekPath ->
                    ?log_debug("Dek directory needs to be updated "
                               "(old value: ~p, new value: ~p)",
                               [OldBucketDekPath, BucketDekPath]),
                    NewCfg = misc:update_proplist(
                               Cfg, [{bucket_dek_path, BucketDekPath}]),
                    case change_config(NewCfg) of
                        ok -> ok;
                        {error, _} = Error ->
                            {error, {change_cfg_failed, Error}}
                    end
            end;
        {error, not_found} -> ok
    end.

wrap_error_msg(ok, _A, _) -> ok;
wrap_error_msg({ok, _} = R, _A, _) -> R;
wrap_error_msg({error, Msg}, A, ExtraArgs) when is_list(Msg), is_atom(A),
                                                is_list(ExtraArgs) ->
    maybe_log_error_to_user_log(A, Msg, ExtraArgs),
    maybe_log_error_to_event_log(A, Msg, ExtraArgs),
    maybe_notify_stats(A),
    {error, {A, Msg}}.

maybe_notify_stats(store_key_error_test) ->
    ok;
maybe_notify_stats(A) ->
    ns_server_stats:notify_counter({<<"encryption_service_failures">>,
                                    [{failure_type, A}]}).

create_encryption_service_stats() ->
    lists:foreach(
      fun (T) ->
          ns_server_stats:create_counter({<<"encryption_service_failures">>,
                                          [{failure_type, T}]})
      end, [read_key_error,
            encrypt_key_error,
            decrypt_key_error,
            store_key_error,
            rotate_integrity_tokens_error,
            remove_old_integrity_tokens_error,
            mac_calculation_error,
            mac_verification_error]).

maybe_log_error_to_event_log(store_key_error_test, _Msg, _ExtraArgs) ->
    ok;
maybe_log_error_to_event_log(A, Msg, ExtraArgs) ->
    event_log:add_log(encryption_service_failure,
                      [{error, A}, {error_msg, iolist_to_binary(Msg)}] ++
                      ExtraArgs).

maybe_log_error_to_user_log(read_key_error, Msg, ExtraArgs) ->
    ale:error(?USER_LOGGER, "Failed to read key ~s: ~s",
              [extract_key_uuid(ExtraArgs), Msg]);
maybe_log_error_to_user_log(encrypt_key_error, Msg, ExtraArgs) ->
    ale:error(?USER_LOGGER, "Failed to encrypt data using key ~s: ~s",
              [extract_key_uuid(ExtraArgs), Msg]);
maybe_log_error_to_user_log(decrypt_key_error, Msg, ExtraArgs) ->
    ale:error(?USER_LOGGER, "Failed to decrypt data using key ~s: ~s",
              [extract_key_uuid(ExtraArgs), Msg]);
maybe_log_error_to_user_log(store_key_error, Msg, ExtraArgs) ->
    ale:error(?USER_LOGGER, "Failed to store key ~s: ~s",
              [extract_key_uuid(ExtraArgs), Msg]);
maybe_log_error_to_user_log(A, Msg, _)
                                   when A == rotate_integrity_tokens_error;
                                        A == remove_old_integrity_tokens_error;
                                        A == mac_calculation_error;
                                        A == mac_verification_error ->
    ale:error(?USER_LOGGER, "Failed key integrity operation (~p): ~s",
              [A, Msg]);
maybe_log_error_to_user_log(_, _, _) ->
    ok.

extract_key_uuid(LogExtraArgs) ->
    proplists:get_value(key_UUID, LogExtraArgs, <<"unknown">>).

garbage_collect_keys(Kind, InUseKeyIds) ->
    KeyDir = key_path(Kind),
    %% Just making sure the list is in expected format (so we don't end up
    %% comparing string and binary)
    lists:foreach(fun (Id) -> {_, true} = {Id, is_binary(Id)} end, InUseKeyIds),
    ListDirRes = file:list_dir(KeyDir),
    ToRetire = select_files_to_retire(KeyDir, InUseKeyIds, ListDirRes),
    case ToRetire of
        [] ->
            ?log_debug("~p keys gc: no keys to retire", [Kind]),
            %% We need to validate keys cache here because sometimes
            %% keys are removed from disk before this function is called
            %% (bucket removal) and we need to make sure that the key cache
            %% is cleared in this case.
            revalidate_key_cache(),
            no_change;
        _ ->
            ?log_info("~p keys gc: retiring ~0p (all keys: ~0p, in "
                      "use: ~0p)", [Kind, ToRetire, ListDirRes, InUseKeyIds]),
            FailedList =
                lists:filtermap(
                  fun (Filename) ->
                      case retire_key(Kind, Filename) of
                          ok -> false;
                          {error, Reason} -> {true, {Filename, Reason}}
                      end
                  end, ToRetire),
            revalidate_key_cache(),
            case FailedList of
                [] -> ok;
                _ ->
                    ?log_error("Failed to retire some key files:~n~p",
                               [FailedList]),
                    {error, FailedList}
            end
    end.

select_files_to_retire(Dir, KeyIdsInUse, {ok, FileList}) ->
    KeyIdsInUseSet = maps:from_keys(KeyIdsInUse, true),

    Parsed = lists:filtermap(
               fun (F) ->
                   maybe
                       [Id, <<"key">>, VsnStr] ?=
                           binary:split(iolist_to_binary(F), <<".">>, [global]),
                       true ?= cb_cluster_secrets:is_valid_key_id(F),
                       {vsn, Vsn} ?= catch {vsn, binary_to_integer(VsnStr)},
                       {true, {Id, F, Vsn}}
                   else
                       _ ->
                           ?log_error("invalid key name ~s in ~s", [F, Dir]),
                           false
                   end
               end, FileList),
    Grouped = maps:groups_from_list(fun ({Id, _, _}) -> Id end,
                                    fun ({_, F, Vsn}) -> {Vsn, F} end,
                                    Parsed),
    lists:flatmap(
      fun ({Id, L}) ->
          case maps:get(Id, KeyIdsInUseSet, false) of
              true ->
                  %% The key is still in use, so we can remove older versions
                  %% of this key only. Normally we should not have any though.
                  [F || {_Vsn, F} <- lists:droplast(lists:usort(L))];
              false ->
                  %% This ID is not used at all, so it is ok to retire all files
                  [F || {_Vsn, F} <- L]
          end
      end, maps:to_list(Grouped));
select_files_to_retire(_Dir, _KeyIdsInUse, {error, enoent}) ->
    [];
select_files_to_retire(Dir, _KeyIdsInUse, {error, Reason}) ->
    ?log_error("Failed to get list of files in ~p: ~p",
               [Dir, Reason]),
    [].

cleanup_retired_keys() ->
    cleanup_retired_keys(calendar:universal_time(),
                         ?RETIRED_KEYS_RETENTION_MONTHS).

cleanup_retired_keys({{CurrentYear, CurrentMonth, _}, _}, RetentionMonths) ->
    ?log_debug("Cleanup retired keys"),
    RetiredDir = retired_keys_dir(),
    maybe
        {ok, Dirs} ?= file:list_dir(RetiredDir),
        CurrentMonths = CurrentYear * 12 + CurrentMonth,
        lists:foreach(
          fun (DirName) ->
              FullPath = filename:join(RetiredDir, DirName),
              maybe
                  {dir, true} ?= {dir, filelib:is_dir(FullPath)},
                  {ok, {Year, Month}} ?= parse_retired_dir_name(DirName),
                  DirMonths = Year * 12 + Month,
                  {old, true} ?=
                      {old, CurrentMonths - DirMonths > RetentionMonths},
                  AllFiles = file:list_dir(FullPath),
                  ?log_info("Permanently removing retired keys directory ~s as "
                            "it is older than ~b months. Keys to be "
                            "removed: ~.0p",
                            [FullPath, RetentionMonths, AllFiles]),
                  ok ?= misc:rm_rf(FullPath)
              else
                  {dir, false} ->
                      ?log_warning("Invalid retired keys directory name "
                                   "(not a directory): ~s, will be "
                                   "ignored", [FullPath]);
                  error ->
                      ?log_warning("Invalid retired keys directory name: ~s, "
                                   "will be ignored", [DirName]);
                  {old, false} ->
                      ok;
                  {error, Reason} ->
                      ?log_error("Failed to remove retired keys directory ~s: "
                                 "~p", [FullPath, Reason])
              end
          end, Dirs),
        ok
    else
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?log_error("Failed to list retired keys directory ~s: ~p",
                       [RetiredDir, Reason]),
            ok
    end.

parse_retired_dir_name(DirName) ->
    try
        [YearStr, MonthStr] = string:tokens(DirName, "-"),
        Year = list_to_integer(YearStr),
        Month = list_to_integer(MonthStr),
        case calendar:valid_date(Year, Month, 1) of
            true -> {ok, {Year, Month}};
            false -> error
        end
    catch
        _:_ -> error
    end.

-ifdef(TEST).

cleanup_retired_keys_test() ->
    %% Create temp directory for test
    RetiredDir = retired_keys_dir(),
    ok = filelib:ensure_dir(RetiredDir ++ "/"),

    %% Helper to create test directories and files
    CreateTestDir =
        fun (YearMonth) ->
            Dir = filename:join(RetiredDir, YearMonth),
            ok = filelib:ensure_dir(Dir ++ "/"),
            ok = file:write_file(filename:join(Dir, "test.key.1"), <<"test">>)
        end,

    try
        %% Create test directories for different months
        lists:foreach(CreateTestDir, [
            "2023-10",  %% Should be removed (>3 months old)
            "2023-11",  %% Should be removed (>3 months old) 
            "2023-12",  %% Should stay (3 months old)
            "2024-01",  %% Should stay (2 months old)
            "2025-02"   %% Should stay (in future)
        ]),

        %% Also create some invalid directory names that should be ignored
        lists:foreach(CreateTestDir, [
            "not-a-date",
            "2023-13",
            "2023-0",
            "2023"
        ]),
        %% Create a file in retiredKeysDir, should be ignored
        ok = file:write_file(filename:join(RetiredDir, "test.key.1"),
                             <<"test">>),

        %% Run cleanup with reference date of 2024-03-01 and 3 month retention
        ok = cleanup_retired_keys({{2024, 3, 1}, {0,0,0}}, 3),

        %% Verify correct directories were removed/kept
        ?assertNot(filelib:is_dir(filename:join(RetiredDir, "2023-10"))),
        ?assertNot(filelib:is_dir(filename:join(RetiredDir, "2023-11"))),
        ?assert(filelib:is_dir(filename:join(RetiredDir, "2023-12"))),
        ?assert(filelib:is_dir(filename:join(RetiredDir, "2024-01"))),
        ?assert(filelib:is_dir(filename:join(RetiredDir, "2025-02"))),

        %% Invalid directories and files should still exist since they were
        %% ignored
        ?assert(filelib:is_dir(filename:join(RetiredDir, "not-a-date"))),
        ?assert(filelib:is_dir(filename:join(RetiredDir, "2023-13"))),
        ?assert(filelib:is_dir(filename:join(RetiredDir, "2023-0"))),
        ?assert(filelib:is_dir(filename:join(RetiredDir, "2023"))),
        ?assert(filelib:is_file(filename:join(RetiredDir, "test.key.1")))

    after
        %% Cleanup test directory
        ok = misc:rm_rf(RetiredDir)
    end.

parse_retired_dir_name_test() ->
    ?assertEqual({ok, {2023, 12}}, parse_retired_dir_name("2023-12")),
    ?assertEqual({ok, {2024, 1}}, parse_retired_dir_name("2024-1")),
    ?assertEqual({ok, {2024, 1}}, parse_retired_dir_name("2024-01")),
    ?assertEqual(error, parse_retired_dir_name("2024-13")),
    ?assertEqual(error, parse_retired_dir_name("2024-0")),
    ?assertEqual(error, parse_retired_dir_name("2024")),
    ?assertEqual(error, parse_retired_dir_name("2024-")),
    ?assertEqual(error, parse_retired_dir_name("2024-")),
    ?assertEqual(error, parse_retired_dir_name("-12")),
    ?assertEqual(error, parse_retired_dir_name("abc-12")),
    ?assertEqual(error, parse_retired_dir_name("2024-abc")),
    ?assertEqual(error, parse_retired_dir_name("")),
    ?assertEqual(error, parse_retired_dir_name("2024-12-25")).


-define(A, "a0000000-0000-0000-0000-000000000000").
-define(B, "b0000000-0000-0000-0000-000000000000").
-define(C, "c0000000-0000-0000-0000-000000000000").
-define(D, "d0000000-0000-0000-0000-000000000000").
-define(E, "e0000000-0000-0000-0000-000000000000").
-define(F, "f0000000-0000-0000-0000-000000000000").


select_files_to_retire_test() ->
    C = ?cut(lists:sort(select_files_to_retire("dir", misc:shuffle(_1),
                                               {ok, misc:shuffle(_2)}))),
    Files = [?A".key.1",
             ?B".key.55", ?B".key.56", ?B".key.57",
             ?C".key.23",
             ?D".key.2", ?D".key.34",
             ?E".key.5",
             %% Invalid files (should be ignored):
             "garbage", "garbage.", "garbage.asd", "garbage.key",
             "garbage.key.", "garbage.key.sdf", "garbage.key.435",
             ?A".", ?A".dsf", ?A".key", ?A".key.", ?A".key.asd"],
    ?assertEqual([], C([], [])),
    ?assertEqual([], C([<<?A>>, <<?B>>], [])),
    ?assertEqual([?A".key.1",  ?B".key.55", ?B".key.56",
                  ?B".key.57", ?C".key.23", ?D".key.2",
                  ?D".key.34", ?E".key.5"],
                 C([], Files)),
    ?assertEqual([?B".key.55", ?B".key.56", ?B".key.57",
                  ?C".key.23", ?D".key.2",  ?D".key.34",
                  ?E".key.5"],
                 C([<<?A>>], Files)),
    ?assertEqual([?A".key.1", ?B".key.55", ?B".key.56",
                  ?C".key.23", ?D".key.2", ?D".key.34",
                  ?E".key.5"],
                 C([<<?B>>], Files)),
    ?assertEqual([?A".key.1",  ?B".key.55", ?B".key.56",
                  ?B".key.57", ?D".key.2", ?D".key.34",
                  ?E".key.5"],
                 C([<<?C>>], Files)),
    ?assertEqual([?B".key.55", ?B".key.56", ?D".key.2",
                  ?D".key.34", ?E".key.5"],
                 C([<<?A>>, <<?B>>, <<?C>>], Files)),
    ?assertEqual([?B".key.55", ?B".key.56", ?D".key.2"],
                 C([<<?A>>, <<?B>>, <<?C>>, <<?D>>, <<?E>>, <<?F>>], Files)).

-endif.

key_path({bucketDek, Bucket}) ->
    case key_path(bucketDek) of
        undefined ->
            undefined;
        PathToDeks ->
            case ns_bucket:uuid(Bucket, direct) of
                not_present ->
                    erlang:error(bucket_not_found);
                UUID ->
                    UUIDStr = binary_to_list(UUID),
                    iolist_to_binary(filename:join([PathToDeks, UUIDStr,
                                                    "deks"]))
            end
    end;
%% Only bucketDek can change in config, other paths are static.
%% Moreover we don't have ns_config started yet when we already need other
%% paths (like logDek path), so we can't call ns_config:read_key_fast for
%% those paths.
key_path(bucketDek) ->
    Cfg = ns_config:read_key_fast(ns_config_sm_key(), []),
    cb_gosecrets_runner:key_path(bucketDek, Cfg);
key_path(Kind) ->
    cb_gosecrets_runner:key_path(Kind, []).

bucket_dek_id(BucketUUID, DekId) ->
    iolist_to_binary(filename:join([binary_to_list(BucketUUID), "deks",
                                    DekId])).

retired_keys_dir() ->
    filename:join(path_config:component_path(data), "retired_keys").

retire_key(Kind, Filename) ->
    Dir = key_path(Kind),
    FromPath = filename:join(Dir, Filename),
    {{Y, M, _}, _} = calendar:universal_time(),
    MonthDir = lists:flatten(io_lib:format("~b-~b", [Y, M])),
    ToPath = filename:join([retired_keys_dir(), MonthDir, Filename]),
    case filelib:ensure_dir(ToPath) of
        ok ->
            case misc:atomic_rename(FromPath, ToPath) of
                ok -> ok;
                {error, Reason} ->
                    ?log_error("Failed to retire ~p key ~p (~p): ~p",
                                [Kind, Filename, FromPath, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?log_error("Failed to ensure dir ~p when retiring key ~p: ~p",
                       [ToPath, Filename, Reason]),
            {error, Reason}
    end.
