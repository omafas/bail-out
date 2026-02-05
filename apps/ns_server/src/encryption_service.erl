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
         maybe_wrap_encryption_error/2,
         change_password/1,
         get_keys_ref/0,
         rotate_data_key/0,
         maybe_clear_backup_key/1,
         get_state/0,
         os_pid/0,
         reconfigure/1,
         store_kek/6,
         store_aws_key/4,
         store_gcp_key/4,
         store_azure_key/4,
         store_kmip_key/5,
         store_hashi_key/5,
         store_dek/6,
         read_dek/2,
         read_dek_file/2,
         extract_dek_id/1,
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
         revalidate_key_cache/0,
         cached_keys_list/0,
         new_dek_record/3,
         new_raw_aes_dek_info/4]).

-ifdef(TEST).
-export([format_aes_key_params/4]).
-endif.


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

store_kek(Id, Key, KekIdToEncrypt, CreationDT, CanBeCached, TestOnly) ->
    store_key(kek, Id, 'raw-aes-gcm', CreationDT,
              ejson:encode(format_aes_key_params(Key, KekIdToEncrypt,
                                                 CanBeCached, false)),
              TestOnly).

store_dek({bucketDek, BucketUUID}, Id, Key, KekIdToEncrypt, CreationDT, Imported) ->
    store_dek(bucketDek, bucket_dek_id(BucketUUID, Id), Key, KekIdToEncrypt,
              CreationDT, Imported);
store_dek(Kind, Id, Key, KekIdToEncrypt, CreationDT, Imported) ->
    store_key(Kind, Id, 'raw-aes-gcm', CreationDT,
              ejson:encode(format_aes_key_params(Key, KekIdToEncrypt, false,
                                                 Imported)),
              false).

format_aes_key_params(Key, KekIdToEncrypt, CanBeCached, Imported) ->
    {[{keyMaterial, base64:encode(Key)},
      {encryptionKeyName, format_encryption_key_name(KekIdToEncrypt)},
      {canBeCached, CanBeCached},
      {imported, Imported}]}.

store_aws_key(Id, Params, CreationDT, TestOnly) ->
    store_key(kek, Id, 'awskms-symmetric', CreationDT,
              ejson:encode(format_aws_key_params(Params)), TestOnly).

format_aws_key_params(#{key_arn := KeyArn, region := Region,
                        profile := Profile, config_file := ConfigFile,
                        credentials_file := CredsFile, use_imds := UseIMDS,
                        req_timeout_ms := ReqTimeoutMs}) ->
    {[{keyArn, iolist_to_binary(KeyArn)},
      {region, iolist_to_binary(Region)},
      {profile, iolist_to_binary(Profile)},
      {credsFile, iolist_to_binary(CredsFile)},
      {configFile, iolist_to_binary(ConfigFile)},
      {useIMDS, UseIMDS},
      {reqTimeoutMs, ReqTimeoutMs}]}.

store_gcp_key(Id, Params, CreationDT, TestOnly) ->
    store_key(kek, Id, 'gcpkms-symmetric', CreationDT,
              ejson:encode(format_gcp_key_params(Params)), TestOnly).

format_gcp_key_params(#{key_resource_id := KeyResourceId,
                        credentials_file := CredsPath,
                        req_timeout_ms := ReqTimeoutMs}) ->
    {[{keyResourceId, iolist_to_binary(KeyResourceId)},
      {credentialsFile, iolist_to_binary(CredsPath)},
      {reqTimeoutMs, ReqTimeoutMs}]}.

store_azure_key(Id, Params, CreationDT, TestOnly) ->
    store_key(kek, Id, 'azurekms', CreationDT,
              ejson:encode(format_azure_key_params(Params)), TestOnly).

format_azure_key_params(#{key_url := KeyUrl,
                          encryption_algorithm := Algorithm,
                          credentials_chain := CredentialsChain,
                          req_timeout_ms := ReqTimeoutMs}) ->
    {[{keyUrl, iolist_to_binary(KeyUrl)},
      {algorithm, iolist_to_binary(Algorithm)},
      {credentialsChain,  iolist_to_binary(lists:join(",", CredentialsChain))},
      {reqTimeoutMs, ReqTimeoutMs}]}.

store_kmip_key(Id, Params, KekIdToEncrypt, CreationDT, TestOnly) ->
    store_key(kek, Id, kmip, CreationDT,
              ejson:encode(format_kmip_key_params(Params, KekIdToEncrypt)),
              TestOnly).

format_kmip_key_params(#{host := Host,
                         port := Port,
                         req_timeout_ms := ReqTimeoutMs,
                         kmip_id := KmipId,
                         key_path := KeyPath,
                         cert_path := CertPath,
                         key_passphrase := PassData,
                         ca_selection := CaSel,
                         encryption_approach := Appr}, KekIdToEncrypt) ->
    {[{host, iolist_to_binary(Host)},
      {port, Port},
      {reqTimeoutMs, ReqTimeoutMs},
      {kmipId, KmipId},
      {keyPath, iolist_to_binary(KeyPath)},
      {certPath, iolist_to_binary(CertPath)},
      {keyPassphrase, base64:encode(PassData)},
      {caSelection, CaSel},
      {cbCaPath, iolist_to_binary(ns_ssl_services_setup:ca_file_path())},
      {encryptionApproach, Appr},
      {encryptionKeyName, format_encryption_key_name(KekIdToEncrypt)}]}.

store_hashi_key(Id, Params, KekIdToEncrypt, CreationDT, TestOnly) ->
    store_key(kek, Id, hashikms, CreationDT,
        ejson:encode(format_hashi_key_params(Params, KekIdToEncrypt)),
        TestOnly).

format_hashi_key_params(#{key_url := KeyURL,
                          req_timeout_ms := ReqTimeoutMs,
                          key_path := KeyPath,
                          cert_path := CertPath,
                          key_passphrase := PassData,
                          ca_selection := CaSel}, KekIdToEncrypt) ->
    {[{keyURL, iolist_to_binary(KeyURL)},
      {reqTimeoutMs, ReqTimeoutMs},
      {keyPath, iolist_to_binary(KeyPath)},
      {certPath, iolist_to_binary(CertPath)},
      {keyPassphrase, base64:encode(PassData)},
      {encryptionKeyName, format_encryption_key_name(KekIdToEncrypt)},
      {caSelection, CaSel},
      {cbCaPath, iolist_to_binary(ns_ssl_services_setup:ca_file_path())}]}.

format_encryption_key_name(undefined) -> <<"encryptionService">>;
format_encryption_key_name(KekIdToEncrypt) -> KekIdToEncrypt.

read_dek(Kind, DekId) ->
    {NewId, NewKind} = case Kind of
                           {bucketDek, BucketUUID} ->
                               {bucket_dek_id(BucketUUID, DekId), bucketDek};
                           _ ->
                               {DekId, Kind}
                       end,
    case ?wrap_error_msg(
           cb_gosecrets_runner:read_key(?RUNNER, NewKind, NewId),
           read_key_error, [{kind, cb_deks:kind2bin(NewKind, <<"unknown">>)},
                            {key_UUID, NewId}]) of
        {ok, Json} ->
            case decode_dek(Json) of
                {ok, {Kind, #{id := DekId} = Dek}} ->
                    {ok, Dek};
                {ok, {Kind, #{id := UnexpectedId}}} ->
                    {error, {unexpected_dek_id, UnexpectedId}};
                {ok, {UnexpectedKind, _}} ->
                    {error, {unexpected_dek_kind, UnexpectedKind}};
                {error, Error} ->
                    Msg = io_lib:format("Failed to decode dek: ~p", [Error]),
                    {error, {dek_decode_error, lists:flatten(Msg)}}
            end;
        {error, Error} ->
            {error, Error}
    end.

read_dek_file(Path, VerifyProof) when is_list(Path), is_boolean(VerifyProof) ->
    case ?wrap_error_msg(
           cb_gosecrets_runner:read_key_file(?RUNNER, Path, VerifyProof),
           read_key_file_error, [{path, Path}]) of
        {ok, Json} ->
            case decode_dek(Json) of
                {ok, {Kind, Dek}} -> {ok, {Kind, Dek}};
                {error, Error} ->
                    Msg = io_lib:format("Failed to decode dek: ~p", [Error]),
                    {error, {dek_decode_error, lists:flatten(Msg)}}
            end;
        {error, Error} ->
            {error, Error}
    end.

extract_dek_id(Path) ->
    FilenameBin = iolist_to_binary(filename:basename(Path)),
    case string:split(FilenameBin, ".key.", trailing) of
        [DekId, _] -> {ok, DekId};
        _ -> {error, {invalid_dek_file_name, Path}}
    end.

decode_dek(Json) ->
    maybe
        {ok, Props} ?= try ejson:decode(Json) of
                           {P} -> {ok, P};
                           _ -> {error, dek_not_json_object}
                       catch
                           _:Error -> {error, Error}
                       end,
        ok ?= case proplists:get_value(<<"type">>, Props) of
                  undefined -> {error, missing_type};
                  <<"raw-aes-gcm">> -> ok;
                  T -> {error, {unknown_dek_type, T}}
              end,
        {ok, {DekId, Kind, Info}} ?=
            decode_key_info(proplists:get_value(<<"info">>, Props)),
        {ok, {Kind, new_dek_record(DekId, 'raw-aes-gcm', Info)}}
    end.

decode_key_info({InfoProps}) when is_list(InfoProps) ->
    maybe
        {ok, {Kind, KeyId}} ?= info_props_to_kind(InfoProps),
        {ok, Key} ?=
            case proplists:get_value(<<"key">>, InfoProps) of
                undefined -> {error, missing_key};
                B64Key ->
                    try
                        {ok, base64:decode(B64Key)}
                    catch
                        _:_ -> {error, invalid_key_base64}
                    end
            end,
        {ok, EncryptionKeyId} ?=
            case proplists:get_value(<<"encryptionKeyId">>, InfoProps) of
                undefined -> {error, missing_encryption_key_id};
                KekId -> {ok, KekId}
            end,
        {ok, CreationTime} ?=
            case proplists:get_value(<<"creationTime">>, InfoProps) of
                undefined -> {error, missing_creation_time};
                CreationTimeISO ->
                    try
                        {ok, iso8601:parse(CreationTimeISO)}
                    catch
                        _:_ -> {error, invalid_creation_time}
                    end
            end,
        {ok, Imported} ?=
            case proplists:get_value(<<"imported">>, InfoProps) of
                undefined -> {ok, false};
                true -> {ok, true};
                false -> {ok, false};
                _ -> {error, invalid_imported}
            end,
        Info = new_raw_aes_dek_info(Key, EncryptionKeyId, CreationTime,
                                    Imported),
        {ok, {KeyId, Kind, Info}}
    end;
decode_key_info(_InfoProps) ->
    {error, dek_info_not_json_object}.

%% Build cb_deks:dek_kind() from kind and name in InfoProps.
%% Non-bucket kinds (configDek, logDek, auditDek) are stored as atoms in file.
%% For bucketDek, name is bucket_dek_id(BucketUUID, DekId) i.e. "UUID/deks/Id".
-spec info_props_to_kind(list()) ->
          {ok, {cb_deks:dek_kind(), cb_deks:dek_id()}} | {error, term()}.
info_props_to_kind(InfoProps) ->
    maybe
        {ok, KindBin} ?= case proplists:get_value(<<"kind">>, InfoProps) of
                             undefined -> {error, missing_kind};
                             <<>> -> {error, missing_kind};
                             KB -> {ok, KB}
                         end,
        {ok, NameBin} ?= case proplists:get_value(<<"name">>, InfoProps) of
                             undefined -> {error, missing_name};
                             <<>> -> {error, missing_name};
                             NB -> {ok, NB}
                         end,
        NonBucketKinds = ?DEK_KIND_LIST_STATIC,
        KindBinToKind = [{atom_to_binary(K), K} || K <- NonBucketKinds],
        case KindBin of
            <<"bucketDek">> ->
                %% name is bucket_dek_id(BucketUUID, DekId)
                case string:split(NameBin, "/deks/") of
                    [BucketUUIDBin, DekIdBin] when BucketUUIDBin =/= <<>>,
                                                   DekIdBin =/= <<>> ->
                        {ok, {{bucketDek, BucketUUIDBin}, DekIdBin}};
                    _ ->
                        {error, invalid_bucket_dek_name}
                end;
            _ ->
                case lists:keyfind(KindBin, 1, KindBinToKind) of
                    {_, Kind} ->
                        {ok, {Kind, NameBin}};
                    false ->
                        {error, {unknown_kind, KindBin}}
                end
        end
    end.

new_dek_record(Id, error, Reason) when is_binary(Id) ->
    #{id => Id, type => error, reason => Reason};
new_dek_record(Id, 'raw-aes-gcm', Info) when is_binary(Id) ->
    #{id => Id, type => 'raw-aes-gcm', info => Info}.

new_raw_aes_dek_info(Key, EncryptionKeyId, CreationTime, Imported) ->
    #{key => ?HIDE(Key),
      encryption_key_id => EncryptionKeyId,
      creation_time => CreationTime,
      imported => Imported}.

-ifdef(TEST).

dek_error_pattern_test() ->
    ?assertMatch(?DEK_ERROR_PATTERN(<<"id">>, {test, "reason"}),
                 new_dek_record(<<"id">>, error, {test, "reason"})),
    ?assertNotMatch(?DEK_ERROR_PATTERN(_, _),
                    new_dek_record(<<"id">>, 'raw-aes-gcm',
                                   new_raw_aes_dek_info(
                                     <<"key">>,
                                     <<"encryptionKeyId">>,
                                     {{2024, 01, 01}, {22, 00, 00}},
                                     false))).

-endif.

encrypt_key(Data, AD, KekId) ->
    encrypt_key(Data, AD, KekId, true).

encrypt_key(Data, AD, KekId, CanUseCachedKeys)
        when is_binary(Data), is_binary(AD), is_binary(KekId),
             is_boolean(CanUseCachedKeys) ->
    FinalAD = <<AD/binary, KekId/binary>>,
    ?wrap_error_msg(
      cb_gosecrets_runner:encrypt_with_key(?RUNNER, Data, FinalAD, kek, KekId,
                                           CanUseCachedKeys),
      encrypt_key_error, [{key_UUID, KekId}]).

decrypt_key(Data, AD, KekId) ->
    decrypt_key(Data, AD, KekId, true).

decrypt_key(Data, AD, KekId, CanUseCachedKeys)
        when is_binary(Data), is_binary(AD), is_binary(KekId),
             is_boolean(CanUseCachedKeys) ->
    FinalAD = <<AD/binary, KekId/binary>>,
    ?wrap_error_msg(
      cb_gosecrets_runner:decrypt_with_key(?RUNNER, Data, FinalAD, kek, KekId,
                                           CanUseCachedKeys),
      decrypt_key_error, [{key_UUID, KekId}]).

%% This function can be called by other nodes
test_existing_key(KekId) when is_binary(KekId) ->
    RandomData = rand:bytes(16),
    RandomAD = rand:bytes(16),
    maybe
        %% We don't want to use cached keys here because we want to test
        %% that the entire chain of keys is in working condition.
        %% E.g. The key-being-tested can be encrypted with a cached key, which
        %% is encrypted with a broken key. If we allow using cached keys, this
        %% test will pass, but after restart we won't be able to decrypt the
        %% key, because the cached key not be there anymore.
        {ok, EncryptedData} ?= encrypt_key(RandomData, RandomAD, KekId,
                                           false),
        {ok, DecryptedData} ?= decrypt_key(EncryptedData, RandomAD, KekId,
                                           false),
        case RandomData =:= DecryptedData of
            true -> ok;
            false -> {error, decrypted_data_mismatch}
        end
    else
        {error, Error} ->
            {error, maybe_wrap_encryption_error(Error, invalid_key_settings)}
    end.

maybe_wrap_encryption_error({T, Error}, Wrapper) when T == encrypt_key_error;
                                                      T == decrypt_key_error ->
    %% We know that if this function is called the secret actually
    %% exists, so if we get this error, it means that we have problems
    %% saving the key on disk (likely because something is wrong with secret
    %% settings). Changing the error to avoid confusion.
    {Wrapper, Error};
maybe_wrap_encryption_error(Error, _Wrapper) ->
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

cached_keys_list() ->
    ?wrap_error_msg(cb_gosecrets_runner:cached_keys_list(?RUNNER),
                    cached_keys_list_error, []).

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

store_key(Kind, Name, Type, {{_, _, _}, {_, _, _}} = CreationDT,
          KeyData, TestOnly) when is_atom(Kind),
                                  is_binary(Name),
                                  is_atom(Type),
                                  is_binary(KeyData),
                                  is_atom(TestOnly) ->
    CreationDTISO = iso8601:format(CreationDT),
    KindBin = cb_deks:kind2bin(Kind, <<"unknown">>),
    ErrorAtom = case TestOnly of
                    true -> store_key_error_test;
                    false -> store_key_error
                end,

    ?wrap_error_msg(
      cb_gosecrets_runner:store_key(?RUNNER, Kind, Name, Type, CreationDTISO,
                                    KeyData, TestOnly),
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
maybe_log_error_to_user_log(A, Msg, _)
                                   when A == store_key_error;
                                        A == rotate_integrity_tokens_error;
                                        A == remove_old_integrity_tokens_error;
                                        A == mac_calculation_error;
                                        A == mac_verification_error ->
    ale:error(?USER_LOGGER, "Encryption key operation failed (~p): ~s",
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
                          ok ->
                              ns_server_stats:notify_counter(
                                {<<"encr_at_rest_retire_key_events">>,
                                 [{type, cb_deks:kind2bin(Kind)}]}),
                              false;
                          {error, Reason} ->
                              ns_server_stats:notify_counter(
                                {<<"encr_at_rest_retire_key_failures">>,
                                 [{type, cb_deks:kind2bin(Kind)}]}),
                              {true, {Filename, Reason}}
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

key_path({bucketDek, BucketUUID}) ->
    case key_path(bucketDek) of
        undefined ->
            undefined;
        PathToDeks ->
            iolist_to_binary(
              filename:join([PathToDeks, binary_to_list(BucketUUID), "deks"]))
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
                {error, exdev} ->
                    %% Cross-filesystem rename, fall back to copy + delete
                    ?log_debug("Cross-filesystem rename failed, "
                               "will try copy+delete ~p to ~p",
                               [FromPath, ToPath]),
                    case file:copy(FromPath, ToPath) of
                        {ok, _} -> ok;
                        {error, CopyReason} ->
                            %% Copy failed, but it is not critical enough to
                            %% fail the whole operation, as the reason we keep
                            %% the keys in retired_keys_dir is "just in case".
                            %% The system can work without it.
                            ?log_error("Copy failed ~p to ~p: ~p",
                                       [FromPath, ToPath, CopyReason])
                    end,
                    case file:delete(FromPath) of
                        ok -> ok;
                        {error, DeleteReason} ->
                            ?log_error("Failed to delete file ~p: ~p",
                                       [FromPath, DeleteReason]),
                            {error, DeleteReason}
                    end;
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
