%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks_raw_utils).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").
-include_lib("ns_common/include/cut.hrl").

-export([bootstrap_get_deks/2,
         external_list/4,
         read_deks_file/3,
         new_deks_file_record/3,
         format_error/1]).

-type deks_file_record() :: #{is_enabled := boolean(),
                              active_id := cb_deks:dek_id() | undefined,
                              dek_ids := [cb_deks:dek_id()]}.

-spec external_list(cb_deks:dek_kind(),
                    fun((cb_deks:dek_id()) -> cb_deks:dek()),
                    fun((binary(), binary()) -> ok | {error, _}), #{}) ->
          {ok, {undefined | cb_deks:dek_id(), [cb_deks:dek_id()], boolean()}} |
          {error, {read_dek_cfg_file_error, {string, term()}}}.
external_list(DekKind, GetCfgDekFun, VerifyMacFun, Opts) ->
    ConfigDir =
        case maps:get(config_path_override, Opts, undefined) of
            undefined ->
                path_config:component_path(data, "config");
            P ->
                P
        end,
    DeksFilePath = filename:join(ConfigDir, ?DEK_CFG_FILENAME),
    maybe
        {ok, Term} ?= read_deks_file(DeksFilePath, GetCfgDekFun, VerifyMacFun),
        case maps:find(DekKind, Term) of
            {ok, #{is_enabled := Enabled,
                   active_id := ActiveKeyId,
                   dek_ids := KeyIds}} ->
                {ok, {ActiveKeyId, KeyIds, Enabled}};
            error ->
                {ok, {undefined, [], false}}
        end
    end.

-spec bootstrap_get_deks(configDek | logDek | auditDek, map()) ->
          {ok, cb_crypto:dek_snapshot()} | {error, _}.
bootstrap_get_deks(DekKind, Opts) ->
    maybe
        GetCfgDekFun =
            fun (CfgKeyId) ->
                case external_read_keys(configDek, [CfgKeyId], Opts) of
                    {ok, {[K], []}} -> {ok, K};
                    {ok, {[], [{CfgKeyId, E}]}} -> {error, E};
                    {error, E} -> {error, E}
                end
            end,
        {ok, {ActiveDekId, DekIds, Enabled}} ?=
            external_list(DekKind, GetCfgDekFun, fun (_, _) -> ok end, Opts),
        {empty, [_ | _]} ?= {empty, DekIds},
        {ok, {Deks, Errors}} ?= external_read_keys(DekKind, DekIds, Opts),
        LogFun = maps:get(log_fun, Opts, fun (_S, _F, _A) -> ok end),
        lists:foreach(fun ({Id, E}) ->
                          %% Using info because some keys errors can be
                          %% ignored
                          LogFun(info, "Error reading key ~s: ~s", [Id, E])
                      end, Errors),
        case Enabled of
            true ->
                case proplists:get_value(ActiveDekId, Errors) of
                    undefined ->
                        DS = cb_crypto:create_deks_snapshot(ActiveDekId, Deks,
                                                            undefined),
                        {ok, DS};
                    KeyError ->
                        {error, {active_key_read_failure, KeyError}}
                end;
            false ->
                DS = cb_crypto:create_deks_snapshot(undefined, Deks, undefined),
                {ok, DS}
        end
    else
        {error, {read_dek_cfg_file_error,
                 {_CfgDekFilePath, {dump_keys_returned, _, _} = E}}} ->
            %% External utilities rely on the dump_keys_returned error, so
            %% we need to remove the read_dek_cfg_file_error wrap here
            %% (because the real reason is not that we can't read the file,
            %% but the fact that we can't read cfg key).
            {error, E};
        {error, missing_kind} -> %% No info about this Kind in the deks file
            {ok, cb_crypto:create_deks_snapshot(undefined, [], undefined)};
        {empty, []} ->
            {ok, cb_crypto:create_deks_snapshot(undefined, [], undefined)};
        {error, _} = Error ->
            Error
    end.

%% Reads keys via dump-guts utility
%% This function is supposed to be used by external utilities
external_read_keys(DekKind, KeyIds, Opts) ->
    LogFun = maps:get(log_fun, Opts, fun (_S, _F, _A) -> ok end),
    ConfigDir =
        case maps:get(config_path_override, Opts, undefined) of
            undefined ->
                path_config:component_path(data, "config");
            P ->
                P
        end,
    BinDir =
        case maps:get(bin_path_override, Opts, undefined) of
            undefined ->
                path_config:component_path(bin);
            BPath ->
                BPath
        end,
    GosecretsCfg = filename:join(ConfigDir, "gosecrets.cfg"),
    GosecretsExec = case misc:is_windows() of
                        true -> "gosecrets.exe";
                        false -> "gosecrets"
                    end,
    GosecretsPath = filename:join(BinDir, GosecretsExec),
    HiddenPass = maps:get(hidden_pass, Opts, ?HIDE(undefined)),
    Input = case ?UNHIDE(HiddenPass) of
                undefined -> undefined;
                Pass -> ?HIDE(Pass ++ "\n")
            end,
    Path = filename:join(BinDir, "dump-keys"),
    maybe
        {ok, DumpKeysPath} ?= case os:find_executable(Path) of
                                  false -> {error, {no_dump_keys, Path}};
                                  DKPath -> {ok, DKPath}
                              end,
        KeyIdsStr = [binary_to_list(K) || K <- KeyIds],
        DumpKeysArgs = ["--gosecrets", GosecretsPath,
                        "--config", GosecretsCfg,
                        "--key-kind", atom_to_list(DekKind)] ++
                       ["--stdin-password" || Input /= undefined] ++
                       ["--key-ids"] ++ KeyIdsStr,
        LogFun(debug, "Calling dump-keys (~s) with args: ~p",
               [DumpKeysPath, DumpKeysArgs]),
        {0, Output} ?= ns_secrets:call_external_script(DumpKeysPath,
                                                       DumpKeysArgs,
                                                       Input,
                                                       60000),
        {JsonKeys} = ejson:decode(Output),
        {Deks, Errors} =
            misc:partitionmap(
              fun ({Id, {Props}}) ->
                  case decode_dump_keys_response(Props) of
                      {ok, {DekKind, #{id := Id} = Dek}} ->
                          {left, Dek};
                      {ok, {DekKind, #{id := UnexpectedId}}} ->
                          {right, {Id, {unexpected_dek_id, UnexpectedId}}};
                      {ok, {UnexpectedKind, _}} ->
                          {right, {Id, {unexpected_dek_kind, UnexpectedKind}}};
                      {error, Error} ->
                          {right, {Id, Error}}
                  end
              end, JsonKeys),
        {ok, {Deks, Errors}}
    else
        {error, _} = Error ->
            Error;
        {Status, ErrorsBin} when is_integer(Status) ->
            {error, {dump_keys_returned, Status, ErrorsBin}}
    end.

decode_dump_keys_response(Props) ->
    case maps:from_list(Props) of
        #{<<"result">> := <<"error">>,
          <<"response">> := Error} ->
            {error, Error};
        #{<<"result">> := <<"raw-aes-gcm">>,
          <<"response">> := KeyProps} ->
            case encryption_service:decode_key_info(KeyProps) of
                {ok, {KeyId, Kind, Info}} ->
                    Dek = encryption_service:new_dek_record(KeyId,
                                                            'raw-aes-gcm',
                                                            Info),
                    {ok, {Kind, Dek}};
                {error, Error} ->
                    Msg = io_lib:format("Failed to decode dek info: ~p",
                                        [Error]),
                    {error, iolist_to_binary(Msg)}
            end;
        _ ->
            {error, <<"Invalid dek format">>}
    end.

-spec read_deks_file(string(), fun((cb_deks:dek_id()) -> cb_deks:dek()),
                     fun((binary(), binary()) -> ok | {error, _})) ->
          {ok, #{cb_deks:dek_kind() := deks_file_record()}} |
          {error, {read_dek_cfg_file_error, {string, term()}}}.
read_deks_file(Path, GetCfgDekFun, VerifyMacFun) ->
    case cb_crypto:read_file(Path, GetCfgDekFun) of
        {T, <<MacSize:32/unsigned-integer, Rest/binary>>} when T == decrypted;
                                                               T == raw ->
            case Rest of
                <<Mac:MacSize/binary, Data/binary>> ->
                    case VerifyMacFun(Mac, Data) of
                        ok ->
                            try
                                {ok, binary_to_term(Data)}
                            catch
                                _:_ ->
                                    {error, {read_dek_cfg_file_error,
                                             {Path, invalid_file_format}}}
                            end;
                        {error, R} ->
                            {error, {read_dek_cfg_file_error, {Path, R}}}
                    end;
                _ ->
                    {error, {read_dek_cfg_file_error, {Path, missing_mac}}}
            end;
        {T, _} when T == decrypted; T == raw ->
            {error, {read_dek_cfg_file_error, {Path, missing_mac}}};
        {error, enoent} ->
            {ok, #{}};
        {error, Reason} ->
            {error, {read_dek_cfg_file_error, {Path, Reason}}}
    end.

-spec new_deks_file_record(cb_deks:dek_id() | undefined, boolean(),
                           [cb_deks:dek_id()]) -> deks_file_record().
new_deks_file_record(ActiveId, IsEnabled, Ids) ->
    #{is_enabled => IsEnabled,
      active_id => ActiveId,
      dek_ids => Ids}.

format_error({read_dek_cfg_file_error, {Path, Reason}})
                                                    when is_binary(Reason) ->
    io_lib:format("Failed to read deks file ~s: ~s",
                  [Path, Reason]);
format_error({read_dek_cfg_file_error, {Path, Reason}}) ->
    io_lib:format("Failed to read deks file ~s: ~s",
                  [Path, format_error(Reason)]);
format_error({active_key_read_failure, Reason}) when is_binary(Reason) ->
    io_lib:format("Failed to read active key: ~s", [Reason]);
format_error(invalid_file_format) ->
    "Invalid file format";
format_error(invalid_file_encryption) ->
    "Invalid file encryption";
format_error(missing_mac) ->
    "Missing MAC";
format_error(bad_header) ->
    "Bad encrypted file header";
format_error({unsupported_encryption_version, Version}) ->
    io_lib:format("Unsupported encrypted file version: ~p", [Version]);
format_error({no_dump_keys, Path}) ->
    io_lib:format("dump-keys utility not found at ~s", [Path]);
format_error({dump_keys_returned, Status, OutputBin}) ->
    io_lib:format("dump-keys utility returned error ~p: ~s",
                  [Status, OutputBin]);
format_error(decrypt_error) ->
    "Failed to decrypt data";
format_error(invalid_data_chunk) ->
    "Invalid encrypted data chunk";
format_error(unknown_magic) ->
    "Unknown encrypted file magic";
format_error(enoent) ->
    "File not found";
format_error({read_active_key_error, Error}) ->
    io_lib:format("Failed to read active key: ~s", [format_error(Error)]);
format_error(key_not_found) ->
    "Key not found";
format_error(need_more_data) ->
    "Incomplete data";
format_error(incomplete_data) ->
    "Incomplete data";
format_error(incomplete_magic) ->
    "Incomplete encrypted file magic";
format_error(no_active_key) ->
    "No active key available";
format_error({still_in_use, StillInUse}) ->
    io_lib:format("Keys still in use: ~s", [format_key_list(StillInUse)]);
format_error({invalid_secret_id, SecretId, Reason}) ->
    io_lib:format("Invalid secret ID ~p: ~p", [SecretId, Reason]);
format_error(not_found) ->
    "Not found";
format_error(retry) ->
    "Operation needs to be retried";
format_error({unsupported_compression_type, Type}) ->
    io_lib:format("Unsupported compression type: ~p", [Type]);
format_error({encrypt_key_error, Msg}) ->
    io_lib:format("Failed to encrypt key: ~s", [Msg]);
format_error({decrypt_key_error, Msg}) ->
    io_lib:format("Failed to decrypt key: ~s", [Msg]);
format_error({store_key_error, Msg}) ->
    io_lib:format("Failed to store key: ~s", [Msg]);
format_error({read_key_error, Msg}) ->
    io_lib:format("Failed to read key: ~s", [Msg]);
format_error({mac_calculation_error, Msg}) ->
    io_lib:format("Failed to calculate MAC: ~s", [Msg]);
format_error({mac_verification_error, Msg}) ->
    io_lib:format("Failed to verify MAC: ~s", [Msg]);
format_error({rotate_integrity_tokens_error, Msg}) ->
    io_lib:format("Failed to rotate integrity tokens: ~s", [Msg]);
format_error({remove_old_integrity_tokens_error, Msg}) ->
    io_lib:format("Failed to remove old integrity tokens: ~s", [Msg]);
format_error(Reason) ->
    io_lib:format("~p", [Reason]).

format_key_list([]) ->
    "(none)";
format_key_list(KeyList) ->
    lists:join(", ", lists:map(fun (?NULL_DEK) -> "null";
                                   (B) when is_binary(B) -> B
                            end, KeyList)).
