%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_gosecrets_runner).

-behaviour(gen_server).

-include("ns_common.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-endif.

-export([start_link/3, start_link/4, extract_hidden_pass/0,
         extract_hidden_pass/1, stop/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([decrypt/2,
         encrypt/2,
         change_password/2,
         get_keys_ref/1,
         rotate_data_key/1,
         maybe_clear_backup_key/2,
         get_state/1,
         os_pid/1,
         copy_secrets/2,
         cleanup_secrets/2,
         set_config/3,
         store_key/9,
         encrypt_with_key/5,
         decrypt_with_key/5,
         read_key/3,
         defaults/0,
         key_path/2,
         rotate_integrity_tokens/2,
         remove_old_integrity_tokens/2,
         get_key_id_in_use/1,
         mac/2,
         verify_mac/3,
         revalidate_key_cache/1]).

-record(state, {config :: file:filename(),
                loop :: pid() | undefined,
                logger = default :: fun((atom(), string, [term()]) -> ok) |
                                    default}).

default_data_key_path(PType) ->
    Filename = case PType of
                   env -> "encrypted_data_keys";
                   script -> "encrypted_data_keys2"
               end,
    filename:join(path_config:component_path(data, "config"), Filename).

default_unencrypted_data_key_path() ->
    filename:join(path_config:component_path(data, "config"), "data_keys").

gosecrets_cfg_path() ->
    filename:join(path_config:component_path(data, "config"), "gosecrets.cfg").

port_file_path() ->
    filename:join(path_config:component_path(data),
                  "couchbase-server.babysitter.smport").

encrypt(Name, Data) ->
    gen_server:call(Name, {encrypt, Data}, infinity).

decrypt(Name, Data) ->
    gen_server:call(Name, {decrypt, Data}, infinity).

change_password(Name, NewPassword) ->
    gen_server:call(Name, {change_password, ?HIDE(NewPassword)}, infinity).

get_keys_ref(Name) ->
    gen_server:call(Name, get_keys_ref, infinity).

get_state(Name) ->
    gen_server:call(Name, get_state, infinity).

rotate_data_key(Name) ->
    gen_server:call(Name, rotate_data_key, infinity).

maybe_clear_backup_key(Name, DataKey) ->
    gen_server:call(Name, {maybe_clear_backup_key, DataKey}, infinity).

os_pid(Name) ->
    gen_server:call(Name, gosecrets_os_pid).

copy_secrets(Name, NewCfg) ->
    case gen_server:call(Name, {copy_secrets, NewCfg}, infinity) of
        {ok, Res} ->
            ?log_debug("Copy secrets finished: ~p", [Res]),
            {ok, Res};
        {error, _} = Error ->
            ?log_error("copy_secrets failed: ~p~nConfig: ~p", [Error, NewCfg]),
            Error
    end.

cleanup_secrets(Name, OldCfg) ->
    case gen_server:call(Name, {cleanup_secrets, OldCfg}, infinity) of
        ok ->
            ?log_debug("Secrets cleanup finished"),
            ok;
        {error, _} = Error ->
            ?log_error("cleanup_secrets failed: ~p~nConfig: ~p",
                       [Error, OldCfg]),
            Error
    end.

set_config(Name, Cfg, ResetPassword) ->
    case gen_server:call(Name, {set_config, Cfg, ResetPassword}, infinity) of
        ok ->
            ?log_debug("Set config finished"),
            ok;
        {error, _} = Error ->
            ?log_error("set_config failed: ~p~nConfig: ~p", [Error, Cfg]),
            Error
    end.

store_key(Name, Kind, KeyName, KeyType, KeyData, EncryptionKeyId, CreationDT,
          CanBeCached, TestOnly) ->
    gen_server:call(
      Name,
      {store_key, Kind, KeyName, KeyType, KeyData, EncryptionKeyId, CreationDT,
       CanBeCached, TestOnly}, infinity).

read_key(Name, Kind, KeyName) ->
    gen_server:call(Name, {read_key, Kind, KeyName}, infinity).

encrypt_with_key(Name, Data, AD, KeyKind, KeyName) ->
    gen_server:call(Name, {encrypt_with_key, Data, AD, KeyKind, KeyName},
                    infinity).

decrypt_with_key(Name, Data, AD, KeyKind, KeyName) ->
    gen_server:call(Name, {decrypt_with_key, Data, AD, KeyKind, KeyName},
                    infinity).

rotate_integrity_tokens(Name, KeyName) ->
    gen_server:call(Name, {rotate_integrity_tokens, KeyName}, infinity).

remove_old_integrity_tokens(Name, Paths) ->
    gen_server:call(Name, {remove_old_integrity_tokens, Paths}, infinity).

get_key_id_in_use(Name) ->
    gen_server:call(Name, get_key_id_in_use, infinity).

mac(Name, Data) ->
    gen_server:call(Name, {mac, Data}, infinity).

verify_mac(Name, Mac, Data) ->
    gen_server:call(Name, {verify_mac, Mac, Data}, infinity).

revalidate_key_cache(Name) ->
    gen_server:call(Name, revalidate_key_cache, infinity).

start_link(Logger, PasswordPromptAllowed, ReadOnly) ->
    start_link(Logger, PasswordPromptAllowed, ReadOnly, gosecrets_cfg_path()).

start_link(Logger, PasswordPromptAllowed, ReadOnly, ConfigPath) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [ConfigPath, Logger, PasswordPromptAllowed, ReadOnly], []).

stop() ->
    gen_server:call(?MODULE, stop, infinity).

prompt_the_password(State, Retries, ReadOnly) ->
    StdIn =
        case application:get_env(handle_ctrl_c) of
            {ok, true} ->
                erlang:open_port({fd, 0, 1}, [in, stream, binary, eof]);
            _ ->
                undefined
        end,
    try
        prompt_the_password(State, Retries, ReadOnly, StdIn)
    after
        case StdIn of
            undefined ->
                ok;
            _ ->
                port_close(StdIn)
        end
    end.

prompt_the_password(State, MaxRetries, ReadOnly, StdIn) ->
    case open_udp_socket(State) of
        {ok, Socket} ->
            try
                save_port_file(Socket),
                prompt_the_password(State, MaxRetries, ReadOnly, StdIn,
                                    Socket, _RetriesLeft = MaxRetries)
            after
                file:delete(port_file_path()),
                catch gen_udp:close(Socket)
            end;
        {error, Error} ->
            {error, {udp_socket_open_failed, Error}}
    end.

prompt_the_password(State, MaxRetries, ReadOnly, StdIn, Socket, RetriesLeft) ->
    {ok, {Addr, Port}} = inet:sockname(Socket),
    log(info, "Waiting for the master password to be supplied (UDP: ~p:~b). "
               "Attempt ~p (~p attempts left)",
               [Addr, Port, MaxRetries - RetriesLeft + 1, RetriesLeft], State),
    receive
        {StdIn, M} ->
            log(error, "Password prompt interrupted: ~p", [M], State),
            {error, interrupted};
        {udp, Socket, FromAddr, FromPort, Password} ->
            case call_init(?HIDE(Password), ReadOnly, State) of
                ok ->
                    gen_udp:send(Socket, FromAddr, FromPort, <<"ok">>),
                    ok;
                {wrong_password, Error} when RetriesLeft > 1 ->
                    log(error, "Incorrect master password. Error: ~p", [Error],
                        State),
                    gen_udp:send(Socket, FromAddr, FromPort, <<"retry">>),
                    timer:sleep(1000),
                    prompt_the_password(State, MaxRetries, ReadOnly, StdIn,
                                        Socket, RetriesLeft - 1);
                {Reply, _Reason} when Reply == error;
                                      Reply == wrong_password  ->
                    gen_udp:send(Socket, FromAddr, FromPort, <<"auth_failure">>),
                    {error, auth_failure}
            end
    end.

init([GosecretsCfgPath, Logger, PasswordPromptAllowed, ReadOnly]) ->
    State = #state{config = GosecretsCfgPath,
                   logger = Logger},

    case filelib:is_file(GosecretsCfgPath) of
        true -> ok;
        false ->
            ok = filelib:ensure_dir(GosecretsCfgPath),
            save_config(GosecretsCfgPath, default_cfg(), State)
    end,

    NewState = State#state{loop = start_gosecrets(GosecretsCfgPath, State)},

    HiddenPass = extract_hidden_pass(),

    case init_gosecrets(HiddenPass, _MaxRetries = 3, PasswordPromptAllowed,
                        ReadOnly, NewState) of
        ok ->
            {ok, NewState};
        shutdown ->
            {error, shutdown};
        {error, Error} ->
            {error, Error}
    end.

save_config(CfgPath, Cfg, State) ->
    log(debug, "Writing ~s:~n~p", [CfgPath, Cfg], State),
    CfgJson = ejson:encode(Cfg),
    case misc:atomic_write_file(CfgPath, CfgJson) of
        ok -> ok;
        {error, Error} ->
            log(error, "Could not write file '~s': ~s (~p)",
                       [CfgPath, file:format_error(Error), Error], State),
            erlang:error({write_failed, CfgPath, Error})
    end.

init_gosecrets(HiddenPass, MaxRetries, PasswordPromptAllowed, ReadOnly,
               State) ->
    case call_init(HiddenPass, ReadOnly, State) of
        ok -> ok;
        {wrong_password, ErrorMsg} ->
            case PasswordPromptAllowed and should_prompt_the_password(State) of
                true ->
                    try
                        case prompt_the_password(State, MaxRetries, ReadOnly) of
                            ok ->
                                ok;
                            {error, Error} ->
                                log(error,
                                    "Stopping babysitter because gosecrets "
                                    "password prompting has failed: ~p",
                                    [Error], State),
                                shutdown
                        end
                    catch
                        C:E:ST ->
                            log(error, "Unhandled exception: ~p~n~p", [E, ST],
                                State),
                            erlang:raise(C, E, ST)
                    end;
                false ->
                    log(error, "Incorrect master password. Error: ~p~n"
                               "Stopping babysitter", [ErrorMsg], State),
                    shutdown
            end;
        {error, Error} ->
            %% Note: Error is logged in call_init
            {error, Error}
    end.

call_init(HiddenPass, ReadOnly, State) ->
    case call_gosecrets({init, ReadOnly, HiddenPass}, State) of
        ok ->
            memorize_hidden_pass(HiddenPass),
            log(info, "Init complete. Password (if used) accepted.", [], State),
            ok;
        {error, "key decrypt failed:" ++ _ = Error} ->
            %% error is logged later to avoid logging error when the default
            %% password is being tried
            {wrong_password, Error};
        {error, Error} ->
            log(error, "Gosecrets initialization failed: ~s", [Error], State),
            {error, Error}
    end.

handle_call({encrypt, Data}, _From, State) ->
    {reply, call_gosecrets({encrypt, Data}, State), State};
handle_call({decrypt, Data}, _From, State) ->
    {reply, convert_empty_data(call_gosecrets({decrypt, Data}, State)), State};
handle_call({change_password, HiddenPass}, _From, State) ->
    Reply = call_gosecrets({change_password, HiddenPass}, State),
    case Reply of
        ok ->
            memorize_hidden_pass(HiddenPass),
            ok;
        {error, _} ->
            ok
    end,
    {reply, Reply, State};
handle_call(get_keys_ref, _From, State) ->
    {reply, call_gosecrets(get_keys_ref, State), State};
handle_call(get_state, _From, State) ->
    {reply, call_gosecrets(get_state, State), State};
handle_call(rotate_data_key, _From, State) ->
    {reply, call_gosecrets(rotate_data_key, State), State};
handle_call({maybe_clear_backup_key, DataKey}, _From, State) ->
    {reply, call_gosecrets({maybe_clear_backup_key, DataKey}, State), State};
handle_call(gosecrets_os_pid, _From, State) ->
    Res = case call_gosecrets({port_info, os_pid}, State) of
              {os_pid, P} -> P;
              undefined -> undefined
          end,
    {reply, Res, State};
handle_call({set_config, Cfg, ResetPassword}, _From,
            #state{config = CfgPath} = State) ->
    try save_config(CfgPath, cfg_to_json(Cfg), State) of
        ok ->
            Pass = case ResetPassword of
                       true -> ?HIDE(undefined);
                       false ->
                           LogInfo = fun (L, F, A) -> log(L, F, A, State) end,
                           extract_hidden_pass(LogInfo)
                   end,
            Res = call_gosecrets({reload_config, Pass}, State),
            case Res of
                ok -> memorize_hidden_pass(Pass);
                {error, _} -> ok
            end,
            {reply, Res, State}
    catch
        error:Error ->
            {reply, {error, format_error(Error)}, State}
    end;
handle_call({copy_secrets, Cfg}, _From, State) ->
    CfgBin = ejson:encode(cfg_to_json(Cfg)),
    {reply, call_gosecrets({copy_secrets, CfgBin}, State), State};
handle_call({cleanup_secrets, Cfg}, _From, State) ->
    CfgBin = ejson:encode(cfg_to_json(Cfg)),
    {reply, call_gosecrets({cleanup_secrets, CfgBin}, State), State};
handle_call({store_key, _Kind, _Name, _KeyType, _KeyData, _EncryptionKeyId,
             _CreationDT, _CanBeCached, _TestOnly} = Cmd, _From, State) ->
    {reply, call_gosecrets(Cmd, State), State};
handle_call({read_key, _Kind, _Name} = Cmd, _From, State) ->
    {reply, call_gosecrets(Cmd, State), State};
handle_call({encrypt_with_key, _Data, _AD, _KeyKind, _Name} = Cmd, _From,
            State) ->
    {reply, convert_empty_data(call_gosecrets(Cmd, State)), State};
handle_call({decrypt_with_key, _Data, _AD, _KeyKind, _Name} = Cmd, _From,
            State) ->
    {reply, convert_empty_data(call_gosecrets(Cmd, State)), State};
handle_call({rotate_integrity_tokens, _KeyName} = Cmd, _From, State) ->
    {reply, call_gosecrets(Cmd, State), State};
handle_call({remove_old_integrity_tokens, _Paths} = Cmd, _From, State) ->
    {reply, call_gosecrets(Cmd, State), State};
handle_call(get_key_id_in_use, _From, State) ->
    {reply, convert_empty_data(call_gosecrets(get_key_id_in_use, State)), State};
handle_call({mac, _Data} = Cmd, _From, State) ->
    {reply, call_gosecrets(Cmd, State), State};
handle_call({verify_mac, _Mac, _Data} = Cmd, _From, State) ->
    {reply, call_gosecrets(Cmd, State), State};
handle_call(revalidate_key_cache, _From, State) ->
    {reply, call_gosecrets(revalidate_key_cache, State), State};
handle_call(stop, _From, State) ->
    {stop, normal, call_gosecrets(stop, State), State};
handle_call(Call, _From, State) ->
    log(warn, "Unhandled call: ~p", [Call], State),
    {reply, {error, not_allowed}, State}.

handle_cast(Msg, State) ->
    log(warn, "Unhandled cast: ~p", [Msg], State),
    {noreply, State}.

handle_info(Info, State) ->
    log(warn, "Unhandled info: ~p", [Info], State),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_gosecrets(CfgPath, State) ->
    Parent = self(),
    LogDebug = fun (F, A) -> log(debug, F, A, State) end,
    {ok, Pid} =
        proc_lib:start_link(
          erlang, apply,
          [fun () ->
                   process_flag(trap_exit, true),
                   Path = path_config:get_path(bin, "gosecrets"),
                   Args = ["--config", CfgPath],
                   LogDebug("Starting ~p with args: ~0p", [Path, Args]),
                   Port =
                       open_port(
                         {spawn_executable, Path},
                         [{packet, 4}, binary, hide, {args, Args}]),
                   proc_lib:init_ack({ok, self()}),
                   gosecrets_loop(Port, Parent, LogDebug)
           end, []]),
    log(debug, "Gosecrets loop started with pid = ~p", [Pid], State),
    Pid.

call_gosecrets(Msg, #state{loop = Pid}) ->
    Pid ! {call, Msg},
    receive
        {reply, Resp} ->
            Resp
    end.

gosecrets_loop(Port, Parent, LogDebug) ->
    receive
        {call, {port_info, I}} ->
            Parent ! {reply, erlang:port_info(Port, I)},
            gosecrets_loop(Port, Parent, LogDebug);
        {call, stop} ->
            true = port_close(Port),
            Parent ! {reply, ok},
            exit(normal);
        {call, Msg} ->
            Port ! {self(), {command, encode(Msg)}},
            wait_call_resp(Port, Parent, LogDebug),
            gosecrets_loop(Port, Parent, LogDebug);
        {Port, {data, <<"L", Data/binary>>}} ->
            handle_gosecrets_log(Data, LogDebug),
            gosecrets_loop(Port, Parent, LogDebug);
        Exit = {'EXIT', _, _} ->
            gosecret_process_exit(Port, Exit, LogDebug)
    end.

wait_call_resp(Port, Parent, LogDebug) ->
    receive
        Exit = {'EXIT', _, _} ->
            gosecret_process_exit(Port, Exit, LogDebug);
        {Port, {data, <<"L", Data/binary>>}} ->
            handle_gosecrets_log(Data, LogDebug),
            wait_call_resp(Port, Parent, LogDebug);
        {Port, {data, <<"S">>}} ->
            Parent ! {reply, ok};
        {Port, {data, <<"S", Data/binary>>}} ->
            Parent ! {reply, {ok, Data}};
        {Port, {data, <<"E", Data/binary>>}} ->
            Parent ! {reply, {error, binary_to_list(Data)}}
    end.

handle_gosecrets_log(Data, LogDebug) ->
    LogDebug("gosecrets: ~s", [Data]).

gosecret_process_exit(Port, Exit, LogDebug) ->
    LogDebug("Received exit ~p for port ~p", [Exit, Port]),
    gosecret_do_process_exit(Port, Exit).

gosecret_do_process_exit(Port, {'EXIT', Port, Reason}) ->
    exit({port_terminated, Reason});
gosecret_do_process_exit(_Port, {'EXIT', _, Reason}) ->
    exit(Reason).

encode({init_read_only, HiddenPass}) ->
    BinaryPassword = encode_password(HiddenPass),
    <<1, BinaryPassword/binary>>;
encode(get_keys_ref) ->
    <<2>>;
encode({encrypt, Data}) ->
    <<3, Data/binary>>;
encode({decrypt, Data}) ->
    <<4, Data/binary>>;
encode({change_password, HiddenPass}) ->
    BinaryPassword = encode_password(HiddenPass),
    <<5, BinaryPassword/binary>>;
encode(rotate_data_key) ->
    <<6>>;
encode({maybe_clear_backup_key, DataKey}) ->
    <<7, DataKey/binary>>;
encode(get_state) ->
    <<8>>;
encode({reload_config, HiddenPass}) ->
    BinaryPassword = encode_password(HiddenPass),
    <<9, BinaryPassword/binary>>;
encode({copy_secrets, ConfigBin}) ->
    <<10, ConfigBin/binary>>;
encode({cleanup_secrets, ConfigBin}) ->
    <<11, ConfigBin/binary>>;
encode({store_key, Kind, Name, KeyType, KeyData, EncryptionKeyId,
        CreationDT, CanBeCached, TestOnly}) ->
    KindBin = atom_to_binary(Kind),
    <<12, (encode_param(KindBin))/binary,
          (encode_param(Name))/binary,
          (encode_param(KeyType))/binary,
          (encode_param(KeyData))/binary,
          (encode_param(EncryptionKeyId))/binary,
          (encode_param(CreationDT))/binary,
          (encode_param(TestOnly))/binary,
          (encode_param(CanBeCached))/binary>>;
encode({encrypt_with_key, Data, AD, KeyKind, Name}) ->
    <<13, (encode_param(Data))/binary,
          (encode_param(AD))/binary,
          (encode_param(KeyKind))/binary,
          (encode_param(Name))/binary>>;
encode({decrypt_with_key, Data, AD, KeyKind, Name}) ->
    <<14, (encode_param(Data))/binary,
          (encode_param(AD))/binary,
          (encode_param(KeyKind))/binary,
          (encode_param(Name))/binary>>;
encode({read_key, Kind, Name}) ->
    <<15, (encode_param(Kind))/binary, (encode_param(Name))/binary>>;
encode({rotate_integrity_tokens, KeyName}) ->
    <<17, (encode_param(KeyName))/binary>>;
encode({remove_old_integrity_tokens, Paths}) ->
    PathsBin = list_to_binary([encode_param(P) || P <- Paths]),
    <<18, PathsBin/binary>>;
encode(get_key_id_in_use) ->
    <<19>>;
encode({init, IsReadOnly, HiddenPass}) ->
    BinaryPassword = encode_password(HiddenPass),
    ReadOnlyBin = case IsReadOnly of
                      true -> <<1>>;
                      false -> <<0>>
                  end,
    <<20, (encode_param(ReadOnlyBin))/binary,
          (encode_param(BinaryPassword))/binary>>;
encode({mac, Data}) ->
    <<21, Data/binary>>;
encode({verify_mac, Mac, Data}) ->
    <<22, (encode_param(Mac))/binary, (encode_param(Data))/binary>>;
encode(revalidate_key_cache) ->
    <<23>>.

encode_param(B) when is_atom(B) ->
    encode_param(atom_to_binary(B));
encode_param(B) when is_binary(B) ->
    S = size(B),
    <<S:32/big-unsigned-integer, B/binary>>.

encode_password(HiddenPass) ->
    case ?UNHIDE(HiddenPass) of
        undefined -> <<0>>;
        P when is_list(P) -> <<1, (list_to_binary(P))/binary>>
    end.

save_port_file(Socket) ->
    {ok, {Addr, Port}} = inet:sockname(Socket),
    AFBin = case size(Addr) of
                4 -> <<"inet">>;
                8 -> <<"inet6">>
            end,
    PortBin = integer_to_binary(Port),
    misc:atomic_write_file(port_file_path(),
                           <<AFBin/binary, " ", PortBin/binary>>).

open_udp_socket(State) ->
    case open_udp_socket_for_AF(inet) of
        {ok, S} ->
            {ok, S};
        {error, Reason1} ->
            log(warn, "Failed to open TCPv4 UDP port: ~p", [Reason1], State),
            case open_udp_socket_for_AF(inet6) of
                {ok, S} ->
                    {ok, S};
                {error, Reason2} ->
                    log(error, "Failed to open TCPv6 UDP port: ~p", [Reason2],
                        State),
                    {error, {Reason1, Reason2}}
            end
    end.

open_udp_socket_for_AF(AFamily) ->
    gen_udp:open(0, [AFamily, {ip, loopback}, {active, true}]).

default_cfg() -> cfg_to_json([]).

memorize_hidden_pass(HiddenPass) ->
    application:set_env(ns_babysitter, master_password, HiddenPass).

extract_hidden_pass() ->
    extract_hidden_pass(fun (L, F, A) -> ?ALE_LOG(L, F, A) end).

extract_hidden_pass(Log) ->
    case application:get_env(ns_babysitter, master_password) of
        {ok, P} ->
            Log(debug, "Using password from application environment", []),
            P;
        _ ->
            ?HIDE(undefined)
    end.

%% [{es_key_storage_type, file},
%%  {es_key_path_type, custom},
%%  {es_encrypt_key, true},
%%  {es_custom_key_path, <<"/path">>},
%%  {es_password_source, env},
%%  {es_password_env, <<"ENV_VAR">>}]
%%
%% [{es_key_storage_type, script},
%%  {es_read_cmd, <<"/path">>},
%%  {es_write_cmd, <<"/path">>},
%%  {es_delete_cmd, <<"/path">>}]

cfg_to_json(Props) ->
    Extract = fun (K) ->
                  D = proplists:get_value(K, defaults(), <<>>),
                  proplists:get_value(K, Props, D)
              end,
    ExtractBin = fun (K) -> iolist_to_binary(Extract(K)) end,

    KeyCfg = fun (Kind) ->
                 {[{kind, Kind},
                   {path, key_path(Kind, Props)},
                   {encryptByKind, kek}]}
             end,
    Kinds = [kek, configDek, logDek, auditDek],
    DeksStoreConfig = case key_path(bucketDek, Props) of
                          undefined -> [];
                          DeksPath -> [{[{kind, bucketDek},
                                         {path, DeksPath},
                                         {encryptByKind, kek}]}]
                      end,
    StoredKeysJson = {storedKeys, [KeyCfg(K) || K <- Kinds] ++ DeksStoreConfig},

    case Extract(es_key_storage_type) of
        file ->
            Encr = Extract(es_encrypt_key),
            PSource = Extract(es_password_source),
            Path =
                case Extract(es_key_path_type) of
                    auto when Encr ->
                        iolist_to_binary(default_data_key_path(PSource));
                    auto ->
                        iolist_to_binary(default_unencrypted_data_key_path());
                    custom ->
                        ExtractBin(es_custom_key_path)
                end,

            PasswordCfg = case Encr of
                              true ->
                                  PS = case PSource of
                                           env ->
                                               EN = ExtractBin(es_password_env),
                                               {[{envName, EN}]};
                                           script ->
                                               C = ExtractBin(es_password_cmd),
                                               {[{passwordCmd, C} || C /= <<>>]}
                                       end,
                                  [{passwordSource, PSource},
                                   {passwordSettings, PS}];
                              false ->
                                  []
                          end,

            {[{encryptionService,
               {[{keyStorageType, file},
                 {keyStorageSettings,
                  {[{path, Path},
                    {encryptWithPassword, Encr}] ++ PasswordCfg}}]}},
              StoredKeysJson]};
        script ->
            R = ExtractBin(es_read_cmd),
            W = ExtractBin(es_write_cmd),
            D = ExtractBin(es_delete_cmd),
            {[{encryptionService,
               {[{keyStorageType, script},
                 {keyStorageSettings,
                  {[{readCmd, R} || R /= <<>>] ++
                   [{writeCmd, W} || W /= <<>>] ++
                   [{deleteCmd, D} || D /= <<>>]}}]}},
              StoredKeysJson]}
    end.

%% Some keys are static, and can't be changed via config (main reason for that
%% is the fact that we need those path before ns_config has started)
key_path(kek, _Cfg) ->
    proplists:get_value(kek_path, defaults(), undefined);
key_path(configDek, _Cfg) ->
    proplists:get_value(config_dek_path, defaults(), undefined);
key_path(logDek, _Cfg) ->
    proplists:get_value(log_dek_path, defaults(), undefined);
key_path(auditDek, _Cfg) ->
    proplists:get_value(audit_dek_path, defaults(), undefined);
key_path(bucketDek, Cfg) ->
    Key = bucket_dek_path,
    case proplists:get_value(Key, Cfg) of
        undefined -> proplists:get_value(Key, defaults(), undefined);
        P -> iolist_to_binary(P)
    end.

defaults() ->
    ConfigDir = path_config:component_path(data, "config"),
    [{es_password_env, "CB_MASTER_PASSWORD"},
     {es_password_source, env},
     {es_encrypt_key, true},
     {es_key_path_type, auto},
     {es_key_storage_type, 'file'},
     {kek_path, iolist_to_binary(filename:join(ConfigDir, "keks"))},
     {config_dek_path, iolist_to_binary(filename:join(ConfigDir, "deks"))},
     {audit_dek_path, iolist_to_binary(filename:join(ConfigDir, "audit_deks"))},
     {log_dek_path, iolist_to_binary(filename:join(ConfigDir, "logs_deks"))}].

format_error({write_failed, CfgPath, Error}) ->
    io_lib:format("Could not write file '~s': ~s (~p)",
                  [CfgPath, file:format_error(Error), Error]);
format_error(Unknown) ->
    io_lib:format("~p", [Unknown]).

should_prompt_the_password(#state{config = Path}) ->
    {ok, CfgJson} = file:read_file(Path),
    {Cfg} = ejson:decode(CfgJson),
    {ESCfg} = proplists:get_value(<<"encryptionService">>, Cfg),
    case proplists:get_value(<<"keyStorageType">>, ESCfg) of
        <<"file">> ->
            {KSCfg} = proplists:get_value(<<"keyStorageSettings">>, ESCfg),
            case proplists:get_value(<<"encryptWithPassword">>, KSCfg) of
                true ->
                    case proplists:get_value(<<"passwordSource">>, KSCfg) of
                        <<"env">> -> true;
                        _ -> false
                    end;
                false ->
                    false
            end;
        _ ->
            false
    end.

convert_empty_data(ok) -> {ok, <<>>};
convert_empty_data(Res) -> Res.

log(LogLevel, F, A, #state{logger = Fun}) when is_function(Fun) ->
    Fun(LogLevel, F, A);
log(LogLevel, F, A, #state{logger = default}) ->
    ?ALE_LOG(LogLevel, F, A).

-ifdef(TEST).

should_prompt_the_password_test() ->
    CfgPath = path_config:tempfile("promt_pass_test_cfg", ".tmp"),
    try
        State = #state{config = CfgPath},
        save_config(CfgPath, cfg_to_json([]), State),
        ?assertEqual(true, should_prompt_the_password(State)),
        save_config(CfgPath, cfg_to_json([{es_key_storage_type, script},
                                          {es_read_cmd, <<"/path">>},
                                          {es_write_cmd, <<"/path">>},
                                          {es_delete_cmd, <<"/path">>}]),
                    State),
        ?assertEqual(false, should_prompt_the_password(State)),
        save_config(CfgPath, cfg_to_json([{es_key_storage_type, file},
                                          {es_encrypt_key, true},
                                          {es_password_source, script},
                                          {es_password_cmd, <<"/path">>}]),
                    State),
        ?assertEqual(false, should_prompt_the_password(State))
    after
        file:delete(CfgPath)
    end.

default_config_encryption_test() ->
    with_gosecrets(
      undefined,
      fun (_CfgPath, Pid) ->
          Data = rand:bytes(512),
          {ok, Encrypted1} = encrypt(Pid, Data),
          {ok, Encrypted2} = encrypt(Pid, Data),
          ?assert(Encrypted1 /= Encrypted2),
          {ok, Data} = decrypt(Pid, Encrypted1),
          {ok, Data} = decrypt(Pid, Encrypted2)
      end).

-define(GET_PASS_SCRIPT, "#!/bin/bash\n\necho -n \"~s\"\n").

change_password_with_password_cmd_test() ->
    Data = rand:bytes(512),
    Password1 = base64:encode_to_string(rand:bytes(8)),
    Password2 = base64:encode_to_string(rand:bytes(8)),
    PassCmd = path_config:tempfile("pass_cmd", ".tmp"),
    ok = file:write_file(PassCmd, io_lib:format(?GET_PASS_SCRIPT, [Password1])),
    {ok, #file_info{mode = Mode}} = file:read_file_info(PassCmd, [raw]),
    ok = file:change_mode(PassCmd, Mode bor 8#00110),
    DKFile = path_config:tempfile("encrypted_datakey", ".tmp"),
    Cfg = [{es_key_storage_type, file},
           {es_encrypt_key, true},
           {es_password_source, script},
           {es_password_cmd, PassCmd},
           {es_key_path_type, custom},
           {es_custom_key_path, DKFile}],

    try
        {EncryptedData1, EncryptedData2} =
            with_gosecrets(
              Cfg,
              fun (_CfgPath, Pid) ->
                  {ok, Encrypted1} = encrypt(Pid, Data),

                  %% Changing password to Password2:
                  %%    1) make sure the command returns new pass
                  %%    2) trigger update in gosecret
                  ok = file:write_file(PassCmd,
                                       io_lib:format(?GET_PASS_SCRIPT,
                                                     [Password2])),
                  ok = change_password(Pid, undefined),

                  %% making sure encryption decryption works
                  {ok, Data} = decrypt(Pid, Encrypted1),
                  {ok, Encrypted2} = encrypt(Pid, Data),
                  {Encrypted1, Encrypted2}
              end),

        %% After gosecrets restart, it should read Password2 via cmd, there
        %% is no need to send password to it
        with_gosecrets(
          Cfg,
          fun (_CfgPath, Pid) ->
              {ok, Data} = decrypt(Pid, EncryptedData1),
              {ok, Data} = decrypt(Pid, EncryptedData2)
          end)
    after
        file:delete(PassCmd),
        file:delete(DKFile)
    end.

datakey_rotation_test() ->
    with_gosecrets(
      undefined,
      fun (_CfgPath, Pid) ->
          Data = rand:bytes(512),
          Password = binary_to_list(rand:bytes(128)),
          {ok, Encrypted1} = encrypt(Pid, Data),
          ok = change_password(Pid, Password),
          {ok, Data} = decrypt(Pid, Encrypted1),
          {ok, Encrypted2} = encrypt(Pid, Data),
          ok = rotate_data_key(Pid),
          {ok, KeysRef} = get_keys_ref(Pid),
          {ok, Data} = decrypt(Pid, Encrypted1),
          {ok, Data} = decrypt(Pid, Encrypted2),
          {ok, Encrypted3} = encrypt(Pid, Data),
          {error, _} = rotate_data_key(Pid),
          maybe_clear_backup_key(Pid, KeysRef),
          {error, _} = decrypt(Pid, Encrypted1),
          {error, _} = decrypt(Pid, Encrypted2),
          {ok, Data} = decrypt(Pid, Encrypted3),
          ok = rotate_data_key(Pid)
      end).

config_reload_test() ->
    Cfg1 = [],
    Cfg2 = [{es_key_storage_type, file},
            {es_encrypt_key, false},
            {es_key_path_type, custom},
            {es_custom_key_path, default_data_key_path(env)}],
    Cfg3 = [{es_key_storage_type, file},
            {es_encrypt_key, false}],
    with_gosecrets(
      Cfg1,
      fun (CfgPath, Pid) ->
          Data = rand:bytes(512),
          Password = binary_to_list(rand:bytes(128)),
          ok = change_password(Pid, Password),
          ?assertEqual({ok, <<"user_configured">>}, get_state(Pid)),
          {ok, Encrypted} = encrypt(Pid, Data),

          %% Returns error because it tries to use the same file as prev config
          {error, _} = copy_secrets(Pid, Cfg2),
          {ok, <<"copied">>} = copy_secrets(Pid, Cfg3),
          ok = set_config(Pid, Cfg3, true),
          {error, _} = cleanup_secrets(Pid, Cfg3),
          ok = cleanup_secrets(Pid, Cfg1),
          ?assertEqual({ok, <<"password_not_used">>}, get_state(Pid)),
          ?assertEqual({ok, Data}, decrypt(Pid, Encrypted)),
          {ok, CurCfgBin} = file:read_file(CfgPath),
          ?assertEqual(ejson:encode(cfg_to_json(Cfg3)),
                       ejson:encode(ejson:decode(CurCfgBin))),
          ?assert(not filelib:is_file(default_data_key_path(env))),
          ?assert(filelib:is_file(default_unencrypted_data_key_path())),

          {ok, <<"copied">>} = copy_secrets(Pid, Cfg1),
          ok = set_config(Pid, Cfg1, true),
          {ok, <<"same">>} = copy_secrets(Pid, Cfg1),
          ok = cleanup_secrets(Pid, Cfg3),
          ?assertEqual({ok, Data}, decrypt(Pid, Encrypted)),
          ?assertEqual({ok, <<"default">>}, get_state(Pid))
      end).

-define(key1, <<"key10000-0000-0000-0000-000000000000">>).
-define(key2, <<"key20000-0000-0000-0000-000000000000">>).
-define(key3, <<"key30000-0000-0000-0000-000000000000">>).
-define(key4, <<"key40000-0000-0000-0000-000000000000">>).
-define(key5, <<"key50000-0000-0000-0000-000000000000">>).
-define(key6, <<"key60000-0000-0000-0000-000000000000">>).
-define(key7, <<"key70000-0000-0000-0000-000000000000">>).

integrity_check_for_stored_keys_test() ->
    DKFile = path_config:tempfile("bucket_dek_test", ".tmp"),
    ok = filelib:ensure_dir(DKFile),
    Cfg = [{bucket_dek_path, iolist_to_binary(DKFile)}],
    BucketPath = fun (Bucket) ->
                     iolist_to_binary(filename:join([DKFile, Bucket, "deks"]))
                 end,
    KeyDirs = [key_path(configDek, Cfg),
               key_path(logDek, Cfg),
               key_path(auditDek, Cfg),
               key_path(kek, Cfg),
               BucketPath("bucket1"),
               BucketPath("bucket2")],
    with_all_stored_keys_cleaned(
      Cfg,
      fun () ->
          with_gosecrets(
            Cfg,
            fun (CfgPath, Pid) ->
                TokensPath = filename:join(filename:dirname(CfgPath),
                                           "stored_keys_tokens"),
                StoreKey = fun (Kind, KeyId, EncryptWithKey) ->
                               store_key(Pid, Kind, KeyId, 'raw-aes-gcm',
                                         rand:bytes(32),
                                         EncryptWithKey,
                                         <<"2024-07-26T19:32:19Z">>,
                                         false, false)
                           end,

                ok = StoreKey(configDek, ?key1, <<"encryptionService">>),

                {ok, [undefined]} = cb_crypto:get_file_dek_ids(TokensPath),

                %% First rotation with key1
                ok = rotate_integrity_tokens(Pid, ?key1),
                {ok, [?key1]} = cb_crypto:get_file_dek_ids(TokensPath),
                ok = remove_old_integrity_tokens(Pid, KeyDirs),
                {ok, [?key1]} = cb_crypto:get_file_dek_ids(TokensPath),

                %% Store more keys of different kinds
                ok = StoreKey(kek, ?key2, <<"encryptionService">>),
                ok = StoreKey(configDek, ?key3, ?key2),
                ok = StoreKey(logDek, ?key4, ?key2),
                ok = StoreKey(auditDek, ?key5, ?key2),
                Bucket1Key = <<"bucket1/deks/", ?key6/binary>>,
                ok = StoreKey(bucketDek, Bucket1Key, ?key2),
                Bucket2Key = <<"bucket2/deks/", ?key7/binary>>,
                ok = StoreKey(bucketDek, Bucket2Key, ?key2),

                %% Second rotation with key1
                ok = rotate_integrity_tokens(Pid, ?key3),
                {ok, [?key3]} = cb_crypto:get_file_dek_ids(TokensPath),

                ok = remove_old_integrity_tokens(Pid, KeyDirs),
                {ok, [?key3]} = cb_crypto:get_file_dek_ids(TokensPath),

                %% Final rotation with empty key
                ok = rotate_integrity_tokens(Pid, <<>>),
                {ok, [undefined]} = cb_crypto:get_file_dek_ids(TokensPath),
                ok = remove_old_integrity_tokens(Pid, KeyDirs),
                {ok, [undefined]} = cb_crypto:get_file_dek_ids(TokensPath),

                %% Verify all keys are still readable
                {ok, _} = read_key(Pid, configDek, ?key1),
                {ok, _} = read_key(Pid, kek, ?key2),
                {ok, _} = read_key(Pid, configDek, ?key3),
                {ok, _} = read_key(Pid, logDek, ?key4),
                {ok, _} = read_key(Pid, auditDek, ?key5),
                {ok, _} = read_key(Pid, bucketDek, Bucket1Key),
                {ok, _} = read_key(Pid, bucketDek, Bucket2Key)
            end)
      end).

store_and_read_key_test() ->
    Cfg = [],
    DecodeKey = fun (EncodedData) ->
                    {Props} = ejson:decode(EncodedData),
                    Type = proplists:get_value(<<"type">>, Props, <<>>),
                    {KeyProps} = proplists:get_value(<<"info">>, Props),
                    KeyBase64 = proplists:get_value(<<"key">>, KeyProps),
                    Bin = base64:decode(KeyBase64),
                    EncrKey = proplists:get_value(<<"encryptionKeyId">>,
                                                  KeyProps),
                    CT = proplists:get_value(<<"creationTime">>, KeyProps),
                    #{type => binary_to_atom(Type),
                      key => Bin,
                      encr_key => EncrKey,
                      creation_time => CT}
                end,
    with_all_stored_keys_cleaned(
      Cfg,
      fun () ->
          with_gosecrets(
            Cfg,
            fun (_CfgPath, Pid) ->
                Key1 = rand:bytes(32),
                Key2 = rand:bytes(32),
                Type = 'raw-aes-gcm',
                ?assertEqual(ok, store_key(Pid, kek, <<"key1">>, Type, Key1,
                                           <<"encryptionService">>,
                                           <<"2024-07-26T19:32:19Z">>,
                                           false, false)),
                ?assertEqual(ok, store_key(Pid, configDek, <<"key2">>, Type,
                                           Key2, <<"key1">>,
                                           <<"2024-07-26T19:32:19Z">>,
                                           false, false)),
                {ok, Key1Encoded} = read_key(Pid, kek, <<"key1">>),
                {ok, Key2Encoded} = read_key(Pid, configDek, <<"key2">>),
                ?assertMatch(#{type := Type,
                               key := Key1,
                               encr_key := <<"encryptionService">>,
                               creation_time := <<"2024-07-26T19:32:19Z">>},
                             DecodeKey(Key1Encoded)),
                ?assertMatch(#{type := Type,
                               key := Key2,
                               encr_key := <<"key1">>,
                               creation_time := <<"2024-07-26T19:32:19Z">>},
                             DecodeKey(Key2Encoded)),
                %% Unknown key
                ?assertMatch({error, "failed to read key key3: no files found"
                                     ++ _},
                             read_key(Pid, configDek, <<"key3">>))
            end)
      end).

unchanged_config_reload_after_password_change_test() ->
    Cfg1 = [],
    with_gosecrets(
      Cfg1,
      fun (CfgPath, Pid) ->
          Data = rand:bytes(512),
          Password = binary_to_list(rand:bytes(128)),
          ?assertEqual({ok, <<"default">>}, get_state(Pid)),
          ok = change_password(Pid, Password),
          ?assertEqual({ok, <<"user_configured">>}, get_state(Pid)),
          {ok, Encrypted} = encrypt(Pid, Data),

          {ok, <<"same">>} = copy_secrets(Pid, Cfg1),
          ok = set_config(Pid, Cfg1, false),
          %% checking that we haven't reset the password
          ?assertEqual({ok, <<"user_configured">>}, get_state(Pid)),
          ?assertEqual({ok, Data}, decrypt(Pid, Encrypted)),
          {ok, CurCfgBin} = file:read_file(CfgPath),
          ?assertEqual(ejson:encode(cfg_to_json(Cfg1)),
                       ejson:encode(ejson:decode(CurCfgBin)))
      end).

env_password_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          Password = base64:encode_to_string(rand:bytes(128)),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    ok = change_password(Pid, Password),
                    encrypt(Pid, Data)
                end),

          os:putenv("CB_MASTER_PASSWORD", Password),
          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    ok = change_password(Pid, ""),
                    decrypt(Pid, Encrypted)
                end),

          os:unsetenv("CB_MASTER_PASSWORD"),
          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    decrypt(Pid, Encrypted)
                end)
      end).

default_env_password_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          Password = base64:encode_to_string(rand:bytes(128)),
          os:putenv("CB_MASTER_PASSWORD", Password),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    %% as it is the first start it should set the password
                    %% that was specified by the env var
                    encrypt(Pid, Data)
                end),

          %% Now we restart, change the env var, and check that empty password
          %% doesn't work, while the original password from the env var works
          Password2 = base64:encode_to_string(rand:bytes(128)),
          os:putenv("CB_MASTER_PASSWORD", Password2),
          with_password_sent(
            _WrongPassword = "", Password,
            fun () ->
                {ok, Data} =
                    with_gosecrets(
                      Cfg,
                      fun (_CfgPath, Pid) ->
                          %% this should reset the password to whatever is
                          %% specified in the env var
                          change_password(Pid, undefined),
                          decrypt(Pid, Encrypted)
                      end)
            end),

          os:unsetenv("CB_MASTER_PASSWORD"),
          with_password_sent(
            Password, Password2,
            fun () ->
                {ok, Data} =
                    with_gosecrets(
                      Cfg,
                      fun (_CfgPath, Pid) ->
                          %% this should reset the password to "" because
                          %% the env var is not set
                          change_password(Pid, undefined),
                          decrypt(Pid, Encrypted)
                      end)
            end),

          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    decrypt(Pid, Encrypted)
                end)
      end).

udp_password_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          Password = base64:encode_to_string(rand:bytes(128)),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    ok = change_password(Pid, Password),
                    encrypt(Pid, Data)
                end),

          with_password_sent(
            "wrong", Password,
            fun () ->
                {ok, Data} =
                    with_gosecrets(
                      Cfg,
                      fun (_CfgPath, Pid) ->
                          decrypt(Pid, Encrypted)
                      end)
            end)
      end).

upgrade_from_7_2_no_password_test() ->
    %% This file name and file content are copied from test 7.2 node.
    %% They should not be changed to whatever this version is using
    %% particularly, we can't use default_data_key_path() here.
    DKeyPath = filename:join(path_config:component_path(data, "config"),
                             "encrypted_data_keys"),
    DKeyNoPass = <<"PQCOjPj3Z5C8gF22/lU6RWUOj3oaArY2SG47ZrknOU"
                   "CYeAumjlE0FWbz9ll3/Qh1XARJUIrIhfjBKDIKf6MA">>,

    %% We can't use with_gosecrets/2 here, because it would remove the data
    %% key file
    memorize_hidden_pass(?HIDE(undefined)),
    with_tmp_cfg(
      undefined,
      fun (CfgPath) ->
          ok = file:write_file(DKeyPath, base64:decode(DKeyNoPass)),
          {ok, Pid} = start_link(default, true, true, CfgPath),
          try
              Data = rand:bytes(512),
              {ok, Encrypted} = encrypt(Pid, Data),
              {ok, Data} = decrypt(Pid, Encrypted)
          after
              unlink(Pid),
              exit(Pid, shutdown)
          end
      end).

upgrade_from_7_2_with_password_test() ->
    %% This file name and file content are copied from test 7.2 node.
    %% They should not be changed to whatever this version is using
    %% particularly, we can't use default_data_key_path() here.
    DKeyPath = filename:join(path_config:component_path(data, "config"),
                             "encrypted_data_keys"),
    DKeyNoPass = <<"PQClXd7LPk4UgKcXuAKjg3+q9/dzCoZ3CZLNpmKtnn"
                   "oJblYKVGRkQzY6w/r7yDjJNV7BF+Ng9RXPT8nKKrMA">>,

    with_password_sent(
      "wrong", "test",
      fun () ->
          %% We can't use with_gosecrets/2 here, because it would remove
          %% the data key file
          with_tmp_cfg(
            undefined,
            fun (CfgPath) ->
                ok = file:write_file(DKeyPath, base64:decode(DKeyNoPass)),
                {ok, Pid} = start_link(default, true, true, CfgPath),
                try
                    Data = rand:bytes(512),
                    {ok, Encrypted} = encrypt(Pid, Data),
                    {ok, Data} = decrypt(Pid, Encrypted)
                after
                    unlink(Pid),
                    exit(Pid, shutdown)
                end
            end)
      end).

invalid_datakey_test() ->
    DKeyPath7_6 = filename:join(path_config:component_path(data, "config"),
                                "encrypted_data_keys"),
    DKeyPathActual = default_data_key_path(env),
    Cfg7_6 = {[{encryptionService,
                {[{keyStorageType, file},
                  {keyStorageSettings,
                   {[{path, iolist_to_binary(DKeyPath7_6)},
                     {encryptWithPassword, true},
                     {passwordSource, env},
                     {passwordSettings,
                      {[{envName, <<"CB_MASTER_PASSWORD">>}]}}]}}]}}]},

    CfgActual = cfg_to_json([]),

%%    Example of correct data for empty password
%%    {"version": 0,
%%     "data": "AN4tJ93IHmbBK7jQhjujZBH3AhgKeCOIHZpBhjjkbB3ETrt"
%%             "npb/ilq96iEHambXrwkf0NWKTwoH+TosGDkNy",
%%     "encrypted": true,
%%     "lockkeySalt": "v76gOte7tV8="}

    BadData = <<"{\"data\": \"bad\", "  %% Bad base64
                 "\"version\": 0, "
                 "\"encrypted\": true, "
                 "\"lockkeySalt\": \"v76gOte7tV8=\"}">>,
    BadSalt = <<"{\"data\": \"AN4tJ93IHmbBK7jQhjujZBH3AhgKeCOIHZpBhjjkbB3ETrt"
                             "npb/ilq96iEHambXrwkf0NWKTwoH+TosGDkNy\", "
                 "\"version\": 0, "
                 "\"encrypted\": true,"
                 "\"lockkeySalt\": \"bad\"}">>,
    BadEncr = <<"{\"data\": \"AN4tJ93IHmbBK7jQhjujZBH3AhgKeCOIHZpBhjjkbB3ETrt"
                             "npb/ilq96iEHambXrwkf0NWKTwoH+TosGDkNy\", "
                 "\"version\": 0, "
                 "\"encrypted\": false, "
                 "\"lockkeySalt\": \"v76gOte7tV8=\"}">>,
    BadJson = <<"{\"data\": \"AN4tJ93IHmbBK7jQhjujZBH3AhgKeCOIHZpBhjjkbB3ETrt"
                             "npb/ilq96iEHambXrwkf0NWKTwoH+TosGDkNy\", "
                 "\"version\": 0, "
                 "\"encrypted\": true, "
                 "\"lockkeySalt\": \"v76gOte7tV8=\"">>,


    invalid_datakey_test_base(Cfg7_6, DKeyPath7_6, <<255, 1, 3>>,
                              "invalid datakeys: data too short"),
    invalid_datakey_test_base(Cfg7_6, DKeyPath7_6, <<2, 1, 3, 255, 1,2,3>>,
                              "invalid datakeys: data too short"),
    invalid_datakey_test_base(Cfg7_6, DKeyPath7_6, <<2, 1, 3>>,
                              "invalid datakeys: empty data"),
    invalid_datakey_test_base(Cfg7_6, DKeyPath7_6, <<>>,
                              "invalid datakeys: empty data"),

    %% Note that in many cases below it returns "data too short", because
    %% it falls back to the old format, and the data turns out to be too short
    invalid_datakey_test_base(CfgActual, DKeyPathActual, BadData,
                              "invalid datakeys: data too short"),
    invalid_datakey_test_base(CfgActual, DKeyPathActual, BadSalt,
                              "invalid datakeys: data too short"),
    invalid_datakey_test_base(CfgActual, DKeyPathActual, BadEncr,
                              "data keys are expected to be encrypted"),
    invalid_datakey_test_base(CfgActual, DKeyPathActual, BadJson,
                              "invalid datakeys: data too short").

invalid_datakey_test_base(Cfg, DKeyPath, DKeyData, ExpectedError) ->
    %% We can't use with_gosecrets/2 here, because it would remove
    %% the data key file
    with_tmp_cfg(
      undefined,
      fun (CfgPath) ->
          ok = file:write_file(CfgPath, ejson:encode(Cfg)),
          ok = file:write_file(DKeyPath, DKeyData),
          Res = start_link(default, true, true, CfgPath),
          case Res of
              {ok, P} ->
                  unlink(P),
                  exit(P, shutdown);
              _ ->
                  ok
          end,
          ?assertEqual({error, ExpectedError}, Res)
      end).

upgrade_from_7_6_test_() ->
    %% This file name and file content are copied from test 7.6 node.
    %% We can't use default_data_key_path() here because path should be 7.6 path
    DKeyWPass = <<"PQClXd7LPk4UgKcXuAKjg3+q9/dzCoZ3CZLNpmKtnn"
                   "oJblYKVGRkQzY6w/r7yDjJNV7BF+Ng9RXPT8nKKrMA">>,
    DKeyNoPass = <<"PQDBdqFxpLMpdTd9go+BxsjwlXIY+MQiOuvz7TE2mS"
                   "3jsGF5FjkS3rGaHEiG9lznJ43uiLNKghb8cBj4IG8A">>,
    DKeyPath = filename:join(path_config:component_path(data, "config"),
                             "encrypted_data_keys"),

    GetSalt =
        fun () ->
            {ok, NewDKeyData} = file:read_file(DKeyPath),
            {NewDKeyProps} = ejson:decode(NewDKeyData),
            NewKeysData = proplists:get_value(<<"data">>, NewDKeyProps),
            ?assert(size(NewKeysData) > 0),
            IsEncrypted = proplists:get_value(<<"encrypted">>, NewDKeyProps),
            ?assert(IsEncrypted),
            Salt = proplists:get_value(<<"lockkeySalt">>, NewDKeyProps),
            ?assert(size(Salt) > 0),
            base64:decode(Salt)
        end,
    DefaultSalt = <<20, 183, 239, 38, 44, 214, 22, 141>>,
    %% When we change password, the lock key salt should change
    ChangePassVerify = fun (Pid, Pass) ->
                           ok = change_password(Pid, Pass),
                           ?assert(GetSalt() =/= DefaultSalt)
                       end,
    %% When data key is rotated, the lock key salt should not change
    %% The format of the datakey file should change to the new format
    RotateDataKeyVerify = fun (Pid) ->
                              ok = rotate_data_key(Pid),
                              ?assert(GetSalt() == DefaultSalt)
                          end,
    ErrReadOnly = {error, "read-only mode is enabled"},

    [?_test(upgrade_from_7_6_test_base(DKeyPath, DKey, Pass, false, VerifyFun))
     || {DKey, Pass} <- [{DKeyWPass, "test"}, {DKeyNoPass, undefined}],
        VerifyFun <- [fun (P) -> ChangePassVerify(P, "test2") end,
                      fun (P) -> ChangePassVerify(P, "test") end,
                      fun (P) -> ChangePassVerify(P, "") end,
                      RotateDataKeyVerify]] ++
    [?_test(upgrade_from_7_6_test_base(DKeyPath, DKey, Pass, true, VerifyFun))
     || {DKey, Pass} <- [{DKeyWPass, "test"}, {DKeyNoPass, undefined}],
        VerifyFun <- [fun (P) -> ErrReadOnly = change_password(P, "asd") end,
                      fun (P) -> ErrReadOnly = rotate_data_key(P) end]].

upgrade_from_7_6_test_base(DKeyPath, OldDataKeyData, MasterPass, IsReadOnly,
                           VerifyFun) ->

    %% We can't use cfg_to_json/1 here, because it can change in future versions
    Cfg = {[{encryptionService,
             {[{keyStorageType, file},
               {keyStorageSettings,
                {[{path, iolist_to_binary(DKeyPath)},
                  {encryptWithPassword, true},
                  {passwordSource, env},
                  {passwordSettings,
                   {[{envName, <<"CB_MASTER_PASSWORD">>}]}}]}}]}}]},

    memorize_hidden_pass(?HIDE(undefined)),
    ok = filelib:ensure_dir(DKeyPath),

    with_password_sent(
      "wrong", MasterPass,
      fun () ->
          %% We can't use with_gosecrets/2 here, because it would remove
          %% the data key file
          with_tmp_cfg(
            undefined,
            fun (CfgPath) ->
                ok = file:write_file(CfgPath, ejson:encode(Cfg)),
                ok = file:write_file(DKeyPath, base64:decode(OldDataKeyData)),
                {ok, Pid} = start_link(default, true, IsReadOnly, CfgPath),
                try
                    Data = rand:bytes(512),
                    {ok, Encrypted} = encrypt(Pid, Data),
                    {ok, Data} = decrypt(Pid, Encrypted),
                    Res = VerifyFun(Pid),
                    {ok, Data} = decrypt(Pid, Encrypted),
                    Res
                after
                    unlink(Pid),
                    exit(Pid, shutdown)
                end
            end)
      end).

change_password_memorization_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          Password = base64:encode_to_string(rand:bytes(128)),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    %% Here it is supposed to memorize the password
                    ok = change_password(Pid, Password),
                    encrypt(Pid, Data)
                end),

          %% Starting gosecrets without reset of memorized password in order
          %% to check the memorized password works
          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    decrypt(Pid, Encrypted)
                end,
                false)
      end).

send_password_memorization_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          Password = base64:encode_to_string(rand:bytes(128)),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    ok = change_password(Pid, Password),
                    encrypt(Pid, Data)
                end),

          %% When we send the passord it is supposed to memorize it,
          %% so after restart it should pick it up automatically
          with_password_sent(
            "wrong", Password,
            fun () ->
                {ok, Data} =
                    with_gosecrets(
                      Cfg,
                      fun (_CfgPath, Pid) ->
                          decrypt(Pid, Encrypted)
                      end)
            end),

          %% Starting gosecrets without reset of memorized password in order
          %% to check the memorized password works
          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    decrypt(Pid, Encrypted)
                end,
                false)
      end).

default_password_memorization_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    encrypt(Pid, Data)
                end),

          %% Starting gosecrets without reset of memorized password in order
          %% to check the memorized password works (in this case the memorized
          %% passowrd is undefined, but still we should check that it doesn't
          %% break anything)
          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    decrypt(Pid, Encrypted)
                end,
                false)
      end).

default_password_from_env_memorization_test() ->
    with_tmp_datakey_cfg(
      fun (Cfg) ->
          Data = rand:bytes(512),
          Password = base64:encode_to_string(rand:bytes(128)),
          os:putenv("CB_MASTER_PASSWORD", Password),
          {ok, Encrypted} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    encrypt(Pid, Data)
                end),

          %% Starting gosecrets without reset of memorized password in order
          %% to check the memorized password works (in this case the memorized
          %% passowrd is undefined, but still we should check that it doesn't
          %% break anything)
          {ok, Data} =
              with_gosecrets(
                Cfg,
                fun (_CfgPath, Pid) ->
                    decrypt(Pid, Encrypted)
                end,
                false)
      end).

mac_test() ->
    Data = rand:bytes(512),
    Cfg = [],
    KeyDirs = [key_path(configDek, Cfg),
               key_path(logDek, Cfg),
               key_path(auditDek, Cfg),
               key_path(kek, Cfg)],
    with_all_stored_keys_cleaned(
      Cfg,
      fun () ->
          with_gosecrets(
            undefined,
            fun (_CfgPath, Pid) ->
                {ok, Mac1} = mac(Pid, Data),
                {ok, WrongMac} = mac(Pid, <<>>),
                MacLen = byte_size(Mac1),
                RandomUUIDMac = <<0, (rand:bytes(MacLen-1))/binary>>,
                ?assertEqual(ok, verify_mac(Pid, Mac1, Data)),
                ?assertEqual({error, "mac is empty"},
                             verify_mac(Pid, <<>>, Data)),
                ?assertEqual({error, "unknown mac version: 3"},
                             verify_mac(Pid, <<3, 4, 5, 6>>, Data)),
                ?assertEqual({error, "unexpected mac length: 4"},
                             verify_mac(Pid, <<0, 1, 2, 3>>, Data)),
                ?assertMatch({error, "invalid utf-8 in uuid: " ++ _},
                             verify_mac(Pid, RandomUUIDMac, Data)),
                ?assertEqual({error, "invalid mac"},
                             verify_mac(Pid, WrongMac, Data)),

                %% Rotate tokens and make sure that old mac is still valid
                ok = store_key(Pid, configDek, ?key1, 'raw-aes-gcm',
                               rand:bytes(32), <<"encryptionService">>,
                               <<"2024-07-26T19:32:19Z">>, false, false),
                ok = rotate_integrity_tokens(Pid, ?key1),

                {ok, Mac2} = mac(Pid, Data),
                ?assertEqual(ok, verify_mac(Pid, Mac1, Data)),
                ?assertEqual(ok, verify_mac(Pid, Mac2, Data)),

                %% Remove old tokens, which should mac Mac1 invalid
                ok = remove_old_integrity_tokens(Pid, KeyDirs),

                ?assertMatch({error, "unknown token: " ++ _},
                             verify_mac(Pid, Mac1, Data)),
                ?assertEqual(ok, verify_mac(Pid, Mac2, Data)),

                %% Rotate tokens multiple times and make sure that Mac2 is still
                %% valid
                ok = rotate_integrity_tokens(Pid, ?key1),
                ok = rotate_integrity_tokens(Pid, ?key1),
                ok = rotate_integrity_tokens(Pid, ?key1),
                ok = rotate_integrity_tokens(Pid, ?key1),

                ?assertEqual(ok, verify_mac(Pid, Mac2, Data)),

                %% Remove old tokens, which should mac Mac2 invalid
                ok = remove_old_integrity_tokens(Pid, KeyDirs),
                ?assertMatch({error, "unknown token: " ++ _},
                             verify_mac(Pid, Mac2, Data))
            end)
      end).

with_tmp_datakey_cfg(Fun) ->
    DKFile = path_config:tempfile("encrypted_datakey", ".tmp"),
    Cfg = [{es_key_path_type, custom}, {es_custom_key_path, DKFile}],
    try
        Fun(Cfg)
    after
        file:delete(DKFile),
        os:unsetenv("CB_MASTER_PASSWORD")
    end.

with_all_stored_keys_cleaned(Cfg, Fun) ->
    Keys = [configDek, logDek, auditDek, kek, bucketDek],
    Paths = [binary_to_list(P) || Key <- Keys,
                                  P <- [key_path(Key, Cfg)],
                                  P =/= undefined],
    [ok = misc:rm_rf(Path) || Path <- Paths],
    try
        Fun()
    after
        [ok = misc:rm_rf(Path) || Path <- Paths]
    end.

with_password_sent(_WrongPassword, undefined, Fun) ->
    Fun();
with_password_sent(WrongPassword, CorrectPassword, Fun) ->
    Parent = self(),
    Ref = make_ref(),
    PortFile = path_config:component_path(
                  data, "couchbase-server.babysitter.smport"),
    case file:delete(PortFile) of
        ok -> ok;
        {error, enoent} -> ok
    end,
    Pid =
        spawn_link(
          fun () ->
              Parent ! {Ref, try_send_password(WrongPassword, PortFile, 300),
                             try_send_password(CorrectPassword, PortFile, 300)}
          end),
    try
        Res = Fun(),

        receive
            {Ref, Res1, Res2} ->
                ?assertEqual({error,{recv_response_failed, "retry"}}, Res1),
                ?assertEqual(ok, Res2)
        after
            120000 ->
                erlang:error(password_confirmation_wait_timed_out)
        end,
        Res
    after
        misc:unlink_terminate_and_wait(Pid, shutdown)
    end.

try_send_password(_Pass, _PortFile, Retries) when Retries =< 0 ->
    {error, password_transfer_failed};
try_send_password(Pass, PortFile, Retries) ->
    case file:read_file(PortFile) of
        {ok, PortFileContentBin} ->
            [InetFamilyBin, PortBin] = string:lexemes(PortFileContentBin, " "),
            Port = binary_to_integer(PortBin),
            {ok, Socket} = gen_udp:open(0),
            try
                Addr = misc:localhost(binary_to_atom(InetFamilyBin), []),
                ok = gen_udp:send(Socket, Addr, Port, [], list_to_binary(Pass)),
                receive
                    {udp, Socket, _, Port, "ok"} ->
                        ok;
                    {udp, Socket, _, Port, Reply} ->
                        {error, {recv_response_failed, Reply}}
                after
                    60000 -> {error, {recv_response_failed, timeout}}
                end
            after
                gen_udp:close(Socket)
            end;
        {error, enoent} ->
            %% Waiting for gosecret to start and open the port
            timer:sleep(200),
            try_send_password(Pass, PortFile, Retries - 1)
    end.

with_gosecrets(Cfg, Fun) ->
    with_gosecrets(Cfg, Fun, true).

with_gosecrets(Cfg, Fun, ResetMemorizePassword) ->
    ResetMemorizePassword andalso memorize_hidden_pass(?HIDE(undefined)),
    with_tmp_cfg(
      Cfg,
      fun (CfgPath) ->
          {ok, Pid} = start_link(default, true, false, CfgPath),
          try
              Fun(CfgPath, Pid)
          after
              unlink(Pid),
              exit(Pid, shutdown)
          end
      end).

with_tmp_cfg(Cfg, Fun) ->
    %% If previous tests finish ungracefully, they can leave default data key
    %% file on disk. Removing it here.
    delete_all_default_files(),
    CfgPath = path_config:tempfile("gosecrets", ".cfg"),
    try
        case Cfg of
            undefined -> ok;
            _ -> save_config(CfgPath, cfg_to_json(Cfg), #state{})
        end,
        Fun(CfgPath)
    after
        delete_all_default_files(),
        file:delete(filename:join(filename:dirname(CfgPath),
                                  "stored_keys_tokens")),
        file:delete(CfgPath)
    end.

delete_all_default_files() ->
    file:delete(default_data_key_path(env)),
    file:delete(default_data_key_path(script)),
    file:delete(default_unencrypted_data_key_path()).


-endif.
