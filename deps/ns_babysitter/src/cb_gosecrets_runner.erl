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

-export([start_link/0, start_link/1]).
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
         defaults/0]).

-record(state, {config :: file:filename(),
                loop :: pid()}).

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

start_link() ->
    start_link(gosecrets_cfg_path()).

start_link(ConfigPath) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [ConfigPath], []).

prompt_the_password(State, Retries) ->
    StdIn =
        case application:get_env(handle_ctrl_c) of
            {ok, true} ->
                erlang:open_port({fd, 0, 1}, [in, stream, binary, eof]);
            _ ->
                undefined
        end,
    try
        prompt_the_password(State, Retries, StdIn)
    after
        case StdIn of
            undefined ->
                ok;
            _ ->
                port_close(StdIn)
        end
    end.

prompt_the_password(State, MaxRetries, StdIn) ->
    case open_udp_socket() of
        {ok, Socket} ->
            try
                save_port_file(Socket),
                prompt_the_password(State, MaxRetries, StdIn,
                                    Socket, _RetriesLeft = MaxRetries)
            after
                file:delete(port_file_path()),
                catch gen_udp:close(Socket)
            end;
        {error, Error} ->
            {error, {udp_socket_open_failed, Error}}
    end.

prompt_the_password(State, MaxRetries, StdIn, Socket, RetriesLeft) ->
    {ok, {Addr, Port}} = inet:sockname(Socket),
    ?log_debug("Waiting for the master password to be supplied (UDP: ~p:~b). "
               "Attempt ~p (~p attempts left)",
               [Addr, Port, MaxRetries - RetriesLeft + 1, RetriesLeft]),
    receive
        {StdIn, M} ->
            ?log_error("Password prompt interrupted: ~p", [M]),
            {error, interrupted};
        {udp, Socket, FromAddr, FromPort, Password} ->
            case call_init(?HIDE(Password), State) of
                ok ->
                    gen_udp:send(Socket, FromAddr, FromPort, <<"ok">>),
                    ok;
                {wrong_password, _} when RetriesLeft > 1 ->
                    gen_udp:send(Socket, FromAddr, FromPort, <<"retry">>),
                    timer:sleep(1000),
                    prompt_the_password(State, MaxRetries, StdIn,
                                        Socket, RetriesLeft - 1);
                {Reply, _Reason} when Reply == error;
                                      Reply == wrong_password  ->
                    gen_udp:send(Socket, FromAddr, FromPort, <<"auth_failure">>),
                    {error, auth_failure}
            end
    end.

init([GosecretsCfgPath]) ->
    case filelib:is_file(GosecretsCfgPath) of
        true -> ok;
        false ->
            ok = filelib:ensure_dir(GosecretsCfgPath),
            save_config(GosecretsCfgPath, default_cfg())
    end,

    State = #state{config = GosecretsCfgPath,
                   loop = start_gosecrets(GosecretsCfgPath)},

    HiddenPass = extract_hidden_pass(),

    init_gosecrets(HiddenPass, _MaxRetries = 3, State),

    {ok, State}.

save_config(CfgPath, Cfg) ->
    ?log_debug("Writing ~s:~n~p", [CfgPath, Cfg]),
    CfgJson = ejson:encode(Cfg),
    case misc:atomic_write_file(CfgPath, CfgJson) of
        ok -> ok;
        {error, Error} ->
            ?log_error("Could not write file '~s': ~s (~p)",
                       [CfgPath, file:format_error(Error),
                        Error]),
            erlang:error({write_failed, CfgPath, Error})
    end.

init_gosecrets(HiddenPass, MaxRetries, State) ->
    case call_init(HiddenPass, State) of
        ok -> ok;
        {wrong_password, _} ->
            case should_prompt_the_password(State) of
                true ->
                    try
                        case prompt_the_password(State, MaxRetries) of
                            ok ->
                                ok;
                            {error, Error} ->
                                ?log_error(
                                  "Stopping babysitter because gosecrets "
                                  "password prompting has failed: ~p",
                                  [Error]),
                                init:stop(),
                                shutdown
                        end
                    catch
                        C:E:ST ->
                            ?log_error("Unhandled exception: ~p~n~p", [E, ST]),
                            erlang:raise(C, E, ST)
                    end;
                false ->
                    ?log_error("Stopping babysitter"),
                    init:stop(),
                    shutdown
            end;
        {error, Error} ->
            erlang:error({gosecrets_init_failed, Error})
    end.

call_init(HiddenPass, State) ->
    case call_gosecrets({init, HiddenPass}, State) of
        ok ->
            memorize_hidden_pass(HiddenPass),
            ?log_info("Init complete. Password (if used) accepted."),
            ok;
        {error, "key decrypt failed:" ++ _ = Error} ->
            ?log_error("Incorrect master password. Error: ~p", [Error]),
            {wrong_password, Error};
        {error, Error} ->
            ?log_error("Gosecrets initialization failed: ~s", [Error]),
            {error, Error}
    end.

handle_call({encrypt, Data}, _From, State) ->
    {reply, call_gosecrets({encrypt, Data}, State), State};
handle_call({decrypt, Data}, _From, State) ->
    {reply,
     case call_gosecrets({decrypt, Data}, State) of
         ok ->
             {ok, <<>>};
         Ret ->
             Ret
     end, State};
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
    try save_config(CfgPath, cfg_to_json(Cfg)) of
        ok ->
            Pass = case ResetPassword of
                       true -> ?HIDE(undefined);
                       false -> extract_hidden_pass()
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
handle_call(Call, _From, State) ->
    ?log_warning("Unhandled call: ~p", [Call]),
    {reply, {error, not_allowed}, State}.

handle_cast(Msg, State) ->
    ?log_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?log_warning("Unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_gosecrets(CfgPath) ->
    Parent = self(),
    {ok, Pid} =
        proc_lib:start_link(
          erlang, apply,
          [fun () ->
                   process_flag(trap_exit, true),
                   Path = path_config:component_path(bin, "gosecrets"),
                   Args = ["--config", CfgPath],
                   ?log_debug("Starting ~p with args: ~0p", [Path, Args]),
                   Port =
                       open_port(
                         {spawn_executable, Path},
                         [{packet, 4}, binary, hide, {args, Args}]),
                   proc_lib:init_ack({ok, self()}),
                   gosecrets_loop(Port, Parent)
           end, []]),
    ?log_debug("Gosecrets loop started with pid = ~p", [Pid]),
    Pid.

call_gosecrets(Msg, #state{loop = Pid}) ->
    Pid ! {call, Msg},
    receive
        {reply, Resp} ->
            Resp
    end.

gosecrets_loop(Port, Parent) ->
    receive
        {call, {port_info, I}} ->
            Parent ! {reply, erlang:port_info(Port, I)},
            gosecrets_loop(Port, Parent);
        {call, Msg} ->
            Port ! {self(), {command, encode(Msg)}},
            wait_call_resp(Port, Parent),
            gosecrets_loop(Port, Parent);
        {Port, {data, <<"L", Data/binary>>}} ->
            handle_gosecrets_log(Data),
            gosecrets_loop(Port, Parent);
        Exit = {'EXIT', _, _} ->
            gosecret_process_exit(Port, Exit)
    end.

wait_call_resp(Port, Parent) ->
    receive
        Exit = {'EXIT', _, _} ->
            gosecret_process_exit(Port, Exit);
        {Port, {data, <<"L", Data/binary>>}} ->
            handle_gosecrets_log(Data),
            wait_call_resp(Port, Parent);
        {Port, {data, <<"S">>}} ->
            Parent ! {reply, ok};
        {Port, {data, <<"S", Data/binary>>}} ->
            Parent ! {reply, {ok, Data}};
        {Port, {data, <<"E", Data/binary>>}} ->
            Parent ! {reply, {error, binary_to_list(Data)}}
    end.

handle_gosecrets_log(Data) ->
    ?log_debug("gosecrets: ~s", [Data]).

gosecret_process_exit(Port, Exit) ->
    ?log_debug("Received exit ~p for port ~p", [Exit, Port]),
    gosecret_do_process_exit(Port, Exit).

gosecret_do_process_exit(Port, {'EXIT', Port, Reason}) ->
    exit({port_terminated, Reason});
gosecret_do_process_exit(_Port, {'EXIT', _, Reason}) ->
    exit(Reason).

encode({init, HiddenPass}) ->
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
    <<11, ConfigBin/binary>>.

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

open_udp_socket() ->
    case open_udp_socket(inet) of
        {ok, S} ->
            {ok, S};
        {error, Reason1} ->
            ?log_warning("Failed to open TCPv4 UDP port: ~p", [Reason1]),
            case open_udp_socket(inet6) of
                {ok, S} ->
                    {ok, S};
                {error, Reason2} ->
                    ?log_error("Failed to open TCPv6 UDP port: ~p", [Reason2]),
                    {error, {Reason1, Reason2}}
            end
    end.

open_udp_socket(AFamily) ->
    gen_udp:open(0, [AFamily, {ip, loopback}, {active, true}]).

default_cfg() -> cfg_to_json([]).

memorize_hidden_pass(HiddenPass) ->
    application:set_env(ns_babysitter, master_password, HiddenPass).

extract_hidden_pass()->
    case application:get_env(ns_babysitter, master_password) of
        {ok, P} ->
            ?log_info("Trying to recover the password from application "
                      "environment"),
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
                    {encryptWithPassword, Encr}] ++ PasswordCfg}}]}}]};
        script ->
            R = ExtractBin(es_read_cmd),
            W = ExtractBin(es_write_cmd),
            D = ExtractBin(es_delete_cmd),
            {[{encryptionService,
               {[{keyStorageType, script},
                 {keyStorageSettings,
                  {[{readCmd, R} || R /= <<>>] ++
                   [{writeCmd, W} || W /= <<>>] ++
                   [{deleteCmd, D} || D /= <<>>]}}]}}]}
    end.

defaults() ->
    [{es_password_env, "CB_MASTER_PASSWORD"},
     {es_password_source, env},
     {es_encrypt_key, true},
     {es_key_path_type, auto},
     {es_key_storage_type, 'file'}].

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

-ifdef(TEST).

should_prompt_the_password_test() ->
    CfgPath = path_config:tempfile("promt_pass_test_cfg", ".tmp"),
    try
        State = #state{config = CfgPath},
        save_config(CfgPath, cfg_to_json([])),
        ?assertEqual(true, should_prompt_the_password(State)),
        save_config(CfgPath, cfg_to_json([{es_key_storage_type, script},
                                          {es_read_cmd, <<"/path">>},
                                          {es_write_cmd, <<"/path">>},
                                          {es_delete_cmd, <<"/path">>}])),
        ?assertEqual(false, should_prompt_the_password(State)),
        save_config(CfgPath, cfg_to_json([{es_key_storage_type, file},
                                          {es_encrypt_key, true},
                                          {es_password_source, script},
                                          {es_password_cmd, <<"/path">>}])),
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
          {ok, Pid} = start_link(CfgPath),
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
                {ok, Pid} = start_link(CfgPath),
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

with_tmp_datakey_cfg(Fun) ->
    DKFile = path_config:tempfile("encrypted_datakey", ".tmp"),
    Cfg = [{es_key_path_type, custom}, {es_custom_key_path, DKFile}],
    try
        Fun(Cfg)
    after
        file:delete(DKFile),
        os:unsetenv("CB_MASTER_PASSWORD")
    end.

with_password_sent(WrongPassword, CorrectPassword, Fun) ->
    Parent = self(),
    Ref = make_ref(),
    PortFile = path_config:component_path(
                  data, "couchbase-server.babysitter.smport"),
    case file:delete(PortFile) of
        ok -> ok;
        {error, enoent} -> ok
    end,
    spawn_link(
      fun () ->
          Parent ! {Ref, try_send_password(WrongPassword, PortFile, 300),
                         try_send_password(CorrectPassword, PortFile, 300)}
      end),

    Res = Fun(),

    receive
        {Ref, Res1, Res2} ->
            ?assertEqual({error,{recv_response_failed, "retry"}}, Res1),
            ?assertEqual(ok, Res2)
    after
        120000 ->
            erlang:error(password_confirmation_wait_timed_out)
    end,

    Res.

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
          {ok, Pid} = start_link(CfgPath),
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
            _ -> save_config(CfgPath, cfg_to_json(Cfg))
        end,
        Fun(CfgPath)
    after
        delete_all_default_files(),
        file:delete(CfgPath)
    end.

delete_all_default_files() ->
    file:delete(default_data_key_path(env)),
    file:delete(default_data_key_path(script)),
    file:delete(default_unencrypted_data_key_path()).


-endif.
