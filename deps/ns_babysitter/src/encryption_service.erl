%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(encryption_service).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([remote_set_password/1]).

-export([set_password/1,
         decrypt/1,
         encrypt/1,
         change_password/1,
         get_data_key/0,
         rotate_data_key/0,
         maybe_clear_backup_key/1]).

data_key_store_path() ->
    filename:join(path_config:component_path(data, "config"), "encrypted_data_keys").

remote_set_password([Node, Password]) ->
    N = list_to_atom(Node),
    RV = rpc:call(N, encryption_service, set_password, [Password]),

    %% This API will be called from couchbase-cli. Mapping the return values to
    %% different exit codes so the cli need not scrape the stdout of the process
    %% that calls this to ascertain the outcome.
    ExitCode = case RV of
                   ok                   -> 0;
                   retry                -> 101;
                   {error, not_allowed} -> 102;
                   {badrpc, nodedown}   -> 103;
                   auth_failure         -> 104;
                   _                    -> 105
               end,
    init:stop(ExitCode).

set_password(Password) ->
    gen_server:call(?MODULE, {set_password, Password}, infinity).

encrypt(Data) ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, {encrypt, Data}, infinity).

decrypt(Data) ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, {decrypt, Data}, infinity).

change_password(NewPassword) ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()},
                    {change_password, NewPassword}, infinity).

get_data_key() ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, get_data_key, infinity).

rotate_data_key() ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, rotate_data_key, infinity).

maybe_clear_backup_key(DataKey) ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, {maybe_clear_backup_key, DataKey}, infinity).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

prompt_the_password(EncryptedDataKey, State) ->
    StdIn =
        case application:get_env(handle_ctrl_c) of
            {ok, true} ->
                erlang:open_port({fd, 0, 1}, [in, stream, binary, eof]);
            _ ->
                undefined
        end,
    RV = prompt_the_password(EncryptedDataKey, State, StdIn, 3),
    case StdIn of
        undefined ->
            ok;
        _ ->
            port_close(StdIn)
    end,
    RV.

prompt_the_password(EncryptedDataKey, State, StdIn, Tries) ->
    prompt_the_password(EncryptedDataKey, State, StdIn, Tries, Tries).

prompt_the_password(EncryptedDataKey, State, StdIn, Try, Tries) ->
    ?log_debug("Waiting for the master password to be supplied. Attempt ~p", [Tries - Try + 1]),
    receive
        {StdIn, M} ->
            ?log_error("Password prompt interrupted: ~p", [M]),
            ns_babysitter_bootstrap:stop(),
            shutdown;
        {'$gen_call', From, {set_password, P}} ->
            ok = set_password(P, State),
            case EncryptedDataKey of
                undefined ->
                    confirm_set_password(From, P);
                _ ->
                    Ret = call_gosecrets({set_data_key, EncryptedDataKey}, State),
                    case Ret of
                        ok ->
                            confirm_set_password(From, P);
                        Error ->
                            ?log_error("Incorrect master password. Error: ~p", [Error]),
                            maybe_retry_prompt_the_password(EncryptedDataKey, State, StdIn, From, Try, Tries)
                    end
            end
    end.

confirm_set_password(From, Password) ->
    application:set_env(ns_babysitter, master_password, Password),
    gen_server:reply(From, ok),
    ok.

maybe_retry_prompt_the_password(_EncryptedDataKey, _State, _StdIn, ReplyTo, 1, _Tries) ->
    gen_server:reply(ReplyTo, auth_failure),
    ?log_error("Incorrect master password!"),
    ns_babysitter_bootstrap:stop(),
    auth_failure;
maybe_retry_prompt_the_password(EncryptedDataKey, State, StdIn, ReplyTo, Try, Tries) ->
    gen_server:reply(ReplyTo, retry),
    prompt_the_password(EncryptedDataKey, State, StdIn, Try - 1, Tries).

set_password(Password, State) ->
    ?log_debug("Sending password to gosecrets"),
    call_gosecrets({set_password, Password}, State).

recover_or_prompt_password(EncryptedDataKey, State) ->
    case application:get_env(master_password) of
        {ok, P} ->
            ?log_info("Password was recovered from application environment"),
            ok = set_password(P, State);
        _ ->
            prompt_the_password(EncryptedDataKey, State)
    end.

init([]) ->
    Path = data_key_store_path(),
    EncryptedDataKey =
        case file:read_file(Path) of
            {ok, DataKey} ->
                ?log_debug("Encrypted data key retrieved from ~p", [Path]),
                DataKey;
            {error, enoent} ->
                ?log_debug("Encrypted data key is not found in ~p", [Path]),
                undefined
        end,
    State = start_gosecrets(),
    Password =
        case os:getenv("CB_MASTER_PASSWORD") of
            false ->
                "";
            S ->
                S
        end,
    ok = set_password(Password, State),

    EncryptedDataKey1 =
        case EncryptedDataKey of
            undefined ->
                ?log_debug("Create new data key."),
                {ok, NewDataKey} = call_gosecrets(create_data_key, State),
                ok = misc:mkdir_p(path_config:component_path(data, "config")),
                ok = misc:atomic_write_file(Path, NewDataKey),
                NewDataKey;
            _ ->
                EncryptedDataKey
        end,
    case call_gosecrets({set_data_key, EncryptedDataKey1}, State) of
        ok ->
            ok;
        Error ->
            ?log_error("Incorrect master password. Error: ~p", [Error]),
            recover_or_prompt_password(EncryptedDataKey, State)
    end,
    {ok, State}.

handle_call({set_password, _}, _From, State) ->
    {reply, {error, not_allowed}, State};
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
handle_call({change_password, NewPassword}, _From, State) ->
    {reply,
     call_gosecrets_and_store_data_key(
       {change_password, NewPassword}, "Master password change", State), State};
handle_call(get_data_key, _From, State) ->
    {reply, call_gosecrets(get_data_key, State), State};
handle_call(rotate_data_key, _From, State) ->
    {reply, call_gosecrets_and_store_data_key(rotate_data_key, "Data key rotation", State), State};
handle_call({maybe_clear_backup_key, DataKey}, _From, State) ->
    {reply, call_gosecrets_and_store_data_key({maybe_clear_backup_key, DataKey},
                                              "Clearing backup key", State), State};
handle_call(_, _From, State) ->
    {reply, {error, not_allowed}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

call_gosecrets_and_store_data_key(Call, Msg, State) ->
    case call_gosecrets(Call, State) of
        ok ->
            ok;
        {ok, NewEncryptedDataKey} ->
            ?log_info("~s succeded", [Msg]),
            ok = misc:atomic_write_file(data_key_store_path(), NewEncryptedDataKey);
        Error ->
            ?log_error("~s failed with ~p", [Msg, Error]),
            Error
    end.

start_gosecrets() ->
    Parent = self(),
    {ok, Pid} =
        proc_lib:start_link(
          erlang, apply,
          [fun () ->
                   process_flag(trap_exit, true),
                   Path = path_config:component_path(bin, "gosecrets"),
                   ?log_debug("Starting ~p", [Path]),
                   Port = open_port({spawn_executable, Path}, [{packet, 2}, binary, hide]),
                   proc_lib:init_ack({ok, self()}),
                   gosecrets_loop(Port, Parent)
           end, []]),
    ?log_debug("Gosecrets loop started with pid = ~p", [Pid]),
    Pid.

call_gosecrets(Msg, Pid) ->
    Pid ! {call, Msg},
    receive
        {reply, <<"S">>} ->
            ok;
        {reply, <<"S", Data/binary>>} ->
            {ok, Data};
        {reply, <<"E", Data/binary>>} ->
            {error, binary_to_list(Data)}
    end.

gosecrets_loop(Port, Parent) ->
    receive
        {call, Msg} ->
            Port ! {self(), {command, encode(Msg)}},
            receive
                Exit = {'EXIT', _, _} ->
                    gosecret_process_exit(Port, Exit);
                {Port, {data, Data}} ->
                    Parent ! {reply, Data}
            end,
            gosecrets_loop(Port, Parent);
        Exit = {'EXIT', _, _} ->
            gosecret_process_exit(Port, Exit)
    end.

gosecret_process_exit(Port, Exit) ->
    ?log_debug("Received exit ~p for port ~p", [Exit, Port]),
    gosecret_do_process_exit(Port, Exit).

gosecret_do_process_exit(Port, {'EXIT', Port, Reason}) ->
    exit({port_terminated, Reason});
gosecret_do_process_exit(_Port, {'EXIT', _, Reason}) ->
    exit(Reason).

encode({set_password, Password}) ->
    BinaryPassoword = list_to_binary(Password),
    <<1, BinaryPassoword/binary>>;
encode(create_data_key) ->
    <<2>>;
encode({set_data_key, DataKey}) ->
    <<3, DataKey/binary>>;
encode(get_data_key) ->
    <<4>>;
encode({encrypt, Data}) ->
    <<5, Data/binary>>;
encode({decrypt, Data}) ->
    <<6, Data/binary>>;
encode({change_password, Password}) ->
    BinaryPassoword = list_to_binary(Password),
    <<7, BinaryPassoword/binary>>;
encode(rotate_data_key) ->
    <<8>>;
encode({maybe_clear_backup_key, DataKey}) ->
    <<9, DataKey/binary>>.
