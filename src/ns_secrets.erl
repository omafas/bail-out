%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ns_secrets).

-behaviour(gen_server).

%% API
-export([start_link/0,
         get_pkey_pass/0,
         load_passphrase/1,
         extract_pkey_pass/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-include("ns_common.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

load_passphrase(PassSettings) when is_list(PassSettings) ->
    gen_server:call(?MODULE, {load_passphrase, fun () -> PassSettings end},
                    30000).

get_pkey_pass() ->
    gen_server:call(?MODULE, get_pkey_pass, 30000).

%%%===================================================================
%%% callbacks
%%%===================================================================

init([]) -> {ok, #{}}.

handle_call({load_passphrase, PassSettingsFun}, _From, State) ->
    Fun = case extract_pkey_pass(PassSettingsFun()) of
              {ok, F} -> F;
              {error, _} -> fun () -> undefined end
          end,
    {reply, ok, State#{pkey_passphrase_fun => Fun}};
handle_call(get_pkey_pass, _From, State) ->
    Res = maps:get(pkey_passphrase_fun, State, fun () -> undefined end),
    {reply, Res, State};
handle_call(_Request, _From, State) ->
    {reply, unhandled, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

extract_pkey_pass(PassSettings) ->
    case proplists:get_value(type, PassSettings) of
        plain ->
            P = proplists:get_value(password, PassSettings),
            {ok, fun () -> binary_to_list(P) end};
        script ->
            extract_pkey_pass_with_script(PassSettings);
        rest ->
            extract_pkey_pass_with_rest(PassSettings);
        undefined ->
            {ok, fun () -> undefined end}
    end.

extract_pkey_pass_with_rest(PassSettings) ->
    URL = binary_to_list(proplists:get_value(url, PassSettings)),
    Timeout = proplists:get_value(timeout, PassSettings),
    AddrSettings = case proplists:get_value(addressFamily, PassSettings) of
                       undefined -> [];
                       AF -> [AF]
                   end,
    HttpsOpts = proplists:get_value(httpsOpts, PassSettings),
    VerifySettings = case HttpsOpts of
                         undefined -> [];
                         _ ->
                             case proplists:get_value(verifyPeer, HttpsOpts,
                                                      true) of
                                 true ->
                                     CA = ns_ssl_services_setup:ca_file_path(),
                                     [{verify, verify_peer}, {cacertfile, CA}];
                                 false ->
                                     [{verify, verify_none}]
                             end
                     end,
    Options = AddrSettings ++ VerifySettings,
    Headers = proplists:get_value(headers, PassSettings, []),
    ?log_info("Calling REST API: ~s", [URL]),
    try rest_utils:request(<<"pkey_passphrase">>, URL, "GET", Headers, <<>>,
                           Timeout, [{connect_options, Options}]) of
        {ok, {{Status, _}, _RespHeaders, RespBody}} when Status == 200 ->
            ?log_info("PKey passphrase REST API call ~s succeded", [URL]),
            {ok, fun () -> binary_to_list(RespBody) end};
        {ok, {{Status, Reason}, _RespHeaders, _RespBody}} ->
            ?log_error("PKey passphrase REST API call ~s returned ~p ~p",
                       [URL, Status, Reason]),
            {error, {rest_failed, URL, {status, Status}}};
        {error, Reason} ->
            ?log_error("PKey passphrase REST API call ~s failed, reason:~n~p",
                       [URL, Reason]),
            {error, {rest_failed, URL, {error, Reason}}}
    catch
        _:E:ST ->
            ?log_error("PKey passphrase REST API call ~s crashed~n"
                       "Exception: ~p~n"
                       "Stacktrace: ~p", [URL, E, ST]),
            {error, {rest_failed, URL, exception}}
    end.

extract_pkey_pass_with_script(PassSettings) ->
    Path = proplists:get_value(path, PassSettings),
    Trim = proplists:get_value(trim, PassSettings),
    Args = proplists:get_value(args, PassSettings),
    Timeout = proplists:get_value(timeout, PassSettings),
    ?log_info("Calling external script to extract pkey passphrase: ~s~n"
              "Args: ~p~nTimeout: ~p", [Path, Args, Timeout]),
    try call_external_script(Path, Args, Timeout) of
        {0, P} ->
            MaybeTrimmed = case Trim of
                               true -> string:trim(P);
                               false -> P
                           end,
            ?log_info("Script executed successfully"),
            {ok, fun () -> binary_to_list(MaybeTrimmed) end};
        {Status, Output} ->
            ?log_error("External pkey passphrase script ~s ~p finished "
                       "with exit status: ~p:~n~s",
                       [Path, Args, Status, Output]),
            {error, {script_execution_failed, {status, Status, Output}}}
    catch
        _:E:ST ->
            ?log_error("External pkey passphrase script execution "
                       "exception: ~s~nArgs: ~p~nException: ~p~n"
                       "Stacktrace: ~p", [Path, Args, E, ST]),
            {error, {script_execution_failed, exception}}
    end.

call_external_script(Path, Args, Timeout) ->
    Port = erlang:open_port({spawn_executable, Path},
                            [stderr_to_stdout, binary,
                             stream, exit_status, hide,
                             {args, Args}]),
    StartTime = erlang:system_time(millisecond),
    Deadline = StartTime + Timeout,
    wait_for_exit(Port, <<>>, Deadline).

wait_for_exit(Port, Output, Deadline) ->
    Now = erlang:system_time(millisecond),
    Timeout = max(Deadline - Now, 0),
    receive
        {Port, {data, Data}} ->
            wait_for_exit(Port, <<Output/binary, Data/binary>>, Deadline);
        {Port, {exit_status, Status}} ->
            {Status, Output}
    after Timeout ->
        port_close(Port),
        error(timeout)
    end.
