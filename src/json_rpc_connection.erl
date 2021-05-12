%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(json_rpc_connection).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/2,
         perform_call/3, perform_call/4,
         reannounce/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {label :: string(),
                counter :: non_neg_integer(),
                sock :: port(),
                id_to_caller_tid :: ets:tid()}).

-define(PREFIX, "json_rpc_connection-").

-define(RPC_TIMEOUT, ?get_timeout(json_rpc_timeout, 60000)).

label_to_name(Pid) when is_pid(Pid) ->
    Pid;
label_to_name(Label) when is_list(Label)  ->
    list_to_atom(?PREFIX ++ Label).

start_link(Label, GetSocket) ->
    proc_lib:start_link(?MODULE, init, [{Label, GetSocket}]).

perform_call(Label, Name, EJsonArg, Timeout) ->
    EJsonArgThunk = fun () -> EJsonArg end,
    gen_server:call(label_to_name(Label), {call, Name, EJsonArgThunk}, Timeout).

perform_call(Label, Name, EJsonArg) ->
    perform_call(Label, Name, EJsonArg, infinity).

reannounce(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, reannounce).

init({Label, GetSocket}) ->
    proc_lib:init_ack({ok, self()}),
    InetSock = GetSocket(),

    Name = label_to_name(Label),
    case erlang:whereis(Name) of
        undefined ->
            ok;
        ExistingPid ->
            erlang:exit(ExistingPid, new_instance_created),
            misc:wait_for_process(ExistingPid, infinity)
    end,
    true = erlang:register(Name, self()),
    ok = inet:setopts(InetSock, [{nodelay, true}]),
    IdToCaller = ets:new(ets, [set, private]),
    _ = proc_lib:spawn_link(erlang, apply, [fun receiver_loop/3, [InetSock, self(), <<>>]]),
    ?log_debug("Observed revrpc connection: label ~p, handling process ~p",
               [Label, self()]),
    gen_event:notify(json_rpc_events, {started, Label, self()}),

    gen_server:enter_loop(?MODULE, [],
                          #state{label = Label,
                                 counter = 0,
                                 sock = InetSock,
                                 id_to_caller_tid = IdToCaller}).

handle_cast(reannounce, #state{label = Label} = State) ->
    gen_event:notify(json_rpc_events, {needs_update, Label, self()}),
    {noreply, State};
handle_cast(_Msg, _State) ->
    erlang:error(unknown).

handle_info({chunk, Chunk}, #state{id_to_caller_tid = IdToCaller} = State) ->
    {KV} = ejson:decode(Chunk),
    {_, Id} = lists:keyfind(<<"id">>, 1, KV),
    [{_, From}] = ets:lookup(IdToCaller, Id),
    ets:delete(IdToCaller, Id),
    ale:debug(?JSON_RPC_LOGGER, "got response: ~p", [KV]),
    {RV, Result} =
        case lists:keyfind(<<"error">>, 1, KV) of
            false ->
                {ok, ok};
            {_, null} ->
                {ok, ok};
            {_, Error} ->
                case Error of
                    <<"rpc: can't find method ", _/binary>> ->
                        {ok, {error, method_not_found}};
                    <<"rpc: can't find service ", _/binary>> ->
                        {ok, {error, method_not_found}};
                    <<"rpc: ", _/binary>> ->
                        ?log_error("Unexpected rpc error: ~p. Die.", [Error]),
                        {stop, {error, {rpc_error, Error}}};
                    _ ->
                        {ok, {error, Error}}
                end
        end,
    Reply = case Result of
                ok ->
                    {_, Res} = lists:keyfind(<<"result">>, 1, KV),
                    {ok, Res};
                {error, _} ->
                    Result
            end,
    gen_server:reply(From, Reply),
    case RV of
        stop ->
            {stop, {error, rpc_error}, State};
        ok ->
            {noreply, State}
    end;
handle_info(socket_closed, State) ->
    ?log_debug("Socket closed"),
    {stop, shutdown, State};
handle_info(Msg, State) ->
    ?log_debug("Unknown msg: ~p", [Msg]),
    {noreply, State}.

handle_call({call, Name, EJsonArgThunk}, From, #state{counter = Counter,
                                                      id_to_caller_tid = IdToCaller,
                                                      sock = Sock} = State) ->
    EJsonArg = EJsonArgThunk(),

    NameB = if
                is_list(Name) ->
                    list_to_binary(Name);
                true ->
                    Name
            end,
    MaybeParams = case EJsonArg of
                      undefined ->
                          [];
                      _ ->
                          %% golang's jsonrpc only supports array of
                          %% single arg
                          [{params, [EJsonArg]}]
                  end,
    EJSON = {[{jsonrpc, <<"2.0">>},
              {id, Counter},
              {method, NameB}
              | MaybeParams]},
    ale:debug(?JSON_RPC_LOGGER,
              "sending jsonrpc call:~p", [ns_config_log:sanitize(EJSON, true)]),
    ok = gen_tcp:send(Sock, [ejson:encode(EJSON) | <<"\n">>]),
    ets:insert(IdToCaller, {Counter, From}),
    {noreply, State#state{counter = Counter + 1}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


receiver_loop(Sock, Parent, Acc) ->
    RecvData = case gen_tcp:recv(Sock, 0) of
                   {error, closed} ->
                       Parent ! socket_closed,
                       erlang:exit(normal);
                   {ok, XRecvData} ->
                       XRecvData
               end,
    Data = case Acc of
               <<>> ->
                   RecvData;
               _ ->
                   <<Acc/binary, RecvData/binary>>
           end,
    NewAcc = receiver_handle_data(Parent, Data),
    receiver_loop(Sock, Parent, NewAcc).

receiver_handle_data(Parent, Data) ->
    case binary:split(Data, <<"\n">>) of
        [Chunk, <<>>] ->
            Parent ! {chunk, Chunk},
            <<>>;
        [Chunk, Rest] ->
            Parent ! {chunk, Chunk},
            receiver_handle_data(Parent, Rest);
        [SingleChunk] ->
            SingleChunk
    end.
