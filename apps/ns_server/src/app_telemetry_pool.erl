%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(app_telemetry_pool).

-include("ns_common.hrl").
-include("rbac.hrl").
-include("websocket.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

%% API
-export([start_link/1, get_pids/0, handle_connect/1, call/3, drop/1,
         update_max/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(client, {reply_channel :: reply_channel(),
                 handler :: undefined | gen_server:from(),
                 user :: #authn_res{},
                 monitor_ref :: undefined | reference()}).

-record(state,
        {clients = #{} :: #{pid() => #client{}},
         max = 1024 :: non_neg_integer()}).

-record(add_client,
        {pid :: pid(),
         client :: #client{},
         continuation :: fun(() -> ok)}).

-record(call,
        {pid :: pid(),
         body :: binary()}).

-record(drop, {pid :: pid()}).

-record(receive_data,
        {pid :: pid(),
         data :: pong | {binary, binary()} | {text, binary()}}).

-record(update_max, {max :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec handle_connect(mochiweb_request()) -> ok.
handle_connect(Req) ->
    Pid = self(),
    Body =
        fun (Payload, _State, ReplyChannel) ->
                lists:foreach(process_frame(Pid, ReplyChannel, _),
                              Payload)
        end,
    case menelaus_websocket:handle_upgrade(Req, Body) of
        {ok, Connection} -> connection_handler(Pid, Connection, Req);
        _ -> ok
    end.

-spec get_pids() -> [pid()].
get_pids() ->
    gen_server:call(?SERVER, get_pids).

-spec call(pid(), binary(), integer()) ->
    {error, term()} | {ok, pong | {binary, binary()} | {text, binary()}}.
call(Pid, Body, Timeout) ->
    try gen_server:call(?SERVER, #call{pid = Pid, body = Body}, Timeout) of
        {error, _} = Error -> Error;
        {ok, Result} -> {ok, Result}
    catch T:E ->
            ?log_debug("Dropping connection ~p because a call got error ~p",
                       [Pid, {T, E}]),
            drop(Pid),
            {error, {T, E}}
    end.

-spec drop(pid()) -> ok.
drop(Pid) ->
    gen_server:call(?SERVER, #drop{pid = Pid}).

-spec update_max(non_neg_integer()) -> ok.
update_max(NewMax) ->
    gen_server:call(?SERVER, #update_max{max = NewMax}).

start_link(ArgsMap) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ArgsMap], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ArgsMap]) ->
    notify_curr_connections(#{}),
    {ok, build_state(ArgsMap)}.

handle_call(#add_client{pid = Pid,
                        client = Client,
                        continuation = Continuation},
            _From,
            State0 = #state{clients = Clients,
                            max = Max}) when map_size(Clients) < Max ->
    {reply, ok, do_add_client(State0, Pid, Client, Continuation)};
handle_call(#add_client{}, _From, State0 = #state{}) ->
    {reply, {error, too_many_clients}, State0};
handle_call(get_pids, _From, State = #state{clients = Clients}) ->
    {reply, maps:keys(Clients), State};
handle_call(#call{pid = Pid, body = Body}, From,
            State0 = #state{clients = Clients}) ->
    case maps:find(Pid, Clients) of
        error ->
            {reply, {error, unknown_pid}, State0};
        {ok, #client{reply_channel = ReplyChannel,
                     handler = undefined,
                     user = AuthnRes} = Client} ->
            case menelaus_roles:is_allowed({[app_telemetry], write},
                                           AuthnRes) of
                false ->
                    State1 = do_drop_client(State0, Pid),
                    {reply, {error, privilege_lost}, State1};
                true ->
                    menelaus_websocket:send_bytes(ReplyChannel, Body),
                    NewClient = Client#client{handler = From},
                    NewClients = maps:update(Pid, NewClient, Clients),
                    State1 = State0#state{clients = NewClients},
                    {noreply, State1}
            end;
        {ok, _Client} ->
            {reply, {error, call_handler_remaining}, State0}
    end;
handle_call(#drop{pid = Pid}, _From, State = #state{}) ->
    {reply, ok, do_drop_client(State, Pid)};
handle_call(#update_max{max = Max}, _From, State0 = #state{}) ->
    State1 = State0#state{max = Max},
    {reply, ok, enforce_max(State1)};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(#receive_data{pid = Pid, data = Data},
            State0 = #state{clients = Clients}) ->
    State1 =
        case maps:find(Pid, Clients) of
            {ok, #client{handler = Handler} = Client} ->
                case Handler of
                    undefined ->
                        ?log_debug("Dropping connection ~p, because it "
                                   "received an unexpected message ", [Pid]),
                        do_drop_client(State0, Pid);
                    _ ->
                        gen_server:reply(Handler, {ok, Data}),
                        NewClient = Client#client{handler = undefined},
                        NewClients = maps:update(Pid, NewClient,
                                                 Clients),
                        State0#state{clients = NewClients}
                end;
            error ->
                ?log_error("Ignoring data from unknown client ~p", [Pid]),
                State0
        end,
    {noreply, State1};
handle_info({'DOWN', _, process, WebsocketPid, ExitReason},
            #state{clients = Clients} = State)
    when is_map_key(WebsocketPid, Clients) ->
    %% Connection went down, make sure that we properly drop it
    ?log_debug("Dropping connection ~p, because the proccess went down "
               "with reason ~p", [WebsocketPid, ExitReason]),
    {noreply, do_drop_client(State, WebsocketPid)};
handle_info({'DOWN', _, process, WebsocketPid, ExitReason}, #state{} = State) ->
    %% A monitored process went don't, but we don't recognise the pid, so
    %% nothing more can be done than logging it
    ?log_warning("Saw process ~p went down with reason ~p, but no websocket "
                 "connection is associated with it",
                 [WebsocketPid, ExitReason]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

connection_handler(Pid, {RespTuple, ReEntry, ReplyChannel}, Req) ->
    AuthnRes = menelaus_auth:get_authn_res(Req),
    Client = #client{reply_channel = ReplyChannel,
                     user = AuthnRes},
    %% Response needs to be sent before the pool starts using the websocket
    %% connection
    RespFun = ?cut(menelaus_util:respond(Req, RespTuple)),
    Call = #add_client{pid = Pid,
                       client = Client,
                       continuation = RespFun},
    %% If the pool crashes, we need to be notified, so that the socket
    %% is cleaned up correctly
    erlang:monitor(process, ?SERVER),
    case gen_server:call(?SERVER, Call) of
        ok ->
            menelaus_websocket:enter(ReEntry, ok);
        {error, too_many_clients} ->
            menelaus_util:reply(Req, 429);
        Error ->
            ?log_error("Failed to handle app telemetry connection: ~p",
                       [Error]),
            menelaus_util:reply_text(Req, <<"Unexpected server error">>,
                                     500)
    end.

process_frame(Pid, ReplyChannel, Frame) ->
    case is_ping(Frame) of
        true ->
            menelaus_websocket:send_pong(ReplyChannel);
        false ->
            receive_data(Pid, Frame)
    end.

receive_data(Pid, Data) ->
    ?SERVER ! #receive_data{pid = Pid, data = Data}.

build_state(ArgsMap) ->
    State0 = #state{},
    maps:fold(
      fun (max, Max, State) ->
              State#state{max=Max}
      end, State0, ArgsMap).

do_add_client(#state{clients = Clients} = State,
              Pid, Client, Continuation) ->
    %% We need to close the socket if the client terminates
    MRef = erlang:monitor(process, Pid),
    NewClients = maps:put(Pid, Client#client{monitor_ref = MRef}, Clients),
    notify_curr_connections(NewClients),
    %% Call continuation function before returning, to ensure it is called prior
    %% to using the websocket connection
    Continuation(),
    State#state{clients = NewClients}.

do_drop_client(#state{clients = Clients} = State, Pid) ->
    case maps:take(Pid, Clients) of
        {#client{reply_channel = ReplyChannel,
                 handler = Handler,
                 monitor_ref = MRef}, NewClients} ->
            erlang:demonitor(MRef, [flush]),
            menelaus_websocket:close(ReplyChannel),
            %% Ensure the socket gets closed
            Pid ! {tcp_closed, normal},
            case Handler of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(Handler, {error, dropped})
            end,

            notify_curr_connections(NewClients),
            State#state{clients = NewClients};
        error ->
            ?log_warning("Failed to drop connection ~p, because the pid is "
                         "not recognised", [Pid]),
            State
    end.

enforce_max(#state{clients = Clients, max = Max} = State)
  when map_size(Clients) =< Max ->
    State;
enforce_max(#state{clients = Clients, max = Max} = State)
  when map_size(Clients) > Max ->
    Pid = hd(maps:keys(Clients)),
    ?log_debug("Dropping connection ~p, because the connection limit was "
               "decreased (~p active connections > ~p max connections)",
               [Pid, map_size(Clients), Max]),
    enforce_max(do_drop_client(State, Pid)).

notify_curr_connections(Clients) ->
    ns_server_stats:notify_gauge(app_telemetry_curr_connections,
                                 map_size(Clients),
                                 #{expiration_s => infinity}).


is_ping(ping) -> true;
is_ping(_) -> false.

-ifdef(TEST).

-define(TIMEOUT, 1000).
-define(TEST_FRAME, <<1:8>>).

-define(expect_message(Pattern, Timeout),
        receive Pattern -> ok
        after Timeout -> erlang:exit(timeout)
        end).

-define(CLIENT_HANDSHAKE,
        {mochiweb_request,
         [placeholder_socket, placeholder_opts, placeholder_method,
          placeholder_raw_path, placeholder_version,
          mochiweb_headers:make([{"Connection", "Upgrade"},
                                 {"Upgrade", "websocket"},
                                 {"Sec-WebSocket-Version", "13"},
                                 {"Sec-WebSocket-Key",
                                  base64:encode(<<"abcdabcdabcdabcd">>)}]),
          placeholder_meta]}).

fake_websocket_loop(State, Body, ReplyChannel) ->
    receive
        {tcp_closed, _} ->
            exit(normal);
        Payload ->
            fake_websocket_loop(Body(Payload, State, ReplyChannel), Body,
                                ReplyChannel)
    end.

setup() ->
    meck:expect(mochiweb_socket, send,
                fun(_Socket, [_Prefix, _Opcode, _Payload, _Version]) ->
                        ok
                end),
    meck:expect(menelaus_util, respond, fun (_, _) -> ok end),
    meck:expect(menelaus_auth, get_authn_res, fun(_) -> #authn_res{} end),
    meck:expect(menelaus_roles, is_allowed, fun(_, _) -> true end).

teardown(_) ->
    meck:unload().

simple_test__() ->
    Parent = self(),
    meck:expect(mochiweb_websocket, upgrade_connection,
                fun ({_, _}, Body) ->
                        Receiver = self(),
                        Response = {101, [], ""},
                        ReplyChannel = fun ({?OPCODE_BINARY, ?TEST_FRAME}) ->
                                               Receiver ! [<<"test">>];
                                           ({?OPCODE_PONG, <<>>}) ->
                                               Parent ! pong_received;
                                           ({?OPCODE_CLOSE, <<>>}) ->
                                               ok
                                       end,
                        ReEntry =
                            fun (State) ->
                                    Parent ! connected,
                                    fake_websocket_loop(State, Body,
                                                        ReplyChannel)
                            end,
                        {Response, ReEntry, ReplyChannel}
                end),

    start_link(#{}),

    Pid = spawn_link(?cut(handle_connect(?CLIENT_HANDSHAKE))),

    %% Wait for fake connection to be established
    ?expect_message(connected, ?TIMEOUT),

    %% Test request and response
    ?assertEqual({ok, <<"test">>},
                 app_telemetry_pool:call(Pid, ?TEST_FRAME, ?TIMEOUT)),

    %% Send ping as if it was from the client, and expect pong sent back
    Pid ! [ping],
    ?expect_message(pong_received, ?TIMEOUT),

    %% Remove privilege, to test that connection is lost
    meck:expect(menelaus_roles, is_allowed, fun(_, _) -> false end),
    ?assertEqual({error, privilege_lost},
                 app_telemetry_pool:call(Pid, ?TEST_FRAME, ?TIMEOUT)),
    ?assertEqual(ok, misc:wait_for_process(Pid, ?TIMEOUT)).

simple_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     fun simple_test__/0}.

-endif.