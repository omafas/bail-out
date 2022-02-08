%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc DCP proxy code that is common for consumer and producer sides
%%
-module(dcp_proxy).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/6, maybe_connect/2,
         connect_proxies/3, nuke_connection/4, terminate_and_wait/2]).

-export([get_socket/1, get_partner/1, get_conn_name/1, get_bucket/1]).

-record(state, {sock = undefined :: port() | undefined,
                connect_info,
                packet_len = undefined,
                buf = <<>> :: binary(),
                ext_module,
                ext_state,
                proxy_to = undefined :: port() | undefined,
                partner = undefined :: pid() | undefined,
                connection_alive
               }).

-define(LIVELINESS_UPDATE_INTERVAL, 1000).

-define(CONNECT_TIMEOUT, ?get_timeout(connect, 180000)).

-define(RECBUF, ?get_param(recbuf, 64 * 1024)).
-define(SNDBUF, ?get_param(sndbuf, 64 * 1024)).

init([Type, ConnName, Node, Bucket, ExtModule, InitArgs]) ->
    {ExtState, State} = ExtModule:init(
                          InitArgs,
                          #state{connect_info = {Type, ConnName, Node, Bucket},
                                 ext_module = ExtModule}),
    self() ! check_liveliness,
    {ok, State#state{
           ext_state = ExtState,
           connection_alive = false}}.

start_link(Type, ConnName, Node, Bucket, ExtModule, InitArgs) ->
    gen_server:start_link(?MODULE, [Type, ConnName, Node, Bucket, ExtModule, InitArgs], []).

get_socket(State) ->
    State#state.sock.

get_partner(State) ->
    State#state.partner.

get_conn_name(State) ->
    {_, ConnName, _, _} = State#state.connect_info,
    ConnName.

get_bucket(State) ->
    {_, _, _, Bucket} = State#state.connect_info,
    Bucket.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({setup_proxy, Partner, ProxyTo}, State) ->
    {noreply, State#state{proxy_to = ProxyTo, partner = Partner}};
handle_cast(Msg, State = #state{ext_module = ExtModule, ext_state = ExtState}) ->
    {noreply, NewExtState, NewState} = ExtModule:handle_cast(Msg, ExtState, State),
    {noreply, NewState#state{ext_state = NewExtState}}.

terminate(_Reason, _State) ->
    ok.

handle_info({tcp, Socket, Data}, #state{sock = Socket} = State) ->
    handle_info({socket_data, Socket, Data}, State);

handle_info({ssl, Socket, Data}, #state{sock = Socket} = State) ->
    handle_info({socket_data, Socket, Data}, State);

handle_info({socket_data, Socket, Data}, State) ->
    NewState = process_data(Data, State),

    %% Set up the socket to receive another message
    ok = network:socket_setopts(Socket, [{active, once}]),

    {noreply, NewState};

handle_info({tcp_closed, Socket}, State) ->
    handle_info({socket_closed, Socket}, State);

handle_info({ssl_closed, Socket}, State) ->
    handle_info({socket_closed, Socket}, State);

handle_info({socket_closed, Socket}, State) ->
    ?log_error("Socket ~p was closed. Closing myself. State = ~p",
               [Socket, State]),
    {stop, socket_closed, State};

handle_info({ssl_error, Socket, Error}, State) ->
    ?log_error("SSL error on socket ~p: ~p. Terminating. State:~n~p",
               [Socket, Error, State]),
    {stop, {ssl_error, Error}, State};

handle_info({'EXIT', _Pid, _Reason} = ExitSignal, State) ->
    ?log_error("killing myself due to exit signal: ~p", [ExitSignal]),
    {stop, {got_exit, ExitSignal}, State};

handle_info(check_liveliness, #state{connection_alive = false} = State) ->
    erlang:send_after(?LIVELINESS_UPDATE_INTERVAL, self(), check_liveliness),
    {noreply, State};
handle_info(check_liveliness,
            #state{connect_info = {_, _, Node, Bucket},
                   connection_alive = true} = State) ->
    %% NOTE: The following comment only applies to pre-OTP18 Erlang.
    %%
    %% We are not interested in the exact time of the last DCP traffic.
    %% We mainly want to know whether there was atleast one DCP message
    %% during the last LIVELINESS_UPDATE_INTERVAL.
    %% An approximate timestamp is good enough.
    %% erlang:now() can be bit expensive compared to os:timestamp().
    %% But, os:timestamp() may not be monotonic.
    %% Since this function gets called only every 1 second, should
    %% be ok to use erlang:now().
    %% Alternatively, we can also attach the timestamp in
    %% dcp_traffic_monitor:node_alive(). But, node_alive is an async operation
    %% so I prefer to attach the timestamp here.
    Now = erlang:monotonic_time(),
    dcp_traffic_monitor:node_alive(Node, {Bucket, Now, self()}),
    erlang:send_after(?LIVELINESS_UPDATE_INTERVAL, self(), check_liveliness),
    {noreply, State#state{connection_alive = false}};

handle_info(Msg, State) ->
    ?log_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call(get_socket, _From, State = #state{sock = Sock}) ->
    {reply, Sock, State};
handle_call(Command, From, State = #state{ext_module = ExtModule, ext_state = ExtState}) ->
    case ExtModule:handle_call(Command, From, ExtState, State) of
        {ReplyType, Reply, NewExtState, NewState} ->
            {ReplyType, Reply, NewState#state{ext_state = NewExtState}};
        {ReplyType, NewExtState, NewState} ->
            {ReplyType, NewState#state{ext_state = NewExtState}}
    end.

handle_packet(<<Magic:8, Opcode:8, _Rest/binary>> = Packet,
              State = #state{ext_module = ExtModule,
                             ext_state = ExtState},
              SendData) ->
    case (suppress_logging(Packet)
          orelse not ale:is_loglevel_enabled(?NS_SERVER_LOGGER, debug)) of
        true ->
            ok;
        false ->
            ?log_debug("Proxy packet: ~s", [dcp_commands:format_packet_nicely(Packet)])
    end,

    Type = case Magic of
               ?REQ_MAGIC ->
                   request;
               ?RES_MAGIC ->
                   response
           end,
    {Action, NewExtState, NewState} = ExtModule:handle_packet(
                                        Type, Opcode, Packet, ExtState, State),
    NewSendData = case Action of
                      proxy ->
                          <<SendData/binary, Packet/binary>>;
                      block ->
                          SendData
                  end,
    {ok,
     NewState#state{ext_state = NewExtState, connection_alive = true},
     NewSendData}.

suppress_logging(<<?REQ_MAGIC:8, ?DCP_MUTATION:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_DELETION:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_EXPIRATION:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_SNAPSHOT_MARKER, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_WINDOW_UPDATE, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_PREPARE:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_SEQNO_ACKNOWLEDGED:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_COMMIT:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_ABORT:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?RES_MAGIC:8, ?DCP_MUTATION:8, _KeyLen:16, _ExtLen:8,
                   _DataType:8, ?SUCCESS:16, _Rest/binary>>) ->
    true;
suppress_logging(<<?RES_MAGIC:8, ?DCP_DELETION:8, _KeyLen:16, _ExtLen:8,
                   _DataType:8, ?SUCCESS:16, _Rest/binary>>) ->
    true;
suppress_logging(<<?RES_MAGIC:8, ?DCP_SNAPSHOT_MARKER:8, _KeyLen:16, _ExtLen:8,
                   _DataType:8, ?SUCCESS:16, _Rest/binary>>) ->
    true;
%% TODO: remove this as soon as memcached stops sending these
suppress_logging(<<?RES_MAGIC:8, ?DCP_WINDOW_UPDATE, _KeyLen:16, _ExtLen:8,
                   _DataType:8, ?SUCCESS:16, _Rest/binary>>) ->
    true;
suppress_logging(<<_:8, ?DCP_NOP:8, _Rest/binary>>) ->
    true;
suppress_logging(<<?RES_MAGIC:8, ?DCP_SYSTEM_EVENT:8, _KeyLen:16, _ExtLen:8,
                   _DataType:8, ?SUCCESS:16, _Rest/binary>>) ->
    true;
suppress_logging(<<?REQ_MAGIC:8, ?DCP_SYSTEM_EVENT:8, _Rest/binary>>) ->
    true;
suppress_logging(_) ->
    false.

maybe_connect(#state{sock = undefined,
                     connect_info = {Type, ConnName, Node, Bucket}} = State,
              RepFeatures) ->
    Sock = connect(Type, ConnName, Node, Bucket, RepFeatures),

    %% setup socket to receive the first message
    ok = network:socket_setopts(Sock, [{active, once}]),

    State#state{sock = Sock};
maybe_connect(State, _) ->
    State.

connect(Type, ConnName, Node, Bucket) ->
    connect(Type, ConnName, Node, Bucket, []).

connect(Type, ConnName, Node, Bucket, RepFeatures) ->
    Cfg = ns_config:latest(),
    Username = ns_config:search_node_prop(Node, Cfg, memcached, admin_user),
    Password = ns_config:search_node_prop(Node, Cfg, memcached, admin_pass),

    {ok, Sock} = connect_inner(Cfg, Node, RepFeatures),
    ok = mc_client_binary:auth(Sock, {<<"PLAIN">>,
                                      {list_to_binary(Username),
                                       list_to_binary(Password)}}),
    ok = mc_client_binary:select_bucket(Sock, Bucket),

    %% Negotiate XAttr and Snappy features if they are to be enabled.
    negotiate_features(Sock, Type, ConnName, RepFeatures),

    ok = dcp_commands:open_connection(Sock, ConnName, Type, RepFeatures, Node),
    Sock.

connect_inner(Cfg, Node, RepFeatures) ->
    SOpts = [misc:get_net_family(), binary,
             {packet, raw}, {active, false}, {nodelay, true},
             {keepalive, true}, {recbuf, ?RECBUF},
             {sndbuf, ?SNDBUF}],

    {Host0, TcpPort, SslPort} = ns_memcached:host_ports(Node, Cfg),

    %% 'Node' can be different in situations where we are upgrading from pre-5.1
    %% clusters. In such situations, the orchestrator node will be proxying the
    %% takeover operation.
    Host = case Node =:= node() of
               true  -> misc:localhost();
               false -> Host0
           end,

    {Protocol, Opts, Port} =
        case proplists:get_bool(ssl, RepFeatures) andalso Node =/= node() of
            true ->
                {ssl, SOpts ++ ns_ssl_services_setup:ssl_client_opts(),
                 SslPort};
            false ->
                {tcp, SOpts, TcpPort}
        end,

    network:socket_connect(Protocol, Host, Port, Opts, ?CONNECT_TIMEOUT).

negotiate_features(Sock, Type, ConnName, Features) ->
    HelloFeatures = mc_client_binary:hello_features(Features),
    case do_negotiate_features(Sock, Type, ConnName, HelloFeatures) of
        ok ->
            ok;
        {error, FailedFeatures} ->
            case lists:member(snappy, FailedFeatures) of
                true ->
                    ?log_debug("Snappy negotiation failed for connection ~p:~p",
                               [ConnName, Type]);
                false ->
                    ok
            end,

            [] = FailedFeatures -- [snappy],
            ok
    end.

do_negotiate_features(_Sock, _Type, _ConnName, []) ->
    ok;
do_negotiate_features(Sock, Type, ConnName, Features) ->
    case mc_client_binary:hello(Sock, "proxy", Features) of
        {ok, Negotiated} ->
            case Features -- Negotiated of
                [] -> ok;
                Val -> {error, Val}
            end;
        Error ->
            ?log_debug("HELLO cmd failed for connection ~p:~p, features = ~p,"
                       "err = ~p", [ConnName, Type, Features, Error]),
            {error, Features}
    end.

disconnect(Sock) ->
    network:socket_close(Sock).

nuke_connection(Type, ConnName, Node, Bucket) ->
    ?log_debug("Nuke DCP connection ~p type ~p on node ~p", [ConnName, Type, Node]),
    disconnect(connect(Type, ConnName, Node, Bucket)).

connect_proxies(Pid1, Sock1, Pid2) ->
    Sock2 = gen_server:call(Pid2, get_socket, infinity),

    gen_server:cast(Pid1, {setup_proxy, Pid2, Sock2}),
    gen_server:cast(Pid2, {setup_proxy, Pid1, Sock1}),
    [{Pid1, Sock1}, {Pid2, Sock2}].

terminate_and_wait(Pairs, normal) ->
    misc:terminate_and_wait([Pid || {Pid, _} <- Pairs], normal);
terminate_and_wait(Pairs, shutdown) ->
    misc:terminate_and_wait([Pid || {Pid, _} <- Pairs], shutdown);
terminate_and_wait(Pairs, _Reason) ->
    [disconnect(Sock) || {_, Sock} <- Pairs],
    misc:terminate_and_wait([Pid || {Pid, _} <- Pairs], kill).

process_data(NewData, #state{buf = PrevData,
                             packet_len = PacketLen, proxy_to = ProxyTo} = State) ->
    Data = <<PrevData/binary, NewData/binary>>,
    {NewState, SendData} = process_data_loop(Data, PacketLen, State, <<>>),
    case SendData of
        <<>> ->
            ok;
        _ ->
            ok = network:socket_send(ProxyTo, SendData)
    end,
    NewState.

process_data_loop(Data, undefined, State, SendData) ->
    case Data of
        <<_Magic:8, _Opcode:8, _KeyLen:16, _ExtLen:8, _DataType:8,
          _VBucket:16, BodyLen:32, _Opaque:32, _CAS:64, _Rest/binary>> ->
            process_data_loop(Data, ?HEADER_LEN + BodyLen, State, SendData);
        _ ->
            {State#state{buf = Data, packet_len = undefined}, SendData}
    end;
process_data_loop(Data, PacketLen, State, SendData) ->
    case byte_size(Data) >= PacketLen of
        false ->
            {State#state{buf = Data, packet_len = PacketLen}, SendData};
        true ->
            {Packet, Rest} = split_binary(Data, PacketLen),
            {ok, NewState, NewSendData} = handle_packet(Packet, State, SendData),
            process_data_loop(Rest, undefined, NewState, NewSendData)
    end.


-ifdef(TEST).
negotiate_features_test() ->
    meck:new(mc_client_binary, [passthrough]),

    V = [xattr, snappy],
    meck:expect(mc_client_binary, hello,
                fun(_, _, _) -> {ok, V} end),
    ?assertEqual(ok, do_negotiate_features([], type, "xyz", V)),

    meck:expect(mc_client_binary, hello,
                fun(_, _, _) -> {ok, [snappy]} end),
    ?assertEqual({error, [xattr]},
                 do_negotiate_features([], type, "xyz", V)),
    ?assertEqual(ok, do_negotiate_features([], type, "xyz",
                                           [snappy])),

    meck:expect(mc_client_binary, hello,
                fun(_, _, _) -> {ok, [xattr]} end),
    ?assertEqual({error, [snappy]},
                 do_negotiate_features([], type, "xyz", V)),
    ?assertEqual(ok, do_negotiate_features([], type, "xyz",
                                           [xattr])),

    meck:expect(mc_client_binary, hello, fun(_, _, _) -> error end),
    ?assertEqual({error, V}, do_negotiate_features([], type, "xyz", V)),

    true = meck:validate(mc_client_binary),
    meck:unload(mc_client_binary).
-endif.
