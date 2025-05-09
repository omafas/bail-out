%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% A gen_server that acts as a DCP endpoint. Used with fake_dcp_server, this
%% server will listen on either end of the DCP connection like memcached's
%% DCPProducer or DCPConsumer. We don't make any distinction between the two
%% here because:
%%     1) We only have to handle a handful of packets
%%     2) We don't need to make any type distinction to do that handling
-module(fake_dcp_endpoint).

-include("mc_constants.hrl").
-include("mc_entry.hrl").
-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(fake_dcp_endpoint_state,
        {server :: pid(),
         socket :: gen_tcp:socket(),
         debug_logging :: boolean(),
         buf = <<>> :: binary(),
         packet_len = undefined}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(ListenSocket, DebugLogging) ->
    Server = self(),
    %% Unnamed, no great way to name these and we may have a few during tests.
    gen_server:start_link(?MODULE, [Server, ListenSocket, DebugLogging], []).

init([Server, ListenSocket, DebugLogging]) ->
    %% We need to listen in the main server loop, so do that via continuation.
    {ok, #fake_dcp_endpoint_state{server = Server,
                                  debug_logging = DebugLogging},
     {continue, {listen, ListenSocket}}}.

handle_call(_Request, _From, State = #fake_dcp_endpoint_state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #fake_dcp_endpoint_state{}) ->
    {noreply, State}.

handle_continue({listen, ListenSocket},
                #fake_dcp_endpoint_state{server = Server} = State) ->
    {ok, Sock} = gen_tcp:accept(ListenSocket, infinity),
    Server ! {listening, self()},
    {noreply, State#fake_dcp_endpoint_state{socket = Sock}}.

handle_info({tcp, Socket, Data},
            #fake_dcp_endpoint_state{socket = Socket} = State) ->
    NewState = process_data(Data, State),

    %% Set up the socket to receive another message
    ok = network:socket_setopts(Socket, [{active, once}]),

    {noreply, NewState};
handle_info({tcp_closed, Socket},
            #fake_dcp_endpoint_state{socket = Socket} = State) ->
    {stop, socket_closed, State}.

terminate(_Reason, _State = #fake_dcp_endpoint_state{}) ->
    ok.

code_change(_OldVsn, State = #fake_dcp_endpoint_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_log(Msg, #fake_dcp_endpoint_state{debug_logging = true}) ->
    ?log_debug(Msg);
maybe_log(_, _) ->
    ok.

send_response(Response, #fake_dcp_endpoint_state{socket = Socket}) ->
    gen_tcp:send(Socket, Response).

handle_packet(<<?REQ_MAGIC:8, ?CMD_HELLO:8, _Bin/binary>>, State) ->
    maybe_log("Handling hello", State),

    %% We will enable every feature.
    AllowedFeatures = mc_client_binary:hello_features_map(),
    Features = [<<V:16>> || {_F, V} <- AllowedFeatures],

    Response = mc_binary:encode(res, #mc_header{
                                        opcode = ?CMD_HELLO,
                                        status = ?SUCCESS},
                                #mc_entry{data = list_to_binary(Features)}),
    send_response(Response, State);
handle_packet(<<?REQ_MAGIC:8, ?DCP_ADD_STREAM:8, _FrameInfoEncodedLen:8,
                _KeyLey:8, _ExtLen:8, _DataType:8, VBOrStatus:16, _BodyLen:32,
                Opaque:32, Bin/binary>>, State) ->
    Msg = io_lib:format("Handling add stream bin ~p vb ~p opaque ~p",
                        [Bin, VBOrStatus, Opaque]),
    maybe_log(Msg, State),

    Response = mc_binary:encode(res, #mc_header{
                                        opcode = ?DCP_ADD_STREAM,
                                        status = ?SUCCESS,
                                        vbucket = VBOrStatus,
                                        opaque = Opaque},
                                #mc_entry{}),
    send_response(Response, State);
handle_packet(<<?REQ_MAGIC:8, ?DCP_CLOSE_STREAM:8, _FrameInfoEncodedLen:8,
                _KeyLey:8, _ExtLen:8, _DataType:8, VBOrStatus:16, _BodyLen:32,
                Opaque:32, Bin/binary>>, State) ->
    Msg = io_lib:format("Handling close stream bin ~p vb ~p op ~p",
                        [Bin, VBOrStatus, Opaque]),
    maybe_log(Msg, State),

    Response = mc_binary:encode(res, #mc_header{
                                        opcode = ?DCP_CLOSE_STREAM,
                                        status = ?SUCCESS,
                                        vbucket = VBOrStatus,
                                        opaque = Opaque},
                                #mc_entry{}),
    send_response(Response, State);
%% Anything non-specific
handle_packet(<<?REQ_MAGIC:8, Opcode:8, _Bin/binary>>, State) ->
    Msg = io_lib:format("Got command for opcode ~p", [Opcode]),
    maybe_log(Msg, State),

    Response = mc_binary:encode(res, #mc_header{
                                        opcode = Opcode,
                                        status = ?SUCCESS},
                                #mc_entry{}),
    send_response(Response, State).

process_data(NewData,
             #fake_dcp_endpoint_state{buf = PrevData,
                                      packet_len = PacketLen} = State) ->
    Data = <<PrevData/binary, NewData/binary>>,
    process_data_loop(Data, PacketLen, State).

process_data_loop(Data, undefined, State) ->
    case Data of
        <<_Magic:8, _Opcode:8, _KeyLen:16, _ExtLen:8, _DataType:8,
            _VBucket:16, BodyLen:32, _Opaque:32, _CAS:64, _Rest/binary>> ->
            process_data_loop(Data, ?HEADER_LEN + BodyLen, State);
        _ ->
            State#fake_dcp_endpoint_state{buf = Data, packet_len = undefined}
    end;
process_data_loop(Data, PacketLen, State) ->
    case byte_size(Data) >= PacketLen of
        false ->
            {State#fake_dcp_endpoint_state{buf = Data, packet_len = PacketLen}};
        true ->
            {Packet, Rest} = split_binary(Data, PacketLen),
            handle_packet(Packet, State),
            process_data_loop(Rest, undefined, State)
    end.
