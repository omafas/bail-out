%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc methods/wrappers related to networking.

-module(network).

-export([
         socket_connect/5,
         socket_setopts/2,
         socket_send/2,
         socket_recv/3,
         socket_close/1,
         sockname/1
        ]).

-spec socket_connect(tcp | ssl,
                     inet:socket_address() | inet:hostname(),
                     inet:port_number(),
                     [gen_tcp:connect_option()],
                     timeout()) ->
                            {ok, gen_tcp:socket() | ssl:sslsocket()} |
                            {error, term()}.
socket_connect(ssl, Host, Port, Opts, Timeout) ->
    Addr =
        case inet:parse_address(Host) of
            {ok, A} -> A;
            {error, einval} -> Host
        end,
    ssl:connect(Addr, Port, Opts, Timeout);
socket_connect(tcp, Host, Port, Opts, Timeout) ->
    gen_tcp:connect(Host, Port, Opts, Timeout).

-spec socket_setopts(gen_tcp:socket() | ssl:sslsocket(),
                     [gen_tcp:option()]) ->
                            ok | {error, term()}.
socket_setopts(Sock, Opts) when is_port(Sock)->
    inet:setopts(Sock, Opts);
socket_setopts(Sock, Opts) ->
    ssl:setopts(Sock, Opts).

-spec socket_send(gen_tcp:socket() | ssl:sslsocket(),
                  term()) ->
                  ok | {error, term()}.
socket_send(Socket, Data) when is_port(Socket) ->
    prim_inet:send(Socket, Data);
socket_send(Socket, Data) ->
    ssl:send(Socket, Data).

-spec socket_recv(gen_tcp:socket() | ssl:sslsocket(),
                  integer(), timeout()) ->
                         {ok, binary() | list()} |
                         {error, term()}.
socket_recv(Socket, NumBytes, Timeout) when is_port(Socket) ->
    prim_inet:recv(Socket, NumBytes, Timeout);
socket_recv(Socket, NumBytes, Timeout) ->
    ssl:recv(Socket, NumBytes, Timeout).

-spec socket_close(Socket) -> term() when
      Socket :: gen_tcp:socket() | ssl:sslsocket().
socket_close(Socket) when is_port(Socket) ->
    gen_tcp:close(Socket);
socket_close(Socket) ->
    ssl:close(Socket).

-spec sockname(gen_tcp:socket() | {ssl, ssl:sslsocket()}) ->
    {ok,
        {inet:ip_address(), inet:port_number()} |
         inet:returned_non_ip_address()} |
        {error, inet:posix() | SslReason :: any()}.
sockname(Socket) when is_port(Socket) ->
    inet:sockname(Socket);
sockname({ssl, Socket}) ->
    ssl:sockname(Socket).
