%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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

-spec socket_setopts(ssl:sslsocket() | gen_tcp:socket(),
                     [gen_tcp:option()]) ->
                            ok | {error, term()}.
socket_setopts({sslsocket, _, _} = Sock, Opts) ->
    ssl:setopts(Sock, Opts);
socket_setopts(Sock, Opts) ->
    inet:setopts(Sock, Opts).

-spec socket_send(ssl:sslsocket() | gen_tcp:socket(),
                  term()) ->
                  ok | {error, term()}.
socket_send({sslsocket, _, _} = Socket, Data) ->
    ssl:send(Socket, Data);
socket_send(Socket, Data) ->
    prim_inet:send(Socket, Data).

-spec socket_recv(ssl:sslsocket() | gen_tcp:socket(),
                  integer(), timeout()) ->
                         {ok, binary() | list()} |
                         {error, term()}.
socket_recv({sslsocket, _, _} = Socket, NumBytes, Timeout) ->
    ssl:recv(Socket, NumBytes, Timeout);
socket_recv(Socket, NumBytes, Timeout) ->
    prim_inet:recv(Socket, NumBytes, Timeout).

-spec socket_close(Socket) -> term() when
      Socket :: ssl:sslsocket() | gen_tcp:socket().
socket_close({sslsocket, _, _} = Socket) ->
    ssl:close(Socket);
socket_close(Socket) ->
    gen_tcp:close(Socket).

-spec sockname(ssl:sslsocket() | gen_tcp:socket()) ->
    {ok,
        {inet:ip_address(), inet:port_number()} |
         inet:returned_non_ip_address()} |
        {error, inet:posix() | SslReason :: any()}.

sockname({ssl, Socket}) ->
    ssl:sockname(Socket);
sockname(Socket) ->
    inet:sockname(Socket).
