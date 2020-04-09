%% @author Couchbase <info@couchbase.com>
%% @copyright 2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

%% @doc methods/wrappers related to networking.

-module(network).

-export([
         socket_connect/5,
         socket_setopts/2,
         socket_send/2,
         socket_recv/3,
         socket_close/1
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
