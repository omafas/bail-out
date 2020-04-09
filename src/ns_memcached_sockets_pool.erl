%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2018 Couchbase, Inc.
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
%% @doc pool of sockets that are used for long blocking memcached calls
%%
-module(ns_memcached_sockets_pool).

-include("ns_common.hrl").

-export([start_link/0]).

-export([executing_on_socket/1, executing_on_socket/2, executing_on_socket/3]).

start_link() ->
    Options = [{name, ?MODULE},
               {connection_timeout, 30000},
               {pool_size_per_dest, 10000}],
    ns_connection_pool:start_link(Options).

take_socket(Options) ->
    Destination = get_destination(Options),
    case ns_connection_pool:maybe_take_socket(?MODULE, Destination) of
        {ok, Sock} ->
            {ok, Sock};
        no_socket ->
            ns_memcached:connect(Options)
    end.

take_socket(undefined, Options) ->
    take_socket(Options);
take_socket(Bucket, Options) ->
    case take_socket(Options) of
        {ok, Socket} ->
            case mc_client_binary:select_bucket(Socket, Bucket) of
                ok ->
                    {ok, Socket};
                Err ->
                    {error, {select_bucket_failed, Err}}
            end;
        Error ->
            Error
    end.

put_socket(undefined, Socket, Options) ->
    put_socket(Socket, Options);
put_socket(Bucket, Socket, Options) ->
    case (catch mc_client_binary:deselect_bucket(Socket)) of
        ok ->
            put_socket(Socket, Options);
        Error ->
            %% If deselect on the socket fails, then remove the socket from
            %% the connection pool. The attempt here is to avoid bubbling the
            %% exception back to the caller of 'executing_on_socket'.
            ?log_debug("Bucket deselect on socket ~p failed. Bucket = ~p, "
                       "Error = ~p", [Socket, Bucket, Error]),
            remove_socket(Socket, Options)
    end.

put_socket(Socket, Options) ->
    Destination = get_destination(Options),
    ns_connection_pool:put_socket(?MODULE, Destination, Socket).

remove_socket(Socket, Options) ->
    Destination = get_destination(Options),
    ns_connection_pool:remove_socket(?MODULE, Destination, Socket).

executing_on_socket(Fun) ->
    executing_on_socket(Fun, undefined).

executing_on_socket(Fun, Bucket) ->
    executing_on_socket(Fun, Bucket, []).

executing_on_socket(Fun, Bucket, Options) ->
    misc:executing_on_new_process(
      fun () ->
              case take_socket(Bucket, Options) of
                  {ok, Sock} ->
                      {ok, SockName} = inet:sockname(Sock),
                      try
                          Result = Fun(Sock),
                          put_socket(Bucket, Sock, Options),
                          Result
                      catch T:E ->
                              Stack = erlang:get_stacktrace(),
                              ?log_debug("Exception while executing on socket "
                                         "~p: ~p~n", [SockName, {T, E, Stack}]),
                              erlang:raise(T, E, Stack)
                      end;
                  Error ->
                      Error
              end
      end).

get_destination(Options) ->
    {ns_memcached, misc:canonical_proplist(Options)}.
