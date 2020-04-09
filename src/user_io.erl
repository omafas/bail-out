%% @author Couchbase <info@couchbase.com>
%% @copyright 2017 Couchbase, Inc.
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
%% @doc i/o server for user interface port.
%%      sends all output to debug log sometimes without much formatting
%%
-module(user_io).

-export([start/0]).

-include("ns_common.hrl").

start() ->
    proc_lib:start_link(erlang, apply, [fun user_io/0, []]).

user_io() ->
    erlang:register(user, self()),
    proc_lib:init_ack({ok, self()}),
    user_io_loop().

user_io_loop() ->
    receive
        {io_request, From, ReplyAs, Stuff} ->
            handle_user_io(From, Stuff),
            From ! {io_reply, ReplyAs, ok},
            user_io_loop()
    end.

handle_user_io(From, {put_chars, Encoding, Mod, Func, Args} = Stuff) ->
    Chars = erlang:apply(Mod, Func, Args),
    case catch unicode:characters_to_list(Chars, Encoding) of
        L when is_list(L) ->
            ?log_debug("put_chars~p: ~p", [From, L]);
        _ ->
            ?log_debug("~p: ~p", [From, Stuff])
    end;
handle_user_io(From, Stuff) ->
    ?log_debug("~p: ~p", [From, Stuff]).
