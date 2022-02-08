%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
