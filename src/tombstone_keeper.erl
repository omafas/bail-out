%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(tombstone_keeper).

-behavior(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).
-export([refresh/0, wipe/0, get/2]).

-export([init/1, handle_call/3, handle_cast/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

refresh() ->
    gen_server:call(?MODULE, refresh).

wipe() ->
    gen_server:call(?MODULE, wipe).

get(Key, Default) ->
    case ets:lookup(?MODULE, Key) of
        [{_, Value}] ->
            Value;
        [] ->
            Default
    end.

%% callbacks
init([]) ->
    _ = ets:new(?MODULE, [named_table, protected]),
    {ok, {}}.

handle_call(refresh, _From, State) ->
    ets:insert(?MODULE, tombstone_agent:refresh_timestamps()),
    {reply, ok, State};
handle_call(wipe, _From, State) ->
    ets:delete_all_objects(?MODULE),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_debug("Unexpected cast:~n~p", [Cast]),
    {noreply, State}.
