%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(tombstone_purger).

-behavior(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).
-export([purge_now/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, {via, leader_registry, ?MODULE}).

-define(PURGE_AGE, ?get_param(purge_age, 5 * 60)).
-define(CHECK_INTERVAL, ?get_param(check_interval, 60000)).

-define(TIMEOUT, ?get_timeout(timeout, 15000)).

start_link() ->
    misc:start_singleton(gen_server, ?MODULE, [], []).

purge_now(Age) ->
    gen_server:call(?SERVER, {purge_now, Age}, ?TIMEOUT).

%% callbacks
init([]) ->
    schedule_timer(),
    {ok, []}.

handle_call({purge_now, Age}, _From, State) ->
    Result = tombstone_agent:purge_cluster(Age),
    {reply, Result, State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_debug("Unexpected cast:~n~p", [Cast]),
    {noreply, State}.

handle_info(check, State) ->
    check(),
    {noreply, State};
handle_info(Msg, State) ->
    ?log_debug("Unexpected message:~n~p", [Msg]),
    {noreply, State}.

%% internal
schedule_timer() ->
    erlang:send_after(?CHECK_INTERVAL, self(), check).

check() ->
    case ns_config:read_key_fast(tombstone_purger_enabled, true) of
        true ->
            case tombstone_agent:purge_cluster(?PURGE_AGE) of
                ok ->
                    ok;
                Error ->
                    ?log_warning("Tombstone purge failed ~w", [Error])
            end;
        false ->
            ok
    end,

    schedule_timer().
