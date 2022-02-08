%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% Centralized time service

-module(ns_tick).

-behaviour(gen_server).

-include("ns_common.hrl").

-define(INTERVAL, 1000).
-define(SERVER, {via, leader_registry, ?MODULE}).

-export([start_link/0, time/0]).

-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1,
         terminate/2]).

-record(state, {tick_interval :: non_neg_integer(),
                time}).

%%
%% API
%%

start_link() ->
    misc:start_singleton(gen_server, ?MODULE, [], []).


time() ->
    gen_server:call(?SERVER, time).


%%
%% gen_server callbacks
%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


init([]) ->
    Interval = misc:get_env_default(tick_interval, ?INTERVAL),
    send_tick_msg(Interval),
    {ok, #state{tick_interval=Interval}}.


handle_call(time, _From, #state{time=Time} = State) ->
    {reply, Time, State}.


handle_cast(Msg, State) ->
    {stop, {unhandled, Msg}, State}.


%% Called once per second on the node where the gen_server runs
handle_info(tick, #state{tick_interval=Interval} = State) ->
    send_tick_msg(Interval),
    Now = os:system_time(millisecond),
    ns_tick_agent:send_tick(ns_node_disco:nodes_actual(), Now),

    {noreply, State#state{time=Now}};
handle_info(_, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

send_tick_msg(Interval) ->
    erlang:send_after(Interval, self(), tick).
