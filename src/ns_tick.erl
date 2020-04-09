%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2018 Couchbase, Inc.
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
%% Centralized time service

-module(ns_tick).

-behaviour(gen_server).

-define(INTERVAL, 1000).
-define(SERVER, {via, leader_registry, ?MODULE}).

-export([start_link/0, time/0]).

-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1,
         terminate/2]).

-record(state, {time}).

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
    timer2:send_interval(Interval, tick),
    {ok, #state{}}.


handle_call(time, _From, #state{time=Time} = State) ->
    {reply, Time, State}.


handle_cast(Msg, State) ->
    {stop, {unhandled, Msg}, State}.


%% Called once per second on the node where the gen_server runs
handle_info(tick, State) ->
    Now = os:system_time(millisecond),
    ns_tick_agent:send_tick(ns_node_disco:nodes_actual(), Now),

    {noreply, State#state{time=Now}};
handle_info(_, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.
