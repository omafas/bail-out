%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(master_activity_events_keeper).

-behaviour(gen_server).

%% API
-export([start_link/0, get_history/0, get_history_raw/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-define(HIBERNATE_TIMEOUT,   ?get_timeout(hibernate, 1000)).
-define(EVENTS_HISTORY_SIZE, ?get_param(history_size, 81920)).

-record(state, {ring}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_history() ->
    [binary_to_term(B) || B <- gen_server:call(?MODULE, get_history)].

get_history_raw() ->
    gen_server:call(?MODULE, get_history).

init(_) ->
    Self = self(),
    ns_pubsub:subscribe_link(master_activity_events,
                             fun (Event, _Ignored) ->
                                     gen_server:cast(Self, {note, Event})
                             end, []),
    {ok, #state{ring=ringbuffer:new(?EVENTS_HISTORY_SIZE)}}.

handle_call(get_history, _From, State) ->
    {reply, ringbuffer:to_list(State#state.ring), State, ?HIBERNATE_TIMEOUT}.

handle_cast({note, Event}, #state{ring = Ring} = State) ->
    NewState = State#state{ring = ringbuffer:add(term_to_binary(Event), Ring)},
    {noreply, NewState, ?HIBERNATE_TIMEOUT}.

handle_info(timeout, State) ->
    {noreply, State, hibernate};
handle_info(_Info, _State) ->
    exit(unexpected).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
