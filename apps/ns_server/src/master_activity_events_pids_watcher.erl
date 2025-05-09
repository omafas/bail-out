%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(master_activity_events_pids_watcher).

-behaviour(gen_server).

%% API
-export([start_link/0, observe_fate_of/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

observe_fate_of(Pid, EventTuple) ->
    try gen_server:call(?MODULE, {observe, Pid, EventTuple})
    catch T:E ->
            ?log_debug("Failed to monitor fate of ~p for master events: ~p", [Pid, {T,E}])
    end.

init(_) ->
    ets:new(mref2event, [private, named_table]),
    {ok, []}.

handle_cast(_, _State) ->
    exit(unexpected).

handle_call({observe, Pid, EventTuple}, _From, _) ->
    MRef = erlang:monitor(process, Pid),
    ets:insert(mref2event, {MRef, EventTuple}),
    {reply, ok, []}.

handle_info({'DOWN', MRef, process, Pid, Reason}, _) ->
    [{MRef, EventTuple}] = ets:lookup(mref2event, MRef),
    ets:delete(mref2event, MRef),
    master_activity_events:note_observed_death(Pid, Reason, EventTuple),
    {noreply, []}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
