%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2012 Couchbase, Inc.
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
