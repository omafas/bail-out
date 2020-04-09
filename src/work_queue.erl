%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2018 Couchbase, Inc.
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
-module(work_queue).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1, start_link/2,
         submit_work/2, submit_sync_work/2, sync_work/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    start_link(fun nothing/0).

start_link(Name) when is_atom(Name) orelse is_tuple(Name) ->
    start_link(Name, fun nothing/0);
start_link(InitFun) when is_function(InitFun) ->
    gen_server:start_link(?MODULE, InitFun, []).

start_link(Name, InitFun) when is_atom(Name) ->
    start_link({local, Name}, InitFun);
start_link(Name, InitFun) when is_tuple(Name) ->
    gen_server:start_link(Name, ?MODULE, InitFun, []).

submit_work(Name, Fun) ->
    gen_server:cast(Name, Fun).

submit_sync_work(Name, Fun) ->
    gen_server:call(Name, Fun, infinity).

sync_work(Name) ->
    gen_server:call(Name, fun nothing/0).

nothing() -> [].

init(InitFun) ->
    InitFun(),
    {ok, []}.

handle_call(Fun, _From, State) ->
    RV = Fun(),
    {reply, RV, State, hibernate}.

handle_cast(Fun, State) ->
    Fun(),
    {noreply, State, hibernate}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
