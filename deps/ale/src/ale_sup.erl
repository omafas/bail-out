%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2014 Couchbase, Inc.
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

-module(ale_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_all, 5, 10},
           [{ale_dymamic_sup,
             {ale_dynamic_sup, start_link, []},
             permanent, 5000, supervisor, [ale_dynamic_sup]},
            {ale_stats_events,
             {gen_event, start_link, [{local, ale_stats_events}]},
             permanent, 1000, worker, []},
            {ale, {ale, start_link, []}, permanent, 5000, worker, [ale]}]}}.
