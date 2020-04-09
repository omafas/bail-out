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
-module(ns_orchestrator_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one, 3, 10}, child_specs()}}.

child_specs() ->
    [{compat_mode_events,
      {gen_event, start_link, [{local, compat_mode_events}]},
      permanent, 1000, worker, dynamic},
     {compat_mode_manager,
      {compat_mode_manager, start_link, []},
      permanent, 1000, worker, [compat_mode_manager]},
     {ns_orchestrator_child_sup, {ns_orchestrator_child_sup, start_link, []},
      permanent, infinity, supervisor, [ns_orchestrator_child_sup]},
     {auto_failover, {auto_failover, start_link, []},
      permanent, 1000, worker, [auto_failover]}].
