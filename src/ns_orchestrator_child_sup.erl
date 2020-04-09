%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
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
-module(ns_orchestrator_child_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 3, 10}, child_specs()}}.

child_specs() ->
    [{ns_janitor_server, {ns_janitor_server, start_link, []},
      permanent, 1000, worker, [ns_janitor_server]},
     {auto_reprovision, {auto_reprovision, start_link, []},
      permanent, 1000, worker, [auto_reprovision]},
     {auto_rebalance, {auto_rebalance, start_link, []},
      permanent, 1000, worker, [auto_rebalance]},
     {ns_orchestrator, {ns_orchestrator, start_link, []},
      permanent, 1000, worker, [ns_orchestrator]}].
