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
-module(mb_master_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([start_link/0]).

-export([init/1]).


start_link() ->
    master_activity_events:note_became_master(),
    supervisor:start_link({local, mb_master_sup}, ?MODULE, []).


init([]) ->
    {ok, {{one_for_one, 3, 10}, child_specs()}}.


%%
%% Internal functions
%%

%% @private
%% @doc The list of child specs.
child_specs() ->
    [{leader_lease_acquirer,
      {leader_lease_acquirer, start_link, []},
      permanent, 10000, worker, []},
     {leader_quorum_nodes_manager,
      {leader_quorum_nodes_manager, start_link, []},
      permanent, 1000, worker, []},
     {ns_tick, {ns_tick, start_link, []},
      permanent, 10, worker, [ns_tick]},
     {ns_orchestrator_sup, {ns_orchestrator_sup, start_link, []},
      permanent, infinity, supervisor, [ns_orchestrator_sup]},
     {collections, {collections, start_link, []},
      permanent, 1000, worker, [collections]}] ++
     [{license_reporting, {license_reporting, start_link, []},
       permanent, 1000, worker, []} || cluster_compat_mode:is_enterprise()].
