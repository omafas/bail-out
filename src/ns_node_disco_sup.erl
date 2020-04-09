%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2012 Couchbase, Inc.
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
-module(ns_node_disco_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          get_child_specs()}}.

get_child_specs() ->
    [
     % gen_event for the node disco events.
     {ns_node_disco_events,
      {gen_event, start_link, [{local, ns_node_disco_events}]},
      permanent, 1000, worker, []},
     % manages node discovery and health.
     {ns_node_disco,
      {ns_node_disco, start_link, []},
      permanent, 1000, worker, []},
     % logs node disco events for debugging.
     {ns_node_disco_log,
      {ns_node_disco_log, start_link, []},
      permanent, 1000, worker, []},
     % listens for ns_config events relevant to node_disco.
     {ns_node_disco_conf_events,
      {ns_node_disco_conf_events, start_link, []},
      permanent, 1000, worker, []},
     % merges incoming config changes.
     {ns_config_rep_merger, {ns_config_rep, start_link_merger, []},
      permanent, brutal_kill, worker, [ns_config_rep]},
     % replicates config across nodes.
     {ns_config_rep, {ns_config_rep, start_link, []},
      permanent, 1000, worker,
      [ns_config_rep]}
    ].
