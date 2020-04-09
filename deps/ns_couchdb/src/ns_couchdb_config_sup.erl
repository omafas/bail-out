%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2017 Couchbase, Inc.
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
%% @doc supervises config related processes on ns_couchdb node
%%
-module(ns_couchdb_config_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one, 3, 10},
          [
           {ns_config_events,
            {gen_event, start_link, [{local, ns_config_events}]},
            permanent, 1000, worker, []},

           {ns_config_events_local,
            {gen_event, start_link, [{local, ns_config_events_local}]},
            permanent, brutal_kill, worker, []},

           {ns_config,
            {ns_config, start_link, [{pull_from_node, ns_node_disco:ns_server_node()}]},
            permanent, 1000, worker, [ns_config, ns_config_default]},

           {ns_couchdb_config_rep,
            {ns_couchdb_config_rep, start_link, []},
            permanent, 1000, worker, []},

           {cb_config_couch_sync,
            {cb_config_couch_sync, start_link, []},
            permanent, 1000, worker, []},

           {ns_config_log,
            {ns_config_log, start_link, []},
            permanent, 1000, worker, []}
          ]}}.
