%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2016 Couchbase, Inc.
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
-module(ns_config_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

-include("ns_common.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    CfgPath = path_config:component_path(etc, "config"),
    % TODO: we'll likely kill that later, because static config is usually empty
    ?log_info("loading static ns_config from ~p", [CfgPath]),
    {ok, {{rest_for_one, 3, 10},
          [
           %% gen_event for the config events.
           {ns_config_events,
            {gen_event, start_link, [{local, ns_config_events}]},
            permanent, 1000, worker, []},

           {ns_config_events_local,
            {gen_event, start_link, [{local, ns_config_events_local}]},
            permanent, brutal_kill, worker, []},

           %% current local state.
           {ns_config,
            {ns_config, start_link, [CfgPath, ns_config_default]},
            permanent, 1000, worker, [ns_config, ns_config_default]},

           {ns_config_remote,
            {ns_config_replica, start_link, []},
            permanent, 1000, worker, [ns_config, ns_config_replica]},

           %% logs config changes for debugging.
           {ns_config_log,
            {ns_config_log, start_link, []},
            permanent, 1000, worker, []}
          ]}}.
