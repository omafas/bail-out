%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
           {tombstone_keeper,
            {tombstone_keeper, start_link, []},
            permanent, 1000, worker, []},

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
