%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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

           {chronicle_events,
            {gen_event, start_link, [{local, chronicle_kv:event_manager(kv)}]},
            permanent, 1000, worker, []},

           {chronicle_compat_events, {chronicle_compat_events, start_link, []},
            permanent, 5000, worker, [chronicle_compat_events]},

           {ns_couchdb_chronicle_dup,
            {ns_couchdb_chronicle_dup, start_link, []},
            permanent, 1000, worker, []},

           {cb_config_couch_sync,
            {cb_config_couch_sync, start_link, []},
            permanent, 1000, worker, []},

           {ns_config_log,
            {ns_config_log, start_link, []},
            permanent, 1000, worker, []}
          ]}}.
