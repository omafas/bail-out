%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
     {ns_config_rep_sup, {ns_config_rep_sup, start_link, []},
      permanent, infinity, supervisor, []}
    ].
