%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_config_rep_sup).

-behavior(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          get_child_specs()}}.

get_child_specs() ->
    [
     % merges incoming config changes.
     {ns_config_rep_merger, {ns_config_rep, start_link_merger, []},
      permanent, brutal_kill, worker, [ns_config_rep]},
     % replicates config across nodes.
     {ns_config_rep, {ns_config_rep, start_link, []},
      permanent, 1000, worker,
      [ns_config_rep]}
    ].
