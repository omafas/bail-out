%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc root supervisor for ns_server

-module(root_sup).

-behavior(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one, 10, 1},
          [{dist_manager, {dist_manager, start_link, []},
            permanent, 1000, worker, [dist_manager]},
           {ns_server_cluster_sup, {ns_server_cluster_sup, start_link, []},
            permanent, infinity, supervisor, [ns_server_cluster_sup]}]}}.
