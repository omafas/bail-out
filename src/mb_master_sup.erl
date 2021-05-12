%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
     {chronicle_master, {chronicle_master, start_link, []},
      permanent, 1000, worker, [chronicle_master]},
     {ns_orchestrator_sup, {ns_orchestrator_sup, start_link, []},
      permanent, infinity, supervisor, [ns_orchestrator_sup]},
     {tombstone_purger, {tombstone_purger, start_link, []},
      permanent, 1000, worker, []}] ++
        [{license_reporting, {license_reporting, start_link, []},
          permanent, 1000, worker, []} || cluster_compat_mode:is_enterprise()].
