%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(single_bucket_kv_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([start_link/1, init/1]).
-export([sync_config_to_couchdb_node/0]).


start_link(BucketName) ->
    Name = list_to_atom(atom_to_list(?MODULE) ++ "-" ++ BucketName),
    supervisor:start_link({local, Name}, ?MODULE, [BucketName]).

child_specs(BucketName) ->
    [
     %% Since config replication (even to local nodes) is asynchronous, it's
     %% possible that when we try to start processes on couchdb node, it
     %% hasn't seen the config for the bucket yet. Depending on a particular
     %% process, it might or might not result in failure. So we explicitly
     %% synchronize config to couchdb node here.
     {sync_config_to_couchdb_node,
      {single_bucket_kv_sup, sync_config_to_couchdb_node, []},
      transient, brutal_kill, worker, []},
     {{docs_kv_sup, BucketName}, {docs_kv_sup, start_link, [BucketName]},
      permanent, infinity, supervisor, [docs_kv_sup]},
     {{ns_memcached_sup, BucketName},
      {ns_memcached_sup, start_link, [BucketName]},
      permanent, infinity, supervisor, [ns_memcached_sup]},
     {{dcp_sup, BucketName}, {dcp_sup, start_link, [BucketName]},
      permanent, infinity, supervisor, [dcp_sup]},
     {{dcp_replication_manager, BucketName},
      {dcp_replication_manager, start_link, [BucketName]},
      permanent, 1000, worker, []},
     {{replication_manager, BucketName},
      {replication_manager, start_link, [BucketName]},
      permanent, 1000, worker, []},
     {{janitor_agent_sup, BucketName},
      {janitor_agent_sup, start_link, [BucketName]},
      permanent, infinity, supervisor, [janitor_agent_sup]},
     {{stats_reader, BucketName}, {stats_reader, start_link, [BucketName]},
      permanent, 1000, worker, [stats_reader]},
     {{goxdcr_stats_reader, BucketName},
      {stats_reader, start_link, ["@xdcr-" ++ BucketName]},
      permanent, 1000, worker, [stats_reader]}
    ].

init([BucketName]) ->
    {ok, {{one_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs(BucketName)}}.

sync_config_to_couchdb_node() ->
    ?log_debug("Syncing config to couchdb node"),

    remote_monitors:wait_for_net_kernel(),
    CouchDBNode = ns_node_disco:couchdb_node(),
    case ns_config_rep:ensure_config_seen_by_nodes([CouchDBNode]) of
        ok ->
            ?log_debug("Synced config to couchdb node successfully"),
            ignore;
        {error, Error} ->
            {error, {couchdb_config_sync_failed, Error}}
    end.
