%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(docs_kv_sup).

-behavior(supervisor).

-include("ns_common.hrl").

-export([start_link/1, init/1]).

start_link(Bucket) ->
    supervisor:start_link(?MODULE, [Bucket]).

init([BucketName]) ->
    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs(BucketName)}}.

child_specs(BucketName) ->
    [{wait_for_net_kernel,
      {remote_monitors, wait_for_net_kernel, []},
      transient, brutal_kill, worker, []},
     {doc_replicator,
      {capi_ddoc_manager, start_replicator, [BucketName]},
      permanent, 1000, worker, [doc_replicator]},
     {doc_replication_srv,
      {doc_replication_srv, start_link, [BucketName]},
      permanent, 1000, worker, [doc_replication_srv]},
     {capi_ddoc_manager_sup,
      {capi_ddoc_manager_sup, start_link_remote,
       [ns_node_disco:couchdb_node(), BucketName]},
      permanent, infinity, supervisor, [capi_ddoc_manager_sup]},
     {capi_set_view_manager,
      {capi_set_view_manager, start_link_remote,
       [ns_node_disco:couchdb_node(), BucketName]},
      permanent, 1000, worker, []},
     {couch_stats_reader,
      {couch_stats_reader, start_link_remote,
       [ns_node_disco:couchdb_node(), BucketName]},
      permanent, 1000, worker, []}].
