%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-2017 Couchbase, Inc.
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
