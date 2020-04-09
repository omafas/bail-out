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

-module(capi_ddoc_manager_sup).

-behaviour(supervisor).

-export([start_link_remote/2]).

-export([init/1]).

%% API
start_link_remote(Node, Bucket) ->
    ns_bucket_sup:ignore_if_not_couchbase_bucket(
      Bucket,
      fun (_BucketConfig) ->
              do_start_link_remote(Node, Bucket)
      end).

%% supervisor callback
init([Bucket, Replicator, ReplicationSrv]) ->
    Specs =
        [{capi_ddoc_manager_events,
          {capi_ddoc_manager, start_link_event_manager, [Bucket]},
          permanent, brutal_kill, worker, []},
         {capi_ddoc_manager,
          {capi_ddoc_manager, start_link, [Bucket, Replicator, ReplicationSrv]},
          permanent, 1000, worker, []}],

    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          Specs}}.

%% internal
server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

do_start_link_remote(Node, Bucket) ->
    Replicator = erlang:whereis(capi_ddoc_manager:replicator_name(Bucket)),
    ReplicationSrv = erlang:whereis(doc_replication_srv:proxy_server_name(Bucket)),

    true = is_pid(Replicator),
    true = is_pid(ReplicationSrv),

    %% This uses supervisor implementation detail. In reality supervisor is
    %% just a gen_server. So we can start it accordingly.
    SupName = {local, server(Bucket)},
    misc:start_link(Node, misc, turn_into_gen_server,
                    [SupName, supervisor,
                     {SupName, ?MODULE,
                      [Bucket, Replicator, ReplicationSrv]}, []]).
