%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

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
