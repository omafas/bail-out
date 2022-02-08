%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc supervisor for ns_memcahced and the processes that have to be restarted
%%      if ns_memcached is restarted
%%

-module(ns_memcached_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

start_link(BucketName) ->
    supervisor:start_link(?MODULE, [BucketName]).

init([BucketName]) ->
    {ok, {{rest_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs(BucketName)}}.

child_specs(BucketName) ->
    [{{ns_memcached, BucketName}, {ns_memcached, start_link, [BucketName]},
      %% sometimes bucket deletion is slow. NOTE: we're not deleting
      %% bucket on system shutdown anymore
      permanent, 86400000, worker, [ns_memcached]},
     {{terse_bucket_info_uploader, BucketName},
      {terse_bucket_info_uploader, start_link, [BucketName]},
      permanent, 1000, worker, []}].
