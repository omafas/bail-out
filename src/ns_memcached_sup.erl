%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2014 Couchbase, Inc.
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
