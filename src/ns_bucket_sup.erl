%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2017 Couchbase, Inc.
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
%% Run a set of processes per bucket

-module(ns_bucket_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([ignore_if_not_couchbase_bucket/2]).

%% used by ns_bucket_worker
-export([start_bucket/1, stop_bucket/1]).

%% supervisor callback
-export([init/1]).


%% API
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_bucket(Bucket) ->
    case supervisor:start_child(?MODULE, bucket_spec(Bucket)) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

stop_bucket(Bucket) ->
    Id = bucket_spec_id(Bucket),
    case supervisor:terminate_child(?MODULE, Id) of
        ok ->
            supervisor:delete_child(?MODULE, Id);
        Error ->
            Error
    end.

ignore_if_not_couchbase_bucket(BucketName, Body) ->
    case ns_bucket:get_bucket(BucketName) of
        not_present ->
            ignore;
        {ok, BucketConfig} ->
            case proplists:get_value(type, BucketConfig) of
                memcached ->
                    ignore;
                _ ->
                    Body(BucketConfig)
            end
    end.

%% supervisor callbacks
init([]) ->
    {ok, {{one_for_one, 3, 10}, []}}.

%% Internal functions
bucket_spec(Bucket) ->
    {bucket_spec_id(Bucket),
     {single_bucket_kv_sup, start_link, [Bucket]},
     permanent, infinity, supervisor,
     [single_bucket_kv_sup]}.

bucket_spec_id(Bucket) ->
    {single_bucket_kv_sup, Bucket}.
