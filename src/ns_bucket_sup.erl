%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
