%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc This service maintains public ETS table that's caching node to
%% active vbuckets mapping.
%%
%% Implementation is using ns_config_events subscription for cache
%% invalidation and dedicated worker.
%%
%% NOTE: that while public ETS table could in principle be updated
%% independently, we're not doing that. Instead any ETS table mutation
%% is done on worker. This is because otherwise it would be possible
%% for cache invalidation to be 'overtaken' by cache update that used
%% vbucket map prior to cache invalidation event.
%%
%% Here's how I think correctness of this approach can be proved.
%% Lets assume that cache has stale information. That means cache
%% invalidation event was either lost (should not be possible) or it
%% caused cache invalidation prior to cache update. So lets assume
%% cache update happened after cache invalidation request was
%% performed. But that implies that cache update could not see old
%% vbucket map (i.e. one that preceded cache invalidation), because at
%% the moment of cache invalidation request was made new vbucket map
%% was already set in config. That gives us contradiction which
%% implies 'badness' cannot happen.
-module(vbucket_map_mirror).
-include("ns_common.hrl").

-export([start_link/0,
         must_node_vbuckets_dict/1,
         node_vbuckets_dict/1]).

-export([init/1, handle_call/3, handle_info/2]).

-behaviour(gen_server2).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = self(),
    ets:new(?MODULE, [set, named_table]),
    chronicle_compat_events:subscribe(
      fun (cluster_compat_version) ->
              Self ! invalidate_buckets;
          (buckets) ->
              Self ! invalidate_buckets;
          (Key) ->
              case ns_bucket:sub_key_match(Key) of
                  {true, Bucket, props} ->
                      Self ! {invalidate_bucket, Bucket};
                  _ ->
                      ok
              end
      end),
    {ok, []}.

handle_info(invalidate_buckets, CurrentBuckets) ->
    NewBuckets = lists:sort(ns_bucket:get_buckets()),
    ToClean = ordsets:subtract(CurrentBuckets, NewBuckets),
    BucketNames  = [Name || {Name, _} <- ToClean],
    [ets:delete(?MODULE, Name) || Name <- BucketNames],
    {noreply, NewBuckets};
handle_info({invalidate_bucket, Bucket}, CurrentBuckets) ->
    ets:delete(?MODULE, Bucket),
    {noreply, lists:keydelete(Bucket, 1, CurrentBuckets)}.

handle_call({compute_map, BucketName}, _From, State) ->
    RV =
        case ets:lookup(?MODULE, BucketName) of
            [] ->
                case ns_bucket:get_bucket(BucketName) of
                    {ok, BucketConfig} ->
                        case proplists:get_value(map, BucketConfig) of
                            undefined ->
                                {error, no_map};
                            Map ->
                                NodeToVBuckets =
                                    compute_map_to_vbuckets_dict(Map),
                                ets:insert(?MODULE,
                                           {BucketName, NodeToVBuckets}),
                                {ok, NodeToVBuckets}
                        end;
                    not_present ->
                        {error, not_present}
                end;
            [{_, Dict}] ->
                {ok, Dict}
        end,
    {reply, RV, State}.

compute_map_to_vbuckets_dict(Map) ->
    {_, NodeToVBuckets0} =
        lists:foldl(fun ([undefined | _], {Idx, Dict}) ->
                            {Idx + 1, Dict};
                        ([Master | _], {Idx, Dict}) ->
                            {Idx + 1,
                             dict:update(Master,
                                         fun (Vbs) ->
                                                 [Idx | Vbs]
                                         end, [Idx], Dict)}
                    end, {0, dict:new()}, Map),
    dict:map(fun (_Key, Vbs) ->
                     lists:reverse(Vbs)
             end, NodeToVBuckets0).

-spec node_vbuckets_dict(bucket_name()) ->
                                {ok, dict:dict()} |
                                {error, no_map | not_present}.
node_vbuckets_dict(BucketName) ->
    case ets:lookup(?MODULE, BucketName) of
        [] ->
            gen_server2:call(?MODULE, {compute_map, BucketName});
        [{_, Dict}] ->
            {ok, Dict}
    end.

must_node_vbuckets_dict(BucketName) ->
    case node_vbuckets_dict(BucketName) of
        {ok, Dict} ->
            Dict;
        {error, Error} ->
            erlang:error({node_vbuckets_dict_failed, BucketName, Error})
    end.
