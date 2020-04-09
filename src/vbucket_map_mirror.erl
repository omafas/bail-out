%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2018 Couchbase, Inc.
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
%% @doc This service maintains public ETS table that's caching node to
%% active vbuckets mapping and node to capi base url mapping.
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
         must_node_vbuckets_dict/1, node_vbuckets_dict/1,
         node_to_inner_capi_base_url/3, submit_full_reset/0,
         node_to_capi_base_url/2]).

start_link() ->
    work_queue:start_link(vbucket_map_mirror, fun mirror_init/0).

mirror_init() ->
    ets:new(vbucket_map_mirror, [set, named_table]),
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events, fun cleaner_loop/2, {Self, []}),
    submit_full_reset().

cleaner_loop({buckets, [{configs, NewBuckets0}]}, {Parent, CurrentBuckets}) ->
    NewBuckets = lists:sort(NewBuckets0),
    ToClean = ordsets:subtract(CurrentBuckets, NewBuckets),
    BucketNames  = [Name || {Name, _} <- ToClean],
    submit_map_reset(Parent, BucketNames),
    {Parent, NewBuckets};
cleaner_loop({{_, _, capi_port}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop({{_, _, ssl_capi_port}, _Value}, State) ->
    submit_full_reset(),
    State;
cleaner_loop(_, Cleaner) ->
    Cleaner.

submit_map_reset(Pid, BucketNames) ->
    work_queue:submit_work(Pid, fun () ->
                                        [ets:delete(vbucket_map_mirror, Name) || Name <- BucketNames],
                                        ok
                                end).

submit_full_reset() ->
    work_queue:submit_work(vbucket_map_mirror,
                           fun () ->
                                   ets:delete_all_objects(vbucket_map_mirror)
                           end).

compute_and_cache_map(BucketName) ->
    case ets:lookup(vbucket_map_mirror, BucketName) of
        [] ->
            case ns_bucket:get_bucket(BucketName) of
                {ok, BucketConfig} ->
                    case proplists:get_value(map, BucketConfig) of
                        undefined ->
                            {error, no_map};
                        Map ->
                            NodeToVBuckets = compute_map_to_vbuckets_dict(Map),
                            ets:insert(vbucket_map_mirror, {BucketName, NodeToVBuckets}),
                            {ok, NodeToVBuckets}
                    end;
                not_present ->
                    {error, not_present}
            end;
        [{_, Dict}] ->
            {ok, Dict}
    end.

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

call_compute_map(BucketName) ->
    work_queue:submit_sync_work(
      vbucket_map_mirror,
      fun () ->
              compute_and_cache_map(BucketName)
      end).

-spec node_vbuckets_dict(bucket_name()) ->
                                {ok, dict:dict()} | {error, no_map | not_present}.
node_vbuckets_dict(BucketName) ->
    case ets:lookup(vbucket_map_mirror, BucketName) of
        [] ->
            call_compute_map(BucketName);
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

call_compute_node_base_url(Node, User, Password) ->
    work_queue:submit_sync_work(
      vbucket_map_mirror,
      fun () ->
              case capi_utils:compute_capi_port(Node) of
                  undefined ->
                      ets:insert(vbucket_map_mirror, {{Node, User, Password}, undefined, false}),
                      undefined;
                  Port ->
                      {RealNode, Schema} = case Node of
                                               {ssl, V} -> {V, <<"https://">>};
                                               _ -> {Node, <<"http://">>}
                                           end,
                      Auth = case {User, Password} of
                                 {undefined, undefined} ->
                                     [];
                                 {_, _} ->
                                     [User, $:, Password, $@]
                             end,

                      H = misc:extract_node_address(RealNode),
                      StorePort = case misc:is_localhost(H) of
                                      true  -> Port;
                                      false -> false
                                  end,
                      HostPort = misc:join_host_port(H, Port),
                      Url = iolist_to_binary([Schema, Auth, HostPort]),
                      ets:insert(vbucket_map_mirror, {{Node, User, Password}, Url, StorePort}),
                      Url
              end
      end).

%% maps Node to http://<ip>:<capi-port> as binary
%%
%% NOTE: it's not necessarily suitable for sending outside because ip
%% can be localhost!
node_to_inner_capi_base_url(Node, User, Password) ->
    case ets:lookup(vbucket_map_mirror, {Node, User, Password}) of
        [] ->
            call_compute_node_base_url(Node, User, Password),
            node_to_inner_capi_base_url(Node, User, Password);
        [{_, URL, _}] ->
            URL
    end.

-spec node_to_capi_base_url(node() | {ssl, node()},
                            iolist() | binary()) -> undefined | binary().
node_to_capi_base_url(Node, LocalAddr) ->
    case ets:lookup(vbucket_map_mirror, {Node, undefined, undefined}) of
        [] ->
            call_compute_node_base_url(Node, undefined, undefined),
            node_to_capi_base_url(Node, LocalAddr);
        [{_, URL, false}] ->
            URL;
        [{_, _URL, Port}] ->
            Schema = case Node of
                         {ssl, _} ->
                             <<"https://">>;
                         _ ->
                             <<"http://">>
                     end,
            iolist_to_binary([Schema, LocalAddr, $:, integer_to_list(Port)])
    end.
