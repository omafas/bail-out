%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc This service maintains public ETS table that's caching various
%% somewhat expensive to compute stuff used by menelaus_web*
%%
-module(menelaus_web_cache).
-include("ns_common.hrl").

-export([start_link/0,
         get_static_value/1,
         lookup_or_compute_with_expiration/3]).

start_link() ->
    work_queue:start_link(menelaus_web_cache, fun cache_init/0).

cache_init() ->
    ets:new(menelaus_web_cache, [set, named_table]),
    VersionsPList = build_versions(),
    ets:insert(menelaus_web_cache, {versions, VersionsPList}),
    PackageVariant = read_package_variant(),
    ets:insert(menelaus_web_cache, {package_variant, PackageVariant}).

implementation_version(Versions) ->
    list_to_binary(proplists:get_value(ns_server, Versions, "unknown")).

build_versions() ->
    Versions = ns_info:version(),
    [{implementationVersion, implementation_version(Versions)},
     {componentsVersion, {struct,
                          lists:map(fun ({K,V}) ->
                                            {K, list_to_binary(V)}
                                    end,
                                    Versions)}}].

read_package_variant() ->
    Filename = filename:join(path_config:component_path(bin, ".."),
                             "VARIANT.txt"),
    case file:read_file(Filename) of
        {ok, C} ->
            string:trim(C);
        Err ->
            ?log_error("Failed to read '~p': ~p", [Filename, Err]),
            <<"">>
    end.

get_static_value(Key) ->
    [{Key, Value}] = ets:lookup(menelaus_web_cache, Key),
    Value.

lookup_value_with_expiration(Key, InvalidPred) ->
    Now = erlang:monotonic_time(millisecond),
    case ets:lookup(menelaus_web_cache, Key) of
        [] ->
            {not_found, Now};
        [{_, Value, Expiration, InvalidationState}] ->
            case Now =< Expiration of
                true ->
                    case InvalidPred(Key, Value, InvalidationState) of
                        true ->
                            {not_found, Now};
                        _ ->
                            {ok, Value}
                    end;
                _ ->
                    {not_found, Now}
            end
    end.

lookup_or_compute_with_expiration(Key, ComputeBody, InvalidPred) ->
    case lookup_value_with_expiration(Key, InvalidPred) of
        {not_found, _} ->
            compute_with_expiration(Key, ComputeBody, InvalidPred);
        {ok, Value} ->
            ns_server_stats:notify_counter(<<"web_cache_hits">>),
            Value
    end.

compute_with_expiration(Key, ComputeBody, InvalidPred) ->
    CachePid = whereis(menelaus_web_cache),
    case CachePid =:= self() of
        true ->
            SeenKeys = get_default(seen_keys_stack, []),
            case lists:member(Key, SeenKeys) of
                true ->
                    erlang:error({loop_detected, Key, SeenKeys});
                false ->
                    do_compute_with_expiration(Key, ComputeBody, InvalidPred)
            end;
        false ->
            work_queue:submit_sync_work(
              CachePid,
              fun () ->
                      do_compute_with_expiration(Key, ComputeBody, InvalidPred)
              end)
    end.

do_compute_with_expiration(Key, ComputeBody, InvalidPred) ->
    case lookup_value_with_expiration(Key, InvalidPred) of
        {not_found, Now} ->
            SeenKeys = get_default(seen_keys_stack, []),
            erlang:put(seen_keys_stack, [Key | SeenKeys]),
            try
                {Value, Age, InvalidationState} = ComputeBody(),
                Expiration = Now + Age,
                ns_server_stats:notify_counter(<<"web_cache_updates">>),
                ets:insert(menelaus_web_cache, {Key, Value, Expiration, InvalidationState}),
                Value
            after
                erlang:put(seen_keys_stack, SeenKeys)
            end;
        {ok, Value} ->
            ns_server_stats:notify_counter(<<"web_cache_inner_hits">>),
            Value
    end.

get_default(Key, Default) ->
    case erlang:get(Key) of
        undefined ->
            Default;
        V ->
            V
    end.
