%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(analytics_prod_compat).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([current_prod_compat_version/0,
         min_supported_prod_compat_version/0,
         supported_prod_compat_version/0,
         prod_spec_from_legacy_version/1,
         compare_prod_compat_version/2]).

-define(COLUMNAR_COMPAT_KEY, <<"/cbas/columnarCompatibility">>).

prod_spec_from_legacy_version(Version) ->
    %% Can be dropped once we drop support for IONIC
    %% IONIC clusters don't contain prodName and prodCompatVersion
    %% in the nodeKVList
    parse_ionic_version(Version).

compare_prod_compat_version(A, B) ->
    ListA = parse_semantic_version(A),
    ListB = parse_semantic_version(B),
    case ListA of
        _ when ListA < ListB ->
            less_than;
        _ when ListA > ListB ->
            greater_than;
        _ ->
            equal
    end.

parse_semantic_version(Version) when is_binary(Version) ->
    parse_semantic_version(binary_to_list(Version));

parse_semantic_version(Version) ->
    lists:map(fun list_to_integer/1, string:tokens(Version, ".")).

current_prod_compat_version() ->
    case metakv:get(?COLUMNAR_COMPAT_KEY) of
        false ->
            min_supported_prod_compat_version();
        {value, Blob, _VC} ->
            ejson:decode(Blob)
    end.

min_supported_prod_compat_version() ->
    list_to_binary(config_profile:search(prod_min_supported_version)).

supported_prod_compat_version() ->
    list_to_binary(case get_product_pretend_version() of
                       undefined ->
                           config_profile:search(prod_compat_version);
                       PretendCompat ->
                           PretendCompat
                   end).

-spec parse_ionic_version(Version :: binary()) ->
          {string(), string(), binary()} | {undefined, undefined, undefined}.
parse_ionic_version(Version) ->
    %% Handle ionic upgrade -- assumes that the prodCompatVersion of the
    %% joining ionic node matches its version. Since we control all ionic
    %% deployments, we can "safely" make this assumption.
    VersionStr = binary_to_list(Version),
    case lists:suffix("-columnar", VersionStr) of
        true ->
            [Vsn | _] = string:tokens(VersionStr, "-"),
            {config_profile:search(prod),
             config_profile:search(prod_name),
             list_to_binary(Vsn)};
        false ->
            {undefined, undefined, undefined}
    end.

get_product_pretend_version() ->
    case application:get_env(ns_server, product_pretend_version) of
        undefined -> undefined;
        {ok, VersionString} -> VersionString
    end.

-ifdef(TEST).
compare_prod_compat_version_test_() ->
    [?_assertEqual(equal,
                   compare_prod_compat_version("1.2.3", "1.2.3")),
     ?_assertEqual(greater_than,
                   compare_prod_compat_version("1.12.3", "1.2.3")),
     ?_assertEqual(equal,
                   compare_prod_compat_version("1.2.3", <<"1.2.3">>)),
     ?_assertEqual(less_than,
                   compare_prod_compat_version(<<"1.2.3">>, "1.12.3"))].

parse_ionic_version_test() ->
    meck:new(config_profile, [passthrough]),
    try
        meck:expect(config_profile, get,
                    fun () ->
                            [
                             {name, ?ANALYTICS_PROFILE_STR},
                             {prod, ?ANALYTICS_PROD},
                             {prod_name, ?ANALYTICS_PROD_NAME}
                            ]
                    end),
        ?assertEqual(
           {?ANALYTICS_PROD, ?ANALYTICS_PROD_NAME, <<"1.0.5">>},
           parse_ionic_version(<<"1.0.5-1234-columnar">>)),
        ?assertEqual({undefined, undefined, undefined},
                     parse_ionic_version(<<"7.6.0-1234-enterprise">>))
    after
        meck:unload(config_profile)
    end.

is_compatible_product_test() ->
    meck:new(config_profile, [passthrough]),
    try
        meck:expect(config_profile, get,
                    fun () ->
                            ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                    end),
        false =
            cluster_compat_mode:is_compatible_product(?ANALYTICS_PROD),
        meck:expect(config_profile, get,
                    fun () ->
                            [
                             {name, ?ANALYTICS_PROFILE_STR},
                             {prod, ?ANALYTICS_PROD},
                             {prod_name, ?ANALYTICS_PROD_NAME}
                            ]
                    end),
        false = cluster_compat_mode:is_compatible_product("Wombat"),
        false = cluster_compat_mode:is_compatible_product(undefined),
        false = cluster_compat_mode:is_compatible_product(?DEFAULT_PROD),
        true = cluster_compat_mode:is_compatible_product(?ANALYTICS_PROD)
    after
        meck:unload(config_profile)
    end.

-endif.
