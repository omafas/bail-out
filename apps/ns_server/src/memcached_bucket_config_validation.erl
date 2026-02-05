%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(memcached_bucket_config_validation).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("ns_common.hrl").
-export([is_error/1,
         is_public_parameter/1,
         is_unsupported_parameter/1,
         requires_restart/1,
         get_error_message/1,
         get_value/1,
         group/1]).

%% @doc Check if the validation result is an error.
-spec is_error(map()) -> boolean().
is_error(#{<<"error">> := _}) ->
    true;
is_error(_) ->
    false.

%% @doc Check if the validation result is a public parameter.
-spec is_public_parameter(map()) -> boolean().
is_public_parameter(Object) ->
    <<"public">> =:= maps:get(<<"visibility">>, Object, <<"public">>).

%% @doc Check if the validation result is an unsupported parameter.
-spec is_unsupported_parameter(map()) -> boolean().
is_unsupported_parameter(Object) ->
    get_error_kind(Object) =:= <<"unsupported">>.

%% @doc Check if the validation result requires a restart.
-spec requires_restart(map()) -> boolean().
requires_restart(Object) ->
    maps:get(<<"requiresRestart">>, Object, false).

%% @doc Get the error kind from the validation result.
-spec get_error_kind(map()) -> binary() | undefined.
get_error_kind(Object) ->
    maps:get(<<"error">>, Object, undefined).

%% @doc Get the error message from the validation result.
-spec get_error_message(map()) -> binary() | undefined.
get_error_message(Object) ->
    maps:get(<<"message">>, Object, get_error_kind(Object)).

%% @doc Get the value from the validation result.
-spec get_value(map()) -> term() | undefined.
get_value(Object) ->
    maps:get(<<"value">>, Object, undefined).

%% @doc Group the validation result into non-errors and errors.
-spec group(map()) -> {map(), map()}.
group(ValidationMap) when is_map(ValidationMap) ->
    Errors = maps:filter(
        fun (_Key, Object) -> is_error(Object) end, ValidationMap),
    OKs = maps:filter(
        fun (_Key, Object) -> not is_error(Object) end, ValidationMap),
    {OKs, Errors}.

-ifdef(TEST).

%% @doc Sample error validation result.
sample_error() ->
    #{
      <<"error">> => <<"error">>,
      <<"message">> => <<"message">>
    }.

sample_public() ->
    #{
      <<"value">> => <<"value">>,
      <<"requiresRestart">> => <<"requiresRestart">>,
      <<"visibility">> => <<"public">>
     }.

sample_internal() ->
    #{
      <<"value">> => <<"value">>,
      <<"requiresRestart">> => <<"requiresRestart">>,
      <<"visibility">> => <<"internal">>
     }.

sample_validation_map() ->
    #{
      <<"foo">> => sample_error(),
      <<"bar">> => sample_public(),
      <<"baz">> => sample_internal()
     }.

is_error_test() ->
    ?assertEqual(true, is_error(sample_error())),
    ?assertEqual(false, is_error(sample_public())),
    ?assertEqual(false, is_error(sample_internal())).

get_error_kind_test() ->
    ?assertEqual(<<"error">>, get_error_kind(sample_error())),
    ?assertEqual(undefined, get_error_kind(sample_public())),
    ?assertEqual(undefined, get_error_kind(sample_internal())).

get_error_message_test() ->
    ?assertEqual(<<"message">>, get_error_message(sample_error())),
    ?assertEqual(undefined, get_error_message(sample_public())),
    ?assertEqual(undefined, get_error_message(sample_internal())).

group_test() ->
    OKs = #{
        <<"bar">> => sample_public(),
        <<"baz">> => sample_internal()
    },
    Errors = #{
        <<"foo">> => sample_error()
    },
    ?assertEqual({OKs, Errors}, group(sample_validation_map())).

-endif.
