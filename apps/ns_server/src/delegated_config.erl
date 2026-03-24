%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(delegated_config).

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
         merge_validation_results/1,
         process_service_api_validation_results/2,
         process_validation_result/2]).

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

%% @doc Group the validation result into non-errors and errors, perform some
%% optional filtering, and massage the value for later consumption. We over-load
%% the functionality of this function as we can hit all of these functionalities
%% in one pass.
-spec group_and_process(map(), map()) -> {map(), map()}.
group_and_process(ValidationMap, Options) when is_map(ValidationMap) ->
    FilterUnsupported = maps:get(filter_unsupported, Options, false),
    FilterInternal = maps:get(filter_internal, Options, false),
    maps:fold(
      fun(K, V, {OkAcc, ErrAcc}) ->
              case is_error(V) of
                  true ->
                      case FilterUnsupported andalso
                          is_unsupported_parameter(V) of
                          true -> {OkAcc, ErrAcc};
                          false -> {OkAcc, ErrAcc#{K => get_error_message(V)}}
                      end;
                  false ->
                      case FilterInternal andalso
                          not is_public_parameter(V) of
                          true -> {OkAcc, ErrAcc};
                          false -> {OkAcc#{K => get_value(V)}, ErrAcc}
                      end
              end
      end, {#{}, #{}}, ValidationMap).

%% @doc Merge the results of all of the service validation payloads together.
%% We should report an error if one of the inputs for a given parameter is
%% an error, any will do. All OKs should be equal, if ok, and merged together.
-spec merge_validation_results(list()) -> {map(), map()}.
merge_validation_results(AllResults) ->
    lists:foldl(
        fun ({ServiceOKs, ServiceErrors}, {OKAcc, ErrorsAcc}) ->
            Fun =
                fun (Key, V1, V2) ->
                    case V1 of
                        V2 -> V1;
                        _ -> erlang:exit(
                            lists:flatten(
                                io_lib:format(
                                    "Values not equal for key "
                                    "~s: ~s ~s",
                                    [Key, V1, V2])))
                    end
                end,
            %% Merge together the OKs, if a key is present in
            %% multiple service responses then they ought to
            %% provide the same value.
            O1 = maps:merge_with(Fun, OKAcc, ServiceOKs),
            %% Should we see any errors we will just return one of
            %% them for any given key. Services might report
            %% errors differently so we shouldn't check that they
            %% are the same.
            E1 = maps:merge(ErrorsAcc, ServiceErrors),
            {O1, E1}
        end, {#{}, #{}}, AllResults).

%% @doc Process the validation results of a delegated config API call. This
%% takes a map of all of the validation results, groups them by OK and error,
%% and applies some common filtering and processing.
-spec process_validation_result(map(), map()) -> {ok, {map(), map()}}.
process_validation_result(Result, Options) ->
    {ok, group_and_process(Result, Options)}.

%% @doc Process the validation results of delegated config API calls from the
%% service API. This takes a list of {Node, {ok, Response}} and will process the
%% Response for all nodes before merging the results.
-spec process_service_api_validation_results(list(), map()) ->
          {ok, {map(), map()}}.
process_service_api_validation_results(AllResults, Options) ->
    R1 = lists:map(
        fun({_, {ok, NodeResult}}) ->
            {ok, R} =
                process_service_api_validation_result(
                    NodeResult,
                    Options),
            R
        end, AllResults),
    {ok, delegated_config:merge_validation_results(R1)}.

%% @doc Process the validation result of a delegated config API call from the
%% service API.
process_service_api_validation_result(Result, Options) ->
    %% TODO MB-64129: When the service API uses json over ejson we can
    %% remove this.
    %% Hack - The validation map parsing code uses the json library
    %% format of the parsed json response (a map) rather than the ejson
    %% format. Convert the parsed json to a string then to the json
    %% library decoded format such that we can re-use that code.
    Str = ejson:encode(Result),
    Json = json:decode(Str),
    process_validation_result(maps:get(<<"validationResult">>, Json), Options).


-ifdef(TEST).

sample_error() ->
    #{
      <<"error">> => <<"error">>,
      <<"message">> => <<"message">>
    }.

sample_unsupported_error() ->
    #{
      <<"error">> => <<"unsupported">>,
      <<"message">> => <<"unsupported parameter">>
    }.

sample_public() ->
    #{
      <<"value">> => <<"value">>,
      <<"requiresRestart">> => true,
      <<"visibility">> => <<"public">>
     }.

sample_internal() ->
    #{
      <<"value">> => <<"internal_value">>,
      <<"requiresRestart">> => false,
      <<"visibility">> => <<"internal">>
     }.

sample_no_visibility() ->
    #{<<"value">> => <<"default_vis">>}.

sample_validation_map() ->
    #{
      <<"foo">> => sample_error(),
      <<"bar">> => sample_public(),
      <<"baz">> => sample_internal()
     }.

is_error_test() ->
    ?assert(is_error(sample_error())),
    ?assertNot(is_error(sample_public())),
    ?assertNot(is_error(sample_internal())),
    ?assertNot(is_error(#{})).

is_public_parameter_test() ->
    ?assert(is_public_parameter(sample_public())),
    ?assertNot(is_public_parameter(sample_internal())),
    %% Default visibility is public.
    ?assert(is_public_parameter(sample_no_visibility())),
    ?assert(is_public_parameter(#{})).

is_unsupported_parameter_test() ->
    ?assert(is_unsupported_parameter(sample_unsupported_error())),
    ?assertNot(is_unsupported_parameter(sample_error())),
    ?assertNot(is_unsupported_parameter(sample_public())),
    ?assertNot(is_unsupported_parameter(#{})).

requires_restart_test() ->
    ?assert(requires_restart(sample_public())),
    ?assertNot(requires_restart(sample_internal())),
    %% Default is false.
    ?assertNot(requires_restart(#{})).

get_error_kind_test() ->
    ?assertEqual(<<"error">>,
                 get_error_kind(sample_error())),
    ?assertEqual(<<"unsupported">>,
                 get_error_kind(sample_unsupported_error())),
    ?assertEqual(undefined, get_error_kind(sample_public())),
    ?assertEqual(undefined, get_error_kind(#{})).

get_error_message_test() ->
    ?assertEqual(<<"message">>,
                 get_error_message(sample_error())),
    ?assertEqual(<<"unsupported parameter">>,
                 get_error_message(sample_unsupported_error())),
    ?assertEqual(undefined,
                 get_error_message(sample_public())),
    %% Falls back to error kind when no message.
    ?assertEqual(<<"error">>,
                 get_error_message(
                   #{<<"error">> => <<"error">>})).

get_value_test() ->
    ?assertEqual(<<"value">>, get_value(sample_public())),
    ?assertEqual(<<"internal_value">>,
                 get_value(sample_internal())),
    ?assertEqual(undefined, get_value(sample_error())),
    ?assertEqual(undefined, get_value(#{})).

group_and_process_test() ->
    {OKs, Errors} =
        group_and_process(sample_validation_map(), #{}),
    ?assertEqual(#{<<"bar">> => <<"value">>,
                   <<"baz">> => <<"internal_value">>},
                 OKs),
    ?assertEqual(#{<<"foo">> => <<"message">>}, Errors).

group_and_process_filter_unsupported_test() ->
    Map = #{<<"a">> => sample_unsupported_error(),
            <<"b">> => sample_error(),
            <<"c">> => sample_public()},
    {OKs, Errors} =
        group_and_process(
          Map, #{filter_unsupported => true}),
    ?assertEqual(#{<<"c">> => <<"value">>}, OKs),
    ?assertEqual(#{<<"b">> => <<"message">>}, Errors).

group_and_process_filter_internal_test() ->
    Map = #{<<"a">> => sample_public(),
            <<"b">> => sample_internal()},
    {OKs, Errors} =
        group_and_process(
          Map, #{filter_internal => true}),
    ?assertEqual(#{<<"a">> => <<"value">>}, OKs),
    ?assertEqual(#{}, Errors).

group_and_process_multiple_errors_test() ->
    E1 = #{<<"error">> => <<"bad">>,
           <<"message">> => <<"msg1">>},
    E2 = #{<<"error">> => <<"bad">>,
           <<"message">> => <<"msg2">>},
    Map = #{<<"x">> => E1, <<"y">> => E2},
    {OKs, Errors} =
        group_and_process(Map, #{}),
    ?assertEqual(#{}, OKs),
    ?assertEqual(#{<<"x">> => <<"msg1">>,
                   <<"y">> => <<"msg2">>}, Errors).

group_and_process_empty_test() ->
    ?assertEqual({#{}, #{}},
                 group_and_process(#{}, #{})).

merge_validation_results_empty_test() ->
    ?assertEqual({#{}, #{}},
                 merge_validation_results([])).

merge_validation_results_single_test() ->
    Input = {#{<<"a">> => <<"v1">>},
             #{<<"b">> => <<"err1">>}},
    ?assertEqual(Input,
                 merge_validation_results([Input])).

merge_validation_results_merge_test() ->
    R1 = {#{<<"a">> => <<"v1">>}, #{}},
    R2 = {#{<<"b">> => <<"v2">>},
           #{<<"c">> => <<"err1">>}},
    ?assertEqual(
       {#{<<"a">> => <<"v1">>,
          <<"b">> => <<"v2">>},
        #{<<"c">> => <<"err1">>}},
       merge_validation_results([R1, R2])).

merge_validation_results_same_ok_values_test() ->
    R1 = {#{<<"a">> => <<"v1">>}, #{}},
    R2 = {#{<<"a">> => <<"v1">>}, #{}},
    ?assertEqual(
       {#{<<"a">> => <<"v1">>}, #{}},
       merge_validation_results([R1, R2])).

merge_validation_results_conflicting_ok_values_test() ->
    R1 = {#{<<"a">> => <<"v1">>}, #{}},
    R2 = {#{<<"a">> => <<"v2">>}, #{}},
    ?assertExit(_, merge_validation_results([R1, R2])).

merge_validation_results_error_merge_test() ->
    R1 = {#{}, #{<<"a">> => <<"err1">>}},
    R2 = {#{}, #{<<"a">> => <<"err2">>}},
    {_, Errors} = merge_validation_results([R1, R2]),
    ?assert(maps:is_key(<<"a">>, Errors)).

process_validation_result_test() ->
    Map = sample_validation_map(),
    {ok, {OKs, Errors}} =
        process_validation_result(Map, #{}),
    ?assertEqual(#{<<"bar">> => <<"value">>,
                   <<"baz">> => <<"internal_value">>},
                 OKs),
    ?assertEqual(#{<<"foo">> => <<"message">>}, Errors).

process_validation_result_with_filters_test() ->
    Map = #{<<"a">> => sample_public(),
            <<"b">> => sample_internal(),
            <<"c">> => sample_unsupported_error()},
    {ok, {OKs, Errors}} =
        process_validation_result(
          Map,
          #{filter_internal => true,
            filter_unsupported => true}),
    ?assertEqual(#{<<"a">> => <<"value">>}, OKs),
    ?assertEqual(#{}, Errors).

-endif.
