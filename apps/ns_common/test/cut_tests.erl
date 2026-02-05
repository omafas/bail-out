%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc - This module contains tests for cut transformations
-module(cut_tests).

-include("cut.hrl").
-include_lib("eunit/include/eunit.hrl").

foo3(F, A, B, C) ->
    F(A, B, C).

simple_params_test() ->
    ?assertEqual({1, 2, 3}, foo3({_, _, _}, 1, 2, 3)),
    ?assertEqual({1, 2, 3}, foo3(?cut({_, _, _}), 1, 2, 3)).

numbered_params_test() ->
    ?assertEqual({1, 2, 3}, foo3(?cut({_1, _2, _3}), 1, 2, 3)),
    ?assertEqual({3, 2, 1}, foo3(?cut({_3, _2, _1}), 1, 2, 3)),
    ?assertEqual({1, 2, 3, 1, 3, 2}, foo3(?cut({_1, _2, _3, _1, _3, _2}),
                                          1, 2, 3)),
    ?assertEqual({2, 3}, foo3(?cut({_2, _3}), 1, 2, 3)),
    ?assertEqual({1, 3}, foo3(?cut({_1, _3}), 1, 2, 3)),
    ?assertEqual({1, [2, 3]}, foo3(?cut({_1, [_2, _3]}), 1, 2, 3)),
    ?assertEqual({[3, 2], 1},
                 foo3(?cut({lists:reverse([_2, _3]), _1}), 1, 2, 3)).

map_comprehensions_test() ->
    A = #{a => 1, b => 2},
    ?assertEqual(#{2 => b, 1 => a}, #{V => K || K := V <- A}),
    F1 = #{V => K || K := V <- _},
    ?assertEqual(#{2 => b, 1 => a}, F1(A)),
    F2 = ?cut(#{V => K || K := V <- maps:merge(#{c => 3}, _)}),
    ?assertEqual(#{2 => b, 1 => a, 3 => c}, F2(A)),
    F3 = ?cut(#{K => _ || K := _V <- A}),
    ?assertEqual(#{a => 5, b => 5}, F3(5)),
    F4 = ?cut(#{K => _1 || K := _V <- _2}),
    ?assertEqual(#{a => 5, b => 5}, F4(5, A)).

-if(?FEATURE_ENABLED(maybe_expr)).
maybe_test() ->
    F1 = fun (1) -> {ok, 1};
             (2) -> {error, 2}
         end,
    %% There is an erlang bug that doesn't allow "maybe" to be a macro arg.
    %% https://github.com/erlang/otp/issues/8268
    %% For that reason we use cut:f instead of ?cut here until we upgrade.
    Cut = cut:f(maybe
                   {ok, 1} ?= F1(_1),
                   _2
               else
                   {error, 2} -> _3
               end),
    ?assertEqual(succ, Cut(1, succ, err)),
    ?assertEqual(err, Cut(2, succ, err)).
-endif.
