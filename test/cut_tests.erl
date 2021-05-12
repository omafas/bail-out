%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
