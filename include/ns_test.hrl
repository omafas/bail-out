%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-define(assertListsEqual(Expect, Expr),
        (fun () ->
                 __X = lists:sort(Expect),
                 __V = lists:sort(Expr),
                 case __V =:= __X of
                     true -> ok;
                     false -> erlang:error({assertListsEqual,
                                            [{module, ?MODULE},
                                             {line, ?LINE},
                                             {expression, (??Expr)},
                                             {missing, __X -- __V},
                                             {extra, __V -- __X}]})
                 end
         end)()).

-define(_assertListsEqual(Expect, Expr),
        ?_test(?assertListsEqual(Expect, Expr))).

-define(assertBinStringsEqual(Expect, Expr),
        (fun () ->
                 __Val = (Expr),
                 __Expect = (Expect),
                 case __Val =/= __Expect of
                        true ->
                            __Pos = binary:longest_common_prefix(
                                      [__Val, __Expect]) + 1,
                            erlang:error(
                              {assertBinStringsEqual,
                               [{module, ?MODULE},
                                {line, ?LINE},
                                {expression, (??Expr)},
                                {diff_at, __Pos},
                                {expected,
                                 misc:bin_part_near(__Expect, __Pos, 30)},
                                {value,
                                 misc:bin_part_near(__Val, __Pos, 30)}]});
                        false ->
                            ok
                    end
         end)()).

-define(_assertBinStringsEqual(Expect, Expr),
        ?_test(?assertBinStringsEqual(Expect, Expr))).
