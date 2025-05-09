%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-define(meckNew(Module, Options),
        case whereis(meck_util:proc_name(Module)) of
            undefined -> meck:new(Module, Options);
            _ -> ok
        end).
-define(meckNew(Module), ?meckNew(Module, [])).

-define(meckUnload(Module),
        case whereis(meck_util:proc_name(Module)) of
            undefined -> ok;
            _ -> meck:unload(Module)
        end).

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

-define(MECK_WAIT_TIMEOUT, 100).


%% This function is made somewhat unwieldy by being a macro, but it ensures
%% that the line number is retained in the error
-define(
   assertProplistsEqualRecursively(Expected, Expr),
   (fun __F(__Expected, __Found, __Context) ->
            ?is_proplist(__Expected) orelse erlang:error({notAProplist,
                                                          __Expected}),
            ?is_proplist(__Found) orelse erlang:error({notAProplist,
                                                       __Found}),
            __Error = fun (Extras) ->
                              erlang:error(
                                {assertProplistsEqualRecursively,
                                 [{module, ?MODULE},
                                  {line, ?LINE},
                                  {expression, ??Expr}
                                 | Extras]})
                      end,
            __ExpKeys = proplists:get_keys(__Expected),
            __FoundKeys = proplists:get_keys(__Found),

            __MissingKeys = __ExpKeys -- __FoundKeys,
            __MissingKeys =:= []
                orelse __Error([{context, __Context},
                                {expected, __Expected},
                                {found, __Found},
                                {error, {missing_keys, __MissingKeys}}]),

            __UnexpectedKeys = __FoundKeys -- __ExpKeys,
            __UnexpectedKeys =:= []
                orelse __Error([{context, __Context},
                                {expected, __Expected},
                                {found, __Found},
                                {error, {unexpected_keys, __MissingKeys}}]),
            lists:foreach(
              fun (__Key) ->
                      __NewContext = __Context ++ [__Key],
                      __ExpectedValue = proplists:get_value(__Key, __Expected),
                      __Error1 =
                          fun (__FoundValue, __ErrorType) ->
                                  __Error([{context, __NewContext},
                                           {expected, __ExpectedValue},
                                           {found, __FoundValue},
                                           {error, __ErrorType}])
                          end,
                      case ?is_proplist(__ExpectedValue) of
                          true ->
                              %% Since the proplists have equal sets of keys,
                              %% we don't need to consider the case where
                              %% the value can't be found
                              __FoundList = proplists:get_value(__Key, __Found),

                              ?is_proplist(__FoundList)
                                  orelse __Error1(__FoundList, not_a_proplist),

                              %% Recursively check sub-proplists
                              __F(__ExpectedValue, __FoundList, __NewContext);
                          false ->
                              %% Not expecting another proplist to recurse down,
                              %% so just directly compare the values
                              __FoundValue = proplists:get_value(__Key,
                                                                 __Found),
                              __FoundValue =:= __ExpectedValue
                                  orelse __Error1(__FoundValue, wrong_value)
                      end
              end, __ExpKeys)
    end)(Expected, Expr, [])).

-define(is_proplist(L),
        (fun(__L) when is_list(__L) ->
                 lists:all(
                   fun({__K, _}) when is_atom(__K) ->
                           true;
                      (__K) when is_atom(__K) ->
                           true;
                      (_) ->
                           false
                   end, __L);
            (_) ->
                 false
         end)(L)).
