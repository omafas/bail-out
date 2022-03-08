%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(promQL).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([format_value/1, parse_value/1, format_promql/1,
         parse_time_duration/1, is_aggregation_op/1]).

%% AST helpers
-export([metric/1, rate/2, sum/1, sum_by/2, sum_without/2, bucket_metric/2,
         named/2, with_label/3, multiply_by_scalar/2, convert_units/3,
         eq/2, eq/3, eq_any/2, re/3, op/2, clamp_min/2, idelta/2]).

-define(DEFAULT_RANGE_INTERVAL, "1m").

eq(Name, Value) -> eq(Name, Value, {[]}).
eq(Name, Value, {M}) -> {[{eq, Name, Value} | M]}.

eq_any(Name, Values) -> {[{eq_any, Name, Values}]}.

re(Name, Value, {M}) -> {[{re, Name, Value} | M]}.

metric(Name) -> eq(<<"name">>, Name).

op(Op, Metrics) -> {Op, [{ignoring, [<<"name">>]}], Metrics}.

rate(Ast, Opts) ->
    {call, irate, none, [range(Ast, Opts)]}.

idelta(Ast, Opts) ->
    {call, idelta, none, [range(Ast, Opts)]}.

%% range vector argument must be an instant vector
range({L} = Ast, Opts) when is_list(L) ->
    case Opts of
        #{range_interval := I} -> {range_vector, Ast, I};
        #{} -> {range_vector, Ast, ?DEFAULT_RANGE_INTERVAL}
    end.

clamp_min(Ast, Min) ->
    {call, clamp_min, none, [Ast, Min]}.

sum(Ast) -> {call, sum, none, [Ast]}.

sum_by(ByFields, Ast) -> {call, sum, {by, ByFields}, [Ast]}.

sum_without(WithoutFields, Ast) -> {call, sum, {without, WithoutFields}, [Ast]}.

bucket_metric(Name, Bucket) ->
    {[{eq, <<"name">>, Name}, {eq, <<"bucket">>, Bucket}]}.

named(Name, Ast) ->
    with_label(<<"name">>, Name, Ast).

with_label(Name, Value, Ast) ->
    {call, label_replace, none, [Ast, Name, Value, <<>>, <<>>]}.

multiply_by_scalar(Ast, Scalar) ->
    {'*', [Ast, Scalar]}.

convert_units(seconds, nanoseconds, Ast) -> multiply_by_scalar(Ast, 1000000000);
convert_units(seconds, microseconds, Ast) -> multiply_by_scalar(Ast, 1000000);
convert_units(seconds, milliseconds, Ast) -> multiply_by_scalar(Ast, 1000);
convert_units(U, U, Ast) -> Ast.

format_value(undefined) -> <<"NaN">>;
format_value(infinity) -> <<"+Inf">>;
format_value(neg_infinity) -> <<"-Inf">>;
format_value(B) when is_binary(B) -> B;
format_value(N) when is_integer(N) -> integer_to_binary(N);
format_value(N) -> float_to_binary(N).

parse_value(<<"NaN">>) -> undefined;
parse_value(<<"+Inf">>) -> infinity;
parse_value(<<"-Inf">>) -> neg_infinity;
parse_value(B) ->
    try
        binary_to_float(B)
    catch
        _:_ -> binary_to_integer(B)
    end.

format_promql(AST) ->
    iolist_to_binary(format_promql_ast(AST)).

-define(BINOP(Op), Op =:= 'or'; Op =:= 'and'; Op =:= 'unless'; Op =:= '/';
                   Op =:= '+';  Op =:= '-';   Op =:= '/';      Op =:= '*';
                   Op =:= '%';  Op =:= '^';   Op =:= '==';     Op =:= '!=';
                   Op =:= '>';  Op =:= '<';   Op =:= '>=';     Op =:= '<=').

-define(AGGREGATION_OP(Op), Op =:= sum;     Op =:= min;   Op =:= max;
                            Op =:= avg;     Op =:= group; Op =:= stddev;
                            Op =:= stdvar;  Op =:= count; Op =:= count_values;
                            Op =:= bottomk; Op =:= topk;  Op =:= quantile).

-define(MERGE_LABEL, <<"name">>).

is_aggregation_op(Op) when ?AGGREGATION_OP(Op) -> true;
is_aggregation_op(_) -> false.

format_promql_ast({call, F, By, Args}) when is_atom(F) ->
    format_promql_ast({call, atom_to_list(F), By, Args});
format_promql_ast({call, F, By, Args}) ->
    ByStr =
        case By of
            {by, L} -> [" by (", lists:join(",", L) ,") "];
            {without, L} -> [" without (", lists:join(",", L) ,") "];
            none -> []
        end,
    [F, ByStr, "(", lists:join(",", [format_promql_ast(E) || E <- Args]), ")"];
format_promql_ast({Op, Exprs}) when ?BINOP(Op) ->
    format_promql_ast({Op, [], Exprs});
format_promql_ast({Op, Opts, Exprs0}) when ?BINOP(Op) ->
    Exprs = case Op of
                'or' -> merge_or_operands(Exprs0);
                _ -> Exprs0
            end,
    OptsIOList = lists:map(fun ({T, L}) ->
                               [atom_to_list(T), "(", lists:join(",", L), ") "]
                           end, Opts),
    OpStr = " " ++ atom_to_list(Op) ++ " " ++ OptsIOList,
    lists:join(OpStr, lists:map(fun ({O, _} = E) when ?BINOP(O) ->
                                        ["(", format_promql_ast(E), ")"];
                                    ({O, _, _} = E) when ?BINOP(O) ->
                                        ["(", format_promql_ast(E), ")"];
                                    (E) ->
                                        format_promql_ast(E)
                                end, Exprs));
format_promql_ast({range_vector, Expr, Duration}) when is_integer(Duration) ->
    format_promql_ast({range_vector, Expr, integer_to_list(Duration) ++ "s"});
format_promql_ast({range_vector, Expr, Duration}) ->
    [format_promql_ast(Expr), "[", Duration, "]"];
format_promql_ast({Labels}) when is_list(Labels) ->
    LabelsIOLists =
      lists:map(
        fun ({re, Name, Value}) ->
                [Name, "=~`", Value, "`"];
            ({not_re, Name, Value}) ->
                [Name, "!~`", Value, "`"];
            ({eq_any, Name, [_|_] = Values}) ->
                Escaped = [escape_re_chars(V) || V <- Values],
                [Name, "=~`", lists:join("|", Escaped), "`"];
            ({not_any, Name, [_|_] = Values}) ->
                Escaped = [escape_re_chars(V) || V <- Values],
                [Name, "!~`", lists:join("|", Escaped), "`"];
            ({eq, Name, Value}) ->
                [Name, "=`", Value, "`"];
            ({not_eq, Name, Value}) ->
                [Name, "!=`", Value, "`"]
        end, Labels),
    ["{" ++ lists:join(",", LabelsIOLists) ++ "}"];
format_promql_ast(Bin) when is_binary(Bin) ->
    <<"`", Bin/binary, "`">>;
format_promql_ast(N) when is_integer(N) ->
    erlang:integer_to_list(N);
format_promql_ast(X) when is_float(X) ->
    erlang:float_to_list(X).

%% Transform "f({name=`m1`, ...}) or f({name=`m2`, ...} or ..." to
%% "f({name=~`m1|m1|...`, ...})" as it works faster.
%% Note: it's correct only if function 'f' commutes with 'or'
merge_or_operands(List) ->
    Sorted = lists:usort(
               fun (A, B) ->
                   comparable(A) =< comparable(B)
               end, List),
    merge_or_operands_sorted(Sorted, []).

merge_or_operands_sorted([], Res) -> lists:reverse(Res);
merge_or_operands_sorted([E], Res) -> merge_or_operands_sorted([], [E | Res]);
merge_or_operands_sorted([E1, E2 | T], Res) ->
    case merge_or_operands(E1, E2) of
        match -> merge_or_operands_sorted([E1 | T], Res);
        {merged, E} -> merge_or_operands_sorted([E | T], Res);
        conflict -> merge_or_operands_sorted([E2 | T], [E1 | Res])
    end.

merge_or_operands({Op, [Op1, Scalar]}, {Op, [Op2, Scalar]})
                                                    when (Op =:= '*' orelse
                                                          Op =:= '/'),
                                                         is_number(Scalar) ->
    case merge_or_operands(Op1, Op2) of
        match -> match;
        conflict -> conflict;
        {merged, M} -> {merged, {Op, [M, Scalar]}}
    end;
merge_or_operands({call, F, By, Args1}, {call, F, By, Args2})
                                        when length(Args1) == length(Args2) ->
    case commute_with_or(F, By) of
        true ->
            {NewArgs, Res} =
                lists:mapfoldl(
                  fun ({_, _}, conflict) ->
                          {undefined, conflict};
                      ({A1, A2}, merged) ->
                          case merge_or_operands(A1, A2) of
                              conflict -> {undefined, conflict};
                              match -> {A1, merged};
                              {merged, _} -> {undefined, conflict}
                          end;
                      ({A1, A2}, match) ->
                          case merge_or_operands(A1, A2) of
                              conflict -> {undefined, conflict};
                              match -> {A1, match};
                              {merged, M} -> {M, merged}
                          end
                  end, match, lists:zip(Args1, Args2)),
            case Res of
                conflict -> conflict;
                merged -> {merged, {call, F, By, NewArgs}};
                match -> match
            end;
        false ->
            conflict
    end;
merge_or_operands({range_vector, E1, D}, {range_vector, E2, D}) ->
    case merge_or_operands(E1, E2) of
        match -> match;
        conflict -> conflict;
        {merged, E} -> {merged, {range_vector, E, D}}
    end;
merge_or_operands({L1}, {L2}) when is_list(L1), is_list(L2) ->
    case {extract_merge_label(L1), extract_merge_label(L2)} of
        {{Names, Rest}, {Names, Rest}} ->
            match;
        {{Names1, Rest}, {Names2, Rest}} ->
            NewNames = lists:umerge(Names1, Names2),
            {merged, {[{eq_any, ?MERGE_LABEL, NewNames} | Rest]}};
        _ ->
            conflict
    end;
merge_or_operands(Q, Q) -> match;
merge_or_operands(_, _) -> conflict.

comparable({List}) when is_list(List) ->
    case extract_merge_label(List) of
        {Names, Rest} -> {{lists:usort(Rest)}, Names};
        not_found -> {{lists:usort(List)}, []}
    end;
comparable({Op, List}) when ?BINOP(Op) ->
    comparable({Op, [], List});
comparable({Op, Opts, Args}) when ?BINOP(Op) ->
    {NewArgs, NewNames} = comparable(Args),
    {{Op, Opts, NewArgs}, NewNames};
comparable({call, F, By, Args}) when is_atom(F) ->
    comparable({call, atom_to_binary(F, latin1), By, Args});
comparable({call, F, By, Args}) ->
    {NewArgs, NewNames} = comparable(Args),
    {{call, F, By, NewArgs}, NewNames};
comparable({range_vector, Expr, Duration}) ->
    {NewExpr, Names} = comparable(Expr),
    {{range_vector, NewExpr, Duration}, Names};
comparable(L) when is_list(L) ->
    lists:mapfoldl(
      fun (A, Acc) ->
          {NewA, Names} = comparable(A),
          {NewA, Acc ++ Names}
      end, [], L);
comparable(Const) when is_number(Const); is_binary(Const) ->
    {Const, []}.

commute_with_or(F, {by, List}) when ?AGGREGATION_OP(F) ->
    lists:member(?MERGE_LABEL, List);
commute_with_or(F, {without, _List}) when ?AGGREGATION_OP(F) -> false;
commute_with_or(F, none) when ?AGGREGATION_OP(F) -> false;
commute_with_or(_, none) -> true.

extract_merge_label(Props) ->
    case lists:keytake(?MERGE_LABEL, 2, Props) of
        {value, {eq, _, N}, Rest} -> {[N], Rest};
        {value, {eq_any, _, NL}, Rest} -> {lists:usort(NL), Rest};
        _ -> not_found
    end.

escape_re_chars(Str) ->
    re:replace(Str, "[\\[\\].\\\\^$|()?*+{}]", "\\\\&",
               [{return,list}, global]).

-spec parse_time_duration(Duration :: string()) ->
    {ok, Milliseconds :: integer()} | {error, binary()}.
parse_time_duration("") ->
    {error, <<"missing unit character in duration">>};
parse_time_duration(Str) when is_list(Str) ->
    parse_time_duration(Str, "", 0).
parse_time_duration("", _, Acc) -> {ok, Acc};
parse_time_duration(Str, PrevUnit, Acc) ->
    ParseUnit =
        fun ("ms" ++ Rest) -> {ok, "ms", 1, Rest};
            ("s" ++ Rest)  -> {ok, "s", 1000, Rest};
            ("m" ++ Rest)  -> {ok, "m", 60 * 1000, Rest};
            ("h" ++ Rest)  -> {ok, "h", 60 * 60 * 1000, Rest};
            ("d" ++ Rest)  -> {ok, "d", 24 * 60 * 60 * 1000, Rest};
            ("w" ++ Rest)  -> {ok, "w", 7 * 24 * 60 * 60 * 1000, Rest};
            ("y" ++ Rest)  -> {ok, "y", 365 * 24 * 60 * 60 * 1000, Rest};
            (_) -> {error, <<"missing unit character in duration">>}
        end,
    ValidUnit =
        fun (Unit) ->
            Allowed = lists:takewhile(fun (U) -> U =/= PrevUnit end,
                                      ["ms", "s", "m", "h", "d", "w", "y"]),
            lists:member(Unit, Allowed)
        end,
    try string:list_to_integer(Str) of
        {error, _} ->
            {error, <<"not a valid duration string">>};
        {Val, Rest} ->
            case ParseUnit(Rest) of
                {ok, Unit, Mult, Rest2} ->
                    case ValidUnit(Unit) of
                        true -> parse_time_duration(Rest2, Unit, Acc + Val * Mult);
                        false -> {error, <<"not valid duration string">>}
                    end;
                {error, Error} -> {error, Error}
            end
    catch
        _:_ -> {error, <<"not a valid duration string">>}
    end.

-ifdef(TEST).

parse_time_duration_test() ->
    ?assertMatch({error, _}, parse_time_duration("")),
    ?assertMatch({error, _}, parse_time_duration("1")),
    ?assertMatch({error, _}, parse_time_duration("42m42")),
    ?assertMatch({error, _}, parse_time_duration("42a")),
    ?assertMatch({error, _}, parse_time_duration("2y3m4h")),
    ?assertMatch({error, _}, parse_time_duration("105w2y5w90s")),
    ?assertEqual({ok, 42}, parse_time_duration("42ms")),
    ?assertEqual({ok, 42000}, parse_time_duration("42s")),
    ?assertEqual({ok, 42000 * 60}, parse_time_duration("42m")),
    ?assertEqual({ok, 42000 * 60 * 60}, parse_time_duration("42h")),
    ?assertEqual({ok, 42000 * 60 * 60 * 24}, parse_time_duration("42d")),
    ?assertEqual({ok, 42000 * 60 * 60 * 24 * 7}, parse_time_duration("42w")),
    ?assertEqual({ok, 42000 * 60 * 60 * 24 * 365}, parse_time_duration("42y")),
    ?assertEqual({ok, 33019506007}, parse_time_duration("1y2w3d4h5m6s7ms")).

format_promql_test() ->
    ?assertEqual(format_promql({[]}), <<"{}">>),
    ?assertEqual(format_promql({[{eq, <<"label1">>, <<"val1">>},
                                 {eq_any, <<"label2">>, [<<"opt1">>,
                                                         <<"opt2">>,
                                                         <<"opt3">>]},
                                 {re, <<"label3">>, "re"}]}),
                 <<"{label1=`val1`,label2=~`opt1|opt2|opt3`,label3=~`re`}">>),
    ?assertEqual(format_promql({call, sum, {by, [<<"label1">>, <<"label2">>]},
                                [{[{eq, <<"name">>, <<"metric">>}]}]}),
                 <<"sum by (label1,label2) ({name=`metric`})">>),
    ?assertEqual(format_promql({'or', [{[{eq, <<"label1">>, <<"val1">>}]},
                                       {[{eq, <<"label2">>, <<"val2">>}]}]}),
                 <<"{label1=`val1`} or {label2=`val2`}">>),
    ?assertEqual(format_promql({'/', [{ignoring, [<<"l1">>, <<"l2">>]},
                                      {group_left, [<<"l3">>, <<"l4">>]}],
                                     [{[{eq, <<"l1">>, <<"v1">>}]},
                                      {[{eq, <<"l2">>, <<"v2">>}]}]}),
                 <<"{l1=`v1`} / ignoring(l1,l2) group_left(l3,l4) "
                   "{l2=`v2`}">>),
    ?assertEqual(format_promql({call, label_replace, none,
                                [{[]}, <<"l1">>, <<"l2">>, <<>>, <<>>]}),
                 <<"label_replace({},`l1`,`l2`,``,``)">>),
    ?assertEqual(format_promql({call, vector, none, [1]}), <<"vector(1)">>),
    ?assertEqual(format_promql({call, rate, none,
                                [{range_vector, {[]}, <<"1m">>}]}),
                 <<"rate({}[1m])">>),
    ?assertEqual(format_promql({'*', [{'+', [{[{eq, <<"l1">>, <<"v1">>}]},
                                             {[{eq, <<"l2">>, <<"v2">>}]}]},
                                      {'+', [{[{eq, <<"l3">>, <<"v3">>}]},
                                             {[{eq, <<"l4">>, <<"v4">>}]}]}]}),
                <<"({l1=`v1`} + {l2=`v2`}) * ({l3=`v3`} + {l4=`v4`})">>),
    ?assertEqual(format_promql({'or', [{[{eq, <<"l1">>, <<"v1">>}]},
                                       {[{eq, <<"l1">>, <<"v1">>}]}]}),
                <<"{l1=`v1`}">>),
    ?assertEqual(format_promql({'or', [{[{eq, <<"name">>, <<"v1">>}]},
                                       {[{eq, <<"name">>, <<"v2">>}]}]}),
                <<"{name=~`v1|v2`}">>),
    ?assertEqual(format_promql({'or', [{[{eq, <<"name">>, <<"v1">>}]},
                                       {[{eq, <<"name">>, <<"v2">>},
                                         {eq, <<"l2">>, <<"v3">>}]}]}),
                <<"{name=`v1`} or {name=`v2`,l2=`v3`}">>),
    ?assertEqual(format_promql(
                   {'or', [{call, irate, none,
                            [{range_vector,
                              {[{eq, <<"name">>, <<"v1">>},
                                {eq, <<"l1">>, <<"v2">>}]},
                              <<"1m">>}]},
                           {[{eq, <<"name">>, <<"v2">>},
                             {eq, <<"l1">>, <<"v2">>}]},
                           {call, irate, none,
                            [{range_vector,
                              {[{eq, <<"l1">>, <<"v2">>},
                                {eq_any, <<"name">>, [<<"v2">>,<<"v3">>]}]},
                              <<"1m">>}]}]}),
                 <<"{name=`v2`,l1=`v2`} or "
                   "irate({name=~`v1|v2|v3`,l1=`v2`}[1m])">>),
    ?assertEqual(format_promql(
                   {'or',
                    [{call, f, none, [1, {[{eq, <<"name">>, <<"v1">>}]}, 2]},
                     {call, f, none, [1, {[{eq, <<"name">>, <<"v1">>}]}, 2]}]}),
                 <<"f(1,{name=`v1`},2)">>),
    ?assertEqual(format_promql(
                   {'or',
                    [{call, f, none, [1, {[{eq, <<"name">>, <<"v1">>}]}, 2]},
                     {call, f, none, [1, {[{eq, <<"name">>, <<"v2">>}]}, 2]}]}),
                 <<"f(1,{name=~`v1|v2`},2)">>),
    ?assertEqual(format_promql(
                   {'or',
                    [{call, f, none, [1,{[{eq, <<"name">>, <<"v1">>}]}, 2]},
                     {call, f, none, [1,{[{eq, <<"name">>, <<"v2">>}]}, 3]}]}),
                 <<"f(1,{name=`v1`},2) or f(1,{name=`v2`},3)">>),
    ?assertEqual(format_promql(
                   {'or',
                    [{call, f, none, [1,{[{eq, <<"name">>, <<"v1">>}]}, 3]},
                     {call, f, none, [2,{[{eq, <<"name">>, <<"v2">>}]}, 3]}]}),
                 <<"f(1,{name=`v1`},3) or f(2,{name=`v2`},3)">>),
    ?assertEqual(format_promql(
                   {'or', [{call, f, none, [{[{eq, <<"name">>, <<"v1">>}]},
                                            {[{eq, <<"name">>, <<"v2">>}]}]},
                           {call, f, none, [{[{eq, <<"name">>, <<"v2">>}]},
                                            {[{eq, <<"name">>, <<"v1">>}]}]}]}),
                 <<"f({name=`v1`},{name=`v2`}) or f({name=`v2`},{name=`v1`})">>),
    ?assertEqual(format_promql({[{not_any, <<"name">>,
                                  [<<"v1">>, <<"v2">>, <<"v3">>]}]}),
                 <<"{name!~`v1|v2|v3`}">>).

-endif.
