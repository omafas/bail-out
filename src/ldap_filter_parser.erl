%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ldap_filter_parser).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    parse/1
]).

parse(StringInput) ->
    case tokens(StringInput) of
        {ok, Tokens} ->
            try ldap_rfc4515_parser:parse(Tokens) of
                {ok, Filter} ->
                    {ok, Filter};
                {error, {_Line, Module, Error}} ->
                    {error, Module:format_error(Error)}
            catch
                _:_ -> {error, "syntax error"}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% We can't use lexer:string/1 because we need state to
%% parse filters like (member=uid=al,dc=example).
%% First '=' here is a token but all other '=' chars are
%% just regular chars in an attribute value (not tokens)
%% so valid result should be:
%%   [{'(', 1}, {str, 1, "member"}, {'=', 1},
%%    {str, 1, "uid=al,dc=example"}, {')', 1}]
tokens(String) -> tokens(String, []).
tokens("", Res) -> {ok, lists:reverse(Res)};
tokens(String, Res) ->
    case ldap_rfc4515_lexer:token([], String) of
        {done, {ok, Token, Line}, Rest} ->
            case is_equals(Token) of
                true ->
                    {Tokens2, Rest2} = parse_value(Rest, Line),
                    tokens(Rest2, Tokens2 ++ [Token | Res]);
                false ->
                    tokens(Rest, [Token | Res])
            end;
        {done, {eof, _}, _Rest} ->
            {error, "unexpected end of file"};
        {done, {error, {_Line, Module, Error}, _}, _} ->
            {error, Module:format_error(Error)};
        {more, _} ->
            {error, "incomplete"}
    end.

is_equals({'~=', _}) -> true;
is_equals({'>=', _}) -> true;
is_equals({'<=', _}) -> true;
is_equals({'=', _}) -> true;
is_equals({':=', _}) -> true;
is_equals({'=*', _}) -> true;
is_equals(_) -> false.

parse_value(Str, Line) ->
    {Str2, Rest} =
        case string:split(Str, ")") of
            [V, R] -> {V, [$) | R]};
            [V] -> {V, ""}
        end,
    StrTokens = [{str, Line, T} || T <- string:split(Str2, "*", all)],
    Tokens = [T || T <- lists:join({'*', Line}, StrTokens),
                   T =/= {str, Line, ""}],
    {lists:reverse(Tokens), Rest}.


-ifdef(TEST).
parse_test_() ->
    Parse = fun (Str) ->
                    {ok, Filter} = parse(Str),
                    Filter
            end,
    [
        ?_assertEqual(eldap:equalityMatch("cn", "val"), Parse("(cn=val)")),
        ?_assertEqual(eldap:present("objectClass"), Parse("(objectClass=*)")),
        ?_assertEqual(eldap:'and'([eldap:approxMatch("object", "user"),
                                   eldap:greaterOrEqual("val", "100"),
                                   eldap:'not'(eldap:lessOrEqual("val", "200")),
                                   eldap:equalityMatch("cn", "andy")]),
                      Parse("(&(object~=user)(val>=100)(!(val<=200))(cn=andy))")),
        ?_assertEqual(eldap:'or'([eldap:equalityMatch("sn", "Smith"),
                                  eldap:equalityMatch("sn", "Johnson")]),
                      Parse("(|(sn=Smith)(sn=Johnson))")),

        ?_assertEqual(eldap:substrings("sn", [{initial, "init"}]),
                      Parse("(sn=init*)")),
        ?_assertEqual(eldap:substrings("sn", [{initial, "init"}, {any, "any"}]),
                      Parse("(sn=init*any*)")),
        ?_assertEqual(eldap:substrings("sn", [{initial, "init"}, {any, "any"},
                                              {final, "fin"}]),
                      Parse("(sn=init*any*fin)")),
        ?_assertEqual(eldap:substrings("sn", [{initial, "init"}, {any, "any1"},
                                              {any, "any2"}]),
                      Parse("(sn=init*any1*any2*)")),
        ?_assertEqual(eldap:substrings("sn", [{initial, "init"}, {any, "any1"},
                                              {any, "any2"}, {final, "fin"}]),
                      Parse("(sn=init*any1*any2*fin)")),
        ?_assertEqual(eldap:substrings("sn", [{any, "any"}]),
                      Parse("(sn=*any*)")),
        ?_assertEqual(eldap:substrings("sn", [{any, "any1"}, {any, "any2"}]),
                      Parse("(sn=*any1*any2*)")),
        ?_assertEqual(eldap:substrings("sn", [{final, "fin"}]),
                      Parse("(sn=*fin)")),
        ?_assertEqual(eldap:substrings("sn", [{any, "any"}, {final, "fin"}]),
                      Parse("(sn=*any*fin)")),
        ?_assertEqual(eldap:substrings("sn", [{any, "any1"}, {any, "any2"},
                                              {final, "fin"}]),
                      Parse("(sn=*any1*any2*fin)")),

        ?_assertEqual(eldap:extensibleMatch("value", [{type,"sn"},
                                                      {matchingRule, "rule"},
                                                      {dnAttributes,true}]),
                      Parse("(sn:dn:rule:=value)")),
        ?_assertEqual(eldap:extensibleMatch("value", [{type,"sn"},
                                                      {matchingRule, "rule"}]),
                      Parse("(sn:rule:=value)")),
        ?_assertEqual(eldap:extensibleMatch("value", [{matchingRule, "rule"}]),
                      Parse("(:rule:=value)")),
        ?_assertEqual(eldap:extensibleMatch("value", [{dnAttributes, true}]),
                      Parse("(:dn:=value)")),
        ?_assertEqual(eldap:extensibleMatch("value", [{type,"sn"},
                                                      {dnAttributes,true}]),
                      Parse("(sn:dn:=value)")),
        ?_assertEqual(eldap:equalityMatch("cn", "uid=al,dc=example,dc=com"),
                      Parse("(cn=uid=al,dc=example,dc=com)"))
    ].
-endif.
