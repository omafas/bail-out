%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

%% This module partially stolen from lager.


%% This parse transform defines the following pseudo-functions.
%%
%% ale:sync(Logger)
%%
%% Ensures that all the log messages have reached all the sinks and
%% have been processed.
%%
%%
%% ale:get_effective_loglevel(Logger)
%%
%% Returns the least restricitve loglevel that would still result in
%% something being logged to at least one of the logger's sinks.
%%
%%
%% ale:is_loglevel_enabled(Logger, LogLevel)
%%
%% Returns true if logging a message with the `LogLevel` will be
%% visible at least in one of the logger's sinks.
%%
%%
%% ale:debug(Logger, Msg),
%% ale:debug(Logger, Fmt, Args)
%% ale:xdebug(Logger, UserData, Msg),
%% ale:xdebug(Logger, UserData, Fmt, Args)
%%
%% ale:info(Logger, Msg),
%% ale:info(Logger, Fmt, Args)
%% ale:xinfo(Logger, UserData, Msg),
%% ale:xinfo(Logger, UserData, Fmt, Args)
%%
%% ale:warn(Logger, Msg),
%% ale:warn(Logger, Fmt, Args)
%% ale:xwarn(Logger, UserData, Msg),
%% ale:xwarn(Logger, UserData, Fmt, Args)
%%
%% ale:error(Logger, Msg),
%% ale:error(Logger, Fmt, Args)
%% ale:xerror(Logger, UserData, Msg),
%% ale:xerror(Logger, UserData, Fmt, Args)
%%
%% ale:critical(Logger, Msg),
%% ale:critical(Logger, Fmt, Args)
%% ale:xcritical(Logger, UserData, Msg),
%% ale:xcritical(Logger, UserData, Fmt, Args)
%%
%% Logs a message to a `Logger` with a specific log level. x* versions
%% take an `UserData` argument that is passed as is to formatter and
%% sinks.
%%
%%
%% ale:log(Logger, LogLevel, Msg)
%% ale:log(Logger, LogLevel, Fmt, Args)
%% ale:xlog(Logger, LogLevel, UserData, Msg)
%% ale:xlog(Logger, LogLevel, UserData, Fmt, Args)
%%
%% Generalized versions of logging pseudo-functions. The main
%% difference is that `LogLevel` doesn't have to be known at compile
%% time.
%%
%%
%% In all the pseudo-functions above `Logger` argument can be an
%% arbitrary expression. The case where actual expression is an atom
%% known at compile-time optimized to call a module generated
%% for the logger directly.

-module(ale_transform).

-include("ale.hrl").

-export([parse_transform/2]).

parse_transform(AST, _Options) ->
    walk_ast([], AST).

walk_ast(Acc, []) ->
    lists:reverse(Acc);
walk_ast(Acc, [{attribute, _, module, {Module, _PmodArgs}}=H|T]) ->
    put(module, Module),
    walk_ast([H|Acc], T);
walk_ast(Acc, [{attribute, _, module, Module}=H|T]) ->
    put(module, Module),
    walk_ast([H|Acc], T);
walk_ast(Acc, [{function, Line, Name, Arity, Clauses}|T]) ->
    put(function, Name),
    walk_ast([{function, Line, Name, Arity,
               walk_clauses([], Clauses)}|Acc], T);
walk_ast(Acc, [H|T]) ->
    walk_ast([H|Acc], T).

walk_clauses(Acc, []) ->
    lists:reverse(Acc);
walk_clauses(Acc, [{clause, Line, Arguments, Guards, Body}|T]) ->
    walk_clauses([{clause, Line, Arguments, Guards,
                   walk_body([], Body)}|Acc], T).

walk_body(Acc, []) ->
    lists:reverse(Acc);
walk_body(Acc, [H|T]) ->
    walk_body([transform(H) | Acc], T).

transform({call, Line, {remote, _,
                        {atom, _, ale},
                        {atom, _, Fn}},
           [LoggerExpr]})
  when Fn =:= sync;
       Fn =:= get_effective_loglevel ->

    {call, Line,
     {remote, Line,
      logger_impl_expr(LoggerExpr), {atom, Line, Fn}}, []};
transform({call, Line, {remote, _,
                        {atom, _, ale},
                        {atom, _, Fn}},
           [LoggerExpr, LogLevelExpr]} = Stmt)
  when Fn =:= is_loglevel_enabled ->
    case valid_loglevel_expr(LogLevelExpr) of
        true ->
            {call, Line,
             {remote, Line,
              logger_impl_expr(LoggerExpr), {atom, Line, Fn}}, [LogLevelExpr]};
        false ->
            Stmt
    end;
transform({call, Line, {remote, _,
                        {atom, _, ale},
                        {atom, _, LogFn}},
           [LoggerExpr, LogLevelExpr | Args]} = Stmt)
  when LogFn =:= log; LogFn =:= xlog ->
    Extended = LogFn =:= xlog,

    case valid_loglevel_expr(LogLevelExpr) andalso
        valid_args(Extended, Args) of
        true ->
            LogLevelExpr1 =
                case Extended of
                    false ->
                        LogLevelExpr;
                    true ->
                        extended_loglevel_expr(LogLevelExpr)
                end,

            emit_logger_call(LoggerExpr, LogLevelExpr1, transform(Args), Line);
        false ->
            Stmt
    end;
transform({call, Line, {remote, _,
                        {atom, _, ale},
                        {atom, _, LogLevel} = LogLevelExpr},
           [LoggerExpr | Args]} = Stmt) ->
    case valid_loglevel(LogLevel) andalso
        valid_args(extended_loglevel(LogLevel), Args) of
        true ->
            emit_logger_call(LoggerExpr, LogLevelExpr, transform(Args), Line);
        false ->
            Stmt
    end;
transform(Stmt) when is_tuple(Stmt) ->
    list_to_tuple(transform(tuple_to_list(Stmt)));
transform(Stmt) when is_list(Stmt) ->
    [transform(S) || S <- Stmt];
transform(Stmt) ->
    Stmt.

emit_logger_call(LoggerNameExpr, LogLevelExpr, Args, Line) ->
    ArgsLine = get_line(LogLevelExpr),

    {call, Line,
     {remote, Line,
      logger_impl_expr(LoggerNameExpr),
      LogLevelExpr},
     [{atom, ArgsLine, get(module)},
      {atom, ArgsLine, get(function)},
      {integer, ArgsLine, Line} |
      Args]}.

extended_loglevel_expr_rt(Line, Expr) ->
    {call, Line,
     {remote, Line,
      {atom, Line, ale_codegen},
      {atom, Line, extended_impl}},
     [Expr]}.

extended_loglevel_expr({atom, Line, LogLevel}) ->
    {atom, Line, ale_codegen:extended_impl(LogLevel)};
extended_loglevel_expr({var, Line, _} = Expr) ->
    extended_loglevel_expr_rt(Line, Expr);
extended_loglevel_expr({call, Line, _, _} = Expr) ->
    extended_loglevel_expr_rt(Line, Expr).

extended_loglevel(LogLevel) ->
    ExtendedLogLevels = [list_to_atom([$x | atom_to_list(LL)])
                         || LL <- ?LOGLEVELS],
    lists:member(LogLevel, ExtendedLogLevels).

normalize_loglevel(LogLevel) ->
    case extended_loglevel(LogLevel) of
        false ->
            LogLevel;
        true ->
            LogLevelStr = atom_to_list(LogLevel),
            [$x | LogLevelStr1] = LogLevelStr,
            list_to_atom(LogLevelStr1)
    end.

valid_loglevel(LogLevel) ->
    NormLogLevel = normalize_loglevel(LogLevel),
    lists:member(NormLogLevel, ?LOGLEVELS).

valid_loglevel_expr({atom, _Line, LogLevel}) ->
    lists:member(LogLevel, ?LOGLEVELS);
valid_loglevel_expr(_Other) ->
    true.

get_line(Expr) ->
    element(2, Expr).

valid_args(ExtendedCall, Args) ->
    N = length(Args),

    case ExtendedCall of
        false ->
            N =:= 1 orelse N =:= 2;
        true ->
            N =:= 2 orelse N =:= 3
    end.

logger_impl_expr(LoggerExpr) ->
    Line = get_line(LoggerExpr),

    case LoggerExpr of
        {atom, _, LoggerAtom} ->
            {atom, Line, ale_codegen:logger_impl(LoggerAtom)};
        _ ->
            {call, Line,
             {remote, Line,
              {atom, Line, ale_codegen},
              {atom, Line, logger_impl}},
             [LoggerExpr]}
    end.
