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
%% ale:debug(Logger, Fmt, Args, Opts)
%% ale:xdebug(Logger, UserData, Msg),
%% ale:xdebug(Logger, UserData, Fmt, Args)
%% ale:xdebug(Logger, UserData, Fmt, Args, Opts)
%%
%% ale:info(Logger, Msg),
%% ale:info(Logger, Fmt, Args)
%% ale:info(Logger, Fmt, Args, Opts)
%% ale:xinfo(Logger, UserData, Msg),
%% ale:xinfo(Logger, UserData, Fmt, Args)
%% ale:xinfo(Logger, UserData, Fmt, Args, Opts)
%%
%% ale:warn(Logger, Msg),
%% ale:warn(Logger, Fmt, Args)
%% ale:warn(Logger, Fmt, Args, Opts)
%% ale:xwarn(Logger, UserData, Msg),
%% ale:xwarn(Logger, UserData, Fmt, Args)
%% ale:xwarn(Logger, UserData, Fmt, Args, Opts)
%%
%% ale:error(Logger, Msg),
%% ale:error(Logger, Fmt, Args)
%% ale:error(Logger, Fmt, Args, Opts)
%% ale:xerror(Logger, UserData, Msg),
%% ale:xerror(Logger, UserData, Fmt, Args)
%% ale:xerror(Logger, UserData, Fmt, Args, Opts)
%%
%% ale:critical(Logger, Msg),
%% ale:critical(Logger, Fmt, Args)
%% ale:critical(Logger, Fmt, Args, Opts)
%% ale:xcritical(Logger, UserData, Msg),
%% ale:xcritical(Logger, UserData, Fmt, Args)
%% ale:xcritical(Logger, UserData, Fmt, Args, Opts)
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

-define(CALL_LOGGER, '__call_logger').

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
walk_ast(Acc, [{function, Location, Name, Arity, Clauses}|T]) ->
    put(function, Name),
    walk_ast([{function, Location, Name, Arity,
               walk_clauses([], Clauses)}|Acc], T);
walk_ast(Acc, [{eof, Location} = EOF]) ->
    Fns = lists:reverse(
            lists:append([mk_call_logger(Location, 0),
                          mk_call_logger(Location, 1),
                          mk_call_logger(Location, 4),
                          mk_call_logger(Location, 5),
                          mk_call_logger(Location, 6),
                          mk_call_logger(Location, 7)])),

    walk_ast([EOF | Fns] ++ Acc, []);
walk_ast(Acc, [H|T]) ->
    walk_ast([H|Acc], T).

walk_clauses(Acc, []) ->
    lists:reverse(Acc);
walk_clauses(Acc, [{clause, Location, Arguments, Guards, Body}|T]) ->
    walk_clauses([{clause, Location, Arguments, Guards,
                   walk_body([], Body)}|Acc], T).

walk_body(Acc, []) ->
    lists:reverse(Acc);
walk_body(Acc, [H|T]) ->
    walk_body([transform(H) | Acc], T).

mk_call_logger(Location, NumArgs) ->
    Arity = NumArgs + 2,
    Args = [{var, Location, list_to_atom("Arg" ++ integer_to_list(I))} ||
               I <- lists:seq(0, NumArgs - 1)],

    [{attribute, Location, compile, {inline, [{?CALL_LOGGER, Arity}]}},
     {attribute, Location, compile,
      {nowarn_unused_function, [{?CALL_LOGGER, Arity}]}},
     {function,
      Location,
      ?CALL_LOGGER, Arity,
      [{clause,
        Location,
        [{var, Location, 'Logger'}, {var, Location, 'Fn'} | Args],
        [],
        [{call,
          Location,
          {remote, Location, {var, Location, 'Logger'}, {var, Location, 'Fn'}},
          Args}]}]}].

transform({call, Location, {remote, _,
                            {atom, _, ale},
                            {atom, _, Fn}},
           [LoggerExpr]})
  when Fn =:= sync;
       Fn =:= get_effective_loglevel ->

    {call, Location,
     {atom, Location, ?CALL_LOGGER},
     [logger_impl_expr(LoggerExpr), {atom, Location, Fn}]};
transform({call, Location, {remote, _,
                            {atom, _, ale},
                            {atom, _, Fn}},
           [LoggerExpr, LogLevelExpr]} = Stmt)
  when Fn =:= is_loglevel_enabled ->
    case valid_loglevel_expr(LogLevelExpr) of
        true ->
            {call, Location,
             {atom, Location, ?CALL_LOGGER},
             [logger_impl_expr(LoggerExpr),
              {atom, Location, Fn},
              LogLevelExpr]};
        false ->
            Stmt
    end;
transform({call, Location, {remote, _,
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

            emit_logger_call(LoggerExpr, LogLevelExpr1, transform(Args), Location);
        false ->
            Stmt
    end;
transform({call, Location, {remote, _,
                            {atom, _, ale},
                            {atom, _, LogLevel} = LogLevelExpr},
           [LoggerExpr | Args]} = Stmt) ->
    case valid_loglevel(LogLevel) andalso
        valid_args(extended_loglevel(LogLevel), Args) of
        true ->
            emit_logger_call(LoggerExpr, LogLevelExpr, transform(Args), Location);
        false ->
            Stmt
    end;
transform(Stmt) when is_tuple(Stmt) ->
    list_to_tuple(transform(tuple_to_list(Stmt)));
transform(Stmt) when is_list(Stmt) ->
    [transform(S) || S <- Stmt];
transform(Stmt) ->
    Stmt.

emit_logger_call(LoggerNameExpr, LogLevelExpr, Args, Location) ->
    ArgsLocation = get_location(LogLevelExpr),
    Line = case Location of
               {L, _Col} -> L;
               L when is_integer(L) -> L
           end,

    {call, Location,
     {atom, Location, ?CALL_LOGGER},
     [logger_impl_expr(LoggerNameExpr),
      LogLevelExpr,
      {atom, ArgsLocation, get(module)},
      {atom, ArgsLocation, get(function)},
      {integer, ArgsLocation, Line} | Args]}.

extended_loglevel_expr_rt(Location, Expr) ->
    {call, Location,
     {remote, Location,
      {atom, Location, ale_codegen},
      {atom, Location, extended_impl}},
     [Expr]}.

extended_loglevel_expr({atom, Location, LogLevel}) ->
    {atom, Location, ale_codegen:extended_impl(LogLevel)};
extended_loglevel_expr({var, Location, _} = Expr) ->
    extended_loglevel_expr_rt(Location, Expr);
extended_loglevel_expr({call, Location, _, _} = Expr) ->
    extended_loglevel_expr_rt(Location, Expr).

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

valid_loglevel_expr({atom, _Location, LogLevel}) ->
    lists:member(LogLevel, ?LOGLEVELS);
valid_loglevel_expr(_Other) ->
    true.

get_location(Expr) ->
    element(2, Expr).

valid_args(ExtendedCall, Args) ->
    N = length(Args),

    case ExtendedCall of
        false ->
            N =:= 1 orelse N =:= 2 orelse N =:= 3;
        true ->
            N =:= 2 orelse N =:= 3 orelse N =:= 4
    end.

logger_impl_expr(LoggerExpr) ->
    Location = get_location(LoggerExpr),

    case LoggerExpr of
        {atom, _, LoggerAtom} ->
            {atom, Location, ale_codegen:logger_impl(LoggerAtom)};
        _ ->
            {call, Location,
             {remote, Location,
              {atom, Location, ale_codegen},
              {atom, Location, logger_impl}},
             [LoggerExpr]}
    end.
