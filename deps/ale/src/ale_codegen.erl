%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ale_codegen).

-export([load_logger/4, logger_impl/1, extended_impl/1, logger/4]).

-include("ale.hrl").

-define(CHARS_LIMIT_DEFAULT, 50000).

logger_impl(Logger) when is_atom(Logger) ->
    logger_impl(atom_to_list(Logger));
logger_impl(Logger) ->
    list_to_atom("ale_logger-" ++ Logger).

extended_impl(LogLevel) ->
    list_to_atom([$x | atom_to_list(LogLevel)]).

load_logger(LoggerName, LogLevel, Formatter, Sinks) ->
    SourceCode = logger(LoggerName, LogLevel, Formatter, Sinks),
    {module, _} = dynamic_compile:load_from_string(SourceCode),
    ok.

logger(LoggerName, LogLevel, Formatter, Sinks) ->
    LoggerNameStr = atom_to_list(LoggerName),
    lists:flatten([header(LoggerNameStr),
                   "\n",
                   exports(),
                   "\n",
                   definitions(LoggerNameStr, LogLevel, Formatter, Sinks)]).

header(LoggerName) ->
    io_lib:format("-module('~s').~n", [atom_to_list(logger_impl(LoggerName))]).

exports() ->
    ["-export([sync/0]).\n",
     "-export([get_effective_loglevel/0]).\n",
     "-export([is_loglevel_enabled/1]).\n",
     [io_lib:format("-export([~p/4, ~p/5, ~p/6, x~p/5, x~p/6, x~p/7]).~n",
                    [LogLevel, LogLevel, LogLevel, LogLevel, LogLevel, LogLevel]) ||
         LogLevel <- ?LOGLEVELS]].

definitions(LoggerName, LoggerLogLevel, Formatter, Sinks) ->
    [sync_definitions(Sinks),
     loglevel_related_definitions(LoggerLogLevel, Sinks),
     lists:map(
       fun (LogLevel) ->
               loglevel_definitions(LoggerName, LoggerLogLevel,
                                    LogLevel, Formatter, Sinks)
       end, ?LOGLEVELS)].

sync_definitions(Sinks) ->
    Syncs =
        [io_lib:format("ok = gen_server:call(~p, sync, infinity),\n", [SinkId])
         || {_, SinkId, _, _} <- Sinks],

    ["sync() -> ",
     Syncs,
     "ok.\n"].

loglevel_related_definitions(LoggerLogLevel, Sinks) ->
    SinkLogLevels = [L || {_, _, L, _} <- Sinks],
    EffectiveLogLevel = ale_utils:effective_loglevel(LoggerLogLevel, SinkLogLevels),

    [io_lib:format("get_effective_loglevel() -> ~p.\n\n", [EffectiveLogLevel]),
     [ale_utils:intersperse(
        ";\n",
        [io_lib:format("is_loglevel_enabled(~p) -> ~p",
                       [L, ale_utils:loglevel_enabled(L, EffectiveLogLevel)])
         || L <- ?LOGLEVELS]),
      ".\n\n"]].

loglevel_definitions(LoggerName, LoggerLogLevel, LogLevel, Formatter, Sinks) ->
    {Preformatted, Raw} =
        case ale_utils:loglevel_enabled(LogLevel, LoggerLogLevel) of
            false ->
                {[], []};
            true ->
                lists:foldl(
                  fun ({_, Sink, SinkLogLevel, SinkMeta}, {P, R} = Acc) ->
                          Enabled =
                              ale_utils:loglevel_enabled(LogLevel, SinkLogLevel),

                          case Enabled of
                              true ->
                                  SinkType = proplists:get_value(type, SinkMeta, preformatted),
                                  Async = proplists:get_value(async, SinkMeta, false),

                                  case SinkType of
                                      preformatted ->
                                          {[{Sink, Async} | P], R};
                                      raw ->
                                          {P, [{Sink, Async} | R]}
                                  end;
                              false ->
                                  Acc
                          end
                  end, {[], []}, Sinks)
        end,

    [generic_loglevel(LoggerName, LogLevel, Formatter, Preformatted, Raw),
     "\n",
     loglevel_1(LogLevel),
     loglevel_2(LogLevel),
     loglevel_3(LogLevel),
     "\n",
     xloglevel_1(LogLevel),
     xloglevel_2(LogLevel),
     xloglevel_3(LogLevel),
     "\n"].

generic_loglevel(LoggerName, LogLevel, Formatter, Preformatted, Raw) ->
    %% inline generated function
    [io_lib:format("-compile({inline, [generic_~p/7]}).~n", [LogLevel]),

     io_lib:format("generic_~p(M, F, L, Data, Fmt, Args, Opts) ->~n", [LogLevel]),

     case Preformatted =/= [] orelse Raw =/= [] of
         true ->
             io_lib:format(
               "Info = ale_utils:assemble_info(~s, ~p, M, F, L, Data),"
               "UserMsg = case Args =/= [] of~n"
               "              true ->~n"
               "                  CharsLimit = proplists:get_value(chars_limit, Opts, ~B),~n"
               "                  %% io_lib:format/3 has had bugs and therefore~n"
               "                  %% to avoid any logging errors, the following~n"
               "                  %% try catch block has been added.~n"
               "                  %% https://github.com/erlang/otp/issues/5053~n"
               "                  try~n"
               "                    io_lib:format(Fmt, Args, [{chars_limit, CharsLimit}])~n"
               "                  catch~n"
               "                    _:_ ->~n"
               "                      io_lib:format(Fmt, Args)~n"
               "                  end;~n"
               "              false -> Fmt~n"
               "          end,~n",
               [LoggerName, LogLevel, ?CHARS_LIMIT_DEFAULT]);
         false ->
             ""
     end,

     case Preformatted =/= [] of
         true ->
             io_lib:format(
               "LogMsg0 = ~p:format_msg(Info, UserMsg),"
               "LogMsg = case unicode:characters_to_binary(LogMsg0) of"
               "             V when is_binary(V) ->"
               "                 V;"
               "             {_, Partial, _} ->"
               "                 <<Partial/binary, \"...truncated due to encoding error\\n\">>"
               "         end,", [Formatter]);
         false ->
             ""
     end,

     lists:map(
       fun ({Sink, Async}) ->
               case Async of
                   true ->
                       io_lib:format(
                         "ok = gen_server:cast('~s', {log, LogMsg}),",
                         [Sink]);
                   false ->
                       io_lib:format(
                         "ok = gen_server:call('~s', {log, LogMsg}, infinity),",
                         [Sink])
               end
       end, Preformatted),

     lists:map(
       fun ({Sink, Async}) ->
               case Async of
                   true ->
                       io_lib:format(
                         "ok = gen_server:cast('~s', {raw_log, Info, UserMsg}),",
                         [Sink]);
                   false ->
                       io_lib:format(
                         "ok = gen_server:call('~s', {raw_log, Info, UserMsg}, infinity),",
                         [Sink])
               end
       end, Raw),

     "ok.\n"].

loglevel_1(LogLevel) ->
    io_lib:format(
      "~p(M, F, L, Msg) -> "
      "generic_~p(M, F, L, undefined, Msg, [], []).~n",
      [LogLevel, LogLevel]).

xloglevel_1(LogLevel) ->
    io_lib:format(
      "x~p(M, F, L, Data, Msg) -> "
      "generic_~p(M, F, L, Data, Msg, [], []).~n",
      [LogLevel, LogLevel]).

loglevel_2(LogLevel) ->
    io_lib:format(
      "~p(M, F, L, Fmt, Args) -> "
      "generic_~p(M, F, L, undefined, Fmt, Args, []).~n",
      [LogLevel, LogLevel]).

xloglevel_2(LogLevel) ->
    io_lib:format(
      "x~p(M, F, L, Data, Fmt, Args) -> "
      "generic_~p(M, F, L, Data, Fmt, Args, []).~n",
      [LogLevel, LogLevel]).

loglevel_3(LogLevel) ->
    io_lib:format(
      "~p(M, F, L, Fmt, Args, Opts) -> "
      "generic_~p(M, F, L, undefined, Fmt, Args, Opts).~n",
      [LogLevel, LogLevel]).

xloglevel_3(LogLevel) ->
    io_lib:format(
      "x~p(M, F, L, Data, Fmt, Args, Opts) -> "
      "generic_~p(M, F, L, Data, Fmt, Args, Opts).~n",
      [LogLevel, LogLevel]).
