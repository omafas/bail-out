%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(ns_log_browser).

-export([start/0]).
-export([log_exists/1, log_exists/2]).
-export([stream_logs/2, stream_logs/3, stream_logs/4]).

-include("ns_common.hrl").

-spec usage([1..255, ...], list()) -> no_return().
usage(Fmt, Args) ->
    io:format(Fmt, Args),
    usage().

-spec usage() -> no_return().
usage() ->
    io:format("Usage: <progname> -report_dir <dir> [-log <name>]~n"),
    halt(1).

start() ->
    Options = case parse_arguments([{h, 0, undefined, false},
                                    {report_dir, 1, undefined},
                                    {log, 1, undefined, ?DEBUG_LOG_FILENAME}],
                                   init:get_arguments()) of
                  {ok, O} ->
                      O;
                  {missing_option, K} ->
                      usage("option ~p is required~n", [K]);
                  {parse_error, {wrong_number_of_args, _, N}, K, _} ->
                      usage("option ~p requires ~p arguments~n", [K, N]);
                  Error -> usage("parse error: ~p~n", [Error])
              end,

    case proplists:get_value(h, Options) of
        true -> usage();
        false -> ok
    end,
    Dir = proplists:get_value(report_dir, Options),
    Log = proplists:get_value(log, Options),

    case log_exists(Dir, Log) of
        true ->
            stream_logs(Dir, Log,
                        fun (Data) ->
                                %% originally standard_io was used here
                                %% instead of group_leader(); though this is
                                %% perfectly valid (e.g. this tested in
                                %% otp/lib/kernel/tests/file_SUITE.erl) it makes
                                %% dialyzer unhappy
                                file:write(group_leader(), Data)
                        end);
        false ->
            usage("Requested log file ~p does not exist.~n", [Log])
    end.

%% Option parser
map_args(K, N, undefined, D, A) ->
    map_args(K, N, fun(L) -> L end, D, A);
map_args(K, N, F, D, A) ->
    try map_args(N, F, D, A)
    catch error:Reason ->
            erlang:error({parse_error, Reason, K, A})
    end.

map_args(_N, _F, D, []) -> D;
map_args(0, _F, _D, _A) -> true;
map_args(one_or_more, F, _D, A) ->
    L = lists:append(A),
    case length(L) of
        0 -> erlang:error(one_or_more);
        _ -> F(L)
    end;
map_args(many, F, _D, A) -> F(lists:append(A));
map_args(multiple, F, _D, A) -> F(A);
map_args(N, F, _D, A) when is_function(F, N) ->
    L = lists:append(A),
    case length(L) of
        N -> apply(F, L);
        X -> erlang:error({wrong_number_of_args, X, N})
    end;
map_args(N, F, _D, A) when is_function(F, 1) ->
    L = lists:append(A),
    N = length(L),
    F(L).

parse_arguments(Opts, Args) ->
    try lists:map(fun
                      ({K, N, F, D}) -> {K, map_args(K, N, F, D, proplists:get_all_values(K, Args))};
                      ({K, N, F}) ->
                         case proplists:get_all_values(K, Args) of
                             [] -> erlang:error({missing_option, K});
                             A -> {K, map_args(K, N, F, undefined, A)}
                         end
                 end, Opts) of
        Options -> {ok, Options}
    catch
        error:{missing_option, K} -> {missing_option, K};
        error:{parse_error, Reason, K, A} -> {parse_error, Reason, K, A}
    end.

log_exists(Log) ->
    {ok, Dir} = application:get_env(error_logger_mf_dir),
    log_exists(Dir, Log).

log_exists(Dir, Log) ->
    Path = filename:join(Dir, Log),
    filelib:is_regular(Path).

stream_logs(Log, Fn) ->
    {ok, Dir} = application:get_env(error_logger_mf_dir),
    stream_logs(Dir, Log, Fn).

stream_logs(Dir, Log, Fn) ->
    stream_logs(Dir, Log, Fn, 65536).

stream_logs(Dir, Log, Fn, ChunkSz) ->
    CurrentLog = filename:join(Dir, Log),
    PastLogs = find_past_logs(Dir, Log),

    lists:foreach(
      fun (P) ->
              case file:open(P, [raw, binary, compressed]) of
                  {ok, IO} ->
                      try
                          stream_logs_loop(IO, ChunkSz, Fn)
                      after
                          ok = file:close(IO)
                      end;
                 Error ->
                      (catch ?log_error("Failed to open file ~s: ~p", [P, Error])),
                      ok
              end
      end, PastLogs ++ [CurrentLog]).

stream_logs_loop(IO, ChunkSz, Fn) ->
    case file:read(IO, ChunkSz) of
        eof ->
            ok;
        {ok, Data} ->
            Fn(Data),
            stream_logs_loop(IO, ChunkSz, Fn)
    end.

find_past_logs(Dir, Log) ->
    {ok, RegExp} = re:compile("^" ++ Log ++ "\.([1-9][0-9]*)(\.gz)?$"),
    {ok, AllFiles} = file:list_dir(Dir),

    PastLogs0 =
        lists:foldl(
          fun (FileName, Acc) ->
                  FullPath = filename:join(Dir, FileName),
                  case filelib:is_regular(FullPath) of
                      true ->
                          case re:run(FileName, RegExp,
                                      [{capture, all_but_first, list}]) of
                              {match, [I | _]} ->
                                  [{FullPath, list_to_integer(I)} | Acc];
                              nomatch ->
                                  Acc
                          end;
                      false ->
                          Acc
                  end
          end, [], AllFiles),

    PastLogs1 = lists:sort(
                  fun ({_, X}, {_, Y}) ->
                          X > Y
                  end, PastLogs0),

    [P || {P, _} <- PastLogs1].
