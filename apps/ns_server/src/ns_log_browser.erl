%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ns_log_browser).

-export([start/0]).
-export([log_exists/1, log_exists/2]).
-export([stream_logs/2]).

-include("ns_common.hrl").

-define(CHUNK_SZ, 65536).

-spec usage([1..255, ...], list()) -> no_return().
usage(Fmt, Args) ->
    io:format(Fmt, Args),
    usage().

-spec usage() -> no_return().
usage() ->
    io:format("Usage: <progname> -report_dir <dir> [-log <name>"
              " -config_dir <dir> -bin_dir <dir> -password_prompt(for"
              " prompt to provide the node master password via stdin)]~n"),
    halt(1).

validate_encr_args(#{config_path := CPath,
                     bin_path := BPath}) ->
    AllUndefined = (undefined =:= CPath) andalso (undefined =:= BPath),
    AllDefined = (undefined =/= CPath) andalso (undefined =/= BPath),

    case AllUndefined orelse AllDefined of
        true ->
            ok;
        false ->
            io:format("-config_dir, -bin_dir params must all be specified "
                      "if any is specified~n"),
            usage()
    end.

start() ->
    Options = case parse_arguments([{h, 0, undefined, false},
                                    {report_dir, 1, undefined},
                                    {config_dir, 1, undefined, undefined},
                                    {bin_dir, 1, undefined, undefined},
                                    {password_prompt, 0, undefined, undefined},
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

    HiddenPassword =
        case proplists:get_value(password_prompt, Options, undefined) of
            true ->
                P = io:get_line(""),
                ?HIDE(string:trim(P));
            undefined ->
                undefined
        end,

    LogDir = proplists:get_value(report_dir, Options),
    EncrArgs = #{config_path => proplists:get_value(config_dir, Options),
                 bin_path => proplists:get_value(bin_dir, Options),
                 hidden_password => HiddenPassword},

    validate_encr_args(EncrArgs),

    BuildDsOptsFn =
        fun (#{config_path := CPath,
               bin_path := BPath}) when CPath =:= undefined;
                                        BPath =:= undefined ->
                {error, missing_encr_browser_opts};
            (#{config_path := CPath,
               bin_path := BPath,
               hidden_password := HiddenPass}) ->
                Opts = #{config_path_override => CPath,
                         bin_path_override => BPath},
                case HiddenPass of
                    undefined ->
                        {ok, Opts};
                    _ ->
                        {ok, Opts#{hidden_pass => HiddenPass}}
                end
        end,

    GetDSFn =
        fun() ->
                case BuildDsOptsFn(EncrArgs) of
                    {ok, BootOpts} ->
                        cb_deks_raw_utils:bootstrap_get_deks(logDek, BootOpts);
                    {error, _} = E ->
                        E
                end
        end,

    LoggerFn =
        fun(Fmt, Args) ->
            io:format("---->Log Browser Output Start<----\n" ++ Fmt ++ "\n"
                      "<----Log Browser Output End---->\n", Args)
        end,

    LogF = proplists:get_value(log, Options),
    case {"memcached.log" =:= LogF, log_exists(LogDir, LogF)} of
        {false, false} ->
            usage("Requested log file ~p does not exist.~n", [LogF]);
        {_, _} ->
            stream_logs(LogF,
                        LogDir,
                        fun (Data) ->
                                %% originally standard_io was used here
                                %% instead of group_leader(); though this is
                                %% perfectly valid (e.g. this tested in
                                %% otp/lib/kernel/tests/file_SUITE.erl) it makes
                                %% dialyzer unhappy
                                file:write(group_leader(), Data)
                        end, GetDSFn, LoggerFn)
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

stream_logs(LogF, ConsumeFn) ->
    {ok, LogPath} = application:get_env(error_logger_mf_dir),

    LoggerFn =
        fun(Fmt, Args) ->
            ?log_error(Fmt, Args)
         end,

    GetDSFn =
        fun() ->
            cb_crypto:fetch_deks_snapshot(logDek)
        end,

    stream_logs(LogF, LogPath, ConsumeFn, GetDSFn, LoggerFn).

stream_logs(LogF, LogPath, ConsumeFn, GetDSFn, LoggerFn) ->
    AllLogs = find_all_logs(LogPath, LogF),

    StdFileStreamFn =
        fun (FPath) ->
                case file:open(FPath, [raw, binary, compressed]) of
                    {ok, IO} ->
                        try
                            stream_logs_loop(IO, ?CHUNK_SZ, ConsumeFn)
                        after
                            ok = file:close(IO)
                        end;
                    Error ->
                        LoggerFn("Failed to open file ~s: ~p", [FPath, Error]),
                        ok
                end
        end,

    EncrFileStreamFn =
        fun (FPath, {ok, DS}) ->
                Fn = fun(Chunk, {AccList, AccSize}) ->
                             case AccSize >= ?CHUNK_SZ of
                                 true ->
                                     ConsumeFn(AccList),
                                     {ok, {[Chunk], iolist_size(Chunk)}};
                                 false ->
                                     NewSize = iolist_size(Chunk) + AccSize,
                                     {ok, {[AccList, Chunk], NewSize}}
                             end
                     end,
                Opts = #{read_chunk_size => ?CHUNK_SZ},
                case cb_crypto:read_file_chunks(FPath, Fn, {[], 0}, DS, Opts) of
                    {ok, {Rest, _Size}} ->
                        ConsumeFn(Rest);
                    {error, _, {Rest, _Size}} = Error ->
                        ConsumeFn(Rest),
                        LoggerFn("Read file chunks failure for file: ~s: error:"
                                 " ~p",
                                 [FPath, Error]),
                        ok
                end;
            (FPath,  {error, missing_encr_browser_opts}) ->
                LoggerFn("Params -config_dir, -bin_dir are all required "
                         "because log ~p is encrypted", [FPath]),
                ok;
            (FPath, {error, _} = Error) ->
                LoggerFn("Failed to get keys to decrypt file ~p: error: ~p",
                         [FPath, Error]),
                ok
        end,

    GetAndCacheDSResFn =
        fun(undefined, _FilePath) ->
                GetDSFn();
           ({ok, DS} = CurrentDsRes, FilePath) ->
                case cb_crypto:can_ds_decrypt_file(FilePath, DS) of
                    true ->
                        CurrentDsRes;
                    false ->
                        GetDSFn()
                end;
           ({error, _} = Error, _FilePath) ->
                Error
        end,

    lists:foldl(
      fun (FilePath, Acc) ->
              case cb_crypto:is_file_encrypted(FilePath) of
                  true ->
                      DsRes = GetAndCacheDSResFn(Acc, FilePath),
                      EncrFileStreamFn(FilePath, DsRes),
                      DsRes;
                  false ->
                      StdFileStreamFn(FilePath),
                      Acc
              end
      end, undefined, AllLogs),
    ok.

stream_logs_loop(IO, ChunkSz, ConsumeFn) ->
    case file:read(IO, ChunkSz) of
        eof ->
            ok;
        {ok, Data} ->
            ConsumeFn(Data),
            stream_logs_loop(IO, ChunkSz, ConsumeFn)
    end.

find_all_logs(LogPath, LogF) ->
    case LogF of
        "memcached.log" ->
            {ok, RegExp} =
                re:compile("^" ++ LogF ++ "\.([0-9][0-9]*)(\.txt|\.cef)?$"),
            find_logs(LogPath, RegExp, fun(X, Y) -> X < Y end);
        _ ->
            {ok, RegExp} =
                re:compile("^" ++ LogF ++ "\.([1-9][0-9]*)(\.gz)?$"),
            BaseLog = filename:join(LogPath, LogF),
            find_logs(LogPath, RegExp, fun(X, Y) -> X > Y end) ++ [BaseLog]
    end.

find_logs(Dir, RegExp, SortFn) ->
    {ok, AllFiles} = file:list_dir(Dir),

    Logs0 =
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

    Logs1 = lists:sort(
              fun ({_, X}, {_, Y}) ->
                  SortFn(X, Y)
              end, Logs0),

    [P || {P, _} <- Logs1].