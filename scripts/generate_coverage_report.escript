#!/usr/bin/env escript
%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-mode(compile).

main(Args) ->
    argparse:run(Args, cli(), #{progname => generate_coverage_report}).

cli() ->
    #{
        arguments => [
            #{name => report_dir, long => "-report-dir", required => true,
              help => "Directory where coverage reports will be generated"},
            #{name => format, long => "-format", type => {atom, [html, txt, all]},
              default => html,
              help => "Output format: html, txt, or all (default: html)"},
            #{name => import_dirs, long => "-import-dirs", required => true,
              nargs => nonempty_list,
              help => "Coverage data directories to import"}
        ],
        handler => fun run/1
    }.

run(#{report_dir := ReportDir, format := Format, import_dirs := CoverageDirs}) ->
    StartTime = erlang:monotonic_time(millisecond),
    ok = filelib:ensure_path(ReportDir ++ "/"),
    init_log(ReportDir),
    log("=== Coverage Report Generation ==="),
    log("Coverage dirs: ~p", [CoverageDirs]),
    log("Report dir: ~s", [ReportDir]),
    log("Format: ~p", [Format]),
    maybe_start_cover(),

    %% Discover all .coverdata files from all directories
    CoverdataFiles = lists:flatmap(
                       fun (Dir) ->
                           filelib:wildcard(filename:join(Dir, "*.coverdata"))
                       end, CoverageDirs),

    log("Found ~p .coverdata files", [length(CoverdataFiles)]),

    %% Import all .coverdata files
    log("Importing .coverdata files..."),
    lists:foreach(fun(File) ->
        log("  Importing: ~s", [filename:basename(File)]),
        case cover:import(File) of
            ok ->
                log("    OK");
            {error, Reason} ->
                log("    ERROR: Failed to import ~s: ~p", [File, Reason])
        end
    end, lists:sort(CoverdataFiles)),

    ImportedModules = cover:imported_modules(),

    HtmlDir = filename:join(ReportDir, "html"),
    TxtDir = filename:join(ReportDir, "txt"),
    %% Generate HTML reports (if requested)
    case lists:member(Format, [html, all]) of
        true ->
            log("Generating HTML reports to ~s", [HtmlDir]),
            generate_html_reports(ImportedModules, HtmlDir);
        false ->
            ok
    end,

    %% Generate TXT reports (if requested)
    case lists:member(Format, [txt, all]) of
        true ->
            log("Generating TXT reports to ~s", [TxtDir]),
            generate_txt_reports(ImportedModules, TxtDir);
        false ->
            ok
    end,

    %% Compute coverage summary
    log("Computing coverage summary..."),
    SummaryMap0 = compute_coverage_summary(ImportedModules),

    %% Stop cover
    catch cover:stop(),

    %% Calculate elapsed time
    EndTime = erlang:monotonic_time(millisecond),
    ElapsedMs = EndTime - StartTime,
    ElapsedSec = ElapsedMs / 1000,
    NumCoverdataFiles = length(CoverdataFiles),
    SummaryMap = SummaryMap0#{elapsed_time_sec => ElapsedSec,
                              coverdata_files_imported => NumCoverdataFiles},

    log("Total time: ~.2f seconds", [ElapsedSec]),

    %% Write summary.txt
    write_summary_file(ReportDir, SummaryMap),
    write_jenkins_metric_file(ReportDir, "coverage_percentage.properties",
                              maps:get(total_coverage, SummaryMap, 0.0)),
    write_jenkins_metric_file(ReportDir, "covered_lines.properties",
                              maps:get(total_covered_lines, SummaryMap, 0)),
    write_jenkins_metric_file(ReportDir, "total_lines.properties",
                              maps:get(total_lines, SummaryMap, 0)),

    %% Output JSON to stdout
    JSON = json:encode(SummaryMap),
    io:format("~s", [JSON]),

    log("=== Coverage Report Generation Complete ===").

generate_html_reports(Modules, HtmlDir) ->
    ok = filelib:ensure_path(HtmlDir ++ "/"),
    case cover:analyse_to_file(Modules, [html, {outdir, HtmlDir}]) of
        {result, Ok, _Fail} ->
            log("  Generated ~p HTML reports", [length(Ok)]);
        {error, Reason} ->
            panic("Failed to generate HTML reports: ~p", [Reason])
    end.

generate_txt_reports(Modules, TxtDir) ->
    ok = filelib:ensure_path(TxtDir ++ "/"),
    case cover:analyse_to_file(Modules, [{outdir, TxtDir}]) of
        {result, Ok, _Fail} ->
            log("  Generated ~p TXT reports", [length(Ok)]);
        {error, Reason} ->
            panic("Failed to generate TXT reports: ~p", [Reason])
    end.

init_log(ReportDir) ->
    LogFile = filename:join(ReportDir, "generate_coverage.log"),
    {ok, Fd} = file:open(LogFile, [write]),
    put(log_fd, Fd).

log(Format) ->
    log(Format, []).

log(Format, Args) ->
    Fd = get(log_fd),
    Timestamp = format_timestamp(),
    Msg = io_lib:format(Format, Args),
    case Fd of
        undefined ->
            io:format(standard_error, "~s ~s~n", [Timestamp, Msg]);
        _ ->
            io:format(Fd, "~s ~s~n", [Timestamp, Msg])
    end.

format_timestamp() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                  [Y, M, D, H, Mi, S]).

maybe_start_cover() ->
    case whereis(cover_server) of
        undefined ->
            log("Starting cover server"),
            case cover:start() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                {error, Error} -> panic("Failed to start cover: ~p", [Error])
            end,
            disable_cover_output();
        _ ->
            ok
    end.

compute_coverage_summary(Modules) ->
    log("Found ~p modules with coverage data", [length(Modules)]),

    %% Compute coverage for each module
    {result, OkRes, FailRes} = cover:analyse(Modules, coverage, module),
    case FailRes of
        [] -> ok;
        _ -> panic("Error analyzing: ~p", [FailRes])
    end,
    {ModuleCoverage, TotalCov, TotalLines} =
        lists:foldl(
          fun ({Module, {Cov, NotCov}}, {ModAcc, CovAcc, TotalAcc}) ->
              Total = Cov + NotCov,
              Pct = if
                        Total > 0 -> (Cov * 100) / Total;
                        true -> 0.0
                    end,
              log("  ~s: ~.2f% (Cov: ~p, NotCov: ~p)",
                  [Module, Pct, Cov, NotCov]),
              {ModAcc#{Module => Pct}, CovAcc + Cov, TotalAcc + Total}
          end, {#{}, 0, 0}, OkRes),

    %% Calculate total coverage
    TotalPercentage = case TotalLines > 0 of
                          true -> (TotalCov * 100) / TotalLines;
                          false ->  0.0
                      end,

    log("Total coverage: ~.2f%", [TotalPercentage]),

    #{
        total_coverage => TotalPercentage,
        total_covered_lines => TotalCov,
        total_lines => TotalLines,
        module_coverage => ModuleCoverage
    }.

panic(Format, Args) ->
    Msg = io_lib:format(Format, Args),
    log("ERROR: ~s", [Msg]),
    io:format(standard_error, "ERROR: ~s~n", [Msg]),
    halt(1).

write_summary_file(ReportDir, SummaryMap) ->
    SummaryFile = filename:join(ReportDir, "summary.txt"),
    TotalCoverage = maps:get(total_coverage, SummaryMap, 0.0),
    TotalLines = maps:get(total_lines, SummaryMap, 0),
    TotalCoveredLines = maps:get(total_covered_lines, SummaryMap, 0),
    ModuleCoverage = maps:get(module_coverage, SummaryMap, #{}),
    ElapsedSec = maps:get(elapsed_time_sec, SummaryMap, 0.0),
    NumCoverdataFiles = maps:get(coverdata_files_imported, SummaryMap, 0),
    SortedByPct = lists:sort(fun({_, A}, {_, B}) -> A =< B end,
                             maps:to_list(ModuleCoverage)),
    Lines = [io_lib:format("Total Line Coverage: ~.2f%~n", [TotalCoverage]),
             io_lib:format("Total lines: ~p~n", [TotalLines]),
             io_lib:format("Total covered lines: ~p~n", [TotalCoveredLines]),
             io_lib:format("Coverdata files imported: ~p~n",
                           [NumCoverdataFiles]),
             io_lib:format("Report generation time: ~.2f seconds~n~n",
                           [ElapsedSec]),
             "Module Coverage (sorted by coverage):\n" |
             [io_lib:format("  ~s: ~.2f%~n", [Mod, Pct])
              || {Mod, Pct} <- SortedByPct]],
    ok = file:write_file(SummaryFile, Lines),
    log("Wrote summary to ~s", [SummaryFile]).

write_jenkins_metric_file(ReportDir, FileName, Value) ->
    MetricsFile = filename:join([ReportDir, "jenkins_metrics", FileName]),
    filelib:ensure_dir(MetricsFile),
    ValueStr = case is_integer(Value) of
                   true -> integer_to_list(Value);
                   false -> io_lib:format("~.2f", [Value])
               end,
    Data = io_lib:format("YVALUE=~s~n", [ValueStr]),
    ok = file:write_file(MetricsFile, Data).

disable_cover_output() ->
    case whereis(cover_server) of
        undefined ->
            ok;
        Pid ->
            group_leader(erlang:whereis(standard_error), Pid),
            ok
    end.