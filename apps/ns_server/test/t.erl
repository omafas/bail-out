% Copyright (c) 2008, Cliff Moon
% Copyright (c) 2008, Powerset, Inc
% Copyright (c) 2009-2019, Couchbase, Inc.
%
% All rights reserved.
%
% Redistribution and use in source and binary forms, with or without
% modification, are permitted provided that the following conditions
% are met:
%
% * Redistributions of source code must retain the above copyright
% notice, this list of conditions and the following disclaimer.
% * Redistributions in binary form must reproduce the above copyright
% notice, this list of conditions and the following disclaimer in the
% documentation and/or other materials provided with the distribution.
% * Neither the name of Powerset, Inc nor the names of its
% contributors may be used to endorse or promote products derived from
% this software without specific prior written permission.
%
% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
% "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
% LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
% FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
% COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
% INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
% BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
% LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
% CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
% LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
% ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
% POSSIBILITY OF SUCH DAMAGE.
%
% Original Author: Cliff Moon

-module(t).

-include("ns_common.hrl").
-include_lib("ale/include/ale.hrl").

-export([start/0, start/1, start_eunit/0, start_triq/0, config/1]).

%% Used by cluster_run --dont-start.
-export([fake_loggers/0]).

start() ->
    run_tests(all).

start(Filter) ->
    run_tests(all, Filter).

start_eunit() ->
    run_tests(eunit).

start_triq() ->
    run_tests(triq).

run_tests(Enabled) ->
    run_tests(Enabled, undefined).

run_tests(Enabled, Filter) ->
    fake_loggers(),
    setup_paths(),
    Modules = get_modules(Filter),
    CoverageEnabled = (length(os:getenv("T_COVERAGE", "")) > 0),
    CodeCoverageDir = filename:join(config(root_dir), ".coverage"),
    FailedTests =
        with_code_coverage(
          fun () ->
              lists:flatmap(
                fun ({Name, Runner}) ->
                    io:format("Running ~p tests for modules: ~p~n",
                              [Name, Modules]),
                    Runner(Modules)
                end, test_runners(Enabled))
          end, CoverageEnabled, Modules, CodeCoverageDir),
    handle_failed_tests(FailedTests).

with_code_coverage(Fun, false, _Modules, _OutputDir) ->
    io:format("Code coverage is disabled~n"),
    Fun();
with_code_coverage(Fun, true, Modules, OutputDir) ->
    try
        cover_init(Modules),
        Res = Fun(),
        cover_analyze(Modules, OutputDir),
        Res
    after
        cover_stop()
    end.

cover_init(Modules) ->
    io:format("Code coverage is enabled~n"),
    {ok, _} = cover:start(),
    %% From documentation:
    %% When running in this mode, modules will be Cover compiled in a more
    %% efficient way, but the resulting code will only work on the same node
    %% they were compiled on.
    ok = cover:local_only(),
    io:format("Compiling modules for code coverage...~n"),
    Errors =
        lists:filtermap(fun (?MODULE) ->
                                %% Do not recompile this module because
                                %% this process will kill itself on unload
                                false;
                            (M) ->
                                %% Using compile_beam because documentation
                                %% claims it is faster than cover:compile()
                                case cover:compile_beam(M) of
                                    {ok, _} ->
                                        false;
                                    {error, Err} ->
                                        {true, {M, Err}}
                                end
                        end, Modules),
    case Errors of
        [] ->
            io:format("finished~n"),
            ok;
        _ ->
            io:format("Compilation for code coverage of the following modules "
                      "has failed:~p~n", [Errors]),
            erlang:error({cover_compilation, Errors})
    end.

-define(COV_IGNORE_FUN(F, A), F == '__call_logger'; {F, A} == {'test', 0}).

cover_analyze(Modules, Dir) ->
    io:format("Analyzing code coverage...~n"),
    %% Calling analyze with 'function' just because we want to skip counting
    %% coverage for some functions below.
    {result, ModRes, ModErr} = cover:analyse(Modules, coverage, function),

    ModErr == [] orelse
        io:format("Code coverage failed for the following modules: ~0p~n",
                  [ModErr]),

    {TotalCov, TotalNotCov} =
        lists:foldl(
          fun ({{_M, F, A}, {_Cov, _NCov}}, Acc) when ?COV_IGNORE_FUN(F, A) ->
                  Acc;
              ({{_M, _F, _A}, {Cov, NCov}}, {ACov, ANCov}) ->
                  {ACov + Cov, ANCov + NCov}
          end, {0, 0}, ModRes),

    Coverage = case TotalCov + TotalNotCov of
                   0 -> 0;
                   Sum -> TotalCov * 100 / Sum
               end,

    io:format("Total code coverage: ~.2f% lines~n"
              "Covered lines:       ~b~n"
              "Total lines:         ~b~n"
              "See detailed per module report here: "
              "file://~s~n",
              [Coverage, TotalCov, TotalCov + TotalNotCov, Dir]),
    file:del_dir_r(Dir),
    ok = filelib:ensure_path(Dir),
    lists:foreach(
        fun (?MODULE) -> ok;
            (M) ->
                F = filename:join(Dir, atom_to_list(M) ++ ".COVER.html"),
                case cover:analyze_to_file(M, [html, {outfile, F}]) of
                    {ok, _} -> ok;
                    {error, R} ->
                        io:format("Failed to analyze coverage for ~p: ~p~n",
                                  [M, R])
                end
        end, Modules),
    io:format("Analyzing code coverage finished~n").

cover_stop() -> catch cover:stop().

all_test_runners() ->
    [{eunit, fun run_eunit_tests/1},
     {triq, fun run_triq_tests/1}].

test_runners(all) ->
    all_test_runners();
test_runners(OneTest) when is_atom(OneTest) ->
    test_runners([OneTest]);
test_runners(Enabled) when is_list(Enabled) ->
    lists:filter(
        fun ({Name, _}) ->
                lists:member(Name, Enabled)
        end, all_test_runners()).

get_modules(Filter) ->
    Ext = code:objfile_extension(),

    Wildcard =
        case Filter of
            undefined ->
                %% Check env var
                case os:getenv("T_WILDCARD") of
                    false -> "*";
                    X -> X
                end;
            _ ->
                %% Filter when passed from the command line is a file name, and
                %% we aren't quoting/escaping it, so it comes as an atom.
                %% Making all of our filenames valid atoms is reasonable.
                atom_to_list(Filter)
        end,

    FullWildcard =
        case lists:member($/, Wildcard) of
            true ->
                Wildcard ++ Ext;
            false ->
                %% We match anything built in the test profile, including our
                %% dependencies. Unless the test profile is build explicitly for
                %% some dependency it is compiled in prod/default profile by
                %% rebar3 and copied into the test profile where appropriate.
                %% This avoids emitting tests for dependencies that we do not
                %% care about.
                %% Note that this is going to pull tests from both ebin and test
                %% directories. Some files are only compiled to one location.
                %% eunit is smart enough to only run each module once though so
                %% we aren't repeating any effort, we just "discover" the module
                %% multiple times.
                filename:join(["**", "_build/test/lib/**/**",
                    Wildcard]) ++ Ext
        end,


    io:format("fullwildcard ~s~n", [FullWildcard]),

    io:format("rootdir ~s~n", [config(root_dir)]),

    Files = filelib:wildcard(FullWildcard, config(root_dir)),


    io:format("files ~s~n", [Files]),
    [list_to_atom(filename:basename(F, Ext)) || F <- Files].

run_eunit_tests(Modules0) ->
    %% eunit:test(module) will also run tests defined in module_tests. This
    %% will filter _tests modules out to avoid running tests twice.
    Modules  = filter_out_unneeded_tests_modules(Modules0),
    Listener = spawn_listener(),
    TestResult = eunit:test([{spawn, M} || M <- Modules],
                            [verbose, {report, Listener}, {print_depth, 100}]),

    receive
        {failed_tests, FailedTests} ->
            case TestResult of
                ok -> FailedTests;
                _ ->
                    %% This is a catch all backstop based on the result of the
                    %% eunit test process. If we skip some failed test messages
                    %% then this will prevent us from passing test jobs/CV.
                    %% The log message isn't great at the moment, but eunit
                    %% will have printed whatever failed further up in the logs.
                    Msg = "Eunit reported that tests failed. Consult logged "
                          "test failures below for failing tests, or the "
                          "eunit logs above for other failures that many not "
                          "have been caught by this harness.",
                    [Msg | FailedTests]
            end
    end.

filter_out_unneeded_tests_modules(Modules) ->
    Set0 = sets:from_list(Modules),
    Set1 = sets:filter(
             fun (Module) ->
                     case is_tests_module(Module) of
                         {true, MainModule} ->
                             %% only filter the module out if the
                             %% corresponding main module is in the set of
                             %% modules to test
                             not sets:is_element(MainModule, Set0);
                         false ->
                             true
                     end
             end, Set0),

    sets:to_list(Set1).

is_tests_module(Module0) ->
    Suffix = "_tests",
    Module = atom_to_list(Module0),
    case lists:suffix(Suffix, Module) of
        true ->
            {Main, _} = lists:split(length(Module) - length(Suffix), Module),
            {true, list_to_atom(Main)};
        false ->
            false
    end.

-define(TRIQ_ITERS, 100).

run_triq_tests(Modules) ->
    lists:flatmap(fun run_module_triq_tests/1, Modules).

run_module_triq_tests(Module) ->
    lists:filter(
      fun (MFA) ->
              io:format("Testing ~s~n", [format_mfa(MFA)]),
              check_triq_prop(MFA) =/= ok
      end, get_module_triq_tests(Module)).

check_triq_prop({M, F, _}) ->
    {Prop, Options} =
        case is_extended_triq_prop(F) of
            true ->
                M:F();
            false ->
                {M:F(), []}
        end,

    do_check_triq_prop(Prop, Options).

do_check_triq_prop(Prop, Options) ->
    Iters = proplists:get_value(iters, Options, ?TRIQ_ITERS),

    case triq:check(Prop, Iters) of
        true ->
            ok;
        _ ->
            [CounterExample|_] = triq:counterexample(),
            triq_prop_diag(CounterExample, Options),
            failed
    end.

triq_prop_diag(CounterExample, Options) ->
    case lists:keyfind(diag, 1, Options) of
        false ->
            ok;
        {diag, DiagFun} ->
            Diag = DiagFun(CounterExample),
            io:format("~nExtra diagnostics:~n~n~p~n~n", [Diag])
    end.

is_extended_triq_prop(Name) ->
    lists:suffix("_", atom_to_list(Name)).

get_module_triq_tests(Module) ->
    Exports = Module:module_info(exports),
    [{Module, F, 0} || {F, 0} <- Exports,
                       is_triq_test(F)].

is_triq_test(Name) when is_atom(Name) ->
    lists:prefix("prop_", atom_to_list(Name)).

%% create all the logger real ns_server has; this prevents failures if test
%% cases log something;
fake_loggers() ->
    ok = application:start(ale),

    ok = ale:start_sink(stderr, tty_safe_stderr_sink, []),

    ok = ale:set_loglevel(?ERROR_LOGGER, debug),
    ok = ale:add_sink(?ERROR_LOGGER, stderr),

    lists:foreach(
      fun (Logger) ->
              ok = ale:start_logger(Logger, debug),
              ok = ale:add_sink(Logger, stderr)
      end,
      ?LOGGERS).

setup_paths() ->
    Prefix = config(prefix_dir),
    BinDir = filename:join(Prefix, "bin"),

    Root = config(root_dir),
    TmpDir = filename:join(Root, "tmp"),
    file:make_dir(TmpDir),

    ets:new(path_config_override, [named_table, set, public]),
    ets:insert_new(path_config_override, {path_config_bindir, BinDir}),
    ets:insert_new(path_config_override, {path_config_tmpdir, TmpDir}),

    [ets:insert(path_config_override, {K, TmpDir})
     || K <- [path_config_tmpdir, path_config_datadir,
              path_config_libdir, path_config_etcdir]],

    application:set_env(ns_server, error_logger_mf_dir, TmpDir),

    application:set_env(kernel, dist_config_file,
                        filename:join(TmpDir, "dist_cfg")),

    case os:getenv("OVERRIDE_EXECUTABLE_PATHS") of
        false -> ok;
        X ->
            lists:foreach(
                fun(<<>>) -> ok;
                   (B) ->
                        [DepBin, PathBin] = binary:split(B, <<"=">>),
                        Dep = binary_to_list(DepBin),
                        Path = binary_to_list(PathBin),
                        io:format("Got path override for ~p to ~p~n",
                                  [Dep, Path]),
                        ets:insert_new(path_config_override, {Dep, Path})
                end, binary:split(list_to_binary(X), <<":">>, [global]))
    end.

spawn_listener() ->
    Parent = self(),
    proc_lib:spawn_link(fun () -> listener_loop(Parent, []) end).

listener_loop(Parent, FailedTests) ->
    receive
        {stop, _, _} ->
            Parent ! {failed_tests, FailedTests};
        {status, Id, {progress, 'begin', {test, TestProps}}} ->
            NewFailedTests = handle_test_progress(Id, TestProps, FailedTests),
            listener_loop(Parent, NewFailedTests);
        _ ->
            listener_loop(Parent, FailedTests)
    end.

handle_test_progress(Id, TestProps, FailedTests) ->
    receive
        {status, Id, Info} ->
            Failed =
                case Info of
                    {progress, 'end', {Result, _}} ->
                        Result =/= ok;
                    {cancel, _} ->
                        true
                end,

            case Failed of
                false ->
                    FailedTests;
                true ->
                    [TestProps | FailedTests]
            end
    end.

handle_failed_tests([]) ->
    ok;
handle_failed_tests(FailedTests) ->
    io:format("=======================================================~n"),
    io:format("  ~s:~n", [bold_red("Failed tests")]),
    lists:foreach(
      fun (TestProps) ->
              io:format("    ~s~n", [format_test_props(TestProps)])
      end, FailedTests),
    io:format("=======================================================~n"),
    failed.

format_test_props([{_, _}|_]=TestProps) ->
    MFA = proplists:get_value(source, TestProps),
    Desc = proplists:get_value(desc, TestProps),
    format_mfa(MFA) ++
        case Desc of
            undefined -> [];
            _ when is_binary(Desc) -> io_lib:format(" (~s)", [Desc]);
            _ -> io_lib:format(" (~p)", [Desc])
        end;
format_test_props(Other) ->
    format_mfa(Other).

format_mfa({M, F, A}) ->
    io_lib:format("~p:~p/~p", [M, F, A]);
format_mfa(Msg) ->
    io_lib:format("~p", [Msg]).

bold_red(Text) ->
    [<<"\e[31;1m">>, Text, <<"\e[0m">>].

config(root_dir) ->
    filename:absname(filename:join([filename:dirname(config(test_dir)), "../../"]));

config(ebin_dir) ->
    filename:absname(filename:join([config(root_dir), "ebin"]));

config(test_dir) ->
    filename:absname(filename:dirname(?FILE));

config(priv_dir) ->
    case init:get_argument(priv_dir) of
        {ok, [[Dir]]} ->
            Dir;
        _Other ->
            Root = config(test_dir),
            filename:absname(
              filename:join([Root, "log", atom_to_list(node())]))
    end;

config(prefix_dir) ->
    case init:get_argument(prefix_dir) of
        {ok, [[Prefix]]} ->
            Prefix;
        _ ->
            Root = config(root_dir),
            filename:absname(
              filename:join([Root, "..", "install"]))
    end.
