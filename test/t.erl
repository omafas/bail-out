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

-export([start/0, start_eunit/0, start_triq/0, config/1]).

%% Used by cluster_run --dont-start.
-export([fake_loggers/0]).

start() ->
    run_tests(all).

start_eunit() ->
    run_tests(eunit).

start_triq() ->
    run_tests(triq).

run_tests(Enabled) ->
    fake_loggers(),
    setup_paths(),
    Modules = get_modules(),
    FailedTests =
        lists:flatmap(
          fun ({Name, Runner}) ->
                  io:format("Running ~p tests for modules: ~p~n", [Name, Modules]),
                  Runner(Modules)
          end, test_runners(Enabled)),

    handle_failed_tests(FailedTests).

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

get_modules() ->
    Ext = code:objfile_extension(),
    Wildcard = case os:getenv("T_WILDCARD") of
                   false -> "*";
                   X -> X
               end,
    FullWildcard =
        case lists:member($/, Wildcard) of
            true ->
                Wildcard ++ Ext;
            false ->
                filename:join(["**", ".eunit", Wildcard]) ++ Ext
        end,

    Files = filelib:wildcard(FullWildcard, config(root_dir)),
    [list_to_atom(filename:basename(F, Ext)) || F <- Files].

run_eunit_tests(Modules0) ->
    %% eunit:test(module) will also run tests defined in module_tests. This
    %% will filter _tests modules out to avoid running tests twice.
    Modules  = filter_out_unneeded_tests_modules(Modules0),
    Listener = spawn_listener(),
    eunit:test([{spawn, M} || M <- Modules], [verbose, {report, Listener}]),

    receive
        {failed_tests, FailedTests} ->
            FailedTests
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

    ok.

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
                    Source = proplists:get_value(source, TestProps),
                    [Source | FailedTests]
            end
    end.

handle_failed_tests([]) ->
    ok;
handle_failed_tests(FailedTests) ->
    io:format("=======================================================~n"),
    io:format("  ~s:~n", [bold_red("Failed tests")]),
    lists:foreach(
      fun (MFA) ->
              io:format("    ~s~n", [format_mfa(MFA)])
      end, FailedTests),
    io:format("=======================================================~n"),
    failed.

format_mfa({M, F, A}) ->
    io_lib:format("~s:~s/~b", [M, F, A]).

bold_red(Text) ->
    [<<"\e[31;1m">>, Text, <<"\e[0m">>].

config(root_dir) ->
    filename:absname(filename:dirname(config(test_dir)));

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
