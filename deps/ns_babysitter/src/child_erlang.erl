%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2020 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
-module(child_erlang).

-include("ns_common.hrl").

-export([handle_arguments/1,
         child_start/1,
         open_port_args/0]).

get_ns_server_vm_extra_args() ->
    case os:getenv("COUCHBASE_NS_SERVER_VM_EXTRA_ARGS") of
        false ->
            [];
        Value ->
            PV = case erl_scan:string(Value ++ ".") of
                     {ok, Ts, _} ->
                         case erl_parse:parse_term(Ts) of
                             {ok, Term0} when is_list(Term0) ->
                                 {ok, Term0};
                             Err0 ->
                                 {parse_error, Err0}
                         end;
                     Err0 ->
                         {scan_error, Err0}
                 end,
            case PV of
                {ok, Term} ->
                    true = is_list(Term),
                    Term;
                Err ->
                    ?log_warning("Got something in COUCHBASE_NS_SERVER_VM_EXTRA_ARGS environment variable (~s) but it's not a list term: ~p",
                                 [Value, Err]),
                    []
            end
    end.

open_port_args() ->
    {AppArgs, AppEnvArgs} = handle_arguments(init:get_arguments()),
    ErlangArgs = ["+A" , "16",
                  "-smp", "enable",
                  "+sbt",  "u",
                  "+P", "327680",
                  "+K", "true",
                  "+swt", "low",
                  "+sbwt", "none",
                  "+MMmcs", case os:getenv("COUCHBASE_MSEG_CACHE_SIZE") of
                                false -> "30";
                                MCS ->
                                    MCS
                            end,
                  %% somehow r14b04 doesn't understand it if the value is
                  %% passed as a separate argument
                  "+e102400",
                  "-setcookie", "nocookie",
                  "-kernel", "error_logger", "false",
                  "-sasl", "sasl_error_logger", "false",
                  "-user", "user_io",
                  "-run", "child_erlang", "child_start", "ns_bootstrap"]
        ++ get_ns_server_vm_extra_args() ++ ["--"],
    AllArgs = ErlangArgs ++ AppArgs,
    ErlPath = filename:join([hd(proplists:get_value(root, init:get_arguments())),
                             "bin", "erl"]),

    Env0 = case os:getenv("ERL_CRASH_DUMP_BASE") of
               false ->
                   [];
               Base ->
                   [{"ERL_CRASH_DUMP", Base ++ ".ns_server"}]
           end,
    Env = [{"NS_SERVER_BABYSITTER_PID", os:getpid()},
           {"CHILD_ERLANG_ENV_ARGS", misc:inspect_term(AppEnvArgs)} | Env0],

    {ErlPath, AllArgs, [{env, Env}, exit_status, use_stdio, stream, eof]}.

child_start(Arg) ->
    try
        do_child_start(Arg)
    catch T:E ->
            io:format("Crap ~p:~p~n~p~n", [T, E, erlang:get_stacktrace()]),
            (catch ?log_debug("Crap to start:  ~p:~p~n~p~n", [T, E, erlang:get_stacktrace()])),
            timer:sleep(1000),
            misc:halt(3)
    end.

do_child_start([ModuleToBootAsString]) ->
    %% erl can be started either with -nouser or with
    %% -user user_io (which redirects outout to debug log)
    %% in any case we want default io:format to print stuff
    %% to console
    %% so we overwrite group_leader from our user_io (or undefined) to
    %% standard_error so io:format will write to console
    %% and io:format(user... will write to debug log (or crash)
    StdErr = erlang:whereis(standard_error),
    {true, have_stderr} = {StdErr =/= undefined, have_stderr},
    erlang:group_leader(StdErr, self()),
    erlang:group_leader(StdErr, erlang:whereis(application_controller)),

    BootModule = list_to_atom(ModuleToBootAsString),
    BootModule:start(),
    %% NOTE: win32 support in erlang handles {fd, 0, 1} specially and
    %% does the right thing. {fd, 0, 0} would not work for example
    Port = erlang:open_port({fd, 0, 1}, [in, stream, binary, eof]),
    child_loop(Port, BootModule).

child_loop_quick_exit(BootModule) ->
    io:format("EOF. Exiting\n"),
    try BootModule:get_quick_stop() of
        Fn ->
            Fn()
    catch _T:_E -> ignore
    end,
    misc:halt(0).

child_loop(Port, BootModule) ->
    io:format("~s: Booted. Waiting for shutdown request\n", [os:getpid()]),
    ?log_debug("~s: Entered child_loop", [os:getpid()]),
    receive
        {Port, {data, <<"shutdown\n">>}} ->
            io:format("~s: got shutdown request. Exiting\n", [os:getpid()]),
            ?log_debug("~s: Got EOL", [os:getpid()]),
            BootModule:stop(),
            ?log_debug("Got EOL: after ~s:stop()", [BootModule]),
            misc:halt(0);
        {Port, eof} ->
            (catch ?log_debug("Got EOF")),
            child_loop_quick_exit(BootModule);
        {Port, {data, <<"die!\n">>}} ->
            (catch ?log_debug("Got die!")),
            child_loop_quick_exit(BootModule);
        {Port, {data, Msg}} ->
            io:format("--------------~n!!! Message from parent: ~s~n------------~n~n", [Msg]),
            (catch ?log_debug("--------------~n!!! Message from parent: ~s~n------------~n~n", [Msg])),
            BootModule:stop(),
            misc:halt(0);
        Unexpected ->
            io:format("Got unexpected message: ~p~n", [Unexpected]),
            (catch ?log_debug("Got unexpected message: ~p~n", [Unexpected])),
            timer:sleep(3000),
            misc:halt(1)
    end.

handle_arguments(Arguments) ->
    lists:foldr(
      fun ({Flag, Values}, {AccArgs, AccEnv} = Acc) ->
              case Flag of
                  _ when Flag =:= root;
                         Flag =:= home;
                         Flag =:= progname;
                         Flag =:= name;
                         Flag =:= hidden;
                         Flag =:= setcookie;
                         Flag =:= detach;
                         Flag =:= noinput;
                         Flag =:= noshell;
                         Flag =:= nouser ->
                      Acc;
                  _ ->
                      %% Make exception for kernel to make sure that ns_server
                      %% node inherits kernel env vars like inetrc and
                      %% dist_config_file
                      case is_application(Flag) and (Flag =/= kernel) of
                          false ->
                              FlagStr = "-" ++ atom_to_list(Flag),
                              AccArgs1 = [FlagStr | Values] ++ AccArgs,
                              {AccArgs1, AccEnv};
                          true ->
                              AccEnv1 = case lists:keymember(Flag, 1, AccEnv) of
                                            false ->
                                                [{Flag, get_all_env(Flag)} | AccEnv];
                                            true ->
                                                AccEnv
                                        end,
                              {AccArgs, AccEnv1}
                      end
              end
      end, {[], []}, Arguments).

is_application(Name) ->
    case application:load(Name) of
        {error, {Error, _}} when Error =/= already_loaded -> false;
        _ -> true
    end.

get_all_env(ns_babysitter) ->
    Env = application:get_all_env(ns_babysitter),
    lists:filter(fun ({master_password, _}) ->
                         false;
                     ({memcached_secrets, _}) ->
                         false;
                     (_) ->
                         true
                 end, Env);
get_all_env(Flag) ->
    application:get_all_env(Flag).
