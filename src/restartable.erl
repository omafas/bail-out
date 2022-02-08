%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(restartable).

-include("ns_common.hrl").

-export([start_link/2, spec/1, restart/1, restart/2]).

start_link(MFA, Shutdown) ->
    Parent = self(),
    proc_lib:start_link(erlang, apply, [fun body/3, [Parent, MFA, Shutdown]]).

spec({Id, MFA, Restart, Shutdown, Type, Modules}) ->
    {Id,
     {restartable, start_link, [MFA, Shutdown]},
     Restart, infinity, Type, Modules}.

restart(Pid) ->
    gen_server:call(Pid, restart, infinity).

restart(Sup, Id) ->
    Children = supervisor:which_children(Sup),
    case lists:keyfind(Id, 1, Children) of
        {Id, Child, _, _} ->
            case Child of
                undefined ->
                    {error, not_running};
                restarting ->
                    {error, restarting};
                _ ->
                    true = is_pid(Child),
                    restart(Child)
            end;
        false ->
            {error, not_found}
    end.

%% internal
body(Parent, MFA, Shutdown) ->
    process_flag(trap_exit, true),

    case start_child(MFA) of
        {ok, Child} ->
            proc_lib:init_ack({ok, self()}),
            loop(Parent, Child, MFA, Shutdown);
        Other ->
            proc_lib:init_ack(Other)
    end.

loop(Parent, Child, MFA, Shutdown) ->
    receive
        {'EXIT', Parent, Reason} ->
            shutdown_child(Child, Shutdown),
            exit(Reason);
        {'EXIT', Child, Reason} ->
            exit(Reason);
        {'$gen_call', From, restart} ->
            ?log_debug("Restarting child ~p~n"
                       "  MFA: ~p~n"
                       "  Shutdown policy: ~p~n"
                       "  Caller: ~p",
                       [Child, MFA, Shutdown, From]),

            shutdown_child(Child, Shutdown),
            RV = start_child(MFA),
            gen_server:reply(From, RV),

            case RV of
                {ok, NewChild} ->
                    loop(Parent, NewChild, MFA, Shutdown);
                Other ->
                    ?log_error("Failed to restart child ~p: ~p", [MFA, Other]),
                    exit({restart_failed, MFA, Other})
            end;
        Msg ->
            Child ! Msg,
            loop(Parent, Child, MFA, Shutdown)
    end.

start_child({M, F, A} = MFA) ->
    RV = erlang:apply(M, F, A),
    case RV of
        {ok, Child} ->
            assert_linked(Child),
            ?log_debug("Started child process ~p~n"
                       "  MFA: ~p",
                       [Child, MFA]);
        _ ->
            ok
    end,

    RV.

shutdown_child(Pid, Shutdown) ->
    {Reason, ExpectedReason, Timeout} =
        case Shutdown of
            brutal_kill ->
                {kill, killed, infinity};
            _ ->
                true = is_integer(Shutdown) orelse (Shutdown =:= infinity),
                {shutdown, shutdown, Shutdown}
        end,

    exit(Pid, Reason),
    case misc:wait_for_process(Pid, Timeout) of
        ok ->
            ?log_debug("Successfully terminated process ~p", [Pid]),
            consume_exit_msg(Pid, ExpectedReason);
        {error, timeout} ->
            true = (Reason =/= kill),

            ?log_warning("Failed to terminate ~p after ~pms. Killing it brutally",
                         [Pid, Timeout]),
            shutdown_child(Pid, brutal_kill)
    end.

consume_exit_msg(Pid, ExpectedReason) ->
    receive
        {'EXIT', Pid, ActualReason} ->
            case ActualReason =:= ExpectedReason of
                true ->
                    ok;
                false ->
                    ?log_warning("Child ~p terminated with unexpected reason ~p. "
                                 "Expected: ~p", [Pid, ActualReason, ExpectedReason])
            end
    end.

assert_linked(Pid) ->
    {links, Links} = process_info(Pid, links),
    true = lists:member(self(), Links).
