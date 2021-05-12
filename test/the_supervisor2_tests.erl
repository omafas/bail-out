%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2017 Couchbase, Inc.
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
-module(the_supervisor2_tests).

-include_lib("eunit/include/eunit.hrl").

-export([init/1]).

init(Spec) ->
    {ok, Spec}.

frequent_crashes_child_start_link(Name) ->
    proc_lib:start_link(erlang, apply, [fun frequent_crashes_child_init/1, [Name]]).

frequent_crashes_child_init(Name) ->
    erlang:register(Name, self()),
    proc_lib:init_ack({ok, self()}),
    receive
        X ->
            erlang:exit(X)
    end.


sync_shutdown_child(Pid, Reason) ->
    Pid ! Reason,
    misc:wait_for_process(Pid, infinity).

find_child(Sup, Name) ->
    {_, ChildAfterDeath, _, _} = lists:keyfind(Name, 1, supervisor2:which_children(Sup)),
    ChildAfterDeath.


frequent_crashes_run() ->
    ChildSpecs = [{c1, {erlang, apply, [fun frequent_crashes_child_start_link/1, [frequent_crashes_child]]},
                   {permanent, 3},
                   1000,
                   worker, []},
                  {c2, {erlang, apply, [fun frequent_crashes_child_start_link/1, [frequent_crashes_child_2]]},
                   {permanent, 1},
                   1000,
                   worker, []}],
    {ok, Sup} = supervisor2:start_link(?MODULE,
                                       {{one_for_one, 2, 3}, ChildSpecs}),


    ChildBeforeDeath = find_child(Sup, c1),
    ?assertEqual(ChildBeforeDeath, erlang:whereis(frequent_crashes_child)),
    sync_shutdown_child(ChildBeforeDeath, die),
    ChildAfterDeath = find_child(Sup, c1),
    ?assertEqual(ChildAfterDeath, erlang:whereis(frequent_crashes_child)),
    ?assertNotEqual(ChildAfterDeath, ChildBeforeDeath),

    timer:sleep(1000),
    sync_shutdown_child(ChildAfterDeath, shutdown),
    timer:sleep(1000),
    sync_shutdown_child(find_child(Sup, c1), shutdown),
    %% three crashes in a row should delay restart

    ?assertEqual(restarting, find_child(Sup, c1)),

    timer:sleep(2000),

    ?assertEqual(restarting, find_child(Sup, c1)),

    sync_shutdown_child(find_child(Sup, c2), shutdown),
    ?assertNotEqual(restarting, find_child(Sup, c2)),

    timer:sleep(500),

    ?assertEqual(restarting, find_child(Sup, c1)),

    timer:sleep(500),

    ?assertNotEqual(undefined, find_child(Sup, c1)),
    ?assertNotEqual(restarting, find_child(Sup, c1)),

    %% delayed restarts reset the restart counters; this makes it two; the
    %% next shutdown should delay the restart again
    sync_shutdown_child(find_child(Sup, c2), die),
    ?assertNotEqual(undefined, find_child(Sup, c2)),
    ?assertNotEqual(restarting, find_child(Sup, c2)),

    sync_shutdown_child(find_child(Sup, c1), die1),
    sync_shutdown_child(find_child(Sup, c2), die),
    ?assertEqual(restarting, find_child(Sup, c2)),
    ?assertEqual(restarting, find_child(Sup, c1)),
    timer:sleep(4000),
    ?assertEqual(2, length([T || T <- [find_child(Sup, c1), find_child(Sup, c2)],
                                 T =/= undefined,
                                 T =/= restarting])),

    erlang:process_flag(trap_exit, true),
    Sup ! {'EXIT', self(), shutdown},
    misc:wait_for_process(Sup, infinity).


frequent_crashes_test_() ->
    {timeout, 20, fun frequent_crashes_run/0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% the code below is taken from:
%% https://github.com/erlang/otp/blob/c59c3a6d57b857913ddfa13f96425ba0d95ccb2d/lib/stdlib/test/supervisor_SUITE.erl
%% some minor modifications are made
%%

%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2011. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%

start_link(InitResult) ->
    supervisor2:start_link({local, sup_test}, ?MODULE, InitResult).

check_exit([]) ->
    ok;
check_exit([Pid | Pids]) ->
    receive
        {'EXIT', Pid, _} ->
            check_exit(Pids)
    end.

%%-------------------------------------------------------------------------
%% Test that the supervisor terminates a restarted child when a different
%% child fails to start.
rest_for_one_other_child_fails_restart_test() ->
    process_flag(trap_exit, true),
    Self = self(),
    Child1 = {child1, {supervisor_3, start_child, [child1, Self]},
              permanent, 1000, worker, []},
    Child2 = {child2, {supervisor_3, start_child, [child2, Self]},
              permanent, 1000, worker, []},
    Children = [Child1, Child2],
    StarterFun = fun() ->
                         {ok, SupPid} = start_link({{rest_for_one, 3, 3600}, Children}),
                         Self ! {sup_pid, SupPid},
                         receive {stop, Self} -> ok end
                 end,
    StarterPid = spawn_link(StarterFun),
    Ok = {{ok, undefined}, Self},
    %% Let the children start.
    Child1Pid = receive {child1, Pid1} -> Pid1 end,
    Child1Pid ! Ok,
    Child2Pid = receive {child2, Pid2} -> Pid2 end,
    Child2Pid ! Ok,
    %% Supervisor started.
    SupPid = receive {sup_pid, Pid} -> Pid end,
    link(SupPid),
    exit(Child1Pid, die),
    %% Let child1 restart but don't let child2.
    Child1Pid2  = receive {child1, Pid3} -> Pid3 end,
    Child1Pid2 ! Ok,
    Child2Pid2 = receive {child2, Pid4} -> Pid4 end,
    Child2Pid2 ! {{stop, normal}, Self},
    %% Let child2 restart.
    receive
        {child2, Child2Pid3} ->
            Child2Pid3 ! Ok;
        {child1, _Child1Pid3} ->
            exit(SupPid, kill),
            check_exit([StarterPid, SupPid]),
            test_server:fail({restarting_started_child, Child1Pid2})
    end,
    StarterPid ! {stop, Self},
    check_exit([StarterPid, SupPid]).

%%-------------------------------------------------------------------------
%% Test that the supervisor terminates a restarted child when a different
%% child fails to start.
one_for_all_other_child_fails_restart_test() ->
    process_flag(trap_exit, true),
    Self = self(),
    Child1 = {child1, {supervisor_3, start_child, [child1, Self]},
              permanent, 1000, worker, []},
    Child2 = {child2, {supervisor_3, start_child, [child2, Self]},
              permanent, 1000, worker, []},
    Children = [Child1, Child2],
    StarterFun = fun() ->
                         {ok, SupPid} = start_link({{one_for_all, 3, 3600}, Children}),
                         Self ! {sup_pid, SupPid},
                         receive {stop, Self} -> ok end
                 end,
    StarterPid = spawn_link(StarterFun),
    Ok = {{ok, undefined}, Self},
    %% Let the children start.
    Child1Pid = receive {child1, Pid1} -> Pid1 end,
    Child1Pid ! Ok,
    Child2Pid = receive {child2, Pid2} -> Pid2 end,
    Child2Pid ! Ok,
    %% Supervisor started.
    SupPid = receive {sup_pid, Pid} -> Pid end,
    link(SupPid),
    exit(Child1Pid, die),
    %% Let child1 restart but don't let child2.
    Child1Pid2  = receive {child1, Pid3} -> Pid3 end,
    Child1Pid2Ref = erlang:monitor(process, Child1Pid2),
    Child1Pid2 ! Ok,
    Child2Pid2 = receive {child2, Pid4} -> Pid4 end,
    Child2Pid2 ! {{stop, normal}, Self},
    %% Check child1 is terminated.
    receive
        {'DOWN', Child1Pid2Ref, _, _, shutdown} ->
            ok;
        {_childName, _Pid} ->
            exit(SupPid, kill),
            check_exit([StarterPid, SupPid]),
            test_server:fail({restarting_child_not_terminated, Child1Pid2})
    end,
    %% Let the restart complete.
    Child1Pid3 = receive {child1, Pid5} -> Pid5 end,
    Child1Pid3 ! Ok,
    Child2Pid3 = receive {child2, Pid6} -> Pid6 end,
    Child2Pid3 ! Ok,
    StarterPid ! {stop, Self},
    check_exit([StarterPid, SupPid]).
