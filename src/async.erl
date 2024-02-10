%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(async).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start/1, start/2,
         start_many/2, start_many/3,
         abort/1, abort/2,
         abort_many/1, abort_many/2,
         send/2,
         with/2, with/3,
         with_many/3, with_many/4,
         wait/1, wait/2,
         wait_many/1, wait_many/2,
         wait_any/1, wait_any/2,
         race/2, map/2, foreach/2, foreach/3,
         run_with_timeout/2,
         get_identity/0]).

start(Fun) ->
    start(Fun, []).

start(Fun, Opts) ->
    SpawnFun =
        case proplists:get_value(monitor, Opts, false) of
            true ->
                fun misc:spawn_monitor/1;
            false ->
                fun proc_lib:spawn/1
        end,

    Parent           = self(),
    ParentController = get_controller(),

    SpawnFun(
      fun () ->
              async_init(Parent, ParentController, Opts, Fun)
      end).

start_many(Fun, Args) ->
    start_many(Fun, Args, []).

start_many(Fun, Args, Opts) ->
    [start(fun () ->
                   Fun(A)
           end, Opts) || A <- Args].

abort(Pid) ->
    abort_many([Pid]).

abort(Pid, Reason) ->
    abort_many([Pid], Reason).

abort_many(Pids) ->
    abort_many(Pids, shutdown).

abort_many(Pids, Reason) ->
    misc:terminate_and_wait(Pids, Reason).

send(Async, Msg) ->
    Async ! {'$async_msg', Msg},
    Msg.

with(AsyncBody, Fun) ->
    with(AsyncBody, [], Fun).

with(AsyncBody, Opts, Fun) ->
    Async = start(AsyncBody, Opts),
    try
        Fun(Async)
    after
        abort(Async)
    end.

with_many(AsyncBody, Args, Fun) ->
    with_many(AsyncBody, Args, [], Fun).

with_many(AsyncBody, Args, Opts, Fun) ->
    Asyncs = start_many(AsyncBody, Args, Opts),
    try
        Fun(Asyncs)
    after
        abort_many(Asyncs)
    end.

wait(Pid) ->
    wait(Pid, []).

wait(Pid, Flags) ->
    call(Pid, get_result, Flags).

wait_many(Pids) ->
    wait_many(Pids, []).

wait_many(Pids, Flags) ->
    case proplists:get_bool(exit_on_first_error, Flags) of
        false ->
            call_many(Pids, get_result, Flags);
        true ->
            call_many_and_exit_on_first_error(Pids, get_result, Flags)
    end.

wait_any(Pids) ->
    wait_any(Pids, []).

wait_any(Pids, Flags) ->
    call_any(Pids, get_result, Flags).

race(Fun1, Fun2) ->
    with(
      Fun1,
      fun (Async1) ->
              with(
                Fun2,
                fun (Async2) ->
                        case wait_any([Async1, Async2]) of
                            {Async1, R} ->
                                {left, R};
                            {Async2, R} ->
                                {right, R}
                        end
                end)
      end).

map(Fun, List) ->
    with_many(
      Fun, List,
      fun (Asyncs) ->
              Results = wait_many(Asyncs),
              [R || {_, R} <- Results]
      end).

foreach(Fun, List) ->
    foreach(Fun, List, []).

foreach(Fun, List, Flags) ->
    with_many(
      Fun, List,
      fun (Asyncs) ->
              _ = wait_many(Asyncs, Flags),
              ok
      end).

run_with_timeout(Fun, Timeout) ->
    try
        with(Fun, [{abort_after, Timeout}], ?cut({ok, wait(_)}))
    catch
        exit:timeout ->
            {error, timeout}
    end.

get_identity() ->
    case get_role() of
        executor ->
            Controller = get_controller(),
            true = is_pid(Controller),

            {ok, Controller};
        _ ->
            not_async
    end.

%% internal
async_init(Parent, ParentController, Opts, Fun) ->
    erlang:monitor(process, Parent),

    set_role(controller),
    maybe_register_with_parent_async(ParentController),

    Adopters = proplists:get_value(adopters, Opts, []),
    lists:foreach(register_for_adoption(_), Adopters),

    process_flag(trap_exit, true),

    Reply      = make_ref(),
    Controller = self(),

    Child =
        spawn_link(
          fun () ->
                  set_role(executor),
                  set_controller(Controller),

                  To = {Controller, Reply},

                  try Fun() of
                      R ->
                          reply(To, {ok, R})
                  catch
                      T:E:Stack ->
                          reply(To, {raised, {T, E, Stack}}),
                          erlang:raise(T, E, Stack)
                  end
          end),

    case proplists:get_value(abort_after, Opts) of
        undefined ->
            ok;
        infinity ->
            ok;
        AbortAfter when is_integer(AbortAfter) ->
            erlang:send_after(AbortAfter, self(), abort_after_expired)
    end,

    async_loop_wait_result(Child, Reply, []).

maybe_register_with_parent_async(undefined) ->
    ok;
maybe_register_with_parent_async(Pid) ->
    {ok, _} = register_with_async(Pid).

register_with_async(Pid) ->
    controller = get_role(),
    case call(Pid, {register_child_async, self()}) of
        {ok, _} = Ok ->
            Ok;
        nack ->
            ?log_debug("Received nack when trying to register with ~p", [Pid]),
            exit(normal)
    end.

async_loop_wait_result(Child, Reply, ChildAsyncs) ->
    receive
        {'DOWN', _MRef, process, Pid, Reason} = Down ->
            maybe_log_down_message(Down),

            %% We change the reason to a {shutdown, _} because we want to
            %% avoid polluting logs with crash reports. Since the process that
            %% died must have already produced a crash report and since it's
            %% not our process that is the cause of the problem, we suppress
            %% the crash report here.
            terminate_now(Child, ChildAsyncs,
                          {shutdown, {monitored_process_died, Pid, Reason}});
        {'EXIT', Child, Reason} ->
            terminate_on_query(undefined, ChildAsyncs, {child_died, Reason});
        %% note, we don't assume that this comes from the parent, because we
        %% can be terminated by parent async, for example, which is not the
        %% actual parent of our process
        {'EXIT', _, Reason} ->
            terminate_now(Child, ChildAsyncs, Reason);
        {'$async_req', From, {register_child_async, Pid}} ->
            reply(From, {ok, Child}),
            async_loop_wait_result(Child, Reply, [Pid | ChildAsyncs]);
        {Reply, Result} ->
            async_loop_handle_result(Child, ChildAsyncs, Result);
        {'$async_msg', Msg} ->
            Child ! Msg,
            async_loop_wait_result(Child, Reply, ChildAsyncs);
        abort_after_expired ->
            terminate_on_query(Child, ChildAsyncs, timeout)
    end.

maybe_terminate_child(undefined) ->
    ok;
maybe_terminate_child(Child)
  when is_pid(Child) ->
    misc:unlink_terminate(Child, shutdown).

terminate_children(Child, ChildAsyncs) ->
    MRefs = [erlang:monitor(process, Pid) || Pid <- [Child | ChildAsyncs],
                                             Pid =/= undefined],
    maybe_terminate_child(Child),
    lists:foreach(misc:terminate(_, shutdown), ChildAsyncs),
    terminate_children_loop(MRefs).

terminate_children_loop([]) ->
    ok;
terminate_children_loop([MRef | Rest] = MRefs) ->
    receive
        {'DOWN', MRef, process, _Pid, _Reason} ->
            terminate_children_loop(Rest);
        {'$async_req', From, {register_child_async, _Pid}} ->
            %% We need to continue responding to register_child_async
            %% requests. If async receives a termination request, it will send
            %% an exit signal to the executor process and will wait for it to
            %% terminate. But the executor might have just spawned a new async
            %% that will try to register with us and will get blocked. If it's
            %% also the case that the executor traps exits and waits on this
            %% newly spawned async and doesn't expect EXITs, we'll deadlock.
            reply(From, nack),
            terminate_children_loop(MRefs)
    end.

terminate_now(Child, ChildAsyncs, Reason) ->
    terminate_children(Child, ChildAsyncs),
    exit(Reason).

terminate_on_query(Child, ChildAsyncs, Reason) ->
    terminate_children(Child, ChildAsyncs),
    async_loop_with_result({die, Reason}).

async_loop_handle_result(Child, ChildAsyncs, Result) ->
    terminate_children(Child, ChildAsyncs),

    case Result of
        {ok, Success} ->
            async_loop_with_result({reply, Success});
        {raised, _} = Raised ->
            async_loop_with_result({die, Raised})
    end.

-spec async_loop_with_result({die, any()} | {reply, any()}) -> no_return().
async_loop_with_result(Result) ->
    receive
        {'DOWN', _MRef, process, Pid, Reason} = Down ->
            maybe_log_down_message(Down),

            %% {shutdown, _} so we don't produce a crash report.
            exit({shutdown, {monitored_process_died, Pid, Reason}});
        {'EXIT', _, Reason} ->
            exit(Reason);
        {'$async_req', From, get_result} ->
            handle_get_result(From, Result);
        {'$async_req', From, {register_child_async, _Pid}} ->
            %% We don't expect register requests at this point, but it's
            %% possible to write a correct async that has such behavior. If we
            %% don't reply, the requesting process will have to wait till we
            %% die, which is unnecessary. So we just respond with nack to kill
            %% it quickly.
            reply(From, nack),
            async_loop_with_result(Result);
        {'$async_req', _, _} = Req ->
            exit({unexpected_request, Req});
        _ ->
            async_loop_with_result(Result)
    end.

handle_get_result(From, {reply, Result}) ->
    reply(From, Result),
    exit(normal);
handle_get_result(_From, {die, Reason}) ->
    %% Wrapping the reason in {shutdown, _} so we don't produce an unneeded
    %% crash report.
    exit({shutdown, {async_died, Reason}}).

call(Pid, Req) ->
    call(Pid, Req, []).

call(Pid, Req, Flags) ->
    [{Pid, R}] = call_many([Pid], Req, Flags),
    R.

call_many(Pids, Req, Flags) ->
    PidMRefs = monitor_asyncs(Pids),
    try
        send_req_many(PidMRefs, Req),
        recv_many(PidMRefs, Flags)
    after
        demonitor_asyncs(PidMRefs)
    end.

call_many_and_exit_on_first_error(Pids, Req, Flags) ->
    Interruptible = proplists:get_bool(interruptible, Flags),

    Parent = self(),
    Ref = make_ref(),

    CallerPids =
        lists:map(
          fun (Pid) ->
                  spawn_link(
                    fun () ->
                            R = call(Pid, Req, Flags),
                            Parent ! {'$async_result', Ref,
                                      {self(), Pid, R}}
                    end)
          end, Pids),

    try
        Results = call_many_and_exit_on_first_error_receive_loop(
                    [], Ref, Pids, Interruptible),
        lists:map(
          fun (Pid) ->
                  lists:keyfind(Pid, 1, Results)
          end, Pids)
    after
        misc:unlink_terminate_many(CallerPids, shutdown),
        abort_many(Pids)
    end.

call_many_and_exit_on_first_error_receive_loop(
  Results, Ref, Pids, Interruptible) ->
    receive
        {'$async_result', Ref, {CallerPid, Pid, R}} ->
            %% Unlink the caller process once it is has sent a
            %% {'async_result', _, _} message.
            erlang:unlink(CallerPid),

            %% The process calling wait_many/2,3 could be trapping an exit -
            %% flush the mailbox of any 'EXIT' message from the CallerPid.
            case process_info(self(), trap_exit) of
                {trap_exit, true} ->
                    misc:flush({'EXIT', CallerPid, _});
                {trap_exit, false} ->
                    ok
            end,

            ResultsNew = [{Pid, R} | Results],
            case length(ResultsNew) =:= length(Pids) of
                true ->
                    ResultsNew;
                false ->
                    call_many_and_exit_on_first_error_receive_loop(
                      ResultsNew, Ref, Pids, Interruptible)
            end;
        {'EXIT', _Pid, _Reason} = Exit when Interruptible ->
            throw({interrupted, Exit})
    end.

call_any(Pids, Req, Flags) ->
    PidMRefs = monitor_asyncs(Pids),
    try
        send_req_many(PidMRefs, Req),
        recv_any(PidMRefs, Flags)
    after
        Pids = demonitor_asyncs(PidMRefs),
        abort_many(Pids),
        drop_extra_resps(PidMRefs)
    end.

drop_extra_resps(PidMRefs) ->
    lists:foreach(
      fun ({_, MRef}) ->
              ?flush({MRef, _})
      end, PidMRefs).

reply({Pid, Tag}, Reply) ->
    Pid ! {Tag, Reply}.

monitor_asyncs(Pids) ->
    [{Pid, erlang:monitor(process, Pid)} || Pid <- Pids].

demonitor_asyncs(PidMRefs) ->
    lists:map(
      fun ({Pid, MRef}) ->
              erlang:demonitor(MRef, [flush]),
              Pid
      end, PidMRefs).

send_req(Pid, MRef, Req) ->
    Pid ! {'$async_req', {self(), MRef}, Req}.

send_req_many(PidMRefs, Req) ->
    lists:foreach(
      fun ({Pid, MRef}) ->
              send_req(Pid, MRef, Req)
      end, PidMRefs).

recv_resp(MRef, Interruptible) ->
    receive
        {MRef, R} ->
            R;
        {'DOWN', MRef, _, _, Reason} ->
            recv_resp_handle_down(Reason);
        {'EXIT', _Pid, _Reason} = Exit when Interruptible ->
            throw({interrupted, Exit})
    end.

recv_resp_handle_down({shutdown, {async_died, Reason}}) ->
    recv_resp_handle_down(Reason);
%% The following clause can only occur when called recursively by the one
%% above. But keeping it in place in case we need to wait on an async running
%% older code.
recv_resp_handle_down({raised, {T, E, Stack}}) ->
    erlang:raise(T, E, Stack);
recv_resp_handle_down(Reason) ->
    exit(Reason).

recv_many(PidMRefs, Flags) ->
    Interruptible = proplists:get_bool(interruptible, Flags),
    [{Pid, recv_resp(MRef, Interruptible)} || {Pid, MRef} <- PidMRefs].


%% NOTE: The ordering of the messages in the mailbox for the process executing
%% recv_any can be changed, given how it is implemented.
%%
%% See a detailed discussion at:
%% https://review.couchbase.org/c/ns_server/+/178908/4..7/src/async.erl#b458

recv_any(PidMRefs, Flags) ->
    Interruptible = proplists:get_bool(interruptible, Flags),
    recv_any_loop(PidMRefs, Interruptible, []).

recv_any_loop(PidMRefs, Interruptible, PendingMsgs) ->
    receive
        {Ref, R} = Msg when is_reference(Ref) ->
            case lists:keyfind(Ref, 2, PidMRefs) of
                {Pid, Ref} ->
                    recv_any_loop_resend_pending(PendingMsgs),
                    {Pid, R};
                false ->
                    recv_any_loop(PidMRefs,
                                  Interruptible,
                                  [Msg | PendingMsgs])
            end;
        {'DOWN', Ref, _, _, Reason} = Msg ->
            case lists:keymember(Ref, 2, PidMRefs) of
                true ->
                    recv_any_loop_resend_pending(PendingMsgs),
                    recv_resp_handle_down(Reason);
                false ->
                    recv_any_loop(PidMRefs,
                                  Interruptible,
                                  [Msg | PendingMsgs])
            end;
        {'EXIT', _Pid, _Reason} = Exit when Interruptible ->
            throw({interrupted, Exit})
    end.

recv_any_loop_resend_pending(PendingMsgs) ->
    lists:foreach(
      fun (Msg) ->
              self() ! Msg
      end, lists:reverse(PendingMsgs)).

set_role(Role) ->
    erlang:put('$async_role', Role).

get_role() ->
    erlang:get('$async_role').

set_controller(Pid) when is_pid(Pid) ->
    executor = get_role(),
    erlang:put('$async_controller', Pid).

get_controller() ->
    erlang:get('$async_controller').

register_for_adoption(Controller) ->
    {ok, Executor} = register_with_async(Controller),
    erlang:monitor(process, Executor).

maybe_log_down_message({'DOWN', _MRef, process, Pid, Reason}) ->
    case misc:is_normal_termination(Reason) of
        true ->
            ok;
        false ->
            ?log_warning("Monitored process ~p "
                         "terminated abnormally (reason = ~p)", [Pid, Reason])
    end.


-ifdef(TEST).
race_test() ->
    0 = ?flush(_),

    {_, Result} = race(?cut(a), ?cut(b)),
    ?assert(lists:member(Result, [a, b])),

    WaitFun = fun () ->
                      Ref = make_ref(),
                      receive
                          Ref ->
                              ok
                      end
              end,

    ?assertEqual({left, a}, race(?cut(a), WaitFun)),
    ?assertEqual({right, b}, race(WaitFun, ?cut(b))),

    0 = ?flush(_).

abort_after_test() ->
    A1 = async:start(?cut(timer:sleep(10000)), [{abort_after, 100}]),
    ?assertExit(timeout, async:wait(A1)),

    A2 = async:start(?cut(timer:sleep(10000)), [{abort_after, 100}]),
    timer:sleep(200),
    ?assertExit(timeout, async:wait(A2)),

    ok = async:with(?cut(timer:sleep(100)),
                    [{abort_after, 200}], async:wait(_)),

    ok = async:with(?cut(timer:sleep(100)),
                    [{abort_after, infinity}], async:wait(_)).

run_with_timeout_test() ->
    {ok, good} = run_with_timeout(?cut(good), 1000),
    {ok, good} = run_with_timeout(?cut(good), infinity),
    {error, timeout} = run_with_timeout(?cut(timer:sleep(1000)), 100).

exceptions_rethrown_test() ->
    ?assertThrow(test, with(?cut(throw(test)), wait(_))),
    ?assertThrow(test2,
                 with(fun () ->
                              with(?cut(throw(test2)), ?cut(wait(_)))
                      end, wait(_))).

async_trap_exit_test() ->
    %% Test that we can abort an async (A), whose body traps exits and spawns
    %% another async (B) that tries to register with A after A has received a
    %% termination request.

    Parent = self(),
    A = async:start(
          fun () ->
                  process_flag(trap_exit, true),
                  Parent ! {child, self()},
                  receive
                      go -> ok
                  end,

                  B = async:start(fun () ->
                                          ok
                                  end),
                  async:wait(B)
          end),

    Child = receive
                {child, Pid} ->
                    Pid
            end,
    Aborter = spawn(fun() -> async:abort(A) end),
    Child ! go,
    ok = misc:wait_for_process(Aborter, infinity).

async_tree_does_not_collapse_test() ->
    %% 1. Spawn 2 child asyncs (Child1 and Child2) via a parent async.
    %% 2. Child1 doesn't get terminated even when Child2 has terminated.
    %% 3. Eventually parent async timeouts.

    AsyncChildsFun =
        fun () ->
            Child1 = async:start(fun() ->
                                         process_flag(trap_exit, true),
                                         receive
                                             {'EXIT', _, _} ->
                                                 ok
                                         end
                                 end),
            Child2 = async:start(fun() -> exit(not_ok) end),
            Children = [Child1, Child2],

            try
                async:wait_many(Children)
            after
                async:abort_many(Children)
            end
        end,

    {error, timeout} = async:run_with_timeout(AsyncChildsFun, 100).

start_shutdown_reporting_async(GrandParent) ->
    Parent = self(),
    MRef = make_ref(),

    Child = async:start(
              fun() ->
                      process_flag(trap_exit, true),
                      Parent ! {MRef, child_init_done},
                      receive
                          {'EXIT', _, shutdown} ->
                              GrandParent ! grandchild_shutdown,
                              ok
                      end
              end),

    receive
        {MRef, child_init_done} ->
            ok
    after
        1000 ->
            exit(setup_error)
    end,
    Child.

async_tree_collapses_test() ->
    %% 1. Spawn 2 child asyncs (Child1 and Child2) via a parent Async.
    %% 2. Child1 exit with a non-normal exit and Child2 gets terminated.

    0 = ?flush(_),
    GrandParent = self(),

    AsyncChildsFun =
        fun () ->
                Child1 = start_shutdown_reporting_async(GrandParent),
                Child2 = async:start(fun() -> exit(not_ok) end),
                Children = [Child2, Child1],

                try
                    async:wait_many(Children)
                after
                    async:abort_many(Children)
                end
        end,

    ?assertExit(not_ok, async:run_with_timeout(AsyncChildsFun, 1000)),

    GrandChildShutdown = receive
                             grandchild_shutdown ->
                                 true
                         after
                             1000 ->
                                 ?flush(grandchild_shutdown),
                                 false
                         end,

    ?assert(GrandChildShutdown),
    0 = ?flush(_).

async_interruptible_test() ->
    0 = ?flush(_),
    GrandParent = self(),

    Pid = erlang:spawn(
            fun () ->
                    erlang:process_flag(trap_exit, true),
                    async:with(
                      fun () ->
                              erlang:process_flag(trap_exit, true),
                              receive
                                  {'EXIT', _, shutdown} ->
                                      GrandParent ! grandchild_shutdown
                              end
                      end,
                      fun (Async) ->
                              GrandParent ! grandchild_spawned,
                              try
                                  async:wait(Async, [interruptible])
                              catch
                                  throw:{interrupted, {'EXIT', _, Reason}} ->
                                      async:abort(Async),
                                      exit(Reason)
                              end
                      end)
            end),

    receive
        grandchild_spawned ->
            ok
    after
        500 ->
            ?flush(grandchild_spawned),
            ?assert(false)
    end,

    exit(Pid, shutdown),

    GrandChildShutdown = receive
                             grandchild_shutdown ->
                                 true
                         after
                             500 ->
                                 ?flush(grandchild_shutdown),
                                 false
                         end,

    ?assert(GrandChildShutdown),
    0 = ?flush(_).

async_tree_collapses_2_test() ->
    %% 1. Spawn 2 child asyncs (Child1 and Child2) via a parent Async.
    %% 2. Child2 exits with a non-normal exit and Child1 gets terminated.
    %%
    %% The parent async returns the error on which Child2 async exited with.

    0 = ?flush(_),
    GrandParent = self(),

    AsyncChildsFun =
        fun () ->
                Child1 = start_shutdown_reporting_async(GrandParent),
                Child2 = async:start(fun() -> exit(not_ok) end),
                Children = [Child1, Child2],

                async:wait_many(Children, [exit_on_first_error, interruptible])
        end,

    ?assertExit({child_died, not_ok},
                async:run_with_timeout(AsyncChildsFun, 1000)),

    GrandChildShutdown = receive
                             grandchild_shutdown ->
                                 true
                         after
                             1000 ->
                                 ?flush(grandchild_shutdown),
                                 false
                         end,

    ?assert(GrandChildShutdown),
    0 = ?flush(_).

async_tree_success_test() ->
    %% 1. Spawn two child asyncs 'a' and 'b'.
    %% 2. Collect the results returned from the Child asyncs and confirm both
    %%    'a' and 'b' were received.
    AsyncChildsFun =
        fun () ->
                async:with_many(
                  fun (C) -> C end, [a, b],
                  fun (Asyncs) ->
                          async:wait_many(Asyncs, [exit_on_first_error])
                  end)
        end,

    {ok, Res} = async:run_with_timeout(AsyncChildsFun, 1000),

    AsyncsResults = [R || {_, R} <- Res],

    ?assertEqual(AsyncsResults, [a, b]).

async_tree_success_1_test() ->
    %% 1. Spawn two 'monitor'-ed child asyncs 'a' and 'b'.
    %% 2. Collect the results returned from the Child asyncs and confirm both
    %%    'a' and 'b' were received and that the returned result is in the order
    %%    in which the asyncs were spawned despite 'a' being slower than 'b'.
    AsyncChildsFun =
        fun () ->
                async:with_many(
                  fun ({Sleep, C}) ->
                          case Sleep of
                              true ->
                                  timer:sleep(100);
                              false ->
                                  ok
                          end,
                          C
                  end, [{true, a}, {false, b}], [{monitor}],
                  fun (Asyncs) ->
                          async:wait_many(Asyncs, [exit_on_first_error])
                  end)
        end,

    {ok, Res} = async:run_with_timeout(AsyncChildsFun, 1000),

    AsyncsResults = [R || {_, R} <- Res],

    ?assertEqual(AsyncsResults, [a, b]).

run_ntimes(Ns, Fun) ->
    lists:foreach(
      fun (N) ->
              ?log_debug("Run ~p: started.", [N]),
              Fun(),
              ?log_debug("Run ~p: completed", [N])
      end, lists:seq(1, Ns)).

%% Randomized tests to test 'exit_on_first_error' flag.
async_randomized_test_success_test() ->
    run_ntimes(100, ?cut(async_randomized_test_success(
                           100 + rand:uniform(50)))).

async_randomized_test_success(NumChildren) ->
    Children = [list_to_atom("child-" ++ integer_to_list(N))
                ||  N <- lists:seq(1, NumChildren)],

    AsyncChildrenFun =
        fun () ->
                async:with_many(
                  fun (C) ->
                          timer:sleep(rand:uniform(5)),
                          C
                  end, Children,
                  fun (Asyncs) ->
                          async:wait_many(Asyncs, [exit_on_first_error])
                  end)
        end,

    {ok, Res} = async:run_with_timeout(AsyncChildrenFun, 1500),

    AsyncsResults = [R || {_, R} <- Res],

    ?assertEqual(AsyncsResults, Children).

async_randomized_test_failure_test() ->
    {timeout, 100 * 5 * 1000,
     run_ntimes(100, ?cut(async_randomized_test_failure(
                            100 + rand:uniform(50))))}.

async_randomized_test_failure(NumChildren) ->

    %% Create 3 sets of Asyncs:
    %% 1. FaultyChildren - which sleep for 1 msec to 5 msecs and exit with
    %% 'not_ok' error.
    %% 2. SlowChildren - which don't run to completion.
    %% 3. FastChildren - which exit normally.

    %% We monitor all these process in the Executor process and when one of the
    %% FaultyChildren exits all of the processes should be aborted. We assert
    %% all of them have been aborted by checking we have received 'NumChildren'
    %% 'DOWN' messages.

    Children = [list_to_atom("child-" ++ integer_to_list(N))
                ||  N <- lists:seq(1, NumChildren)],

    {FaultyChildren, RestChildren} =
        lists:split(rand:uniform(NumChildren - 2), Children),
    {SlowChildren, _} =
        lists:split(rand:uniform(length(RestChildren)), RestChildren),

    SleepTimes = [{C, rand:uniform(5)} || C <- FaultyChildren],

    ?log_debug("Test params:~nFaultyChildren - ~p.~nSlowChildren - ~p.~n"
               "RestChildren - ~p.~n"
               "SleepTimes - ~p.",
               [FaultyChildren, SlowChildren, RestChildren, SleepTimes]),

    0 = ?flush(_),

    Parent = self(),

    Executor =
        spawn(
          fun () ->
                  Executor = self(),
                  AsyncChildrenFun =
                      fun () ->
                              async:with_many(
                                fun (C) ->
                                        Executor ! {monitor, self()},
                                        receive
                                            proceed ->
                                                ok
                                        end,

                                        case lists:member(C, FaultyChildren) of
                                            true ->
                                                Time =
                                                    proplists:get_value(
                                                      C, SleepTimes),
                                                timer:sleep(Time),
                                                exit(not_ok);
                                            false ->
                                                case lists:member(
                                                       C, SlowChildren) of
                                                    true ->
                                                        receive
                                                            alien_message ->
                                                                %% wait forever.
                                                                ok
                                                        end;
                                                    false ->
                                                        ok
                                                end
                                        end
                                end, Children,
                                fun (Asyncs) ->
                                        async:wait_many(
                                          Asyncs, [exit_on_first_error])
                                end)
                      end,
                  MonitorAllAsyncsAndProceedFun =
                      fun F (Pids) ->
                              receive
                                  {monitor, Pid} ->
                                      erlang:monitor(process, Pid),
                                      Pids1 = [Pid | Pids],

                                      case length(Pids1) =:= NumChildren of
                                          true ->
                                              lists:foreach(
                                                fun (P) ->
                                                        P ! proceed
                                                end, Pids1);
                                          false ->
                                              F(Pids1)
                                      end
                              end
                      end,

                  Res =
                      try
                          async:with(
                            AsyncChildrenFun, [{abort_after, 1500}],
                            fun (Async) ->
                                    MonitorAllAsyncsAndProceedFun([]),
                                    async:wait(Async)
                            end),
                          %% There are faulty children and the above async run
                          %% should error out and therefore it's 'not_ok' if it
                          %% completed successfully.
                          not_ok
                      catch
                          exit:timeout ->
                              {error, timeout};
                          exit:{child_died, not_ok} ->
                              %% Once of the faultyChildren have exited and
                              %% therefore convert this to a 'ok'.
                              ok
                      end,
                  TestResult =
                      case Res of
                          ok ->
                              try
                                  ?must_flush({'DOWN', _, process, _, _},
                                              NumChildren, 1000)
                              catch
                                  throw:{error,
                                         {no_messages, _Timeout, _Tag}} ->
                                      {error, all_asyncs_not_aborted}
                              end;
                          Error ->
                              Error
                      end,
                  Parent ! {rand_test_result, TestResult}
          end),

    receive
        {rand_test_result, TestResult} ->
            ?assertEqual(ok, TestResult)
    after
        3500 ->
            exit(Executor, kill),
            ?flush({rand_test_result, _}),
            ?assert(false)
    end,

    0 = ?flush(_).
-endif.
