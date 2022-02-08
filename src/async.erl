%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

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
         race/2, map/2, foreach/2,
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
    call_many(Pids, get_result, Flags).

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
    with_many(
      Fun, List,
      fun (Asyncs) ->
              _ = wait_many(Asyncs),
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
-endif.
