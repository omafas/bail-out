%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ns_pubsub).

-behaviour(gen_event).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {func, func_state}).

%% API
-export([subscribe_link/1, subscribe_link/2, subscribe_link/3, unsubscribe/1]).

%% gen_event callbacks
-export([code_change/3, init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2]).

%% called by proc_lib:start from subscribe_link/3
-export([do_subscribe_link/4,
         do_subscribe_link_continue/3]).


%%
%% API
%%
subscribe_link(Name) ->
    subscribe_link(Name, msg_fun(self()), ignored).

-spec subscribe_link(any(), fun((term()) -> Ignored :: any())) -> pid().
subscribe_link(Name, Fun) ->
    subscribe_link(
      Name,
      fun (Event, State) ->
              Fun(Event),
              State
      end, ignored).

-spec subscribe_link(any(),
                     fun((Event :: term(),
                          State :: any()) -> NewState :: any()),
                     InitState :: any()) -> pid().
subscribe_link(Name, Fun, InitState) ->
    case proc_lib:start(?MODULE, do_subscribe_link,
                        [Name, Fun, InitState, self()]) of
        {error, Error} ->
            error(Error);
        Pid when is_pid(Pid) ->
            Pid
    end.

unsubscribe(Pid) ->
    Pid ! unsubscribe,
    misc:wait_for_process(Pid, infinity),

    %% consume exit message in case trap_exit is true
    receive
        %% we expect the process to die normally; if it's not the case then
        %% this should be handled explicitly by parent process;
        {'EXIT', Pid, normal} ->
            ok
    after 0 ->
            ok
    end.

%%
%% gen_event callbacks
%%
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


init(State) ->
    {ok, State}.


handle_call(_Request, State) ->
    {ok, ok, State}.


handle_event(Event, State = #state{func=Fun, func_state=FS}) ->
    NewState = Fun(Event, FS),
    {ok, State#state{func_state=NewState}}.


handle_info(_Msg, State) ->
    {ok, State}.


terminate(_Args, _State) ->
    ok.


%%
%% Internal functions
%%

%% Function sending a message to a pid
msg_fun(Pid) ->
    fun (Event, ignored) ->
            Pid ! Event,
            ignored
    end.


do_subscribe_link(Name, Fun, State, Parent) ->
    process_flag(trap_exit, true),
    erlang:link(Parent),

    Ref = make_ref(),
    Handler = {?MODULE, Ref},
    ok = gen_event:add_sup_handler(Name, Handler,
                                   #state{func=Fun, func_state=State}),

    proc_lib:init_ack(Parent, self()),

    proc_lib:hibernate(?MODULE, do_subscribe_link_continue,
                       [Name, Parent, Handler]).

do_subscribe_link_continue(Name, Parent, Handler) ->
    ExitReason =
        receive
            unsubscribe ->
                normal;
            {'gen_event_EXIT', Handler, Reason} ->
                case Reason of
                    normal ->
                        %% normal means that our handler got removed; we
                        %% obviously don't expect this to happen
                        {handler_removed_unexpectedly, Name};
                    shutdown ->
                        %% gen_event is shutting down; we don't want the
                        %% subscriber to ignore this;
                        ?log_debug("gen_event ~p is shutting down. "
                                   "Propagating to the subscriber ~p.",
                                   [Name, Parent]),
                        {gen_event_shutdown, Name};
                    _ ->
                        {handler_crashed, Name, Reason}
                end;
            {'EXIT', Parent, Reason} ->
                ?log_debug("Parent process of subscription ~p "
                           "exited with reason ~p", [{Name, Parent}, Reason],
                                                    [{chars_limit, 1000}]),
                normal;
            {'EXIT', Pid, Reason} ->
                ?log_debug("Linked process ~p of subscription ~p "
                           "died unexpectedly with reason ~p",
                           [Pid, {Name, Parent}, Reason],
                           [{chars_limit, 1000}]),
                {linked_process_died, Pid, Reason};
            X ->
                ?log_error("Subscription ~p got unexpected message: ~p",
                           [{Name, Parent}, X]),
                unexpected_message
        end,

    (catch gen_event:delete_handler(Name, Handler, unused)),

    exit(ExitReason).


-ifdef(TEST).

-define(N, 100).
-define(TABLE, ns_pubsub_test_table).
-define(COUNTER, counter).
-define(EVENTS, ns_pubsub_test_events).

times(0, _F, _A) ->
    ok;
times(N, F, A) ->
    apply(F, A),
    times(N - 1, F, A).

kill_silently_sync(P) ->
    misc:unlink_terminate_and_wait(P, kill).

create_table() ->
    ?TABLE = ets:new(?TABLE, [public, set, named_table]),
    N = rand:uniform(1000000),
    true = ets:insert_new(?TABLE, {?COUNTER, N}),
    ok.

table_updater() ->
    proc_lib:spawn_link(
      fun () ->
              table_updater_loop()
      end).

table_updater_loop() ->
    NewCounter = ets:update_counter(?TABLE, ?COUNTER, 1),
    gen_event:notify(?EVENTS, {counter_updated, NewCounter}),
    table_updater_loop().

setup() ->
    setup(msg_fun(self()), ignored).

setup(Fun, State) ->
    ok = create_table(),
    {ok, EventMgr} = gen_event:start_link({local, ?EVENTS}),
    Updater = table_updater(),
    Subscription = ns_pubsub:subscribe_link(?EVENTS, Fun, State),
    {EventMgr, Updater, Subscription}.

cleanup({EventMgr, Updater, Subscription}) ->
    kill_silently_sync(Updater),
    kill_silently_sync(Subscription),
    kill_silently_sync(EventMgr),

    true = ets:delete(?TABLE),
    flush_all().

wrap(Setup, Cleanup, Body) ->
    R = Setup(),
    try
        Body(R)
    after
        Cleanup(R)
    end.

flush_all() ->
    receive
        _Msg ->
            flush_all()
    after
        0 ->
            ok
    end.

subscribe_test_() ->
    {spawn, [{timeout, 20, ?_test(times(?N, fun test_subscribe/0, []))}]}.

%% test that we don't lose notifications right after subscription
test_subscribe() ->
    wrap(
      fun setup/0, fun cleanup/1,
      fun (_) ->
              [{?COUNTER, Initial}] = ets:lookup(?TABLE, ?COUNTER),

              receive
                  {counter_updated, Value} ->
                      true = (Value =< Initial + 1)
              end
      end).

unsubscribe_test_() ->
    {spawn, [{timeout, 20, ?_test(times(?N, fun test_unsubscribe/0, []))}]}.

%% test that we don't receive new updates after we've unsubscribed
test_unsubscribe() ->
    wrap(
      fun setup/0, fun cleanup/1,
      fun ({_EventMgr, _Updater, Subscription}) ->
              ok = ns_pubsub:unsubscribe(Subscription),
              flush_all(),

              Msg =
                  receive
                      {counter_updated, _} = M ->
                          M
                  after
                      10 ->
                          timeout
                  end,

              ?assertEqual(timeout, Msg)
      end).

kill_test_() ->
    {spawn, [?_test(test_shutdown()),
             ?_test(test_crash()),
             ?_test(test_parent_crash()),
             ?_test(test_event_mgr_crash())]}.

just_fail() ->
    %% NOTE: nif_error is to silence dialyzer
    erlang:nif_error(test_timeout_hit).

test_shutdown() ->
    wrap(
      fun setup/0, fun cleanup/1,
      fun ({EventMgr, Updater, Subscription}) ->
              process_flag(trap_exit, true),

              exit(Updater, kill),
              exit(EventMgr, shutdown),

              receive
                  {'EXIT', Subscription, {gen_event_shutdown, ?EVENTS}} ->
                      ok
              after
                  1000 ->
                      just_fail()
              end
      end).

test_crash() ->
    Crasher =
        fun (crash, _) ->
                exit(crashed);
            (_, _) ->
                ok
        end,

    wrap(
      fun () -> setup(Crasher, ignored) end, fun cleanup/1,
      fun ({_EventMgr, Updater, Subscription}) ->
              process_flag(trap_exit, true),

              exit(Updater, kill),
              gen_event:notify(?EVENTS, crash),

              receive
                  {'EXIT', Subscription, {handler_crashed, _, {'EXIT', crashed}}} ->
                      ok
              after
                  1000 ->
                      just_fail()
              end
      end).

test_event_mgr_crash() ->
    wrap(
      fun setup/0, fun cleanup/1,
      fun ({EventMgr, Updater, Subscription}) ->
              process_flag(trap_exit, true),

              exit(Updater, kill),
              exit(EventMgr, kill),

              receive
                  {'EXIT', Subscription, {linked_process_died, _, killed}} ->
                      ok
              after
                  1000 ->
                      just_fail()
              end
      end).

test_parent_crash() ->
    process_flag(trap_exit, true),
    Parent = self(),

    Pid =
        proc_lib:spawn(
          fun () ->
                  wrap(
                    fun setup/0, fun cleanup/1,
                    fun ({EventMgr, Updater, _} = R) ->
                            unlink(EventMgr),
                            unlink(Updater),

                            Parent ! ({self(), R}),

                            simply_sleep()
                    end)
          end),

    receive
        {Pid, {EventMgr, Updater, Subscription}} ->
            link(Subscription),
            exit(Pid, crash),

            try
                receive
                    {'EXIT', Subscription, normal} ->
                        ok
                after
                    1000 ->
                        just_fail()
                end
            after
                kill_silently_sync(Updater),
                kill_silently_sync(Subscription),
                kill_silently_sync(EventMgr)
            end
    end.

simply_sleep() ->
    timer:sleep(10000),
    simply_sleep().
-endif.
