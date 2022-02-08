%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(leader_registry_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    {ok, _} = gen_event:start_link({local, leader_events}),
    {ok, _} = leader_registry:start_link().

cleanup(_) ->
    misc:unlink_terminate_and_wait(whereis(leader_registry), shutdown),
    misc:unlink_terminate_and_wait(whereis(leader_events), shutdown).

basic_test_() ->
    {spawn,
     {setup, fun setup/0, fun cleanup/1, fun basic_test__/0}}.

basic_test__() ->
    Parent = self(),
    Wait = fun R() -> receive Msg -> Parent ! {self(), Msg}, R() end end,

    A = spawn_link(Wait),
    B = spawn_link(fun R() ->
                           receive
                               {unregister, Name} ->
                                   leader_registry:unregister_name(Name),
                                   Parent ! {self(), unregistered},
                                   R();
                               _ ->
                                   R()
                           end
                   end),
    C = spawn_link(Wait),

    undefined = leader_registry:whereis_name(a),
    undefined = leader_registry:whereis_name(b),

    ?assertExit(not_a_leader, leader_registry:register_name(a, A)),
    ?assertExit(not_a_leader, leader_registry:unregister_name(a)),

    gen_event:sync_notify(leader_events, {new_leader, node()}),

    lists:foreach(
      fun ({Name, Pid}) ->
              yes = leader_registry:register_name(Name, Pid),
              Pid = leader_registry:whereis_name(Name)
      end, [{a, A}, {b, B}, {c, C}]),

    ?assertExit({duplicate_name, _, _, _}, leader_registry:register_name(a, B)),

    leader_registry:send(a, test),
    receive
        {A, test} ->
            ok
    after
        1000 ->
            exit(no_message_received)
    end,

    ?assertExit({badarg, _}, leader_registry:send(d, test)),

    misc:unlink_terminate_and_wait(A, shutdown),
    undefined = leader_registry:whereis_name(a),

    ?assertExit(not_supported, leader_registry:unregister_name(b)),

    leader_registry:send(b, {unregister, b}),
    receive
        {B, unregistered} ->
            ok
    after
        1000 ->
            exit(unregister_timeout)
    end,
    undefined = leader_registry:whereis_name(b),

    %% Unknown names should work.
    ok = leader_registry:unregister_name(b),

    gen_event:sync_notify(leader_events, {new_leader, undefined}),
    %% make sure leader_registry has processed the notification
    _ = sys:get_state(leader_registry),
    undefined = leader_registry:whereis_name(c),

    misc:unlink_terminate_and_wait(B, shutdown),
    misc:unlink_terminate_and_wait(C, shutdown).

kill_test_() ->
    {spawn,
     {setup, fun setup/0, fun cleanup/1, fun kill_test__/0}}.

kill_test__() ->
    gen_event:sync_notify(leader_events, {new_leader, node()}),
    kill_test_loop(1000).

kill_test_loop(0) ->
    ok;
kill_test_loop(I) ->
    undefined = leader_registry:whereis_name(a),

    A = spawn_link(fun R() ->
                           receive _ ->
                                   R()
                           end
                   end),
    yes = leader_registry:register_name(a, A),
    misc:unlink_terminate_and_wait(A, kill),
    kill_test_loop(I - 1).
