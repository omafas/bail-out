%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(gen_server2_tests).

-behavior(gen_server2).

-include_lib("eunit/include/eunit.hrl").

-export([handle_call/3]).

start() ->
    {ok, Pid} = gen_server2:start(?MODULE, [], []),
    Pid.

handle_call(start_job, _From, State) ->
    Ok = make_ref(),
    Parent = self(),
    gen_server2:async_job(fun () ->
                                  register(async_job, self()),
                                  Parent ! Ok,
                                  timer:sleep(10000)
                          end,
                          fun (_, S) ->
                                  {noreply, S}
                          end),

    receive
        Ok ->
            ok
    end,

    {reply, ok, State}.

jobs_get_terminated_test_() ->
    {timeout, 1, fun do_test_jobs_get_terminated/0}.

do_test_jobs_get_terminated() ->
    Server1 = start(),
    ok = gen_server2:call(Server1, start_job),
    misc:terminate_and_wait(Server1, shutdown),
    ok = check_name(async_job),

    Server2 = start(),
    ok = gen_server2:call(Server2, start_job),
    misc:terminate_and_wait(Server2, kill),
    ok = check_name(async_job).

check_name(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            timer:sleep(10),
            check_name(Name)
    end.
