%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(one_shot_barrier).

-include("ns_common.hrl").

-export([start_link/1, notify/1, wait/1]).

start_link(Name) ->
    proc_lib:start_link(erlang, apply, [fun barrier_body/1, [Name]]).

notify(Name) ->
    ?log_debug("Notifying on barrier ~p", [Name]),

    ok = gen_server:call(Name, notify),
    ok = misc:wait_for_process(Name, infinity),

    ?log_debug("Successfuly notified on barrier ~p", [Name]),

    ok.

wait(Name) ->
    MRef = erlang:monitor(process, Name),
    receive
        {'DOWN', MRef, process, _, Reason} ->
            case Reason of
                noproc ->
                    %% barrier has already been signaled on
                    ok;
                normal ->
                    ok;
                _ ->
                    exit({barrier_died, Reason})
            end;
        Msg ->
            exit({unexpected_message, Msg})
    end.

%% internal
barrier_body(Name) ->
    erlang:register(Name, self()),
    proc_lib:init_ack({ok, self()}),

    ?log_debug("Barrier ~p has started", [Name]),

    receive
        {'$gen_call', {FromPid, _} = From, notify} ->
            ?log_debug("Barrier ~p got notification from ~p", [Name, FromPid]),
            gen_server:reply(From, ok),
            exit(normal);
        Msg ->
            ?log_error("Barrier ~p got unexpected message ~p", [Name, Msg]),
            exit({unexpected_message, Msg})
    end.
