%% @author Couchbase <info@couchbase.com>
%% @copyright 2015 Couchbase, Inc.
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
