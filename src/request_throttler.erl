%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2014 Couchbase, Inc.
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

-module(request_throttler).

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([request/3]).

-export([hibernate/3, unhibernate_trampoline/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {}).

-define(TABLE, ?MODULE).
-define(HIBERNATE_TABLE, request_throttler_hibernations).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

request(Type, Body, RejectBody) ->
    case note_request(Type) of
        {ok, ThrottlerPid} ->
            do_request(Type, Body, ThrottlerPid);
        {reject, Error} ->
            system_stats_collector:increment_counter({Type, Error}, 1),
            RejectBody(Error, describe_error(Error))
    end.

hibernate(M, F, A) ->
    gen_server:cast(?MODULE, {note_hibernate, self()}),
    erlang:hibernate(?MODULE, unhibernate_trampoline, [M, F, A]).

unhibernate_trampoline(M, F, A) ->
    gen_server:cast(?MODULE, {note_unhibernate, self()}),
    erlang:apply(M, F, A).

do_request(Type, Body, ThrottlerPid) ->
    try
        system_stats_collector:increment_counter({request_enters, Type}, 1),
        Body()
    after
        note_request_done(Type, ThrottlerPid)
    end.

note_request(Type) ->
    case memory_usage() < memory_limit() of
        true ->
            gen_server:call(?MODULE, {note_request, self(), Type}, infinity);
        false ->
            {reject, memory_limit_exceeded}
    end.

note_request_done(Type, ThrottlerPid) ->
    gen_server:cast(ThrottlerPid, {note_request_done, self(), Type}).

%% gen_server callbacks
init([]) ->
    ?TABLE = ets:new(?TABLE, [named_table, set, protected]),
    ?HIBERNATE_TABLE = ets:new(?HIBERNATE_TABLE, [named_table, set, protected]),
    {ok, #state{}}.

handle_call({note_request, Pid, Type}, _From, State) ->
    Limit = request_limit(Type),
    ets:insert_new(?TABLE, {Type, 0}),
    [{_, Old}] = ets:lookup(?TABLE, Type),
    RV = case Old >= Limit of
             true ->
                 {reject, request_limit_exceeded};
             false ->
                 ets:update_counter(?TABLE, Type, 1),
                 MRef = erlang:monitor(process, Pid),
                 true = ets:insert_new(?TABLE, {Pid, Type, MRef}),
                 {ok, self()}
         end,
    {reply, RV, State};
handle_call(Request, _From, State) ->
    ?log_error("Got unknown request ~p", [Request]),
    {reply, unhandled, State}.

handle_cast({note_hibernate, Pid}, State) ->
    system_stats_collector:increment_counter({request_enters, hibernate}, 1),
    true = ets:insert_new(?HIBERNATE_TABLE, {Pid, true}),
    {noreply, State};
handle_cast({note_unhibernate, Pid}, State) ->
    system_stats_collector:increment_counter({request_leaves, hibernate}, 1),
    true = ets:delete(?HIBERNATE_TABLE, Pid),
    {noreply, State};
handle_cast({note_request_done, Pid, Type}, State) ->
    system_stats_collector:increment_counter({request_leaves, Type}, 1),
    Count = ets:update_counter(?TABLE, Type, -1),
    true = (Count >= 0),

    [{_, Type, MRef}] = ets:lookup(?TABLE, Pid),
    erlang:demonitor(MRef, [flush]),
    true = ets:delete(?TABLE, Pid),
    {noreply, State};
handle_cast(Cast, State) ->
    ?log_error("Got unknown cast ~p", [Cast]),
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    [{_, Type, MRef}] = ets:lookup(?TABLE, Pid),
    true = ets:delete(?TABLE, Pid),
    case ets:lookup(?HIBERNATE_TABLE, Pid) of
        [] ->
            ok;
        _ ->
            ets:delete(?HIBERNATE_TABLE, Pid),
            system_stats_collector:increment_counter({request_leaves, hibernate}, 1)
    end,

    system_stats_collector:increment_counter({request_leaves, Type}, 1),
    Count = ets:update_counter(?TABLE, Type, -1),
    true = (Count >= 0),

    {noreply, State};
handle_info(Msg, State) ->
    ?log_error("Got unknown message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
memory_limit() ->
    Limit = ns_config:read_key_fast(drop_request_memory_threshold_mib,
                                    undefined),
    case Limit of
        undefined ->
            1 bsl 64;
        _ ->
            Limit
    end.

memory_usage() ->
    try
        Usage = erlang:memory(total),
        Usage bsr 20
    catch
        error:notsup ->
            0
    end.

request_limit(Type) ->
    Limit = ns_config:read_key_fast({request_limit, Type},
                                    undefined),
    case Limit of
        undefined ->
            1 bsl 64;
        _ ->
            Limit
    end.

describe_error(memory_limit_exceeded) ->
    "Request throttled because memory limit has been exceeded";
describe_error(request_limit_exceeded) ->
    "Request throttled because maximum "
        "number of simultaneous connections has been exceeded".
