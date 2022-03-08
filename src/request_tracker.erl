%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(request_tracker).

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([request/2]).

-export([hibernate/4, unhibernate_trampoline/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {}).

-define(TABLE, ?MODULE).
-define(HIBERNATE_TABLE, request_tracker_hibernations).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

request(Type, Body) ->
    {ok, ThrottlerPid} = gen_server:call(?MODULE,
                                         {note_request, self(), Type},
                                         infinity),
    TypeBin = atom_to_binary(Type, latin1),
    ns_server_stats:notify_counter(<<TypeBin/binary, "_request_enters">>),
    try
        Body()
    after
        note_request_done(Type, ThrottlerPid)
    end.

hibernate(Req, M, F, A) ->
    ns_server_stats:notify_counter(<<"request_hibernates">>),
    gen_server:cast(?MODULE, {note_hibernate, self()}),
    menelaus_util:hibernate(Req, ?MODULE, unhibernate_trampoline, [M, F, A]).

unhibernate_trampoline(M, F, A) ->
    ns_server_stats:notify_counter(<<"request_unhibernates">>),
    gen_server:cast(?MODULE, {note_unhibernate, self()}),
    erlang:apply(M, F, A).

note_request_done(Type, ThrottlerPid) ->
    gen_server:cast(ThrottlerPid, {note_request_done, self(), Type}).

%% gen_server callbacks
init([]) ->
    ?TABLE = ets:new(?TABLE, [named_table, set, protected]),
    ?HIBERNATE_TABLE = ets:new(?HIBERNATE_TABLE, [named_table, set, protected]),
    {ok, #state{}}.

handle_call({note_request, Pid, Type}, _From, State) ->
    MRef = erlang:monitor(process, Pid),
    true = ets:insert_new(?TABLE, {Pid, Type, MRef}),
    {reply, {ok, self()}, State};
handle_call(Request, _From, State) ->
    ?log_error("Got unknown request ~p", [Request]),
    {reply, unhandled, State}.

handle_cast({note_hibernate, Pid}, State) ->
    true = ets:insert_new(?HIBERNATE_TABLE, {Pid, true}),
    {noreply, State};
handle_cast({note_unhibernate, Pid}, State) ->
    true = ets:delete(?HIBERNATE_TABLE, Pid),
    {noreply, State};
handle_cast({note_request_done, Pid, Type}, State) ->
    TypeBin = atom_to_binary(Type, latin1),
    ns_server_stats:notify_counter(<<TypeBin/binary, "_request_leaves">>),
    [{_, Type, MRef}] = ets:take(?TABLE, Pid),
    erlang:demonitor(MRef, [flush]),
    {noreply, State};
handle_cast(Cast, State) ->
    ?log_error("Got unknown cast ~p", [Cast]),
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    [{_, Type, MRef}] = ets:take(?TABLE, Pid),
    case ets:lookup(?HIBERNATE_TABLE, Pid) of
        [] ->
            ok;
        _ ->
            ets:delete(?HIBERNATE_TABLE, Pid),
            ns_server_stats:notify_counter(<<"request_unhibernates">>)
    end,

    TypeBin = atom_to_binary(Type, latin1),
    ns_server_stats:notify_counter(<<TypeBin/binary, "_request_leaves">>),
    {noreply, State};
handle_info(Msg, State) ->
    ?log_error("Got unknown message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
