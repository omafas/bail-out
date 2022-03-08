%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(cb_log_counter_sink).

-behaviour(gen_server).

%% API
-export([start_link/1, meta/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("ale/include/ale.hrl").

-record(state, {}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

meta() ->
    [{type, raw}].

init([]) ->
    {ok, #state{}, hibernate}.

handle_call({log, _Msg}, _From, State) ->
    {reply, ok, State};

handle_call({raw_log, #log_info{loglevel=LogLevel}, _Msg}, _From, State) ->
    catch ns_server_stats:notify_counter({logs, [{severity, LogLevel}]}),
    {reply, ok, State};

handle_call(sync, _From, State) ->
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
