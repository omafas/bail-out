%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ale_stderr_sink).

-behaviour(gen_server).

%% API
-export([start_link/1, meta/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ale.hrl").

-record(state, { port :: port() }).

start_link(Name) ->
    start_link(Name, []).

start_link(Name, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

meta() ->
    [{type, preformatted}].

init([_Opts]) ->
    process_flag(trap_exit, true),

    Port = open_port({fd, 2, 2}, [out, binary]),
    {ok, #state{port = Port}}.

handle_call({log, Msg}, _From, State) ->
    RV = do_log(Msg, State),
    {reply, RV, State};

handle_call(sync, _From, State) ->
    {reply, ok, State};

handle_call(Request, _From, State) ->
    {stop, {unexpected_call, Request}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'EXIT', Port, Reason}, #state{port = Port} = State) ->
    {stop, {stderr_port_died, Reason}, State};
handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_log(Msg, #state{port = Port}) when is_binary(Msg) ->
    erlang:port_command(Port, Msg),
    ok.
