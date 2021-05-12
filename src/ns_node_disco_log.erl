%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_node_disco_log).

-behaviour(gen_event).

-export([start_link/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(state, {}).

start_link() ->
    {ok, spawn_link(fun() ->
                            ok = gen_event:add_sup_handler(ns_node_disco_events,
                                                           ?MODULE, ignored),
                            receive
                                _ -> ok
                            end
                    end)}.

init(ignored) ->
    {ok, #state{}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_event({ns_node_disco_events, _NodesBefore, NodesAfter}, State) ->
    ?log_info("ns_node_disco_log: nodes changed: ~p", [NodesAfter]),
    {ok, State};

handle_event(_, State) ->
    {ok, State}.

handle_call(Request, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p)", [Request, State]),
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.
