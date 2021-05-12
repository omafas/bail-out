%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_node_disco_rep_events).

-behaviour(gen_event).

%% API
-export([add_sup_handler/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(state, {}).

add_sup_handler() ->
    gen_event:add_sup_handler(ns_node_disco_events, ?MODULE, []).

init([]) ->
    {ok, #state{}}.

handle_event({ns_node_disco_events, Old, New}, State) ->
    case New -- Old of
        [] ->
            ok;
        NewNodes ->
            ?log_debug("Detected a new nodes (~p).  Moving config around.",
                       [NewNodes]),
            %% we know that new node will also try to replicate config
            %% to/from us. So we half our traffic by enforcing
            %% 'initiative' from higher node to lower node
            ns_config_rep:pull_and_push([N || N <- NewNodes, N < node()])
    end,
    {ok, State}.

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
