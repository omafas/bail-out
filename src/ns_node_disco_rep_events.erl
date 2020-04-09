%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2012 Couchbase, Inc.
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
