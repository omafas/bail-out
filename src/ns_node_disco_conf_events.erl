%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2017 Couchbase, Inc.
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
-module(ns_node_disco_conf_events).

-behaviour(gen_event).

%% gen_event callbacks
-export([start_link/0, init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(state, {}).

start_link() ->
    % The ns_node_disco_conf_events gen_event handler will inform
    % me when relevant ns_config configuration changes.
    misc:start_event_link(fun () ->
                                  ok = gen_event:add_sup_handler(ns_config_events, ?MODULE, ignored)
                          end).

init(ignored) ->
    {ok, #state{}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.
handle_info(_Info, State)      -> {ok, State}.
handle_call(_Request, State)   -> {ok, ok, State}.

handle_event({nodes_wanted, _V}, State) ->
    ?log_debug("ns_node_disco_conf_events config on nodes_wanted"),
    % The event may get to us really late, so don't pass along the param.
    ns_node_disco:nodes_wanted_updated(),
    {ok, State};

handle_event({otp, _V}, State) ->
    ?log_debug("ns_node_disco_conf_events config on otp"),
    % The event may get to us really late, so don't pass along the param.
    ns_node_disco:nodes_wanted_updated(),
    {ok, State};

handle_event(_E, State) ->
    {ok, State}.
