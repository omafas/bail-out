%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% A module for any not per-bucket remote calls.
-module(remote_api).

-behavior(gen_server).

-include("ns_common.hrl").

-define(DEFAULT_TIMEOUT, ?get_timeout(default, 10000)).

%% remote calls
-export([get_indexes/1, get_fts_indexes/1, get_service_remote_items/2,
         apply_node_settings/2, invalidate_ldap_cache/1]).

%% gen_server callbacks and functions
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% remote calls

%% introduced in 4.0
get_indexes(Node) ->
    do_call(Node, get_indexes).

%% introduced in 4.5
get_fts_indexes(Node) ->
    do_call(Node, get_fts_indexes).

%% introduced in 5.5
get_service_remote_items(Node, Mod) ->
    do_call(Node, {get_service_remote_items, Mod}).

%% introduced in 6.5
apply_node_settings(Node, Settings) ->
    do_call(Node, {apply_node_settings, Settings}).

%% introduced in 6.5
invalidate_ldap_cache(Nodes) ->
    gen_server:multi_call(Nodes, ?MODULE, invalidate_ldap_cache,
                          get_timeout(invalidate_ldap_cache)).

%% gen_server callbacks and functions
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, {}}.

handle_call(get_indexes, _From, State) ->
    {reply, service_index:get_indexes(), State};
handle_call(get_fts_indexes, _From, State) ->
    {reply, service_fts:get_indexes(), State};
handle_call({get_service_remote_items, Mod}, _From, State) ->
    {reply, service_status_keeper:get_items(Mod), State};
handle_call({apply_node_settings, Settings}, _From, State) ->
    {reply, menelaus_web_node:apply_node_settings(Settings), State};
handle_call(invalidate_ldap_cache, _From, State) ->
    {reply, menelaus_web_ldap:invalidate_ldap_cache(), State};
handle_call(Request, {Pid, _} = _From, State) ->
    ?log_warning("Got unknown call ~p from ~p (node ~p)", [Request, Pid, node(Pid)]),
    {reply, unhandled, State}.

handle_cast(Msg, State) ->
    ?log_warning("Got unknown cast ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?log_warning("Got unknown message ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
get_timeout(Request) when is_atom(Request) ->
    ?get_timeout(Request, ?DEFAULT_TIMEOUT);
get_timeout({Request, _}) when is_atom(Request) ->
    get_timeout(Request).

do_call(Node, Request) ->
    gen_server:call({?MODULE, Node}, Request, get_timeout(Request)).
