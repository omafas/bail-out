%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(cb_auth_info).

-behavior(gen_server).

-export([start_link/0, get/0]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_call/3,
         handle_info/2, terminate/2, code_change/3]).

-include("couch_db.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get() -> {auth, binary(), binary()} | {error, server_not_ready}.
get() ->
    gen_server:call(?MODULE, get, infinity).

init([]) ->
    {ok, nil}.

handle_call(get, _From, State) ->
    Reply = try
        Config = ns_config:get(),
        AU = ns_config:search_node_prop(Config, memcached, admin_user),
        AP = ns_config:search_node_prop(Config, memcached, admin_pass),
        {auth, ?l2b(AU), ?l2b(AP)}
    catch _:_Error ->
        {error, server_not_ready}
    end,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _) ->
    {ok, State}.
