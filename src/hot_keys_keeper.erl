%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% This is a ghost gen_server to be backward compatible. In a mixed-mode
%% cluster, nodes running lower version can make calls to nodes running this
%% version of the code - and therefore this code will have to linger around.
%% Eventally when the lowest version we support is beyond 7.1.0 this can
%% be removed entirely.
%%
-module(hot_keys_keeper).

-behaviour(gen_server2).

%% API
-export([start_link/0]).

-export([handle_call/3]).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

handle_call({get_local_keys, _}, _From, State) ->
    {reply, [], State};
handle_call(all_local_hot_keys, _From, State) ->
    {reply, [], State}.
