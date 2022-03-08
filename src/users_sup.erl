%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc supervisor for all things related to user storage

-module(users_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include("ns_common.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 3, 10}, child_specs()}}.

child_specs() ->
    [{user_storage_events,
      {gen_event, start_link, [{local, user_storage_events}]},
      permanent, 1000, worker, []},

     {users_storage_sup,
      {users_storage_sup, start_link, []},
      permanent, infinity, supervisor, []},

     {compiled_roles_cache, {menelaus_roles, start_compiled_roles_cache, []},
      permanent, 1000, worker, [versioned_cache]},

     {roles_cache, {roles_cache, start_link, []},
      permanent, 1000, worker, []}].
