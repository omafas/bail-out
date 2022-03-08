%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc supervisor for users storage and corresponding replicator

-module(users_storage_sup).

-behaviour(supervisor).

-export([start_link/0, stop_replicator/0]).
-export([init/1]).

-include("ns_common.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 3, 10}, child_specs()}}.

stop_replicator() ->
    case supervisor:terminate_child(?MODULE, users_replicator) of
        ok ->
            ok = supervisor:delete_child(?MODULE, users_replicator);
        Error ->
            ?log_debug("Error terminating users_replicator ~p", [Error])
    end.

child_specs() ->
    [{users_replicator,
      {menelaus_users, start_replicator, []},
      permanent, 1000, worker, [doc_replicator]},

     {users_storage,
      {menelaus_users, start_storage, []},
      permanent, 1000, worker, [replicated_dets, replicated_storage]}].
