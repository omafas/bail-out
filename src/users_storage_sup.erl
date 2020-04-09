%% @author Couchbase <info@couchbase.com>
%% @copyright 2017 Couchbase, Inc.
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
