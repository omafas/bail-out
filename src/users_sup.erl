%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
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
