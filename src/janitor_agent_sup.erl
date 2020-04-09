%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
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
%% @doc suprevisor for janitor_agent and all the processes that need to be terminated
%%      if janitor agent gets killed
%%
-module(janitor_agent_sup).

-behaviour(supervisor).

-export([start_link/1, init/1, get_registry_pid/1]).

start_link(BucketName) ->
    Name = list_to_atom(atom_to_list(?MODULE) ++ "-" ++ BucketName),
    supervisor:start_link({local, Name}, ?MODULE, [BucketName]).

init([BucketName]) ->
    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs(BucketName)}}.

child_specs(BucketName) ->
    [{rebalance_subprocesses_registry,
      {ns_process_registry, start_link,
       [get_registry_name(BucketName), [{terminate_command, kill}]]},
      permanent, 86400000, worker, [ns_process_registry]},

     {janitor_agent, {janitor_agent, start_link, [BucketName]},
      permanent, brutal_kill, worker, []}].

get_registry_name(BucketName) ->
    list_to_atom(atom_to_list(rebalance_subprocesses_registry) ++ "-" ++ BucketName).

get_registry_pid(BucketName) ->
    ns_process_registry:lookup_pid(get_registry_name(BucketName), ns_process_registry).
