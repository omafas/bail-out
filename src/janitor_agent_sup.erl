%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
