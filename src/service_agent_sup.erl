%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(service_agent_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, parent).

init(parent) ->
    Children =
        [{service_agent_children_sup,
          {supervisor, start_link,
           [{local, service_agent_children_sup}, ?MODULE, child]},
          permanent, infinity, supervisor, []},
         {service_agent_worker, {erlang, apply, [fun start_link_worker/0, []]},
          permanent, 1000, worker, []}],
    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          Children}};
init(child) ->
    {ok, {{one_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          []}}.

start_link_worker() ->
    chronicle_compat_events:start_refresh_worker(fun is_notable_event/1,
                                                 fun refresh_children/0).

is_notable_event({node, Node, membership}) when Node =:= node() ->
    true;
is_notable_event({node, Node, services}) when Node =:= node() ->
    true;
is_notable_event(rest_creds) ->
    true;
is_notable_event(cluster_compat_version) ->
    true;
is_notable_event(_) ->
    false.

wanted_children() ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    Services =
        [S || S <- ns_cluster_membership:topology_aware_services(),
              ns_cluster_membership:should_run_service(Snapshot, S, node())],
    [{service_agent, S} || S <- Services].

running_children() ->
    [Id || {Id, _, _, _} <- supervisor:which_children(service_agent_children_sup)].

refresh_children() ->
    Running = ordsets:from_list(running_children()),
    Wanted = ordsets:from_list(wanted_children()),

    ToStart = ordsets:subtract(Wanted, Running),
    ToStop = ordsets:subtract(Running, Wanted),

    lists:foreach(fun stop_child/1, ToStop),
    lists:foreach(fun start_child/1, ToStart),
    ok.

child_spec({service_agent, Service} = Id) ->
    {Id,
     {service_agent, start_link, [Service]},
     permanent, 1000, worker, [service_agent]}.

start_child(Id) ->
    {ok, _Pid} = supervisor:start_child(service_agent_children_sup, child_spec(Id)).

stop_child(Id) ->
    ok = supervisor:terminate_child(service_agent_children_sup, Id),
    ok = supervisor:delete_child(service_agent_children_sup, Id).
