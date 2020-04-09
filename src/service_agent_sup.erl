%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2017 Couchbase, Inc.
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
    RV = {ok, Pid} =
        work_queue:start_link(
          service_agent_worker,
          fun () ->
                  ns_pubsub:subscribe_link(ns_config_events, fun handle_cfg_event/2, self())
          end),
    work_queue:submit_work(Pid, fun refresh_children/0),
    RV.

is_notable_event({{node, Node, membership}, _}) when Node =:= node() ->
    true;
is_notable_event({{node, Node, services}, _}) when Node =:= node() ->
    true;
is_notable_event({rest_creds, _}) ->
    true;
is_notable_event({cluster_compat_version, _}) ->
    true;
is_notable_event(_) ->
    false.

handle_cfg_event(Event, Worker) ->
    case is_notable_event(Event) of
        false ->
            Worker;
        true ->
            work_queue:submit_work(Worker, fun refresh_children/0),
            Worker
    end.

wanted_children(Config) ->
    Services = [S || S <- ns_cluster_membership:topology_aware_services(),
                     ns_cluster_membership:should_run_service(Config, S, node())],
    [{service_agent, S} || S <- Services].

running_children() ->
    [Id || {Id, _, _, _} <- supervisor:which_children(service_agent_children_sup)].

refresh_children() ->
    Config = ns_config:get(),

    Running = ordsets:from_list(running_children()),
    Wanted = ordsets:from_list(wanted_children(Config)),

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
