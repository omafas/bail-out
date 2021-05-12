%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(health_monitor_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, parent).

init(parent) ->
    Children =
        [{ns_server_monitor, {ns_server_monitor, start_link, []},
          permanent, 1000, worker, [ns_server_monitor]},
         {service_monitor_children_sup,
          {supervisor, start_link,
           [{local, service_monitor_children_sup}, ?MODULE, child]},
          permanent, infinity, supervisor, []},
         {service_monitor_worker,
          {erlang, apply, [fun start_link_worker/0, []]},
          permanent, 1000, worker, []},
         {node_monitor, {node_monitor, start_link, []},
          permanent, 1000, worker, [node_monitor]},
         {node_status_analyzer, {node_status_analyzer, start_link, []},
          permanent, 1000, worker, [node_status_analyzer]}],
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
is_notable_event(_) ->
    false.

wanted_children() ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    [S || S <- health_monitor:supported_services(),
          ns_cluster_membership:should_run_service(Snapshot, S, node())].

running_children() ->
    [S || {{S, _}, _, _, _} <- supervisor:which_children(service_monitor_children_sup)].

refresh_children() ->
    Running = ordsets:from_list(running_children()),
    Wanted = ordsets:from_list(wanted_children()),

    ToStart = ordsets:subtract(Wanted, Running),
    ToStop = ordsets:subtract(Running, Wanted),

    lists:foreach(fun stop_child/1, ToStop),
    lists:foreach(fun start_child/1, ToStart),
    ok.

child_specs(kv) ->
    [{{kv, dcp_traffic_monitor}, {dcp_traffic_monitor, start_link, []},
      permanent, 1000, worker, [dcp_traffic_monitor]},
     {{kv, kv_stats_monitor}, {kv_stats_monitor, start_link, []},
      permanent, 1000, worker, [kv_stats_monitor]},
     {{kv, kv_monitor}, {kv_monitor, start_link, []},
      permanent, 1000, worker, [kv_monitor]}];
child_specs(Service) ->
    ?log_debug("Unsupported service ~p", [Service]),
    exit(unsupported_service).

start_child(Service) ->
    Children = child_specs(Service),
    lists:foreach(
      fun (Child) ->
              {ok, _Pid} = supervisor:start_child(service_monitor_children_sup, Child)
      end, Children).

stop_child(Service) ->
    Children = [Id || {Id, _, _, _, _, _} <- child_specs(Service)],
    lists:foreach(
      fun (Id) ->
              ok = supervisor:terminate_child(service_monitor_children_sup, Id),
              ok = supervisor:delete_child(service_monitor_children_sup, Id)
      end, Children).
