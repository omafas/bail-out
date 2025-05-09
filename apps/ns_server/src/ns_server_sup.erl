%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_server_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

%% API

-export([node_name_changed/0,
         start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(MAX_R, misc:get_env_default(max_r, 3)).
-define(MAX_T, misc:get_env_default(max_t, 10)).

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc Notify the supervisor that the node's name has changed so it
%% can restart children that care.
node_name_changed() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) ->
            {ok, _} = restartable:restart(Pid, ns_doctor_sup),
            {ok, _} = restartable:restart(Pid, leader_services_sup);
        undefined ->
            %% ns_server_sup has not started yet, no need to restart anything
            ok
    end.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    pre_start(),
    {ok, {{one_for_one, ?MAX_R, ?MAX_T},
          child_specs()}}.

pre_start() ->
    misc:ping_jointo().

child_specs() ->
    [suppress_max_restart_intensity:spec(
       {ns_disksup, {ns_disksup, start_link, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, 1000, worker, []}),

     {diag_handler_worker, {work_queue, start_link, [diag_handler_worker]},
      permanent, 1000, worker, []},

     {dir_size, {dir_size, start_link, []},
      permanent, 1000, worker, [dir_size]},

     {request_tracker, {request_tracker, start_link, []},
      permanent, 1000, worker, [request_tracker]},

     {chronicle_kv_log, {chronicle_kv_log, start_link, []},
      permanent, 1000, worker, [chronicle_kv_log]},

     %% ns_log starts after ns_config because it needs the config to
     %% find where to persist the logs
     {ns_log, {ns_log, start_link, []},
      permanent, 1000, worker, [ns_log]},

     {event_log_events, {gen_event, start_link, [{local, event_log_events}]},
      permanent, 1000, worker, dynamic},

     {event_log_server, {event_log_server, start_link, []},
      permanent, 5000, worker, [event_log_server]},

     {initargs_updater, {initargs_updater, start_link, []},
      permanent, 1000, worker, [initargs_updater]},

     {timer_lag_recorder, {timer_lag_recorder, start_link, []},
      permanent, 1000, worker, []},

     suppress_max_restart_intensity:spec(
       {ns_babysitter_log_consumer,
        {ns_log, start_link_babysitter_log_consumer, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, 1000, worker, []}),

     {prometheus_cfg, {prometheus_cfg, start_link, []},
      permanent, 1000, worker, [prometheus_cfg]},

     {memcached_passwords, {memcached_passwords, start_link, []},
      permanent, 1000, worker, []},

     {memcached_permissions, {memcached_permissions, start_link, []},
      permanent, 1000, worker, []},

     {ns_email_alert, {ns_email_alert, start_link, []},
      permanent, 1000, worker, [ns_email_alert]},

     {ns_node_disco_sup, {ns_node_disco_sup, start_link, []},
      permanent, infinity, supervisor,
      [ns_node_disco_sup]},

     {tombstone_agent, {tombstone_agent, start_link, []},
      permanent, 1000, worker, []},

     {vbucket_map_mirror, {vbucket_map_mirror, start_link, []},
      permanent, brutal_kill, worker, []},

     {capi_url_cache, {capi_url_cache, start_link, []},
      permanent, brutal_kill, worker, []},

     {bucket_info_cache, {bucket_info_cache, start_link, []},
      permanent, brutal_kill, worker, []},

     {ns_tick_event, {gen_event, start_link, [{local, ns_tick_event}]},
      permanent, 1000, worker, dynamic},

     {buckets_events, {gen_event, start_link, [{local, buckets_events}]},
      permanent, 1000, worker, dynamic},

     {ns_stats_event, {gen_event, start_link, [{local, ns_stats_event}]},
      permanent, 1000, worker, dynamic},

     {samples_loader_tasks, {samples_loader_tasks, start_link, []},
      permanent, 1000, worker, []},

     {ns_heart_sup, {ns_heart_sup, start_link, []},
      permanent, infinity, supervisor, [ns_heart_sup]},

     restartable:spec(
       {ns_doctor_sup, {ns_doctor_sup, start_link, []},
        permanent, infinity, supervisor, [ns_doctor_sup]}),

     {master_activity_events, {gen_event, start_link, [{local, master_activity_events}]},
      permanent, brutal_kill, worker, dynamic},

     {xdcr_ckpt_store,
      {simple_store, start_link, [?XDCR_CHECKPOINT_STORE, true]},
      permanent, 1000, worker, []},

     {metakv_worker,
      {work_queue, start_link, [metakv_worker]},
      permanent, 1000, worker, []},

     {index_events,
      {gen_event, start_link, [{local, index_events}]},
      permanent, brutal_kill, worker, dynamic},

     {index_settings_manager, {index_settings_manager, start_link, []},
      permanent, 1000, worker, [index_settings_manager]},

     {query_settings_manager, {query_settings_manager, start_link, []},
      permanent, 1000, worker, [query_settings_manager]},

     {eventing_settings_manager, {eventing_settings_manager, start_link, []},
      permanent, 1000, worker, [work_queue]},

     {analytics_settings_manager, {analytics_settings_manager, start_link, []},
      permanent, 1000, worker, [analytics_settings_manager]},

     {audit_events,
      {gen_event, start_link, [{local, audit_events}]},
      permanent, brutal_kill, worker, dynamic}] ++

    [suppress_max_restart_intensity:spec(
       {encryption_service,
        {encryption_service,
         start_link, []},
        {permanent, 1, ?MAX_R, ?MAX_T}, 5000, worker, [encryption_service]})
     || cluster_compat_mode:is_enterprise()] ++

    [{ns_memcached_sockets_pool, {ns_memcached_sockets_pool, start_link, []},
      permanent, 1000, worker, []},
     %% Started before menelaus_sup, so that children such as app_telemetry_pool
     %% can notify metrics immediately
     {ns_server_stats, {ns_server_stats, start_link, []},
      permanent, 1000, worker, [ns_server_stats]}] ++

    [{cb_cluster_secrets, {cb_cluster_secrets, start_link_node_monitor, []},
      permanent, 1000, worker, []} || cluster_compat_mode:is_enterprise()] ++

    [{menelaus, {menelaus_sup, start_link, []},
      permanent, infinity, supervisor,
      [menelaus_sup]},

     %% Note: many of the processes started by ns_ports_setup try to connect
     %% to ns_server rest port for various reasons. So ns_ports_setup needs to
     %% go after menelaus_sup.
     suppress_max_restart_intensity:spec(
       {ns_ports_setup, {ns_ports_setup, start, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, brutal_kill, worker, []}),

     {service_agent_sup, {service_agent_sup, start_link, []},
      permanent, infinity, supervisor, [service_agent_sup]},

     {memcached_auth_server, {memcached_auth_server, start_link, []},
      permanent, 1000, worker, []},

     suppress_max_restart_intensity:spec(
       {ns_audit_cfg, {ns_audit_cfg, start_link, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, 1000, worker, []}),

     suppress_max_restart_intensity:spec(
       {ns_audit, {ns_audit, start_link, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, 1000, worker, []}),

     suppress_max_restart_intensity:spec(
       {memcached_config_mgr, {memcached_config_mgr, start_link, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, 1000, worker, []}),

     {testconditions_store, {simple_store, start_link, [testconditions, false]},
      permanent, 1000, worker, []},

     {terse_cluster_info_uploader,
      {terse_cluster_info_uploader, start_link, []},
      permanent, 1000, worker, []},

     {ns_bucket_worker_sup, {ns_bucket_worker_sup, start_link, []},
      permanent, infinity, supervisor, [ns_bucket_worker_sup]},

     {{stats_reader, "@system"}, {stats_reader, start_link, ["@system"]},
      permanent, 1000, worker, [start_reader]},

     {{stats_reader, "@system-processes"}, {stats_reader, start_link, ["@system-processes"]},
      permanent, 1000, worker, [start_reader]},

     {{stats_reader, "@query"}, {stats_reader, start_link, ["@query"]},
      permanent, 1000, worker, [stats_reader]},

     {{stats_reader, "@global"}, {stats_reader, start_link, ["@global"]},
      permanent, 1000, worker, [stats_reader]}] ++

     [{goxdcr_status_keeper, {goxdcr_status_keeper, start_link, []},
       permanent, 1000, worker, [goxdcr_status_keeper]}
      || cluster_compat_mode:is_goxdcr_enabled()] ++

     [{services_stats_sup, {services_stats_sup, start_link, []},
      permanent, infinity, supervisor, []},

     suppress_max_restart_intensity:spec(
       {compaction_daemon, {compaction_daemon, start_link, []},
        {permanent, 4, ?MAX_R, ?MAX_T}, 86400000, worker, [compaction_daemon]}),

     {cluster_logs_sup, {cluster_logs_sup, start_link, []},
      permanent, infinity, supervisor, []},

     {collections, {collections, start_link, []},
      permanent, 1000, worker, [collections]},

     %% Note to the users of leader_events. The events are announced
     %% synchronously, make sure not to block mb_master for too long.
     {leader_events, {gen_event, start_link, [{local, leader_events}]},
      permanent, 1000, worker, dynamic},

     %% Starts mb_master_sup, which has all processes that start on the master
     %% node.
     restartable:spec(
       {leader_services_sup, {leader_services_sup, start_link, []},
        permanent, infinity, supervisor, []}),

     %% Needs mb_master and leader_events.
     {ns_tick_agent, {ns_tick_agent, start_link, []},
      permanent, 1000, worker, []},

     {master_activity_events_ingress, {gen_event, start_link, [{local, master_activity_events_ingress}]},
      permanent, brutal_kill, worker, dynamic},

     {master_activity_events_timestamper, {master_activity_events, start_link_timestamper, []},
      permanent, brutal_kill, worker, dynamic},

     {master_activity_events_pids_watcher, {master_activity_events_pids_watcher, start_link, []},
      permanent, brutal_kill, worker, dynamic},

     {master_activity_events_keeper, {master_activity_events_keeper, start_link, []},
      permanent, brutal_kill, worker, dynamic},

     {health_monitor_sup, {health_monitor_sup, start_link, []},
      permanent, infinity, supervisor, [health_monitor_sup]},

     {rebalance_agent, {rebalance_agent, start_link, []},
      permanent, 5000, worker, []},

     {kv_hibernation_agent, {kv_hibernation_agent, start_link, []},
      permanent, 5000, worker, []},

     {ns_rebalance_report_manager, {ns_rebalance_report_manager, start_link, []},
      permanent, 1000, worker, []},

     {creds_rotation, {cb_creds_rotation, start_link, []},
      permanent, 1000, worker, [creds_rotation]}].
