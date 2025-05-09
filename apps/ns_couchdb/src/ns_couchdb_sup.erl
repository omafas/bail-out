%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc main supervisor for ns_couchdb node
%%

-module(ns_couchdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, restart_capi_ssl_service/0]).

%% Supervisor callbacks
-export([init/1]).

-define(MAX_R, misc:get_env_default(max_r, 7)).
-define(MAX_T, misc:get_env_default(max_t, 15)).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one, ?MAX_R, ?MAX_T}, child_specs()} }.

child_specs() ->
    [
     suppress_max_restart_intensity:spec(
       {menelaus_users_auth_cache, {menelaus_users, start_auth_cache, []},
        {permanent, 5, ?MAX_R, ?MAX_T}, 1000, worker, [versioned_cache]}),

     suppress_max_restart_intensity:spec(
       {menelaus_users_lock_cache, {menelaus_users, start_lock_cache, []},
        {permanent, 5, ?MAX_R, ?MAX_T}, 1000, worker, [versioned_cache]}),

     {cb_couch_sup, {cb_couch_sup, start_link, []},
      permanent, 5000, supervisor, [cb_couch_sup]},

     %% this must be placed after cb_couch_sup since couchdb starts
     %% sasl application
     {cb_init_loggers, {cb_init_loggers, start_link, []},
      transient, 1000, worker, [cb_init_loggers]},

     {timeout_diag_logger, {timeout_diag_logger, start_link, []},
      permanent, 1000, worker, [timeout_diag_logger]},

     {ns_memcached_sockets_pool, {ns_memcached_sockets_pool, start_link, []},
      permanent, 1000, worker, []},

     {ns_couchdb_stats_collector, {ns_couchdb_stats_collector, start_link, []},
      permanent, 1000, worker, [ns_couchdb_stats_collector]},

     {ns_couchdb_config_sup, {ns_couchdb_config_sup, start_link, []},
      permanent, infinity, supervisor,
      [ns_couchdb_config_sup]},

     suppress_max_restart_intensity:spec(
       {compiled_roles_cache, {menelaus_roles,
                               start_compiled_roles_cache, []},
        {permanent, 5, ?MAX_R, ?MAX_T}, 1000, worker, [versioned_cache]}),

     suppress_max_restart_intensity:spec(
       {roles_cache, {roles_cache, start_link, []},
        {permanent, 5, ?MAX_R, ?MAX_T}, 1000, worker, []}),

     {request_tracker, {request_tracker, start_link, []},
      permanent, 1000, worker, [request_tracker]},

     {vbucket_map_mirror, {vbucket_map_mirror, start_link, []},
      permanent, brutal_kill, worker, []},

     {capi_url_cache, {capi_url_cache, start_link, []},
      permanent, brutal_kill, worker, []},

     {set_view_update_daemon, {set_view_update_daemon, start_link, []},
      permanent, 1000, worker, [set_view_update_daemon]},

     suppress_max_restart_intensity:spec(
       restartable:spec(
         {ns_capi_ssl_service,
          {ns_ssl_services_setup, start_link_capi_service, []},
          {permanent, 4, ?MAX_R, ?MAX_T}, 1000, worker, []})),

     {dir_size, {dir_size, start_link, []},
      permanent, 1000, worker, [dir_size]}
    ].

restart_capi_ssl_service() ->
    SuppressMaxRChildPid =
        suppress_max_restart_intensity:actual_child_pid(?MODULE,
                                                        ns_capi_ssl_service),

    case SuppressMaxRChildPid of
        undefined -> {error, not_running};
        Pid when is_pid(Pid) ->
            case restartable:restart(Pid) of
                {ok, _} ->
                    ok;
                Error ->
                    Error
            end
    end.
