%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_server_cluster_sup).

-behavior(supervisor).

-include("ns_common.hrl").

%% API
-export([start_link/0,
         start_ns_server/0, stop_ns_server/0, restart_ns_server/0]).

%% Supervisor callbacks
-export([init/1]).

%%
%% API
%%

%% @doc Start the supervisor
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%
%% Supervisor callbacks
%%

init([]) ->
    {ok, {{one_for_one, 10, 1},
          [{local_tasks, {local_tasks, start_link, []},
            permanent, brutal_kill, worker, [local_tasks]},
           {log_os_info, {log_os_info, start_link, []},
            transient, 1000, worker, [log_os_info]},
           {timeout_diag_logger, {timeout_diag_logger, start_link, []},
            permanent, 1000, worker, [timeout_diag_logger, diag_handler]},
           {ns_cookie_manager,
            {ns_cookie_manager, start_link, []},
            permanent, 1000, worker, []},
           {chronicle_local, {chronicle_local, start_link, []},
            permanent, 5000, worker, [chronicle_local]},
           {ns_cluster, {ns_cluster, start_link, []},
            permanent, 5000, worker, [ns_cluster]},
           {sigar, {sigar, start_link, []},
            permanent, 5000, worker, []},
           {ns_config_sup, {ns_config_sup, start_link, []},
            permanent, infinity, supervisor,
            [ns_config_sup]},
           {netconfig_updater, {netconfig_updater, start_link, []},
            permanent, 5000, worker, [netconfig_updater]},
           {json_rpc_connection_sup,
            {json_rpc_connection_sup, start_link, []},
            permanent, infinity, supervisor,
            [json_rpc_connection_sup]},
           restartable:spec(
             {ns_server_nodes_sup, {ns_server_nodes_sup, start_link, []},
              permanent, infinity, supervisor, [ns_server_nodes_sup]}),
           {remote_api, {remote_api, start_link, []},
            permanent, 1000, worker, [remote_api]},
           {ns_gc_runner, {ns_gc_runner, start_link, []},
            permanent, 1000, worker, [ns_gc_runner]}
          ]}}.

%% @doc Start ns_server and couchdb
start_ns_server() ->
    supervisor:restart_child(?MODULE, ns_server_nodes_sup).

%% @doc Stop ns_server and couchdb
stop_ns_server() ->
    try
        %% ports need to be shut down before stopping ns_server to avoid errors
        %% in go components when menelaus disappears
        ns_ports_setup:shutdown_ports()
    catch
        T:E ->
            %% it's ok if we fail to stop the ports; the only bad thing that
            %% will happen are errors in go components logs; at the same time,
            %% we want stop_ns_server to work if ns_server already stopped;
            %% this gives us this
            ?log_warning("Failed to shutdown ports before "
                         "ns_server shutdown: ~p. "
                         "This is usually normal.", [{T,E}])
    end,

    supervisor:terminate_child(?MODULE, ns_server_nodes_sup).

restart_ns_server() ->
    restartable:restart(?MODULE, ns_server_nodes_sup).
