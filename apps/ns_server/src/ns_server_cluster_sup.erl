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
          [{atomic_persistent_term, {cb_atomic_persistent_term, start_link, []},
            permanent, 5000, worker, [cb_atomic_persistent_term]},
           {local_tasks, {local_tasks, start_link, []},
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
           {ns_gc_runner, {ns_gc_runner, start_link, []},
            permanent, 1000, worker, [ns_gc_runner]}] ++
              case cgroups:supported() of
                  true ->
                      [{ns_cgroups_manager,
                        {ns_cgroups_manager, start_link, []},
                        permanent, 1000, worker, []}];
                  _ ->
                      []
              end
         }}.

%% @doc Start ns_server and couchdb
start_ns_server() ->
    %% We don't want to start ns_server automatically because ns_cluster can
    %% have unfinished cluster join or leave. For this reason, the ns_server
    %% spec is not added to list of specs above. Ns_cluster adds it to the
    %% supervisor when it's ready.
    %% At the same time, during normal startup, remote_api should be started
    %% after ns_server. For this reason, its spec is added here as well.
    %% Note that for historical reasons, remote_api doesn't get stopped
    %% when stop_ns_server is called, so normally after stop & start,
    %% remote_api will already be running, so we should ignore {error, running}
    %% for that process.
    NsServer = {ns_server_nodes_sup, {ns_server_nodes_sup, start_link, []},
                permanent, infinity, supervisor, [ns_server_nodes_sup]},
    RemoteAPI = {remote_api, {remote_api, start_link, []},
                 permanent, 1000, worker, [remote_api]},

    NsServerStartRes = start_child(restartable:spec(NsServer)),

    IsNsServerRunning = case NsServerStartRes of
                            ok -> true;
                            {error, running} -> true;
                            {error, _} -> false
                        end,

    case IsNsServerRunning of
        true ->
            case start_child(RemoteAPI) of
                ok -> NsServerStartRes;
                {error, running} -> NsServerStartRes;
                {error, Error} -> {error, Error}
            end;
        false ->
            NsServerStartRes
    end.

start_child(Spec) ->
    case supervisor:start_child(?MODULE, Spec) of
        {ok, _Child} ->
            ok;
        {ok, _Child, _} ->
            ok;
        {error, {already_started, _Child}} ->
            %% to match the behavior of restart_child below
            {error, running};
        {error, already_present} ->
            Name = element(1, Spec),
            case supervisor:restart_child(?MODULE, Name) of
                {ok, _Child} ->
                    ok;
                {ok, _Child, _} ->
                    ok;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.

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

    case supervisor:terminate_child(?MODULE, ns_server_nodes_sup) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

restart_ns_server() ->
    restartable:restart(?MODULE, ns_server_nodes_sup).
