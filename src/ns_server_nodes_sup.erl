%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc supervisor that ensures that the whole ns_server_sup will be restarted
%%      if couchdb_node port crashes
%%

-module(ns_server_nodes_sup).

-include("ns_common.hrl").

-behaviour(supervisor2).

%% API
-export([start_link/0, start_couchdb_node/0, is_couchdb_node_started/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {rest_for_one,
            misc:get_env_default(max_r, 3),
            misc:get_env_default(max_t, 10)}, child_specs()} }.

child_specs() ->
    [
     {chronicle_compat_events, {chronicle_compat_events, start_link, []},
      permanent, 5000, worker, [chronicle_compat_events]},

     {remote_monitors, {remote_monitors, start_link, []},
      permanent, 1000, worker, []},

     %% needs to be started before ns_ssl_services_sup because ssl endpoint is
     %% started there
     menelaus_sup:barrier_spec(menelaus_barrier),

     %% connection pool that we use when talking to goxdcr/indexer/cbq-engine
     {rest_lhttpc_pool_sup, {rest_lhttpc_pool_sup, start_link, []},
      permanent, infinity, supervisor, []},

     {memcached_refresh, {memcached_refresh, start_link, []},
      permanent, 1000, worker, []},

     {ns_secrets, {ns_secrets, start_link, []}, permanent, 1000, worker, []},

     {ns_ssl_services_sup,
      {ns_ssl_services_sup, start_link, []},
      permanent, infinity, supervisor, []},

     {ldap_auth_cache, {ldap_auth_cache, start_link, []},
      permanent, 1000, worker, []},

     {users_sup,
      {users_sup, start_link, []},
      permanent, infinity, supervisor, []},

     %% we cannot "kill" this guy anyways. Thus hefty shutdown timeout.
     {start_couchdb_node, {?MODULE, start_couchdb_node, []},
      {permanent, 5}, 86400000, worker, []},

     {wait_for_couchdb_node, {erlang, apply, [fun wait_link_to_couchdb_node/0, []]},
      permanent, 1000, worker, []},

     {setup_dirs,
      {ns_storage_conf, setup_storage_paths, []},
      transient, brutal_kill, worker, []},

     {ns_server_sup, {ns_server_sup, start_link, []},
      permanent, infinity, supervisor, [ns_server_sup]},

     menelaus_sup:barrier_notify_spec(menelaus_barrier_notify)].

create_ns_couchdb_spec() ->
    CouchIni = case init:get_argument(couch_ini) of
                   error ->
                       [];
                   {ok, [[]]} ->
                       [];
                   {ok, [Values]} ->
                       ["-couch_ini" | Values]
               end,

    SSLDistOpts = case init:get_argument(ssl_dist_optfile) of
                      {ok, [Path]} when Path =/= [] ->
                          ["-ssl_dist_optfile" | Path];
                      _ ->
                          []
                  end,

    KernelInetrc =
        case init:get_argument(kernel) of
            {ok, Args} -> lists:concat([["-kernel" | V] || V <- Args]);
            _ -> []
        end,

    {ok, CookieFile} = application:get_env(ns_babysitter, cookiefile),

    SchedulersArgs = case misc:read_cpu_count_env() of
                         {ok, N} ->
                             NStr = integer_to_list(N),
                             ["+S", NStr ++ ":" ++ NStr];
                         undefined -> []
                     end,

    ErlangArgs = CouchIni ++ KernelInetrc ++ SSLDistOpts ++ SchedulersArgs ++
        ["-name", atom_to_list(ns_node_disco:couchdb_node()),
         "-smp", "enable",
         "+P", "327680",
         "+K", "true",
         "-kernel", "logger", "[{handler, default, undefined}]",
         "-nouser",
         "-hidden",
         "-proto_dist", "cb",
         "-epmd_module", atom_to_list(net_kernel:epmd_module()),
         "-start_epmd", "false",
         "-setcookie", "nocookie",
         "-ns_couchdb", "cookiefile", misc:inspect_term(CookieFile),
         "-run", "child_erlang", "child_start", "ns_couchdb"],

    ns_ports_setup:create_erl_node_spec(
      ns_couchdb, [{ns_server_node, node()}], "NS_COUCHDB_ENV_ARGS", ErlangArgs).

start_couchdb_node() ->
    ns_port_server:start_link_named(
      ns_couchdb_port,
      fun () ->
              ShortName = misc:node_name_short(ns_node_disco:couchdb_node()),
              ok = misc:wait_for_nodename(ShortName),
              create_ns_couchdb_spec()
      end).

wait_link_to_couchdb_node() ->
    proc_lib:start_link(erlang, apply, [fun start_wait_link_to_couchdb_node/0, []]).

start_wait_link_to_couchdb_node() ->
    erlang:register(wait_link_to_couchdb_node, self()),
    do_wait_link_to_couchdb_node(true).

do_wait_link_to_couchdb_node(Initial) ->
    ?log_debug("Waiting for ns_couchdb node to start"),
    Self = self(),
    Ref = make_ref(),
    MRef = erlang:monitor(process, erlang:whereis(ns_couchdb_port)),

    proc_lib:spawn_link(
      fun () ->
              RV = misc:poll_for_condition(
                     fun () ->
                             case rpc:call(ns_node_disco:couchdb_node(),
                                           erlang, apply,
                                           [fun is_couchdb_node_ready/0, []], 5000) of
                                 {ok, _} = OK -> OK;
                                 Other ->
                                     ?log_debug("ns_couchdb is not ready: ~p", [Other]),
                                     false
                             end
                     end,
                     60000, 200),
              Self ! {Ref, RV}
      end),

    receive
        {Ref, RV} ->
            case RV of
                {ok, Pid} ->
                    case Initial of
                        true ->
                            proc_lib:init_ack({ok, self()});
                        _ -> ok
                    end,
                    remote_monitors:monitor(Pid),
                    wait_link_to_couchdb_node_loop(Pid);
                timeout ->
                    exit(timeout)
            end;
        {'DOWN', MRef, process, Pid, Reason} ->
            ?log_error("ns_couchdb_port(~p) died with reason ~p", [Pid, Reason]),
            exit(Reason)
    end.

wait_link_to_couchdb_node_loop(Pid) ->
    receive
        {remote_monitor_down, Pid, unpaused} ->
            ?log_debug("Link to couchdb node was unpaused."),
            do_wait_link_to_couchdb_node(false);
        {remote_monitor_down, Pid, Reason} ->
            ?log_debug("Link to couchdb node was lost. Reason: ~p", [Reason]),
            exit(normal);
        Msg ->
            ?log_debug("Exiting due to message: ~p", [Msg]),
            exit(normal)
    end.

%% NOTE: rpc-ed between ns_server and ns_couchdb nodes.
is_couchdb_node_ready() ->
    case erlang:whereis(ns_couchdb_sup) of
        P when is_pid(P) ->
            try supervisor:which_children(P) of
                _ ->
                    {ok, P}
            catch _:_ ->
                    false
            end;
        _ ->
            false
    end.

is_couchdb_node_started() ->
    is_pid(erlang:whereis(ns_couchdb_port)).
