%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Called on each server restart we update initargs file and also if port
%% information and services on the node change.
-module(initargs_updater).

-behaviour(gen_server2).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_info/2]).

-include("ns_common.hrl").
-include("cut.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
    PortKeys = service_ports:all_port_keys(),
    EventFilter = fun (rest) -> true;
                      (nodes_wanted) -> true;
                      ({node, Node, services}) when Node =:= node() -> true;
                      ({node, Node, Key}) when Node =:= node() ->
                          lists:member(Key, PortKeys);
                      (_) -> false
                  end,
    chronicle_compat_events:notify_if_key_changes(EventFilter, notify_change),
    save_initargs(),
    {ok, #{}}.

handle_info(notify_change, State) ->
    misc:flush(notify_change),
    save_initargs(),
    {noreply, State, hibernate};
handle_info(Info, State) ->
    ?log_warning("Unexpected message(~p, ~p)", [Info]),
    {noreply, State, hibernate}.

%% Internal functions.
save_initargs() ->
    NodeServices = ns_cluster_membership:node_services(node()),
    ServicePorts = [{K, service_ports:get_port(K)} ||
                    K <- service_ports:all_port_keys()],
    save_initargs([{services, NodeServices},
                   {nodes_wanted, ns_node_disco:nodes_wanted()}] ++
                  ServicePorts).

save_initargs(AdditionalServerData) ->
    DefaultData = build_initargs(),
    NewServerData = misc:update_proplist(
                      proplists:get_value(ns_server, DefaultData, []),
                      AdditionalServerData),
    Data = lists:keyreplace(ns_server, 1, DefaultData,
                            {ns_server, NewServerData}),
    do_save_initargs(erlang:term_to_binary(Data)).

do_save_initargs(Data) ->
    {ok, DataDir} = application:get_env(ns_server, path_config_datadir),
    ok = misc:atomic_write_file(filename:join(DataDir, "initargs"), Data).

build_initargs() ->
    InitArgs = init:get_arguments(),
    InitArgs1 = [{pid, os:getpid()},
                 {code_path, get_code_path()}
                 | InitArgs],
    InitArgs2 = case file:get_cwd() of
                    {ok, CWD} ->
                        [{cwd, CWD} | InitArgs1];
                    _ ->
                        InitArgs1
                end,

    AppEnvs = [{App, application:get_all_env(App)} ||
                  {App, _, _} <- application:loaded_applications()],

    misc:update_proplist(InitArgs2, AppEnvs).

get_code_path() ->
    [filename:absname(P) || P <- code:get_path()].
