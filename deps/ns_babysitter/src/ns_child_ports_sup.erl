%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_child_ports_sup).

-behavior(supervisor).

-export([start_link/0, set_dynamic_children/1,
         send_command/2,
         create_ns_server_supervisor_spec/0]).

-export([init/1,
         restart_port/1,
         current_ports/0, find_port/1]).

-include("ns_common.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 100, 10}, []}}.

send_command(PortName, Command) ->
    try
        do_send_command(PortName, Command)
    catch T:E:S ->
            ?log_error("Failed to send command ~p to port ~p due to ~p:~p. Ignoring...~n~p",
                       [Command, PortName, T, E, S]),
            {T, E}
    end.

find_port(PortName) ->
    Childs = supervisor:which_children(?MODULE),
    [Pid] = [Pid || {Id, Pid, _, _} <- Childs,
                    Pid =/= undefined,
                    element(1, Id) =:= PortName],
    Pid.

do_send_command(PortName, Command) ->
    Pid = find_port(PortName),
    Pid ! {send_to_port, Command},
    {ok, Pid}.

-spec set_dynamic_children([any()]) -> pid().
set_dynamic_children(RequestedIds) ->
    CurrentIds = [erlang:element(1, C) || C <- supervisor:which_children(?MODULE)],
    IdsToTerminate = CurrentIds -- RequestedIds,
    IdsToLaunch = RequestedIds -- CurrentIds,

    PidBefore = erlang:whereis(?MODULE),

    lists:foreach(fun terminate_port/1, IdsToTerminate),
    lists:foreach(fun launch_port/1, IdsToLaunch),

    PidAfter = erlang:whereis(?MODULE),
    PidBefore = PidAfter.

launch_port(NCAO) ->
    ?log_info("supervising port: ~p", [NCAO]),
    {ok, _C} = supervisor:start_child(?MODULE,
                                      create_child_spec(NCAO)).

create_ns_server_supervisor_spec() ->
    {ErlCmd, NSServerArgs, NSServerOpts} = child_erlang:open_port_args(),

    Options = case misc:get_env_default(ns_server, dont_suppress_stderr_logger, false) of
                  true ->
                      [ns_server_no_stderr_to_stdout | NSServerOpts];
                  _ ->
                      NSServerOpts
              end,

    NCAO = {ns_server, ErlCmd, NSServerArgs, Options},
    create_child_spec(NCAO).

create_child_spec({Name, _Cmd, _Args, _Opts} = Id) ->
    %% wrap parameters into function here to protect passwords
    %% that could be inside those parameters from being logged
    restartable:spec(
      {Id,
       {supervisor_cushion, start_link,
        [Name, 5000, infinity, ns_port_server, start_link,
         [fun() -> Id end]]},
       permanent, 86400000, worker,
       [ns_port_server]}).

terminate_port(Id) ->
    ?log_info("unsupervising port: ~p", [Id]),
    ok = supervisor:terminate_child(?MODULE, Id),
    ok = supervisor:delete_child(?MODULE, Id).

restart_port(Id) ->
    ?log_info("restarting port: ~p", [Id]),
    {ok, _} = restartable:restart(?MODULE, Id).

current_ports() ->
    % Children will look like...
    %   [{memcached,<0.77.0>,worker,[ns_port_server]},
    %    {ns_port_init,undefined,worker,[]}]
    %
    % Or possibly, if a child died, like...
    %   [{memcached,undefined,worker,[ns_port_server]},
    %    {ns_port_init,undefined,worker,[]}]
    %
    Children = supervisor:which_children(?MODULE),
    [NCAO || {NCAO, Pid, _, _} <- Children,
             Pid /= undefined].
