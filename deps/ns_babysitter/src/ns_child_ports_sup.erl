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
set_dynamic_children(NCAOs) ->
    RequestedIds = [sanitize(NCAO) || NCAO <- NCAOs],
    CurrentIds = [erlang:element(1, C) || C <- supervisor:which_children(?MODULE)],
    IdsToTerminate = CurrentIds -- RequestedIds,

    RequestedIdsParams = lists:zip(RequestedIds, NCAOs),
    IdsParamsToLaunch = lists:filter(fun ({Id, _NCAO}) ->
                                             not lists:member(Id, CurrentIds)
                                     end, RequestedIdsParams),

    PidBefore = erlang:whereis(?MODULE),

    lists:foreach(fun(Id) ->
                          terminate_port(Id)
                  end,
                  IdsToTerminate),
    lists:foreach(fun({Id, NCAO}) ->
                          launch_port(Id, NCAO)
                  end,
                  IdsParamsToLaunch),

    PidAfter = erlang:whereis(?MODULE),
    PidBefore = PidAfter.

sanitize_value(Value) ->
    crypto:hash(sha256, term_to_binary(Value)).

sanitize(Struct) ->
    misc:rewrite_tuples(
      fun ({"CBAUTH_REVRPC_URL", V}) ->
              {stop, {"CBAUTH_REVRPC_URL", sanitize_value(V)}};
          (_Other) ->
              continue
      end, Struct).

launch_port(Id, NCAO) ->
    ?log_info("supervising port: ~p", [Id]),
    {ok, _C} = supervisor:start_child(?MODULE,
                                      create_child_spec(Id, NCAO)).

create_ns_server_supervisor_spec() ->
    {ErlCmd, NSServerArgs, NSServerOpts} = child_erlang:open_port_args(),

    Options = case misc:get_env_default(ns_server, dont_suppress_stderr_logger, false) of
                  true ->
                      [ns_server_no_stderr_to_stdout | NSServerOpts];
                  _ ->
                      NSServerOpts
              end,

    NCAO = {ns_server, ErlCmd, NSServerArgs, Options},
    create_child_spec(NCAO, NCAO).

create_child_spec(Id, {Name, Cmd, Args, Opts}) ->
    %% wrap parameters into function here to protect passwords
    %% that could be inside those parameters from being logged
    restartable:spec(
      {Id,
       {supervisor_cushion, start_link,
        [Name, 5000, infinity, ns_port_server, start_link,
         [fun() -> {Name, Cmd, Args, Opts} end]]},
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
