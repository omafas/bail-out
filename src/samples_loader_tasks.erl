%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(samples_loader_tasks).

-behaviour(gen_server).

-include("ns_common.hrl").

%% gen_server API
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_loading_sample/2, get_tasks/1]).

-export([perform_loading_task/2]).

start_loading_sample(Name, Quota) ->
    gen_server:call(?MODULE, {start_loading_sample, Name, Quota}, infinity).

get_tasks(Timeout) ->
    gen_server:call(?MODULE, get_tasks, Timeout).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-record(state, {
          tasks = [] :: [{string(), pid()}],
          token_pid :: undefined | pid()
         }).

init([]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({start_loading_sample, Name, Quota}, _From, #state{tasks = Tasks} = State) ->
    case lists:keyfind(Name, 1, Tasks) of
        false ->
            Pid = start_new_loading_task(Name, Quota),
            ns_heart:force_beat(),
            NewState = State#state{tasks = [{Name, Pid} | Tasks]},
            {reply, ok, maybe_pass_token(NewState)};
        _ ->
            {reply, already_started, State}
    end;
handle_call(get_tasks, _From, State) ->
    {reply, State#state.tasks, State}.


handle_cast(_, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason} = Msg, #state{tasks = Tasks,
                                                token_pid = TokenPid} = State) ->
    case lists:keyfind(Pid, 2, Tasks) of
        false ->
            ?log_error("Got exit not from child: ~p", [Msg]),
            exit(Reason);
        {Name, _} ->
            ?log_debug("Consumed exit signal from samples loading task ~s: ~p", [Name, Msg]),
            ns_heart:force_beat(),
            case Reason of
                normal ->
                    ale:info(?USER_LOGGER, "Completed loading sample bucket ~s", [Name]);
                {failed_to_load_samples, Status, Output} ->
                    ale:error(?USER_LOGGER,
                              "Loading sample bucket ~s failed. "
                              "Samples loader exited with status ~b.~n"
                              "Loader's output was:~n~n~s",
                              [Name, Status, Output]);
                _ ->
                    ale:error(?USER_LOGGER,
                              "Loading sample bucket ~s failed: ~p",
                              [Name, Reason])
            end,
            NewTokenPid = case Pid =:= TokenPid of
                              true ->
                                  ?log_debug("Token holder died"),
                                  undefined;
                              _ ->
                                  TokenPid
                          end,
            NewState = State#state{tasks = lists:keydelete(Pid, 2, Tasks),
                                   token_pid = NewTokenPid},
            {noreply, maybe_pass_token(NewState)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_pass_token(#state{token_pid = undefined,
                        tasks = [{Name, FirstPid}|_]} = State) ->
    FirstPid ! allowed_to_go,
    ?log_info("Passed samples loading token to task: ~s", [Name]),
    State#state{token_pid = FirstPid};
maybe_pass_token(State) ->
    State.

start_new_loading_task(Name, Quota) ->
    proc_lib:spawn_link(?MODULE, perform_loading_task, [Name, Quota]).

perform_loading_task(Name, Quota) ->
    receive
        allowed_to_go -> ok
    end,

    Host = misc:extract_node_address(node()),
    ClusterOpts = case misc:disable_non_ssl_ports() of
                      true ->
                          SslPort = service_ports:get_port(ssl_rest_port),
                          Cluster = "https://" ++ misc:join_host_port(
                                                    Host, SslPort),
                          ["--cluster", Cluster,
                           "--cacert",
                           ns_ssl_services_setup:memcached_cert_path()];
                      false ->
                          Port = service_ports:get_port(rest_port),
                          ["--cluster", misc:join_host_port(Host, Port)]
                  end,
    BinDir = path_config:component_path(bin),

    Cmd = BinDir ++ "/cbimport",
    Args = ["json",
            "--bucket", Name,
            "--format", "sample",
            "--bucket-quota", integer_to_list(Quota),
            "--threads", "2",
            "--verbose",
            "--dataset", "file://" ++ filename:join([BinDir, "..",
                                        "samples", Name ++ ".zip"])] ++
            ClusterOpts,

    Env = [{"CB_USERNAME", "@ns_server"},
           {"CB_PASSWORD", ns_config_auth:get_password(special)} |
           ns_ports_setup:build_cbauth_env_vars(ns_config:latest(), cbimport)],

    {Status, Output} = misc:run_external_tool(Cmd, Args, Env),
    case Status of
        0 ->
            ok;
        _ ->
            exit({failed_to_load_samples, Status, Output})
    end.
