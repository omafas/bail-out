%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(samples_loader_tasks).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("global_tasks.hrl").

%% gen_server API
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_loading_sample/5, get_tasks/1]).

-import(menelaus_web_samples, [is_http/1]).

start_loading_sample(Sample, Bucket, Quota, CacheDir, BucketState) ->
    gen_server:call(?MODULE, {start_loading_sample, Sample, Bucket, Quota,
                              CacheDir, BucketState}, infinity).

get_tasks(Timeout) ->
    gen_server:call(?MODULE, get_tasks, Timeout).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-record(state, {queued_tasks = [] :: [{bucket_name(), pid(), binary()}],
                running_tasks = [] :: [{bucket_name(), pid(), binary()}]}).

init([]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({start_loading_sample, Sample, Bucket, Quota, CacheDir,
             BucketState}, _From,
            #state{queued_tasks = Tasks} = State) ->
    ?log_debug("Received request to load sample ~p into bucket ~p",
               [Sample, Bucket]),
    case lists:keyfind(Bucket, 1, Tasks) of
        false ->
            TaskId = misc:uuid_v4(),
            Pid = start_new_loading_task(
                    TaskId, Sample, Bucket, Quota, CacheDir, BucketState),
            create_queued_task(TaskId, Bucket),
            ns_heart:force_beat(),
            Task = {Bucket, Pid, TaskId},
            ?log_debug("Queue loading task ~p", [Task]),
            NewState = State#state{queued_tasks = Tasks ++ [Task]},
            {reply, {newly_started, TaskId}, maybe_start_task(NewState)};
        {_, _, TaskId} = T ->
            ?log_debug("Loading task ~p is already queued", [T]),
            {reply, {already_started, TaskId}, State}
    end;
%% Get all queued and running tasks
handle_call(get_tasks, _From, State) ->
    {reply, get_all_tasks(State), State}.


handle_cast(_, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason} = Msg,
            #state{queued_tasks = QueuedTasks,
                   running_tasks = RunningTasks} = State) ->
    case lists:keyfind(Pid, 2, RunningTasks) of
        false ->
            ?log_error("Got exit not from child: ~p", [Msg]),
            exit(Reason);
        {Name, _, TaskId} = Task ->
            ?log_debug("Consumed exit signal from samples loading task ~s: ~p",
                       [Name, Msg]),
            ns_heart:force_beat(),
            case Reason of
                normal ->
                    update_task_status(TaskId, completed),
                    ale:info(?USER_LOGGER, "Completed loading sample bucket ~s",
                             [Name]);
                {failed_to_load_samples, Status, Output} ->
                    update_task_status(TaskId, failed),
                    ale:error(?USER_LOGGER,
                              "Task ~p - loading sample bucket ~s failed. "
                              "Samples loader exited with status ~b.~n"
                              "Loader's output was:~n~n~s",
                              [TaskId, Name, Status, Output]);
                _ ->
                    update_task_status(TaskId, failed),
                    ale:error(?USER_LOGGER,
                              "Task ~p - loading sample bucket ~s failed: ~p",
                              [TaskId, Name, Reason])
            end,
            %% Check whether to remove task from running or queued
            NewState =
                case lists:member(Task, RunningTasks) of
                    true ->
                        ?log_debug("Token holder died"),
                        State#state{running_tasks = lists:delete(Task,
                                                                 RunningTasks)};
                    _ ->
                        State#state{queued_tasks = lists:delete(Task,
                                                                QueuedTasks)}
                end,
            {noreply, maybe_start_task(NewState)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{} = State) ->
    %% Set the status of each queued or running task to 'failed'
    TaskIds = [TaskId || {_, _, TaskId} <- get_all_tasks(State)],
    global_tasks:update_tasks(TaskIds,
                              lists:keyreplace(status, 1, _, {status, failed})).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal tasks list
get_all_tasks(State) ->
    State#state.running_tasks ++ State#state.queued_tasks.

get_max_concurrent_sample_loads() ->
    ns_config:read_key_fast({serverless, max_concurrent_sample_loads}, 1).

maybe_start_task(#state{running_tasks = RunningTasks,
                        queued_tasks = [{Bucket, FirstPid, TaskId} = Task
                                       | OtherQueuedTasks]} = State) ->
    case length(RunningTasks) < get_max_concurrent_sample_loads() of
        true ->
            FirstPid ! allowed_to_go,
            update_task_status(TaskId, running),
            ?log_info("Started sample loading task for bucket ~s (~p)",
                      [Bucket, TaskId]),
            State#state{running_tasks = [Task | RunningTasks],
                        queued_tasks = OtherQueuedTasks};
        false ->
            State
    end;
maybe_start_task(State) ->
    State.

-spec create_queued_task(binary(), string()) -> ok.
create_queued_task(TaskId, BucketName) ->
    global_tasks:update_task(
      #global_task{
         task_id = TaskId,
         type = loadingSampleBucket,
         status = queued,
         extras = [{bucket, BucketName},
                   {bucket_uuid, ns_bucket:uuid(BucketName, direct)}]}).

-spec update_task_status(binary(), status()) -> ok.
update_task_status(TaskId, Status) ->
    global_tasks:update_tasks(
      [TaskId],
      fun (Task) ->
              lists:keyreplace(status, 1, Task, {status, Status})
      end).

start_new_loading_task(TaskId, Sample, Bucket, Quota, CacheDir, BucketState) ->
    proc_lib:spawn_link(
      erlang, apply, [fun perform_loading_task/6,
                      [TaskId, Sample, Bucket, Quota, CacheDir, BucketState]]).

perform_loading_task(TaskId, Sample, Bucket, Quota, CacheDir, BucketState) ->
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
                           "--cacert", ns_ssl_services_setup:ca_file_path()];
                      false ->
                          Port = service_ports:get_port(rest_port),
                          ["--cluster", misc:join_host_port(Host, Port)]
                  end,
    BinDir = path_config:component_path(bin),
    NumReplicas0 = case length(ns_cluster_membership:nodes_wanted()) of
                       1 -> 0;
                       _ -> 1
                   end,
    %% Honor any min replica setting otherwise the bucket creation could
    %% fail if the min replica value isn't met.
    NumReplicas = max(NumReplicas0, ns_bucket:get_min_replicas()),

    Cmd = BinDir ++ "/cbimport",
    {DataSet, AdditionalArgs} =
        case is_http(Sample) of
            true ->
                {Sample,
                 ["--http-cache-directory", CacheDir]};
            false ->
                {"file://" ++
                     filename:join([BinDir, "..", "samples",
                                    Sample ++ ".zip"]),
                 []}
        end,
    Args = ["json",
            "--bucket", Bucket,
            "--format", "sample",
            "--threads", "2",
            "--verbose",
            "--gocbcore-log-level", "debug",
            "--dataset", DataSet] ++
            AdditionalArgs ++
            ClusterOpts ++
            case BucketState of
                bucket_must_exist ->
                    ["--disable-bucket-config"];
                bucket_must_not_exist ->
                    ["--bucket-quota", integer_to_list(Quota),
                     "--bucket-replicas", integer_to_list(NumReplicas)]
            end,

    Name = "cbimport_" ++ binary_to_list(TaskId),
    Env0 = [{"CB_USERNAME", "@ns_server"},
            {"CB_PASSWORD", ns_config_auth:get_password(special)}] ++
        case ns_ssl_services_setup:client_cert_auth_state() of
            State when State =:= "mandatory" ->
                ClientPassFun = ns_secrets:get_pkey_pass(client_cert),
                [{"CB_CLIENT_CERT",
                  ns_ssl_services_setup:chain_file_path(client_cert)},
                 {"CB_CLIENT_KEY",
                  ns_ssl_services_setup:pkey_file_path(client_cert)},
                 {"CB_CLIENT_KEY_PASSWORD", ClientPassFun()}];
            _ ->
                []
        end,
    Env = Env0 ++
        ns_ports_setup:build_cbauth_env_vars(ns_config:latest(), Name),
    {Status, Output} = misc:run_external_tool(Cmd, Args, Env),
    case Status of
        0 ->
            ok;
        _ ->
            exit({failed_to_load_samples, Status, Output})
    end.
