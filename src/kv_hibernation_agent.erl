%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(kv_hibernation_agent).

-behaviour(gen_server).

-include("cut.hrl").
-include("ns_common.hrl").
-include("bucket_hibernation.hrl").

%% API
-export([start_link/0]).
-export([get_agents/1,
         set_service_manager/2,
         unset_service_manager/2,
         prepare_pause_bucket/3,
         unprepare_pause_bucket/2,
         pause_bucket/3,
         unpause_bucket/2,
         resume_bucket/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 10 * 1000). % 10 secs.

%% TODO: These default timeouts are a function of the blobStorage
%% upload/download speeds and the size of the data - therefore needs
%% re-evaluation.

-define(PAUSE_BUCKET_TIMEOUT,
        ?get_timeout(pause_bucket, infinity)).

-define(UNPAUSE_BUCKET_TIMEOUT,
        ?get_timeout(unpause_bucket, 5 * 1000)). % 5 secs.

-define(RESUME_BUCKET_TIMEOUT,
        ?get_timeout(resume_bucket, infinity)).

-record(state, {bucket = undefined :: undefined | bucket_name(),
                hibernation_manager = undefined
                    :: undefined | {pid(), reference()},
                service_manager = undefined :: undefined | {pid(), reference()},
                worker  = undefined :: undefined | {pid(), reference()},
                %% from is gen_server:from() i.e a reference passed
                %% back by gen_server module to the callback module when
                %% gen_server:handle_call is called.
                %%
                %% gen_server:from() isn't exported by the gen_server module.
                %% Will have to make do with the following type spec.
                %%
                from = undefined :: undefined | {pid(), gen_server:reply_tag()},
                op = undefined :: undefined | pause_bucket | resume_bucket}).

server(Node) ->
    {?SERVER, Node}.

get_agents(Nodes) ->
    Result = multi_call(Nodes, get_agent, ?TIMEOUT),
    handle_multicall_result(get_agent, Result, fun extract_ok_responses/1).

set_service_manager(Nodes, Manager) ->
    Result = multi_call(
               Nodes, {set_service_manager, Manager}, ?TIMEOUT),
    handle_multicall_result(set_service_manager, Result).

unset_service_manager(Nodes, Manager) ->
    Result = multi_call(
               Nodes, {unset_service_manager, Manager}, ?TIMEOUT),
    handle_multicall_result(unset_service_manager, Result).

prepare_pause_bucket(Bucket, Nodes, HibernationManager) ->
    Results = multi_call(
                Nodes, {prepare_pause_bucket, Bucket, HibernationManager},
                ?TIMEOUT),
    handle_multicall_result(prepare_pause_bucket, Results).

unprepare_pause_bucket(Bucket, Nodes) ->
    Results = multi_call(
                Nodes, {unprepare_pause_bucket, Bucket},
                ?TIMEOUT),
    handle_multicall_result(unprepare_pause_bucket, Results).

pause_bucket(Args, Node, Manager) ->
    gen_server:call(server(Node), {if_service_manager, Manager,
                                   {hibernation_op,
                                    {pause_bucket, Args}}},
                    ?PAUSE_BUCKET_TIMEOUT).

unpause_bucket(Bucket, Node) ->
    gen_server:call(server(Node), {unpause_bucket, Bucket},
                    ?UNPAUSE_BUCKET_TIMEOUT).

resume_bucket(Args, Node, Manager) ->
    gen_server:call(server(Node), {if_service_manager, Manager,
                                   {hibernation_op,
                                    {resume_bucket, Args}}},
                    ?RESUME_BUCKET_TIMEOUT).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(get_agent, _From, State) ->
    {reply, {ok, self()}, State};

handle_call({set_service_manager, Pid}, _From,
            #state{service_manager = Manager} = State) ->
    case Manager of
       undefined ->
            handle_set_service_manager(State, Pid);
       _ ->
            ?log_error("set_service_manager called by ~p when "
                       "service_manager already exists. Current Manager "
                       "~p", [Pid, Manager]),
            State1 = handle_unset_service_manager(State),
            handle_set_service_manager(State1, Pid)
    end;

handle_call({unset_service_manager, Pid}, _From,
            #state{service_manager = {Manager, _}} = State) ->
    case Pid of
        Manager ->
            {reply, ok, handle_unset_service_manager(State)};
        _ ->
            ?log_error("unset_service_manager called by"
                       " non-manager process"),
            {reply, nack, State}
    end;

handle_call({if_service_manager, Caller, SubCall}, From,
            #state{service_manager = {Manager, _}} = State) ->
    case Caller of
        Manager ->
            handle_sub_call(SubCall, From, State);
        _ ->
            {reply, {error, not_service_manager}, State}
    end;

handle_call({unpause_bucket, Bucket}, _From, State) ->
    {reply, do_unpause_bucket(Bucket), State};

handle_call({prepare_pause_bucket, Bucket, HibernationManager}, _From, State) ->
    MRef = erlang:monitor(process, HibernationManager),
    {reply, ok, State#state{bucket = Bucket,
                            hibernation_manager = {HibernationManager, MRef}}};

handle_call({unprepare_pause_bucket, Bucket}, _From,
            #state{bucket = Bucket} = State) ->
    {reply, ok, functools:chain(
                  State,
                  [unset_bucket(_),
                   demonitor_unset_hibernation_manager(_)])}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason} = DownMsg,
            #state{service_manager = {Pid, MRef},
                   hibernation_manager = HibManager,
                   op = Op} = State) ->
    %% kv service_manager has crashed - abort any running kv hibernation
    %% operations.

    ?log_debug("Service Manager for kv went down. DownMsg: ~p", [DownMsg]),

    %% In cases where hibernation_manager is still being monitored, preserve the
    %% Op field so it can be used in the DOWN of the hibernation_manager
    State1 = handle_unset_service_manager(State),
    StateFinal = case HibManager of
                     undefined ->
                         State1;
                     {_, _} ->
                         State1#state{op = Op}
                 end,
    {noreply, StateFinal};

handle_info({'DOWN', MRef, process, Pid, _Reason} = DownMsg,
            #state{bucket = Bucket,
                   hibernation_manager = {Pid, MRef},
                   worker = Worker,
                   op = Op} = State) ->
    %% Received when hibernation_manager crashes.
    %%
    %% We need to unpause any bucket that was paused. It can happen that
    %% pause for the bucket orchestrated via kv service_manager has finished,
    %% but the overall pause_bucket operation failed due to failures in another
    %% Services (or any other failure that crashes the hibernation_manager).

    ?log_debug("Hibernation Manager went down. Down msg: ~p", [DownMsg]),

    maybe_terminate_worker(Worker),

    Op = pause_bucket,
    do_unpause_bucket(Bucket),
    {noreply, functools:chain(
                State,
                [unset_bucket(_),
                 unset_op(_),
                 unset_worker(_),
                 demonitor_unset_hibernation_manager(_)])};

handle_info({MRef, {Op, done}},
            #state{worker = {_, MRef},
                   from = From,
                   op = Op} = State) ->
    maybe_reply(From, ok),
    {noreply, unset_from(State)};
handle_info({'EXIT', WorkerPid, normal},
            #state{worker = {WorkerPid, _}} = State) ->
    {noreply, unset_worker(State)};
handle_info({'EXIT', WorkerPid, Reason},
            #state{worker = {WorkerPid, _}, from = From} = State) ->
    %% Worker process has crashed, send the error to 'From' which will
    %% trigger a shutdown of kv service_manager.
    maybe_reply(From, Reason),

    {noreply, functools:chain(
                State, [unset_worker(_), unset_from(_)])}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_set_service_manager(State, Manager) ->
    MRef = erlang:monitor(process, Manager),
    {reply, ok, State#state{service_manager = {Manager, MRef}}}.

handle_unset_service_manager(#state{service_manager = {_Pid, MRef},
                                    worker = Worker} = State) ->
    erlang:demonitor(MRef, [flush]),

    maybe_terminate_worker(Worker),
    functools:chain(State,
                    [unset_from(_),
                     unset_op(_),
                     unset_worker(_),
                     unset_service_manager(_)]).

handle_sub_call({hibernation_op,
                 {pause_bucket,
                  #bucket_hibernation_op_args{bucket = Bucket} = Args}},
                From,
                #state{from = undefined,
                       bucket = Bucket} = State) ->
    {WorkerPid, Ref} =
        run_on_worker(pause_bucket, ?cut(do_pause_bucket(Args))),
    {noreply, State#state{worker = {WorkerPid, Ref},
                          from = From,
                          op = pause_bucket}};

handle_sub_call({hibernation_op,
                 {resume_bucket, Args}},
                From,
                #state{from = undefined} = State) ->
    {WorkerPid, Ref} =
        run_on_worker(resume_bucket, ?cut(do_resume_bucket(Args))),
    {noreply, State#state{worker = {WorkerPid, Ref},
                          from = From,
                          op = resume_bucket}};

handle_sub_call({hibernation_op, _},
                 _From, #state{op = Op,
                               bucket = Bucket} = State) ->
    {reply, State, {error, {hibernation_op_running, {Op, Bucket}}}}.

append_path_separator(Path) ->
    false = misc:is_windows(),
    Path ++ "/".

do_pause_bucket(#bucket_hibernation_op_args{
                   bucket = Bucket,
                   remote_path = RemotePath0,
                   blob_storage_region = BlobStorageRegion,
                   rate_limit = RateLimit} = Args) ->
    %% Kill all the DCP replications for this bucket.
    ok = replication_manager:set_incoming_replication_map(Bucket, []),

    ok = ns_memcached:pause_bucket(Bucket),

    {ok, LocalPath0} = ns_storage_conf:this_node_bucket_dbdir(Bucket),
    LocalPath = append_path_separator(LocalPath0),

    RemotePath = hibernation_utils:get_bucket_data_remote_path(
                   RemotePath0, node(), Bucket),

    ?log_info("pause_bucket op started. Bucket: ~p, Source: ~p, Dest: ~p, "
              "BlobStorageRegion: ~p, RateLimit - ~.2f MiB/s.",
              [Bucket, LocalPath, RemotePath, BlobStorageRegion,
               RateLimit / ?MIB]),

    ok = hibernation_utils:check_test_condition(node_pause_before_data_sync),
    ok = hibernation_utils:sync_s3(
           Args#bucket_hibernation_op_args{remote_path = RemotePath}, LocalPath,
           to),
    ?log_info("pause_bucket op completed."),
    ok.

do_resume_bucket(#bucket_hibernation_op_args{
                    bucket = Bucket,
                    remote_path = RemotePath0,
                    blob_storage_region = BlobStorageRegion,
                    rate_limit = RateLimit} = Args) ->
    ?log_info("Delete old bucket files: started."),
    %% On a new resume, we cleanup any old data for previously failed resumes
    ok = ns_storage_conf:delete_unused_buckets_db_files(),
    ?log_info("Delete old bucket files: completed."),

    RemotePath = RemotePath0 ++ "/",
    {ok, LocalPath} = ns_storage_conf:this_node_dbdir(),

    ?log_info("resume_bucket op started. Bucket: ~p, Source: ~p, Dest: ~p, "
              "BlobStorageRegion - ~p, RateLimit - ~.2f MiB/s",
              [Bucket, RemotePath, LocalPath, BlobStorageRegion,
               RateLimit / ?MIB]),

    ok = hibernation_utils:check_test_condition(node_resume_before_data_sync),

    ok = hibernation_utils:sync_s3(Args#bucket_hibernation_op_args{
                                     remote_path = RemotePath}, LocalPath,
                                   from),
    ?log_info("resume_bucket op completed"),
    ok.

do_unpause_bucket(Bucket) ->
    ok = ns_memcached:unpause_bucket(Bucket).

run_on_worker(Op, OpBody) ->
    Parent = self(),
    Ref = make_ref(),

    Worker = proc_lib:spawn_link(
               fun () ->
                       OpBody(),
                       Parent ! {Ref, {Op, done}}
               end),

    {Worker, Ref}.

unset_worker(State) ->
    State#state{worker = undefined}.

unset_from(State) ->
    State#state{from = undefined}.

unset_service_manager(State) ->
    State#state{service_manager = undefined}.

unset_bucket(State) ->
    State#state{bucket = undefined}.

unset_op(State) ->
    State#state{op = undefined}.

demonitor_unset_hibernation_manager(
  #state{hibernation_manager = {_, MRef}} = State) ->
    erlang:demonitor(MRef, [flush]),
    State#state{hibernation_manager = undefined}.

maybe_terminate_worker(undefined) ->
    ok;
maybe_terminate_worker({WorkerPid, _}) ->
    misc:unlink_terminate_and_wait(WorkerPid, shutdown).

maybe_reply(undefined, _Result) ->
    ok;
maybe_reply(From, Result) ->
    gen_server:reply(From, Result).

is_good_result(ok) ->
    true;
is_good_result({ok, _}) ->
    true;
is_good_result(_) ->
    false.

just_ok(_) ->
    ok.

multi_call(Nodes, Request, Timeout) ->
   misc:multi_call(Nodes, ?SERVER,
                   Request, Timeout, fun is_good_result/1).

handle_multicall_result(Call, Result) ->
    handle_multicall_result(Call, Result, fun just_ok/1).

handle_multicall_result(Call, {Good, Bad}, OkFun) ->
    case Bad of
        [] ->
            OkFun(Good);
        _ ->
            process_bad_results(Call, Bad)
    end.

process_bad_results(Call, Bad) ->
    ?log_error("kv_hibernation_agent call (~p) failed on some nodes:~n~p",
               [Call, Bad]),
    {error, {bad_nodes, Call, Bad}}.

extract_ok_responses(Replies) ->
    ActualReplies =
        [begin
             {ok, ActualRV} = RV,
             {N, ActualRV}
         end || {N, RV} <- Replies],
    {ok, ActualReplies}.
