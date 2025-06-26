%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% This module lets you treat a memcached process as a gen_server.
%% Right now we have one of these registered per node, which stays
%% connected to the local memcached server as the admin user. All
%% communication with that memcached server is expected to pass
%% through distributed erlang, not using memcached prototocol over the
%% LAN.
%%
%% Calls to memcached that can potentially take long time are passed
%% down to one of worker processes. ns_memcached process is
%% maintaining list of ready workers and calls queue. When
%% gen_server:call arrives it is added to calls queue. And if there's
%% ready worker, it is dequeued and sent to worker. Worker then does
%% direct gen_server:reply to caller.
%%
-module(ns_memcached).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("rbac.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("cb_cluster_secrets.hrl").

-define(CHECK_INTERVAL, 10000).
-define(CHECK_WARMUP_INTERVAL, 500).
-define(CONNECT_DONE_RETRY_INTERVAL, 500).
-define(TIMEOUT,             ?get_timeout(outer, 300000)).
-define(TIMEOUT_HEAVY,       ?get_timeout(outer_heavy, 300000)).
-define(TIMEOUT_VERY_HEAVY,  ?get_timeout(outer_very_heavy, 360000)).
-define(MARK_WARMED_TIMEOUT, ?get_timeout(mark_warmed, 5000)).
-define(STATUSES_TIMEOUT,    ?get_timeout(statuses, 5000)).
%% half-second is definitely 'slow' for any definition of slow
-define(SLOW_CALL_THRESHOLD_MICROS, 500000).
-define(GET_KEYS_TIMEOUT,       ?get_timeout(get_keys, 60000)).
-define(GET_KEYS_OUTER_TIMEOUT, ?get_timeout(get_keys_outer, 70000)).
-define(MAGMA_CREATION_TIMEOUT, ?get_timeout(magma_creation, 300000)).
-define(SET_KEYS_TIMEOUT,       ?get_timeout(set_keys, 30000)).
-define(PRUNE_KEYS_TIMEOUT,       ?get_timeout(prune_keys, 30000)).
-define(REENCRYPT_BUCKET_METADATA_TIMEOUT,
        ?get_timeout(reencrypt_bucket_metadata, 3000)).
-define(GET_METADATA_FILE_DEK_IDS_TIMEOUT,
        ?get_timeout(get_metadata_file_dek_ids, 3000)).

-define(RECBUF, ?get_param(recbuf, 64 * 1024)).
-define(SNDBUF, ?get_param(sndbuf, 64 * 1024)).

-define(CONNECTION_ATTEMPTS, 5).
-define(DEFAULT_TIMEOUT, infinity).

-define(JWT_LIFETIME_MS, ?get_param(jwt_lifetime, 600000)).
-define(JWT_RENEWAL_WINDOW_MS, ?CHECK_INTERVAL * 2).

%% gen_server API
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-type set_vbucket_option() :: {topology, list()} | {use_snapshot, binary()}.
-export_type([set_vbucket_option/0]).

-type status() :: connecting | init | connected | warmed | paused.

-record(state, {
                running_fast = 0,
                running_heavy = 0,
                running_very_heavy = 0,
                %% NOTE: otherwise dialyzer seemingly thinks it's possible
                %% for queue fields to be undefined
                fast_calls_queue = impossible :: queue:queue(),
                heavy_calls_queue = impossible :: queue:queue(),
                very_heavy_calls_queue = impossible :: queue:queue(),
                status :: status(),
                start_time :: undefined | tuple(),
                bucket :: bucket_name(),
                worker_features = [],
                worker_pids :: [pid()],
                sock = still_connecting :: port() | still_connecting,
                work_requests = [],
                warmup_stats = [] :: [{binary(), binary()}],
                control_queue :: pid() | undefined,
                check_in_progress = false :: boolean(),
                next_check_after = ?CHECK_INTERVAL :: integer(),
                jwt_expires :: undefined | integer(),
                bucket_uuid :: binary()
               }).

%% external API
-export([active_buckets/0,
         server/1,
         get_mark_warmed_timeout/0,
         bucket_statuses/0,
         bucket_statuses/1,
         get_all_buckets_details/0,
         get_bucket_state/1,
         mark_warmed/2,
         disable_traffic/2,
         set_data_ingress/2,
         delete_vbucket/2,
         delete_vbuckets/2,
         sync_delete_vbucket/2,
         get_vbucket_details_stats/2,
         get_single_vbucket_details_stats/3,
         host_ports/1,
         host_ports/2,
         list_vbuckets/1, list_vbuckets/2,
         local_connected_and_list_vbuckets/1,
         local_connected_and_list_vbucket_details/2,
         set_vbucket/3, set_vbucket/4,
         set_vbuckets/2,
         pause_bucket/1,
         unpause_bucket/1,
         stats/2,
         warmup_stats/1,
         raw_stats/5,
         raw_stats/6,
         flush/1,
         set/9, add/5, get/5, delete/5,
         get_from_replica/4,
         get_meta/5,
         get_xattrs/6,
         update_with_rev/7,
         get_seqno_stats/2,
         get_mass_dcp_docs_estimate/2,
         get_dcp_docs_estimate/3,
         set_cluster_config/3,
         get_ep_startup_time_for_xdcr/1,
         perform_checkpoint_commit_for_xdcr/3,
         get_random_key/1, get_random_key/2,
         compact_vbucket/3,
         get_vbucket_high_seqno/2,
         get_all_vb_seqnos/1,
         wait_for_seqno_persistence/3,
         get_keys/4,
         config_validate/2,
         config_reload/1,
         set_tls_config/1,
         get_failover_log/2,
         get_failover_logs/2,
         get_collections_uid/1,
         maybe_add_impersonate_user_frame_info/2,
         delete_bucket/2,
         get_config_stats/2,
         set_active_dek_for_bucket_uuid/2,
         set_active_dek/2,
         prune_log_or_audit_encr_keys/2,
         get_dek_ids_in_use/1,
         drop_deks/4,
         get_fusion_storage_snapshot/4,
         mount_fusion_vbucket/3,
         maybe_start_fusion_uploaders/2,
         maybe_stop_fusion_uploaders/2,
         get_active_guest_volumes/1,
         sync_fusion_log_store/2,
         bucket_metadata_file/1
        ]).

%% for ns_memcached_sockets_pool, memcached_file_refresh only
-export([connect/2, connect/3]).

%% for diagnostics/debugging
-export([perform_very_long_call/2]).

-include("mc_constants.hrl").
-include("mc_entry.hrl").

%%
%% gen_server API implementation
%%

start_link(Bucket) ->
    %% Sync with node monitor to make sure the key is created by the time
    %% when ns_memcached is started. Note that ns_memcached can't call
    %% cb_cluster_secrets, because cb_cluster_secrets calls ns_memcached, so
    %% deadlock is possible.
    cluster_compat_mode:is_enterprise() andalso
        cb_cluster_secrets:sync_with_node_monitor(),
    gen_server:start_link({local, server(Bucket)}, ?MODULE, Bucket, []).


%%
%% gen_server callback implementation
%%

init(Bucket) ->
    ?log_debug("Starting ns_memcached"),
    Q = queue:new(),
    WorkersCount = case ns_config:search_node(ns_memcached_workers_count) of
                       false -> 4;
                       {value, DefinedWorkersCount} ->
                           DefinedWorkersCount
                   end,
    Self = self(),
    {ok, ControlQueue} = work_queue:start_link(),
    work_queue:submit_work(
      ControlQueue, ?cut(run_connect_phase(Self, Bucket, WorkersCount))),
    BucketUUID = ns_bucket:uuid(Bucket, direct),
    true = is_binary(BucketUUID),
    State = #state{
               status = connecting,
               bucket = Bucket,
               worker_features = get_worker_features(),
               work_requests = [],
               worker_pids = [],
               fast_calls_queue = Q,
               heavy_calls_queue = Q,
               very_heavy_calls_queue = Q,
               running_fast = WorkersCount,
               control_queue = ControlQueue,
               bucket_uuid = BucketUUID
              },
    {ok, State}.

run_connect_phase(Parent, Bucket, WorkersCount) ->
    ?log_debug("Started 'connecting' phase of ns_memcached-~s. Parent is ~p",
               [Bucket, Parent]),
    RV = case connect(?MODULE_STRING ++ "-" ++ Bucket, [json]) of
             {ok, Sock} ->
                 gen_tcp:controlling_process(Sock, Parent),
                 {ok, Sock};
             {error, _} = Error  ->
                 Error
         end,
    Parent ! {connect_done, WorkersCount, RV}.

get_worker_features() ->
    FeatureSet = [%% Always use json feature as the local memcached would have
                  %% enabled this feature in/after 6.5.
                  {json, true},
                  {collections, true}],
    misc:canonical_proplist(FeatureSet).

worker_init(Parent, ParentState) ->
    ParentState1 = do_worker_init(ParentState),
    worker_loop(Parent, ParentState1, #state.running_fast).

do_worker_init(#state{bucket = Bucket} = State) ->
    AgentName = ?MODULE_STRING ++ "-" ++ Bucket ++ "/worker",
    {ok, Sock} = connect(AgentName, State#state.worker_features),

    {ok, SockName} = inet:sockname(Sock),
    erlang:put(sockname, SockName),

    ok = mc_client_binary:select_bucket(Sock, Bucket),
    State#state{sock = Sock}.

worker_loop(Parent, #state{sock = Sock,
                           bucket = Bucket} = State, PrevCounterSlot) ->
    ok = inet:setopts(Sock, [{active, once}]),
    {Msg, From, StartTS, CounterSlot} = gen_server:call(Parent, {get_work, PrevCounterSlot}, infinity),
    case inet:setopts(Sock, [{active, false}]) of
        %% Exit if socket is closed by memcached, which is possible if our
        %% previous request was erroneous.
        {error, einval} ->
            error(lost_connection);
        ok ->
            ok
    end,

    receive
        {tcp, Sock, Data} ->
            error({extra_data_on_socket, Data})
    after 0 ->
            ok
    end,

    WorkStartTS = os:timestamp(),

    erlang:put(last_call, Msg),
    case do_handle_call(Msg, From, State) of
        %% note we only accept calls that don't mutate state. So in- and
        %% out- going states asserted to be same.
        {reply, R, State} ->
            gen_server:reply(From, R);
        {compromised_reply, R, State} ->
            ?log_warning(
               "Call ~p (return value ~p) compromised connection for bucket"
               " ~p. Reconnecting.", [Msg, R, Bucket]),
            gen_server:reply(From, R),
            error({compromised_reply, R})
    end,

    verify_report_long_call(Bucket, StartTS, WorkStartTS, State, Msg, []),
    worker_loop(Parent, State, CounterSlot).

handle_call({get_work, CounterSlot}, From, #state{work_requests = Froms} = State) ->
    State2 = State#state{work_requests = [From | Froms]},
    Counter = erlang:element(CounterSlot, State2) - 1,
    State3 = erlang:setelement(CounterSlot, State2, Counter),
    {noreply, maybe_deliver_work(State3)};
handle_call(connected_and_list_vbuckets, From, State) ->
    handle_connected_call(list_vbuckets, From, State);
handle_call({connected_and_list_vbucket_details, Keys}, From, State) ->
    handle_connected_call({get_vbucket_details_stats, all, Keys}, From, State);
handle_call(warmed, _From, #state{status = paused} = State) ->
    {reply, false, State};
handle_call(warmed, From, #state{status = warmed} = State) ->
    %% A bucket is set to "warmed" state in ns_memcached,
    %% after the bucket is loaded in memcached and ns_server
    %% has enabled traffic to it.
    %% So, normally, a "warmed" state in ns_memcached also
    %% indicates that the bucket is also ready in memcached.
    %% But in some failure scenarios where memcached becomes
    %% unresponsive, it may take up to 10s for ns_memcached
    %% to realize there is an issue.
    %% So, in addition to checking ns_memcached state, also
    %% retrive stats from memcached to verify it is
    %% responsive.
    handle_call(verify_warmup, From, State);
handle_call(warmed, _From, State) ->
    {reply, false, State};
handle_call(paused, _From, #state{status = paused} = State) ->
    {reply, true, State};
handle_call(paused, _From, State) ->
    {reply, false, State};
handle_call(status, From, #state{status = warmed} = State) ->
    handle_call(verify_warmup_status, From, State);
handle_call(status, _From, #state{status = Status} = State) ->
    {reply, Status, State};
handle_call(disable_traffic, _From, #state{bucket_uuid = BucketUUID} = State) ->
    case State#state.status of
        Status when Status =:= warmed; Status =:= connected ->
            ?log_info("Disabling traffic and unmarking bucket as warmed"),
            case mc_client_binary:disable_traffic(State#state.sock) of
                ok ->
                    State2 = State#state{status=connected,
                                         start_time = os:timestamp()},
                    BucketName = State2#state.bucket,
                    event_log:add_log(bucket_offline,
                                      [{bucket, list_to_binary(BucketName)},
                                       {bucket_uuid, BucketUUID}]),
                    {reply, ok, State2};
                {memcached_error, _, _} = Error ->
                    ?log_error("disabling traffic failed: ~p", [Error]),
                    {reply, Error, State}
            end;
        _ ->
            {reply, bad_status, State}
    end;
handle_call(prepare_pause_bucket, _From,
            #state{sock = Sock, worker_pids = WorkerPids,
                   control_queue = ControlQPid} = State) ->
    %% Memcached will close all selected sockets when it processes the pause,
    %% so prepare for a graceful pause so that it doesn't crash ns_memcached
    [misc:unlink_terminate_and_wait(Pid, shutdown) ||
        Pid <- [ControlQPid | WorkerPids]],
    ok = mc_client_binary:deselect_bucket(Sock),
    ?log_debug("Prepare pause completed"),
    {reply, ok, State#state{worker_pids = [], control_queue = undefined}};
handle_call(complete_pause_bucket, _From, #state{bucket=Bucket} = State) ->
    ?log_debug("Pausing completed for bucket: ~p", [Bucket]),
    {reply, ok, State#state{status=paused}};
handle_call({unpause_bucket, Bucket}, _From, #state{bucket = Bucket,
                                                    sock = Sock} = State) ->
    ?log_info("Unpausing bucket: ~p", [Bucket]),
    case mc_client_binary:unpause_bucket(Sock, Bucket) of
        ok ->
            ?log_debug("Unpaused bucket: ~p", [Bucket]),
            %% Happens on pause failure, at that point we unpause and
            %% re-initialize
            {stop, unpaused, ok, State};
        {memcached_error,key_eexists,_} ->
            ?log_debug("Bucket ~p already unpaused", [Bucket]),
            %% Re-initialize in this case as well because this also happens on
            %% pause failure
            {stop, unpaused, ok, State};
        {memcached_error, _, _} = Error ->
            ?log_error("Unpausing bucket ~p failed: ~p", [Bucket, Error]),
            {reply, Error, State}
    end;
handle_call({set_bucket_data_ingress, Status}, _From, State) ->
    case mc_client_binary:set_bucket_data_ingress(
           State#state.sock, State#state.bucket, Status) of
        ok ->
            {reply, ok, State};
        {memcached_error, _, _} = Error ->
            ?log_error("setting bucket data ingress failed: ~p", [Error]),
            {reply, Error, State}
    end;
handle_call(mark_warmed, From, State) ->
    handle_call({mark_warmed, undefined}, From, State);
handle_call({mark_warmed, DataIngress}, _From, #state{status=Status,
                                                      bucket=Bucket,
                                                      start_time=Start,
                                                      bucket_uuid = BucketUUID,
                                                      sock=Sock} = State) ->
    {NewStatus, Reply} =
        case Status of
            connected ->
                case DataIngress of
                    undefined ->
                        ok;
                    _ ->
                        mc_client_binary:set_bucket_data_ingress(Sock, Bucket,
                                                                 DataIngress)
                end,
                ?log_info("Enabling traffic to bucket ~p", [Bucket]),
                case mc_client_binary:enable_traffic(Sock) of
                    ok ->
                        Time = timer:now_diff(os:timestamp(), Start) div 1000000,
                        ?log_info("Bucket ~p marked as warmed in ~p seconds",
                                  [Bucket, Time]),
                        event_log:add_log(bucket_online,
                                          [{bucket, list_to_binary(Bucket)},
                                           {bucket_uuid, BucketUUID},
                                           {warmup_time, Time}]),
                        %% Make best effort to update the status of bucket
                        %% readiness on node.
                        gen_event:notify(buckets_events, {warmed, Bucket}),
                        {warmed, ok};
                    Error ->
                        ?log_error("Failed to enable traffic to bucket ~p: ~p",
                                   [Bucket, Error]),
                        {Status, Error}
                end;
            warmed ->
                {warmed, ok};
            _ ->
                {Status, bad_status}
        end,

    {reply, Reply, State#state{status=NewStatus}};
handle_call(warmup_stats, _From, State) ->
    {reply, State#state.warmup_stats, State};
handle_call(maybe_reencrypt_bucket_metadata, _From,
            #state{bucket = Bucket, bucket_uuid = BucketUUID} = State) ->
    {ok, DS} = cb_crypto:fetch_deks_snapshot({bucketDek, BucketUUID}),
    {reply, maybe_reencrypt_bucket_metadata(Bucket, BucketUUID, DS), State};
handle_call(get_metadata_file_dek_ids, _From,
            #state{bucket_uuid = BucketUUID} = State) ->
    {reply, get_metadata_file_dek_ids_direct(BucketUUID), State};
handle_call(Msg, From, State) ->
    StartTS = os:timestamp(),
    NewState = queue_call(Msg, From, StartTS, State),
    {noreply, NewState}.

perform_very_long_call(Fun) ->
    perform_very_long_call(Fun, undefined).

perform_very_long_call(Fun, Bucket) ->
    perform_very_long_call(Fun, Bucket, []).

perform_very_long_call(Fun, Bucket, Options) ->
    ns_memcached_sockets_pool:executing_on_socket(
      fun (Sock) ->
              {reply, Result} = Fun(Sock),
              Result
      end, Bucket, Options).

verify_report_long_call(Bucket, StartTS, ActualStartTS, State, Msg, RV) ->
    try
        RV
    after
        EndTS = os:timestamp(),
        Diff = timer:now_diff(EndTS, ActualStartTS),
        QDiff = timer:now_diff(EndTS, StartTS),
        Bucket = State#state.bucket,
        ns_server_stats:notify_histogram(
          {<<"memcached_call_time">>, [{bucket, Bucket}]},
          Diff div 1000),
        ns_server_stats:notify_histogram(
          {<<"memcached_q_call_time">>, [{bucket, Bucket}]},
          QDiff div 1000),
        if
            Diff > ?SLOW_CALL_THRESHOLD_MICROS ->
                ?log_debug("Call ~p for bucket ~p took too long: ~p us",
                           [Msg, Bucket, Diff]);
            true ->
                ok
        end
    end.

%% anything effectful is likely to be heavy
assign_queue({delete_vbucket, _}) -> #state.very_heavy_calls_queue;
assign_queue({delete_vbuckets, _}) -> #state.very_heavy_calls_queue;
assign_queue({sync_delete_vbucket, _}) -> #state.very_heavy_calls_queue;
assign_queue(flush) -> #state.very_heavy_calls_queue;
assign_queue({set_vbucket, _, _, _}) -> #state.heavy_calls_queue;
assign_queue({set_vbuckets, _}) -> #state.very_heavy_calls_queue;
assign_queue({add, _KeyFun, _Uid, _VBucket, _ValueFun}) ->
    #state.heavy_calls_queue;
assign_queue({get, _KeyFun, _Uid, _VBucket, _Identity}) ->
    #state.heavy_calls_queue;
assign_queue({get_from_replica, _Fun, _Uid, _VBucket}) ->
    #state.heavy_calls_queue;
assign_queue({delete, _KeyFun, _Uid, _VBucket, _Identity}) ->
    #state.heavy_calls_queue;
assign_queue({set, _KeyFun, _Uid, _VBucket, _ValueFun, _Flags, _Expiry,
              _PreserveTTL, _Identity}) ->
    #state.heavy_calls_queue;
assign_queue({get_keys, _VBuckets, _Params}) -> #state.heavy_calls_queue;
assign_queue({get_keys, _VBuckets, _Params, _Identity}) -> #state.heavy_calls_queue;
assign_queue({get_mass_dcp_docs_estimate, _VBuckets}) -> #state.very_heavy_calls_queue;
assign_queue({get_vbucket_details_stats, all, _}) -> #state.very_heavy_calls_queue;
assign_queue(get_all_vb_seqnos) -> #state.very_heavy_calls_queue;
assign_queue(_) -> #state.fast_calls_queue.

queue_to_counter_slot(#state.very_heavy_calls_queue) -> #state.running_very_heavy;
queue_to_counter_slot(#state.heavy_calls_queue) -> #state.running_heavy;
queue_to_counter_slot(#state.fast_calls_queue) -> #state.running_fast.

queue_call(Msg, From, StartTS, State) ->
    QI = assign_queue(Msg),
    Q = erlang:element(QI, State),
    CounterSlot = queue_to_counter_slot(QI),
    Q2 = queue:snoc(Q, {Msg, From, StartTS, CounterSlot}),
    State2 = erlang:setelement(QI, State, Q2),
    maybe_deliver_work(State2).

maybe_deliver_work(#state{running_very_heavy = RunningVeryHeavy,
                          running_fast = RunningFast,
                          work_requests = Froms} = State) ->
    case Froms of
        [] ->
            State;
        [From | RestFroms] ->
            StartedHeavy =
                %% we only consider starting heavy calls if
                %% there's extra free worker for fast calls. Thus
                %% we're considering heavy queues first. Otherwise
                %% we'll be starving them.
                case RestFroms =/= [] orelse RunningFast > 0 of
                    false ->
                        failed;
                    _ ->
                        StartedVeryHeavy =
                            case RunningVeryHeavy of
                                %% we allow only one concurrent very
                                %% heavy call. Thus it makes sense to
                                %% consider very heavy queue first
                                0 ->
                                    try_deliver_work(State, From, RestFroms, #state.very_heavy_calls_queue);
                                _ ->
                                    failed
                            end,
                        case StartedVeryHeavy of
                            failed ->
                                try_deliver_work(State, From, RestFroms, #state.heavy_calls_queue);
                            _ ->
                                StartedVeryHeavy
                        end
                end,
            StartedFast =
                case StartedHeavy of
                    failed ->
                        try_deliver_work(State, From, RestFroms, #state.fast_calls_queue);
                    _ ->
                        StartedHeavy
                end,
            case StartedFast of
                failed ->
                    State;
                _ ->
                    maybe_deliver_work(StartedFast)
            end
    end.

%% -spec try_deliver_work(#state{}, any(), [any()], (#state.very_heavy_calls_queue) | (#state.heavy_calls_queue) | (#state.fast_calls_queue)) ->
%%                               failed | #state{}.
-spec try_deliver_work(#state{}, any(), [any()], 5 | 6 | 7) ->
                              failed | #state{}.
try_deliver_work(State, From, RestFroms, QueueSlot) ->
    Q = erlang:element(QueueSlot, State),
    case queue:is_empty(Q) of
        true ->
            failed;
        _ ->
            {_Msg, _From, _StartTS, CounterSlot} = Call = queue:head(Q),
            gen_server:reply(From, Call),
            State2 = State#state{work_requests = RestFroms},
            Counter = erlang:element(CounterSlot, State2),
            State3 = erlang:setelement(CounterSlot, State2, Counter + 1),
            erlang:setelement(QueueSlot, State3, queue:tail(Q))
    end.

maybe_add_impersonate_user_frame_info(undefined, McHeader) ->
    McHeader;
maybe_add_impersonate_user_frame_info(Identity, McHeader) ->
    %% Add the user on whose behalf @ns_server will perform a memcached
    %% operation ('Oper').
    %%
    %% Protocol Specification:
    %% http://src.couchbase.org/source/xref/trunk/kv_engine/docs/
    %% BinaryProtocol.md#164

    OnBehalfOf = case Identity of
                     {User, external} ->
                         iolist_to_binary([$^, User]);
                     {User, _} ->
                         list_to_binary(User)
                 end,

    McFrameInfo = #mc_frame_info{obj_id = ?IMPERSONATE_USER_ID,
                                 obj_data = OnBehalfOf},
    add_frame_info(McFrameInfo, McHeader).

maybe_add_preserve_ttl_frame_info(false, McHeader) ->
    McHeader;
maybe_add_preserve_ttl_frame_info(true, McHeader) ->
    %% If the request modifies an existing document the expiry time from the
    %% existing document should be used instead of the TTL provided. If document
    %% don't exist the provided TTL should be used.
    %%
    %% Protocol Specification:
    %% https://src.couchbase.org/source/xref/trunk/kv_engine/docs/
    %% BinaryProtocol.md?r=4f50f87b#176


    McFrameInfo = #mc_frame_info{obj_id = ?PRESERVE_TTL},
    add_frame_info(McFrameInfo, McHeader).

add_frame_info(McFrameInfo, McHeader) ->
    Rest =
        case McHeader#mc_header.frame_infos of
            undefined ->
                [];
            Infos when is_list(Infos) ->
                Infos
        end,
    McHeader#mc_header{frame_infos = [McFrameInfo | Rest]}.


handle_data_call(Command, KeyFun, CollectionsUid, VBucket, State) ->
    handle_data_call(Command, KeyFun, CollectionsUid, VBucket, #mc_entry{},
                     State).

handle_data_call(Command, KeyFun, CollectionsUid, VBucket, McEntry, State) ->
    handle_data_call(Command, KeyFun, CollectionsUid, VBucket, false, undefined,
                     McEntry, State).

handle_data_call(Command, KeyFun, CollectionsUid, VBucket, PreserveTTL,
                 Identity, McEntry,
                 #state{worker_features = Features} = State) ->
    CollectionsEnabled = proplists:get_bool(collections, Features),
    EncodedKey = mc_binary:maybe_encode_uid_in_key(CollectionsEnabled,
                                                   CollectionsUid, KeyFun()),
    McHeader0 = #mc_header{vbucket = VBucket},
    McHeader1 = maybe_add_impersonate_user_frame_info(Identity, McHeader0),
    McHeader = maybe_add_preserve_ttl_frame_info(PreserveTTL, McHeader1),

    Reply = mc_client_binary:cmd(
              Command, State#state.sock, undefined, undefined,
              {McHeader,
               McEntry#mc_entry{key = EncodedKey}}),
    {reply, Reply, State}.

do_handle_call(verify_warmup,  _From, #state{bucket = Bucket,
                                             sock = Sock} = State) ->
    Stats = retrieve_warmup_stats(Sock),
    {reply, has_started(Stats, Bucket), State};
do_handle_call(verify_warmup_status, _From, #state{bucket = Bucket,
                                                   sock = Sock} = State) ->
    Stats = retrieve_warmup_stats(Sock),
    Response = case has_started(Stats, Bucket) of
        true -> warmed;
        _ -> connected
    end,
    {reply, Response, State};
do_handle_call({raw_stats, SubStat, Value, StatsFun, StatsFunState},
               _From, State) ->
    try mc_binary:quick_stats(State#state.sock, SubStat, Value, StatsFun,
                              StatsFunState) of
        Reply ->
            {reply, Reply, State}
    catch T:E ->
            {reply, {exception, {T, E}}, State}
    end;
do_handle_call({delete_vbuckets, VBuckets}, _From, #state{sock=Sock} = State) ->
    try
        {reply, mc_client_binary:delete_vbuckets(Sock, VBuckets), State}
    catch
        {error, _} = Err ->
            {compromised_reply, Err, State}
    end;
do_handle_call({delete_vbucket, VBucket}, _From, #state{sock=Sock} = State) ->
    case mc_client_binary:delete_vbucket(Sock, VBucket) of
        ok ->
            {reply, ok, State};
        {memcached_error, einval, _} ->
            ok = mc_client_binary:set_vbucket(Sock, VBucket,
                                              dead),
            Reply = mc_client_binary:delete_vbucket(Sock, VBucket),
            {reply, Reply, State}
    end;
do_handle_call({sync_delete_vbucket, VBucket}, _From, #state{sock=Sock} = State) ->
    ?log_info("sync-deleting vbucket ~p", [VBucket]),
    Reply = mc_client_binary:sync_delete_vbucket(Sock, VBucket),
    {reply, Reply, State};
do_handle_call({get_vbucket_details_stats, VBucket, Keys}, _From, State) ->
    Reply = get_vbucket_details(State#state.sock, VBucket, Keys),
    {reply, Reply, State};
do_handle_call(list_vbuckets, _From, State) ->
    Reply = mc_binary:quick_stats(
              State#state.sock, <<"vbucket">>,
              fun (<<"vb_", K/binary>>, V, Acc) ->
                      [{list_to_integer(binary_to_list(K)),
                        binary_to_existing_atom(V, latin1)} | Acc]
              end, []),
    {reply, Reply, State};
do_handle_call(flush, _From, State) ->
    Reply = mc_client_binary:flush(State#state.sock),
    {reply, Reply, State};

do_handle_call({delete, KeyFun, CollectionsUid, VBucket, Identity}, _From,
               State) ->
    handle_data_call(?DELETE, KeyFun, CollectionsUid, VBucket, false, Identity,
                     #mc_entry{}, State);

do_handle_call({set, KeyFun, CollectionsUid, VBucket, ValFun, Flags,
                Expiry, PreserveTTL, Identity}, _From, State) ->
    handle_data_call(?SET, KeyFun, CollectionsUid, VBucket, PreserveTTL,
                     Identity,
                     #mc_entry{data = ValFun(), flag = Flags, expire = Expiry},
                     State);

do_handle_call({add, KeyFun, CollectionsUid, VBucket, ValFun}, _From, State) ->
    handle_data_call(?ADD, KeyFun, CollectionsUid, VBucket,
                     #mc_entry{data = ValFun()}, State);

do_handle_call({get, KeyFun, CollectionsUid, VBucket, Identity}, _From,
               State) ->
    handle_data_call(?GET, KeyFun, CollectionsUid, VBucket, false, Identity,
                     #mc_entry{}, State);

do_handle_call({get_from_replica, KeyFun, CollectionsUid, VBucket}, _From,
               State) ->
    handle_data_call(?CMD_GET_REPLICA, KeyFun, CollectionsUid, VBucket, State);

do_handle_call({set_vbuckets, VBsToSet}, _From, #state{sock = Sock} = State) ->
    ToSet = [{VB, VBState, construct_vbucket_info_json(Options)}
             || {VB, VBState, Options} <- VBsToSet],
    try
        Reply = mc_client_binary:set_vbuckets(Sock, ToSet),
        Good = case Reply of
                   ok ->
                       ToSet;
                   {errors, BadVBs} ->
                       ?log_error("Failed to change following vbucket "
                                  "state ~n~p", [BadVBs]),
                       ToSet -- [Bad || {Bad, _ErrMsg} <- BadVBs]
               end,
        ?log_info("Changed vbucket state ~n~p", [Good]),
        [(catch master_activity_events:note_vbucket_state_change(
                  State#state.bucket, dist_manager:this_node(),
                  VBucket, VBState,
                  VBInfoJson)) || {VBucket, VBState, VBInfoJson} <- Good],
        {reply, Reply, State}
    catch
        {error, _} = Err ->
            %% We should not reuse this socket on these errors.
            ?log_error("Failed to change vbucket states: ~p~n~p",
                       [Err, ToSet]),
            {compromised_reply, Err, State}
    end;
do_handle_call({set_vbucket, VBucket, VBState, Options}, _From,
               #state{sock=Sock, bucket=BucketName} = State) ->
    VBInfoJson = construct_vbucket_info_json(Options),
    (catch master_activity_events:note_vbucket_state_change(
             BucketName, dist_manager:this_node(), VBucket, VBState,
             VBInfoJson)),
    Reply = mc_client_binary:set_vbucket(Sock, VBucket, VBState, VBInfoJson),
    case Reply of
        ok ->
            ?log_info("Changed bucket ~p vbucket ~p state to ~p",
                      [BucketName, VBucket, VBState]);
        _ ->
            ?log_error("Failed to change bucket ~p vbucket ~p state to ~p: ~p",
                       [BucketName, VBucket, VBState, Reply])
    end,
    {reply, Reply, State};
do_handle_call({get_dcp_docs_estimate, VBucketId, ConnName}, _From, State) ->
    {reply, mc_client_binary:get_dcp_docs_estimate(State#state.sock, VBucketId, ConnName), State};
do_handle_call({get_mass_dcp_docs_estimate, VBuckets}, _From, State) ->
    {reply, mc_client_binary:get_mass_dcp_docs_estimate(State#state.sock, VBuckets), State};
do_handle_call({get_random_key, CollectionsUid}, _From, State) ->
    CollectionsEnabled = proplists:get_bool(collections,
                                            State#state.worker_features),
    true = (CollectionsEnabled andalso is_integer(CollectionsUid)) orelse
               (CollectionsUid =:= undefined),
    RV = mc_client_binary:get_random_key(State#state.sock, CollectionsUid),
    {reply, RV, State};
do_handle_call({get_vbucket_high_seqno, VBucketId}, _From, State) ->
    StatName = <<"vb_", (iolist_to_binary(integer_to_list(VBucketId)))/binary, ":high_seqno">>,
    Res = mc_binary:quick_stats(
            State#state.sock, iolist_to_binary([<<"vbucket-seqno ">>, integer_to_list(VBucketId)]),
            fun (K, V, Acc) ->
                    case K of
                        StatName ->
                            list_to_integer(binary_to_list(V));
                        _ ->
                            Acc
                    end
            end,
            undefined),
    {reply, Res, State};
do_handle_call(get_all_vb_seqnos, _From, State = #state{sock = Sock}) ->
    {reply, mc_client_binary:get_all_vb_seqnos(Sock), State};

%% This is left in place to support backwards compat from nodes with version
%% lower than 7.6.
do_handle_call({get_keys, VBuckets, Params}, From, State) ->
    do_handle_call({get_keys, VBuckets, Params, undefined}, From, State);
do_handle_call({get_keys, VBuckets, Params, Identity}, _From,
    #state{worker_features = Features} = State) ->
    RV = mc_binary:get_keys(
        State#state.sock, Features, VBuckets, Params, ?GET_KEYS_TIMEOUT,
        Identity),

    case RV of
        {ok, _}  ->
            {reply, RV, State};
        {memcached_error, _} ->
            %% we take special care to leave the socket in the sane state in
            %% case of expected memcached errors (think rebalance)
            {reply, RV, State};
        {error, _} ->
            %% any other error might leave unread responses on the socket so
            %% we can't reuse it
            {compromised_reply, RV, State}
    end;

do_handle_call(pause_bucket_stub, _From, State) ->
    ?log_debug("Pause stub called"),
    {reply, ok, State};
do_handle_call(unpause_bucket_stub, _From, State) ->
    ?log_debug("UnPause stub called"),
    {reply, ok, State};

do_handle_call(_, _From, State) ->
    {reply, unhandled, State}.

handle_cast(start_completed, #state{start_time=Start,
                                    bucket=Bucket} = State) ->
    ale:info(?USER_LOGGER, "Bucket ~p loaded on node ~p in ~p seconds.",
             [Bucket, dist_manager:this_node(),
              timer:now_diff(os:timestamp(), Start) div 1000000]),
    gen_event:notify(buckets_events, {loaded, Bucket}),
    send_check_config_msg(State),
    BucketConfig = case ns_bucket:get_bucket(State#state.bucket) of
                       {ok, BC} -> BC;
                       not_present -> []
                   end,
    NewStatus = case proplists:get_value(type, BucketConfig, unknown) of
                    memcached ->
                        %% memcached buckets are warmed up automagically
                        gen_event:notify(buckets_events, {warmed, Bucket}),
                        warmed;
                    _ ->
                        connected
                end,
    {noreply, State#state{status=NewStatus, warmup_stats=[]}}.

issue_jwt() ->
    {ok, JWT} =
        jwt_issuer:issue("@fusion", [metakv2_access],
                         ?JWT_LIFETIME_MS div 1000),
    {_, Payload = #{<<"exp">> := Expires}} = jose_jwt:peek_payload(JWT),
    ?log_debug("Issue JWT ~p", [Payload]),
    {ok, JWT, Expires}.

handle_info({connect_done, WorkersCount, RV},
            #state{bucket = Bucket,
                   status = OldStatus,
                   bucket_uuid = BucketUUID} = State) ->
    gen_event:notify(buckets_events, {started, Bucket}),
    erlang:process_flag(trap_exit, true),
    Self = self(),
    case RV of
        {ok, Sock} ->
            {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
            {ok, JWT, Expires} =
                case ns_bucket:is_fusion(BucketConfig) of
                    true ->
                        issue_jwt();
                    false ->
                        {ok, undefined, undefined}
                end,
            try ensure_bucket(Sock, Bucket, false, JWT, BucketUUID) of
                ok ->
                    connecting = OldStatus,

                    ?log_info("Main ns_memcached connection established: ~p",
                              [RV]),

                    Self ! check_started,

                    InitialState = State#state{
                                     start_time = os:timestamp(),
                                     sock = Sock,
                                     status = init,
                                     jwt_expires = Expires
                                    },
                    WorkerPids = [proc_lib:spawn_link(erlang,
                                                      apply, [fun worker_init/2,
                                                      [Self, InitialState]])
                     || _ <- lists:seq(1, WorkersCount)],

                    chronicle_compat_events:subscribe(
                      fun (cluster_compat_version) ->
                              true;
                          (rest) ->
                              true;
                          ({node, Node, rest}) when Node =:= node() ->
                              true;
                          (Key) ->
                              case collections:key_match(Key) of
                                  {true, Bucket} ->
                                      true;
                                  _ ->
                                      false
                              end
                      end,
                      fun (Key) ->
                              ?log_debug(
                                 "Triggering config check due to event on "
                                 "key ~p", [Key]),
                              Self ! check_config_soon
                      end),
                    %% The bucket cluster map must be updated after it is
                    %% created. This was previously handled in ns_memcached_sup
                    %% when terse_bucket_info_uploader started up again after
                    %% ns_memcached restarted (in 7.2). It is no longer the
                    %% case that terse_bucket_info_uploader restarts (as it is
                    %% not supervised by ns_memcached_sup but by ns_bucket_sup)
                    %% so the refresh must be explicitly requested.
                    terse_bucket_info_uploader:refresh(Bucket),

                    {noreply, InitialState#state{worker_pids = WorkerPids}};
                {error, {bucket_create_error,
                         {memcached_error, key_eexists, _}}} ->
                    ?log_debug("ensure_bucket failed as bucket ~p has not "
                               "completed coming online", [Bucket]),
                    erlang:send_after(?CONNECT_DONE_RETRY_INTERVAL, Self,
                                      {connect_done, WorkersCount, RV}),
                    {noreply, State};
                {error, bucket_paused} ->
                    ?log_debug("Bucket is paused: ~p", [Bucket]),
                    {noreply, State#state{start_time = os:timestamp(),
                                          sock = Sock,
                                          status = paused}};
                Error ->
                    ?log_info("ensure_bucket failed: ~p", [Error]),
                    {stop, Error, State}
            catch
                exit:{shutdown, reconfig} ->
                    {stop, {shutdown, reconfig}, State#state{sock = Sock}}
            end;
        Error ->
            ?log_info("Failed to establish ns_memcached connection: ~p", [RV]),
            {stop, Error, State}
    end;
handle_info(check_started, #state{status=Status} = State)
  when Status =:= connected orelse Status =:= warmed ->
    %% XXX: doesn't appear this path is reachable... ensure that's the case
    %% and then remove this function.
    exit({shutdown, cant_happen}),
    send_check_started_msg(),
    {noreply, State};
handle_info(check_started,
            #state{bucket=Bucket, sock=Sock} = State) ->
    Stats = retrieve_warmup_stats(Sock),
    case has_started(Stats, Bucket) of
        true ->
            Pid = self(),
            proc_lib:spawn_link(
              fun () ->
                      memcached_passwords:sync(),
                      memcached_permissions:sync(),

                      gen_server:cast(Pid, start_completed),
                      %% we don't want exit signal in parent's message
                      %% box if everything went fine. Otherwise
                      %% ns_memcached would terminate itself (see
                      %% handle_info for EXIT message below)
                      erlang:unlink(Pid)
              end),
            {noreply, State};
        false ->
            {ok, S} = Stats,
            send_check_started_msg(),
            {noreply, State#state{warmup_stats = S}}
    end;
handle_info(check_config_soon, #state{check_in_progress = true} = State) ->
    {noreply, State#state{next_check_after = 0}};
handle_info(check_config, #state{check_in_progress = true} = State) ->
    {noreply, State};
handle_info(Message, #state{control_queue = undefined, status = Status,
                            bucket = Bucket, check_in_progress = false} = State)
  when Message =:= check_config_soon orelse Message =:= check_config ->
    misc:flush(check_config_soon),
    misc:flush(check_config),
    ?log_debug("Ignoring any config checks: Bucket ~p, Status: ~p",
               [Bucket, Status]),
    {noreply, State};
handle_info(Message, #state{worker_features = WF, control_queue = Q,
                            bucket = Bucket, check_in_progress = false,
                            jwt_expires = JWTExpires,
                            bucket_uuid = BucketUUID} = State)
  when Message =:= check_config_soon orelse Message =:= check_config ->
    misc:flush(check_config_soon),
    misc:flush(check_config),

    case get_worker_features() of
        WF ->
            Self = self(),
            Now = erlang:system_time(second),

            {ok, JWT, NewExpires} =
                case JWTExpires of
                    undefined ->
                        {ok, undefined, undefined};
                    _ ->
                        case (JWTExpires - Now) * 1000 =<
                            ?JWT_RENEWAL_WINDOW_MS of
                            true ->
                                issue_jwt();
                            false ->
                                {ok, undefined, JWTExpires}
                        end
                end,

            work_queue:submit_work(
              Q,
              fun () ->
                      perform_very_long_call_with_timing(
                        Bucket, ensure_bucket,
                        ensure_bucket(_, Bucket, true, JWT, BucketUUID),
                        [json]),
                      Self ! complete_check
              end),
            {noreply, State#state{check_in_progress = true,
                                  next_check_after = ?CHECK_INTERVAL,
                                  jwt_expires = NewExpires}};
        OldWF ->
            ?log_info("Restarting due to features change from ~p to ~p",
                      [OldWF, WF]),
            {stop, {shutdown, feature_mismatch}, State}
    end;
handle_info(complete_check, State = #state{check_in_progress = true}) ->
    send_check_config_msg(State),
    {noreply, State#state{check_in_progress = false}};
handle_info({'EXIT', _, Reason} = Msg, State) ->
    ?log_debug("Got ~p. Exiting.", [Msg]),
    {stop, Reason, State};
handle_info(Msg, State) ->
    ?log_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.


terminate(_Reason, #state{sock = still_connecting}) ->
    ?log_debug("Dying when socket is not yet connected");
terminate(Reason, #state{bucket=Bucket, sock=Sock, bucket_uuid=BucketUUID}) ->
    try
        do_terminate(Reason, Bucket, Sock, BucketUUID)
    after
        gen_event:notify(buckets_events, {stopped, Bucket}),
        ?log_debug("Terminated.")
    end.

do_terminate(Reason, Bucket, Sock, BucketUUID) ->
    Config = ns_config:get(),
    BucketConfigs = ns_bucket:get_buckets(),
    NoBucket = not lists:keymember(Bucket, 1, BucketConfigs),
    ThisNode = dist_manager:this_node(),
    NodeDying = (ns_config:search(Config, i_am_a_dead_man) =/= false
                 orelse
                 not lists:member(Bucket,
                                  ns_bucket:node_bucket_names(ThisNode,
                                                              BucketConfigs))),

    Deleting = NoBucket orelse NodeDying,
    Reconfig = (Reason =:= {shutdown, reconfig}),

    case Deleting orelse Reconfig of
        true ->
            %% We no longer use 'for deletion' as this was scarey for some
            %% users as they thought their data was being deleted. Whereas
            %% the bucket is actually just not provisioned to serve data.
            ale:info(?USER_LOGGER, "Shutting down bucket ~p on ~p ~s",
                     [Bucket, ThisNode, if
                                            Reconfig -> "for reconfiguration";
                                            Deleting -> ""
                                        end]),

            %% force = true means that that ep_engine will not try to flush
            %% outstanding mutations to disk before deleting the bucket. So we
            %% need to set it to false when we need to delete and recreate the
            %% bucket just because some setting changed.
            Force = not Reconfig,

            %% files are deleted here only when bucket is deleted; in all the
            %% other cases (like node removal or failover) we leave them on
            %% the file system and let others decide when they should be
            %% deleted
            DeleteData = NoBucket,

            delete_bucket(Sock, Bucket, BucketUUID, Force, DeleteData);
        false ->
            %% if this is system shutdown bucket engine now can reliably
            %% delete all buckets as part of shutdown. if this is supervisor
            %% crash, we're fine too
            ale:info(?USER_LOGGER,
                     "Control connection to memcached on ~p disconnected. "
                     "Check logs for details.", [ThisNode])
    end.

delete_bucket(Sock, Bucket, BucketUUID, Force, DeleteData) ->
    ?log_info("Deleting bucket ~p from memcached (force = ~p)",
              [Bucket, Force]),

    try
        case mc_client_binary:delete_bucket(Sock, Bucket, [{force, Force}]) of
            ok ->
                ok;
            {memcached_error, key_enoent, undefined} ->
                ?log_warning("Bucket ~p appears to be already deleted",
                             [Bucket]);
            Error ->
                ?log_error("Failed to delete bucket ~p: ~p", [Bucket, Error])
        end
    catch
        T:E ->
            ?log_error("Failed to delete bucket ~p: ~p", [Bucket, {T, E}])
    after
        case DeleteData of
            true ->
                ?log_debug("Proceeding into vbuckets dbs deletions"),
                try
                    ns_couchdb_api:delete_databases_and_files_uuid(BucketUUID)
                catch
                    Class:Exception ->
                        ?log_error("Failed to delete couchdb bucket ~p: ~p",
                                   [Bucket, {Class, Exception}])
                end;
            false ->
                ok
        end
    end.

delete_bucket(Bucket, Opts) ->
    ns_memcached_sockets_pool:executing_on_socket(
      mc_client_binary:delete_bucket(_, Bucket, Opts)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%
%% API
%%
perform_very_long_call_with_timing(Bucket, Name, Fun, Opts) ->
    perform_very_long_call(
      fun(Sock) ->
              StartTS = os:timestamp(),
              ok = Fun(Sock),
              Diff = timer:now_diff(os:timestamp(), StartTS),
              if
                  Diff > ?SLOW_CALL_THRESHOLD_MICROS ->
                      ?log_debug("~p took too long: ~p us", [Name, Diff]);
                  true ->
                      ok
              end,
              {reply, ok}
      end, Bucket, Opts).

-spec active_buckets() -> [bucket_name()].
active_buckets() ->
    [Bucket || ?MODULE_STRING "-" ++ Bucket <-
                   [atom_to_list(Name) || Name <- registered()]].

-spec status(node(), bucket_name(), pos_integer() | infinity) ->
    status() | no_status.
status(Node, Bucket, Timeout) ->
    try
        do_call({server(Bucket), Node}, Bucket, status, Timeout)
    catch
        T:E:Stack ->
            ?log_debug("Failure to get status for bucket ~p on ~p.~n~p",
                       [Bucket, Node, {T, E, Stack}]),
            no_status
    end.

-spec mark_warmed(bucket_name(), undefined | data_ingress_status()) -> any().
mark_warmed(Bucket, DataIngress) ->
    gen_server:call(server(Bucket), {mark_warmed, DataIngress},
                    ?MARK_WARMED_TIMEOUT).

-spec get_mark_warmed_timeout() -> pos_integer().
get_mark_warmed_timeout() ->
    ?MARK_WARMED_TIMEOUT.

-spec bucket_statuses() -> [atom()].
bucket_statuses() ->
    bucket_statuses(?STATUSES_TIMEOUT).

-spec bucket_statuses(pos_integer() | integer) -> [atom()].
bucket_statuses(Timeout) ->
    RVs = misc:parallel_map(
        fun (Bucket) ->
            {Bucket, status(dist_manager:this_node(), Bucket,
                Timeout)}
        end, active_buckets(), infinity),
    RVs.

%% @doc Send flush command to specified bucket
-spec flush(bucket_name()) -> ok.
flush(Bucket) ->
    do_call({server(Bucket), dist_manager:this_node()}, Bucket, flush,
            ?TIMEOUT_VERY_HEAVY).


%% @doc send an add command to memcached instance
-spec add(bucket_name(), binary(), integer(), integer(), binary()) ->
                 {ok, #mc_header{}, #mc_entry{}, any()}.
add(Bucket, Key, CollectionsUid, VBucket, Value) ->
    do_call(server(Bucket), Bucket,
            {add, fun () -> Key end, CollectionsUid, VBucket,
             fun () -> Value end}, ?TIMEOUT_HEAVY).

%% @doc send get command to memcached instance
-spec get(bucket_name(), binary(), undefined | integer(), integer(),
          undefined | rbac_identity()) ->
                 {ok, #mc_header{}, #mc_entry{}, any()}.
get(Bucket, Key, CollectionsUid, VBucket, Identity) ->
    do_call(server(Bucket), Bucket,
            {get, fun () -> Key end, CollectionsUid, VBucket, Identity},
            ?TIMEOUT_HEAVY).

%% @doc send get_from_replica command to memcached instance. for testing only
-spec get_from_replica(bucket_name(), binary(), integer(), integer()) ->
          {ok, #mc_header{}, #mc_entry{}, any()}.
get_from_replica(Bucket, Key, CollectionsUid, VBucket) ->
    do_call(server(Bucket), Bucket,
            {get_from_replica, fun () -> Key end, CollectionsUid, VBucket},
            ?TIMEOUT_HEAVY).

%% @doc send an get metadata command to memcached
-spec get_meta(bucket_name(), binary(), undefined | integer(), integer(),
               undefined | rbac_identity()) ->
                      {ok, rev(), integer(), integer()}
                          | {memcached_error, key_enoent, integer()}
                          | mc_error().
get_meta(Bucket, Key, CollectionsUid, VBucket, Identity) ->
    EncodedKey = mc_binary:maybe_encode_uid_in_key(
                   CollectionsUid =/= undefined, CollectionsUid, Key),
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:get_meta(Sock, EncodedKey,
                                                VBucket, Identity)}
      end, Bucket, [collections || CollectionsUid =/= undefined]).

%% @doc get xattributes for specified key
-spec get_xattrs(bucket_name(), binary(), undefined | integer(),
                 integer(), [atom()], undefined | rbac_identity()) ->
                        {ok, integer(), [{binary(), term()}]}
                            | {memcached_error, key_enoent, integer()}
                            | mc_error().
get_xattrs(Bucket, Key, CollectionsUid, VBucket, Permissions, Identity) ->
    EncodedKey = mc_binary:maybe_encode_uid_in_key(
                   CollectionsUid =/= undefined, CollectionsUid, Key),
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_binary:get_xattrs(Sock, EncodedKey, VBucket,
                                           Permissions, Identity)}
      end, Bucket, [xattr | [collections || CollectionsUid =/= undefined]]).

%% @doc send a delete command to memcached instance
-spec delete(bucket_name(), binary(), undefined | integer(), integer(),
             undefined | rbac_identity()) ->
                    {ok, #mc_header{}, #mc_entry{}, any()} |
                    {memcached_error, any(), any()}.
delete(Bucket, Key, CollectionsUid, VBucket, Identity) ->
    do_call(server(Bucket), Bucket,
            {delete, fun () -> Key end, CollectionsUid, VBucket, Identity},
            ?TIMEOUT_HEAVY).

%% @doc send a set command to memcached instance
-spec set(bucket_name(), binary(), undefined | integer(), integer(),
          binary(), integer(), integer(), boolean(),
          undefined | rbac_identity()) ->
          {ok, #mc_header{}, #mc_entry{}, any()} |
          {memcached_error, any(), any()}.
set(Bucket, Key, CollectionsUid, VBucket, Value, Flags, Expiry, PreserveTTL,
    Identity) ->
    do_call(server(Bucket), Bucket,
            {set, fun () -> Key end, CollectionsUid, VBucket,
             fun () -> Value end, Flags, Expiry, PreserveTTL, Identity},
            ?TIMEOUT_HEAVY).

-spec update_with_rev(Bucket::bucket_name(), VBucket::vbucket_id(),
                      Id::binary(), Value::binary() | undefined, Rev :: rev(),
                      Deleted::boolean(), LocalCAS::non_neg_integer()) ->
                             {ok, #mc_header{}, #mc_entry{}} |
                             {memcached_error, atom(), binary()}.
update_with_rev(Bucket, VBucket, Id, Value, Rev, Deleted, LocalCAS) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:update_with_rev(
                        Sock, VBucket, Id, Value, Rev, Deleted, LocalCAS)}
      end, Bucket).

%% @doc Delete a vbucket. Will set the vbucket to dead state if it
%% isn't already, blocking until it successfully does so.
-spec delete_vbucket(bucket_name(), vbucket_id()) ->
                            ok | mc_error().
delete_vbucket(Bucket, VBucket) ->
    do_call(server(Bucket), Bucket,
            {delete_vbucket, VBucket}, ?TIMEOUT_VERY_HEAVY).

-spec delete_vbuckets(bucket_name(), [vbucket_id()]) ->
    ok | {errors, [{vbucket_id(), mc_error()}]} | {error, any()}.
delete_vbuckets(Bucket, VBuckets) ->
    case VBuckets of
        [] ->
            ok;
        _ ->
            do_call(server(Bucket), Bucket, {delete_vbuckets, VBuckets},
                    ?TIMEOUT_VERY_HEAVY)
    end.

-spec sync_delete_vbucket(bucket_name(), vbucket_id()) ->
                                 ok | mc_error().
sync_delete_vbucket(Bucket, VBucket) ->
    do_call(server(Bucket), Bucket, {sync_delete_vbucket, VBucket},
            infinity).

-spec get_single_vbucket_details_stats(bucket_name(), vbucket_id(),
                                       [nonempty_string()]) ->
                                              {ok, [{nonempty_string(),
                                                     nonempty_string()}]} |
                                              mc_error().
get_single_vbucket_details_stats(Bucket, VBucket, ReqdKeys) ->
    case get_vbucket_details_stats(Bucket, VBucket, ReqdKeys) of
        {ok, Dict} ->
            case dict:find(VBucket, Dict) of
                {ok, Val} ->
                    {ok, Val};
                _ ->
                    %% In case keys aren't present in the memcached return
                    %% value.
                    {ok, []}
            end;
        Err ->
            Err
    end.

-spec get_vbucket_details_stats(bucket_name(), [nonempty_string()]) ->
                                       {ok, dict:dict()} | mc_error().
get_vbucket_details_stats(Bucket, ReqdKeys) ->
    get_vbucket_details_stats(Bucket, all, ReqdKeys).

-spec get_vbucket_details_stats(bucket_name(), all | vbucket_id(),
                                [nonempty_string()]) ->
                                       {ok, dict:dict()} | mc_error().
get_vbucket_details_stats(Bucket, VBucket, ReqdKeys) ->
    do_call(server(Bucket), Bucket,
            {get_vbucket_details_stats, VBucket, ReqdKeys}, ?TIMEOUT).

-spec host_ports(node(), any()) ->
                        {nonempty_string(),
                         pos_integer() | undefined,
                         pos_integer() | undefined}.
host_ports(Node, Config) ->
    [Port, SslPort] =
        [begin
             DefaultPort = service_ports:get_port(Defaultkey, Config, Node),
             ns_config:search_node_prop(Node, Config, memcached,
                                        DedicatedKey, DefaultPort)
         end || {Defaultkey, DedicatedKey} <-
                    [{memcached_port, dedicated_port},
                     {memcached_ssl_port, dedicated_ssl_port}]],
    Host = misc:extract_node_address(Node),
    {Host, Port, SslPort}.

-spec host_ports(node()) ->
                        {nonempty_string(),
                         pos_integer() | undefined,
                         pos_integer() | undefined}.
host_ports(Node) ->
    host_ports(Node, ns_config:get()).

-spec list_vbuckets(bucket_name()) ->
                           {ok, [{vbucket_id(), vbucket_state()}]} | mc_error().
list_vbuckets(Bucket) ->
    list_vbuckets(dist_manager:this_node(), Bucket).


-spec list_vbuckets(node(), bucket_name()) ->
                           {ok, [{vbucket_id(), vbucket_state()}]} | mc_error().
list_vbuckets(Node, Bucket) ->
    do_call({server(Bucket), Node}, Bucket, list_vbuckets, ?TIMEOUT).

-spec local_connected_and_list_vbuckets(bucket_name()) -> warming_up | {ok, [{vbucket_id(), vbucket_state()}]}.
local_connected_and_list_vbuckets(Bucket) ->
    do_call(server(Bucket), Bucket, connected_and_list_vbuckets, ?TIMEOUT).

-spec local_connected_and_list_vbucket_details(bucket_name(), [string()]) ->
                                                      warming_up |
                                                      {ok, dict:dict()}.
local_connected_and_list_vbucket_details(Bucket, Keys) ->
    do_call(server(Bucket), Bucket, {connected_and_list_vbucket_details, Keys},
            ?TIMEOUT).


set_vbucket(Bucket, VBucket, VBState) ->
    set_vbucket(Bucket, VBucket, VBState, []).

-spec set_vbucket(bucket_name(), vbucket_id(), vbucket_state(),
                  [set_vbucket_option()]) -> ok | mc_error().
set_vbucket(Bucket, VBucket, VBState, Options) ->
    do_call(server(Bucket), Bucket, {set_vbucket, VBucket, VBState, Options},
            ?TIMEOUT_HEAVY).

-spec set_vbuckets(bucket_name(),
                   [{vbucket_id(), vbucket_state(), [set_vbucket_option()]}]) ->
          ok | {errors, [{{vbucket_id(), vbucket_state(),
                           [set_vbucket_option()]},
                          mc_error()}]} |
          {error, any()}.
set_vbuckets(Bucket, ToSet) ->
    case ToSet of
        [] ->
            ok;
        _ ->
            do_call(server(Bucket), Bucket,
                    {set_vbuckets, ToSet}, ?TIMEOUT_VERY_HEAVY)
    end.

-spec pause_bucket(bucket_name()) -> ok | {error, any()}.
pause_bucket(Bucket) ->
    ok = gen_server:call(server(Bucket), prepare_pause_bucket,
                         ?TIMEOUT_VERY_HEAVY),
    Rv = perform_very_long_call(
           fun(Sock) ->
                   Reply = mc_client_binary:pause_bucket(Sock, Bucket),
                   {reply, Reply}
           end
          ),
    case Rv of
        ok ->
            gen_server:call(server(Bucket), complete_pause_bucket,
                            ?TIMEOUT_VERY_HEAVY);
        Error ->
            ?log_error("Pausing bucket ~p failed: ~p", [Bucket, Error]),
            failure
    end.

-spec unpause_bucket(bucket_name()) -> ok | {error, any()}.
unpause_bucket(Bucket) ->
    gen_server:call(server(Bucket), {unpause_bucket, Bucket},
                    ?TIMEOUT_VERY_HEAVY).

fetch_stats(Sock, Key) ->
    mc_binary:quick_stats(Sock, Key, fun mc_binary:quick_stats_append/3, []).

-spec stats(bucket_name(), binary() | string()) ->
                   {ok, [{binary(), binary()}]} | mc_error().
stats(Bucket, Key) ->
    perform_very_long_call(?cut({reply, fetch_stats(_, Key)}), Bucket).

-spec warmup_stats(bucket_name()) -> [{binary(), binary()}].
warmup_stats(Bucket) ->
    do_call(server(Bucket), Bucket, warmup_stats, ?TIMEOUT).

-spec raw_stats(node(), bucket_name(), binary(),
                fun ((StatName, StatValue, Acc) -> Acc),
                Acc) ->
          {ok, any()} | {exception, any()} | {error, any()}
              when StatName :: binary(),
                   StatValue :: binary(),
                   Acc :: any().
raw_stats(Node, Bucket, SubStats, Fn, FnState) ->
    raw_stats(Node, Bucket, SubStats, undefined, Fn, FnState).

-spec raw_stats(node(), bucket_name(), binary(), binary() | undefined,
                fun ((StatName, StatValue, Acc) -> Acc),
                Acc) ->
          {ok, any()} | {exception, any()} | {error, any()}
              when StatName :: binary(),
                   StatValue :: binary(),
                   Acc :: any().
raw_stats(Node, Bucket, SubStats, Value, Fn, FnState) ->
    do_call({server(Bucket), Node}, Bucket,
            {raw_stats, SubStats, Value, Fn, FnState}, ?TIMEOUT).

-spec get_vbucket_high_seqno(bucket_name(), vbucket_id()) ->
                                    {ok, {undefined | seq_no()}}.
get_vbucket_high_seqno(Bucket, VBucketId) ->
    do_call(server(Bucket), Bucket,
            {get_vbucket_high_seqno, VBucketId}, ?TIMEOUT).

-spec get_all_vb_seqnos(bucket_name()) ->
          {ok, [{vbucket_id(), seq_no()}]} | mc_error().
get_all_vb_seqnos(Bucket) ->
    do_call(server(Bucket), Bucket,
            get_all_vb_seqnos, ?TIMEOUT).

-spec get_seqno_stats(ext_bucket_name(), vbucket_id() | undefined) ->
                             [{binary(), binary()}].
get_seqno_stats(Bucket, VBucket) ->
    Key = case VBucket of
              undefined ->
                  <<"vbucket-seqno">>;
              _ ->
                  list_to_binary(io_lib:format("vbucket-seqno ~B", [VBucket]))
          end,
    perform_very_long_call(
      fun (Sock) ->
              {ok, Stats} =
                  mc_binary:quick_stats(
                    Sock,
                    Key,
                    fun (K, V, Acc) ->
                            [{K, V} | Acc]
                    end, []),
              {reply, Stats}
      end, Bucket).

%%
%% Internal functions
%%
connect(AgentName, Options) ->
    Retries = proplists:get_value(retries, Options, ?CONNECTION_ATTEMPTS),
    connect(AgentName, Options, Retries).

connect(AgentName, Options, Tries) ->
    try
        do_connect(AgentName, Options)
    catch
        E:R ->
            case Tries of
                1 ->
                    ?log_warning("Unable to connect: ~p.", [{E, R}]),
                    {error, couldnt_connect_to_memcached};
                _ ->
                    ?log_warning("Unable to connect: ~p, retrying.", [{E, R}]),
                    timer:sleep(1000), % Avoid reconnecting too fast.
                    connect(AgentName, Options, Tries - 1)
            end
    end.

do_connect(AgentName, Options) ->
    Config = ns_config:get(),
    Port = service_ports:get_port(memcached_dedicated_port, Config),
    User = ns_config:search_node_prop(Config, memcached, admin_user),
    Pass = ns_config_auth:get_password(node(), Config, special),
    AFamilies = proplists:get_value(try_afamily, Options,
                                    [misc:get_net_family()]),
    HelloFeatures = proplists:delete(try_afamily, Options),
    Timeout = proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT),
    {ok, Sock} = lists:foldl(
                   fun (_AFamily, {ok, Socket}) ->
                           {ok, Socket};
                       (AFamily, Acc) ->
                           RV = gen_tcp:connect(misc:localhost(AFamily, []),
                                                Port,
                                                [AFamily,
                                                 binary,
                                                 {packet, 0},
                                                 {active, false},
                                                 {recbuf, ?RECBUF},
                                                 {sndbuf, ?SNDBUF}],
                                                Timeout),
                           case RV of
                               {ok, S} -> {ok, S};
                               _ -> [{AFamily, RV} | Acc]
                           end
                   end, [], AFamilies),

    try
        case mc_client_binary:auth(Sock, {<<"PLAIN">>,
                                          {list_to_binary(User),
                                           list_to_binary(Pass)}}) of
            ok -> ok;
            Err ->
                ?log_debug("Login failed for ~s with provided password"
                           " ~p", [User, ns_config_log:sanitize_value(Pass)]),
                error({auth_failure, Err})
        end,
        Features = mc_client_binary:hello_features(HelloFeatures),
        {ok, Negotiated} = mc_client_binary:hello(Sock, AgentName, Features),
        Failed = Features -- Negotiated,
        Failed == [] orelse error({feature_negotiation_failed, Failed}),
        {ok, Sock}
    catch
        T:E ->
            gen_tcp:close(Sock),
            throw({T, E})
    end.

ensure_bucket(Sock, Bucket, BucketSelected, JWT, BucketUUID) ->
    %% This testpoint simulates the case where the bucket is not selectable
    %% (returns enoent) but does exist so cannot be created.
    case simulate_slow_bucket_operation(Bucket, slow_bucket_creation,
                                        ?CONNECT_DONE_RETRY_INTERVAL) of
        true ->
            {error, {bucket_create_error,
                     {memcached_error, key_eexists, not_used}}};
        false ->
            case get_bucket_state(Bucket) of
                {ok, <<"paused">>} ->
                    {error, bucket_paused};
                _ ->
                    ensure_bucket_inner(Sock, Bucket, BucketSelected, JWT,
                                        BucketUUID)
            end
    end.

ensure_bucket_inner(Sock, Bucket, BucketSelected, JWT, BucketUUID) ->
    %% Only catch exceptions when getting the bucket config. Once we're
    %% past that point and into the guts of this function there is code
    %% that may exit with reason {shutdown, reconfig} and that exit should
    %% not be caught. The reason is the changing of bucket parameters may
    %% require a bucket deletion/recreation which happens as a result of
    %% the exit.
    try memcached_bucket_config:get(Bucket) of
        {error, not_present} ->
            case BucketSelected of
                true ->
                    %% Bucket disappeared from under us...just swallow the
                    %% error.
                    ?log_debug("Bucket ~p not found during ensure_bucket",
                               [Bucket]),
                    ok;
                false ->
                    %% We were trying to setup handling of the bucket.
                    not_present
            end;
        BConf ->
            case do_ensure_bucket(Sock, Bucket, BConf, BucketSelected, JWT,
                                  BucketUUID) of
                ok ->
                    memcached_bucket_config:ensure_collections(Sock, BConf);
                Error ->
                    Error
            end
    catch
        E:R:S ->
            ?log_error("Unable to get config for bucket ~p: ~p",
                       [Bucket, {E, R, S}]),
            {E, R}
    end.

do_ensure_bucket(Sock, Bucket, BConf, true, JWT, _BucketUUID) ->
    ensure_selected_bucket(Sock, Bucket, BConf, JWT);
do_ensure_bucket(Sock, Bucket, BConf, false, JWT, BucketUUID) ->
    case select_and_ensure_bucket(Sock, Bucket, BConf, JWT) of
        ok ->
            ok;
        {memcached_error, key_enoent, _} ->
            DBSubDir = ns_storage_conf:this_node_bucket_dbdir(BucketUUID),
            ok = filelib:ensure_path(DBSubDir),

            %% Note: cb_cluster_secrets should have deks created by this moment
            %% (we do sync with cb_cluster_secret before start_link for that).
            %% Also note that there is no race between the process of creating
            %% a bucket with deks (this process), and dek removal by
            %% cb_cluster_secrets because cb_cluster_secrets can only remove
            %% a dek if encryption-key-ids stat returns a list that doesn't
            %% include the dek. But since the bucket is not created yet, that
            %% stat request should never succeed, which makes removal
            %% impossible.
            {ok, DS} = cb_crypto:fetch_deks_snapshot({bucketDek, BucketUUID}),

            ok = write_bucket_metadata(Bucket, BucketUUID, DS),

            {ActiveDek, Deks} = cb_crypto:get_all_deks(DS),
            {Engine, ConfigString, ConfigStringForLogging} =
                memcached_bucket_config:start_params(BConf, ActiveDek, Deks,
                                                     JWT),

            BucketConfig = memcached_bucket_config:get_bucket_config(BConf),
            Timeout = case ns_bucket:node_kv_backend_type(BucketConfig) of
                          magma ->
                              ?MAGMA_CREATION_TIMEOUT;
                          _ ->
                              %% Use whatever the default value is
                              undefined
                      end,
            case mc_client_binary:create_bucket(Sock, Bucket, Engine,
                                                ConfigString, Timeout) of
                ok ->
                    ?log_info("Created bucket ~p with config string ~s",
                              [Bucket, ConfigStringForLogging]),
                    ok = mc_client_binary:select_bucket(Sock, Bucket);
                Error ->
                    {error, {bucket_create_error, Error}}
            end;
        Other ->
            Other
    end.

select_and_ensure_bucket(Sock, Bucket, BConf, JWT) ->
    case mc_client_binary:select_bucket(Sock, Bucket) of
        ok ->
            case ensure_selected_bucket(Sock, Bucket, BConf, JWT) of
                ok ->
                    ok;
                {error, config_only_bucket} ->
                    %% Have to deselect the bucket otherwise the only thing
                    %% kv allows is get_cluster_config.
                    ok = mc_client_binary:deselect_bucket(Sock),
                    {memcached_error, key_enoent, config_only_bucket}
            end;
        {memcached_error, key_enoent, _} = Err ->
            Err;
        Error ->
            {error, {bucket_select_error, Error}}
    end.

ensure_selected_bucket(Sock, Bucket, BConf, JWT) ->
    case memcached_bucket_config:ensure(Sock, BConf, JWT) of
        restart ->
            ale:info(
              ?USER_LOGGER,
              "Restarting bucket ~p due to configuration change",
              [Bucket]),
            exit({shutdown, reconfig});
        {error, _} = Error ->
            Error;
        ok ->
            ok
    end.

server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

retrieve_warmup_stats(Sock) ->
    mc_client_binary:stats(Sock, <<"warmup">>, fun (K, V, Acc) -> [{K, V}|Acc] end, []).

simulate_slow_bucket_operation(Bucket, TestConditionName, Interval) ->
    TestCondition = {TestConditionName, Bucket},
    case testconditions:get(TestCondition) of
        false ->
            false;
        0 ->
            false;
        Delay ->
            NewDelay = case Delay =< Interval of
                           true ->
                               0;
                           _ ->
                               Delay - Interval
                       end,
            ?log_debug("Simulating slow operation (~p) for bucket ~p. "
                       "Pending delay ~p seconds",
                       [TestConditionName, Bucket, Delay/1000]),
            testconditions:set(TestCondition, NewDelay),
            true
    end.

has_started({memcached_error, key_enoent, _}, _) ->
    %% this is memcached bucket, warmup is done :)
    true;
has_started(Stats, Bucket) ->
    case simulate_slow_bucket_operation(Bucket, ep_slow_bucket_warmup,
                                       ?CHECK_WARMUP_INTERVAL) of
        false ->
            has_started_inner(Stats);
        true ->
            false
    end.

has_started_inner({ok, WarmupStats}) ->
    case lists:keyfind(<<"ep_warmup_thread">>, 1, WarmupStats) of
        {_, <<"complete">>} ->
            true;
        {_, V} when is_binary(V) ->
            false
    end.

do_call(Server, Bucket, Msg, Timeout) ->
    StartTS = os:timestamp(),
    try
        gen_server:call(Server, Msg, Timeout)
    after
        try
            EndTS = os:timestamp(),
            Diff = timer:now_diff(EndTS, StartTS),
            ns_server_stats:notify_histogram(
              {<<"memcached_e2e_call_time">>, [{bucket, Bucket}]},
              Diff div 1000)
        catch T:E:S ->
                ?log_debug("failed to measure ns_memcached call:~n~p",
                           [{T, E, S}])
        end
    end.

-spec disable_traffic(bucket_name(), non_neg_integer() | infinity) -> ok | bad_status | mc_error().
disable_traffic(Bucket, Timeout) ->
    gen_server:call(server(Bucket), disable_traffic, Timeout).

-spec set_data_ingress(bucket_name(), data_ingress_status()) -> ok | mc_error().
set_data_ingress(Bucket, Status) ->
    do_call(server(Bucket), Bucket, {set_bucket_data_ingress, Status},
            ?TIMEOUT).

-spec wait_for_seqno_persistence(bucket_name(), vbucket_id(), seq_no()) -> ok | mc_error().
wait_for_seqno_persistence(Bucket, VBucketId, SeqNo) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:wait_for_seqno_persistence(Sock, VBucketId, SeqNo)}
      end, Bucket).

-spec compact_vbucket(bucket_name(), vbucket_id(),
                      {integer(), integer(), boolean(), [cb_deks:dek_id()]}) ->
                             ok | mc_error().
compact_vbucket(Bucket, VBucket, {PurgeBeforeTS, PurgeBeforeSeqNo, DropDeletes,
                                  ObsoleteKeyIds}) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:compact_vbucket(Sock, VBucket,
                                                       PurgeBeforeTS,
                                                       PurgeBeforeSeqNo,
                                                       DropDeletes,
                                                       ObsoleteKeyIds)}
      end, Bucket, [json]).


-spec get_dcp_docs_estimate(bucket_name(), vbucket_id(), string()) ->
                                   {ok, {non_neg_integer(), non_neg_integer(), binary()}}.
get_dcp_docs_estimate(Bucket, VBucketId, ConnName) ->
    do_call(server(Bucket), Bucket,
            {get_dcp_docs_estimate, VBucketId, ConnName}, ?TIMEOUT).

-spec get_mass_dcp_docs_estimate(bucket_name(), [vbucket_id()]) ->
                                        {ok, [{non_neg_integer(), non_neg_integer(), binary()}]}.
get_mass_dcp_docs_estimate(Bucket, VBuckets) ->
    do_call(server(Bucket), Bucket,
            {get_mass_dcp_docs_estimate, VBuckets}, ?TIMEOUT_VERY_HEAVY).

%% The function might be rpc'ed beginning from 6.5
get_random_key(Bucket) ->
    get_random_key(Bucket, undefined).

get_random_key(Bucket, CollectionsUid) ->
    do_call(server(Bucket), Bucket, {get_random_key, CollectionsUid}, ?TIMEOUT).

get_ep_startup_time_for_xdcr(Bucket) ->
    perform_very_long_call(
      fun (Sock) ->
              {ok, StartupTime} =
                  mc_binary:quick_stats(
                    Sock, <<>>,
                    fun (K, V, Acc) ->
                            case K =:= <<"ep_startup_time">> of
                                true -> V;
                                _ -> Acc
                            end
                    end, undefined),
              false = StartupTime =:= undefined,
              {reply, StartupTime}
      end, Bucket).

perform_checkpoint_commit_for_xdcr(Bucket, VBucketId, Timeout) ->
    perform_very_long_call(fun (Sock) -> do_perform_checkpoint_commit_for_xdcr(Sock, VBucketId, Timeout) end, Bucket).

do_perform_checkpoint_commit_for_xdcr(Sock, VBucketId, Timeout) ->
    case Timeout of
        infinity -> ok;
        _ -> timer:exit_after(Timeout, timeout)
    end,
    StatsKey = iolist_to_binary(io_lib:format("vbucket-seqno ~B", [VBucketId])),
    SeqnoKey = iolist_to_binary(io_lib:format("vb_~B:high_seqno", [VBucketId])),
    {ok, Seqno} = mc_binary:quick_stats(Sock, StatsKey,
                                        fun (K, V, Acc) ->
                                                case K =:= SeqnoKey of
                                                    true -> list_to_integer(binary_to_list(V));
                                                    _ -> Acc
                                                end
                                        end, []),
    case is_integer(Seqno) of
        true ->
            do_perform_checkpoint_commit_for_xdcr_loop(Sock, VBucketId, Seqno);
        _ ->
            {reply, {memcached_error, not_my_vbucket}}
    end.

do_perform_checkpoint_commit_for_xdcr_loop(Sock, VBucketId, WaitedSeqno) ->
    case mc_client_binary:wait_for_seqno_persistence(Sock,
                                                     VBucketId,
                                                     WaitedSeqno) of
        ok -> {reply, ok};
        {memcached_error, etmpfail, _} ->
            do_perform_checkpoint_commit_for_xdcr_loop(Sock, VBucketId, WaitedSeqno);
        {memcached_error, OtherError, _} ->
            {reply, {memcached_error, OtherError}}
    end.

get_keys(Bucket, NodeVBuckets, Params, Identity) ->
    try
        {ok, do_get_keys(Bucket, NodeVBuckets, Params, Identity)}
    catch
        exit:timeout ->
            {error, timeout}
    end.

do_get_keys_call(Bucket, Node, VBuckets, Params, Identity) ->
    GetKeys = case Identity =:= undefined andalso
                  not cluster_compat_mode:is_cluster_76() of
                  true ->
                      {get_keys, VBuckets, Params};
                  false ->
                      {get_keys, VBuckets, Params, Identity}
              end,
    do_call({server(Bucket), Node}, Bucket, GetKeys, infinity).

do_get_keys(Bucket, NodeVBuckets, Params, Identity) ->
    misc:parallel_map(
      fun ({Node, VBuckets}) ->
              try do_get_keys_call(Bucket, Node, VBuckets, Params, Identity) of
                  unhandled ->
                      {Node, {ok, []}};
                  R ->
                      {Node, R}
              catch
                  T:E ->
                      {Node, {T, E}}
              end
      end, NodeVBuckets, ?GET_KEYS_OUTER_TIMEOUT).

-spec config_validate(binary(), [inet | inet6]) -> ok | mc_error().
config_validate(NewConfig, AFamilies) ->
    misc:executing_on_new_process(
      fun () ->
              {ok, Sock} = connect(?MODULE_STRING ++ "/validate",
                                   [{retries, 1}, {try_afamily, AFamilies}]),
              mc_client_binary:config_validate(Sock, NewConfig)
      end).

config_reload(AFamilies) ->
    misc:executing_on_new_process(
      fun () ->
              {ok, Sock} = connect(?MODULE_STRING ++ "/reload",
                                   [{retries, 1}, {try_afamily, AFamilies}]),
              mc_client_binary:config_reload(Sock)
      end).

set_tls_config(Config) ->
    perform_very_long_call(
      fun (Sock) ->
          case mc_client_binary:set_tls_config(Sock, Config) of
              ok -> {reply, ok};
              {memcached_error, S, Msg} -> {reply, {error, {S, Msg}}}
          end
      end).

set_active_dek_for_bucket_uuid(BucketUUID, ActiveDek) ->
    maybe
        {ok, BucketName} ?= ns_bucket:uuid2bucket(BucketUUID),
        set_active_dek_for_bucket(BucketName, BucketUUID, ActiveDek)
    end.

set_active_dek_for_bucket(Bucket, UUID, _ActiveDek) ->
    {ok, DeksSnapshot} = cb_crypto:fetch_deks_snapshot({bucketDek, UUID}),
    NsMemcachedExists = (whereis(server(Bucket)) =/= undefined),
    case set_active_dek(Bucket, DeksSnapshot) of
        ok ->
            %% if bucket exists in memcached, ns_memcached should also exist
            maybe_reencrypt_bucket_metadata(Bucket);
        {error, not_found} -> %% bucket does not exist (in memcached!)
            case NsMemcachedExists of
                true ->
                    %% ns_memcached is running, we should retry when bucket
                    %% is created
                    {error, retry};
                false ->
                    %% ns_memcached was not running before the call, so it is
                    %% safe to return ok
                    %% If bucket needs to be created, ns_memcached will create
                    %% it and use newest keys. If bucket doesn't need to be
                    %% created (e.g. it is failed over), there is no need to
                    %% notify memcached at all. The same is true for the bucket
                    %% metadata file.
                    %% Note that it is important that we check if ns_memcached
                    %% is running before calling set_active_dek, otherwise it is
                    %% possible that ns_memcached creates the bucket with old
                    %% keys:
                    %% 1. ns_memcached fetches old keys
                    %% 2. this process calls set_active_dek (and gets not_found)
                    %% 3. ns_memcached creates the bucket with old keys
                    %% 4. ns_memcached crashes
                    %% 5. we check if ns_memcached is running and returning ok
                    ?log_debug("Ignoring not_found when setting encryption "
                               "keys for bucket ~p (bucket doesn't seem to "
                               "exist in memcached)", [Bucket]),
                    ok
            end;
        {error, E} ->
            {error, E}
    end.

maybe_reencrypt_bucket_metadata(BucketName) ->
    Server = server(BucketName),
    try
        gen_server:call(Server, maybe_reencrypt_bucket_metadata,
                        ?REENCRYPT_BUCKET_METADATA_TIMEOUT)
    catch
        exit:timeout ->
            {error, timeout};
        exit:{noproc, {gen_server, call,
                       [Server, maybe_reencrypt_bucket_metadata, _]}} ->
            {error, retry}
    end.

maybe_reencrypt_bucket_metadata(BucketName, BucketUUID, DeksSnapshot) ->
    CurrentActiveDekId = cb_crypto:get_dek_id(DeksSnapshot),
    case get_metadata_file_dek_ids_direct(BucketUUID) of
        {ok, [CurrentActiveDekId]} ->
            %% Metadata file exists and active dek id is the same as the
            %% current active dek id, no need to reencrypt
            ok;
        {ok, _} ->
            %% Metadata file exists and active dek id is different,
            %% rewrite the file
            write_bucket_metadata(BucketName, BucketUUID, DeksSnapshot);
        {error, E} ->
            ?log_error("Failed to get dek ids for metadata file for bucket ~p: ~p",
                       [BucketUUID, E]),
            {error, E}
    end.

write_bucket_metadata(BucketName, BucketUUID, DeksSnapshot) ->
    DBSubDir = ns_storage_conf:this_node_bucket_dbdir(BucketUUID),
    BucketMetadataFile = bucket_metadata_file(DBSubDir),
    ok = filelib:ensure_dir(BucketMetadataFile),

    ?log_debug("Writing bucket ~p metadata file: ~s",
               [BucketName, BucketMetadataFile]),

    BucketMetadata = {[{name, iolist_to_binary(BucketName)},
                       {uuid, ns_bucket:uuid(BucketName, direct)}]},
    cb_crypto:atomic_write_file(BucketMetadataFile,
                                ejson:encode(BucketMetadata), DeksSnapshot).

set_active_dek(TypeOrBucket, DeksSnapshot) ->
    ?log_debug("Setting active encryption key id for ~p: ~p...",
               [TypeOrBucket, cb_crypto:get_dek_id(DeksSnapshot)]),
    RV = perform_very_long_call(
           fun (Sock) ->
               case mc_client_binary:set_active_encryption_key(
                      Sock, TypeOrBucket, DeksSnapshot, ?SET_KEYS_TIMEOUT) of
                   ok -> {reply, ok};
                   {memcached_error, S, Msg} -> {reply, {error, {S, Msg}}}
               end
           end),

    case RV of
        ok ->
            ?log_debug("Setting encryption key for ~p succeeded",
                       [TypeOrBucket]),
            ok;
        {error, couldnt_connect_to_memcached} ->
            ?log_debug("Setting encryption key for ~p failed: "
                       "couldnt_connect_to_memcached", [TypeOrBucket]),
            {error, retry};
        %% It can happen during start, when bucket is not created yet
        {error, {key_enoent, undefined}} ->
            ?log_debug("Setting encryption key for ~p failed: "
                       "key_enoent", [TypeOrBucket]),
            {error, not_found};
        {error, {not_supported, undefined}} ->
            ?log_debug("Setting encryption key for ~p failed: "
                       "not_supported", [TypeOrBucket]),
            %% memcached returns not supported for buckets
            %% that exist on other nodes, but not on this node
            %% (ns_server uploads terse bucket info for such
            %% buckets), so from encryption perspective
            %% it is the same as when bucket does not exist
            {error, not_found};
        {error, E} ->
            ?log_error("Setting encryption key for ~p failed: ~p",
                       [TypeOrBucket, E]),
            {error, E}
    end.

prune_log_or_audit_encr_keys(Type, KeyIds) ->
    KeyIdsCleaned =  lists:map(fun(?NULL_DEK) -> <<"unencrypted">>;
                                   (Id) -> Id
                               end, KeyIds),
    ?log_debug("Pruning keys for ~p: KeyIds: ~p", [Type, KeyIdsCleaned]),
    perform_very_long_call(
      fun (Sock) ->
              case mc_client_binary:prune_log_or_audit_encr_keys(
                     Sock, Type, KeyIdsCleaned, ?PRUNE_KEYS_TIMEOUT) of
                  ok -> {reply, ok};
                  {memcached_error, S, Msg} -> {reply, {error, {S, Msg}}}
              end
      end).

sanitize_in_use_keys(InUseKeys) ->
    lists:map(fun (<<"unencrypted">>) -> ?NULL_DEK;
                  (undefined) -> ?NULL_DEK;
                  (K) -> K
              end, InUseKeys).

get_encryption_key_ids_stat(Bucket, StatsFn) ->
    RV = perform_very_long_call(
           fun (Sock) ->
                   case mc_binary:quick_stats(Sock, <<"encryption-key-ids">>,
                                              StatsFn, []) of
                       {ok, Ids} ->
                           {reply, {ok, Ids}};
                       {memcached_error, not_supported, _} ->
                           %% memcached returns not supported for buckets
                           %% that exist on other nodes, but not on this node
                           %% (ns_server uploads terse bucket info for such
                           %% buckets), so from encryption perspective
                           %% it is the same as when bucket does not exist
                           ?log_debug("Get deks in use for ~p returned "
                                      "not_supported", [Bucket]),
                           {reply, {error, not_found}};
                       {memcached_error, Error, Msg} ->
                           ?log_error("Failed to get dek ids in use for "
                                      "~p: ~p", [Bucket, {Error, Msg}]),
                           {reply, {error, Error}}
                   end
           end, Bucket),

    case RV of
        {ok, _} -> RV;
        {error, couldnt_connect_to_memcached} ->
            {error, retry};
        %% It can happen during start, when bucket is not created yet
        {error, {select_bucket_failed,
                 {memcached_error, key_enoent, undefined}}} ->
            {error, not_found};
        {error, E} ->
            {error, E}
    end.

get_dek_ids_in_use(Type) when Type =:= "@audit"; Type =:= "@logs" ->
    TypeBin = list_to_binary(Type),
    get_encryption_key_ids_stat(
      undefined,
      fun (<<"encryption-key-ids">>, V, _Acc) ->
              %% Decoded Format:
              %%   {[{"<<@audit>>", ["<<key1>>", ...]},
              %%     {<<"@logs">>,  ["<<key1>>", ...]}]},
              {TypeAndKeys} = ejson:decode(V),
              lists:flatmap(
                fun ({T, Keys}) when T =:= TypeBin ->
                        sanitize_in_use_keys(Keys);
                    ({_, _}) ->
                        []
                end, TypeAndKeys)
      end);
get_dek_ids_in_use(BucketUUID) ->
    maybe
        {ok, BucketName} ?= ns_bucket:uuid2bucket(BucketUUID),
        {ok, DekIds} ?= get_encryption_key_ids_stat(
                          BucketName,
                          fun (<<"encryption-key-ids">>, V, _Acc) ->
                                  %% Format: ["key1", "key2", ...],
                                  sanitize_in_use_keys(ejson:decode(V))
                          end),
        {ok, MDFileDekIds} ?= get_metadata_file_dek_ids(BucketName),
        {ok, lists:uniq(DekIds ++ sanitize_in_use_keys(MDFileDekIds))}
    end.

get_metadata_file_dek_ids(Bucket) ->
    Server = server(Bucket),
    try
        gen_server:call(Server, get_metadata_file_dek_ids,
                        ?GET_METADATA_FILE_DEK_IDS_TIMEOUT)
    catch
        exit:timeout ->
            {error, timeout};
        exit:{noproc, {gen_server, call,
                       [Server, get_metadata_file_dek_ids, _]}} ->
            ?log_debug("Can't get metadata file dek ids: ~p is not started...",
                       [Server]),
            {error, retry}
    end.

get_metadata_file_dek_ids_direct(BucketUUID) ->
    DBSubDir = ns_storage_conf:this_node_bucket_dbdir(BucketUUID),
    BucketMetadataFile = bucket_metadata_file(DBSubDir),
    cb_crypto:get_file_dek_ids(BucketMetadataFile).

-spec get_bucket_stats(binary(), binary(), binary()) ->
          {ok, any()} | mc_error() | {error, couldnt_connect_to_memcached}.
get_bucket_stats(RootKey, StatKey, SubKey) ->
    perform_very_long_call(
      fun(Sock) ->
              case mc_client_binary:stats(Sock, RootKey,
                                          fun(K, V, Acc) ->
                                                  [{K, V} | Acc]
                                          end, []) of
                  {ok, BucketsDetailsRaw} ->
                      {BucketDetails} =
                          ejson:decode(
                            misc:expect_prop_value(StatKey,
                                                   BucketsDetailsRaw)),
                      {reply, {ok, proplists:get_value(SubKey, BucketDetails)}};
                  {memcached_error, _, _} = Err ->
                      {reply, Err}
              end
      end).

get_all_buckets_details() ->
    get_bucket_stats(<<"bucket_details">>, <<"bucket details">>, <<"buckets">>).

get_bucket_state(Bucket) ->
    get_bucket_stats(list_to_binary("bucket_details " ++ Bucket),
                     list_to_binary(Bucket), <<"state">>).

-spec get_failover_log(bucket_name(), vbucket_id()) ->
                              [{integer(), integer()}] | mc_error().
get_failover_log(Bucket, VBucket) ->
    perform_very_long_call(
      ?cut({reply, mc_client_binary:get_failover_log(_, VBucket)}), Bucket).

-spec get_failover_logs(bucket_name(), [vbucket_id()]) -> Result when
      Result :: Success | Error,
      Success :: {ok, [{vbucket_id(), FailoverLog}]},
      FailoverLog :: [{integer(), integer()}],
      Error :: {error, {failed_to_get_failover_log,
                        bucket_name(), vbucket_id(), mc_error()}}.
get_failover_logs(Bucket, VBuckets) ->
    %% TODO: consider using "failovers" stat instead
    perform_very_long_call(
      ?cut({reply, get_failover_logs_loop(_, VBuckets, [])}), Bucket).

get_failover_logs_loop(_Sock, [], Acc) ->
    {ok, lists:reverse(Acc)};
get_failover_logs_loop(Sock, [V | VBs], Acc) ->
    case mc_client_binary:get_failover_log(Sock, V) of
        FailoverLog when is_list(FailoverLog) ->
            get_failover_logs_loop(Sock, VBs, [FailoverLog | Acc]);
        Error ->
            {error, {failed_to_get_failover_log, V, Error}}
    end.

-spec set_cluster_config(integer(), integer(), binary()) -> ok | mc_error().
set_cluster_config(Rev, RevEpoch, Blob) ->
    perform_very_long_call(
      ?cut({reply, mc_client_binary:set_cluster_config(_, "", Rev, RevEpoch, Blob)})).

get_collections_uid(Bucket) ->
    RV =
        perform_very_long_call(
          ?cut({reply, memcached_bucket_config:get_current_collections_uid(_)}),
          Bucket),
    case RV of
        {error, {select_bucket_failed, {memcached_error, key_enoent, _}}} ->
            {error, bucket_not_found};
        UID ->
            {ok, collections:convert_uid_from_memcached(UID)}
    end.

handle_connected_call(Call, From, #state{status = Status} = State) ->
    case Status of
        S when (S =:= init orelse S =:= connecting) ->
            {reply, warming_up, State};
        _ ->
            handle_call(Call, From, State)
    end.

construct_value(topology, Topology) ->
    [lists:map(fun (undefined) ->
                       null;
                   (Node) ->
                       Node
               end, Chain) || Chain <- Topology];
construct_value(_, V) ->
    V.

construct_vbucket_info_json([]) ->
    undefined;
construct_vbucket_info_json(Options) ->
    {[{Key, construct_value(Key, Value)} || {Key, Value} <- Options]}.

durability_keys() ->
    ["state", "topology", "high_seqno", "high_prepared_seqno"].

get_stats_key(["state"]) ->
    <<"vbucket">>;
get_stats_key(ReqdKeys) ->
    case ReqdKeys -- durability_keys() of
        [] ->
            <<"vbucket-durability-state">>;
        _ ->
            <<"vbucket-details">>
    end.

vbucket_stats_key(Key, VBucket) ->
    iolist_to_binary([Key, " ", integer_to_list(VBucket)]).

get_vbucket_details(Sock, all, ReqdKeys) ->
    get_vbucket_details_inner(Sock, get_stats_key(ReqdKeys), ReqdKeys);
get_vbucket_details(Sock, VBucket, ReqdKeys) when is_integer(VBucket) ->
    get_vbucket_details_inner(
      Sock, vbucket_stats_key(get_stats_key(ReqdKeys), VBucket), ReqdKeys).

get_vbucket_details_inner(Sock, DetailsKey, ReqdKeys) ->
    mc_binary:quick_stats(
      Sock, DetailsKey,
      fun (<<"vb_", VBKey/binary>>, BinVal, Dict) ->
              {VB, Key} = case binary:split(VBKey, [<<":">>]) of
                              [BinVB, BinK] -> {BinVB, binary_to_list(BinK)};
                              [BinVB] -> {BinVB, "state"}
                          end,
              case lists:member(Key, ReqdKeys) of
                  true ->
                      VBucket = list_to_integer(binary_to_list(VB)),
                      NewVal = [{Key, binary_to_list(BinVal)}],
                      dict:update(VBucket,
                                  fun (OldVal) ->
                                          NewVal ++ OldVal
                                  end, NewVal, Dict);
                  false ->
                      Dict
              end
      end, dict:new()).

send_check_started_msg() ->
    erlang:send_after(?CHECK_WARMUP_INTERVAL, self(), check_started).

send_check_config_msg(#state{next_check_after = After}) ->
    erlang:send_after(After, self(), check_config).

get_config_stats(Bucket, SubKey) ->
    perform_very_long_call(
      fun (Sock) ->
              Res = mc_client_binary:stats(
                      Sock, <<"config">>,
                      fun (K, V, Acc) ->
                              [{K, V} | Acc]
                      end, []),
              case Res of
                  {ok, Props} ->
                      {reply, proplists:get_value(SubKey, Props)};
                  Err ->
                      {reply, Err}
              end
      end, Bucket).

drop_deks(BucketUUID, IdsToDrop, ContinuationId, Continuation) ->
    maybe
        {ok, BucketName} ?= ns_bucket:uuid2bucket(BucketUUID),
        ?log_debug("Initiating db compaction for bucket ~p in order to get rid "
                   "of old keys: ~p...", [BucketName, IdsToDrop]),
        IdsToDropMcd = lists:map(fun (?NULL_DEK) -> <<"unencrypted">>;
                                     (Id) -> Id
                                 end, IdsToDrop),
        ok ?= drop_metadata_file_dek_ids(BucketName, BucketUUID, IdsToDropMcd),
        ok ?= compaction_api:partially_compact_db_files(
                BucketName, IdsToDropMcd, ContinuationId, Continuation),
        {ok, started}
    end.

drop_metadata_file_dek_ids(BucketName, BucketUUID, IdsToDrop) ->
    %% Metadata file should normally be already encrypted by the active key
    %% so most likely there is nothing to do here.
    %% Also, race with ns_memcached is ok here because:
    %% 1. metadata file is written atomically
    %% 2. ns_memcached can't encrypt it with older key than the current key,
    %%    so even if it races with us, the file will get encrypted by
    %%    the newer key
    %% Why do we need to check metadata file first instead of just calling
    %% ns_memcached? Because ns_memcached starts after cb_cluster_secrets
    %% so it is possible that that ns_memcached doesn't exist yet.
    maybe
        {ok, DekIds} ?= get_metadata_file_dek_ids_direct(BucketUUID),
        TranslatedIds = lists:map(fun (undefined) -> ?NULL_DEK;
                                      (Id) -> Id
                                  end, DekIds),
        ShouldRewrite = lists:any(lists:member(_, IdsToDrop), TranslatedIds),
        case ShouldRewrite of
            true -> maybe_reencrypt_bucket_metadata(BucketName);
            false -> ok
        end
    end.

-spec get_fusion_storage_snapshot(bucket_name(), vbucket_id(), string(),
                                  non_neg_integer()) ->
          {ok, binary()} | mc_error().
get_fusion_storage_snapshot(Bucket, VBucket, SnapshotUUID, Validity) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:get_fusion_storage_snapshot(
                        Sock, VBucket, SnapshotUUID, Validity)}
      end, Bucket, [json]).

-spec mount_fusion_vbucket(bucket_name(), vbucket_id(), [list()]) ->
          {ok, binary()} | mc_error().
mount_fusion_vbucket(Bucket, VBucket, Volumes) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:mount_fusion_vbucket(
                        Sock, VBucket, Volumes)}
      end, Bucket, [json]).

fetch_fusion_stats(Sock, Bucket, Key) ->
    case fetch_stats(Sock, iolist_to_binary(["fusion ", Key])) of
        {ok, Stats} ->
            {ok, ejson:decode(proplists:get_value(<<"fusion">>, Stats))};
        Other ->
            ?log_error("Error retrieving stat ~p for bucket ~p: ~p",
                       [Key, Bucket, Other]),
            Other
    end.

%% this is a temporary crutch for magma issues. If uploader cannot be
%% stopped/started we drop the error to the log file and rely on
%% janitor eventually fixing things
try_to_perform_very_long_call(Fun, Bucket, Opts) ->
    try
        perform_very_long_call(Fun, Bucket, Opts)
    catch
        T:E:Stack ->
            ?log_error("Error calling memcached for bucket ~p: ~p",
                       [Bucket, {T, E, Stack}]),
            {memcached_error, internal,
             iolist_to_binary(io_lib:format("Caught an exception ~p", [E]))}
    end.

reply_start_stop_uploaders(What, RVs) ->
    case [T || {_, RV} = T <- misc:enumerate(RVs, 0), RV =/= ok] of
        [] ->
            {reply, ok};
        Bad ->
            ?log_error("Errors ~s fusion uploaders: ~p", [What, Bad]),
            {reply, error}
    end.

-spec maybe_start_fusion_uploaders(
        bucket_name(), [{vbucket_id(), integer()}]) -> ok | error | mc_error().
maybe_start_fusion_uploaders(Bucket, Uploaders) ->
    try_to_perform_very_long_call(
      fun (Sock) ->
              RVs =
                  [mc_client_binary:start_fusion_uploader(Sock, VBucket, Term)
                   || {VBucket, Term} <- Uploaders],
              reply_start_stop_uploaders("starting", RVs)
      end, Bucket, [json]).

-spec maybe_stop_fusion_uploaders(bucket_name(), [vbucket_id()]) ->
          ok | error | mc_error().
maybe_stop_fusion_uploaders(Bucket, VBuckets) ->
    try_to_perform_very_long_call(
      fun (Sock) ->
              RVs = [mc_client_binary:stop_fusion_uploader(Sock, VBucket) ||
                        VBucket <- VBuckets],
              reply_start_stop_uploaders("stopping", RVs)
      end, Bucket, []).

-spec get_active_guest_volumes(bucket_name()) -> {ok, [binary()]} | mc_error().
get_active_guest_volumes(Bucket) ->
    try_to_perform_very_long_call(
      fun (Sock) ->
              {reply, fetch_fusion_stats(Sock, Bucket, "active_guest_volumes")}
      end, Bucket, [json]).

-spec sync_fusion_log_store(bucket_name(), [vbucket_id()]) ->
          ok | mc_error().
sync_fusion_log_store(Bucket, VBuckets) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply,
               functools:sequence_(
                 [?cut(mc_client_binary:sync_fusion_log_store(Sock, VBucket)) ||
                     VBucket <- VBuckets])}
      end, Bucket).

bucket_metadata_file(BucketDir) ->
    filename:join([BucketDir, "cm", "bucket.metadata"]).