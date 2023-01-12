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
-include("cut.hrl").

-define(CHECK_INTERVAL, 10000).
-define(CHECK_WARMUP_INTERVAL, 500).
-define(CONNECT_DONE_RETRY_INTERVAL, 500).
-define(TIMEOUT,             ?get_timeout(outer, 300000)).
-define(TIMEOUT_HEAVY,       ?get_timeout(outer_heavy, 300000)).
-define(TIMEOUT_VERY_HEAVY,  ?get_timeout(outer_very_heavy, 360000)).
-define(WARMED_TIMEOUT,      ?get_timeout(warmed, 5000)).
-define(MARK_WARMED_TIMEOUT, ?get_timeout(mark_warmed, 5000)).
%% half-second is definitely 'slow' for any definition of slow
-define(SLOW_CALL_THRESHOLD_MICROS, 500000).
-define(GET_KEYS_TIMEOUT,       ?get_timeout(get_keys, 60000)).
-define(GET_KEYS_OUTER_TIMEOUT, ?get_timeout(get_keys_outer, 70000)).
-define(MAGMA_CREATION_TIMEOUT, ?get_timeout(magma_creation, 300000)).

-define(RECBUF, ?get_param(recbuf, 64 * 1024)).
-define(SNDBUF, ?get_param(sndbuf, 64 * 1024)).

-define(CONNECTION_ATTEMPTS, 5).

%% gen_server API
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {
          running_fast = 0,
          running_heavy = 0,
          running_very_heavy = 0,
          %% NOTE: otherwise dialyzer seemingly thinks it's possible
          %% for queue fields to be undefined
          fast_calls_queue = impossible :: queue:queue(),
          heavy_calls_queue = impossible :: queue:queue(),
          very_heavy_calls_queue = impossible :: queue:queue(),
          status :: connecting | init | connected | warmed,
          start_time :: undefined | tuple(),
          bucket :: bucket_name(),
          worker_features = [],
          sock = still_connecting :: port() | still_connecting,
          work_requests = [],
          warmup_stats = [] :: [{binary(), binary()}],
          control_queue :: pid(),
          check_in_progress = false :: boolean(),
          next_check_after = ?CHECK_INTERVAL :: integer()
         }).

%% external API
-export([active_buckets/0,
         warmed_buckets/0,
         warmed_buckets/1,
         mark_warmed/2,
         mark_warmed/1,
         disable_traffic/2,
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
         stats/2,
         warmup_stats/1,
         raw_stats/5,
         flush/1,
         set/7, add/5, get/5, delete/5,
         get_from_replica/4,
         get_meta/5,
         get_xattrs/6,
         update_with_rev/7,
         get_seqno_stats/2,
         get_mass_dcp_docs_estimate/2,
         get_dcp_docs_estimate/3,
         set_cluster_config/4,
         set_cluster_config/3,
         get_ep_startup_time_for_xdcr/1,
         perform_checkpoint_commit_for_xdcr/3,
         get_random_key/1, get_random_key/2,
         compact_vbucket/3,
         get_vbucket_high_seqno/2,
         wait_for_seqno_persistence/3,
         get_keys/4,
         config_validate/2,
         config_reload/1,
         set_tls_config/1,
         get_failover_log/2,
         get_failover_logs/2,
         get_collections_uid/1,
         maybe_add_impersonate_user_frame_info/2
        ]).

%% for ns_memcached_sockets_pool, memcached_file_refresh only
-export([connect/0, connect/1]).

%% for diagnostics/debugging
-export([perform_very_long_call/2]).

-include("mc_constants.hrl").
-include("mc_entry.hrl").

%%
%% gen_server API implementation
%%

start_link(Bucket) ->
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

    State = #state{
               status = connecting,
               bucket = Bucket,
               worker_features = get_worker_features(),
               work_requests = [],
               fast_calls_queue = Q,
               heavy_calls_queue = Q,
               very_heavy_calls_queue = Q,
               running_fast = WorkersCount,
               control_queue = ControlQueue
              },
    {ok, State}.

run_connect_phase(Parent, Bucket, WorkersCount) ->
    ?log_debug("Started 'connecting' phase of ns_memcached-~s. Parent is ~p",
               [Bucket, Parent]),
    RV = case connect() of
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
                  {collections, collections:enabled()}],
    misc:canonical_proplist(FeatureSet).

worker_init(Parent, ParentState) ->
    ParentState1 = do_worker_init(ParentState),
    worker_loop(Parent, ParentState1, #state.running_fast).

do_worker_init(State) ->
    {ok, Sock} = connect(State#state.worker_features),

    {ok, SockName} = inet:sockname(Sock),
    erlang:put(sockname, SockName),

    ok = mc_client_binary:select_bucket(Sock, State#state.bucket),
    State#state{sock = Sock}.

worker_loop(Parent, #state{sock = Sock} = State, PrevCounterSlot) ->
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
            ?log_warning("Call ~p (return value ~p) compromised our connection. Reconnecting.",
                         [Msg, R]),
            gen_server:reply(From, R),
            error({compromised_reply, R})
    end,

    verify_report_long_call(StartTS, WorkStartTS, State, Msg, []),
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
handle_call(disable_traffic, _From, State) ->
    case State#state.status of
        Status when Status =:= warmed; Status =:= connected ->
            ?log_info("Disabling traffic and unmarking bucket as warmed"),
            case mc_client_binary:disable_traffic(State#state.sock) of
                ok ->
                    State2 = State#state{status=connected,
                                         start_time = os:timestamp()},
                    BucketName = State2#state.bucket,
                    UUID = ns_bucket:uuid(BucketName, direct),
                    event_log:add_log(bucket_offline,
                                      [{bucket, list_to_binary(BucketName)},
                                       {bucket_uuid, UUID}]),
                    {reply, ok, State2};
                {memcached_error, _, _} = Error ->
                    ?log_error("disabling traffic failed: ~p", [Error]),
                    {reply, Error, State}
            end;
        _ ->
            {reply, bad_status, State}
    end;
handle_call(mark_warmed, _From, #state{status=Status,
                                       bucket=Bucket,
                                       start_time=Start,
                                       sock=Sock} = State) ->
    {NewStatus, Reply} =
        case Status of
            connected ->
                ?log_info("Enabling traffic to bucket ~p", [Bucket]),
                case mc_client_binary:enable_traffic(Sock) of
                    ok ->
                        Time = timer:now_diff(os:timestamp(), Start) div 1000000,
                        ?log_info("Bucket ~p marked as warmed in ~p seconds",
                                  [Bucket, Time]),
                        UUID = ns_bucket:uuid(Bucket, direct),
                        event_log:add_log(bucket_online,
                                          [{bucket, list_to_binary(Bucket)},
                                           {bucket_uuid, UUID},
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

verify_report_long_call(StartTS, ActualStartTS, State, Msg, RV) ->
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
                ?log_debug("call ~p took too long: ~p us", [Msg, Diff]);
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
assign_queue({set, _KeyFun, _Uid, _VBucket, _ValueFun, _Flags, _Identity}) ->
    #state.heavy_calls_queue;
assign_queue({get_keys, _VBuckets, _Params}) -> #state.heavy_calls_queue;
assign_queue({get_keys, _VBuckets, _Params, _Identity}) -> #state.heavy_calls_queue;
assign_queue({get_mass_dcp_docs_estimate, _VBuckets}) -> #state.very_heavy_calls_queue;
assign_queue({get_vbucket_details_stats, all, _}) -> #state.very_heavy_calls_queue;
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
    McHeader#mc_header{frame_infos = [McFrameInfo]}.

handle_data_call(Command, KeyFun, CollectionsUid, VBucket, State) ->
    handle_data_call(Command, KeyFun, CollectionsUid, VBucket, #mc_entry{},
                     State).

handle_data_call(Command, KeyFun, CollectionsUid, VBucket, McEntry, State) ->
    handle_data_call(Command, KeyFun, CollectionsUid, VBucket, undefined,
                     McEntry, State).

handle_data_call(Command, KeyFun, CollectionsUid, VBucket, Identity, McEntry,
                 #state{worker_features = Features} = State) ->
    CollectionsEnabled = proplists:get_bool(collections, Features),
    EncodedKey = mc_binary:maybe_encode_uid_in_key(CollectionsEnabled,
                                                   CollectionsUid, KeyFun()),
    McHeader0 = #mc_header{vbucket = VBucket},
    McHeader = maybe_add_impersonate_user_frame_info(Identity, McHeader0),

    Reply = mc_client_binary:cmd(
              Command, State#state.sock, undefined, undefined,
              {McHeader,
               McEntry#mc_entry{key = EncodedKey}}),
    {reply, Reply, State}.

do_handle_call(verify_warmup,  _From, #state{bucket = Bucket,
                                             sock = Sock} = State) ->
    Stats = retrieve_warmup_stats(Sock),
    {reply, has_started(Stats, Bucket), State};
do_handle_call({raw_stats, SubStat, StatsFun, StatsFunState}, _From, State) ->
    try mc_binary:quick_stats(State#state.sock, SubStat, StatsFun, StatsFunState) of
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
    ok = mc_client_binary:set_vbucket(Sock, VBucket, dead),
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
    handle_data_call(?DELETE, KeyFun, CollectionsUid, VBucket, Identity,
                     #mc_entry{}, State);

do_handle_call({set, KeyFun, CollectionsUid, VBucket, ValFun, Flags,
                Identity}, _From, State) ->
    handle_data_call(?SET, KeyFun, CollectionsUid, VBucket, Identity,
                     #mc_entry{data = ValFun(), flag = Flags}, State);

do_handle_call({add, KeyFun, CollectionsUid, VBucket, ValFun}, _From, State) ->
    handle_data_call(?ADD, KeyFun, CollectionsUid, VBucket,
                     #mc_entry{data = ValFun()}, State);

do_handle_call({get, KeyFun, CollectionsUid, VBucket, Identity}, _From,
               State) ->
    handle_data_call(?GET, KeyFun, CollectionsUid, VBucket, Identity,
                     #mc_entry{}, State);

do_handle_call({get_from_replica, KeyFun, CollectionsUid, VBucket}, _From,
               State) ->
    handle_data_call(?CMD_GET_REPLICA, KeyFun, CollectionsUid, VBucket, State);

do_handle_call({set_vbuckets, VBsToSet}, _From, #state{sock = Sock} = State) ->
    ToSet = [{VB, VBState, construct_vbucket_info_json(Topology)}
             || {VB, VBState, Topology} <- VBsToSet],
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
do_handle_call({set_vbucket, VBucket, VBState, Topology}, _From,
               #state{sock=Sock, bucket=BucketName} = State) ->
    VBInfoJson = construct_vbucket_info_json(Topology),
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
do_handle_call({set_cluster_config, Rev, RevEpoch, Blob}, _From,
               State = #state{bucket = Bucket, sock = Sock}) ->
    {reply, mc_client_binary:set_cluster_config(Sock, Bucket, Rev, RevEpoch, Blob),
     State};
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

%% This is left in place to support backwards compat from nodes with version
%% lower than 7.1. Lower than 7.1 version Nodes won't provide identity.
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

handle_info({connect_done, WorkersCount, RV}, #state{bucket = Bucket,
                                                     status = OldStatus} = State) ->
    gen_event:notify(buckets_events, {started, Bucket}),
    erlang:process_flag(trap_exit, true),
    Self = self(),

    case RV of
        {ok, Sock} ->
            try ensure_bucket(Sock, Bucket, false) of
                ok ->
                    connecting = OldStatus,

                    ?log_info("Main ns_memcached connection established: ~p",
                              [RV]),

                    Self ! check_started,

                    InitialState = State#state{
                                     start_time = os:timestamp(),
                                     sock = Sock,
                                     status = init
                                    },
                    [proc_lib:spawn_link(erlang, apply, [fun worker_init/2,
                                                         [Self, InitialState]])
                     || _ <- lists:seq(1, WorkersCount)],

                    chronicle_compat_events:subscribe(
                      fun (cluster_compat_version) ->
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

                    {noreply, InitialState};
                {error, {bucket_create_error,
                         {memcached_error, key_eexists, _}}} ->
                    ?log_debug("ensure_bucket failed as bucket ~p has not "
                               "completed coming online", [Bucket]),
                    erlang:send_after(?CONNECT_DONE_RETRY_INTERVAL, Self,
                                      {connect_done, WorkersCount, RV}),
                    {noreply, State};
                Error ->
                    ?log_info("ensure_bucket failed: ~p", [Error]),
                    {stop, Error}
            catch
                exit:{shutdown, reconfig} ->
                    {stop, {shutdown, reconfig}, State#state{sock = Sock}}
            end;
        Error ->
            ?log_info("Failed to establish ns_memcached connection: ~p", [RV]),
            {stop, Error}
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
handle_info(Message, #state{worker_features = WF, control_queue = Q,
                            bucket = Bucket, check_in_progress = false} = State)
  when Message =:= check_config_soon orelse Message =:= check_config ->
    misc:flush(check_config_soon),
    misc:flush(check_config),
    case get_worker_features() of
        WF ->
            Self = self(),
            work_queue:submit_work(
              Q,
              fun () ->
                      perform_very_long_call_with_timing(
                        Bucket, ensure_bucket, ensure_bucket(_, Bucket, true)),
                      Self ! complete_check
              end),
            {noreply, State#state{check_in_progress = true,
                                  next_check_after = ?CHECK_INTERVAL}};
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
terminate(Reason, #state{bucket=Bucket, sock=Sock}) ->
    try
        do_terminate(Reason, Bucket, Sock)
    after
        gen_event:notify(buckets_events, {stopped, Bucket}),
        ?log_debug("Terminated.")
    end.

do_terminate(Reason, Bucket, Sock) ->
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
            ale:info(?USER_LOGGER, "Shutting down bucket ~p on ~p for ~s",
                     [Bucket, ThisNode, if
                                            Reconfig -> "reconfiguration";
                                            Deleting -> "deletion"
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

            delete_bucket(Sock, Bucket, Force, DeleteData);
        false ->
            %% if this is system shutdown bucket engine now can reliably
            %% delete all buckets as part of shutdown. if this is supervisor
            %% crash, we're fine too
            ale:info(?USER_LOGGER,
                     "Control connection to memcached on ~p disconnected. "
                     "Check logs for details.", [ThisNode])
    end.

delete_bucket(Sock, Bucket, Force, DeleteData) ->
    ?log_info("Deleting bucket ~p from memcached (force = ~p)",
              [Bucket, Force]),

    try
        ok = mc_client_binary:delete_bucket(Sock, Bucket, [{force, Force}])
    catch
        T:E ->
            ?log_error("Failed to delete bucket ~p: ~p", [Bucket, {T, E}])
    after
        case DeleteData of
            true ->
                ?log_debug("Proceeding into vbuckets dbs deletions"),
                ns_couchdb_api:delete_databases_and_files(Bucket);
            false ->
                ok
        end
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%
%% API
%%
perform_very_long_call_with_timing(Bucket, Name, Fun) ->
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
      end, Bucket).

-spec active_buckets() -> [bucket_name()].
active_buckets() ->
    [Bucket || ?MODULE_STRING "-" ++ Bucket <-
                   [atom_to_list(Name) || Name <- registered()]].

-spec warmed(node(), bucket_name(), pos_integer() | infinity) -> boolean().
warmed(Node, Bucket, Timeout) ->
    try
        do_call({server(Bucket), Node}, Bucket, warmed, Timeout)
    catch
        _:_ ->
            false
    end.

-spec mark_warmed([node()], bucket_name())
                 -> Result
                        when Result :: {Replies, BadNodes},
                             Replies :: [{node(), any()}],
                             BadNodes :: [node()].
mark_warmed(Nodes, Bucket) ->
    gen_server:multi_call(Nodes, server(Bucket),
                          mark_warmed, ?MARK_WARMED_TIMEOUT).

-spec mark_warmed(bucket_name()) -> any().
mark_warmed(Bucket) ->
    gen_server:call(server(Bucket), mark_warmed, ?MARK_WARMED_TIMEOUT).

warmed_buckets() ->
    warmed_buckets(?WARMED_TIMEOUT).

warmed_buckets(Timeout) ->
    RVs = misc:parallel_map(
            fun (Bucket) ->
                    {Bucket, warmed(dist_manager:this_node(), Bucket,
                                    Timeout)}
            end, active_buckets(), infinity),
    [Bucket || {Bucket, true} <- RVs].

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
          binary(), integer(), undefined | rbac_identity()) ->
                 {ok, #mc_header{}, #mc_entry{}, any()} |
                 {memcached_error, any(), any()}.
set(Bucket, Key, CollectionsUid, VBucket, Value, Flags, Identity) ->
    do_call(server(Bucket), Bucket,
            {set, fun () -> Key end, CollectionsUid, VBucket,
             fun () -> Value end, Flags, Identity}, ?TIMEOUT_HEAVY).

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
    set_vbucket(Bucket, VBucket, VBState, undefined).

-spec set_vbucket(bucket_name(), vbucket_id(), vbucket_state(),
                  [[node()]] | undefined) -> ok | mc_error().
set_vbucket(Bucket, VBucket, VBState, Topology) ->
    do_call(server(Bucket), Bucket, {set_vbucket, VBucket, VBState, Topology},
            ?TIMEOUT_HEAVY).

-spec set_vbuckets(bucket_name(),
                   [{vbucket_id(), vbucket_state(), [[node()]] | undefined}]) ->
    ok |
    {errors, [{{vbucket_id(), vbucket_state(), [[node()]] | undefined},
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

-spec stats(bucket_name(), binary() | string()) ->
                   {ok, [{binary(), binary()}]} | mc_error().
stats(Bucket, Key) ->
    perform_very_long_call(
      fun (Sock) ->
              Reply = mc_binary:quick_stats(
                        Sock, Key, fun mc_binary:quick_stats_append/3, []),
              {reply, Reply}
      end, Bucket).

-spec warmup_stats(bucket_name()) -> [{binary(), binary()}].
warmup_stats(Bucket) ->
    do_call(server(Bucket), Bucket, warmup_stats, ?TIMEOUT).

-spec raw_stats(node(), bucket_name(), binary(), fun(), any()) -> {ok, any()} | {exception, any()} | {error, any()}.
raw_stats(Node, Bucket, SubStats, Fn, FnState) ->
    do_call({server(Bucket), Node}, Bucket,
            {raw_stats, SubStats, Fn, FnState}, ?TIMEOUT).


-spec get_vbucket_high_seqno(bucket_name(), vbucket_id()) ->
                                    {ok, {undefined | seq_no()}}.
get_vbucket_high_seqno(Bucket, VBucketId) ->
    do_call(server(Bucket), Bucket,
            {get_vbucket_high_seqno, VBucketId}, ?TIMEOUT).

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
connect() ->
    connect([]).

connect(Options) ->
    Retries = proplists:get_value(retries, Options, ?CONNECTION_ATTEMPTS),
    connect(Options, Retries).

connect(Options, Tries) ->
    try
        do_connect(Options)
    catch
        E:R ->
            case Tries of
                1 ->
                    ?log_warning("Unable to connect: ~p.", [{E, R}]),
                    {error, couldnt_connect_to_memcached};
                _ ->
                    ?log_warning("Unable to connect: ~p, retrying.", [{E, R}]),
                    timer:sleep(1000), % Avoid reconnecting too fast.
                    connect(Options, Tries - 1)
            end
    end.

do_connect(Options) ->
    Config = ns_config:get(),
    Port = service_ports:get_port(memcached_dedicated_port, Config),
    User = ns_config:search_node_prop(Config, memcached, admin_user),
    Pass = ns_config:search_node_prop(Config, memcached, admin_pass),
    AFamilies = proplists:get_value(try_afamily, Options,
                                    [misc:get_net_family()]),
    HelloFeatures = proplists:delete(try_afamily, Options),
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
                                                 {sndbuf, ?SNDBUF}]),
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
        {ok, Negotiated} = mc_client_binary:hello(Sock, "regular", Features),
        Failed = Features -- Negotiated,
        Failed == [] orelse error({feature_negotiation_failed, Failed}),
        {ok, Sock}
    catch
        T:E ->
            gen_tcp:close(Sock),
            throw({T, E})
    end.

ensure_bucket(Sock, Bucket, BucketSelected) ->
    %% This testpoint simulates the case where the bucket is not selectable
    %% (returns enoent) but does exist so cannot be created.
    case simulate_slow_bucket_operation(Bucket, slow_bucket_creation,
                                        ?CONNECT_DONE_RETRY_INTERVAL) of
        true ->
            {error, {bucket_create_error,
                     {memcached_error, key_eexists, not_used}}};
        false ->
            ensure_bucket_inner(Sock, Bucket, BucketSelected)
    end.

ensure_bucket_inner(Sock, Bucket, BucketSelected) ->
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
            case do_ensure_bucket(Sock, Bucket, BConf, BucketSelected) of
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

do_ensure_bucket(Sock, Bucket, BConf, true) ->
    ensure_selected_bucket(Sock, Bucket, BConf);
do_ensure_bucket(Sock, Bucket, BConf, false) ->
    case mc_client_binary:select_bucket(Sock, Bucket) of
        ok ->
            ensure_selected_bucket(Sock, Bucket, BConf);
        {memcached_error, key_enoent, _} ->
            {ok, DBSubDir} =
                ns_storage_conf:this_node_bucket_dbdir(Bucket),
            ok = filelib:ensure_dir(DBSubDir),

            {Engine, ConfigString} =
                memcached_bucket_config:start_params(BConf),

            BucketConfig = memcached_bucket_config:get_bucket_config(BConf),
            Timeout = case ns_bucket:kv_backend_type(BucketConfig) of
                          magma ->
                              ?MAGMA_CREATION_TIMEOUT;
                          _ ->
                              %% Use whatever the default value is
                              undefined
                      end,

            case mc_client_binary:create_bucket(Sock, Bucket, Engine,
                                                ConfigString, Timeout) of
                ok ->
                    ?log_info("Created bucket ~p with config string ~p",
                              [Bucket, ConfigString]),
                    ok = mc_client_binary:select_bucket(Sock, Bucket);
                Error ->
                    {error, {bucket_create_error, Error}}
            end;
        Error ->
            {error, {bucket_select_error, Error}}
    end.

ensure_selected_bucket(Sock, Bucket, BConf) ->
    case memcached_bucket_config:ensure(Sock, BConf) of
        restart ->
            ale:info(
              ?USER_LOGGER,
              "Restarting bucket ~p due to configuration change",
              [Bucket]),
            exit({shutdown, reconfig});
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

-spec wait_for_seqno_persistence(bucket_name(), vbucket_id(), seq_no()) -> ok | mc_error().
wait_for_seqno_persistence(Bucket, VBucketId, SeqNo) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:wait_for_seqno_persistence(Sock, VBucketId, SeqNo)}
      end, Bucket).

-spec compact_vbucket(bucket_name(), vbucket_id(),
                      {integer(), integer(), boolean()}) ->
                             ok | mc_error().
compact_vbucket(Bucket, VBucket, {PurgeBeforeTS, PurgeBeforeSeqNo, DropDeletes}) ->
    perform_very_long_call(
      fun (Sock) ->
              {reply, mc_client_binary:compact_vbucket(Sock, VBucket,
                                                       PurgeBeforeTS, PurgeBeforeSeqNo, DropDeletes)}
      end, Bucket).


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

-spec set_cluster_config(bucket_name(), integer(), integer(), binary()) -> ok | mc_error().
set_cluster_config(Bucket, Rev, RevEpoch, Blob) ->
    do_call(server(Bucket), Bucket, {set_cluster_config, Rev, RevEpoch, Blob}, ?TIMEOUT).

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
    %% This is to handle cluster compat scenario to support pre 7.1 get_keys
    %% request to continue to work as before without the Identity info
    GetKeys = case Identity of
                  undefined -> {get_keys, VBuckets, Params};
                  _ -> {get_keys, VBuckets, Params, Identity}
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
              {ok, Sock} = connect([{retries, 1},
                                    {try_afamily, AFamilies}]),
              mc_client_binary:config_validate(Sock, NewConfig)
      end).

config_reload(AFamilies) ->
    misc:executing_on_new_process(
      fun () ->
              {ok, Sock} = connect([{retries, 1},
                                    {try_afamily, AFamilies}]),
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

construct_topology(Topology) ->
    [lists:map(fun (undefined) ->
                       null;
                   (Node) ->
                       Node
               end, Chain) || Chain <- Topology].

construct_topology_json(Topology) ->
    {[{topology, construct_topology(Topology)}]}.

construct_vbucket_info_json(undefined) ->
    undefined;
construct_vbucket_info_json(Topology) ->
    construct_topology_json(Topology).

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

get_vbucket_details(Sock, all, ReqdKeys) ->
    get_vbucket_details_inner(Sock, get_stats_key(ReqdKeys), ReqdKeys);
get_vbucket_details(Sock, VBucket, ReqdKeys) when is_integer(VBucket) ->
    VBucketStr = integer_to_list(VBucket),
    get_vbucket_details_inner(
      Sock, iolist_to_binary([get_stats_key(ReqdKeys), " ", VBucketStr]),
      ReqdKeys).

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
