%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_bucket_worker).

-behavior(gen_server).

-export([start_link/0]).
-export([start_transient_buckets/1, stop_transient_buckets/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include("cut.hrl").
-include("ns_common.hrl").

-define(SERVER, ?MODULE).
-define(TIMEOUT, ?get_timeout(default, 60000)).

-record(state, {running_buckets   :: [bucket_name()],
                running_uploaders :: [bucket_name()],
                transient_buckets :: undefined |
                                     {pid(), reference(), [bucket_name()]}}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec start_transient_buckets(Buckets) -> Result when
      Buckets       :: [bucket_name()],
      Result        :: {ok, Ref} | {error, Error},
      Ref           :: reference(),
      Error         :: BucketError | ConflictError,
      BucketError   :: {buckets_already_running, Buckets},
      ConflictError :: {conflict, pid()}.
start_transient_buckets(Buckets) ->
    gen_server:call(?SERVER,
                    {start_transient_buckets, Buckets, self()}, ?TIMEOUT).

-spec stop_transient_buckets(Ref) -> Result when
      Ref            :: reference(),
      Result         :: {ok, BucketStatuses} | {error, Error},
      BucketStatuses :: [{bucket_name(), BucketStatus}],
      BucketStatus   :: running | not_running,
      Error          :: bad_reference.
stop_transient_buckets(Ref) ->
    gen_server:call(?SERVER, {stop_transient_buckets, Ref}, ?TIMEOUT).

%% callbacks
init([]) ->
    chronicle_compat_events:notify_if_key_changes(
      fun ns_bucket:buckets_change/1, update_buckets),

    self() ! update_buckets,
    {ok, #state{running_buckets = [],
                running_uploaders = [],
                transient_buckets = undefined}}.

handle_call({start_transient_buckets, Buckets, Pid}, _From, State) ->
    handle_start_transient_buckets(Buckets, Pid, State);
handle_call({stop_transient_buckets, Ref}, _From, State) ->
    handle_stop_transient_buckets(Ref, State);
handle_call(Call, From, State) ->
    ?log_warning("Received unexpected "
                 "call ~p from ~p. State:~n~p", [Call, From, State]),
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_warning("Received unexpected cast ~p. State:~n~p", [Cast, State]),
    {noreply, State}.

handle_info(update_buckets, State) ->
    {noreply, update_buckets(State)};
handle_info({'DOWN', MRef, _, Pid, Reason}, State) ->
    {noreply, handle_down(MRef, Pid, Reason, State)};
handle_info(Msg, State) ->
    ?log_warning("Received unexpected message ~p. State:~n~p", [Msg, State]),
    {noreply, State}.

update_buckets(#state{running_buckets = RunningBuckets} = State) ->
    NewBuckets = compute_buckets_to_run(State),

    ToStart = NewBuckets -- RunningBuckets,
    ToStop  = RunningBuckets -- NewBuckets,

    start_buckets(ToStart),
    stop_buckets(ToStop),

    State1 = manage_terse_bucket_uploaders(State),

    cleanup_orphan_buckets(State1),

    State1#state{running_buckets = NewBuckets}.

%% The terse bucket uploader (TBU) sends CCCP information to memcached so
%% that SDKs can bootstrap against memcached. The TBU does this on all
%% nodes even if the "real" bucket doesn't run on this node and even
%% if this node isn't a kv node.
%% Sending the CCCP info to memcached results in a config-only bucket
%% getting created. If the "real" bucket does run on this node then the
%% create_bucket request to memcached will replace the config-only bucket.
manage_terse_bucket_uploaders(
  #state{running_uploaders = RunningUploaders} = State) ->
    AllBuckets = ns_bucket:get_bucket_names(),
    ToStart = AllBuckets -- RunningUploaders,
    ToStop = RunningUploaders -- AllBuckets,

    start_uploaders(ToStart),
    stop_uploaders(ToStop),

    State#state{running_uploaders = AllBuckets}.

compute_buckets_to_run(State) ->
    RegularBuckets = ns_bucket:node_bucket_names(node()),
    TransientBuckets = transient_buckets(State),
    lists:usort(TransientBuckets ++ RegularBuckets).

start_buckets(Buckets) ->
    lists:foreach(fun start_one_bucket/1, Buckets).

start_one_bucket(Bucket) ->
    ?log_debug("Starting new bucket: ~p", [Bucket]),
    ok = ns_bucket_sup:start_bucket(Bucket).

stop_buckets(Buckets) ->
    lists:foreach(fun stop_one_bucket/1, Buckets).

stop_one_bucket(Bucket) ->
    ?log_debug("Stopping child for dead bucket: ~p", [Bucket]),
    TimeoutPid =
        diag_handler:arm_timeout(
          30000,
          fun (_) ->
                  ?log_debug("Observing slow bucket supervisor "
                             "stop request for ~p", [Bucket]),
                  timeout_diag_logger:log_diagnostics(
                    {slow_bucket_stop, Bucket})
          end),

    try
        ok = ns_bucket_sup:stop_bucket(Bucket)
    after
        diag_handler:disarm_timeout(TimeoutPid)
    end.

start_uploaders(Buckets) ->
    lists:foreach(fun start_one_uploader/1, Buckets).

start_one_uploader(Bucket) ->
    ?log_debug("Starting uploader for bucket: ~p", [Bucket]),
    ok = ns_bucket_sup:start_uploader(Bucket).

stop_uploaders(Buckets) ->
    lists:foreach(fun stop_one_uploader/1, Buckets).

stop_one_uploader(Bucket) ->
    ?log_debug("Stopping uploader for bucket: ~p", [Bucket]),
    ok = ns_bucket_sup:stop_uploader(Bucket),
    delete_config_only_bucket(Bucket).

delete_config_only_bucket(Bucket) ->
    case (catch ns_memcached:delete_bucket(Bucket,
                                           [{type, 'ClusterConfigOnly'}])) of
        ok ->
            ok;
        {memcached_error, key_enoent, undefined} ->
            %% Bucket deleted already by ns_memcached terminating
            ok;
        Error ->
            %% The deletion failed which leaves the bucket in memcached. The
            %% resources used by a config-only bucket are minimal and the
            %% deletion will be retried the next time update_buckets is run.
            ?log_error("Failed to delete config-only bucket ~p: ~p",
                       [Bucket, Error])
    end.

handle_start_transient_buckets(Buckets, Pid, State) ->
    %% Make sure we start/stop all buckets that need to be
    %% started/stopped. Since starting a transient bucket while it already
    %% exists on the node is considered an error, this increases the
    %% probability of catching bugs.
    UpdatedState = update_buckets(State),

    case functools:sequence_(
           [?cut(check_no_conflicts(UpdatedState)),
            ?cut(check_buckets_not_running(Buckets, UpdatedState))]) of
        ok ->
            ?log_debug("Starting transient buckets ~p per request from ~p",
                       [Buckets, Pid]),
            MRef = erlang:monitor(process, Pid),
            {reply, {ok, MRef},
             update_buckets(UpdatedState#state{transient_buckets =
                                                   {Pid, MRef, Buckets}})};
        Error ->
            {reply, Error, UpdatedState}
    end.

check_buckets_not_running(Buckets, #state{running_buckets = Running}) ->
    Intersection = lists:filter(lists:member(_, Running), Buckets),
    case Intersection of
        [] ->
            ok;
        _ ->
            {error, {buckets_already_running, Intersection}}
    end.

check_no_conflicts(#state{transient_buckets = Transient}) ->
    case Transient of
        undefined ->
            ok;
        {Pid, _, _} ->
            {error, {conflict, Pid}}
    end.

handle_stop_transient_buckets(Ref, State) ->
    case check_transient_buckets_ref(Ref, State) of
        ok ->
            erlang:demonitor(Ref, [flush]),

            Buckets  = transient_buckets(State),
            NewState = update_buckets(
                         State#state{transient_buckets = undefined}),

            %% We expect that when a transient bucket is removed, it will
            %% continue running as a normal bucket. So let's check that.
            Statuses = [{Bucket, bucket_status(Bucket, NewState)} ||
                           Bucket <- Buckets],

            ?log_debug("Buckets ~p are no longer transient. Statuses:~n~p",
                       [Buckets, Statuses]),

            {reply, {ok, Statuses}, NewState};
        Error ->
            {reply, Error, State}
    end.

check_transient_buckets_ref(Ref, #state{transient_buckets = Transient}) ->
    case Transient of
        {_Pid, OurRef, _} when Ref =:= OurRef ->
            ok;
        _ ->
            {error, bad_reference}
    end.

bucket_status(Bucket, #state{running_buckets = Running}) ->
    case lists:member(Bucket, Running) of
        true ->
            running;
        false ->
            not_running
    end.

handle_down(MRef, Pid, Reason,
            #state{transient_buckets = {Pid, MRef, Buckets}} = State) ->
    ?log_error("Process ~p holding transient bucket ~p died with reason ~p.",
               [Pid, Buckets, Reason]),
    update_buckets(State#state{transient_buckets = undefined}).

transient_buckets(#state{transient_buckets = undefined}) ->
    [];
transient_buckets(#state{transient_buckets = {_Pid, _MRef, Buckets}}) ->
    Buckets.

%% Check if memcached has any config-only buckets which are not associated
%% with a running uploader and delete them.
cleanup_orphan_buckets(#state{running_uploaders = RunningUploaders}) ->
    Buckets = ns_memcached:get_all_buckets_details(),

    lists:foreach(
      fun({Bucket}) ->
              Name = binary_to_list(proplists:get_value(<<"name">>, Bucket)),
              IsOrphan = not lists:member(Name, RunningUploaders),
              Type = binary_to_list(proplists:get_value(<<"type">>, Bucket)),
              case {IsOrphan, Type} of
                  {true, "ClusterConfigOnly"} ->
                      ?log_debug("Deleting orphan config-only bucket ~p",
                                 [Name]),
                      delete_config_only_bucket(Name),
                      ok;
                  {_, _} ->
                      ok
              end
      end, Buckets).
