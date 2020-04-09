%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2016 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(capi_set_view_manager).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link_remote/2]).

%% API
-export([set_vbucket_states/3,
         wait_index_updated/2, initiate_indexing/1,
         get_safe_purge_seqs/1]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").
-include("ns_common.hrl").

-record(state, {bucket :: bucket_name(),
                local_docs = [] :: [#doc{}],
                num_vbuckets :: non_neg_integer(),
                use_replica_index :: boolean(),
                wanted_states :: [missing | active | replica],
                rebalance_states :: [rebalance_vbucket_state()]}).

set_vbucket_states(Bucket, WantedStates, RebalanceStates) ->
    gen_server:call(server(Bucket), {set_vbucket_states, WantedStates, RebalanceStates}, infinity).

wait_index_updated(Bucket, VBucket) ->
    gen_server:call(server(Bucket), {wait_index_updated, VBucket}, infinity).

initiate_indexing(Bucket) ->
    gen_server:call(server(Bucket), initiate_indexing, infinity).

-define(csv_call_all(Call, Bucket, DDocId, RestArgs),
        ok = ?csv_call(Call, mapreduce_view, Bucket, DDocId, RestArgs),
        ok = ?csv_call(Call, spatial_view, Bucket, DDocId, RestArgs)).

-define(csv_call(Call, Kind, Bucket, DDocId, RestArgs),
        %% hack to not introduce any variables in the caller environment
        ((fun () ->
                  %% we may want to downgrade it to ?views_debug at some point
                  ?views_debug("~nCalling couch_set_view:~p(~s, <<\"~s\">>, <<\"~s\">>, ~w)", [Call, Kind, Bucket, DDocId, RestArgs]),
                  try
                      {__Time, __Result} = timer:tc(couch_set_view, Call, [Kind, Bucket, DDocId | RestArgs]),

                      ?views_debug("~ncouch_set_view:~p(~s, <<\"~s\">>, <<\"~s\">>, ~w) returned ~p in ~bms",
                                   [Call, Kind, Bucket, DDocId, RestArgs, __Result, __Time div 1000]),
                      __Result
                  catch
                      __T:__E ->
                          ?views_debug("~ncouch_set_view:~p(~s, <<\"~s\">>, <<\"~s\">>, ~w) raised~n~p:~p",
                                       [Call, Kind, Bucket, DDocId, RestArgs, __T, __E]),

                          %% rethrowing the exception
                          __Stack = erlang:get_stacktrace(),
                          erlang:raise(__T, __E, __Stack)
                  end
          end)())).


compute_index_states(WantedStates, RebalanceStates) ->
    AllVBs = lists:seq(0, erlang:length(WantedStates)-1),
    Triples = lists:zip3(AllVBs,
                         WantedStates,
                         RebalanceStates),
                                                % Ensure Active, Passive and Replica are ordsets
    {Active, Passive, Replica} = lists:foldr(
                                   fun ({VBucket, WantedState, TmpState}, {AccActive, AccPassive, AccReplica}) ->
                                           case {WantedState, TmpState} of
                                               %% {replica, passive} ->
                                               %%     {AccActibe, [VBucket | AccPassive], [VBucket | AccReplica]};
                                               {_, passive} ->
                                                   {AccActive, [VBucket | AccPassive], AccReplica};
                                               {active, _} ->
                                                   {[VBucket | AccActive], AccPassive, AccReplica};
                                               {replica, _} ->
                                                   {AccActive, AccPassive, [VBucket | AccReplica]};
                                               _ ->
                                                   {AccActive, AccPassive, AccReplica}
                                           end
                                   end,
                                   {[], [], []},
                                   Triples),
    AllMain = ordsets:union(Active, Passive),
    MainCleanup = ordsets:subtract(AllVBs, AllMain),
    ReplicaCleanup = ordsets:subtract(ordsets:subtract(AllVBs, Replica), Active),
    PauseVBuckets = [VBucket
                     || {VBucket, WantedState, TmpState} <- Triples,
                        TmpState =:= paused,
                        begin
                            active = WantedState,
                            true
                        end],
    UnpauseVBuckets = ordsets:subtract(AllVBs, PauseVBuckets),
    {Active, Passive, MainCleanup, Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets}.

do_apply_vbucket_states(SetName, Active, Passive, MainCleanup, Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets, State) ->
    DDocIds = get_live_ddoc_ids(State),
    RVs = [{DDocId, (catch begin
                               apply_index_states(SetName, DDocId,
                                                  Active, Passive, MainCleanup, Replica, ReplicaCleanup,
                                                  PauseVBuckets, UnpauseVBuckets,
                                                  State),
                               ok
                           end)}
           || DDocId <- DDocIds],
    BadDDocs = [Pair || {_Id, Val} = Pair <- RVs,
                        Val =/= ok],
    case BadDDocs of
        [] ->
            ok;
        _ ->
            ?log_error("Failed to apply index states for the following ddocs:~n~p", [BadDDocs])
    end,
    ok.

change_vbucket_states(#state{bucket = Bucket,
                             wanted_states = WantedStates,
                             rebalance_states = RebalanceStates} = State) ->
    SetName = list_to_binary(Bucket),
    {Active, Passive, MainCleanup, Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets} =
        compute_index_states(WantedStates, RebalanceStates),
    do_apply_vbucket_states(SetName, Active, Passive, MainCleanup, Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets, State).

start_link_remote(Node, Bucket) ->
    ns_bucket_sup:ignore_if_not_couchbase_bucket(
      Bucket,
      fun (BucketConfig) ->
              UseReplicaIndex = (proplists:get_value(replica_index, BucketConfig) =/= false),
              VBucketsCount = proplists:get_value(num_vbuckets, BucketConfig),

              misc:start_link(Node, misc, turn_into_gen_server,
                              [{local, server(Bucket)},
                               ?MODULE,
                               {Bucket, UseReplicaIndex, VBucketsCount}, []])
      end).

init({Bucket, UseReplicaIndex, NumVBuckets}) ->
    Self = self(),
    proc_lib:init_ack({ok, Self}),

    ok = misc:wait_for_local_name(couch_server, 10000),

    wait_for_bucket_to_start(Bucket, 0),

    {_, Docs} =
        capi_ddoc_manager:subscribe_link(
          Bucket,
          fun (Event) ->
                  case Event of
                      {suspend, _} ->
                          ok = gen_server:call(Self, Event, infinity);
                      {resume, _, _} ->
                          Self ! Event
                  end
          end),

    State = #state{bucket=Bucket,
                   local_docs = Docs,
                   num_vbuckets = NumVBuckets,
                   use_replica_index=UseReplicaIndex,
                   wanted_states = [],
                   rebalance_states = []},

    [maybe_define_group(DDocId, State)
     || DDocId <- get_live_ddoc_ids(State)],

    gen_server:enter_loop(?MODULE, [], State).

wait_for_bucket_to_start(Bucket, Time) ->
    RV = ns_memcached:perform_very_long_call(
           fun (_Sock) ->
                   ?log_debug("Engine for bucket ~p is created in memcached", [Bucket]),
                   {reply, ok}
           end, Bucket),
    case RV of
        ok ->
            ok;
        {error, {select_bucket_failed, {memcached_error, key_enoent, _}}} = RV ->
            case Time of
                10000 ->
                    ?log_error("Engine for bucket ~p didn't start in ~p ms. Exit.", [Bucket, Time]),
                    exit(RV);
                _ ->
                    timer:sleep(500),

                    Time1 = Time + 500,
                    ?log_debug("Waiting for engine to start. Bucket: ~p, Wait time: ~p ms.",
                               [Bucket, Time1]),
                    wait_for_bucket_to_start(Bucket, Time1)
            end;
        Error ->
            exit(Error)
    end.

get_live_ddoc_ids(#state{local_docs = Docs}) ->
    [Id || #doc{id = Id, deleted = false} <- Docs].

handle_call({wait_index_updated, VBucket}, From, State) ->
    ok = proc_lib:start_link(erlang, apply, [fun do_wait_index_updated/4, [From, VBucket, self(), State]]),
    {noreply, State};
handle_call(initiate_indexing, _From, State) ->
    BinBucket = list_to_binary(State#state.bucket),
    DDocIds = get_live_ddoc_ids(State),
    [case DDocId of
         <<"_design/dev_", _/binary>> -> ok;
         _ ->
             couch_set_view:trigger_update(mapreduce_view, BinBucket, DDocId, 0),
             couch_set_view:trigger_update(spatial_view, BinBucket, DDocId, 0)
     end || DDocId <- DDocIds],
    {reply, ok, State};
handle_call({set_vbucket_states, WantedStates, RebalanceStates}, _From,
            State) ->
    State2 = State#state{wanted_states = WantedStates,
                         rebalance_states = RebalanceStates},
    case State2 =:= State of
        true ->
            {reply, ok, State};
        false ->
            change_vbucket_states(State2),
            {reply, ok, State2}
    end;
handle_call({suspend, Ref}, From, #state{local_docs = Docs} = State) ->
    gen_server:reply(From, ok),

    receive
        {resume, Ref, {error, #doc{id = DDocId}, Error}} ->
            ?log_info("Resume operation after unsuccessful update: id ~s, error: ~p",
                      [DDocId, Error]),
            {noreply, State};
        {resume, Ref, {ok, #doc{id = DDocId,
                                deleted = Deleted} = Doc}} ->
            ?log_info("Processing ddoc update: id ~s, deleted ~p",
                      [DDocId, Deleted]),

            NewDocs = lists:keystore(DDocId, #doc.id, Docs, Doc),
            NewState = State#state{local_docs = NewDocs},

            case Deleted of
                false ->
                    %% we need to redefine set view whenever document changes;
                    %% but previous group for current value of design document
                    %% can still be alive; thus using maybe_define_group
                    maybe_define_group(DDocId, NewState),
                    change_vbucket_states(NewState);
                true ->
                    ok
            end,

            {noreply, NewState}
    end.

handle_cast(Request, State) ->
    {stop, {unexpected_cast, Request}, State}.

handle_info(Info, State) ->
    ?log_info("Ignoring unexpected message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

server(Bucket) when is_binary(Bucket) ->
    server(erlang:binary_to_list(Bucket));
server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

maybe_define_group(DDocId,
                   #state{bucket = Bucket,
                          num_vbuckets = NumVBuckets,
                          use_replica_index = UseReplicaIndex} = _State) ->
    SetName = list_to_binary(Bucket),
    Params = #set_view_params{max_partitions=NumVBuckets,
                              active_partitions=[],
                              passive_partitions=[],
                              use_replica_index=UseReplicaIndex},

    try
        ok = ?csv_call(define_group, mapreduce_view, SetName, DDocId, [Params])
    catch
        throw:view_already_defined ->
            already_defined
    end,
    try
        ok = ?csv_call(define_group, spatial_view, SetName, DDocId, [Params])
    catch
        throw:view_already_defined ->
            already_defined
    end.

define_and_reapply_index_states(SetName, DDocId, Active, Passive, Cleanup,
                                Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets,
                                State, OriginalException, NowException) ->
    ?log_info("Got view_undefined exception:~n~p", [NowException]),
    case OriginalException of
        undefined ->
            ok;
        _ ->
            ?log_error("Got second exception after trying to redefine undefined view:~n~p~nOriginal exception was:~n~p",
                       [NowException, OriginalException]),
            erlang:apply(erlang, raise, OriginalException)
    end,
    maybe_define_group(DDocId, State),
    apply_index_states(SetName, DDocId, Active, Passive, Cleanup,
                       Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets,
                       State, NowException).

apply_index_states(SetName, DDocId, Active, Passive, Cleanup,
                   Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets,
                   State) ->
    apply_index_states(SetName, DDocId, Active, Passive, Cleanup,
                       Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets,
                       State, undefined).

apply_index_states(SetName, DDocId, Active, Passive, Cleanup,
                   Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets,
                   #state{use_replica_index = UseReplicaIndex} = State,
                   PastException) ->

    try
        PausingOn = cluster_compat_mode:is_index_pausing_on(),
        case PausingOn of
            false ->
                ok;
            true ->
                ?csv_call_all(mark_partitions_indexable,
                              SetName, DDocId, [UnpauseVBuckets])
        end,

        %% this should go first because some of the replica vbuckets might
        %% need to be cleaned up from main index
        ?csv_call_all(set_partition_states,
                      SetName, DDocId, [Active, Passive, Cleanup]),

        case UseReplicaIndex of
            true ->
                ?csv_call_all(add_replica_partitions,
                              SetName, DDocId, [Replica]),
                ?csv_call_all(remove_replica_partitions,
                              SetName, DDocId, [ReplicaCleanup]);
            false ->
                ok
        end,

        case PausingOn of
            false ->
                ok;
            true ->
                ?csv_call_all(mark_partitions_unindexable,
                              SetName, DDocId, [PauseVBuckets])
        end

    catch
        T:E ->
            Stack = erlang:get_stacktrace(),
            Exc = [T, E, Stack],
            Undefined = case Exc of
                            [throw, {error, view_undefined}, _] ->
                                true;
                            [error, view_undefined, _] ->
                                true;
                            _ ->
                                false
                        end,
            case Undefined of
                true ->
                    define_and_reapply_index_states(
                      SetName, DDocId, Active, Passive, Cleanup,
                      Replica, ReplicaCleanup, PauseVBuckets, UnpauseVBuckets,
                      State, PastException, Exc);
                _ ->
                    erlang:raise(T, E, Stack)
            end
    end.

do_wait_index_updated({Pid, _} = From, VBucket,
                      ParentPid, #state{bucket = Bucket} = State) ->
    DDocIds = get_live_ddoc_ids(State),
    BinBucket = list_to_binary(Bucket),
    Refs = lists:foldl(
             fun (DDocId, Acc) ->
                     case DDocId of
                         <<"_design/dev_", _/binary>> -> Acc;
                         _ ->
                             RefMapReduce =
                                 couch_set_view:monitor_partition_update(
                                   mapreduce_view, BinBucket, DDocId,
                                   VBucket),
                             couch_set_view:trigger_update(
                               mapreduce_view, BinBucket, DDocId, 0),
                             RefSpatial =
                                 couch_set_view:monitor_partition_update(
                                   spatial_view, BinBucket, DDocId,
                                   VBucket),
                             couch_set_view:trigger_update(
                               spatial_view, BinBucket, DDocId, 0),
                             [RefMapReduce, RefSpatial | Acc]
                     end
             end, [], DDocIds),
    ?log_debug("References to wait: ~p (~p, ~p)", [Refs, Bucket, VBucket]),
    ParentMRef = erlang:monitor(process, ParentPid),
    CallerMRef = erlang:monitor(process, Pid),
    proc_lib:init_ack(ok),
    erlang:unlink(ParentPid),
    [receive
         {Ref, Msg} -> % Ref is bound
             case Msg of
                 updated -> ok;
                 _ ->
                     ?log_debug("Got unexpected message from ddoc monitoring. Assuming that's shutdown: ~p", [Msg])
             end;
         {'DOWN', ParentMRef, _, _, _} = DownMsg ->
             ?log_debug("Parent died: ~p", [DownMsg]),
             exit({parent_exited, DownMsg});
         {'DOWN', CallerMRef, _, _, _} = DownMsg ->
             ?log_debug("Caller died: ~p", [DownMsg]),
             exit(normal)
     end
     || Ref <- Refs],
    ?log_debug("All refs fired"),
    gen_server:reply(From, ok).

-spec get_safe_purge_seqs(BucketName :: binary()) -> [{non_neg_integer(), non_neg_integer()}].
get_safe_purge_seqs(BucketName) ->
    lists:foldl(
      fun (<<"_design/dev_", _/binary>>, SafeSeqs) ->
              %% we ignore dev design docs
              SafeSeqs;
          (DDocId, SafeSeqs) ->
              case (catch couch_set_view:get_indexed_seqs(mapreduce_view, BucketName, DDocId, prod)) of
                  {ok, IndexSeqs} ->
                      misc:ukeymergewith(
                        fun ({Vb, SeqA}, {_, SeqB}) ->
                                {Vb, case SeqA < SeqB of
                                         true -> SeqA;
                                         _ -> SeqB
                                     end}
                        end, 1, SafeSeqs, IndexSeqs);
                  {not_found, missing} ->
                      SafeSeqs;
                  Error ->
                      ?log_error("ignoring unexpected get_indexed_seqs(~s, ~s) failure: ~p", [BucketName, DDocId, Error]),
                      SafeSeqs
              end
      end, [], capi_utils:fetch_ddoc_ids(BucketName)).
