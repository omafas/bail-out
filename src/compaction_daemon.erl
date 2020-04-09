%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2018 Couchbase, Inc.
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

-module(compaction_daemon).

-behaviour(gen_server).

-include("ns_common.hrl").

%% API
-export([start_link/0,
         inhibit_view_compaction/2, uninhibit_view_compaction/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(compaction_state, {buckets_to_compact :: [binary()],
                           compactor_pid :: undefined | pid(),
                           scheduler,
                           compactor_fun :: fun(),
                           compactor_config}).

-record(state, {kv_compaction :: #compaction_state{},
                views_compaction :: #compaction_state{},
                master_compaction :: #compaction_state{},

                %% bucket for which view compaction is inhibited (note
                %% it's also set when we're running 'uninhibit' views
                %% compaction as 'final stage')
                view_compaction_inhibited_bucket :: undefined | binary(),
                %% if view compaction is inhibited this field will
                %% hold monitor ref of the guy who requested it. If
                %% that guy dies, inhibition will be canceled
                view_compaction_inhibited_ref :: (undefined | reference()),
                %% when we have 'uninhibit' request, we're starting
                %% our compaction with 'forced' view compaction asap
                %%
                %% requested indicates we have this request
                view_compaction_uninhibit_requested = false :: boolean(),
                %% started indicates we have started such compaction
                %% and waiting for it to complete
                view_compaction_uninhibit_started = false :: boolean(),
                %% this is From to reply to when such compaction is
                %% done
                view_compaction_uninhibit_requested_waiter,

                %% do we need to stop kv compaction during uninhibition
                view_compaction_uninhibit_stop_kv = false :: boolean(),

                %% mapping from compactor pid to #forced_compaction{}
                running_forced_compactions,
                %% reverse mapping from #forced_compaction{} to compactor pid
                forced_compaction_pids}).

-record(config, {db_fragmentation,
                 view_fragmentation,
                 allowed_period,
                 parallel_view_compact = false,
                 do_purge = false,
                 daemon}).

-record(daemon_config, {check_interval,
                        min_db_file_size,
                        min_view_file_size}).

-record(period, {from_hour,
                 from_minute,
                 to_hour,
                 to_minute,
                 abort_outside}).

-record(forced_compaction, {type :: bucket | bucket_purge | db | view,
                            name :: binary()}).

-define(MASTER_COMPACTION_INTERVAL, 3600).

%% API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_last_rebalance_or_failover_timestamp() ->
    case ns_config:search_raw(ns_config:get(), counters) of
        {value, [{'_vclock', VClock} | _]} ->
            GregorianSecondsTS = vclock:get_latest_timestamp(VClock),
            GregorianSecondsTS - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}});
        _ -> 0
    end.

%% While Pid is alive prevents autocompaction of views for given
%% bucket and given node. Returned Ref can be used to uninhibit
%% autocompaction.
%%
%% If views of given bucket are being compacted right now, it'll wait
%% for end of compaction rather than aborting it.
%%
%% Attempt to inhibit already inhibited compaction will result in nack.
%%
%% Note: we assume that caller of both inhibit_view_compaction and
%% uninhibit_view_compaction is somehow related to Pid and will die
%% automagically if Pid dies, because autocompaction daemon may
%% actually forget about this calls.
-spec inhibit_view_compaction(bucket_name(), pid()) ->
                                     {ok, reference()} | nack.
inhibit_view_compaction(Bucket, Pid) ->
    gen_server:call(?MODULE,
                    {inhibit_view_compaction, list_to_binary(Bucket), Pid},
                    infinity).


%% Using Ref returned from inhibit_view_compaction undoes it's
%% effect. It'll also consider kicking views autocompaction for given
%% bucket asap. And it's do it _before_ replying.
-spec uninhibit_view_compaction(bucket_name(), reference()) -> ok | nack.
uninhibit_view_compaction(Bucket, Ref) ->
    gen_server:call(?MODULE,
                    {uninhibit_view_compaction, list_to_binary(Bucket), Ref},
                    infinity).

%% gen_server callbacks
init([]) ->
    process_flag(trap_exit, true),

    Self = self(),

    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({buckets, _}) ->
              Self ! config_changed;
          ({autocompaction, _}) ->
              Self ! config_changed;
          ({{node, Node, compaction_daemon}, _}) when node() =:= Node ->
              Self ! config_changed;
          (_) ->
              ok
      end),

    ets:new(compaction_daemon, [protected, named_table, set]),

    {ok, KVThrottle} = new_concurrency_throttle:start_link({1, kv_throttle}, undefined),
    ets:insert(compaction_daemon, {kv_throttle, KVThrottle}),

    CheckInterval = get_check_interval(ns_config:get()),

    {ok, #state{kv_compaction =
                    init_scheduled_compaction(CheckInterval, compact_kv,
                                              fun spawn_scheduled_kv_compactor/2),
                views_compaction =
                    init_scheduled_compaction(CheckInterval, compact_views,
                                              fun spawn_scheduled_views_compactor/2),
                master_compaction =
                    init_scheduled_compaction(?MASTER_COMPACTION_INTERVAL, compact_master,
                                              fun spawn_scheduled_master_db_compactor/2),
                running_forced_compactions=dict:new(),
                forced_compaction_pids=dict:new()}}.

handle_cast(Message, State) ->
    ?log_warning("Got unexpected cast ~p:~n~p", [Message, State]),
    {noreply, State}.

handle_call({force_compact_bucket, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=bucket, name=Bucket},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                Configs = compaction_config(Bucket),
                Pid = spawn_forced_bucket_compactor(Bucket, Configs),
                register_forced_compaction(Pid, Compaction, State)
        end,
    {reply, ok, NewState};
handle_call({force_purge_compact_bucket, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=bucket_purge, name=Bucket},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                {Config, Props} = compaction_config(Bucket),
                Pid = spawn_forced_bucket_compactor(Bucket,
                                                    {Config#config{do_purge = true}, Props}),
                State1 = register_forced_compaction(Pid, Compaction, State),
                ale:info(?USER_LOGGER,
                         "Started deletion purge compaction for bucket `~s`",
                         [Bucket]),
                State1
        end,
    {reply, ok, NewState};
handle_call({force_compact_db_files, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=db, name=Bucket},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                {Config, _} = compaction_config(Bucket),
                OriginalTarget = {[{type, db}]},
                Pid = spawn_dbs_compactor(Bucket, Config, true, OriginalTarget),
                register_forced_compaction(Pid, Compaction, State)
        end,
    {reply, ok, NewState};
handle_call({force_compact_view, Bucket, DDocId}, _From, State) ->
    Name = <<Bucket/binary, $/, DDocId/binary>>,
    Compaction = #forced_compaction{type=view, name=Name},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                {Config, _} = compaction_config(Bucket),
                OriginalTarget = {[{type, view},
                                   {name, DDocId}]},
                Pid = spawn_view_compactor(Bucket, DDocId, Config, true,
                                           OriginalTarget),
                register_forced_compaction(Pid, Compaction, State)
        end,
    {reply, ok, NewState};
handle_call({cancel_forced_bucket_compaction, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=bucket, name=Bucket},
    {reply, ok, maybe_cancel_compaction(Compaction, State)};
handle_call({cancel_forced_db_compaction, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=db, name=Bucket},
    {reply, ok, maybe_cancel_compaction(Compaction, State)};
handle_call({cancel_forced_view_compaction, Bucket, DDocId}, _From, State) ->
    Name = <<Bucket/binary, $/, DDocId/binary>>,
    Compaction = #forced_compaction{type=view, name=Name},
    {reply, ok, maybe_cancel_compaction(Compaction, State)};
handle_call({inhibit_view_compaction, BinBucketName, Pid}, _From,
            #state{view_compaction_inhibited_ref = OldRef,
                   view_compaction_uninhibit_requested = UninhibitRequested,
                   views_compaction =
                       #compaction_state{buckets_to_compact = BucketsToCompact,
                                         compactor_pid = CompactorPid}} = State) ->
    case OldRef =/= undefined orelse UninhibitRequested of
        true ->
            {reply, nack, State};
        _ ->
            MRef = erlang:monitor(process, Pid),
            NewState = State#state{
                         view_compaction_inhibited_bucket = BinBucketName,
                         view_compaction_inhibited_ref = MRef,
                         view_compaction_uninhibit_requested = false},
            case CompactorPid =/= undefined andalso hd(BucketsToCompact) =:= BinBucketName of
                true ->
                    ?log_info("Killing currently running index compactor to handle inhibit_view_compaction request for ~s", [BinBucketName]),
                    erlang:exit(CompactorPid, shutdown);
                false ->
                    ok
            end,
            {reply, {ok, MRef}, NewState}
    end;
handle_call({uninhibit_view_compaction, BinBucketName, MRef}, From,
                  #state{view_compaction_inhibited_bucket = BinBucketName,
                         view_compaction_inhibited_ref = MRef,
                         view_compaction_uninhibit_requested = false,
                         views_compaction =
                             #compaction_state{compactor_pid = CompactorPid,
                                               scheduler = Scheduler} = CompactionState
                        } = State) ->
    erlang:demonitor(MRef, [flush]),

    case ddoc_names(BinBucketName) of
        [] ->
            {reply, ok, State#state{view_compaction_inhibited_ref = undefined,
                                    view_compaction_inhibited_bucket = undefined,
                                    view_compaction_uninhibit_requested_waiter = undefined,
                                    view_compaction_uninhibit_requested = false,
                                    view_compaction_uninhibit_started = false}};
        _ ->
            {Config, _ConfigProps} = compaction_config(BinBucketName),
            StopKV = not Config#config.parallel_view_compact,
            NewState =
                case CompactorPid of
                    undefined ->
                        NewCompactionState =
                            CompactionState#compaction_state{
                              scheduler = compaction_scheduler:cancel(Scheduler)},
                        State#state{
                          view_compaction_uninhibit_started = true,
                          views_compaction =
                              compact_views_for_paused_bucket(BinBucketName, NewCompactionState)};
                    _ ->
                        State#state{view_compaction_uninhibit_started = false}
                end,

            {noreply, NewState#state{view_compaction_uninhibit_requested_waiter = From,
                                     view_compaction_inhibited_ref = undefined,
                                     view_compaction_uninhibit_requested = true,
                                     view_compaction_uninhibit_stop_kv = StopKV}}
    end;
handle_call({uninhibit_view_compaction, _BinBucketName, _MRef} = Msg, _From, State) ->
    ?log_debug("Sending back nack on uninhibit_view_compaction: ~p, state: ~p", [Msg, State]),
    {reply, nack, State};
handle_call({is_bucket_inhibited, BinBucketName}, _From,
            #state{view_compaction_inhibited_bucket = BinBucketName,
                   view_compaction_uninhibit_requested = false} = State) ->
    {reply, true, State};
handle_call({is_bucket_inhibited, _BinBucketName}, _From, State) ->
    {reply, false, State};
handle_call(is_uninhibited_requested, _From,
            #state{view_compaction_uninhibit_requested = Requested,
                   view_compaction_uninhibit_stop_kv = StopKV} = State) ->
    {reply, {Requested, StopKV}, State};
handle_call(Event, _From, State) ->
    ?log_warning("Got unexpected call ~p:~n~p", [Event, State]),
    {reply, ok, State}.

handle_info(compact_kv, #state{kv_compaction=CompactionState} = State) ->
    {noreply, State#state{kv_compaction=
                              process_scheduler_message(compact_kv, CompactionState)}};
handle_info(compact_master, #state{master_compaction=CompactionState} = State) ->
    {noreply, State#state{master_compaction=
                              process_scheduler_message(compact_master, CompactionState)}};
handle_info(compact_views, #state{views_compaction=CompactionState} = State) ->
    {noreply, State#state{views_compaction=
                              process_scheduler_message(compact_views, CompactionState)}};
handle_info({'DOWN', MRef, _, _, _}, #state{view_compaction_inhibited_ref=MRef,
                                            view_compaction_inhibited_bucket=BinBucket}=State) ->
    ?log_debug("Looks like vbucket mover inhibiting view compaction for for bucket \"~s\" is dead."
               " Canceling inhibition", [BinBucket]),
    {noreply, State#state{view_compaction_inhibited_ref = undefined,
                          view_compaction_inhibited_bucket = undefined}};
handle_info({'EXIT', Compactor, Reason},
            #state{kv_compaction=#compaction_state{compactor_pid=Compactor}=
                       CompactorState} = State) ->
    log_compactors_exit(Reason, CompactorState),
    {noreply, State#state{kv_compaction=
                              process_compactors_exit(Reason, CompactorState)}};
handle_info({'EXIT', Compactor, Reason},
            #state{master_compaction=#compaction_state{compactor_pid=Compactor}=
                       CompactorState} = State) ->
    log_compactors_exit(Reason, CompactorState),
    {noreply, State#state{master_compaction=
                              process_compactors_exit(Reason, CompactorState)}};
handle_info({'EXIT', Compactor, Reason},
            #state{views_compaction=#compaction_state{compactor_pid = Compactor,
                                                      buckets_to_compact = Buckets} =
                       CompactorState} = State) ->
    log_compactors_exit(Reason, CompactorState),

    NewState = case (Buckets =:= [State#state.view_compaction_inhibited_bucket]
                     andalso State#state.view_compaction_uninhibit_started) of
                   true ->
                       gen_server:reply(State#state.view_compaction_uninhibit_requested_waiter, ok),
                       State#state{view_compaction_inhibited_bucket = undefined,
                                   view_compaction_uninhibit_requested_waiter = undefined,
                                   view_compaction_uninhibit_requested = false,
                                   view_compaction_uninhibit_started = false};
                   _ ->
                       State
               end,

    case NewState#state.view_compaction_uninhibit_requested andalso
        not NewState#state.view_compaction_uninhibit_started of
        true ->
            {noreply,
             NewState#state{view_compaction_uninhibit_started = true,
                            views_compaction =
                                compact_views_for_paused_bucket(
                                  NewState#state.view_compaction_inhibited_bucket,
                                  CompactorState)}};
        false ->
            {noreply, NewState#state{views_compaction=
                                         process_compactors_exit(Reason, CompactorState)}}
    end;
handle_info({'EXIT', Pid, Reason},
            #state{running_forced_compactions=Compactions} = State) ->
    case dict:find(Pid, Compactions) of
        {ok, #forced_compaction{type=Type, name=Name}} ->
            case Reason of
                normal ->
                    case Type of
                        bucket_purge ->
                            ale:info(?USER_LOGGER,
                                     "Purge deletion compaction of bucket `~s` completed",
                                     [Name]);
                        _ ->
                            ale:info(?USER_LOGGER,
                                     "User-triggered compaction of ~p `~s` completed.",
                                     [Type, Name])
                    end;
                _ ->
                    case Type of
                        bucket_purge ->
                            ale:info(?USER_LOGGER,
                                     "Purge deletion compaction of bucket `~s` failed: ~p. "
                                     "See logs for detailed reason.",
                                     [Name, Reason]);
                        _ ->
                            ale:error(?USER_LOGGER,
                                      "User-triggered compaction of ~p `~s` failed: ~p. "
                                      "See logs for detailed reason.",
                                      [Type, Name, Reason])
                    end
            end,

            {noreply, unregister_forced_compaction(Pid, State)};
        error ->
            ?log_error("Unexpected process termination ~p: ~p. Dying.",
                       [Pid, Reason]),
            {stop, Reason, State}
    end;
handle_info(config_changed,
            #state{kv_compaction = KVCompaction,
                   views_compaction = ViewsCompaction,
                   master_compaction = MasterCompaction} = State) ->
    misc:flush(config_changed),
    Config = ns_config:get(),

    {noreply, State#state{kv_compaction = process_config_change(Config, KVCompaction),
                          views_compaction = process_config_change(Config, ViewsCompaction),
                          master_compaction = process_config_change(Config, MasterCompaction)}};
handle_info(Info, State) ->
    ?log_warning("Got unexpected message ~p:~n~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_check_interval(Config) ->
    #daemon_config{check_interval=CheckInterval} = compaction_daemon_config(Config),
    CheckInterval.

%% Internal functions
get_dbs_compactor(BucketName, Config, Force) ->
    OriginalTarget = {[{type, bucket}]},
    [{type, database},
     {important, true},
     {name, BucketName},
     {fa, {fun spawn_dbs_compactor/4,
           [BucketName, Config, Force, OriginalTarget]}}].

get_forced_master_db_compactor(BucketName, Config) ->
    [{type, database},
     {important, true},
     {name, BucketName},
     {fa, {fun spawn_master_db_compactor/3,
           [BucketName, {Config, []}, true]}}].

-spec spawn_scheduled_kv_compactor(binary(), {#config{}, list()}) -> pid().
spawn_scheduled_kv_compactor(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              check_period_and_maybe_exit(Config),
              exit_if_no_bucket(BucketName),

              ?log_info("Start compaction of vbuckets for bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              Compactor = get_dbs_compactor(BucketName, Config, false),
              misc:wait_for_process(chain_compactors([Compactor]), infinity)
      end).

get_view_compactors(BucketName, DDocNames, Config, Force) ->
    OriginalTarget = {[{type, bucket}]},
    [ [{type, view},
       {name, <<BucketName/binary, $/, DDocId/binary>>},
       {important, false},
       {fa, {fun spawn_view_compactor/5,
             [BucketName, DDocId, Config, Force =:= true, OriginalTarget]}}] ||
        DDocId <- DDocNames ].

-spec spawn_scheduled_views_compactor(binary(), {#config{}, list()}) -> pid().
spawn_scheduled_views_compactor(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              check_period_and_maybe_exit(Config),
              exit_if_no_bucket(BucketName),
              exit_if_bucket_is_paused(BucketName),

              ns_couchdb_api:try_to_cleanup_indexes(BucketName),

              ?log_info("Start compaction of indexes for bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              DDocNames = ddoc_names(BucketName),
              DDocNames =/= [] orelse exit(normal),

              Compactors = get_view_compactors(BucketName, DDocNames, Config, false),

              %% make sure that scheduled non-parallel view compaction doesn't happen
              %% during the kv compaction
              Config#config.parallel_view_compact orelse get_kv_token(),

              misc:wait_for_process(chain_compactors(Compactors), infinity)
      end).

-spec spawn_views_compactor_for_paused_bucket(binary(), {#config{}, list()}) -> pid().
spawn_views_compactor_for_paused_bucket(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              exit_if_no_bucket(BucketName),

              ns_couchdb_api:try_to_cleanup_indexes(BucketName),

              ?log_info("Start compaction of indexes for paused bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              DDocNames = ddoc_names(BucketName),
              DDocNames =/= [] orelse exit(normal),

              Compactors = get_view_compactors(BucketName, DDocNames, Config, true),

              %% make sure that scheduled non-parallel view compaction doesn't happen
              %% during the kv compaction
              Config#config.parallel_view_compact orelse get_kv_token(),

              misc:wait_for_process(chain_compactors(Compactors), infinity)
      end).

-spec spawn_forced_bucket_compactor(binary(), {#config{}, list()}) -> pid().
spawn_forced_bucket_compactor(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              exit_if_no_bucket(BucketName),

              ns_couchdb_api:try_to_cleanup_indexes(BucketName),

              ?log_info("Start forced compaction of bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              DDocNames = ddoc_names(BucketName),

              MasterDbCompactor = get_forced_master_db_compactor(BucketName, Config),
              DbsCompactor = get_dbs_compactor(BucketName, Config, true),
              DDocCompactors = get_view_compactors(BucketName, DDocNames, Config, true),

              case Config#config.parallel_view_compact of
                  true ->
                      DbsPid = chain_compactors([MasterDbCompactor, DbsCompactor]),
                      ViewsPid = chain_compactors(DDocCompactors),

                      misc:wait_for_process(DbsPid, infinity),
                      misc:wait_for_process(ViewsPid, infinity);
                  false ->
                      AllCompactors = [MasterDbCompactor | [DbsCompactor | DDocCompactors]],
                      Pid = chain_compactors(AllCompactors),
                      misc:wait_for_process(Pid, infinity)
              end
      end).

chain_compactors(Compactors) ->
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              process_flag(trap_exit, true),
              do_chain_compactors(Parent, Compactors)
      end).

do_chain_compactors(_Parent, []) ->
    ok;
do_chain_compactors(Parent, [Compactor | Compactors]) ->
    {Fun, Args} = proplists:get_value(fa, Compactor),
    Important = proplists:get_value(important, Compactor, true),
    Pid = erlang:apply(Fun, Args),

    receive
        {'EXIT', Parent, Reason} = Exit ->
            ?log_debug("Got exit signal from parent: ~p", [Exit]),
            exit(Pid, Reason),
            misc:wait_for_process(Pid, infinity),
            exit(Reason);
        {'EXIT', Pid, normal} ->
            do_chain_compactors(Parent, Compactors);
        {'EXIT', Pid, Reason} ->
            Type = proplists:get_value(type, Compactor),
            Name = proplists:get_value(name, Compactor),

            case Important of
                true ->
                    ale:warn(?USER_LOGGER,
                             "Compactor for ~p `~s` (pid ~p) terminated "
                             "unexpectedly: ~p",
                             [Type, Name, Compactor, Reason]),
                    exit(Reason);
                false ->
                    %% Don't spam the user, just log it
                    ?log_info("Compactor for ~p `~s` (pid ~p) terminated "
                              "unexpectedly (ignoring this): ~p",
                              [Type, Name, Compactor, Reason])
            end
    end.

spawn_dbs_compactor(BucketName, Config, Force, OriginalTarget) ->
    proc_lib:spawn_link(
      fun () ->
              VBucketDbs = all_bucket_dbs(BucketName),
              NumVBuckets = length(VBucketDbs),

              DoCompact =
                  case Force of
                      true ->
                          ?log_info("Forceful compaction of bucket ~s requested",
                                    [BucketName]),
                          true;
                      false ->
                          bucket_needs_compaction(BucketName, NumVBuckets, Config)
                  end,

              case DoCompact andalso NumVBuckets =/= 0 of
                  true ->
                      ok;
                  false ->
                      exit(normal)
              end,

              ?log_info("Compacting databases for bucket ~s", [BucketName]),

              {Options, SafeViewSeqs} =
                  case Config#config.do_purge of
                      true ->
                          {{0, 0, true}, []};
                      _ ->
                          {NowMegaSec, NowSec, _} = os:timestamp(),
                          NowEpoch = NowMegaSec * 1000000 + NowSec,

                          Interval0 = compaction_api:get_purge_interval(BucketName) * 3600 * 24,
                          Interval = erlang:round(Interval0),

                          PurgeTS0 = NowEpoch - Interval,

                          RebTS = get_last_rebalance_or_failover_timestamp(),

                          PurgeTS = case RebTS > PurgeTS0 of
                                        true ->
                                            RebTS - Interval;
                                        false ->
                                            PurgeTS0
                                    end,

                          {{PurgeTS, 0, false}, ns_couchdb_api:get_safe_purge_seqs(BucketName)}
                  end,

              Force orelse get_kv_token(),

              {ok, Manager} = compaction_dbs:start_link(BucketName, VBucketDbs, Force, OriginalTarget),
              ?log_debug("Started KV compaction manager ~p", [Manager]),

              NWorkers = ns_config:read_key_fast(compaction_number_of_kv_workers, 1),
              VBucketConfig = vbucket_compaction_config(Config, NumVBuckets),
              Compactors =
                  [ [{type, vbucket_worker},
                     {name, integer_to_list(I)},
                     {important, true},
                     {fa, {fun spawn_vbuckets_compactor/6,
                           [BucketName, Manager, VBucketConfig,
                            Force, Options, SafeViewSeqs]}}] ||
                      I <- lists:seq(1, NWorkers) ],

              Pids = [chain_compactors([Compactor]) || Compactor <- Compactors],
              ?log_debug("Started KV compaction workers ~p", [Pids]),
              [misc:wait_for_process(Pid, infinity) || Pid <- Pids],
              misc:unlink_terminate_and_wait(Manager, shutdown),

              ?log_info("Finished compaction of databases for bucket ~s",
                        [BucketName])
      end).

vbucket_compaction_config(Config, NumVBuckets) ->
    FragThreshold = Config#config.db_fragmentation,
    NewFragThreshold = do_vbucket_compaction_config(FragThreshold, NumVBuckets),
    Config#config{db_fragmentation = NewFragThreshold}.

do_vbucket_compaction_config({FragLimit, FragSizeLimit}, NumVBuckets)
  when is_integer(FragSizeLimit) ->
    {FragLimit, FragSizeLimit div NumVBuckets};
do_vbucket_compaction_config(FragThreshold, _) ->
    FragThreshold.

make_per_vbucket_compaction_options({TS, 0, DD} = GeneralOption, {Vb, _}, SafeViewSeqs) ->
    case lists:keyfind(Vb, 1, SafeViewSeqs) of
        false ->
            GeneralOption;
        {_, Seq} ->
            {TS, Seq, DD}
    end.

vbucket_needs_compaction({DataSize, FileSize}, Config) ->
    #config{daemon=#daemon_config{min_db_file_size=MinFileSize},
            db_fragmentation=FragThreshold} = Config,

    file_needs_compaction(DataSize, FileSize, FragThreshold, MinFileSize).

get_db_size_info(Bucket, VBucket) ->
    case ns_memcached:get_single_vbucket_details_stats(
           Bucket, VBucket, ["db_data_size", "db_file_size"]) of
        {ok, Props} ->
            {ok, {list_to_integer(proplists:get_value("db_data_size", Props)),
                  list_to_integer(proplists:get_value("db_file_size", Props))}};
        {memcached_error, _, _} = Error ->
            Error
    end.

maybe_compact_vbucket(BucketName, {VBucket, DbName},
                      Config, Force, Options) ->
    Bucket = binary_to_list(BucketName),
    {DataSize, FileSize} = SizeInfo =
        case get_db_size_info(Bucket, VBucket) of
            {ok, Info} ->
                Info;
            {memcached_error, not_my_vbucket, _} ->
                ?log_debug("Seems that vbucket ~p is gone from the node. "
                           "Skipping compaction", [VBucket]),
                exit(normal);
            Error ->
                exit({stats_error, Error})
        end,

    Force orelse vbucket_needs_compaction(SizeInfo, Config) orelse exit(normal),

    %% effectful
    ensure_can_db_compact(DbName, SizeInfo),

    ?log_info("Compacting '~s', DataSize = ~p, FileSize = ~p, Options = ~p",
              [DbName, DataSize, FileSize, Options]),
    Ret = ns_memcached:compact_vbucket(Bucket, VBucket, Options),
    ?log_info("Compaction of ~p has finished with ~p", [DbName, Ret]),
    Ret.

spawn_vbucket_compactor(BucketName, VBucketAndDb, Config, Force, Options) ->
    proc_lib:spawn_link(
      fun () ->
              case maybe_compact_vbucket(BucketName, VBucketAndDb, Config, Force, Options) of
                  ok ->
                      ok;
                  Result ->
                      exit(Result)
              end
      end).

compact_vbuckets_loop(BucketName, Manager, Config, Force, Options, SafeViewSeqs, Parent) ->
    exit_if_uninhibit_requsted_for_non_parallel_compaction(),
    VBucketAndDb = compaction_dbs:pick_db_to_compact(Manager),
    VBucketAndDb =/= undefined orelse exit(normal),

    Compactor =
        [{type, vbucket},
         {name, element(2, VBucketAndDb)},
         {important, false},
         {fa, {fun spawn_vbucket_compactor/5,
               [BucketName, VBucketAndDb, Config, Force,
                make_per_vbucket_compaction_options(Options, VBucketAndDb, SafeViewSeqs)]}}],

    do_chain_compactors(Parent, [Compactor]),
    compaction_dbs:update_progress(Manager),
    compact_vbuckets_loop(BucketName, Manager, Config, Force, Options, SafeViewSeqs, Parent).

spawn_vbuckets_compactor(BucketName, Manager, Config, Force, Options, SafeViewSeqs) ->
    Parent = self(),
    proc_lib:spawn_link(
      fun () ->
              process_flag(trap_exit, true),
              compact_vbuckets_loop(BucketName, Manager, Config, Force, Options, SafeViewSeqs, Parent)
      end).

maybe_compact_master_db(BucketName, Config, Force) ->
    DbName = db_name(BucketName, "master"),
    SizeInfo = ns_couchdb_api:get_master_vbucket_size(BucketName),

    Force orelse master_db_needs_compaction(SizeInfo, Config) orelse exit(normal),

    %% effectful
    ensure_can_db_compact(DbName, SizeInfo),

    ?log_info("Compacting `~s'", [DbName]),
    Ret = compact_master_vbucket(BucketName),
    ?log_info("Compaction of ~p has finished with ~p", [DbName, Ret]),
    Ret.

master_db_needs_compaction(SizeInfo, Config) ->
    vbucket_needs_compaction(SizeInfo, master_db_compaction_config(Config)).

master_db_compaction_config(Config) ->
    FragThreshold = Config#config.db_fragmentation,
    NewFragThreshold = do_master_db_compaction_config(FragThreshold),
    Config#config{db_fragmentation = NewFragThreshold}.

do_master_db_compaction_config({FragLimit, FragSizeLimit})
  when is_integer(FragSizeLimit) ->
    case is_integer(FragLimit) of
        true ->
            {FragLimit, undefined};
        false ->
            %% Master vbucket is special in that it only contains metadata and
            %% generally is very small. So it's ok to cheat here and pretend
            %% that fragmentation percentage threshold is defined.
            {50, undefined}
    end;
do_master_db_compaction_config(FragThreshold) ->
    FragThreshold.

compact_master_vbucket(BucketName) ->
    process_flag(trap_exit, true),

    {ok, Compactor, Db} = ns_couchdb_api:start_master_vbucket_compact(BucketName),

    CompactorRef = erlang:monitor(process, Compactor),

    receive
        {'EXIT', _Parent, Reason} = Exit ->
            ?log_debug("Got exit signal from parent: ~p", [Exit]),

            erlang:demonitor(CompactorRef),
            ok = ns_couchdb_api:cancel_master_vbucket_compact(Db),
            Reason;
        {'DOWN', CompactorRef, process, Compactor, normal} ->
            ok;
        {'DOWN', CompactorRef, process, Compactor, noproc} ->
            %% compactor died before we managed to monitor it;
            %% we will treat this as an error
            {db_compactor_died_too_soon, db_name(BucketName, "master")};
        {'DOWN', CompactorRef, process, Compactor, Reason} ->
            Reason
    end.

spawn_scheduled_master_db_compactor(BucketName, Config) ->
    spawn_master_db_compactor(BucketName, Config, false).

spawn_master_db_compactor(BucketName, {Config, ConfigProps}, Force) ->
    proc_lib:spawn_link(
      fun () ->
              check_period_and_maybe_exit(Config),
              exit_if_no_bucket(BucketName),

              Force orelse ?log_info("Start compaction of master db for bucket ~s with config: ~n~p",
                                     [BucketName, ConfigProps]),
              case maybe_compact_master_db(BucketName, Config, Force) of
                  ok ->
                      ok;
                  Result ->
                      exit(Result)
              end
      end).

spawn_view_compactor(BucketName, DDocId, Config, Force, OriginalTarget) ->
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              process_flag(trap_exit, true),

              Compactors =
                  [ [{type, view},
                     {important, true},
                     {name, view_name(BucketName, DDocId, Kind, Type)},
                     {fa, {fun spawn_view_index_compactor/7,
                           [BucketName, DDocId,
                            Kind, Type,
                            Config, Force, OriginalTarget]}}] ||
                      Type <- [main, replica],
                      Kind <- [mapreduce_view, spatial_view]],

              do_chain_compactors(Parent, Compactors)
      end).

view_name(BucketName, DDocId, Kind, Type) ->
    <<(atom_to_binary(Kind, latin1))/binary, $/,
      BucketName/binary, $/, DDocId/binary, $/,
      (atom_to_binary(Type, latin1))/binary>>.

spawn_view_index_compactor(BucketName, DDocId,
                           Kind, Type,
                           Config, Force, OriginalTarget) ->
    Force orelse exit_if_uninhibit_requsted(),
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              ViewName = view_name(BucketName, DDocId, Kind, Type),

              Force orelse view_needs_compaction(BucketName, DDocId, Kind, Type, Config)
                  orelse exit(normal),

              %% effectful
              ensure_can_view_compact(BucketName, DDocId, Kind, Type),

              TriggerType = case Force of
                                true ->
                                    manual;
                                false ->
                                    scheduled
                            end,

              ?log_info("Compacting indexes for ~s. Compaction is ~p", [ViewName, TriggerType]),
              process_flag(trap_exit, true),

              InitialStatus = [{original_target, OriginalTarget},
                               {trigger_type, TriggerType}],

              do_spawn_view_index_compactor(Parent, BucketName,
                                            DDocId, Kind, Type, InitialStatus),

              ?log_info("Finished compacting indexes for ~s", [ViewName])
      end).

do_spawn_view_index_compactor(Parent, BucketName, DDocId, Kind, Type, InitialStatus) ->
    Compactor = start_view_index_compactor(BucketName, DDocId,
                                           Kind, Type, InitialStatus),
    CompactorRef = erlang:monitor(process, Compactor),
    CompactorInfo = {Compactor, BucketName, DDocId, Kind, Type},
    ?log_debug("Spawned view index compactor ~p", [CompactorInfo]),

    receive
        {'EXIT', Parent, Reason} = Exit ->
            ?log_debug("Got exit signal from parent: ~p. Will cancel compactor ~p",
                       [Exit, CompactorInfo]),
            erlang:demonitor(CompactorRef),
            ok = ns_couchdb_api:cancel_view_compact(BucketName, DDocId, Kind, Type),
            exit(Reason);
        {'DOWN', CompactorRef, process, Compactor, normal} ->
            ok;
        {'DOWN', CompactorRef, process, Compactor, noproc} ->
            exit({index_compactor_died_too_soon, CompactorInfo});
        {'DOWN', CompactorRef, process, Compactor, Reason}
          when Reason =:= shutdown;
               Reason =:= {updater_died, shutdown};
               Reason =:= {updated_died, noproc};
               Reason =:= {updater_died, {updater_error, shutdown}} ->
            ?log_debug("Compactor ~p exited with reason ~p, Will retry", [CompactorInfo, Reason]),
            do_spawn_view_index_compactor(Parent, BucketName,
                                          DDocId, Kind, Type, InitialStatus);
        {'DOWN', CompactorRef, process, Compactor, Reason} ->
            ?log_debug("Compactor ~p exited with reason ~p, Will exit", [CompactorInfo, Reason]),
            exit(Reason)
    end.

start_view_index_compactor(BucketName, DDocId, Kind, Type, InitialStatus) ->
    case ns_couchdb_api:start_view_compact(BucketName, DDocId, Kind, Type, InitialStatus) of
        {ok, Pid} ->
            Pid;
        {error, Error} ->
            ?log_debug("Compactor ~p was not created. Reason: ~p",
                       [{BucketName, DDocId, Kind, Type}, Error]),
            case Error of
                initial_build ->
                    exit(normal);
                empty_group ->
                    exit(normal)
            end
    end.

bucket_needs_compaction(BucketName, NumVBuckets,
                        #config{daemon=#daemon_config{min_db_file_size=MinFileSize},
                                db_fragmentation=FragThreshold}) ->
    try aggregated_size_info(binary_to_list(BucketName)) of
        {DataSize, FileSize} ->
            ?log_debug("`~s` data size is ~p, disk size is ~p",
                       [BucketName, DataSize, FileSize]),

            file_needs_compaction(DataSize, FileSize,
                                  FragThreshold, MinFileSize * NumVBuckets)
    catch exit:{noproc, _} ->
            ?log_debug("memcached is not started for bucket ~p yet", [BucketName]),
            false
    end.

file_needs_compaction(DataSize, FileSize, FragThreshold, MinFileSize) ->
    %% NOTE: If there are no vbuckets on this node MinFileSize will be
    %% 0 so less then or _equals_ is really important to avoid
    %% division by zero
    case FileSize =< MinFileSize of
        true ->
            false;
        false ->
            FragSize = FileSize - DataSize,
            Frag = round((FragSize / FileSize) * 100),

            check_fragmentation(FragThreshold, Frag, FragSize)
    end.

aggregated_size_info(Bucket) ->
    {ok, {DS, FS}} =
        ns_memcached:raw_stats(node(), Bucket, <<"diskinfo">>,
                               fun (<<"ep_db_file_size">>, V, {DataSize, _}) ->
                                       {DataSize, V};
                                   (<<"ep_db_data_size">>, V, {_, FileSize}) ->
                                       {V, FileSize};
                                   (_, _, Tuple) ->
                                       Tuple
                               end, {<<"0">>, <<"0">>}),
    {list_to_integer(binary_to_list(DS)),
     list_to_integer(binary_to_list(FS))}.

check_fragmentation(FragThreshold, Frag, FragSize) ->
    {FragLimit, FragSizeLimit} = normalize_fragmentation(FragThreshold),

    true = is_integer(FragLimit),
    true = is_integer(FragSizeLimit),

    (Frag >= FragLimit) orelse (FragSize >= FragSizeLimit).

ensure_can_db_compact(DbName, {DataSize, _}) ->
    SpaceRequired = space_required(DataSize),

    {ok, DbDir} = ns_storage_conf:this_node_dbdir(),
    Free = free_space(DbDir),

    case Free >= SpaceRequired of
        true ->
            ok;
        false ->
            ale:error(?USER_LOGGER,
                      "Cannot compact database `~s`: "
                      "the estimated necessary disk space is about ~p bytes "
                      "but the currently available disk space is ~p bytes.",
                      [DbName, SpaceRequired, Free]),
            exit({not_enough_space, DbName, SpaceRequired, Free})
    end.

space_required(DataSize) ->
    round(DataSize * 2.0).

free_space(Path) ->
    Stats = ns_disksup:get_disk_data(),
    {ok, RealPath} = misc:realpath(Path, "/"),
    {ok, {_, Total, Usage}} =
        ns_storage_conf:extract_disk_stats_for_path(Stats, RealPath),
    trunc(Total - (Total * (Usage / 100))) * 1024.

view_needs_compaction(BucketName, DDocId, Kind, Type,
                      #config{view_fragmentation=FragThreshold,
                              daemon=#daemon_config{min_view_file_size=MinFileSize}}) ->
    Info = get_group_data_info(BucketName, DDocId, Kind, Type),

    case Info of
        disabled ->
            %% replica index can be disabled; so we'll skip it here instead of
            %% spamming logs with irrelevant crash reports
            false;
        _ ->
            InitialBuild = proplists:get_value(initial_build, Info),
            case InitialBuild of
                true ->
                    false;
                false ->
                    FileSize = proplists:get_value(disk_size, Info),
                    DataSize = proplists:get_value(data_size, Info, 0),

                    ?log_debug("`~s` data_size is ~p, disk_size is ~p",
                               [view_name(BucketName, DDocId, Kind, Type), DataSize, FileSize]),

                    file_needs_compaction(DataSize, FileSize,
                                          FragThreshold, MinFileSize)
            end
    end.

ensure_can_view_compact(BucketName, DDocId, Kind, Type) ->
    Info = get_group_data_info(BucketName, DDocId, Kind, Type),

    case Info of
        disabled ->
            exit(normal);
        _ ->
            DataSize = proplists:get_value(data_size, Info, 0),
            SpaceRequired = space_required(DataSize),

            {ok, ViewsDir} = ns_storage_conf:this_node_ixdir(),
            Free = free_space(ViewsDir),

            case Free >= SpaceRequired of
                true ->
                    ok;
                false ->
                    ale:error(?USER_LOGGER,
                              "Cannot compact view `~s`: "
                              "the estimated necessary disk space is about ~p bytes "
                              "but the currently available disk space is ~p bytes.",
                              [view_name(BucketName, DDocId, Kind, Type), SpaceRequired, Free]),
                    exit({not_enough_space,
                          BucketName, DDocId, SpaceRequired, Free})
            end
    end.

get_group_data_info(BucketName, DDocId, Kind, main) ->
    {ok, Info} = ns_couchdb_api:get_view_group_data_size(BucketName, DDocId, Kind),
    Info;
get_group_data_info(BucketName, DDocId, Kind, replica) ->
    MainInfo = get_group_data_info(BucketName, DDocId, Kind, main),
    proplists:get_value(replica_group_info, MainInfo, disabled).

ddoc_names(BucketName) ->
    capi_utils:fetch_ddoc_ids(BucketName).

search_node_default(Config, Key, Default) ->
    case ns_config:search_node(Config, Key) of
        false ->
            Default;
        {value, Value} ->
            Value
    end.

compaction_daemon_config(Config) ->
    Props = search_node_default(Config, compaction_daemon, []),
    daemon_config_to_record(Props).

compaction_config_props(Config, BucketName) ->
    Global = search_node_default(Config, autocompaction, []),
    BucketConfig = get_bucket(BucketName, Config),
    PerBucket = case proplists:get_value(autocompaction, BucketConfig, []) of
                    false -> [];
                    SomeValue -> SomeValue
                end,

    lists:foldl(
      fun ({Key, _Value} = KV, Acc) ->
              [KV | proplists:delete(Key, Acc)]
      end, Global, PerBucket).

compaction_config(BucketName) ->
    compaction_config(ns_config:get(), BucketName).

compaction_config(Config, BucketName) ->
    ConfigProps = compaction_config_props(Config, BucketName),
    DaemonConfig = compaction_daemon_config(Config),
    ConfigRecord = config_to_record(ConfigProps, DaemonConfig),

    {ConfigRecord, ConfigProps}.

config_to_record(Config, DaemonConfig) ->
    do_config_to_record(Config, #config{daemon=DaemonConfig}).

do_config_to_record([], Acc) ->
    Acc;
do_config_to_record([{database_fragmentation_threshold, V} | Rest], Acc) ->
   do_config_to_record(Rest, Acc#config{db_fragmentation=V});
do_config_to_record([{view_fragmentation_threshold, V} | Rest], Acc) ->
    do_config_to_record(Rest, Acc#config{view_fragmentation=V});
do_config_to_record([{parallel_db_and_view_compaction, V} | Rest], Acc) ->
    do_config_to_record(Rest, Acc#config{parallel_view_compact=V});
do_config_to_record([{allowed_time_period, V} | Rest], Acc) ->
    do_config_to_record(Rest, Acc#config{allowed_period=allowed_period_record(V)});
do_config_to_record([{_OtherKey, _V} | Rest], Acc) ->
    %% NOTE !!!: releases before 2.2.0 raised error here
    do_config_to_record(Rest, Acc).

normalize_fragmentation({Percentage, Size}) ->
    NormPercencentage =
        case Percentage of
            undefined ->
                100;
            _ when is_integer(Percentage) ->
                Percentage
        end,

    NormSize =
        case Size of
            undefined ->
                %% just set it to something really big (to simplify further
                %% logic a little bit)
                1 bsl 64;
            _ when is_integer(Size) ->
                Size
        end,

    {NormPercencentage, NormSize}.


daemon_config_to_record(Config) ->
    do_daemon_config_to_record(Config, #daemon_config{}).

do_daemon_config_to_record([], Acc) ->
    Acc;
do_daemon_config_to_record([{min_db_file_size, V} | Rest], Acc) ->
    do_daemon_config_to_record(Rest, Acc#daemon_config{min_db_file_size=V});
do_daemon_config_to_record([{min_view_file_size, V} | Rest], Acc) ->
    do_daemon_config_to_record(Rest, Acc#daemon_config{min_view_file_size=V});
do_daemon_config_to_record([{check_interval, V} | Rest], Acc) ->
    do_daemon_config_to_record(Rest, Acc#daemon_config{check_interval=V}).

allowed_period_record(Config) ->
    do_allowed_period_record(Config, #period{}).

do_allowed_period_record([], Acc) ->
    Acc;
do_allowed_period_record([{from_hour, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{from_hour=V});
do_allowed_period_record([{from_minute, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{from_minute=V});
do_allowed_period_record([{to_hour, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{to_hour=V});
do_allowed_period_record([{to_minute, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{to_minute=V});
do_allowed_period_record([{abort_outside, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{abort_outside=V});
do_allowed_period_record([{OtherKey, _V} | _], _Acc) ->
    %% should not happen
    exit({invalid_period_key_bug, OtherKey}).

-spec check_period_and_maybe_exit(#config{}) -> ok.
check_period_and_maybe_exit(#config{allowed_period=undefined}) ->
    ok;
check_period_and_maybe_exit(#config{allowed_period=Period}) ->
    do_check_period_and_maybe_exit(Period).

do_check_period_and_maybe_exit(#period{from_hour=FromHour,
                                       from_minute=FromMinute,
                                       to_hour=ToHour,
                                       to_minute=ToMinute,
                                       abort_outside=AbortOutside}) ->
    {HH, MM, _} = erlang:time(),

    Now0 = time_to_minutes(HH, MM),
    From = time_to_minutes(FromHour, FromMinute),
    To0 = time_to_minutes(ToHour, ToMinute),

    To = case To0 < From of
             true ->
                 To0 + 24 * 60;
             false ->
                 To0
         end,

    Now = case Now0 < From of
              true ->
                  Now0 + 24 * 60;
              false ->
                  Now0
          end,

    CanStart = (Now >= From) andalso (Now < To),
    MinutesLeft = To - Now,

    case CanStart of
        true ->
            case AbortOutside of
                true ->
                    shutdown_compactor_after(MinutesLeft),
                    ok;
                false ->
                    ok
            end;
        false ->
            exit(normal)
    end.

time_to_minutes(HH, MM) ->
    HH * 60 + MM.

shutdown_compactor_after(MinutesLeft) ->
    Compactor = self(),

    MillisLeft = MinutesLeft * 60 * 1000,
    ?log_debug("Scheduling a timer to shut "
               "ourselves down in ~p ms", [MillisLeft]),

    spawn_link(
      fun () ->
              process_flag(trap_exit, true),

              receive
                  {'EXIT', Compactor, _Reason} ->
                      ok;
                  Msg ->
                      exit({unexpected_message_bug, Msg})
              after
                  MillisLeft ->
                      ?log_info("Shutting compactor ~p down "
                                "not to abuse allowed time period", [Compactor]),
                      exit(Compactor, shutdown)
              end
      end).

init_scheduled_compaction(CheckInterval, Message, Fun) ->
    self() ! Message,
    #compaction_state{buckets_to_compact=[],
                      compactor_pid=undefined,
                      scheduler=compaction_scheduler:init(CheckInterval, Message),
                      compactor_fun = Fun}.

process_scheduler_message(Msg, #compaction_state{buckets_to_compact=Buckets0,
                                                 scheduler=Scheduler,
                                                 compactor_pid=undefined} = State) ->
    Buckets =
        case Buckets0 of
            [] ->
                lists:map(fun list_to_binary/1,
                          ns_bucket:node_bucket_names_of_type(node(), membase, couchstore));
            _ ->
                Buckets0
        end,

    case Buckets of
        [] ->
            ?log_debug("No buckets to compact for ~p. Rescheduling compaction.", [Msg]),
            State#compaction_state{scheduler=compaction_scheduler:schedule_next(Scheduler)};
        _ ->
            ?log_debug("Starting compaction (~p) for the following buckets: ~n~p",
                       [Msg, Buckets]),
            NewState1 = State#compaction_state{buckets_to_compact=Buckets,
                                               scheduler=
                                                   compaction_scheduler:start_now(Scheduler)},
            compact_next_bucket(NewState1)
    end.

log_compactors_exit(Reason, #compaction_state{compactor_pid=Compactor}) ->
    case Reason of
        normal ->
            ok;
        shutdown ->
            ?log_debug("Compactor ~p exited with reason ~p", [Compactor, Reason]);
        _ ->
            ?log_error("Compactor ~p exited unexpectedly: ~p. "
                       "Moving to the next bucket.", [Compactor, Reason])
    end.

process_compactors_exit(Reason, #compaction_state{buckets_to_compact=Buckets,
                                                  scheduler=Scheduler} = State) ->
    true = Buckets =/= [],

    NewBuckets =
        case Reason of
            shutdown ->
                %% this can happen either on config change or if compaction
                %% has been stopped due to end of allowed time period; in the
                %% latter case there's no need to compact bucket again; but it
                %% won't hurt: compaction shall be terminated rightaway with
                %% reason normal
                Buckets;
            _ ->
                tl(Buckets)
        end,

    NewState = State#compaction_state{compactor_pid=undefined,
                                      compactor_config=undefined,
                                      buckets_to_compact=NewBuckets},
    case NewBuckets of
        [] ->
            ?log_debug("Finished compaction iteration."),
            NewState#compaction_state{scheduler =
                                          compaction_scheduler:schedule_next(Scheduler)};
        _ ->
            compact_next_bucket(NewState)
    end.

process_config_change(Config, #compaction_state{scheduler = Scheduler} = State) ->
    CheckInterval = get_check_interval(Config),
    NewScheduler = compaction_scheduler:set_interval(CheckInterval, Scheduler),

    maybe_restart_compactor(Config, State),
    State#compaction_state{scheduler = NewScheduler}.

maybe_restart_compactor(_NewConfig, #compaction_state{compactor_pid=undefined}) ->
    ok;
maybe_restart_compactor(_NewConfig, #compaction_state{buckets_to_compact=[]}) ->
    ok;
maybe_restart_compactor(NewConfig, #compaction_state{compactor_pid=Compactor,
                                                     buckets_to_compact=[CompactedBucket|_],
                                                     compactor_config=Config}) ->
    {MaybeNewConfig, _} = compaction_config(NewConfig, CompactedBucket),
    case MaybeNewConfig =:= Config of
        true ->
            ok;
        false ->
            ?log_info("Restarting compactor ~p since "
                      "autocompaction config has changed", [Compactor]),
            exit(Compactor, shutdown)
    end.

-spec compact_next_bucket(#compaction_state{}) -> #compaction_state{}.
compact_next_bucket(#compaction_state{buckets_to_compact=Buckets,
                                      compactor_pid=undefined,
                                      compactor_fun=Fun} = State) ->
    true = [] =/= Buckets,
    compact_bucket(hd(Buckets), Fun, State).

-spec compact_bucket(binary(), fun(), #compaction_state{}) -> #compaction_state{}.
compact_bucket(BucketName, Fun, State) ->
    {InitialConfig, _ConfigProps} = Configs = compaction_config(BucketName),

    Compactor = Fun(BucketName, Configs),
    State#compaction_state{compactor_pid = Compactor,
                           compactor_config = InitialConfig}.

-spec compact_views_for_paused_bucket(binary(), #compaction_state{}) -> #compaction_state{}.
compact_views_for_paused_bucket(BucketName, State) ->
    compact_bucket(BucketName,
                   fun spawn_views_compactor_for_paused_bucket/2,
                   State#compaction_state{buckets_to_compact = [BucketName]}).

db_name(BucketName, SubName) when is_list(SubName) ->
    db_name(BucketName, list_to_binary(SubName));
db_name(BucketName, VBucket) when is_integer(VBucket) ->
    db_name(BucketName, integer_to_list(VBucket));
db_name(BucketName, SubName) when is_binary(SubName) ->
    true = is_binary(BucketName),

    <<BucketName/binary, $/, SubName/binary>>.

all_bucket_dbs(BucketName) ->
    BucketConfig = get_bucket(BucketName),
    NodeVBuckets = ns_bucket:all_node_vbuckets(BucketConfig),
    [{V, db_name(BucketName, V)} || V <- NodeVBuckets].

get_bucket(BucketName) ->
    get_bucket(BucketName, ns_config:get()).

get_bucket(BucketName, Config) ->
    BucketNameStr = binary_to_list(BucketName),

    case ns_bucket:get_bucket(BucketNameStr, Config) of
        not_present ->
            exit({bucket_not_present, BucketName});
        {ok, BucketConfig} ->
            BucketConfig
    end.

bucket_exists(BucketName) ->
    case ns_bucket:get_bucket(binary_to_list(BucketName)) of
        {ok, Config} ->
            (ns_bucket:bucket_type(Config) =:= membase) andalso
                lists:member(node(), ns_bucket:get_servers(Config));
        not_present ->
            false
    end.

exit_if_no_bucket(BucketName) ->
    case bucket_exists(BucketName) of
        true ->
            ok;
        false ->
            exit(normal)
    end.

exit_if_bucket_is_paused(BucketName) ->
    case gen_server:call(?MODULE, {is_bucket_inhibited, BucketName}, infinity) of
        true ->
            exit(normal);
        false ->
            ok
    end.

exit_if_uninhibit_requsted() ->
    case gen_server:call(?MODULE, is_uninhibited_requested, infinity) of
        {true, _} ->
            exit(normal);
        {false, _} ->
            ok
    end.

exit_if_uninhibit_requsted_for_non_parallel_compaction() ->
    case gen_server:call(?MODULE, is_uninhibited_requested, infinity) of
        {true, true} ->
            ?log_info("Uninhibit is requested for non-parallel index compaction. Interrupt KV compaction to start index compaction sooner."),
            exit(normal);
        {_, _} ->
            ok
    end.

-spec register_forced_compaction(pid(), #forced_compaction{},
                                 #state{}) -> #state{}.
register_forced_compaction(Pid, Compaction,
                    #state{running_forced_compactions=Compactions,
                           forced_compaction_pids=CompactionPids} = State) ->
    error = dict:find(Compaction, CompactionPids),

    NewCompactions = dict:store(Pid, Compaction, Compactions),
    NewCompactionPids = dict:store(Compaction, Pid, CompactionPids),

    State#state{running_forced_compactions=NewCompactions,
                forced_compaction_pids=NewCompactionPids}.

-spec unregister_forced_compaction(pid(), #state{}) -> #state{}.
unregister_forced_compaction(Pid,
                             #state{running_forced_compactions=Compactions,
                                    forced_compaction_pids=CompactionPids} = State) ->
    Compaction = dict:fetch(Pid, Compactions),

    NewCompactions = dict:erase(Pid, Compactions),
    NewCompactionPids = dict:erase(Compaction, CompactionPids),

    State#state{running_forced_compactions=NewCompactions,
                forced_compaction_pids=NewCompactionPids}.

-spec is_already_being_compacted(#forced_compaction{}, #state{}) -> boolean().
is_already_being_compacted(Compaction,
                           #state{forced_compaction_pids=CompactionPids}) ->
    dict:find(Compaction, CompactionPids) =/= error.

-spec maybe_cancel_compaction(#forced_compaction{}, #state{}) -> #state{}.
maybe_cancel_compaction(Compaction,
                        #state{forced_compaction_pids=CompactionPids} = State) ->
    case dict:find(Compaction, CompactionPids) of
        error ->
            State;
        {ok, Pid} ->
            exit(Pid, shutdown),
            receive
                %% let all the other exit messages be handled by corresponding
                %% handle_info clause so that error message is logged
                {'EXIT', Pid, shutdown} ->
                    unregister_forced_compaction(Pid, State);
                {'EXIT', Pid, _Reason} = Exit ->
                    {noreply, NewState} = handle_info(Exit, State),
                    NewState
            end
    end.

get_kv_token() ->
    [{_, Pid}] = ets:lookup(compaction_daemon, kv_throttle),
    new_concurrency_throttle:send_back_when_can_go(Pid, go),
    receive
        {'EXIT', _, Reason} = ExitMsg ->
            ?log_debug("Got exit waiting for token: ~p", [ExitMsg]),
            erlang:exit(Reason);
        go ->
            ok
    end.
