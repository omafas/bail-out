%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2018 Couchbase, Inc.
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
-module(ns_doctor).

-define(STALE_TIME, 5000000). % 5 seconds in microseconds
-define(LOG_INTERVAL, 60000). % How often to dump current status to the logs
-define(MAX_XDCR_TASK_ERRORS, 10).

-include("ns_common.hrl").

-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1, get_node/2,
         get_tasks_version/0, build_tasks_list/2, wait_statuses/2]).

-record(state, {
          nodes :: dict:dict(),
          nodes_wanted :: [node()],
          tasks_hash_nodes :: undefined | dict:dict(),
          tasks_hash :: undefined | integer(),
          tasks_version :: undefined | string()
         }).

-record(cfg_handler_state, {
          rebalance_state,
          recovery_state,
          buckets
         }).

-define(doctor_debug(Msg), ale:debug(?NS_DOCTOR_LOGGER, Msg)).
-define(doctor_debug(Fmt, Args), ale:debug(?NS_DOCTOR_LOGGER, Fmt, Args)).

-define(doctor_info(Msg), ale:info(?NS_DOCTOR_LOGGER, Msg)).
-define(doctor_info(Fmt, Args), ale:info(?NS_DOCTOR_LOGGER, Fmt, Args)).

-define(doctor_warning(Msg), ale:warn(?NS_DOCTOR_LOGGER, Msg)).
-define(doctor_warning(Fmt, Args), ale:warn(?NS_DOCTOR_LOGGER, Fmt, Args)).

-define(doctor_error(Msg), ale:error(?NS_DOCTOR_LOGGER, Msg)).
-define(doctor_error(Fmt, Args), ale:error(?NS_DOCTOR_LOGGER, Fmt, Args)).


%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(priority, high),
    self() ! acquire_initial_status,
    ns_pubsub:subscribe_link(ns_config_events,
                             fun handle_config_event/2,
                             #cfg_handler_state{}),
    case misc:get_env_default(dont_log_stats, false) of
        false ->
            timer2:send_interval(?LOG_INTERVAL, log);
        _ -> ok
    end,
    {ok, #state{nodes=dict:new(),
                nodes_wanted=ns_node_disco:nodes_wanted()}}.

handle_recovery_status_change(not_running, {running, _Bucket, _UUID}) ->
    {not_running, true};
handle_recovery_status_change({running, _NewBucket, NewUUID} = New,
                              {running, _OldBucket, OldUUID}) ->
    case OldUUID =:= NewUUID of
        true ->
            {New, false};
        false ->
            {New, true}
    end;
handle_recovery_status_change({running, _NewBucket, _NewUUID} = New, not_running) ->
    {New, true};
handle_recovery_status_change(not_running, not_running) ->
    {not_running, false};
handle_recovery_status_change(New, undefined) ->
    {New, true}.

handle_config_event({rebalance_status_uuid, NewValue},
                    #cfg_handler_state{rebalance_state = RebalanceState} = State) ->
    case NewValue of
        RebalanceState ->
            State;
        _ ->
            ns_doctor ! significant_change,
            State#cfg_handler_state{rebalance_state = NewValue}
    end;
handle_config_event({recovery_status, NewValue},
                    #cfg_handler_state{recovery_state = RecoveryState} = State) ->
    {NewState, Changed} = handle_recovery_status_change(NewValue, RecoveryState),
    case Changed of
        true ->
            ns_doctor ! significant_change,
            State#cfg_handler_state{recovery_state = NewState};
        false ->
            ok
    end;
handle_config_event({buckets, Buckets},
                    #cfg_handler_state{buckets = KnownBuckets} = State) ->
    BucketConfigs = proplists:get_value(configs, Buckets, []),
    BucketNames = lists:sort([Name || {Name, _} <- BucketConfigs]),
    case BucketNames =:= KnownBuckets of
        true ->
            State;
        false ->
            ns_doctor ! significant_change,
            State#cfg_handler_state{buckets = BucketNames}
    end;
handle_config_event({nodes_wanted, _} = Msg, State) ->
    ns_doctor ! Msg,
    State;
handle_config_event(_, State) ->
    State.


handle_call(get_tasks_version, _From, State) ->
    NewState = maybe_refresh_tasks_version(State),
    {reply, NewState#state.tasks_version, NewState};

handle_call({get_node, Node}, _From, #state{nodes=Nodes} = State) ->
    RV = case dict:find(Node, Nodes) of
             {ok, Status} ->
                 LiveNodes = [node() | nodes()],
                 annotate_status(Node, Status,
                                 erlang:monotonic_time(),
                                 LiveNodes);
             _ ->
                 []
         end,
    {reply, RV, State};

handle_call(get_nodes, _From, #state{nodes=Nodes} = State) ->
    Now = erlang:monotonic_time(),
    LiveNodes = [node()|nodes()],
    Nodes1 = dict:map(
               fun (Node, Status) ->
                       annotate_status(Node, Status, Now, LiveNodes)
               end, Nodes),
    {reply, Nodes1, State}.


handle_cast({heartbeat, Node, Status},
            #state{nodes_wanted = NodesWanted} = State)->
    case lists:member(Node, NodesWanted) of
        true ->
            {noreply, process_heartbeat(Node, Status, State)};
        false ->
            ?log_debug("Ignoring heartbeat from an unknown node ~p", [Node]),
            {noreply, State}
    end;

handle_cast(Msg, State) ->
    ?doctor_warning("Unexpected cast: ~p", [Msg]),
    {noreply, State}.


handle_info(significant_change, State) ->
    %% force hash recomputation next time maybe_refresh_tasks_version is called
    {noreply, State#state{tasks_hash_nodes = undefined}};
handle_info(acquire_initial_status, #state{nodes=NodeDict} = State) ->
    Replies = ns_heart:status_all(),
    %% Get an initial status so we don't start up thinking everything's down
    Nodes = lists:foldl(fun ({Node, Status}, Dict) ->
                                update_status(Node, Status, Dict)
                        end, NodeDict, Replies),
    ?doctor_debug("Got initial status:~n~p", [lists:sort(dict:to_list(Nodes))]),
    {noreply, State#state{nodes=Nodes}};

handle_info(log, #state{nodes=NodeDict} = State) ->
    ?doctor_debug("Current node statuses:~n~p",
                  [lists:sort(dict:to_list(NodeDict))]),
    {noreply, State};
handle_info({nodes_wanted, NewNodes0}, #state{nodes=Statuses} = State) ->
    NewNodes = lists:sort(NewNodes0),
    CurrentNodes = lists:sort(dict:fetch_keys(Statuses)),
    ToRemove = ordsets:subtract(CurrentNodes, NewNodes),

    NewStatuses =
        lists:foldl(
          fun (Node, Acc) ->
                  dict:erase(Node, Acc)
          end, Statuses, ToRemove),

    {noreply, State#state{nodes=NewStatuses,
                          nodes_wanted=NewNodes}};
handle_info(Info, State) ->
    ?doctor_warning("Unexpected message ~p in state", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% API

get_nodes() ->
    try gen_server:call(?MODULE, get_nodes) of
        Nodes -> Nodes
    catch
        E:R ->
            ?doctor_error("Error attempting to get nodes: ~p", [{E, R}]),
            dict:new()
    end.

get_node(Node) ->
    try gen_server:call(?MODULE, {get_node, Node}) of
        Status -> Status
    catch
        E:R ->
            ?doctor_error("Error attempting to get node ~p: ~p", [Node, {E, R}]),
            []
    end.

get_tasks_version() ->
    gen_server:call(?MODULE, get_tasks_version).

build_tasks_list(PoolId, RebStatusTimeout) ->
    NodesDict = gen_server:call(?MODULE, get_nodes),
    AllRepDocs = goxdcr_rest:find_all_replication_docs(),
    Buckets = ns_bucket:get_bucket_names(),
    do_build_tasks_list(NodesDict, PoolId, AllRepDocs, Buckets, RebStatusTimeout).

wait_statuses(Nodes0, Timeout) ->
    Nodes = sets:from_list(Nodes0),

    misc:executing_on_new_process(
      fun () ->
              Statuses0 = gen_server:call(?MODULE, get_nodes),
              Statuses  = dict:filter(
                            fun (_, Status) ->
                                    not proplists:get_bool(down, Status)
                            end, Statuses0),
              Got       = sets:from_list(dict:fetch_keys(Statuses)),
              Missing   = sets:subtract(Nodes, Got),

              case sets:size(Missing) of
                  0 ->
                      {ok, Statuses};
                  _ ->
                      TRef = make_ref(),
                      erlang:send_after(Timeout, self(), TRef),

                      ns_pubsub:subscribe_link(ns_doctor_events),
                      wait_statuses_loop(Statuses, Nodes, Missing, TRef)
              end
      end).

wait_statuses_loop(Statuses, Interesting, Missing, TRef) ->
    receive
        TRef ->
            ?log_error("Couldn't get statuses for ~p", [sets:to_list(Missing)]),
            {error, {timeout, sets:to_list(Missing)}};
        {node_status, Node, Status} ->
            case sets:is_element(Node, Interesting) of
                true ->
                    NewStatuses = dict:store(Node, Status, Statuses),
                    NewMissing = sets:del_element(Node, Missing),

                    case sets:size(NewMissing) =:= 0 of
                        true ->
                            {ok, NewStatuses};
                        false ->
                            wait_statuses_loop(NewStatuses,
                                               Interesting, NewMissing, TRef)
                    end;
                false ->
                    wait_statuses_loop(Statuses, Interesting, Missing, TRef)
            end
    end.

%% Internal functions

is_significant_buckets_change(OldStatus, NewStatus) ->
    [OldActiveBuckets, OldReadyBuckets,
     NewActiveBuckets, NewReadyBuckets] =
        [lists:sort(proplists:get_value(Field, Status, []))
         || Status <- [OldStatus, NewStatus],
            Field <- [active_buckets, ready_buckets]],
    OldActiveBuckets =/= NewActiveBuckets
        orelse OldReadyBuckets =/= NewReadyBuckets.

update_status(Name, Status0, Dict) ->
    TS = erlang:monotonic_time(),
    Status = [{last_heard, TS} | Status0],
    PrevStatus = case dict:find(Name, Dict) of
                     {ok, V} -> V;
                     error -> []
                 end,
    case is_significant_buckets_change(PrevStatus, Status) of
        true ->
            NeedBuckets = lists:sort(ns_bucket:node_bucket_names(Name)),
            OldReady = lists:sort(proplists:get_value(ready_buckets, PrevStatus, [])),
            NewReady = lists:sort(proplists:get_value(ready_buckets, Status, [])),
            case ordsets:intersection(ordsets:subtract(OldReady, NewReady), NeedBuckets) of
                [] ->
                    ok;
                MissingBuckets ->
                    MissingButActive = ordsets:intersection(lists:sort(proplists:get_value(active_buckets, Status, [])),
                                                            MissingBuckets),
                    ?log_error("The following buckets became not ready on node ~p: ~p, those of them are active ~p",
                               [Name, MissingBuckets, MissingButActive])
            end,
            case ordsets:subtract(NewReady, OldReady) of
                [] -> ok;
                NewlyReady ->
                    ?log_info("The following buckets became ready on node ~p: ~p", [Name, NewlyReady])
            end,
            gen_event:notify(buckets_events, {significant_buckets_change, Name});
        _ ->
            ok
    end,

    gen_event:notify(ns_doctor_events, {node_status, Name, Status}),
    dict:store(Name, Status, Dict).

annotate_status(Node, Status, Now, LiveNodes) ->
    LastHeard = proplists:get_value(last_heard, Status),
    Diff      = misc:convert_time_unit(Now - LastHeard, microsecond),

    Stale = case Diff > ?STALE_TIME of
                true ->
                    [ stale | Status];
                false ->
                    Status
            end,
    case lists:member(Node, LiveNodes) of
        true ->
            Stale;
        false ->
            [ down | Stale ]
    end.

maybe_refresh_tasks_version(#state{nodes = Nodes,
                                   tasks_hash_nodes = VersionNodes} = State)
  when Nodes =:= VersionNodes ->
    State;
maybe_refresh_tasks_version(State) ->
    Nodes = State#state.nodes,
    TasksHash =
        dict:fold(
          fun (TaskNode, NodeInfo, Hash) ->
                  Hash1 = compute_local_tasks_hash(TaskNode, NodeInfo, Hash),
                  ActiveBuckets = proplists:get_value(active_buckets, NodeInfo, []),
                  add_hash({active_buckets, TaskNode, ActiveBuckets}, Hash1)
          end, new_hash(), Nodes),

    Buckets = ns_bucket:get_bucket_names(),
    RebalanceStatus = ns_config:read_key_fast(rebalance_status_uuid, undefined),
    RecoveryStatus = ns_orchestrator:is_recovery_running(),

    FinalHash = final_hash(add_hash({Buckets,
                                     RebalanceStatus, RecoveryStatus}, TasksHash)),
    case FinalHash =:= State#state.tasks_hash of
        true ->
            %% hash did not change, only nodes. Cool
            State#state{tasks_hash_nodes = Nodes};
        _ ->
            %% hash changed. Generate new version
            State#state{tasks_hash_nodes = Nodes,
                        tasks_hash = FinalHash,
                        tasks_version = integer_to_list(FinalHash)}
    end.

new_hash() ->
    sets:new().

add_hash(Term, HashSet) ->
    sets:add_element(erlang:phash2(Term), HashSet).

final_hash(HashSet) ->
    erlang:phash2(HashSet).

compute_local_tasks_hash(TaskNode, NodeInfo, Hash) ->
    lists:foldl(
      fun (Task, Acc) ->
              case proplists:get_value(type, Task) of
                  indexer ->
                      add_hash({indexer,
                                lists:keyfind(design_documents, 1, Task),
                                lists:keyfind(set, 1, Task)}, Acc);
                  view_compaction ->
                      add_hash({view_compaction,
                                lists:keyfind(design_documents, 1, Task),
                                lists:keyfind(set, 1, Task)}, Acc);
                  bucket_compaction ->
                      add_hash({bucket_compaction,
                                lists:keyfind(bucket, 1, Task)}, Acc);
                  loadingSampleBucket ->
                      add_hash(Task, Acc);
                  xdcr ->
                      add_hash(lists:keyfind(id, 1, Task), Acc);
                  warming_up ->
                      add_hash({warming_up,
                                lists:keyfind(bucket, 1, Task)}, Acc);
                  cluster_logs_collect ->
                      add_hash({cluster_logs_collect,
                                TaskNode,
                                lists:keydelete(perNode, 1, Task)}, Acc);
                  _ ->
                      Acc
              end
      end, Hash, proplists:get_value(local_tasks, NodeInfo, [])).

task_operation(extract, Indexer, RawTask)
  when Indexer =:= indexer ->
    {_, ChangesDone} = lists:keyfind(changes_done, 1, RawTask),
    {_, TotalChanges} = lists:keyfind(total_changes, 1, RawTask),
    {_, BucketName} = lists:keyfind(set, 1, RawTask),
    {_, DDocIds} = lists:keyfind(design_documents, 1, RawTask),

    [{{Indexer, BucketName, DDocId}, {ChangesDone, TotalChanges}}
       || DDocId <- DDocIds];
task_operation(extract, ViewCompaction, RawTask)
  when ViewCompaction =:= view_compaction ->
    {_, ChangesDone} = lists:keyfind(changes_done, 1, RawTask),
    {_, TotalChanges} = lists:keyfind(total_changes, 1, RawTask),
    {_, BucketName} = lists:keyfind(set, 1, RawTask),
    {_, DDocIds} = lists:keyfind(design_documents, 1, RawTask),
    {_, OriginalTarget} = lists:keyfind(original_target, 1, RawTask),
    {_, TriggerType} = lists:keyfind(trigger_type, 1, RawTask),

    [{{ViewCompaction, BucketName, DDocId, OriginalTarget, TriggerType},
      {ChangesDone, TotalChanges}}
       || DDocId <- DDocIds];
task_operation(extract, BucketCompaction, RawTask)
  when BucketCompaction =:= bucket_compaction ->
    {_, VBucketsDone} = lists:keyfind(vbuckets_done, 1, RawTask),
    {_, TotalVBuckets} = lists:keyfind(total_vbuckets, 1, RawTask),
    {_, BucketName} = lists:keyfind(bucket, 1, RawTask),
    {_, OriginalTarget} = lists:keyfind(original_target, 1, RawTask),
    {_, TriggerType} = lists:keyfind(trigger_type, 1, RawTask),
    [{{BucketCompaction, BucketName, OriginalTarget, TriggerType},
      {VBucketsDone, TotalVBuckets}}];
task_operation(extract, XDCR, RawTask)
  when XDCR =:= xdcr ->
    ChangesLeft = proplists:get_value(changes_left, RawTask, 0),
    DocsChecked = proplists:get_value(docs_checked, RawTask, 0),
    DocsWritten = proplists:get_value(docs_written, RawTask, 0),
    MaxVBReps = proplists:get_value(max_vbreps, RawTask, null),
    Errors = proplists:get_value(errors, RawTask, []),
    {_, Id} = lists:keyfind(id, 1, RawTask),
    [{{XDCR, Id}, {ChangesLeft, DocsChecked, DocsWritten, Errors, MaxVBReps}}];
task_operation(extract, _, _) ->
    ignore;

task_operation(finalize, {Indexer, BucketName, DDocId}, Changes)
  when Indexer =:= indexer ->
    finalize_indexer_or_compaction(Indexer, BucketName, DDocId, Changes);
task_operation(finalize, {ViewCompaction, BucketName, DDocId, _, _}, Changes)
  when ViewCompaction =:= view_compaction ->
    finalize_indexer_or_compaction(ViewCompaction, BucketName, DDocId, Changes);
task_operation(finalize, {BucketCompaction, BucketName, _, _}, {ChangesDone, TotalChanges})
  when BucketCompaction =:= bucket_compaction ->
    Progress = (ChangesDone * 100) div TotalChanges,

    [{type, BucketCompaction},
     {recommendedRefreshPeriod, 2.0},
     {status, running},
     {bucket, BucketName},
     {changesDone, ChangesDone},
     {totalChanges, TotalChanges},
     {progress, Progress}].


finalize_xcdr_plist({ChangesLeft, DocsChecked, DocsWritten, Errors, MaxVBReps}) ->
    FlattenedErrors = lists:flatten(Errors),
    SortedErrors = lists:reverse(lists:sort(FlattenedErrors)),
    Len = length(SortedErrors),
    OutputErrors =
        case Len > ?MAX_XDCR_TASK_ERRORS of
            true ->
                lists:sublist(SortedErrors, ?MAX_XDCR_TASK_ERRORS);
            false ->
                SortedErrors
        end,
    [{type, xdcr},
     {recommendedRefreshPeriod, 10.0},
     {changesLeft, ChangesLeft},
     {docsChecked, DocsChecked},
     {docsWritten, DocsWritten},
     {maxVBReps, MaxVBReps},
     {errors, OutputErrors}].


finalize_indexer_or_compaction(IndexerOrCompaction, BucketName, DDocId,
                               {ChangesDone, TotalChanges}) ->
    Progress = case TotalChanges =:= 0 orelse ChangesDone > TotalChanges of
                   true -> 100;
                   _ ->
                       (ChangesDone * 100) div TotalChanges
               end,
    [{type, IndexerOrCompaction},
     {recommendedRefreshPeriod, 2.0},
     {status, running},
     {bucket, BucketName},
     {designDocument, DDocId},
     {changesDone, ChangesDone},
     {totalChanges, TotalChanges},
     {progress, Progress}].

task_operation(fold, {indexer, _, _},
               {ChangesDone1, TotalChanges1},
               {ChangesDone2, TotalChanges2}) ->
    {ChangesDone1 + ChangesDone2, TotalChanges1 + TotalChanges2};
task_operation(fold, {view_compaction, _, _, _, _},
               {ChangesDone1, TotalChanges1},
               {ChangesDone2, TotalChanges2}) ->
    {ChangesDone1 + ChangesDone2, TotalChanges1 + TotalChanges2};
task_operation(fold, {bucket_compaction, _, _, _},
               {ChangesDone1, TotalChanges1},
               {ChangesDone2, TotalChanges2}) ->
    {ChangesDone1 + ChangesDone2, TotalChanges1 + TotalChanges2};
task_operation(fold, {xdcr, _},
              {ChangesLeft1, DocsChecked1, DocsWritten1, Errors1, MaxVBReps1},
              {ChangesLeft2, DocsChecked2, DocsWritten2, Errors2, MaxVBReps2}) ->
    NewMaxVBReps = case MaxVBReps1 =:= null orelse MaxVBReps2 =:= null of
                       true -> null;
                       _ -> MaxVBReps1 + MaxVBReps2
                   end,
    {ChangesLeft1 + ChangesLeft2,
     DocsChecked1 + DocsChecked2,
     DocsWritten1 + DocsWritten2,
     [Errors1 | Errors2],
     NewMaxVBReps}.


task_maybe_add_cancel_uri({bucket_compaction, BucketName, {OriginalTarget}, manual},
                          Value, PoolId) ->
    OriginalTargetType = proplists:get_value(type, OriginalTarget),
    true = (OriginalTargetType =/= undefined),

    Ending = case OriginalTargetType of
                 db ->
                     "cancelDatabasesCompaction";
                 bucket ->
                     "cancelBucketCompaction"
             end,

    URI = menelaus_util:bin_concat_path(
            ["pools", PoolId, "buckets", BucketName,
             "controller", Ending]),

    [{cancelURI, URI} | Value];
task_maybe_add_cancel_uri({view_compaction, BucketName, _DDocId,
                           {OriginalTarget}, manual},
                          Value, PoolId) ->
    OriginalTargetType = proplists:get_value(type, OriginalTarget),
    true = (OriginalTargetType =/= undefined),

    URIComponents =
        case OriginalTargetType of
            bucket ->
                ["pools", PoolId, "buckets", BucketName,
                 "controller", "cancelBucketCompaction"];
            view ->
                TargetDDocId = proplists:get_value(name, OriginalTarget),
                true = (TargetDDocId =/= undefined),

                ["pools", PoolId, "buckets", BucketName, "ddoc", TargetDDocId,
                 "controller", "cancelViewCompaction"]
        end,

    URI = menelaus_util:bin_concat_path(URIComponents),
    [{cancelURI, URI} | Value];
task_maybe_add_cancel_uri(_, Value, _) ->
    Value.


pick_latest_cluster_collect_task(AllNodeTasks) ->
    AllCollectTasks = [{N,
                        proplists:get_value(status, T),
                        proplists:get_value(timestamp, T),
                        T}
                       || {N, Ts} <- AllNodeTasks,
                          T <- Ts,
                          lists:keyfind(type, 1, T) =:= {type, cluster_logs_collect}],

    TaskGrEq =
        fun ({_N1, S1, Ts1, _},
             {_N2, S2, Ts2, _}) ->
                R1 = (S1 =:= running),
                R2 = (S2 =:= running),
                case R1 =:= R2 of
                    %% if they're both are running
                    %% or not running then we
                    %% compare timestamps
                    true -> Ts1 >= Ts2;
                    %% if they're different, then
                    %% one of them is
                    %% "running". And first task is
                    %% "greater" if it's running
                    false -> R1
                end
        end,

    case AllCollectTasks of
        [] -> [];
        [Head | Tail] ->
            {Node, _S, _Ts, Task} =
                lists:foldl(
                  fun (CandidateTask, AccTask) ->
                          case TaskGrEq(AccTask, CandidateTask) of
                              true -> AccTask;
                              false -> CandidateTask
                          end
                  end, Head, Tail),
            TSProp =
                case proplists:get_value(timestamp, Task) of
                    undefined -> [];
                    TS ->
                        [{ts, iolist_to_binary(format_time(TS))}]
                end,

            StatusProp = case lists:keyfind(status, 1, Task) of
                             SX when is_tuple(SX) -> [SX];
                             false -> []
                         end,

            PerNode = [{N, {struct, [{couch_util:to_binary(K), couch_util:to_binary(V)} || {K, V} <- KV]}}
                       || {N, KV} <- proplists:get_value(perNode, Task, [])],

            RefreshProp = case StatusProp of
                              [{status, running}] ->
                                  [{recommendedRefreshPeriod, 2},
                                   {cancelURI, <<"/controller/cancelLogsCollection">>}
                                  ];
                              _ ->
                                  []
                          end,

            ProgressProp = case lists:keyfind(progress, 1, Task) of
                               false -> [];
                               PX -> [PX]
                           end,

            FinalTask = [{node, Node},
                         {type, clusterLogsCollection},
                         {perNode, {struct, PerNode}}]
                ++ ProgressProp ++ TSProp ++ StatusProp ++ RefreshProp,

            [FinalTask]
    end.

format_time({{Year,Month,Day},{Hour,Min,Sec}}) ->
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
                  [Year, Month, Day, Hour, Min, Sec]).

do_build_tasks_list(NodesDict, PoolId, AllRepDocs, Buckets, RebStatusTimeout) ->
    AllNodeTasks =
        dict:fold(
          fun (Node, NodeInfo, Acc) ->
                  NodeTasks = proplists:get_value(local_tasks, NodeInfo, []),
                  [{Node, NodeTasks} | Acc]
          end, [], NodesDict),

    AllRawTasks = lists:append([Ts || {_N, Ts} <- AllNodeTasks]),

    TasksDict =
        lists:foldl(
          fun (RawTask, TasksDict0) ->
                  case task_operation(extract, proplists:get_value(type, RawTask), RawTask) of
                      ignore -> TasksDict0;
                      Signatures ->
                          lists:foldl(
                            fun ({Signature, Value}, AccDict) ->
                                    dict:update(
                                      Signature,
                                      fun (ValueOld) ->
                                              task_operation(fold, Signature, ValueOld, Value)
                                      end,
                                      Value,
                                      AccDict)
                            end, TasksDict0, Signatures)
                  end
          end, dict:new(), AllRawTasks),

    CompactionAndIndexingTasks =
        dict:fold(fun ({xdcr, _}, _, Acc) -> Acc;
                      (Signature, Value, Acc) ->
                          Value1 = task_operation(finalize, Signature, Value),
                          FinalValue = task_maybe_add_cancel_uri(Signature, Value1, PoolId),
                          [FinalValue | Acc]
                  end, [], TasksDict),

    XDCRTasks = build_xdcr_tasks(TasksDict, AllRepDocs),
    SampleBucketTasks = build_sample_buckets_tasks(AllRawTasks),
    WarmupTasks = build_warmup_tasks(AllRawTasks),
    CollectTask = pick_latest_cluster_collect_task(AllNodeTasks),
    RebalanceTask = build_rebalance_task(RebStatusTimeout),
    RecoveryTask = build_recovery_task(PoolId),
    OrphanBucketsTasks = build_orphan_buckets_tasks(Buckets, NodesDict),
    GSITask = build_gsi_task(),

    Tasks0 = lists:append([CompactionAndIndexingTasks, XDCRTasks,
                           SampleBucketTasks, WarmupTasks, CollectTask,
                           RebalanceTask, RecoveryTask, OrphanBucketsTasks,
                           GSITask]),

    Tasks1 =
        lists:sort(
          fun (A, B) ->
                  TypeA = task_type(A),
                  TypeB = task_type(B),

                  PrioA = task_priority(TypeA),
                  PrioB = task_priority(TypeB),
                  {PrioA, task_name(A, TypeA)} =< {PrioB, task_name(B, TypeB)}
          end, Tasks0),

    [{struct, V} || V <- Tasks1].

build_xdcr_tasks(TasksDict, AllRepDocs) ->
    [begin
         {_, Id} = lists:keyfind(id, 1, Doc0),
         Sig = {xdcr, Id},
         {type, Type0} = lists:keyfind(type, 1, Doc0),
         Type = case Type0 of
                    <<"xdc">> ->
                        capi;
                    <<"xdc-xmem">> ->
                        xmem
                end,

         Doc1 = lists:keydelete(type, 1, Doc0),
         Doc2 = [{replicationType, Type} | Doc1],
         Doc3 =
             case dict:find(Sig, TasksDict) of
                 {ok, Value} ->
                     PList = finalize_xcdr_plist(Value),
                     Status =
                         case (proplists:get_bool(pauseRequested, Doc2)
                               andalso proplists:get_value(maxVBReps, PList) =:= 0) of
                             true ->
                                 paused;
                             _ ->
                                 running
                         end,
                     [{status, Status} | Doc2] ++ PList;
                 _ ->
                     [{status, notRunning}, {type, xdcr} | Doc2]
             end,

         CancelURI = menelaus_util:bin_concat_path(["controller", "cancelXDCR", Id]),
         SettingsURI = menelaus_util:bin_concat_path(["settings", "replications", Id]),

         [{cancelURI, CancelURI},
          {settingsURI, SettingsURI} | Doc3]
     end || Doc0 <- AllRepDocs].

build_sample_buckets_tasks(AllRawTasks) ->
    SampleBucketTasks0 = lists:filter(fun (RawTask) ->
                                              case lists:keyfind(type, 1, RawTask) of
                                                  {_, loadingSampleBucket} -> true;
                                                  _ -> false
                                              end
                                      end, AllRawTasks),
    [[{status, running} | KV] || KV <- SampleBucketTasks0].

build_warmup_tasks(AllRawTasks) ->
    WarmupTasks0 = lists:filter(fun (RawTask) ->
                                        case lists:keyfind(type, 1, RawTask) of
                                            {_, warming_up} -> true;
                                            _ -> false
                                        end
                                end, AllRawTasks),
    [[{status, running} | KV] || KV <- WarmupTasks0].

build_rebalance_task(Timeout) ->
    Task = do_build_rebalance_task(Timeout),
    [Task].

jsonify_rebalance_type(graceful_failover) ->
    gracefulFailover;
jsonify_rebalance_type(T) ->
    T.

do_build_rebalance_task(Timeout) ->
    case ns_config:search(rebalance_status_uuid) of
        false -> [];
        {value, undefined} -> [];
        {value, Id} -> [{statusId, Id}]
    end ++
        case ns_orchestrator:rebalance_progress(Timeout) of
            {running, PerNode} ->
                DetailedProgress = get_detailed_progress(),
                RebalanceInfo =
                    case ns_rebalance_observer:get_rebalance_info() of
                        {ok, RV} -> RV;
                        _ -> []
                    end,

                [{type, rebalance},
                 {subtype,
                  jsonify_rebalance_type(
                    ns_config:read_key_fast(rebalance_type, rebalance))},
                 {recommendedRefreshPeriod, 0.25},
                 {status, running},
                 {progress, case lists:foldl(fun ({_, Progress}, {Total, Count}) ->
                                                     {Total + Progress, Count + 1}
                                             end, {0, 0}, PerNode) of
                                {_, 0} -> 0;
                                {TotalRebalanceProgress, RebalanceNodesCount} ->
                                    TotalRebalanceProgress * 100.0 / RebalanceNodesCount
                            end},
                 {perNode,
                  {struct, [{Node, {struct, [{progress, Progress * 100}]}}
                            || {Node, Progress} <- PerNode]}},
                 {detailedProgress, DetailedProgress}] ++ RebalanceInfo;
            FullProgress ->
                ReportURI =
                    case ns_rebalance_report_manager:get_last_report_uuid() of
                        undefined ->
                            [];
                        UUID ->
                            URI = list_to_binary(
                                    "/logs/rebalanceReport?reportID="),
                            [{lastReportURI, <<URI/binary, UUID/binary>>}]
                    end,
                [{type, rebalance},
                 {status, notRunning},
                 {statusIsStale, FullProgress =/= not_running},
                 {masterRequestTimedOut, (FullProgress =:= {error, timeout})}
                 | case ns_config:search(rebalance_status) of
                       {value, {none, ErrorMessage}} ->
                           [{errorMessage, iolist_to_binary(ErrorMessage)}];
                       _ -> []
                   end] ++ ReportURI
        end.

build_orphan_buckets_tasks(Buckets, NodesDict) ->
    Orphans =
        dict:fold(
          fun (Node, NodeInfo, Acc) ->
                  NodeBuckets = proplists:get_value(active_buckets, NodeInfo, []),
                  case NodeBuckets -- Buckets of
                      [] ->
                          Acc;
                      NodeOrphans ->
                          lists:foldl(
                            fun (Bucket, D) ->
                                    misc:dict_update(Bucket,
                                                     fun (Nodes) ->
                                                             [Node | Nodes]
                                                     end, [], D)
                            end, Acc, NodeOrphans)
                  end
          end, dict:new(), NodesDict),
    dict:fold(
      fun (Bucket, BucketNodes, Acc) ->
              Task = [{type, orphanBucket},
                      {bucket, list_to_binary(Bucket)},
                      {nodes, lists:usort(BucketNodes)},
                      {status, running},
                      {recommendedRefreshPeriod, 2.0}],
              [Task | Acc]
      end, [], Orphans).

get_detailed_progress() ->
    case ns_rebalance_observer:get_detailed_progress() of
        {ok, GlobalDetails, PerNode} ->
            PerNodeJSON0 = [{N, {struct, [{ingoing, {struct, Ingoing}},
                                          {outgoing, {struct, Outgoing}}]}} ||
                               {N, Ingoing, Outgoing} <- PerNode],
            PerNodeJSON = {struct, PerNodeJSON0},
            {struct, GlobalDetails ++ [{perNode, PerNodeJSON}]};
        _ ->
            {struct, []}
    end.

task_type(Task) ->
    {type, Type} = lists:keyfind(type, 1, Task),
    Type.

task_priority(recovery) ->
    0;
task_priority(orphanBucket) ->
    0;
task_priority(rebalance) ->
    1;
task_priority(xdcr) ->
    2;
task_priority(indexer) ->
    3;
task_priority(bucket_compaction) ->
    4;
task_priority(view_compaction) ->
    5;
task_priority(_) ->
    6.

task_name(Task, xdcr) ->
    proplists:get_value(id, Task);
task_name(Task, Type)
  when Type =:= indexer; Type =:= bucket_compaction;
       Type =:= view_compaction; Type =:= orphanBucket ->
    Bucket = proplists:get_value(bucket, Task),
    true = (Bucket =/= undefined),

    MaybeDDoc = proplists:get_value(designDocument, Task),
    {Bucket, MaybeDDoc};
task_name(_Task, _Type) ->
    undefined.

get_node(Node, NodeStatuses) ->
    case dict:find(Node, NodeStatuses) of
        {ok, Info} -> Info;
        error -> [down]
    end.

build_gsi_task() ->
    {ok, Indexes, Stale, _} = service_index:get_indexes(),
    build_gsi_index_task(Indexes, Stale, []).

build_gsi_index_task([], _, Acc) ->
    Acc;
build_gsi_index_task([Index | Rest], Stale, Acc) ->
    Status = proplists:get_value(status, Index),
    case Status of
        <<"Building">> ->
            Task = [{type, global_indexes},
                    {recommendedRefreshPeriod, 2.0},
                    {status, running},
                    {bucket, proplists:get_value(bucket, Index)},
                    {index, proplists:get_value(index, Index)},
                    {id, proplists:get_value(id, Index)},
                    {progress, proplists:get_value(progress, Index)},
                    {statusIsStale, Stale}],
            build_gsi_index_task(Rest, Stale, [Task | Acc]);
        _ ->
            %% Skip this index if it is already built
            build_gsi_index_task(Rest, Stale, Acc)
    end.

build_recovery_task(PoolId) ->
    case ns_orchestrator:recovery_status() of
        not_in_recovery ->
            [];
        {ok, Status} ->
            Bucket = proplists:get_value(bucket, Status),
            RecoveryUUID = proplists:get_value(uuid, Status),
            true = (Bucket =/= undefined),
            true = (RecoveryUUID =/= undefined),

            StopURI = menelaus_util:bin_concat_path(
                        ["pools", PoolId, "buckets", Bucket,
                         "controller", "stopRecovery"],
                        [{recovery_uuid, RecoveryUUID}]),
            CommitURI = menelaus_util:bin_concat_path(
                          ["pools", PoolId, "buckets", Bucket,
                           "controller", "commitVBucket"],
                          [{recovery_uuid, RecoveryUUID}]),
            RecoveryStatusURI =
                menelaus_util:bin_concat_path(
                  ["pools", PoolId, "buckets", Bucket, "recoveryStatus"],
                  [{recovery_uuid, RecoveryUUID}]),

            [[{type, recovery},
              {bucket, list_to_binary(Bucket)},
              {uuid, RecoveryUUID},
              {recommendedRefreshPeriod, 10.0},

              {stopURI, StopURI},
              {commitVBucketURI, CommitURI},
              {recoveryStatusURI, RecoveryStatusURI}]]
    end.

process_heartbeat(Node, Status, State) ->
    Nodes = update_status(Node, Status, State#state.nodes),
    NewState0 = State#state{nodes=Nodes},
    NewState = maybe_refresh_tasks_version(NewState0),
    case NewState0#state.tasks_hash =/= NewState#state.tasks_hash of
        true ->
            gen_event:notify(buckets_events, {significant_buckets_change, Node});
        _ ->
            ok
    end,
    NewState.
