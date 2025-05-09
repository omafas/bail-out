%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(rebalance_stage_info).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/1,
         get_stage_info/2,
         get_progress/1,
         get_current_stage/1,
         get_progress_for_stage/3,
         update_progress/4,
         update_stage_info/4,
         diff_timestamp/2,
         binarify_timestamp/1]).
-export_type([stage_info/0]).

-record(stage_details, {start_time = false,
                        complete_time = false,
                        sub_stages = [] :: [{atom(), #stage_details{}}],
                        notable_events = []}).

-record(stage_info, {per_stage_progress :: dict:dict(),
                     aggregated :: dict:dict(),
                     per_stage_info :: [{atom(), #stage_details{}}]}).

-type stage_info() :: #stage_info{}.

%% Need StageNodes as when rebalance starts we need to show a minimum stages of
%% rebalance that are expected to occur, usually the services involved.
-spec init([node()]) -> #stage_info{}.
init(StageNodes) ->
    PerStageProgress = dict:from_list(init_per_stage_progress(StageNodes)),
    Aggregated = aggregate(PerStageProgress),
    StageInfo = init_per_stage_info(StageNodes),
    lists:foreach(
      fun ({Stage, _Nodes}) ->
              notify_progress(Stage, 0)
      end, StageNodes),
    #stage_info{per_stage_progress = PerStageProgress,
                aggregated = Aggregated,
                per_stage_info = StageInfo}.

init_per_stage_progress(StageNodes) ->
    [{Stage, dict:from_list([{N, 0} || N <- Nodes])} ||
        {Stage, Nodes} <- StageNodes, Nodes =/= []].

init_per_stage_info(StageNodes) ->
    [{Stage, #stage_details{}} || {Stage, Nodes} <- StageNodes, Nodes =/= []].

-spec get_progress(#stage_info{}) -> dict:dict().
get_progress(#stage_info{aggregated = Aggregated}) ->
    Aggregated.

-spec update_progress(atom(), boolean(), dict:dict(), #stage_info{}) ->
    #stage_info{}.
update_progress(
  ProgressStage, NotifyMetric, StageProgress,
  #stage_info{per_stage_progress = OldPerStageProgress} = StageInfo) ->
    NewPerStageProgress = do_update_progress(ProgressStage, StageProgress,
                                             OldPerStageProgress),
    NotifyMetric andalso notify_progress(ProgressStage, NewPerStageProgress),
    Aggregated = aggregate(NewPerStageProgress),
    StageInfo#stage_info{
      per_stage_progress = NewPerStageProgress,
      aggregated = Aggregated}.

do_update_progress(Stage, StageProgress, PerStage) ->
    dict:update(Stage,
                fun (OldStageProgress) ->
                        dict:merge(fun (_, _, New) ->
                                           New
                                   end, OldStageProgress, StageProgress)
                end, StageProgress, PerStage).

notify_progress(Stage, Progress) when is_number(Progress) ->
    %% Notify with no expiration so that we don't get gaps when progress isn't
    %% made for >= 3 mins
    ns_server_stats:notify_gauge(
      {rebalance_progress,
       [{stage, Stage}]},
      Progress,
      #{expiration_s => infinity});
notify_progress(Stage, PerStageProgress) ->
    case dict:find(Stage, PerStageProgress) of
        error ->
            0;
        {ok, NewStageProgress} ->
            SummedProgress =
                dict:fold(
                  fun (_Node, NodeProgress, Sum) ->
                          case NodeProgress of
                              %% Some stages report sub-stage with the progress
                              {_SubStage, P} when is_number(P) -> Sum + P;
                              P when is_number(P) -> Sum + P
                          end
                  end, 0, NewStageProgress),

            Progress = SummedProgress / dict:size(NewStageProgress),
            notify_progress(Stage, Progress)
    end.

aggregate(PerStage) ->
    TmpAggr = dict:fold(
                fun (_, StageProgress, AggAcc) ->
                        dict:fold(
                          fun (Node, NodeProgress, Acc) ->
                                  misc:dict_update(
                                    Node,
                                    fun ({Count, Sum}) ->
                                            {Count + 1, Sum + NodeProgress}
                                    end, {0, 0}, Acc)
                          end, AggAcc, StageProgress)
                end, dict:new(), PerStage),

    dict:map(fun (_, {Count, Sum}) ->
                     Sum / Count
             end, TmpAggr).

-spec get_stage_info(#stage_info{}, [{atom(), list()}]) -> {list()}.
get_stage_info(StageInfo, AllStageDetails) ->
    {get_per_stage_info(StageInfo, AllStageDetails)}.

-spec get_current_stage(stage_info()) -> atom().
get_current_stage(#stage_info{per_stage_info = PerStageInfo}) ->
    Stages = lists:filtermap(
               fun ({Stage, #stage_details{start_time = S,
                                           complete_time = C}}) ->
                       case S =/= false andalso C =:= false of
                           true -> {true, Stage};
                           false -> false
                       end
               end, PerStageInfo),
    case Stages of
        [] -> undefined;
        [Stage | _] -> Stage
    end.

-spec get_progress_for_stage(atom(), #stage_info{}, list()) -> list().
get_progress_for_stage(Stage,
                       #stage_info{per_stage_progress = PerStageProgress},
                       Default) ->
    case dict:find(Stage, PerStageProgress) of
        {ok, Progress} ->
            dict:to_list(Progress);
        error ->
            Default
    end.

-spec diff_timestamp(false | erlang:timestamp(), false | erlang:timestamp()) ->
    integer().
diff_timestamp(false, false) ->
    false;
diff_timestamp(false, StartTS) ->
    diff_timestamp(os:timestamp(), StartTS);
diff_timestamp(EndTS, StartTS) ->
    round(timer:now_diff(EndTS, StartTS) / 1000).

-spec binarify_timestamp(false | erlang:timestamp()) -> binary().
binarify_timestamp(false) ->
    false;
binarify_timestamp(Time) ->
    erlang:list_to_binary(misc:timestamp_iso8601(Time, local)).

get_per_stage_info(#stage_info{
                      per_stage_progress = PerStageProgress,
                      per_stage_info = PerStageInfo}, AllStageDetails) ->
    AllStageProgress = get_per_stage_progress(PerStageProgress),
    lists:map(
      fun ({Stage, StageInfo}) ->
              construct_per_stage_json(AllStageProgress,
                                       AllStageDetails,
                                       Stage,
                                       StageInfo)
      end, PerStageInfo).

construct_per_stage_json(AllStageProgress, AllStageDetails, Stage, StageInfo) ->
    {ok, PerNodeProgress} = dict:find(Stage, AllStageProgress),
    StageDetails = construct_per_stage_details_json(Stage, AllStageDetails),
    TotalStageProgress = case lists:foldl(fun ({_, P}, {Total, Count}) ->
                                                  {Total + P, Count + 1}
                                          end, {0, 0}, PerNodeProgress) of
                             {_, 0} ->
                                 0;
                             {TotalProgress, NodesCount} ->
                                 TotalProgress * 100.0 / NodesCount
                         end,
    ProgressInfoJson = construct_per_stage_progress_json(TotalStageProgress,
                                                         PerNodeProgress,
                                                         StageInfo),
    StageInfoJson = construct_per_stage_info_json(StageInfo, AllStageProgress,
                                                  AllStageDetails),
    {list_to_binary(get_stage_name(Stage)),
     {ProgressInfoJson ++ StageInfoJson ++ StageDetails}}.

get_stage_name(kv_delta_recovery) ->
    "deltaRecovery";
get_stage_name(fts) ->
    "search";
get_stage_name(Name) when is_atom(Name) ->
    ns_cluster_membership:user_friendly_service_name(Name);
get_stage_name(Name) when is_list(Name) ->
    Name.

construct_per_stage_details_json(Stage, AllStageDetails) ->
    case lists:keyfind(Stage, 1, AllStageDetails) of
        {Stage, StageDetails} ->
            [{details, StageDetails}];
        false ->
            []
    end.

construct_per_stage_progress_json(
  TSP, _, #stage_details{complete_time = false}) when TSP == 0 ->
    [];
construct_per_stage_progress_json(TotalStageProgress, PerNodeProgress,
                                  #stage_details{complete_time = false}) ->
    [{totalProgress, TotalStageProgress},
     {perNodeProgress, {PerNodeProgress}}];
construct_per_stage_progress_json(_, PerNodeProgress, _StageInfo) ->
    Completed = [{N, 1.0} || {N, _} <- PerNodeProgress],
    [{totalProgress, 100.0},
     {perNodeProgress, {Completed}}].

construct_per_stage_info_json(#stage_details{
                                 start_time = StartTime,
                                 complete_time = EndTime,
                                 sub_stages = SubStages,
                                 notable_events = NotableEvents},
                              AllStageProgress, AllStageDetails) ->
    SubStagesInfo = case SubStages of
                        [] -> [];
                        _ -> [{subStages,
                               {[construct_per_stage_json(AllStageProgress,
                                                          AllStageDetails,
                                                          SubStage,
                                                          SubStageInfo) ||
                                    {SubStage, SubStageInfo} <- SubStages]}}]
                    end,
    Events = case NotableEvents of
                 [] -> [];
                 _ -> [{events, {NotableEvents}}]
             end,

    [{startTime, binarify_timestamp(StartTime)},
     {completedTime, binarify_timestamp(EndTime)},
     {timeTaken, diff_timestamp(EndTime, StartTime)}] ++ SubStagesInfo ++ Events.

get_per_stage_progress(PerStageProgress) ->
    dict:map(fun (_, StageProgress) ->
                     dict:to_list(StageProgress)
             end, PerStageProgress).

update_stage({started, _}, TS, StageInfo) ->
    StageInfo#stage_details{start_time = TS, complete_time = false};
update_stage(completed, TS, StageInfo) ->
    StageInfo#stage_details{complete_time = TS};
update_stage({notable_event, Text}, TS,
             #stage_details{notable_events = NotableEvents} = StageInfo) ->
    Time = binarify_timestamp(TS),
    Msg = list_to_binary(Text),
    StageInfo#stage_details{notable_events = [{Time, Msg} | NotableEvents]}.

-spec update_stage_info(atom() | [atom()],
                        {started, [node()]} | completed
                       | {notable_event, string()},
                        erlang:timestamp(), #stage_info{}) -> #stage_info{}.
update_stage_info(Stage, StageInfoUpdate, TS, StageInfo) ->
    NewStageInfo =
        maybe_create_new_stage_progress(Stage, StageInfoUpdate, StageInfo),
    NewPerStageInfo =
        update_stage_info_rec(Stage, StageInfoUpdate, TS,
                              NewStageInfo#stage_info.per_stage_info),
    NewStageInfo#stage_info{per_stage_info = NewPerStageInfo}.

update_stage_info_rec([Stage | _] = AllStages, Update, TS, AllStageInfo) ->
    case lists:keysearch(Stage, 1, AllStageInfo) of
        false ->
            case Update of
                {started, Nodes} when Nodes =/= [] ->
                    update_existing_stage_info(
                      AllStages, Update, #stage_details{}, TS,
                      [{Stage, #stage_details{}} | AllStageInfo]);
                _ ->
                    AllStageInfo
            end;
        {value, {Stage, OldInfo}} ->
            update_existing_stage_info(AllStages, Update, OldInfo, TS,
                                       AllStageInfo)
    end.

update_existing_stage_info([Stage | SubStages], Update, OldInfo, TS,
                           AllStageInfo) ->
    NewStageInfo =
        case SubStages of
            [] ->
                update_stage(Update, TS, OldInfo);
            _ ->
                NewSubStages =
                    update_stage_info_rec(SubStages, Update, TS,
                                          OldInfo#stage_details.sub_stages),
                OldInfo#stage_details{sub_stages = NewSubStages}
        end,
    lists:keyreplace(Stage, 1, AllStageInfo, {Stage, NewStageInfo}).

maybe_create_new_stage_progress(
  Stage, {started, Nodes},
  #stage_info{per_stage_progress = PerStageProgress} = StageInfo)
  when Nodes =/= [] ->
    ProgressStage = lists:last(Stage),
    %% Only notify metric if this is a top level stage, not a sub-stage
    %% (such as failover of each bucket, and the kv_delta_recovery sub-stage)
    NotifyMetric = length(Stage) =:= 1,
    case dict:find(ProgressStage, PerStageProgress) of
        {ok, _} ->
            StageInfo;
        _ ->
            [{ProgressStage, Dict}] =
                init_per_stage_progress([{ProgressStage, Nodes}]),
            update_progress(ProgressStage, NotifyMetric, Dict, StageInfo)
    end;
maybe_create_new_stage_progress([Stage], completed, StageInfo) ->
    %% Make sure that completed stages get their progress set to 100%.
    %% [Stage] ensures that we ignore sub-stages
    notify_progress(Stage, 1),
    StageInfo;
maybe_create_new_stage_progress(_Stage, _Info, StageInfo) ->
    StageInfo.

-ifdef(TEST).
test_get_current_stage(PerStageInfo, ExpectedStage) ->
    ?assertEqual(ExpectedStage,
                 get_current_stage(#stage_info{per_stage_info = PerStageInfo})).

get_current_stage_test() ->
    [test_get_current_stage([], undefined),
     test_get_current_stage([{kv, #stage_details{start_time = false}}],
                             undefined),
     test_get_current_stage([{kv, #stage_details{start_time = true,
                                                 complete_time = true}}],
                             undefined),
     test_get_current_stage([{kv, #stage_details{start_time = false}},
                             {index, #stage_details{start_time = true,
                                                    complete_time = false}}],
                             index),
     test_get_current_stage([{kv, #stage_details{start_time = true,
                                                 complete_time = false}},
                             {index, #stage_details{start_time = true,
                                                    complete_time = true}}],
                             kv)].
-endif.
