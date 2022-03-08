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

-export([init/1,
         get_stage_info/2,
         get_progress/1,
         update_progress/3,
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
init(StageNodes) ->
    PerStageProgress = dict:from_list(init_per_stage_progress(StageNodes)),
    Aggregated = aggregate(PerStageProgress),
    StageInfo = init_per_stage_info(StageNodes),
    #stage_info{per_stage_progress = PerStageProgress,
                aggregated = Aggregated,
                per_stage_info = StageInfo}.

init_per_stage_progress(StageNodes) ->
    [{Stage, dict:from_list([{N, 0} || N <- Nodes])} ||
        {Stage, Nodes} <- StageNodes, Nodes =/= []].

init_per_stage_info(StageNodes) ->
    [{Stage, #stage_details{}} || {Stage, Nodes} <- StageNodes, Nodes =/= []].

get_progress(#stage_info{aggregated = Aggregated}) ->
    Aggregated.

update_progress(
  Stage, StageProgress,
  #stage_info{per_stage_progress = OldPerStageProgress} = StageInfo) ->
    NewPerStageProgress = do_update_progress(Stage, StageProgress,
                                             OldPerStageProgress),
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

get_stage_info(StageInfo, AllStageDetails) ->
    {get_per_stage_info(StageInfo, AllStageDetails)}.

diff_timestamp(false, false) ->
    false;
diff_timestamp(false, StartTS) ->
    diff_timestamp(os:timestamp(), StartTS);
diff_timestamp(EndTS, StartTS) ->
    round(timer:now_diff(EndTS, StartTS) / 1000).

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
    case dict:find(ProgressStage, PerStageProgress) of
        {ok, _} ->
            StageInfo;
        _ ->
            [{ProgressStage, Dict}] =
                init_per_stage_progress([{ProgressStage, Nodes}]),
            update_progress(ProgressStage, Dict, StageInfo)
    end;
maybe_create_new_stage_progress(_Stage, _Info, StageInfo) ->
    StageInfo.
