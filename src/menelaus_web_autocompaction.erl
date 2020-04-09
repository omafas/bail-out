%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
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

%% @doc implementation of aultautocompaction REST API's

-module(menelaus_web_autocompaction).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_get_global_settings/1,
         handle_set_global_settings/1,
         build_bucket_settings/1,
         build_global_settings/1,
         parse_validate_purge_interval/2,
         parse_validate_settings/2]).

-import(menelaus_util,
        [reply_json/3,
         parse_validate_number/4,
         parse_validate_boolean_field/3]).


handle_get_global_settings(Req) ->
    JSON = [{autoCompactionSettings, build_global_settings(ns_config:latest())},
            {purgeInterval, compaction_api:get_purge_interval(global)}],
    reply_json(Req, {struct, JSON}, 200).

build_global_settings(Config) ->
    IndexCompaction = index_settings_manager:get(compaction),
    true = (IndexCompaction =/= undefined),
    {_, Fragmentation} = lists:keyfind(fragmentation, 1, IndexCompaction),

    CompMode = index_settings_manager:get(compactionMode),
    true = (CompMode =/= undefined),
    Circ0 = index_settings_manager:get(circularCompaction),
    true = (Circ0 =/= undefined),
    Int = proplists:get_value(interval, Circ0),
    Circ1 = [{abort_outside,
              proplists:get_value(abort_outside, Circ0)}] ++ Int,
    Circ = [{daysOfWeek, proplists:get_value(daysOfWeek, Circ0)},
            {interval, build_allowed_time_period(Circ1)}],

    IndexSettings =
        [{indexCompactionMode, CompMode},
         {indexCircularCompaction, {struct, Circ}},
         {indexFragmentationThreshold,
          {struct, [{percentage, Fragmentation}]}}],

    case ns_config:search(Config, autocompaction) of
        false ->
            do_build_settings([], IndexSettings);
        {value, ACSettings} ->
            do_build_settings(ACSettings, IndexSettings)
    end.

build_bucket_settings(Settings) ->
    do_build_settings(Settings, []).

do_build_settings(Settings, Extra) ->
    PropFun = fun ({JSONName, CfgName}) ->
                      case proplists:get_value(CfgName, Settings) of
                          undefined -> [];
                          {Percentage, Size} ->
                              [{JSONName, {struct, [{percentage, Percentage},
                                                    {size,  Size}]}}]
                      end
              end,
    DBAndView = lists:flatmap(PropFun,
                              [{databaseFragmentationThreshold, database_fragmentation_threshold},
                               {viewFragmentationThreshold, view_fragmentation_threshold}]),

    {struct, [{parallelDBAndViewCompaction, proplists:get_bool(parallel_db_and_view_compaction,
                                                               Settings)}
              | case proplists:get_value(allowed_time_period, Settings) of
                    undefined -> [];
                    V -> [{allowedTimePeriod, build_allowed_time_period(V)}]
                end] ++ DBAndView ++ Extra}.

build_allowed_time_period(AllowedTimePeriod) ->
    {struct, [{JSONName, proplists:get_value(CfgName, AllowedTimePeriod)}
              || {JSONName, CfgName} <- [{fromHour, from_hour},
                                         {toHour, to_hour},
                                         {fromMinute, from_minute},
                                         {toMinute, to_minute},
                                         {abortOutside, abort_outside}]]}.

handle_set_global_settings(Req) ->
    Params = mochiweb_request:parse_post(Req),
    SettingsRV = parse_validate_settings(Params, true),
    PurgeIntervalRV = parse_validate_purge_interval(Params, global),
    ValidateOnly = (proplists:get_value("just_validate", mochiweb_request:parse_qs(Req)) =:= "1"),
    case {ValidateOnly, SettingsRV, PurgeIntervalRV} of
        {_, {errors, Errors}, _} ->
            reply_json(Req, {struct, [{errors, {struct, Errors}}]}, 400);
        {_, _, [{error, Field, Msg}]} ->
            reply_json(Req, {struct, [{errors, {struct, [{Field, Msg}]}}]}, 400);
        {true, {ok, _ACSettings, _}, _} ->
            reply_json(Req, {struct, [{errors, {struct, []}}]}, 200);
        {false, {ok, ACSettings, MaybeIndex}, _} ->
            ns_config:set(autocompaction, ACSettings),

            MaybePurgeInterval =
                case PurgeIntervalRV of
                    [{ok, purge_interval, PurgeInterval}] ->
                        case compaction_api:get_purge_interval(global) =:= PurgeInterval of
                            true ->
                                ok;
                            false ->
                                compaction_api:set_global_purge_interval(PurgeInterval)
                        end,
                        [{purge_interval, PurgeInterval}];
                    [] ->
                        []
                end,

            index_compaction_settings(MaybeIndex),
            ns_audit:modify_compaction_settings(Req, ACSettings ++ MaybePurgeInterval
                                                ++ MaybeIndex),
            reply_json(Req, [], 200)
    end.

index_compaction_settings(Settings) ->
    case proplists:get_value(index_fragmentation_percentage, Settings) of
        undefined ->
            ok;
        Val ->
            index_settings_manager:update(compaction, [{fragmentation, Val}])
    end,
    SetList = [{index_circular_compaction_days, daysOfWeek},
               {index_circular_compaction_abort, abort_outside},
               {index_circular_compaction_interval, interval}],
    Set = lists:filtermap(
            fun ({K, NewK}) ->
                    case proplists:get_value(K, Settings) of
                        undefined ->
                            false;
                        V ->
                            {true, {NewK, V}}
                    end
            end, SetList),
    case Set of
        [] ->
            ok;
        _ ->
            index_settings_manager:update(circularCompaction, Set)
    end,
    case proplists:get_value(index_compaction_mode, Settings) of
        undefined ->
            ok;
        Mode ->
            index_settings_manager:update(compactionMode, Mode)
    end.

mk_number_field_validator_error_maker(JSONName, Msg, Args) ->
    [{error, JSONName, iolist_to_binary(io_lib:format(Msg, Args))}].

mk_number_field_validator(Min, Max, Params) ->
    mk_number_field_validator(Min, Max, Params, list_to_integer).

mk_number_field_validator(Min, Max, Params, ParseFn) ->
    fun ({JSONName, CfgName, HumanName}) ->
            case proplists:get_value(JSONName, Params) of
                undefined -> [];
                V ->
                    case parse_validate_number(V, Min, Max, ParseFn) of
                        {ok, IntV} -> [{ok, CfgName, IntV}];
                        invalid ->
                            Msg = case ParseFn of
                                      list_to_integer -> "~s must be an integer";
                                      _ -> "~s must be a number"
                                  end,
                            mk_number_field_validator_error_maker(JSONName, Msg, [HumanName]);
                        too_small ->
                            mk_number_field_validator_error_maker(
                              JSONName, "~s is too small. Allowed range is ~p - ~p",
                              [HumanName, Min, Max]);
                        too_large ->
                            mk_number_field_validator_error_maker(
                              JSONName, "~s is too large. Allowed range is ~p - ~p",
                              [HumanName, Min, Max])
                    end
            end
    end.

mk_string_field_validator(AV, Params) ->
    fun ({JSONName, CfgName, HumanName}) ->
            case proplists:get_value(JSONName, Params) of
                undefined ->
                    [];
                Val ->
                    %% Is Val one of the acceptable ones?
                    %% Some settings are list of strings
                    %% E.g. daysOfWeek can be "Sunday, Thursday"
                    %% Need to validate each token.
                    Tokens = string:tokens(Val, ","),
                    case lists:any(fun (V) -> not lists:member(V, AV) end, Tokens) of
                        true ->
                            [{error, JSONName,
                              iolist_to_binary(io_lib:format("~s must be one of ~p",
                                                             [HumanName, AV]))}];
                        false ->
                            [{ok, CfgName, list_to_binary(Val)}]
                    end
            end
    end.

parse_and_validate_time_interval(JSONName, Params) ->
    FromH = JSONName ++ "[fromHour]",
    FromM = JSONName ++ "[fromMinute]",
    ToH = JSONName ++ "[toHour]",
    ToM = JSONName ++ "[toMinute]",
    Abort = JSONName ++ "[abortOutside]",

    Hours = [{FromH, from_hour, "from hour"}, {ToH, to_hour, "to hour"}],
    Mins = [{FromM, from_minute, "from minute"}, {ToM, to_minute, "to minute"}],

    Res0 = lists:flatmap(mk_number_field_validator(0, 23, Params), Hours)
        ++ lists:flatmap(mk_number_field_validator(0, 59, Params), Mins)
        ++ parse_validate_boolean_field(Abort, abort_outside, Params),

    Err = lists:filter(fun ({error, _, _}) -> true; (_) -> false end, Res0),
    %% If validation failed for any field then return error.
    case Err of
        [] ->
            Res0;
        _ ->
            Err
    end.

parse_and_validate_extra_index_settings(Params) ->
    CModeValidator = mk_string_field_validator(["circular", "full"], Params),
    RV0 = CModeValidator({"indexCompactionMode", index_compaction_mode,
                          "index compaction mode"}),

    DaysList = misc:get_days_list(),
    DaysValidator = mk_string_field_validator(DaysList, Params),
    RV1 = DaysValidator({"indexCircularCompaction[daysOfWeek]",
                         index_circular_compaction_days,
                         "index circular compaction days"}) ++ RV0,

    Time0 = parse_and_validate_time_interval("indexCircularCompaction[interval]",
                                            Params),
    TimeResults = case Time0 of
                      [] ->
                          Time0;
                      [{error, _, _}|_] ->
                          Time0;
                      _ ->
                          {_, {_, _, Abort}, Time1} = lists:keytake(abort_outside, 2, Time0),
                          [{ok, index_circular_compaction_abort, Abort},
                           {ok, index_circular_compaction_interval,
                            [{F, V} || {ok, F, V} <- Time1]}]
                  end,
    TimeResults ++ RV1.

parse_validate_purge_interval(Params, ephemeral) ->
    do_parse_validate_purge_interval(Params, 0.0007);
parse_validate_purge_interval(Params, _) ->
    do_parse_validate_purge_interval(Params, 0.04).

do_parse_validate_purge_interval(Params, LowerLimit) ->
    Fun = mk_number_field_validator(LowerLimit, 60, Params, list_to_float),
    case Fun({"purgeInterval", purge_interval, "metadata purge interval"}) of
        [{error, Field, Msg}]->
            [{error, iolist_to_binary(Field), Msg}];
        RV ->
            RV
    end.

parse_validate_settings(Params, ExpectIndex) ->
    PercResults = lists:flatmap(mk_number_field_validator(2, 100, Params),
                                [{"databaseFragmentationThreshold[percentage]",
                                  db_fragmentation_percentage,
                                  "database fragmentation"},
                                 {"viewFragmentationThreshold[percentage]",
                                  view_fragmentation_percentage,
                                  "view fragmentation"}]),
    SizeResults = lists:flatmap(mk_number_field_validator(1, infinity, Params),
                                [{"databaseFragmentationThreshold[size]",
                                  db_fragmentation_size,
                                  "database fragmentation size"},
                                 {"viewFragmentationThreshold[size]",
                                  view_fragmentation_size,
                                  "view fragmentation size"}]),

    IndexResults =
        case ExpectIndex of
            true ->
                PercValidator = mk_number_field_validator(2, 100, Params),
                RV0 = PercValidator({"indexFragmentationThreshold[percentage]",
                                     index_fragmentation_percentage,
                                     "index fragmentation"}),
                parse_and_validate_extra_index_settings(Params) ++ RV0;
            false ->
                []
        end,

    ParallelResult =
        case parse_validate_boolean_field(
               "parallelDBAndViewCompaction", parallel_db_and_view_compaction, Params) of
            [] ->
                [{error, "parallelDBAndViewCompaction", <<"parallelDBAndViewCompaction is missing">>}];
            X ->
                X
        end,
    PeriodTimeResults = parse_and_validate_time_interval("allowedTimePeriod",
                                                        Params),

    Errors0 = [{iolist_to_binary(Field), Msg} ||
                  {error, Field, Msg} <- lists:append([PercResults, ParallelResult, PeriodTimeResults,
                                                       SizeResults, IndexResults])],
    BadFields = lists:sort(["databaseFragmentationThreshold",
                            "viewFragmentationThreshold"]),
    Errors = case ordsets:intersection(lists:sort(proplists:get_keys(Params)),
                                       BadFields) of
                 [] ->
                     Errors0;
                 ActualBadFields ->
                     Errors0 ++
                         [{<<"_">>,
                           iolist_to_binary([<<"Got unsupported fields: ">>,
                                             string:join(ActualBadFields, " and ")])}]
             end,
    case Errors of
        [] ->
            SizePList = [{F, V} || {ok, F, V} <- SizeResults],
            PercPList = [{F, V} || {ok, F, V} <- PercResults],
            MainFields =
                [{F, V} || {ok, F, V} <- ParallelResult]
                ++
                [{database_fragmentation_threshold, {
                    proplists:get_value(db_fragmentation_percentage, PercPList),
                    proplists:get_value(db_fragmentation_size, SizePList)}},
                 {view_fragmentation_threshold, {
                    proplists:get_value(view_fragmentation_percentage, PercPList),
                    proplists:get_value(view_fragmentation_size, SizePList)}}],

            AllFields =
                case PeriodTimeResults of
                    [] ->
                        MainFields;
                    _ -> [{allowed_time_period, [{F, V} || {ok, F, V} <- PeriodTimeResults]}
                          | MainFields]
                end,
            MaybeIndexResults = [{F, V} || {ok, F, V} <- IndexResults],
            {ok, AllFields, MaybeIndexResults};
        _ ->
            {errors, Errors}
    end.


-ifdef(TEST).
basic_parse_validate_settings_test() ->
    %% TODO: Need to mock cluster_compat_mode:is_cluster_***
    {ok, Stuff0, []} =
        parse_validate_settings([{"databaseFragmentationThreshold[percentage]", "10"},
                                 {"viewFragmentationThreshold[percentage]", "20"},
                                 {"indexFragmentationThreshold[size]", "42"},
                                 {"indexFragmentationThreshold[percentage]", "43"},
                                 {"parallelDBAndViewCompaction", "false"},
                                 {"allowedTimePeriod[fromHour]", "0"},
                                 {"allowedTimePeriod[fromMinute]", "1"},
                                 {"allowedTimePeriod[toHour]", "2"},
                                 {"allowedTimePeriod[toMinute]", "3"},
                                 {"allowedTimePeriod[abortOutside]", "false"}], false),
    Stuff1 = lists:sort(Stuff0),
    ?assertEqual([{allowed_time_period, [{from_hour, 0},
                                         {to_hour, 2},
                                         {from_minute, 1},
                                         {to_minute, 3},
                                         {abort_outside, false}]},
                  {database_fragmentation_threshold, {10, undefined}},
                  {parallel_db_and_view_compaction, false},
                  {view_fragmentation_threshold, {20, undefined}}],
                 Stuff1),
    ok.

extra_field_parse_validate_settings_test() ->
    {errors, Stuff0} =
        parse_validate_settings([{"databaseFragmentationThreshold", "10"},
                                 {"viewFragmentationThreshold", "20"},
                                 {"parallelDBAndViewCompaction", "false"},
                                 {"allowedTimePeriod[fromHour]", "0"},
                                 {"allowedTimePeriod[fromMinute]", "1"},
                                 {"allowedTimePeriod[toHour]", "2"},
                                 {"allowedTimePeriod[toMinute]", "3"},
                                 {"allowedTimePeriod[abortOutside]", "false"}],
                                false),
    ?assertEqual(
       [{<<"_">>,
         <<"Got unsupported fields: databaseFragmentationThreshold and viewFragmentationThreshold">>}],
       Stuff0),

    {errors, Stuff1} =
        parse_validate_settings([{"databaseFragmentationThreshold", "10"},
                                 {"parallelDBAndViewCompaction", "false"},
                                 {"allowedTimePeriod[fromHour]", "0"},
                                 {"allowedTimePeriod[fromMinute]", "1"},
                                 {"allowedTimePeriod[toHour]", "2"},
                                 {"allowedTimePeriod[toMinute]", "3"},
                                 {"allowedTimePeriod[abortOutside]", "false"}],
                                false),
    ?assertEqual([{<<"_">>, <<"Got unsupported fields: databaseFragmentationThreshold">>}],
                 Stuff1),
    ok.
-endif.
