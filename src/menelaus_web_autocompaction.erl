%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%% @doc implementation of aultautocompaction REST API's

-module(menelaus_web_autocompaction).

-include("ns_common.hrl").
-include("ns_bucket.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_get_global_settings/1,
         handle_set_global_settings/1,
         build_bucket_settings/2,
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
    reply_json(Req, {JSON}, 200).

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
         {indexCircularCompaction, {Circ}},
         {indexFragmentationThreshold,
          {[{percentage, Fragmentation}]}}],

    case compaction_daemon:get_autocompaction_settings(Config) of
        [] ->
            do_build_settings([], IndexSettings, global);
        ACSettings ->
            do_build_settings(ACSettings, IndexSettings, global)
    end.

build_bucket_settings(Settings, BackendStorage) ->
    do_build_settings(Settings, [], BackendStorage).

do_build_settings(Settings, _Extra, magma) ->
    Default = compaction_daemon:global_magma_frag_percent(),
    {build_magma_fragmention_percentage(Settings, Default)};
%% Build global or non-magma bucket settings
do_build_settings(Settings, Extra, BackendStorageOrGlobal) ->
    PropFun = fun ({JSONName, CfgName}) ->
                      case proplists:get_value(CfgName, Settings) of
                          undefined -> [];
                          {Percentage, Size} ->
                              [{JSONName, {[{percentage, Percentage},
                                            {size, Size}]}}]
                      end
              end,
    DBAndView = lists:flatmap(PropFun,
                              [{databaseFragmentationThreshold, database_fragmentation_threshold},
                               {viewFragmentationThreshold, view_fragmentation_threshold}]),

    MagmaFragPercent =
        case BackendStorageOrGlobal of
            global ->
                build_magma_fragmention_percentage(Settings,
                                                   ?MAGMA_FRAG_PERCENTAGE);
            _ -> []
        end,

    {[{parallelDBAndViewCompaction, proplists:get_bool(parallel_db_and_view_compaction,
                                                       Settings)}
              | case proplists:get_value(allowed_time_period, Settings) of
                    undefined -> [];
                    V -> [{allowedTimePeriod, build_allowed_time_period(V)}]
                end] ++ MagmaFragPercent ++ DBAndView ++ Extra}.

build_magma_fragmention_percentage(Settings, Default) ->
    Pct = proplists:get_value(magma_fragmentation_percentage, Settings,
                              Default),
    [{magmaFragmentationPercentage, Pct}].

build_allowed_time_period(AllowedTimePeriod) ->
    {[{JSONName, proplists:get_value(CfgName, AllowedTimePeriod)}
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
            reply_json(Req, {[{errors, {Errors}}]}, 400);
        {_, _, [{error, Field, Msg}]} ->
            reply_json(Req, {[{errors, {[{Field, Msg}]}}]}, 400);
        {true, {ok, _ACSettings, _}, _} ->
            reply_json(Req, {[{errors, {[]}}]}, 200);
        {false, {ok, ACSettings, MaybeIndex}, _} ->
            compaction_daemon:set_autocompaction_settings(ACSettings),

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
                "undefined" ->
                    [{ok, CfgName, undefined}];
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

compare_from_and_to_time_validator([], _) -> [];
compare_from_and_to_time_validator([{_, _, IntFromH}, {_, _, IntToH},
                                    {_, _, IntFromM}, {_, _, IntToM}, _] = Res1, JSONName) ->
    case IntFromH =:= IntToH andalso IntFromM =:= IntToM of
        true ->
            Msg = "Start time must not be the same as end time",
            [{error, JSONName, iolist_to_binary(Msg)}];
        _ ->
            Res1
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
    Res1 = case length(Res0) of
               0 ->
                   Res0;
               5 ->
                   Res0;
               _ ->
                   Msg = "Must specify all of the following: fromHour, "
                         "fromMinute, toHour, toMinute, abortOutside",
                   Res0 ++ [{error, JSONName, iolist_to_binary(Msg)}]
           end,

    Err = lists:filter(fun ({error, _, _}) -> true; (_) -> false end, Res1),
    %% If validation failed for any field then return error.
    case Err of
        [] ->
            case JSONName of
                "allowedTimePeriod" ->
                    compare_from_and_to_time_validator(Res1, JSONName);
                _ -> Res1
            end;
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

parse_validate_magma_fragmentation_percentage(Params) ->
    case cluster_compat_mode:is_cluster_71() of
        true ->
            Fun = mk_number_field_validator(10, 100, Params),
            case Fun({"magmaFragmentationPercentage", magma_fragmentation_percentage,
                      "magma fragmentation percentage"}) of
                [{error, Field, Msg}]->
                    [{error, iolist_to_binary(Field), Msg}];
                RV ->
                    case RV of
                        [{ok, magma_fragmentation_percentage, FragPercent}] ->
                            [{magma_fragmentation_percentage, FragPercent}];
                        [] ->
                            []
                    end
            end;
        false ->
            case proplists:get_value("magmaFragmentationPercentage", Params) of
                undefined ->
                    [];
                _ ->
                    [{error, "magmaFragmentationPercentage",
                      <<"Magma Fragmentation Percentage is not allowed until "
                        "entire cluster is upgraded to 7.1">>}]
            end
    end.

parse_validate_settings(Params, ExpectIndex) ->
    case proplists:get_value("storageBackend", Params) of
        "magma" ->
            do_parse_validate_settings(Params, ExpectIndex, magma);
        _ ->
            do_parse_validate_settings(Params, ExpectIndex, not_magma)
    end.

do_parse_validate_settings(Params, _ExpectIndex, magma) ->
    MagmaFragResults = parse_validate_magma_fragmentation_percentage(Params),
    Errors = [{iolist_to_binary(Field), Msg} ||
              {error, Field, Msg} <- lists:append([MagmaFragResults])],
    case Errors of
        [] ->
            {ok, MagmaFragResults, []};
        _ ->
            {errors, Errors}
    end;
%% Non-magma bucket or global settings
do_parse_validate_settings(Params, ExpectIndex, not_magma) ->
    GlobalSettings =
        compaction_daemon:get_autocompaction_settings(ns_config:latest()),
    {GDBFragPct, GDBFragSz} =
        proplists:get_value(database_fragmentation_threshold, GlobalSettings),
    {GViewFragPct, GViewFragSz} =
        proplists:get_value(view_fragmentation_threshold, GlobalSettings),

    PercValidator = mk_number_field_validator(2, 100, Params),
    SizeValidator = mk_number_field_validator(1, infinity, Params),

    ValidatorFun =
        fun (Validator, {_, Key, _} = ValidatorParams, Default) ->
                case Validator(ValidatorParams) of
                    [] ->
                        [{ok, Key, Default}];
                    ValueOrError ->
                        ValueOrError
                end
        end,

    DBFragPct = ValidatorFun(PercValidator,
                             {"databaseFragmentationThreshold[percentage]",
                              db_fragmentation_percentage,
                              "database fragmentation"}, GDBFragPct),
    ViewFragPct = ValidatorFun(PercValidator,
                               {"viewFragmentationThreshold[percentage]",
                                view_fragmentation_percentage,
                                "view fragmentation"}, GViewFragPct),
    PercResults = DBFragPct ++ ViewFragPct,

    DbFragSz = ValidatorFun(SizeValidator,
                            {"databaseFragmentationThreshold[size]",
                             db_fragmentation_size,
                             "database fragmentation size"}, GDBFragSz),
    ViewFragSz = ValidatorFun(SizeValidator,
                              {"viewFragmentationThreshold[size]",
                               view_fragmentation_size,
                               "view fragmentation size"}, GViewFragSz),
    SizeResults = DbFragSz ++ ViewFragSz,

    IndexResults =
        case ExpectIndex of
            true ->
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
    MagmaFragResults = parse_validate_magma_fragmentation_percentage(Params),

    Errors0 = [{iolist_to_binary(Field), Msg} ||
                  {error, Field, Msg} <- lists:append([PercResults,
                                                       ParallelResult,
                                                       PeriodTimeResults,
                                                       SizeResults,
                                                       IndexResults,
                                                       MagmaFragResults])],
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
                MagmaFragResults ++
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
setup_meck() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_cluster_71,
                fun () -> true end),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, search,
                fun (_, _) -> {value,
                               [{database_fragmentation_threshold,
                                 {30, undefined}},
                                {view_fragmentation_threshold,
                                 {30, undefined}}]}
                end),
    meck:new(chronicle_kv, [passthrough]),
    meck:expect(chronicle_kv, get,
                fun (_, _) ->
                        {ok,
                         {[{database_fragmentation_threshold,
                            {global_database_fragmentation_percentage,
                             undefined}},
                           {view_fragmentation_threshold,
                            {global_view_fragmentation_percentage, undefined}},
                           {magma_fragmentation_percentage,
                            global_magma_fragmentation_percentage}],
                          {<<"f663189bff34bd2523ee5ff25480d845">>, 4}}}
                end).

teardown_meck() ->
    meck:unload(cluster_compat_mode),
    meck:unload(ns_config),
    meck:unload(chronicle_kv).

basic_parse_validate_settings_test() ->
    setup_meck(),
    Settings = [{"databaseFragmentationThreshold[percentage]", "10"},
                {"viewFragmentationThreshold[percentage]", "20"},
                {"indexFragmentationThreshold[size]", "42"},
                {"indexFragmentationThreshold[percentage]", "43"},
                {"magmaFragmentationPercentage", "51"},
                {"parallelDBAndViewCompaction", "false"},
                {"allowedTimePeriod[fromHour]", "0"},
                {"allowedTimePeriod[fromMinute]", "1"},
                {"allowedTimePeriod[toHour]", "2"},
                {"allowedTimePeriod[toMinute]", "3"},
                {"allowedTimePeriod[abortOutside]", "false"}],

    Expected = [{allowed_time_period, [{from_hour, 0},
                                       {to_hour, 2},
                                       {from_minute, 1},
                                       {to_minute, 3},
                                       {abort_outside, false}]},
                {database_fragmentation_threshold, {10, undefined}},
                {magma_fragmentation_percentage, 51},
                {parallel_db_and_view_compaction, false},
                {view_fragmentation_threshold, {20, undefined}}],

    {ok, Stuff0, []} = parse_validate_settings(Settings, false),
    Stuff1 = lists:sort(Stuff0),
    ?assertEqual(Expected, Stuff1),

    meck:expect(cluster_compat_mode, is_cluster_71,
                fun () -> false end),
    Stuff2 = parse_validate_settings(Settings, false),
    ?assertEqual(
       {errors,[{<<"magmaFragmentationPercentage">>,
         <<"Magma Fragmentation Percentage is not allowed until "
           "entire cluster is upgraded to 7.1">>}]},
       Stuff2),

    %% Show that magmaFragmentation isn't required
    Settings3 = lists:keydelete("magmaFragmentationPercentage", 1, Settings),
    Expected3 = lists:keydelete(magma_fragmentation_percentage, 1, Expected),
    {ok, Stuff3, []} = parse_validate_settings(Settings3, false),
    ?assertEqual(lists:sort(Expected3), lists:sort(Stuff3)),

    teardown_meck(),
    ok.

extra_field_parse_validate_settings_test() ->
    setup_meck(),
    {errors, Stuff0} =
        parse_validate_settings([{"databaseFragmentationThreshold", "10"},
                                 {"viewFragmentationThreshold", "20"},
                                 {"magmaFragmentationPercentage", "77"},
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
    teardown_meck(),
    ok.

compare_from_and_to_time_validator_test() ->
    setup_meck(),
    {errors, Stuff0} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"allowedTimePeriod[fromHour]", "1"},
                                 {"allowedTimePeriod[fromMinute]", "2"},
                                 {"allowedTimePeriod[toHour]", "1"},
                                 {"allowedTimePeriod[toMinute]", "2"},
                                 {"allowedTimePeriod[abortOutside]", "false"}],
                                false),
    ?assertEqual([{<<"allowedTimePeriod">>,
                   <<"Start time must not be the same as end time">>}],
                 Stuff0),
    teardown_meck(),
    ok.

incomplete_settings_test() ->
    setup_meck(),
    {errors, Stuff0} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"allowedTimePeriod[fromHour]", "0"}],
                                false),
    ?assertEqual([{<<"allowedTimePeriod">>,
                   <<"Must specify all of the following: fromHour, fromMinute, "
                     "toHour, toMinute, abortOutside">>}],
                 Stuff0),

    {errors, Stuff1} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"indexCircularCompaction[interval][toMinute]",
                                  "22"}],
                                true),
    ?assertEqual([{<<"indexCircularCompaction[interval]">>,
                   <<"Must specify all of the following: fromHour, fromMinute, "
                     "toHour, toMinute, abortOutside">>}],
                 Stuff1),
    teardown_meck(),
    ok.

use_global_default_test() ->
    setup_meck(),
    %% Setting database percentage only; view should use global value
    {ok, Result0, []} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"databaseFragmentationThreshold[percentage]",
                                  "19"}],
                                false),
    Expected0 =
        [{parallel_db_and_view_compaction,false},
         {database_fragmentation_threshold,{19,undefined}},
         {view_fragmentation_threshold,
          {global_view_fragmentation_percentage,undefined}}],
    ?assertEqual(Expected0, Result0),

    %% Setting view percentage only; database should use global value
    {ok, Result1, []} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"viewFragmentationThreshold[percentage]",
                                  "25"}],
                                false),
    Expected1 =
        [{parallel_db_and_view_compaction,false},
         {database_fragmentation_threshold,
          {global_database_fragmentation_percentage,undefined}},
         {view_fragmentation_threshold,{25,undefined}}],
    ?assertEqual(Expected1, Result1),

    %% Setting database size only; view should use global value
    {ok, Result2, []} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"databaseFragmentationThreshold[size]",
                                  "12345"}],
                                false),
    Expected2 =
        [{parallel_db_and_view_compaction,false},
         {database_fragmentation_threshold,
          {global_database_fragmentation_percentage,12345}},
         {view_fragmentation_threshold,
          {global_view_fragmentation_percentage,undefined}}],
    ?assertEqual(Expected2, Result2),

    %% Setting view size only; database should use global value
    {ok, Result3, []} =
        parse_validate_settings([{"parallelDBAndViewCompaction", "false"},
                                 {"viewFragmentationThreshold[size]",
                                  "77777"}],
                                false),
    Expected3 =
        [{parallel_db_and_view_compaction,false},
         {database_fragmentation_threshold,
          {global_database_fragmentation_percentage,undefined}},
         {view_fragmentation_threshold,
          {global_view_fragmentation_percentage,77777}}],
    ?assertEqual(Expected3, Result3),
    teardown_meck(),
    ok.

reset_fragmentation_size_test() ->
    setup_meck(),

    %% Setting fragmention size to "undefined" resets the value to undefined.
    Settings = [{"databaseFragmentationThreshold[size]", "undefined"},
                {"viewFragmentationThreshold[size]", "undefined"},
                {"parallelDBAndViewCompaction", "false"}],

    Expected = [{parallel_db_and_view_compaction,false},
                {database_fragmentation_threshold,
                 {global_database_fragmentation_percentage, undefined}},
                {view_fragmentation_threshold,
                 {global_view_fragmentation_percentage, undefined}}],

    {ok, Stuff, []} = parse_validate_settings(Settings, false),
    ?assertEqual(Expected, Stuff),

    %% Setting fragmentation percentage to "undefined" resets the value
    %% to undefined.
    Settings2 = Settings ++ [{"databaseFragmentationThreshold[percentage]",
                              "undefined"},
                             {"viewFragmentationThreshold[percentage]",
                              "undefined"}],
    Expected2 = [{parallel_db_and_view_compaction,false},
                 {database_fragmentation_threshold, {undefined, undefined}},
                 {view_fragmentation_threshold, {undefined, undefined}}],
    {ok, Stuff2, []} = parse_validate_settings(Settings2, false),
    ?assertEqual(Expected2, Stuff2),

    teardown_meck(),
    ok.

-endif.
