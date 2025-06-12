%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(index_settings_manager).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behavior(json_settings_manager).

-export([start_link/0,
         get/1,
         get_from_config/3,
         update/2,
         update_txn/1,
         config_default/0,
         is_memory_optimized/1]).

-export([cfg_key/0,
         is_enabled/0,
         known_settings/0,
         on_update/2,
         config_upgrade_to_76/1,
         config_upgrade_to_morpheus/1]).

-import(json_settings_manager,
        [id_lens/1, allow_missing_lens/1]).

-define(INDEX_CONFIG_KEY, {metakv, <<"/indexing/settings/config">>}).
-define(SHARD_AFFINITY_SECRET_KEY,
        <<"/indexing/settings/config/features/ShardAffinity">>).
-define(SHARD_AFFINITY_OBJ_KEY, <<"indexer.default.enable_shard_affinity">>).

start_link() ->
    json_settings_manager:start_link(?MODULE).

get(Key) ->
    json_settings_manager:get(?MODULE, Key, undefined).

get_from_config(Config, Key, Default) ->
    json_settings_manager:get_from_config(?MODULE, Config, Key, Default).

cfg_key() ->
    ?INDEX_CONFIG_KEY.

is_enabled() ->
    true.

on_update(Key, Value) ->
    gen_event:notify(index_events, {index_settings_change, Key, Value}).

update(Key, Value) ->
    json_settings_manager:update(?MODULE, [{Key, Value}]).

update_txn(Props) ->
    json_settings_manager:update_txn(?MODULE, Props).

-spec is_memory_optimized(any()) -> boolean().
is_memory_optimized(?INDEX_STORAGE_MODE_MEMORY_OPTIMIZED) ->
    true;
is_memory_optimized(_) ->
    false.

default_settings() ->
    DaysOfWeek = misc:get_days_list(),
    CircDefaults = [{daysOfWeek, list_to_binary(string:join(DaysOfWeek, ","))},
                    {abort_outside, false},
                    {interval, compaction_interval_default()}],

    [{memoryQuota, 512},
     {generalSettings,
      general_settings_defaults(?MIN_SUPPORTED_VERSION)},
     {compaction, compaction_defaults()},
     {storageMode, <<"">>},
     {compactionMode, <<"circular">>},
     {circularCompaction, CircDefaults},
     {guardrails, guardrail_defaults()}].

known_settings() ->
    %% Currently chronicle and ns_config upgrades are
    %% non-atomic and the cluster compat version resides
    %% in chronicle. So it's possible for a node to observe an intermediate
    %% state where chronicle is upgraded while ns_config is not. So in those
    %% few places that rely on ns_config upgrades,
    %% cluster_compat_mode:get_ns_config_compat_version() must be used.
    ClusterVersion = cluster_compat_mode:get_ns_config_compat_version(),
    known_settings(ClusterVersion).

known_settings(ClusterVersion) ->
    [{memoryQuota, memory_quota_lens()},
     {generalSettings, general_settings_lens(ClusterVersion)},
     {compaction, compaction_lens()},
     {storageMode, id_lens(<<"indexer.settings.storage_mode">>)},
     {compactionMode,
      id_lens(<<"indexer.settings.compaction.compaction_mode">>)},
     {circularCompaction, circular_compaction_lens()},
     {guardrails, guardrails_lens()}].

%% settings manager populates settings per version. For each online upgrade,
%% it computes the delta between adjacent supported versions to update only the
%% settings that changed between the two.
%% Note that a node (running any version) is seeded with settings specified in
%% config_default(). If we specify settings(LATEST_VERSION) here, the node
%% contains settings as per LATEST_VERSION at start. A node with LATEST_VERSION
%% settings may be part of a cluster with compat_version v1 < latest_version. If
%% the version moves up from v1 to latest, config_upgrade_to_latest is called.
%% This will update settings that changed between v1 and latest (when the node
%% was already initialized with latest_version settings). So config_default()
%% must specify settings for the min supported version.
config_default() ->
    {?INDEX_CONFIG_KEY, json_settings_manager:build_settings_json(
                          default_settings(),
                          maps:new(),
                          known_settings(?MIN_SUPPORTED_VERSION))}.

memory_quota_lens() ->
    Key = <<"indexer.settings.memory_quota">>,

    Get = fun (Map) ->
                  maps:get(Key, Map) div ?MIB
          end,
    Set = fun (Value, Map) ->
                  maps:put(Key, Value * ?MIB, Map)
          end,
    {Get, Set}.

indexer_threads_lens() ->
    Key = <<"indexer.settings.max_cpu_percent">>,
    Get = fun (Map) ->
                  maps:get(Key, Map) div 100
          end,
    Set = fun (Value, Map) ->
                  maps:put(Key, Value * 100, Map)
          end,
    {Get, Set}.

general_settings_lens_props(ClusterVersion) ->
    [{redistributeIndexes,
      id_lens(<<"indexer.settings.rebalance.redistribute_indexes">>)},
     {numReplica,
      id_lens(<<"indexer.settings.num_replica">>)},
     {enablePageBloomFilter,
      id_lens(<<"indexer.settings.enable_page_bloom_filter">>)}] ++
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_76) of
         true ->
            [{memHighThreshold,
              id_lens(<<"indexer.settings.thresholds.mem_high">>)},
             {memLowThreshold,
              id_lens(<<"indexer.settings.thresholds.mem_low">>)},
             {unitsHighThreshold,
              id_lens(<<"indexer.settings.thresholds.units_high">>)},
             {unitsLowThreshold,
              id_lens(<<"indexer.settings.thresholds.units_low">>)},
             {blobStorageScheme,
              id_lens(<<"indexer.settings.rebalance.blob_storage_scheme">>)},
             {blobStorageBucket,
              id_lens(<<"indexer.settings.rebalance.blob_storage_bucket">>)},
             {blobStoragePrefix,
              id_lens(<<"indexer.settings.rebalance.blob_storage_prefix">>)},
             {blobStorageRegion,
              id_lens(<<"indexer.settings.rebalance.blob_storage_region">>)},
             {enableShardAffinity,
              id_lens(<<"indexer.settings.enable_shard_affinity">>)}];
        false ->
            []
    end ++
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_MORPHEUS) of
        true ->
            [{deferBuild,
              id_lens(<<"indexer.settings.defer_build">>)}];
        false ->
            []
    end ++
        [{indexerThreads,
          indexer_threads_lens()},
         {memorySnapshotInterval,
          id_lens(<<"indexer.settings.inmemory_snapshot.interval">>)},
         {stableSnapshotInterval,
          id_lens(<<"indexer.settings.persisted_snapshot.interval">>)},
         {maxRollbackPoints,
          id_lens(<<"indexer.settings.recovery.max_rollbacks">>)},
         {logLevel,
          id_lens(<<"indexer.settings.log_level">>)}].

default_rollback_points() ->
    case ns_config_default:init_is_enterprise() of
        true ->
            ?DEFAULT_MAX_ROLLBACK_PTS_PLASMA;
        false ->
            ?DEFAULT_MAX_ROLLBACK_PTS_FORESTDB
    end.

general_settings_defaults(ClusterVersion) ->
    [{redistributeIndexes, false},
     {numReplica, config_profile:get_value({indexer, num_replica}, 0)},
     {enablePageBloomFilter, false}] ++
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_76) of
        true ->
            [{memHighThreshold,
              config_profile:get_value({indexer, mem_high_threshold}, 70)},
             {memLowThreshold,
              config_profile:get_value({indexer, mem_low_threshold}, 50)},
             {unitsHighThreshold,
              config_profile:get_value({indexer, units_high_threshold}, 60)},
             {unitsLowThreshold,
              config_profile:get_value({indexer, units_low_threshold}, 40)},
             {blobStorageScheme, <<"">>},
             {blobStorageBucket, <<"">>},
             {blobStoragePrefix, <<"">>},
             {blobStorageRegion, <<"">>},
             {enableShardAffinity, default_shard_affinity()}];
        false ->
            []
    end ++
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_MORPHEUS) of
        true ->
            [{deferBuild, false}];
        false ->
            []
    end ++
        [{indexerThreads, 0},
         {memorySnapshotInterval, 200},
         {stableSnapshotInterval, 5000},
         {maxRollbackPoints, default_rollback_points()},
         {logLevel, <<"info">>}].

general_settings_lens(ClusterVersion) ->
    json_settings_manager:props_lens(general_settings_lens_props(ClusterVersion)).

compaction_interval_default() ->
    [{from_hour, 0},
     {to_hour, 0},
     {from_minute, 0},
     {to_minute, 0}].

compaction_interval_lens() ->
    Key = <<"indexer.settings.compaction.interval">>,
    Get = fun (Map) ->
                  Int0 = binary_to_list(maps:get(Key, Map)),
                  [From, To] = string:tokens(Int0, ","),
                  [FromH, FromM] = string:tokens(From, ":"),
                  [ToH, ToM] = string:tokens(To, ":"),
                  [{from_hour, list_to_integer(FromH)},
                   {from_minute, list_to_integer(FromM)},
                   {to_hour, list_to_integer(ToH)},
                   {to_minute, list_to_integer(ToM)}]
          end,
    Set = fun (Values0, Map) ->
                  Values =
                      case Values0 of
                          [] ->
                              compaction_interval_default();
                          _ ->
                              Values0
                      end,

                  {_, FromHour} = lists:keyfind(from_hour, 1, Values),
                  {_, ToHour} = lists:keyfind(to_hour, 1, Values),
                  {_, FromMinute} = lists:keyfind(from_minute, 1, Values),
                  {_, ToMinute} = lists:keyfind(to_minute, 1, Values),

                  Value = iolist_to_binary(
                            io_lib:format("~2.10.0b:~2.10.0b,~2.10.0b:~2.10.0b",
                                          [FromHour, FromMinute, ToHour, ToMinute])),

                  maps:put(Key, Value, Map)
          end,
    {Get, Set}.

circular_compaction_lens_props() ->
    [{daysOfWeek,
      id_lens(<<"indexer.settings.compaction.days_of_week">>)},
     {abort_outside,
      id_lens(<<"indexer.settings.compaction.abort_exceed_interval">>)},
     {interval, compaction_interval_lens()}].

circular_compaction_lens() ->
    json_settings_manager:props_lens(circular_compaction_lens_props()).

compaction_lens_props() ->
    [{fragmentation, id_lens(<<"indexer.settings.compaction.min_frag">>)},
     {interval, compaction_interval_lens()}].

compaction_defaults() ->
    [{fragmentation, 30},
     {interval, compaction_interval_default()}].

compaction_lens() ->
    json_settings_manager:props_lens(compaction_lens_props()).

guardrail_defaults() ->
    proplists:get_value(
      index,
      menelaus_web_guardrails:default_for_metakv()).

guardrails_lens() ->
    json_settings_manager:props_lens(
      lists:map(
        fun ({Key, Config}) ->
                Path = "indexer.settings.guardrails."
                    ++ atom_to_list(Key) ++ ".",

                %% Each field needs allow_missing_lens, since we are
                %% introducing this in a patch release, so we can't make cluster
                %% compat mode checks to avoid fetching the fields early
                Lenses =
                    lists:map(
                      fun({SubKey, _Value}) ->
                              {SubKey,
                               allow_missing_lens(
                                 iolist_to_binary([Path,
                                                   atom_to_list(SubKey)]))}
                      end, Config),
                {Key, json_settings_manager:props_lens(Lenses)}
        end, guardrail_defaults())).

config_upgrade_to_76(Config) ->
    config_upgrade_settings(Config, ?MIN_SUPPORTED_VERSION,
                            ?VERSION_76).

config_upgrade_to_morpheus(Config) ->
    config_upgrade_settings(Config, ?VERSION_76, ?VERSION_MORPHEUS).

-spec(default_shard_affinity() -> boolean()).
default_shard_affinity() ->
    not config_profile:get_bool({indexer, disable_shard_affinity}).

-spec(decode_shard_affinity_json_blob(binary()) -> boolean()).
decode_shard_affinity_json_blob(Blob) ->
    try ejson:decode(Blob) of
        {Json} ->
            case proplists:lookup(?SHARD_AFFINITY_OBJ_KEY, Json) of
                none ->
                    default_shard_affinity();
                {_, Bool} ->
                    %% Since we won't have validators on the "way in" for this
                    %% variable we must make sure to convert it to the actual
                    %% boolean atom because it may still be a string or a binary.
                    misc:convert_to_boolean(Bool)
            end;
        BadBlob ->
            %% you could technically decode json without the tuple structure
            %% around it (ex: true|false) and in that case we just want to
            %% fallback to default since the data was technically valid JSON,
            %% but incorrect.
            invalid_json_blob_result(
              lists:flatten(
                io_lib:format("Invalid JSON structure: '~p'", [BadBlob])))
    catch throw:{invalid_json, Err}:_ -> %% only catch json decoding errors
            invalid_json_blob_result(Err)
    end.

invalid_json_blob_result(Err) ->
    ?log_error("Error decoding shard-affinity JSON blob: ~p.", [Err]),
    default_shard_affinity().

config_upgrade_settings(Config, OldVersion, NewVersion) ->
    NewSettings =
        case NewVersion =:= ?VERSION_76 of
            true ->
                Current = general_settings_defaults(NewVersion),
                %% This section will check for a secret key in metakv and if
                %% found, use that to determine the current default. This is
                %% related to MB-58541 where we are allowing the use of a
                %% setting before the cluster is fully upgraded.
                PreviousOrDefault =
                    case metakv:ns_config_get(Config,
                                              ?SHARD_AFFINITY_SECRET_KEY) of
                        false ->
                            default_shard_affinity();
                        {value, Blob, _VC} ->
                            decode_shard_affinity_json_blob(Blob)
                    end,
                Updated =
                    misc:update_proplist(Current, [{enableShardAffinity,
                                                    PreviousOrDefault}]),
                Updated -- general_settings_defaults(OldVersion);
            false ->
                general_settings_defaults(NewVersion) --
                    general_settings_defaults(OldVersion)
        end,

    json_settings_manager:upgrade_existing_key(
      ?MODULE, Config, [{generalSettings, NewSettings}],
      known_settings(NewVersion)).

-ifdef(TEST).

-define(SHARD_AFFINITY_JSON_BLOB(__TRUE_FALSE),
        io_lib:format("{\"indexer.default.enable_shard_affinity\": ~p}",
                      [__TRUE_FALSE])).
default_test() ->
    config_profile:load_default_profile_for_test(),
    Versions = [?MIN_SUPPORTED_VERSION, ?VERSION_76, ?VERSION_MORPHEUS],
    lists:foreach(fun(V) -> default_versioned(V) end, Versions),
    config_profile:unload_profile_for_test().

default_versioned(Version) ->
    Keys = fun (L) -> lists:sort([K || {K, _} <- L]) end,

    ?assertEqual(Keys(known_settings(Version)),
                 Keys(default_settings())),
    ?assertEqual(Keys(compaction_lens_props()), Keys(compaction_defaults())),
    ?assertEqual(Keys(general_settings_lens_props(Version)),
                 Keys(general_settings_defaults(Version))).

evaluate_with_profile(Profile, TestFun) ->
    meck:new(config_profile, [passthrough]),
    mock_config_profile(Profile),
    TestFun(),
    meck:unload(config_profile).

mock_config_profile(default) ->
    meck:expect(config_profile, get,
                fun () -> [{name, ?DEFAULT_PROFILE_STR},
                           {{indexer, disable_shard_affinity}, true}] end);
mock_config_profile(serverless) ->
    meck:expect(config_profile, get,
                fun () -> [{name, ?SERVERLESS_PROFILE_STR}] end);
mock_config_profile(provisioned) ->
    meck:expect(config_profile, get,
                fun () -> [{name, ?PROVISIONED_PROFILE_STR}] end).

config_upgrade_test_() ->
    TestFun =
        fun (Profile, Expected) ->
                ?cut(evaluate_with_profile(
                       Profile,
                       fun () ->
                               config_upgrade_test_generic(
                                 #config{static = [[], []],
                                         dynamic = [[], []]}, Expected)

                       end))
        end,
    {foreach, fun () -> ok end,
     [{"profile: default", TestFun(default, false)},
      {"profile: serverless", TestFun(serverless, true)},
      {"profile: provisioned", TestFun(provisioned, true)}]}.

config_upgrade_special_metakv_key_test_() ->
    TestFunMetakv =
        fun (Profile, Expected) ->
                ?cut(evaluate_with_profile(
                       Profile,
                       fun () ->
                               Metakv =
                                   {{metakv, ?SHARD_AFFINITY_SECRET_KEY},
                                    ?SHARD_AFFINITY_JSON_BLOB(true)},
                               Config = #config{static = [[], []],
                                                dynamic = [[Metakv], []]},
                               config_upgrade_test_generic(Config, Expected)
                       end))
        end,

    {foreach, fun () -> ok end,
     [{"profile: default, value: true", TestFunMetakv(default, true)},
      {"profile: serverless, value: true", TestFunMetakv(serverless, true)},
      {"profile: provisioned, value: true", TestFunMetakv(provisioned, true)}]}.

config_upgrade_special_metakv_key_false_test_() ->
    TestFunMetakv =
        fun (Profile, Expected) ->
                ?cut(evaluate_with_profile(
                       Profile,
                       fun () ->
                               Metakv =
                                   {{metakv, ?SHARD_AFFINITY_SECRET_KEY},
                                    ?SHARD_AFFINITY_JSON_BLOB(false)},
                               Config = #config{static = [[], []],
                                                dynamic = [[Metakv], []]},
                               config_upgrade_test_generic(Config, Expected)
                       end))
        end,

    {foreach, fun () -> ok end,
     [{"profile: default, value: false", TestFunMetakv(default, false)},
      {"profile: serverless, value: false", TestFunMetakv(serverless, false)},
      {"profile: provisioned, value: false", TestFunMetakv(provisioned, false)}]}.


config_upgrade_test_generic(Config, ShardAffinityValue) ->
    CmdList = config_upgrade_to_76(Config),
    [{set, {metakv, Meta}, Data}] = CmdList,
    ?assertEqual(<<"/indexing/settings/config">>, Meta),
    Result =
        io_lib:format("{\"indexer.settings.enable_shard_affinity\":~p,"
                      "\"indexer.settings.rebalance.blob_storage_bucket\":\"\","
                      "\"indexer.settings.rebalance.blob_storage_prefix\":\"\","
                      "\"indexer.settings.rebalance.blob_storage_region\":\"\","
                      "\"indexer.settings.rebalance.blob_storage_scheme\":\"\","
                      "\"indexer.settings.thresholds.mem_high\":70,"
                      "\"indexer.settings.thresholds.mem_low\":50,"
                      "\"indexer.settings.thresholds.units_high\":60,"
                      "\"indexer.settings.thresholds.units_low\":40}",
                      [ShardAffinityValue]),
    ?assertEqual(list_to_binary(Result), Data),

    CmdList2 = config_upgrade_to_morpheus(Config),
    [{set, {metakv, Meta2}, Data2}] = CmdList2,
    ?assertEqual(<<"/indexing/settings/config">>, Meta2),
    ?assertEqual(<<"{\"indexer.settings.defer_build\":false}">>,
                 Data2).

enable_shard_affinity_76_test() ->
    evaluate_with_profile(
      default,
      fun () ->
              ?assert(
                 proplists:is_defined(
                   enableShardAffinity,
                   general_settings_defaults(?VERSION_76))),
              ?assert(
                 not proplists:is_defined(
                       enableShardAffinity,
                       general_settings_defaults(?VERSION_72)))
      end).

shard_affinity_blob_test() ->
    ResultTrue =
        decode_shard_affinity_json_blob(?SHARD_AFFINITY_JSON_BLOB(true)),
    ?assertEqual(ResultTrue, true),
    ResultFalse =
        decode_shard_affinity_json_blob(?SHARD_AFFINITY_JSON_BLOB(false)),
    ?assertEqual(ResultFalse, false).

shard_affinity_bad_blob_test() ->
    eval_shard_affinity_bad_blob(default, false).

shard_affinity_bad_blob_provisioned_test() ->
    eval_shard_affinity_bad_blob(provisioned, true).

shard_affinity_bad_blob_serverless_test() ->
    eval_shard_affinity_bad_blob(serverless, true).

eval_shard_affinity_bad_blob(Profile, Default) ->
    evaluate_with_profile(
      Profile,
      fun () ->
              BadBlob = "{\"indexer.default.enable_shard_affinity\": True}",
              Result = decode_shard_affinity_json_blob(BadBlob),
              ?assertEqual(Result, default_shard_affinity()),

              BadBlob2 = "{\"indexer.enable_shard_affinity\": true}",
              Result2 = decode_shard_affinity_json_blob(BadBlob2),
              ?assertEqual(Result2, default_shard_affinity()),

              BadBlob3 = "{\"key\": []}",
              Result3 = decode_shard_affinity_json_blob(BadBlob3),
              ?assertEqual(Result3, default_shard_affinity()),

              GoodBlob = "{\"indexer.default.enable_shard_affinity\": false}",
              Result4 = decode_shard_affinity_json_blob(GoodBlob),
              ?assertEqual(Result4, false),

              OnlyFalseBlob = "false",
              Result5 = decode_shard_affinity_json_blob(OnlyFalseBlob),
              ?assertEqual(Result5, Default),

              OnlyTrueBlob = "true",
              Result6 = decode_shard_affinity_json_blob(OnlyTrueBlob),
              ?assertEqual(Result6, Default)
      end).

-endif.
