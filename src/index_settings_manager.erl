%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(index_settings_manager).

-include("ns_common.hrl").

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
         config_upgrade_to_70/1,
         config_upgrade_to_NEO/1]).

-import(json_settings_manager,
        [id_lens/1]).

-define(INDEX_CONFIG_KEY, {metakv, <<"/indexing/settings/config">>}).

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
     {generalSettings, general_settings_defaults(?LATEST_VERSION_NUM)},
     {compaction, compaction_defaults()},
     {storageMode, <<"">>},
     {compactionMode, <<"circular">>},
     {circularCompaction, CircDefaults}].

known_settings() ->
    %% Currently chronicle and ns_config upgrades are
    %% non-atomic. Additionally, since 7.1 the cluster compat version resides
    %% in chronicle. So it's possible for a node to observe an intermediate
    %% state where chronicle is upgraded while ns_config is not. So in those
    %% few places that rely on ns_config upgrades from pre-7.1 to 7.1,
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
     {circularCompaction, circular_compaction_lens()}].

config_default() ->
    {?INDEX_CONFIG_KEY, json_settings_manager:build_settings_json(
                          default_settings(),
                          dict:new(), known_settings(?LATEST_VERSION_NUM))}.

memory_quota_lens() ->
    Key = <<"indexer.settings.memory_quota">>,

    Get = fun (Dict) ->
                  dict:fetch(Key, Dict) div ?MIB
          end,
    Set = fun (Value, Dict) ->
                  dict:store(Key, Value * ?MIB, Dict)
          end,
    {Get, Set}.

indexer_threads_lens() ->
    Key = <<"indexer.settings.max_cpu_percent">>,
    Get = fun (Dict) ->
                  dict:fetch(Key, Dict) div 100
          end,
    Set = fun (Value, Dict) ->
                  dict:store(Key, Value * 100, Dict)
          end,
    {Get, Set}.

general_settings_lens_props(ClusterVersion) ->
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_70) of
        true ->
            [{numReplica,
              id_lens(<<"indexer.settings.num_replica">>)}] ++
            case ns_config_default:init_is_enterprise() of
                true ->
                    [{redistributeIndexes,
                      id_lens(<<"indexer.settings.rebalance."
                                "redistribute_indexes">>)}];
                false ->
                    []
            end;
        _ ->
            []
    end ++
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_NEO) of
        true ->
            [{enablePageBloomFilter,
              id_lens(<<"indexer.settings.enable_page_bloom_filter">>)}];
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
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_70) of
        true ->
            [{numReplica, 0}] ++
            case ns_config_default:init_is_enterprise() of
                true ->
                    [{redistributeIndexes, false}];
                false ->
                    []
            end;
        _ ->
            []
    end ++
    case cluster_compat_mode:is_enabled_at(ClusterVersion, ?VERSION_NEO) of
        true ->
            [{enablePageBloomFilter, false}];
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
    Get = fun (Dict) ->
                  Int0 = binary_to_list(dict:fetch(Key, Dict)),
                  [From, To] = string:tokens(Int0, ","),
                  [FromH, FromM] = string:tokens(From, ":"),
                  [ToH, ToM] = string:tokens(To, ":"),
                  [{from_hour, list_to_integer(FromH)},
                   {from_minute, list_to_integer(FromM)},
                   {to_hour, list_to_integer(ToH)},
                   {to_minute, list_to_integer(ToM)}]
          end,
    Set = fun (Values0, Dict) ->
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

                  dict:store(Key, Value, Dict)
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

config_upgrade_to_70(Config) ->
    config_upgrade_settings(Config, ?VERSION_66, ?VERSION_70).

config_upgrade_to_NEO(Config) ->
    config_upgrade_settings(Config, ?VERSION_70, ?VERSION_NEO).

config_upgrade_settings(Config, OldVersion, NewVersion) ->
    NewSettings = general_settings_defaults(NewVersion) --
        general_settings_defaults(OldVersion),
    json_settings_manager:upgrade_existing_key(
      ?MODULE, Config, [{generalSettings, NewSettings}],
      known_settings(NewVersion)).

-ifdef(TEST).
setup_meck() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> true end),
    meck:new(ns_config_default, [passthrough]),
    meck:expect(ns_config_default, init_is_enterprise,
                fun () -> true end).

teardown_meck() ->
    meck:unload(cluster_compat_mode),
    meck:unload(ns_config_default).

defaults_test() ->
    setup_meck(),
    Keys = fun (L) -> lists:sort([K || {K, _} <- L]) end,

    ?assertEqual(Keys(known_settings(?LATEST_VERSION_NUM)), Keys(default_settings())),
    ?assertEqual(Keys(compaction_lens_props()), Keys(compaction_defaults())),
    ?assertEqual(Keys(general_settings_lens_props(?LATEST_VERSION_NUM)),
                 Keys(general_settings_defaults(?LATEST_VERSION_NUM))),
    teardown_meck().

config_upgrade_test() ->
    setup_meck(),
    CmdList = config_upgrade_to_70([]),
    [{set, {metakv, Meta}, Data}] = CmdList,
    ?assertEqual(<<"/indexing/settings/config">>, Meta),
    ?assertEqual(<<"{\"indexer.settings.rebalance.redistribute_indexes\":false,"
                   "\"indexer.settings.num_replica\":0}">>, Data),

    CmdList2 = config_upgrade_to_NEO([]),
    [{set, {metakv, Meta2}, Data2}] = CmdList2,
    ?assertEqual(<<"/indexing/settings/config">>, Meta2),
    ?assertEqual(<<"{\"indexer.settings.enable_page_bloom_filter\":false}">>,
                 Data2),

    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> false end),
    meck:expect(ns_config_default, init_is_enterprise,
                fun () -> false end),

    CmdList3 = config_upgrade_to_70([]),
    [{set, {metakv, Meta3}, Data3}] = CmdList3,
    ?assertEqual(<<"/indexing/settings/config">>, Meta3),
    ?assertEqual(<<"{\"indexer.settings.num_replica\":0}">>, Data3),

    teardown_meck().

-endif.
