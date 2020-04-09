%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-2019 Couchbase, Inc.
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
         on_update/2]).

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
     {generalSettings, general_settings_defaults()},
     {compaction, compaction_defaults()},
     {storageMode, <<"">>},
     {compactionMode, <<"circular">>},
     {circularCompaction, CircDefaults}].

known_settings() ->
    [{memoryQuota, memory_quota_lens()},
     {generalSettings, general_settings_lens()},
     {compaction, compaction_lens()},
     {storageMode, id_lens(<<"indexer.settings.storage_mode">>)},
     {compactionMode,
      id_lens(<<"indexer.settings.compaction.compaction_mode">>)},
     {circularCompaction, circular_compaction_lens()}].

config_default() ->
    {?INDEX_CONFIG_KEY, json_settings_manager:build_settings_json(
                          default_settings(),
                          dict:new(), known_settings())}.

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

general_settings_lens_props() ->
    [{indexerThreads, indexer_threads_lens()},
     {memorySnapshotInterval, id_lens(<<"indexer.settings.inmemory_snapshot.interval">>)},
     {stableSnapshotInterval, id_lens(<<"indexer.settings.persisted_snapshot.interval">>)},
     {maxRollbackPoints, id_lens(<<"indexer.settings.recovery.max_rollbacks">>)},
     {logLevel, id_lens(<<"indexer.settings.log_level">>)}].

default_rollback_points() ->
    case ns_config_default:init_is_enterprise() of
        true ->
            ?DEFAULT_MAX_ROLLBACK_PTS_PLASMA;
        false ->
            ?DEFAULT_MAX_ROLLBACK_PTS_FORESTDB
    end.

general_settings_defaults() ->
    [{indexerThreads, 0},
     {memorySnapshotInterval, 200},
     {stableSnapshotInterval, 5000},
     {maxRollbackPoints, default_rollback_points()},
     {logLevel, <<"info">>}].

general_settings_lens() ->
    json_settings_manager:props_lens(general_settings_lens_props()).

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


-ifdef(TEST).
defaults_test() ->
    Keys = fun (L) -> lists:sort([K || {K, _} <- L]) end,

    ?assertEqual(Keys(known_settings()), Keys(default_settings())),
    ?assertEqual(Keys(compaction_lens_props()), Keys(compaction_defaults())),
    ?assertEqual(Keys(general_settings_lens_props()),
                 Keys(general_settings_defaults())).
-endif.
