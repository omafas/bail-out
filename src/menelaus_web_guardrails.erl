%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(menelaus_web_guardrails).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("ns_test.hrl").
-endif.

-export([handle_get/2, handle_post/2]).

-export([default_config/0, config_upgrade_to_76/1,
         build_json_for_audit/1]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

handle_get(Path, Req) ->
    menelaus_util:assert_is_76(),
    menelaus_util:assert_config_profile_flag({resource_management, enabled}),

    menelaus_web_settings2:handle_get(Path, params(), undefined,
                                      guardrail_monitor:get_config(), Req).

handle_post(Path, Req) ->
    menelaus_util:assert_is_76(),
    menelaus_util:assert_config_profile_flag({resource_management, enabled}),

    menelaus_web_settings2:handle_post(
      fun (Params, Req2) ->
              case Params of
                  [] -> ok;
                  _ ->
                      Values = update_config(Params),
                      ns_audit:resource_management(Req, Values)
              end,
              handle_get(Path, Req2)
      end, Path, params(), undefined, guardrail_monitor:get_config(), [], Req).

params() ->
    [
     %% Bucket resident ratio configuration
     {"bucket.residentRatio.enabled",
      #{type => bool,
        cfg_key => [bucket, resident_ratio, enabled]}},
     {"bucket.residentRatio.couchstoreMinimum",
      #{type => {num, 0, 100},
        cfg_key => [bucket, resident_ratio, couchstore_minimum]}},
     {"bucket.residentRatio.magmaMinimum",
      #{type => {num, 0, 100},
        cfg_key => [bucket, resident_ratio, magma_minimum]}},
     %% Max data per node per bucket configuration
     {"bucket.dataSizePerNode.enabled",
      #{type => bool,
        cfg_key => [bucket, data_size, enabled]}},
     {"bucket.dataSizePerNode.couchstoreMaximum",
      #{type => {num, 0, infinity},
        cfg_key => [bucket, data_size, couchstore_maximum]}},
     {"bucket.dataSizePerNode.magmaMaximum",
      #{type => {num, 0, infinity},
        cfg_key => [bucket, data_size, magma_maximum]}},
     %% Max number of collections per memory quota in MB
     {"bucket.collectionsPerQuota.enabled",
      #{type => bool,
        cfg_key => [collections_per_quota, enabled]}},
     {"bucket.collectionsPerQuota.maximum",
      #{type => {num, 0, infinity},
        cfg_key => [collections_per_quota, maximum]}},
     %% Max disk usage % per node
     {"diskUsage.enabled",
      #{type => bool,
        cfg_key => [disk_usage, enabled]}},
     {"diskUsage.maximum",
      #{type => {num, 0, 100},
        cfg_key => [disk_usage, maximum]}},
     %% Min number of cores per node per bucket
     {"coresPerBucket.enabled",
      #{type => bool,
        cfg_key => [cores_per_bucket, enabled]}},
     {"coresPerBucket.minimum",
      #{type => {num, 0, infinity},
        cfg_key => [cores_per_bucket, minimum]}}
    ].

%% Gets resource management configuration from the config profile, using default
%% values specified below
-spec default_config() -> proplists:proplist().
default_config() ->
    %% Override defaults with any values specified in the config profile
    [{resource_management,
      lists:foldl(
        fun update_sub_config/2, raw_default_config(),
        config_profile:get_value(resource_management, []))}].

%% Default config, without being overriden by config profile
raw_default_config() ->
    [
     %% Bucket level resources
     {bucket,
      %% Resident ratio percentage minimum
      [{resident_ratio,
        [{enabled, false},
         {couchstore_minimum, 1},
         {magma_minimum, 0.2}]},
       %% Max data size per bucket on a node in TB
       {data_size,
        [{enabled, false},
         {couchstore_maximum, 2},
         {magma_maximum, 16}]}
      ]},
     %% Minimum cores required per bucket
     {cores_per_bucket,
      [{enabled, false},
       {minimum, 0.4}]},
     %% Max disk usage % per node
     {disk_usage,
      [{enabled, false},
       {maximum, 96}]},
     %% Max no. of collections per bucket quota in MB
     {collections_per_quota,
      [{enabled, false},
       {maximum, 1}]}
    ].

update_sub_config({[], Value}, _) ->
    Value;
update_sub_config({[Key | Keys], Value}, []) ->
    [{Key, update_sub_config({Keys, Value}, [])}];
update_sub_config({[Key | Keys], Value}, SubConfig) ->
    lists:keyreplace(Key, 1, SubConfig,
                     {Key,
                      update_sub_config({Keys, Value},
                                        proplists:get_value(Key, SubConfig,
                                                            []))}).

update_config(Changes) ->
    OldConfig = guardrail_monitor:get_config(),

    NewConfig = lists:foldl(fun update_sub_config/2, OldConfig, Changes),

    ns_config:set(resource_management, NewConfig),
    NewConfig.

config_upgrade_to_76(_Config) ->
    [{set, resource_management,
      proplists:get_value(resource_management, default_config())}].

-spec build_json_for_audit(proplists:proplist()) -> proplists:proplist().
build_json_for_audit(Settings) ->
    [{settings, {json, build_json(Settings)}}].

build_json(Settings) ->
    {lists:map(
       fun({Key, Value}) when is_list(Value) ->
               {Key, build_json(Value)};
          ({Key, Value}) ->
               {Key, Value}
       end, Settings)}.

-ifdef(TEST).

meck_modules() ->
    [config_profile, ns_config, ns_bucket].

basic_test_setup() ->
    meck:new(meck_modules(), [passthrough]).

default_config_t() ->
    assert_config_equal([{resource_management, raw_default_config()}],
                        default_config()),
    SetConfigProfile =
        fun (Config) ->
                meck:expect(config_profile, get_value,
                            fun (Key, Default) ->
                                    proplists:get_value(Key, Config, Default)
                            end)
        end,

    SetConfigProfile([{resource_management,
                       [{[bucket, resident_ratio, enabled], true},
                        {[bucket, resident_ratio, couchstore_minimum], 10},
                        {[bucket, resident_ratio, magma_minimum], 1},
                        {[cores_per_bucket, enabled], true},
                        {[bucket, data_size, enabled], true},
                        {[bucket, data_size, couchstore_maximum], 1.6},
                        {[bucket, data_size, magma_maximum], 16},
                        {[disk_usage, enabled], true},
                        {[disk_usage, maximum], 85},
                        {[collections_per_quota, enabled], true}]
                      }]),
    assert_config_equal([{resource_management,
                          [{bucket,
                            [{resident_ratio,
                              [{enabled, true},
                               {couchstore_minimum, 10},
                               {magma_minimum, 1}]
                             },
                             {data_size,
                              [{enabled, true},
                               {couchstore_maximum, 1.6},
                               {magma_maximum, 16}]}]
                           },
                           {cores_per_bucket,
                            [{enabled, true},
                             {minimum, 0.4}]},
                           {disk_usage,
                            [{enabled, true},
                             {maximum, 85}]},
                           {collections_per_quota,
                            [{enabled, true},
                             {maximum, 1}]}]
                         }], default_config()).

assert_config_equal(Expected, Found) when is_list(Expected)->
    ?assert(is_list(Found)),
    ?assertListsEqual(proplists:get_keys(Expected), proplists:get_keys(Found)),
    lists:foreach(
      fun (Key) ->
              case proplists:get_value(Key, Expected) of
                  ExpectedList when is_list(ExpectedList) ->
                      FoundList = proplists:get_value(Key, Found),
                      ?assert(is_list(FoundList)),
                      assert_config_equal(ExpectedList, FoundList);
                  Value ->
                      ?assertEqual(Value, proplists:get_value(Key, Found))
              end
      end, proplists:get_keys(Expected)).

assert_config_update(Expected, Update, Initial) ->
    meck:expect(ns_config, read_key_fast,
                fun (resource_management, []) ->
                        Initial
                end),

    assert_config_equal(Expected, update_config(Update)),

    meck:called(ns_config, set, [{resource_management, Expected}]).

update_configs_t() ->
    %% Test update_sub_config alone
    ?assertEqual(value1,
                 update_sub_config({[], value1}, [])),
    assert_config_equal([{key1, value1}],
                        update_sub_config({[key1], value1}, [])),
    assert_config_equal([{key1, [{key2, value2}]}],
                        update_sub_config({[key1, key2], value2}, [])),
    assert_config_equal([{key1, [{key2, value2}]}],
                        update_sub_config({[key1, key2], value2},
                                          [{key1, []}])),
    assert_config_equal([{key1, [{key2, value2}]},
                         {key3, [{key4, value4}]}],
                        update_sub_config({[key1, key2], value2},
                                          [{key1, []},
                                           {key3, [{key4, value4}]}])),

    meck:expect(ns_config, set, fun (_Key, _Config) -> ok end),

    %% Test update_configs
    assert_config_update([{bucket, [{resident_ratio, [{enabled, true}]}]}],
                         [{[bucket, resident_ratio, enabled], true}],
                         []),

    assert_config_update([{bucket, [{resident_ratio, [{enabled, true}]}]}],
                         [{[bucket, resident_ratio, enabled], true}],
                         [{bucket, [{resident_ratio, [{enabled, false}]}]}]),

    assert_config_update([{bucket, [{resident_ratio,
                                     [{enabled, true},
                                      {couchstore_minimum, 10}]}]}],
                         [{[bucket, resident_ratio, enabled], true}],
                         [{bucket, [{resident_ratio,
                                     [{enabled, false},
                                      {couchstore_minimum, 10}]}]}]).

basic_test_teardown() ->
    meck:unload(meck_modules()).

basic_test_() ->
    {setup,
     fun () ->
             basic_test_setup()
     end,
     fun(_) ->
             basic_test_teardown()
     end,
     [{"default config test", fun () -> default_config_t() end},
      {"update configs test", fun () -> update_configs_t() end}]}.

test_build_json(ExpectedEJson, Proplist) ->
    [{settings, {json, EJson}}] = Result = build_json_for_audit(Proplist),
    ?assertEqual(ExpectedEJson, EJson),
    %% Test that the output can be converted to json by ns_audit
    ejson:encode({json_builder:prepare_list(Result)}).

build_json_test() ->
    test_build_json({[]}, []),

    test_build_json({[{key, value}]}, [{key, value}]),

    test_build_json({[{key, {[]}}]}, [{key, []}]),

    test_build_json({[{key0, {[{key1, value1}]}}]}, [{key0, [{key1, value1}]}]),

    test_build_json({[{key0, {[{key1, {[{key2, value2}]}}, {key3, value3}]}}]},
                    [{key0, [{key1, [{key2, value2}]}, {key3, value3}]}]),

    ok.

-endif.
