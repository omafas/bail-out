%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_web_mcd_settings).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_global_get/1,
         handle_effective_get/2,
         handle_global_post/1,
         handle_global_delete/2,
         handle_node_get/2,
         handle_node_post/2,
         handle_node_setting_get/3,
         handle_node_setting_delete/3,
         config_upgrade_to_79/1,
         config_upgrade_to_80/1]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3]).

%% We are encoding our base64 parameters directly into the config rather than
%% storing the raw value. This lets us handle parameters on this node even if
%% other nodes in the cluster are an older version correctly, as the base64
%% encoded version will still be sent to memcached on those nodes, rather than
%% a non-encoded version.
-define(BASE64_PARAMS, [dcp_disconnect_when_stuck_name_regex]).

%% Updates to these settings go to the '{node, node(), memcached}' or
%% 'memcached' keys depending on whether the write is per-node or global.
%% These change values of keys in memcached.json.

ns_config_setting_names() ->
    [{max_connections, {int, 2000, ?MAX_32BIT_SIGNED_INT}},
     {num_reader_threads, fun validate_num_threads/1},
     {num_writer_threads, fun validate_num_threads/1},
     {num_auxio_threads, fun validate_num_storage_auxio_nonio_threads/1},
     {num_nonio_threads, fun validate_num_storage_auxio_nonio_threads/1},
     {num_storage_threads, fun validate_num_storage_auxio_nonio_threads/1},
     {magma_flusher_thread_percentage, {int, 0, 100}},
     {system_connections, {int, 1000, ?MAX_32BIT_SIGNED_INT}},
     {verbosity, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {ssl_cipher_list, string},
     {connection_idle_time, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {breakpad_enabled, bool},
     {breakpad_minidump_dir_path, string},
     {dedupe_nmvb_maps, bool},
     {tracing_enabled, bool},
     {datatype_snappy, bool},
     {tcp_keepalive_idle, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {tcp_keepalive_interval, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {tcp_keepalive_probes, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {tcp_user_timeout, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {free_connection_pool_size, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {max_client_connection_details, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {fusion_migration_rate_limit, fun validate_fusion_rate/1},
     {fusion_sync_rate_limit, fun validate_fusion_rate/1},
     {fusion_num_uploader_threads, {int, 1, 128}},
     {fusion_num_migrator_threads, {int, 1, 128}},
     {fusion_max_pending_upload_bytes, {int, 0, ?MAX_64BIT_UNSIGNED_INT}},
     {fusion_max_pending_upload_bytes_lwm_percentage, {int, 0, 100}},
     {dcp_consumer_max_marker_version, {one_of, ["2.0", "2.2"]}},
     {dcp_snapshot_marker_hps_enabled, bool},
     {dcp_snapshot_marker_purge_seqno_enabled, bool},
     {subdoc_multi_max_paths, {int, 16, ?MAX_32BIT_SIGNED_INT}},
     {subdoc_offload_size_threshold, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {subdoc_offload_paths_threshold, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {snapshot_download_throttle_bytes, {int, 0, ?MAX_64BIT_UNSIGNED_INT}}]
        ++
        %% KV stopped supporting this is 7.6, they just ignore it, but we
        %% should probably support it in mixed mode. Even though we "support" it
        %% in the API, we will not actually set it in the memcached config on
        %% this node, just the other older nodes.
        case cluster_compat_mode:is_cluster_79() of
            true -> [];
            false ->
                [{connection_limit_mode, {one_of, ["disconnect", "recycle"]}}]
        end.

%% Updates to these settings go to the '{node, node(), memcached_config_extra}'
%% or 'memcached_config_extra' keys depending on whether the write is per-node
%% or global. These add new keys to memcached.json.

extra_ns_config_setting_names() ->
    [{default_reqs_per_event, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {reqs_per_event_high_priority, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {reqs_per_event_med_priority, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {reqs_per_event_low_priority, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {threads, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {dcp_disconnect_when_stuck_timeout_seconds,
      {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {dcp_disconnect_when_stuck_name_regex, string},
     {external_auth_request_timeout, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {not_locked_returns_tmpfail, bool},
     {clustermap_push_notifications_enabled, bool},
     {magma_blind_write_optimisation_enabled, bool},
     {file_fragment_checksum_enabled, bool},
     {file_fragment_max_chunk_size, {int, 0, ?MAX_64BIT_UNSIGNED_INT}},
     {file_fragment_checksum_length, {int, 0, ?MAX_64BIT_UNSIGNED_INT}},
     {snapshot_download_fsync_interval, {int, 0, ?MAX_64BIT_UNSIGNED_INT}},
     {snapshot_download_write_size, {int, 0, ?MAX_64BIT_UNSIGNED_INT}}].

%% Updates to these settings go to chronicle keys 'memcached_config_settings'
%% (global) or '{node, Node, memcached_config_settings}' (per-node).
%% These change values of keys in memcached.json.

chronicle_setting_names() ->
    [{throttle_enabled, bool},
     {read_unit_size, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {write_unit_size, {int, 0, ?MAX_32BIT_SIGNED_INT}},
     {node_capacity, {int, 0, ?MAX_64BIT_UNSIGNED_INT}}].

all_setting_names() ->
    chronicle_setting_names() ++ ns_config_setting_names() ++
        extra_ns_config_setting_names().

validate_chronicle_params_for_cluster_compat(Params) ->
    ParamNames = [K || {K, _} <- Params],
    ChronicleParams = [K || {K, _} <- chronicle_setting_names(),
                            lists:member(atom_to_list(K), ParamNames)],

    %% We do not allow any chronicle related params to be set in
    %% cluster on compat version prior to totoro as memcached
    %% chronicle settings are introduced in totoro
    case ChronicleParams =/= [] andalso
         not cluster_compat_mode:is_cluster_totoro() of
        true ->
            Msg = io_lib:format(
                    "The following parameters are not supported prior to "
                    "version ~p: ~p", [?VERSION_TOTORO, ChronicleParams]),
            menelaus_util:web_exception(400, iolist_to_binary(Msg));
        false ->
            ok
    end.

supported_nodes() ->
    ns_node_disco:nodes_wanted().

parse_validate_node("self") ->
    parse_validate_node(atom_to_list(node()));
parse_validate_node(Name) ->
    NodesWantedS = [atom_to_list(N) || N <- supported_nodes()],
    case lists:member(Name, NodesWantedS) of
        false ->
            unknown;
        true ->
            {ok, list_to_atom(Name)}
    end.

with_parsed_node(Name, Req, Body) ->
    case parse_validate_node(Name) of
        unknown ->
            reply_json(Req, [], 404);
        {ok, Node} ->
            Body(Node)
    end.

%% Resolves the logical config type to the pair of actual config keys used
%% for settings and extra settings respectively.
ns_config_keys(memcached_config_global) ->
    {memcached, memcached_config_extra};
ns_config_keys({memcached_config_node, Node}) ->
    {{node, Node, memcached},
     {node, Node, memcached_config_extra}}.

chronicle_config_key(memcached_config_global) ->
    memcached_config_settings;
chronicle_config_key({memcached_config_node, Node}) ->
    {node, Node, memcached_config_settings}.

handle_global_get(Req) ->
    handle_get(Req, memcached_config_global, 200).

handle_effective_get(Name, Req) ->
    with_parsed_node(
      Name, Req,
      fun (Node) ->
              KVsGlobal = build_setting_kvs(memcached_config_global),
              KVsLocal = build_setting_kvs({memcached_config_node, Node}),
              KVsDefault =
                  build_ns_config_default_setting_kvs(Node) ++
                  ?MCD_SETTINGS_CHRONICLE_DEFAULTS,
              KVs = lists:foldl(
                      fun ({K, V}, Acc) ->
                              lists:keystore(K, 1, Acc, {K, V})
                      end, [], lists:append([KVsDefault, KVsGlobal, KVsLocal])),
              reply_json(Req, {KVs}, 200)
      end).

handle_node_get(Name, Req) ->
    with_parsed_node(
      Name, Req,
      fun (Node) ->
              handle_get(Req, {memcached_config_node, Node}, 200)
      end).

map_settings(SettingNames, Settings) ->
    lists:flatmap(
      fun ({Name, _}) ->
              case lists:keyfind(Name, 1, Settings) of
                  false ->
                      [];
                  {_, Value0} ->
                      Value =
                          case lists:member(Name, ?BASE64_PARAMS) of
                              true ->
                                  base64:decode(Value0);
                              false ->
                                  case is_list(Value0) of
                                      true ->
                                          list_to_binary(Value0);
                                      false ->
                                          Value0
                                  end
                          end,
                      [{Name, Value}]
              end
      end, SettingNames).

build_ns_config_default_setting_kvs(Node) ->
    {value, McdSettings} =
        ns_config:search(ns_config:latest(), {node, Node, memcached_defaults}),
    map_settings(all_setting_names(), McdSettings).

build_setting_kvs(ConfigType)
  when ConfigType =:= memcached_config_global;
       element(1, ConfigType) =:= memcached_config_node ->
    {SettingsKey, ExtraConfigKey} = ns_config_keys(ConfigType),
    ChronicleKey = chronicle_config_key(ConfigType),
    build_setting_kvs(SettingsKey, ExtraConfigKey, ChronicleKey).

build_setting_kvs(SettingsKey, ExtraConfigKey, ChronicleKey) ->
    {value, McdSettings} =
        ns_config:search(ns_config:latest(), SettingsKey),
    ExtraSettings =
        ns_config:search(ns_config:latest(), ExtraConfigKey, []),
    ChronicleSettings =
        chronicle_compat:get(
          direct, ChronicleKey, #{default => []}),
    map_settings(ns_config_setting_names(), McdSettings)
        ++ map_settings(
             extra_ns_config_setting_names(), ExtraSettings)
        ++ map_settings(
             chronicle_setting_names(), ChronicleSettings).

handle_get(Req, ConfigType, Status) ->
    KVs = build_setting_kvs(ConfigType),
    reply_json(Req, {KVs}, Status).

handle_global_post(Req) ->
    handle_post(Req, memcached_config_global).

handle_node_post(Name, Req) ->
    with_parsed_node(
      Name, Req,
      fun (Node) ->
              handle_post(Req, {memcached_config_node, Node})
      end).

handle_post(Req, ConfigType) ->
    KnownNames = lists:map(
                   fun ({A, _}) -> atom_to_list(A) end,
                   all_setting_names()),
    Params = mochiweb_request:parse_post(Req),
    UnknownParams = [K || {K, _} <- Params,
                          not lists:member(K, KnownNames)],
    case UnknownParams of
        [] ->
            validate_chronicle_params_for_cluster_compat(Params),
            continue_handle_post(Req, Params, ConfigType);
        _ ->
            Msg = io_lib:format("Unknown POST parameters: ~p", [UnknownParams]),
            reply_json(Req, {[{'_', iolist_to_binary(Msg)}]}, 400)
    end.

validate_param(Value, {int, Min, Max}) ->
    menelaus_util:parse_validate_number(Value, Min, Max);
validate_param(Value, bool) ->
    case Value of
        "true" ->
            {ok, true};
        "false" ->
            {ok, false};
        _->
            <<"must be either true or false">>
    end;
validate_param(Value, string) ->
    {ok, Value};
validate_param(Value, {one_of, Values}) ->
    case lists:member(Value, Values) of
        true -> {ok, list_to_binary(Value)};
        false -> list_to_binary(io_lib:format("must be one of: [~s]",
                                              [string:join(Values, ", ")]))
    end;
validate_param(Value, Fun) when is_function(Fun, 1) ->
    Fun(Value).

validate_fusion_rate("0") ->
    {ok, 0};
validate_fusion_rate(Value) ->
    validate_param(Value, {int, 1024 * 1024 * 8, ?MAX_32BIT_SIGNED_INT}).

%% Support "default" for backwards compatibility.
validate_num_threads("default") -> {ok, <<"balanced">>};
validate_num_threads("disk_io_optimized") -> {ok, <<"disk_io_optimized">>};
validate_num_threads("balanced") -> {ok, <<"balanced">>};
validate_num_threads(Value) -> validate_param(Value, {int, 1, 64}).

validate_num_storage_auxio_nonio_threads("default") -> {ok, <<"default">>};
validate_num_storage_auxio_nonio_threads(Value) -> validate_param(Value, {int, 1, 64}).

continue_handle_post(Req, Params, ConfigType) ->
    {SettingsKey, ExtraConfigKey} = ns_config_keys(ConfigType),
    ChronicleKey = chronicle_config_key(ConfigType),
    AllNames = all_setting_names(),
    ParsedParams =
        lists:flatmap(
          fun ({Name, ValidationType}) ->
                  NameString = atom_to_list(Name),
                  case lists:keyfind(NameString, 1, Params) of
                      false ->
                          [];
                      {_, Value} ->
                          [{Name, validate_param(Value, ValidationType)}]
                  end
          end, AllNames),
    InvalidParams = [{K, V} || {K, V} <- ParsedParams,
                               case V of
                                   {ok, _} -> false;
                                   _ -> true
                               end],
    case InvalidParams of
        [_|_] ->
            reply_json(Req, {InvalidParams}, 400);
        [] ->
            KVs0 = [{K, V} || {K, {ok, V}} <- ParsedParams],
            %% We are encoding our base64 parameters directly into the config
            %% rather than storing the raw value. This lets us handle parameters
            %% on this node even if other nodes in the cluster are an older
            %% version correctly, as the base64 encoded version will still be
            %% sent to memcached on those nodes, rather than a non-encoded
            %% version.
            KVs = lists:map(
                    fun ({Name, Value}) ->
                            case lists:member(Name, ?BASE64_PARAMS) of
                                true ->
                                    {Name, base64:encode(Value)};
                                false ->
                                    {Name, Value}
                            end
                    end, KVs0),
            {COS, CNS} =
                case update_chronicle_config(
                       chronicle_setting_names(), ChronicleKey, KVs) of
                    {updated, {COS0, CNS0}} ->
                        {COS0, CNS0};
                    unchanged ->
                        {[], []}
                end,
            {OS, NS} =
                case update_config(
                       ns_config_setting_names(), SettingsKey, KVs) of
                    {commit, _, {OS0, NS0}} ->
                        {OS0, NS0};
                    _ ->
                        {[], []}
                end,
            {EOS, ENS} =
                case update_config(
                       extra_ns_config_setting_names(), ExtraConfigKey, KVs) of
                    {commit, _, {EOS0, ENS0}} ->
                        {EOS0, ENS0};
                    _ ->
                        {[], []}
                end,
            %% Merge both the old settings and extra old settings, so that we
            %% can add a single event log.
            OldSettings = OS ++ EOS ++ COS,
            NewSettings = NS ++ ENS ++ CNS,

            if
                NewSettings =/= [] andalso NewSettings =/= OldSettings ->
                    event_log:add_log(
                      memcached_cfg_changed,
                      [{old_settings, {OldSettings}},
                       {new_settings, {NewSettings}}]);
                true ->
                    ok
            end,

            handle_get(Req, ConfigType, 202)
    end.

update_config(Settings, ConfigKey, KVs) ->
    ns_config:run_txn(
      fun (OldConfig, SetFn) ->
              OldValue = ns_config:search(OldConfig, ConfigKey, []),
              NewValue =
                  lists:foldl(
                    fun ({K, V}, NewValue0) ->
                            case lists:keyfind(K, 1, Settings) =/= false of
                                true ->
                                    lists:keystore(K, 1, NewValue0, {K, V});
                                _ ->
                                    NewValue0
                            end
                    end, OldValue, KVs),
              NewConfig = case NewValue =/= OldValue of
                              true ->
                                  SetFn(ConfigKey, NewValue, OldConfig);
                              _ ->
                                  OldConfig
                          end,
              {commit, NewConfig, {OldValue, NewValue}}
      end).

update_chronicle_config(Settings, ChronicleKey, KVs) ->
    OldValue = chronicle_compat:get(
                 direct, ChronicleKey, #{default => []}),
    NewValue =
        lists:foldl(
          fun ({K, V}, Acc) ->
                  case lists:keyfind(K, 1, Settings)
                           =/= false of
                      true ->
                          lists:keystore(K, 1, Acc, {K, V});
                      false ->
                          Acc
                  end
          end, OldValue, KVs),
    case NewValue =/= OldValue of
        true ->
            {ok, _} = chronicle_kv:set(kv, ChronicleKey, NewValue),
            {updated, {OldValue, NewValue}};
        false ->
            unchanged
    end.

handle_node_setting_get(NodeName, SettingName, Req) ->
    with_parsed_node(
      NodeName, Req,
      fun (Node) ->
              KVs = build_setting_kvs({memcached_config_node, Node}),
              MaybeProp = [V || {K, V} <- KVs,
                                atom_to_list(K) =:= SettingName],
              case MaybeProp of
                  [] ->
                      reply_json(Req, [], 404);
                  [Value] ->
                      reply_json(Req, {[{value, Value}]})
              end
      end).

handle_global_delete(Setting, Req) ->
    %% global delete only supports the extra settings, these are settings that
    %% we can add, so we should be able to delete them. The other settings are
    %% always present in the global config, so we should not allow their
    %% deletion.
    MaybeExtra = [K || {K, _} <- extra_ns_config_setting_names(),
                       atom_to_list(K) =:= Setting],
    case MaybeExtra of
        [] ->
            Msg = io_lib:format("Unknown/unsupported setting: ~p", [Setting]),
            reply_json(Req, {[{'_', iolist_to_binary(Msg)}]}, 404);
        _ ->
            {_, ExtraConfigKey} = ns_config_keys(memcached_config_global),
            case do_delete_txn(ExtraConfigKey, hd(MaybeExtra)) of
                ok ->
                    reply_json(Req, [], 202);
                missing ->
                    reply_json(Req, [], 404)
            end
    end.

handle_node_setting_delete(NodeName, SettingName, Req) ->
    with_parsed_node(
      NodeName, Req,
      fun (Node) ->
              case perform_delete_txn(Node, SettingName) of
                  ok ->
                      reply_json(Req, [], 202);
                  missing ->
                      reply_json(Req, [], 404)
              end
      end).

perform_delete_txn(Node, SettingName) ->
    MaybeSetting = [K || {K, _} <- ns_config_setting_names(),
                         atom_to_list(K) =:= SettingName],
    MaybeExtra = [K || {K, _} <- extra_ns_config_setting_names(),
                       atom_to_list(K) =:= SettingName],
    MaybeChronicle =
        [K || {K, _} <- chronicle_setting_names(),
              atom_to_list(K) =:= SettingName],
    {SettingsKey, ExtraConfigKey} =
        ns_config_keys({memcached_config_node, Node}),
    if
        MaybeSetting =/= [] ->
            do_delete_txn(SettingsKey, hd(MaybeSetting));
        MaybeExtra =/= [] ->
            do_delete_txn(ExtraConfigKey, hd(MaybeExtra));
        MaybeChronicle =/= [] ->
            ChronicleKey = chronicle_config_key(
                             {memcached_config_node, Node}),
            do_delete_chronicle(
              ChronicleKey, hd(MaybeChronicle));
        true ->
            missing
    end.

do_delete_txn(Key, Setting) ->
    RV =
        ns_config:run_txn(
          fun (Config, SetFn) ->
                  OldValue = ns_config:search(Config, Key, []),
                  NewValue = lists:keydelete(Setting, 1, OldValue),
                  case OldValue =:= NewValue of
                      true ->
                          {abort, []};
                      false ->
                          {commit, SetFn(Key, NewValue, Config)}
                  end
          end),
    case RV of
        {abort, _} ->
            missing;
        {commit, _} ->
            ok
    end.

do_delete_chronicle(ChronicleKey, Setting) ->
    Rv =
        chronicle_kv:transaction(kv, [ChronicleKey],
            fun (Snapshot) ->
                OldSettings =
                    chronicle_compat:get(Snapshot, ChronicleKey,
                                         #{default => []}),
                NewSettings = lists:keydelete(Setting, 1, OldSettings),
                case OldSettings =:= NewSettings of
                    true ->
                        {abort, {error, missing}};
                    false ->
                        {commit, [{set, ChronicleKey, NewSettings}]}

                end
            end
        ),

    case Rv of
        {ok, _} ->
            ok;
        {error, missing} ->
            missing
    end.

config_upgrade_to_79(Config) ->
    config_upgrade_to_79_global_memcached_cfg(Config) ++
    config_upgrade_to_79_node_memcached_cfg(Config).

config_upgrade_to_79_global_memcached_cfg(Config) ->
    case ns_config:search(Config, memcached) of
        false -> [];
        {value, Value} ->
            case proplists:get_value(connection_limit_mode, Value) of
                undefined -> [];
                _ ->
                    [{set, memcached,
                         proplists:delete(connection_limit_mode, Value)}]
            end

    end.

config_upgrade_to_79_node_memcached_cfg(Config) ->
    lists:foldl(
        fun(Node, Acc) ->
            NodeMcdCfg = ns_config:search(Config, {node, Node, memcached}, []),
            case NodeMcdCfg of
                [] -> Acc;
                Value ->
                    case proplists:get_value(connection_limit_mode, Value) of
                        undefined -> Acc;
                        _ ->
                            [{set, {node, Node, memcached},
                              proplists:delete(connection_limit_mode, Value)}
                            | Acc]
                    end
            end
        end, [], supported_nodes()).

config_upgrade_to_80(Config) ->
    %% We want to enable the magma blind write optimisation by default for all
    %% 8.0 clusters. We have a get_param though such that we can turn this off
    %% prior to the upgrade just in case.
    EnableMagmaBlindWriteOptimisation =
        [{magma_blind_write_optimisation_enabled,
          ?get_param(magma_blind_write_optimisation_enabled, true)}],
    case ns_config:search(Config, memcached) of
        false ->
            %% This key really should exist...
            [{set, memcached, EnableMagmaBlindWriteOptimisation}];
        {value, Value} ->
            %% Merge the new setting into the existing memcached config...
            [{set, memcached,
              misc:update_proplist(Value, EnableMagmaBlindWriteOptimisation)}]
    end.

-ifdef(TEST).
upgrade_config_test_setup() ->
    ns_config_default:ns_config_default_mock_setup(),

    %% We upgrade node keys so we need to mock the method we use to get the
    %% nodes list.
    meck:new(ns_node_disco),
    meck:expect(ns_node_disco, nodes_wanted,
                fun () -> [node()] end),

    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, search_node_with_default,
                fun(_, Default) ->
                        Default
                end).

upgrade_config_test_teardown(R) ->
    ns_config_default:ns_config_default_mock_teardown(R),
    meck:unload().

upgrade_config_from_76_to_79_t() ->
    %% We'll use a default config to test, but add some of the older verrsion
    %% values that are relevant to the upgrade.
    Default = ns_config_default:default(?VERSION_79),

    %% Firstly lets add the connection_limit_mode to the global memcached config
    GlobalMcdCfgKey = memcached,
    {GlobalMcdCfgKey, MemcachedCfg} =
        lists:keyfind(GlobalMcdCfgKey, 1, Default),
    NewMemcachedCfg = lists:keystore(connection_limit_mode, 1, MemcachedCfg,
                                     {connection_limit_mode, "disconnect"}),
    TestCfg0 = lists:keystore(GlobalMcdCfgKey, 1, Default,
                              {GlobalMcdCfgKey, NewMemcachedCfg}),

    %% Now, lets add connection_limit_mode to the node specific memcached
    %% config.
    NodeMcdCfgKey = {node, node(), memcached},
    {NodeMcdCfgKey, NodeMcdCfg} = lists:keyfind(NodeMcdCfgKey, 1, TestCfg0),
    NewNodeMcdCfg = lists:keystore(connection_limit_mode, 1, NodeMcdCfg,
                                   {connection_limit_mode, "disconnect"}),
    TestCfg1 = lists:keyreplace(NodeMcdCfgKey, 1, TestCfg0,
                                {NodeMcdCfgKey, NewNodeMcdCfg}),

    Txns = config_upgrade_to_79([TestCfg1]),

    {set, GlobalMcdCfgKey, UpgradedGlobalMcdCfg} =
        lists:keyfind(GlobalMcdCfgKey, 2, Txns),
    ?assertEqual(false,
                 lists:keyfind(connection_limit_mode, 1,
                               UpgradedGlobalMcdCfg)),

    {set, NodeMcdCfgKey, UpgradedNodeMcdCfg} =
        lists:keyfind(NodeMcdCfgKey, 2, Txns),
    ?assertEqual(false,
                 lists:keyfind(connection_limit_mode, 1, UpgradedNodeMcdCfg)).

upgrade_config_to_80_t() ->
    Default = ns_config_default:default(?VERSION_80),

    Txns0 = config_upgrade_to_80([Default]),
    ?assertEqual(
       [{set, memcached,
         [{magma_blind_write_optimisation_enabled, true}]}],
       Txns0),

    DefaultWithoutMcd = proplists:delete(memcached, Default),
    Txns1 = config_upgrade_to_80([DefaultWithoutMcd]),
    ?assertEqual(
       [{set, memcached,
         [{magma_blind_write_optimisation_enabled, true}]}], Txns1),

    meck:expect(ns_config, search_node_with_default,
                fun({_,magma_blind_write_optimisation_enabled}, _) ->
                        false
                end),

    Txns2 = config_upgrade_to_80([DefaultWithoutMcd]),
    ?assertEqual(
       [{set, memcached,
         [{magma_blind_write_optimisation_enabled, false}]}], Txns2).

unique_settings_test() ->
    try
        meck:new(cluster_compat_mode),
        meck:expect(cluster_compat_mode,  is_cluster_79,
                    fun () -> true end),
        Names = [N || {N, _} <- all_setting_names()],
        UniqueNames = lists:usort(Names),
        ?assertEqual(length(UniqueNames), length(Names))
    after
        meck:unload()
    end.

upgrade_config_test_() ->
    {setup, fun upgrade_config_test_setup/0,
            fun upgrade_config_test_teardown/1,
            [fun upgrade_config_from_76_to_79_t/0,
             fun upgrade_config_to_80_t/0]}.

-endif.
