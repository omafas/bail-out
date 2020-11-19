%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
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
-module(ns_config_default).

-include("ns_common.hrl").
-include("ns_config.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([default/0, upgrade_config/1, get_current_version/0, encrypt_and_save/1, decrypt/1,
         init_is_enterprise/0]).

-define(ISASL_PW, "isasl.pw").
-define(NS_LOG, "ns_log").

get_current_version() ->
    {6,5,1}.

get_data_dir() ->
    RawDir = path_config:component_path(data),
    case misc:realpath(RawDir, "/") of
        {ok, X} -> X;
        _ -> RawDir
    end.

detect_enterprise_version(NsServerVersion) ->
    case re:run(NsServerVersion, <<"-enterprise$">>) of
        nomatch ->
            false;
        _ ->
            true
    end.

is_forced(EnvVar) ->
    case os:getenv(EnvVar) of
        false ->
            false;
        "0" ->
            false;
        _ ->
            true
    end.

init_is_enterprise() ->
    MaybeNsServerVersion =
        [V || {ns_server, _, V} <- application:loaded_applications()],
    case lists:any(fun (V) -> detect_enterprise_version(V) end, MaybeNsServerVersion) of
        true ->
            true;
        _ ->
            is_forced("FORCE_ENTERPRISE")
    end.

init_saslauthd_enabled() ->
    IsForced = is_forced("FORCE_SASLAUTHD"),
    IsLinux = os:type() =:= {unix, linux},

    IsForced orelse IsLinux.

default() ->
    DataDir = get_data_dir(),

    DefaultQuotas = memory_quota:default_quotas([kv, cbas, fts]),
    {_, KvQuota} = lists:keyfind(kv, 1, DefaultQuotas),
    {_, FTSQuota} = lists:keyfind(fts, 1, DefaultQuotas),
    {_, CBASQuota} = lists:keyfind(cbas, 1, DefaultQuotas),

    BreakpadMinidumpDir = path_config:minidump_dir(),
    ok = misc:mkdir_p(BreakpadMinidumpDir),

    IsEnterprise = init_is_enterprise(),
    SASLAuthdEnabled = init_saslauthd_enabled(),

    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),

    {AuditGlobalLogs, AuditLocalLogs} =
        case misc:get_env_default(path_audit_log, []) of
            [] ->
                {[{log_path, LogDir}], []};
            Path ->
                {[], [{log_path, Path}]}
        end,

    [{{node, node(), config_version}, get_current_version()},
     {directory, path_config:component_path(data, "config")},
     {{node, node(), is_enterprise}, IsEnterprise},
     {{node, node(), saslauthd_enabled}, SASLAuthdEnabled},
     {index_aware_rebalance_disabled, false},
     {max_bucket_count, ?MAX_BUCKETS_SUPPORTED},
     {autocompaction, [{database_fragmentation_threshold, {30, undefined}},
                       {view_fragmentation_threshold, {30, undefined}}]},
     {set_view_update_daemon,
      [{update_interval, 5000},
       {update_min_changes, 5000},
       {replica_update_min_changes, 5000}]},
     {{node, node(), compaction_daemon}, [{check_interval, 30},
                                          {min_db_file_size, 131072},
                                          {min_view_file_size, 20 * 1024 * 1024}]},
     {nodes_wanted, [node()]},
     {quorum_nodes, [node()]},
     {server_groups, [[{uuid, <<"0">>},
                       {name, <<"Group 1">>},
                       {nodes, [node()]}]]},
     {{node, node(), membership}, active},
     %% In general, the value in these key-value pairs are property lists,
     %% like [{prop_atom1, value1}, {prop_atom2, value2}].
     %%
     %% See the proplists erlang module.
     %%
     %% A change to any of these rest properties probably means a restart of
     %% mochiweb is needed.
     %%
     %% Modifiers: menelaus REST API
     %% Listeners: some menelaus module that configures/reconfigures mochiweb
     {{couchdb, max_parallel_indexers}, 4},
     {{couchdb, max_parallel_replica_indexers}, 2},

     %% Default config for metakv index settings in minimum supported version,
     index_settings_manager:config_default(),

     %% {rest_creds, {User, {password, {Salt, Mac}}}}
     %% {rest_creds, null} means no login/password auth check.
     {rest_creds, null},

     {remote_clusters, []},
     {{node, node(), isasl}, [{path, filename:join(DataDir, ?ISASL_PW)}]},

     {audit,
      [{auditd_enabled, false},
       {rotate_interval, 86400},
       {rotate_size, 20*1024*1024},
       {disabled, []},
       {sync, []}] ++ AuditGlobalLogs},

     {{node, node(), audit}, AuditLocalLogs},

     {memcached, []},

     {{node, node(), memcached_defaults},
      [{max_connections, 65000},
       {system_connections, 5000},
       {connection_idle_time, 0},
       {verbosity, 0},
       {privilege_debug, false},
       {opentracing_enabled, false},
       {opentracing_module, ""},
       {opentracing_config, ""},
       {breakpad_enabled, true},
       %% Location that Breakpad should write minidumps upon memcached crash.
       {breakpad_minidump_dir_path, BreakpadMinidumpDir},
       {dedupe_nmvb_maps, false},
       {tracing_enabled, IsEnterprise},
       {datatype_snappy, true},
       {num_reader_threads, <<"default">>},
       {num_writer_threads, <<"default">>}]},

     %% Memcached config
     {{node, node(), memcached},
      [{port, service_ports:default(memcached_port, IsEnterprise)},
       {dedicated_port,
        service_ports:default(memcached_dedicated_port, IsEnterprise)},
       {dedicated_ssl_port,
        service_ports:default(memcached_dedicated_ssl_port, IsEnterprise)},
       {ssl_port, service_ports:default(memcached_ssl_port, IsEnterprise)},
       {admin_user, "@ns_server"},
       {other_users, ["@cbq-engine", "@projector", "@goxdcr", "@index", "@fts",
                      "@eventing", "@cbas"]},
       {admin_pass, binary_to_list(couch_uuids:random())},
       {engines,
        [{membase,
          [{engine, path_config:component_path(lib, "memcached/ep.so")},
           {static_config_string,
            "failpartialwarmup=false"}]},
         {memcached,
          [{engine,
            path_config:component_path(lib, "memcached/default_engine.so")},
           {static_config_string, "vb0=true"}]}]},
       {config_path, path_config:default_memcached_config_path()},
       {audit_file, ns_audit_cfg:default_audit_json_path()},
       {rbac_file, filename:join(path_config:component_path(data, "config"), "memcached.rbac")},
       {log_path, LogDir},
       %% Prefix of the log files within the log path that should be rotated.
       {log_prefix, "memcached.log"},
       %% Number of recent log files to retain.
       {log_generations, 20},
       %% how big log file needs to grow before memcached starts using
       %% next file
       {log_cyclesize, 1024*1024*10},
       %% flush interval of memcached's logger in seconds
       {log_sleeptime, 19},
       %% Milliseconds between log rotation runs.
       {log_rotation_period, 39003}]},

     {{node, node(), memcached_config},
      {[
        {interfaces, {memcached_config_mgr, get_interfaces, []}},
        {ssl_cipher_list, {memcached_config_mgr, get_ssl_cipher_list, []}},
        {ssl_cipher_order, {memcached_config_mgr, get_ssl_cipher_order, []}},
        {client_cert_auth, {memcached_config_mgr, client_cert_auth, []}},
        {ssl_minimum_protocol, {memcached_config_mgr, ssl_minimum_protocol, []}},

        {connection_idle_time, connection_idle_time},
        {privilege_debug, privilege_debug},

        {breakpad,
         {[{enabled, breakpad_enabled},
           {minidump_dir, {memcached_config_mgr, get_minidump_dir, []}}]}},

        %% OpenTracing configuration
        {opentracing,
         {[{enabled, opentracing_enabled},
           {module, {"~s", [opentracing_module]}},
           {config, {"~s", [opentracing_config]}}
           ]}},

        {admin, {"~s", [admin_user]}},

        {verbosity, verbosity},
        {audit_file, {"~s", [audit_file]}},
        {rbac_file, {"~s", [rbac_file]}},
        {dedupe_nmvb_maps, dedupe_nmvb_maps},
        {tracing_enabled, tracing_enabled},
        {datatype_snappy, {memcached_config_mgr, is_snappy_enabled, []}},
        {xattr_enabled, true},
        {scramsha_fallback_salt, {memcached_config_mgr, get_fallback_salt, []}},
        {collections_enabled, {memcached_config_mgr, collections_enabled, []}},
        {max_connections, max_connections},
        {system_connections, system_connections},
        {num_reader_threads, num_reader_threads},
        {num_writer_threads, num_writer_threads},

        {logger,
         {[{filename, {"~s/~s", [log_path, log_prefix]}},
           {cyclesize, log_cyclesize},
           {sleeptime, log_sleeptime}]}},

        {external_auth_service,
            {memcached_config_mgr, get_external_auth_service, []}},
        {active_external_users_push_interval,
            {memcached_config_mgr, get_external_users_push_interval, []}}
       ]}},

     {memory_quota, KvQuota},
     {fts_memory_quota, FTSQuota},
     {cbas_memory_quota, CBASQuota},

     {buckets, [{configs, []}]},

     %% Secure headers config
     {secure_headers, []},

     %% This is needed, because we want REST API's on pre 6.5 nodes
     %% to return proxy as 0, not as "undefined", so it doesn't break golang
     %% components on those nodes, which expect numerical value
     %% To be removed after the support of pre 6.5 nodes will be
     %% discontinued
     {{node, node(), moxi}, [{port, 0}]},

     %% removed since 4.0
     {{node, node(), port_servers}, []},

     {{node, node(), ns_log}, [{filename, filename:join(DataDir, ?NS_LOG)}]},

     %% Modifiers: menelaus
     %% Listeners: ? possibly ns_log
     {email_alerts,
      [{recipients, ["root@localhost"]},
       {sender, "couchbase@localhost"},
       {enabled, false},
       {email_server, [{user, ""},
                       {pass, ""},
                       {host, "localhost"},
                       {port, 25},
                       {encrypt, false}]},
       {alerts, [auto_failover_node, auto_failover_maximum_reached,
                 auto_failover_other_nodes_down,
                 auto_failover_cluster_too_small, auto_failover_disabled,
                 ip, disk, overhead, ep_oom_errors, ep_item_commit_failed,
                 audit_dropped_events, indexer_ram_max_usage,
                 ep_clock_cas_drift_threshold_exceeded,
                 communication_issue]}
      ]},
     {alert_limits, [
       %% Maximum percentage of overhead compared to max bucket size (%)
       {max_overhead_perc, 50},
       %% Maximum disk usage before warning (%)
       {max_disk_used, 90},
       %% Maximum Indexer RAM Usage before warning (%)
       {max_indexer_ram, 75}
      ]},
     {replication, [{enabled, true}]},
     {log_redaction_default_cfg, [{redact_level, none}]},

     {auto_failover_cfg, [{enabled, true},
                          % timeout is the time (in seconds) a node needs to be
                          % down before it is automatically faileovered
                          {timeout, 120},
                          % max_nodes is the maximum number of nodes that may be
                          % automatically failovered
                          {max_nodes, 1},
                          % count is the number of nodes that were auto-failovered
                          {count, 0}]},
     % auto-reprovision (mostly applicable to ephemeral buckets) is the operation that
     % is carried out when memcached process on a node restarts within the auto-failover
     % timeout.
     {auto_reprovision_cfg, [{enabled, true},
                             % max_nodes is the maximum number of nodes that may be
                             % automatically reprovisioned
                             {max_nodes, 1},
                             % count is the number of nodes that were auto-reprovisioned
                             {count, 0}]},

     %% everything is unlimited by default
     {{request_limit, rest}, undefined},
     {{request_limit, capi}, undefined},
     {drop_request_memory_threshold_mib, undefined},
     {password_policy, [{min_length, 6}, {must_present, []}]}] ++
        service_ports:default_config(IsEnterprise) ++
        rebalance_quirks:default_config().

%% returns list of changes to config to upgrade it to current version.
%% This will be invoked repeatedly by ns_config until list is empty.
%%
%% NOTE: API-wise we could return new config but that would require us
%% to handle vclock updates
-spec upgrade_config([[{term(), term()}]]) -> [{set, term(), term()}].
upgrade_config(Config) ->
    CurrentVersion = get_current_version(),
    ConfigVersion = ns_config:search_node_with_default(node(), Config,
                                                       config_version, {1, 7}),
    assert_not_developer_preview(CurrentVersion, ConfigVersion, Config),
    case ConfigVersion of
        CurrentVersion ->
            [];
        {5,0} ->
            [{set, {node, node(), config_version}, {5,1,1}} |
             upgrade_config_from_5_0_to_5_1_1()];
        {5,1,1} ->
            [{set, {node, node(), config_version}, {5,5}} |
             upgrade_config_from_5_1_1_to_5_5(Config)];
        {5,5} ->
            [{set, {node, node(), config_version}, {5,5,3}} |
             upgrade_config_from_5_5_to_5_5_3()];
        {5,5,3} ->
            [{set, {node, node(), config_version}, {6,0,4}} |
             upgrade_config_from_5_5_3_to_6_0_4(Config)];
        {6,0,4} ->
            [{set, {node, node(), config_version}, {6,5}} |
             upgrade_config_from_6_0_4_to_6_5(Config)];
        {6,5} ->
            [{set, {node, node(), config_version}, CurrentVersion} |
             upgrade_config_from_6_5_to_6_5_1(Config)];
        OldVersion ->
            ?log_error("Detected an attempt to offline upgrade from "
                       "unsupported version ~p. Terminating.", [OldVersion]),
            catch ale:sync_all_sinks(),
            misc:halt(1)
    end.

assert_not_developer_preview(CurrentVsn, ConfigVsn, Config) ->
    case cluster_compat_mode:is_developer_preview(Config) of
        false -> ok;
        true when CurrentVsn == ConfigVsn -> ok;
        true ->
            ?log_error("Can't offline upgrade from a developer preview cluster"),
            catch ale:sync_all_sinks(),
            misc:halt(1)
    end.

upgrade_key(Key, DefaultConfig) ->
    WholeKey = {node, node(), Key},
    {value, Value} = ns_config:search([DefaultConfig], WholeKey),
    {set, WholeKey, Value}.

upgrade_sub_keys(Key, SubKeys, Config, DefaultConfig) ->
    WholeKey = {node, node(), Key},
    {value, DefaultVal} = ns_config:search([DefaultConfig], WholeKey),
    {value, CurrentVal} = ns_config:search(Config, WholeKey),
    {set, WholeKey, do_upgrade_sub_keys(SubKeys, CurrentVal, DefaultVal)}.

do_upgrade_sub_keys(SubKeys, {Json}, {DefaultJson}) ->
    {do_upgrade_sub_keys(SubKeys, Json, DefaultJson)};
do_upgrade_sub_keys(SubKeys, Props, DefaultProps) ->
    lists:foldl(
      fun ({delete, SubKey}, Acc) ->
              lists:keydelete(SubKey, 1, Acc);
          (SubKey, Acc) ->
              Val = {SubKey, _} = lists:keyfind(SubKey, 1, DefaultProps),
              lists:keystore(SubKey, 1, Acc, Val)
      end, Props, SubKeys).

rename_key(OldKey, NewKey, Config) ->
    WholeOldKey = {node, node(), OldKey},
    WholeNewKey = {node, node(), NewKey},
    case ns_config:search(Config, WholeOldKey) of
        {value, Val} ->
            [{delete, WholeOldKey},
             {set, WholeNewKey, Val}];
        false ->
            []
    end.

upgrade_config_from_5_0_to_5_1_1() ->
    rebalance_quirks:upgrade_config_project_intact_patched().

upgrade_config_from_5_1_1_to_5_5(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_5_1_1_to_5_5(Config, DefaultConfig).

do_upgrade_config_from_5_1_1_to_5_5(Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig),
     upgrade_sub_keys(memcached, [other_users], Config, DefaultConfig)].

upgrade_config_from_5_5_to_5_5_3() ->
    DefaultConfig = default(),
    do_upgrade_config_from_5_5_to_5_5_3(DefaultConfig).

do_upgrade_config_from_5_5_to_5_5_3(DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig)].

upgrade_config_from_5_5_3_to_6_0_4(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_5_5_3_to_6_0_4(Config, DefaultConfig).

do_upgrade_config_from_5_5_3_to_6_0_4(Config, DefaultConfig) ->
    [upgrade_sub_keys(memcached, [admin_user], Config, DefaultConfig)].

upgrade_config_from_6_0_4_to_6_5(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_6_0_4_to_6_5(Config, DefaultConfig).

do_upgrade_config_from_6_0_4_to_6_5(Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig),
     upgrade_sub_keys(memcached, [dedicated_ssl_port],
                      Config, DefaultConfig),
     upgrade_key(moxi, DefaultConfig) |
     rename_key(ldap_enabled, saslauthd_enabled, Config)].

upgrade_config_from_6_5_to_6_5_1(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_6_5_to_6_5_1(Config, DefaultConfig).

do_upgrade_config_from_6_5_to_6_5_1(Config, DefaultConfig) ->
    [upgrade_sub_keys(memcached, [admin_user], Config, DefaultConfig)].

encrypt_config_val(Val) ->
    {ok, Encrypted} = encryption_service:encrypt(term_to_binary(Val)),
    {encrypted, Encrypted}.

encrypt(Config) ->
    misc:rewrite_tuples(fun ({admin_pass, Pass}) ->
                                {stop, {admin_pass, encrypt_config_val(Pass)}};
                            ({sasl_password, Pass}) ->
                                {stop, {sasl_password, encrypt_config_val(Pass)}};
                            ({metakv_sensitive, Val}) ->
                                {stop, {metakv_sensitive, encrypt_config_val(Val)}};
                            ({cookie, Cookie}) ->
                                {stop, {cookie, encrypt_config_val(Cookie)}};
                            ({pass, Pass}) ->
                                {stop, {pass, encrypt_config_val(Pass)}};
                            ({password, Pass}) ->
                                {stop, {password, encrypt_config_val(Pass)}};
                            (_) ->
                                continue
                        end, Config).

encrypt_and_save(Config) ->
    {value, DirPath} = ns_config:search(Config, directory),
    Dynamic = ns_config:get_kv_list_with_config(Config),
    case cluster_compat_mode:is_enterprise() of
        true ->
            {ok, DataKey} = encryption_service:get_data_key(),
            EncryptedConfig = encrypt(Dynamic),
            ns_config:save_config_sync([EncryptedConfig], DirPath),
            encryption_service:maybe_clear_backup_key(DataKey);
        false ->
            ns_config:save_config_sync([Dynamic], DirPath)
    end.

decrypt(Config) ->
    misc:rewrite_tuples(fun ({encrypted, Val}) when is_binary(Val) ->
                                {ok, Decrypted} = encryption_service:decrypt(Val),
                                {stop, binary_to_term(Decrypted)};
                            (_) ->
                                continue
                        end, Config).


-ifdef(TEST).
upgrade_5_1_1_to_5_5_test() ->
    Cfg = [[{some_key, some_value},
            {{node, node(), memcached}, [{old, info}, {other_users, old}]},
            {{node, node(), memcached_defaults}, old_memcached_defaults},
            {{node, node(), memcached_config}, old_memcached_config}]],

    Default = [{{node, node(), memcached}, [{some, stuff}, {other_users, new}]},
               {{node, node(), memcached_defaults}, [{some, stuff},
                                                     {new_field, enable}]},
               {{node, node(), memcached_config}, new_memcached_config}],

    ?assertMatch([{set, {node, _, memcached_config}, new_memcached_config},
                  {set, {node, _, memcached_defaults}, [{some, stuff},
                                                        {new_field, enable}]},
                  {set, {node, _, memcached}, [{old, info}, {other_users, new}]}],
                 do_upgrade_config_from_5_1_1_to_5_5(Cfg, Default)).

upgrade_5_5_to_5_5_3_test() ->
    Default = [{some_key, some_other_value},
               {{node, node(), memcached}, [{some, stuff}, {other_users, new}]},
               {{node, node(), memcached_config}, new_memcached_config}],

    ?assertMatch([{set, {node, _, memcached_config}, new_memcached_config}],
                 do_upgrade_config_from_5_5_to_5_5_3(Default)).

upgrade_5_5_3_to_6_0_4_test() ->
    Cfg = [[{some_key, some_value},
            {{node, node(), memcached}, [{old, info}, {admin_user, old}]}]],

    Default = [{{node, node(), memcached}, [{some, stuff}, {admin_user, new}]}],

    ?assertMatch([{set, {node, _, memcached}, [{old, info}, {admin_user, new}]}],
                 do_upgrade_config_from_5_5_3_to_6_0_4(Cfg, Default)).

upgrade_6_0_4_to_6_5_test() ->
    Cfg1 = [[{some_key, some_value},
             {{node, node(), memcached}, [{old, info}]},
             {{node, node(), memcached_defaults}, [{k1, v1}]},
             {{node, node(), memcached_config}, [{interfaces,
                                                  [{[{x, y},
                                                     {maxconn, 1}]}]}]},
             {{node, node(), ldap_enabled}, true}]],
    Default = [{{node, node(), memcached_config}, [{interfaces,
                                                    [{[{x, y}]}]}]},
               {{node, node(), memcached}, [{some, stuff},
                                            {dedicated_ssl_port, 123}]},
               {{node, node(), memcached_defaults}, [{k1, v1}, {k2, v2}]},
               {{node, node(), moxi}, new_moxi_value}],

    ?assertMatch([{set, {node, _, memcached_config}, [{interfaces,
                                                       [{[{x, y}]}]}]},
                  {set, {node, _, memcached_defaults}, [{k1, v1}, {k2, v2}]},
                  {set, {node, _, memcached}, [{old, info},
                                               {dedicated_ssl_port, 123}]},
                  {set, {node, _, moxi}, new_moxi_value},
                  {delete, {node, _, ldap_enabled}},
                  {set, {node, _, saslauthd_enabled}, true}],
                 do_upgrade_config_from_6_0_4_to_6_5(Cfg1, Default)),
    Cfg2 = [[{some_key, some_value},
             {{node, node(), memcached}, [{old, info}]}]],
    ?assertMatch([{set, {node, _, memcached_config}, [{interfaces,
                                                       [{[{x, y}]}]}]},
                  {set, {node, _, memcached_defaults}, [{k1, v1}, {k2, v2}]},
                  {set, {node, _, memcached}, [{old, info},
                                               {dedicated_ssl_port, 123}]},
                  {set, {node, _, moxi}, new_moxi_value}],
                 do_upgrade_config_from_6_0_4_to_6_5(Cfg2, Default)).

upgrade_6_5_to_6_5_1_test() ->
    Cfg = [[{some_key, some_value},
            {{node, node(), memcached}, [{old, info}, {admin_user, old}]}]],

    Default = [{{node, node(), memcached}, [{some, stuff}, {admin_user, new}]}],

    ?assertMatch([{set, {node, _, memcached}, [{old, info}, {admin_user, new}]}],
                 do_upgrade_config_from_6_5_to_6_5_1(Cfg, Default)).

no_upgrade_on_current_version_test() ->
    ?assertEqual([], upgrade_config([[{{node, node(), config_version}, get_current_version()}]])).

all_upgrades_test() ->
    Default = default(),
    KVs = misc:update_proplist(Default,
                               [{{node, node(), config_version}, {5,0}}]),
    Cfg = #config{dynamic = [KVs], uuid = <<"uuid">>},
    UpgradedCfg = ns_config:upgrade_config(Cfg, fun upgrade_config/1),

    UpgradedKVs = [{K, ns_config:strip_metadata(V)} ||
                      {K, V} <- hd(UpgradedCfg#config.dynamic)],

    ?assertEqual([], UpgradedKVs -- Default),
    ?assertEqual([], Default -- UpgradedKVs).

%% dialyzer proves that statically and complains about impossible code
%% path if I use ?assert... Sucker
detect_enterprise_version_test() ->
    true = detect_enterprise_version(<<"1.8.0r-9-ga083a1e-enterprise">>),
    true = not detect_enterprise_version(<<"1.8.0r-9-ga083a1e-comm">>).
-endif.
