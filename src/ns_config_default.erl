%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_config_default).

-include("ns_common.hrl").
-include("ns_config.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([default/0, upgrade_config/1, get_current_version/0, encrypt_and_save/1,
         decrypt/1, fixup/1, init_is_enterprise/0]).

-define(ISASL_PW, "isasl.pw").
-define(NS_LOG, "ns_log").
-define(EVENT_LOG, "event_log").

get_current_version() ->
    %% This function identifies the version of the config and one of its
    %% uses is during an offline upgrade.  A newer release will use this
    %% version to determine what upgrade operations need to take place.
    %% If the newer release doesn't know about this version it will not
    %% be able to complete the upgrade.  As an example, this version was
    %% changed in 6.0.4 after 6.5.0 had shipped.  As 6.5.0 had no knowledge
    %% of the 6.0.4 version (as it didn't exist when 6.5.0 shipped) it
    %% was unable to perform an upgrade.
    list_to_tuple(?VERSION_72).

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
    IsForced orelse misc:is_linux().

%% On Windows we need to specify a default je_malloc configuration value.
je_malloc_conf_default() ->
    case misc:is_windows() of
        true->
            "narenas:1";
        false ->
            undefined
    end.

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
    JeMallocConfDefault = je_malloc_conf_default(),

    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),

    {AuditGlobalLogs, AuditLocalLogs} =
        case misc:get_env_default(path_audit_log, []) of
            [] ->
                {[{log_path, LogDir}], []};
            Path ->
                {[], [{log_path, Path}]}
        end,

    ScramshaFallbackSalt = crypto:strong_rand_bytes(12),

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

     %% Default config for metakv settings in minimum supported version for
     %% the various services that use it.
     index_settings_manager:config_default(),
     eventing_settings_manager:config_default(),
     query_settings_manager:config_default(),
     analytics_settings_manager:config_default(),

     %% {rest_creds, {User, {password, {Salt, Mac}}}}
     %% {rest_creds, null} means no login/password auth check.
     {rest_creds, null},

     {client_cert_auth, [{state, "disable"}, {prefixes, []}]},
     {scramsha_fallback_salt, ScramshaFallbackSalt},

     {remote_clusters, []},
     {{node, node(), isasl}, [{path, filename:join(DataDir, ?ISASL_PW)}]},

     {audit,
      [{auditd_enabled, false},
       {rotate_interval, 86400},
       {rotate_size, 20*1024*1024},
       {disabled, []},
       {enabled, []},
       {disabled_users, []},
       {sync, []}] ++ AuditGlobalLogs},

     {{node, node(), audit}, AuditLocalLogs},

     {memcached, []},

     {{node, node(), memcached_defaults},
      [{max_connections, 65000},
       {system_connections, 5000},
       {connection_idle_time, 0},
       {verbosity, 0},
       {privilege_debug, false},
       {breakpad_enabled, true},
       %% Location that Breakpad should write minidumps upon memcached crash.
       {breakpad_minidump_dir_path, BreakpadMinidumpDir},
       {dedupe_nmvb_maps, false},
       {je_malloc_conf, JeMallocConfDefault},
       {tracing_enabled, IsEnterprise},
       {datatype_snappy, true},
       {num_reader_threads, <<"default">>},
       {num_writer_threads, <<"default">>},
       {num_auxio_threads, <<"default">>},
       {num_nonio_threads, <<"default">>},
       {num_storage_threads, <<"default">>},
       {connection_limit_mode, <<"disconnect">>},
       {free_connection_pool_size, 0},
       {tcp_keepalive_idle, 360},
       {tcp_keepalive_interval, 10},
       {tcp_keepalive_probes, 3}]},

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
                      "@eventing", "@cbas", "@backup"]},
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
       %% Milliseconds between log rotation runs.
       {log_rotation_period, 39003}]},

     {{node, node(), memcached_config},
      {[
        {interfaces, {memcached_config_mgr, get_interfaces, []}},
        {client_cert_auth, {memcached_config_mgr, client_cert_auth, []}},

        {connection_idle_time, connection_idle_time},
        {privilege_debug, privilege_debug},

        {breakpad,
         {[{enabled, breakpad_enabled},
           {minidump_dir, {memcached_config_mgr, get_minidump_dir, []}}]}},

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
        {enforce_tenant_limits_enabled,
         {memcached_config_mgr, should_enforce_limits, []}},
        {max_connections, max_connections},
        {system_connections, system_connections},
        {num_reader_threads, num_reader_threads},
        {num_writer_threads, num_writer_threads},
        {num_auxio_threads, num_auxio_threads},
        {num_nonio_threads, num_nonio_threads},
        {num_storage_threads, num_storage_threads},

        {logger,
         {[{filename, {"~s/~s", [log_path, log_prefix]}},
           {cyclesize, log_cyclesize}]}},

        {external_auth_service,
            {memcached_config_mgr, get_external_auth_service, []}},
        {active_external_users_push_interval,
            {memcached_config_mgr, get_external_users_push_interval, []}},
        {prometheus, {memcached_config_mgr, prometheus_cfg, []}},
        {connection_limit_mode, connection_limit_mode},
        {free_connection_pool_size, free_connection_pool_size},
        {tcp_keepalive_idle, tcp_keepalive_idle},
        {tcp_keepalive_interval, tcp_keepalive_interval},
        {tcp_keepalive_probes, tcp_keepalive_probes}
       ]}},

     {memory_quota, KvQuota},
     {fts_memory_quota, FTSQuota},
     {cbas_memory_quota, CBASQuota},

     {buckets, [{configs, []}]},

     %% Secure headers config
     {secure_headers, []},

     %% removed since 4.0
     {{node, node(), port_servers}, []},

     {{node, node(), ns_log}, [{filename, filename:join(DataDir, ?NS_LOG)}]},
     {{node, node(), event_log}, [{filename, filename:join(DataDir, ?EVENT_LOG)}]},

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
       {alerts, alert_keys_no_memory_check() },
       %% The alerts which should produce UI pop-ups.
       {pop_up_alerts, alert_keys_no_memory_check()}
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

     % auto-reprovision (mostly applicable to ephemeral buckets) is the operation that
     % is carried out when memcached process on a node restarts within the auto-failover
     % timeout.
     {auto_reprovision_cfg, [{enabled, true},
                             % max_nodes is the maximum number of nodes that may be
                             % automatically reprovisioned
                             {max_nodes, 1},
                             % count is the number of nodes that were auto-reprovisioned
                             {count, 0}]},

     {password_policy, [{min_length, 6}, {must_present, []}]}] ++
        service_ports:default_config(IsEnterprise) ++
        rebalance_quirks:default_config() ++
        auto_rebalance_settings:default_config() ++
        menelaus_web_auto_failover:default_config(IsEnterprise).

%% memory_threshold is excluded from alerts and pop_up_alerts here for
%% backward compatibility reasons (because it was added in a minor
%% release). It can be removed when memory_alert_email is added as
%% a proper alert (first major release after 7.1).
alert_keys_no_memory_check() ->
    menelaus_alert:alert_keys() -- [memory_threshold].

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
        {6,5} ->
            [{set, {node, node(), config_version}, {6,5,1}} |
             upgrade_config_from_6_5_to_6_5_1(Config)];
        {6,5,1} ->
                [{set, {node, node(), config_version}, {7,0}} |
                 upgrade_config_from_6_5_1_to_7_0(Config)];
        {7,0} ->
                [{set, {node, node(), config_version}, {7,1}} |
                 upgrade_config_from_7_0_to_7_1(Config)];
        {7,1} ->
            %% When upgrading to the latest config_version always upgrade
            %% service_ports.
            service_ports:offline_upgrade(Config) ++
                [{set, {node, node(), config_version}, CurrentVersion} |
                 upgrade_config_from_7_1_to_7_2(Config)];
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

upgrade_config_from_6_5_to_6_5_1(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_6_5_to_6_5_1(Config, DefaultConfig).

do_upgrade_config_from_6_5_to_6_5_1(Config, DefaultConfig) ->
    [upgrade_sub_keys(memcached, [admin_user], Config, DefaultConfig)].

upgrade_config_from_6_5_1_to_7_0(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_6_5_1_to_7_0(Config, DefaultConfig).

do_upgrade_config_from_6_5_1_to_7_0(Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig),
     upgrade_sub_keys(memcached, [other_users], Config, DefaultConfig)].

upgrade_config_from_7_0_to_7_1(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_7_0_to_7_1(Config, DefaultConfig).

do_upgrade_config_from_7_0_to_7_1(Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig),
     %% Do targeted upgrade of specific memcached keys/subkeys to preserve
     %% any custom changes that may have been made.
     upgrade_sub_keys(memcached, [{delete, log_sleeptime}],
                      Config, DefaultConfig)].

upgrade_config_from_7_1_to_7_2(Config) ->
    DefaultConfig = default(),
    do_upgrade_config_from_7_1_to_7_2(Config, DefaultConfig).

do_upgrade_config_from_7_1_to_7_2(_Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig)].

encrypt_config_val(Val) ->
    {ok, Encrypted} = encryption_service:encrypt(term_to_binary(Val)),
    {encrypted, Encrypted}.

encrypt(Config) ->
    misc:rewrite_tuples(fun ({admin_pass, Pass}) ->
                                {stop, {admin_pass, encrypt_config_val(Pass)}};
                            ({sasl_password, Pass}) ->
                                %% Maybe present on mixed version clusters.
                                %% The key is eventually deleted at some time
                                %% after compat mode is elevated to 7.0.
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

fixup(KV) ->
    dist_manager:fixup_config(KV).

-ifdef(TEST).
upgrade_6_5_to_6_5_1_test() ->
    Cfg = [[{some_key, some_value},
            {{node, node(), memcached}, [{old, info}, {admin_user, old}]}]],

    Default = [{{node, node(), memcached}, [{some, stuff}, {admin_user, new}]}],

    ?assertMatch([{set, {node, _, memcached}, [{old, info}, {admin_user, new}]}],
                 do_upgrade_config_from_6_5_to_6_5_1(Cfg, Default)).

upgrade_6_5_1_to_7_0_test() ->
    Cfg = [[{some_key, some_value},
            {{node, node(), memcached}, [{old, info}, {other_users, old}]},
            {{node, node(), memcached_defaults}, old_memcached_defaults},
            {{node, node(), memcached_config}, old_memcached_config}]],

    Default = [{{node, node(), memcached}, [{some, stuff}, {other_users, new}]},
               {{node, node(), memcached_defaults}, [{some, stuff},
                                                     {num_storage_threads, 4}]},
               {{node, node(), memcached_config}, new_memcached_config}],

    ?assertMatch([{set, {node, _, memcached_config}, new_memcached_config},
                  {set, {node, _, memcached_defaults},
                   [{some, stuff}, {num_storage_threads, 4}]},
                  {set, {node, _, memcached},
                   [{old, info}, {other_users, new}]}],
                 do_upgrade_config_from_6_5_1_to_7_0(Cfg, Default)).

no_upgrade_on_current_version_test() ->
    ?assertEqual([], upgrade_config([[{{node, node(), config_version}, get_current_version()}]])).

all_upgrades_test_() ->
    {setup,
     fun () ->
             ns_config:mock_tombstone_agent(),
             meck:new(sigar),
             meck:expect(sigar, get_cgroups_info,
                         fun () ->  #{supported => false} end)
     end,
     fun (_) ->
             meck:unload(sigar),
             ns_config:unmock_tombstone_agent()
     end,
     ?_test(test_all_upgrades())}.

test_all_upgrades() ->
    Default = default(),
    KVs = misc:update_proplist(Default,
                               [{{node, node(), config_version}, {6,5}}]),
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
