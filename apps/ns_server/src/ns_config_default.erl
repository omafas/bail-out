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
-export([ns_config_default_mock_setup/0,
         ns_config_default_mock_teardown/1]).
-endif.

-export([default/1, upgrade_config/1, get_current_version/0, encrypt_and_save/2,
         decrypt/1, fixup/1, init_is_enterprise/0, generate_internal_pass/0]).

-define(ISASL_PW, "isasl.pw").
-define(NS_LOG, "ns_log").
-define(EVENT_LOG, "event_log").
-define(DEFAULT_AUDIT_PRUNE_INTERVAL, 0).

get_current_version() ->
    %% This function identifies the version of the config and one of its
    %% uses is during an offline upgrade.  A newer release will use this
    %% version to determine what upgrade operations need to take place.
    %% If the newer release doesn't know about this version it will not
    %% be able to complete the upgrade.  As an example, this version was
    %% changed in 6.0.4 after 6.5.0 had shipped.  As 6.5.0 had no knowledge
    %% of the 6.0.4 version (as it didn't exist when 6.5.0 shipped) it
    %% was unable to perform an upgrade.
    list_to_tuple(?LATEST_VERSION_NUM).

get_min_supported_version() ->
    list_to_tuple(?MIN_SUPPORTED_VERSION).

get_data_dir() ->
    RawDir = path_config:component_path(data),
    case misc:realpath(RawDir, "/") of
        {ok, X} -> X;
        _ -> RawDir
    end.

detect_enterprise_version(NsServerVersion) ->
    case re:run(NsServerVersion, <<"-enterprise(-analytics)?$">>) of
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

%% Some settings in default() reflect those of LATEST_VERSION_NUM:
%% - database_dir, index_dir, memcached keys (node-specific settings)
%% Some settings in default() reflect those of MIN_SUPPORTED_VERSION:
%% - (analytics|query|index)_settings_manager (cluster-wide settings)
%%
%% upgrade_config_from_X_to_Y paths in this file are "offline" upgrades. They
%% update settings when the node version changes (at start).
%% config_upgrade_to_X paths in ns_online_config_upgrader are "online" upgrades.
%% They occur when the cluster is online and cluster_compat_version changes.
%%
%% Cluster-wide settings:
%% cluster_init starts off with compat_version undefined. It upgrades ns_config
%% starting from minimum supported version, cycles through each supported
%% version until it reaches latest version. This is to arrive at the maximum
%% mutually agreed upon supported version among all nodes in the cluster. As
%% cluster_compat_version goes up, ns_config is updated using
%% ns_online_config_upgrader:config_upgrade_to_X functions.
%% If setting A is introduced in latest_version (here in defaults()), a node
%% running latest_version is already up to date in its config, with respect to
%% A. The node may however belong to a cluster with compat_version < latest. A
%% change in cluster_compat_version to latest will call config_upgrade_to_latest
%% and can attempt to add the setting A already present in its config. This path
%% is exercised during cluster init (undefined -> min supported > ... > latest).
%% This needs to be accounted for by either: initializing settings to min
%% supported here and adding strictly new settings during upgrades (see
%% index_settings_manager), or handling settings already present during upgrades
%% correctly.
%%
%% Node-specific settings:
%% Similarly, it is possible that a customer is already running with certain
%% settings specified in memcached keys. During an offline upgrade, it is
%% necessary to account for settings that were previously configured and retain
%% them as needed, account for duplicates.
%%
%% TLDR: In any upgrade path, it is safer to assume a setting already exists to
%% - avoid losing a previously configured setting
%% - prevent duplicates
default(Vsn) ->
    DataDir = get_data_dir(),

    DefaultQuotas = memory_quota:default_quotas([kv, cbas, fts], Vsn),
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
     {max_bucket_count, config_profile:get_value(max_buckets_supported,
                                                 ?MAX_BUCKETS_SUPPORTED)},
     {set_view_update_daemon,
      [{update_interval, 5000},
       {update_min_changes, 5000},
       {replica_update_min_changes, 5000}]},
     {{node, node(), compaction_daemon}, [{check_interval, 30},
                                          {min_db_file_size, 131072},
                                          {min_view_file_size, 20 * 1024 * 1024}]},
     {nodes_wanted, []},
     {quorum_nodes, [node()]},
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
       {prune_age, ?DEFAULT_AUDIT_PRUNE_INTERVAL},
       {disabled, []},
       {enabled, []},
       {disabled_users, []},
       {sync, []}] ++ AuditGlobalLogs},

     {{node, node(), audit}, AuditLocalLogs},

     %% The {node, node(), memcached}, memcached and
     %% {node, node(), memcached_defaults} keys are parameters that are
     %% used to set the values of the keys in memcached.json.
     %%
     %% There’s a pecking order. Parameter values in the per-node memcached
     %% key override the global memcached key which override the per-node
     %% memcached_defaults. (There are no global memcached defaults.)

     {memcached, []},

     {{node, node(), memcached_defaults},
      [{max_connections, 65000},
       {system_connections, 5000},
       {connection_idle_time, 0},
       {verbosity, 0},
       {breakpad_enabled, true},
       %% Location that Breakpad should write minidumps upon memcached crash.
       {breakpad_minidump_dir_path, BreakpadMinidumpDir},

       %% Configuration profile
       {dedupe_nmvb_maps, false},
       {je_malloc_conf, JeMallocConfDefault},
       {tracing_enabled, IsEnterprise},
       {datatype_snappy, true},
       {num_reader_threads,
        config_profile:get_value(num_reader_threads, <<"balanced">>)},
       {num_writer_threads,
        config_profile:get_value(num_writer_threads, <<"balanced">>)},
       {num_auxio_threads, <<"default">>},
       {num_nonio_threads, <<"default">>},
       {num_storage_threads, <<"default">>},
       {tcp_keepalive_idle, 360},
       {tcp_keepalive_interval, 10},
       {tcp_keepalive_probes, 3},
       {tcp_user_timeout, 30},
       {free_connection_pool_size, 0},
       {max_client_connection_details, 0},
       {fusion_migration_rate_limit, 1024 * 1024 * 75},
       {fusion_sync_rate_limit, 1024 * 1024 * 75},
       {dcp_consumer_max_marker_version, <<"2.2">>},
       {dcp_snapshot_marker_hps_enabled, true},
       {dcp_snapshot_marker_purge_seqno_enabled, true}]},

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
       {admin_pass, {v2, [generate_internal_pass()]}},
       {engines,
        [{membase,
          [{engine, path_config:component_path(lib, "memcached/ep.so")},
           {static_config_string, ""}]},
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

     %% This section defines the "schema" for the memcached.json file. Each
     %% of the keys ends up as a key in the JSON object in the file.

     {{node, node(), memcached_config},
      {[
        {interfaces, {memcached_config_mgr, get_interfaces, []}},
        {client_cert_auth, {memcached_config_mgr, client_cert_auth, []}},

        {connection_idle_time, connection_idle_time},

        {breakpad,
         {[{enabled, breakpad_enabled},
           {minidump_dir, {memcached_config_mgr, get_minidump_dir, []}}]}},

        {deployment_model, {memcached_config_mgr, get_config_profile, []}},
        {verbosity, verbosity},
        {audit_file, {"~s", [audit_file]}},
        {rbac_file, {"~s", [rbac_file]}},
        {dedupe_nmvb_maps, dedupe_nmvb_maps},
        {tracing_enabled, tracing_enabled},
        {datatype_snappy, {memcached_config_mgr, is_snappy_enabled, []}},
        {xattr_enabled, true},
        {scramsha_fallback_salt, {memcached_config_mgr, get_fallback_salt, []}},
        {scramsha_fallback_iteration_count,
         {memcached_config_mgr, get_scram_fallback_iter_count, []}},
        {collections_enabled, true},
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
        {sasl_mechanisms, {memcached_config_mgr, sasl_mechanisms, []}},
        {ssl_sasl_mechanisms, {memcached_config_mgr, ssl_sasl_mechanisms, []}},
        {tcp_keepalive_idle, tcp_keepalive_idle},
        {tcp_keepalive_interval, tcp_keepalive_interval},
        {tcp_keepalive_probes, tcp_keepalive_probes},
        {tcp_user_timeout, tcp_user_timeout},
        {free_connection_pool_size, free_connection_pool_size},
        {max_client_connection_details, max_client_connection_details},
        {fusion_migration_rate_limit, fusion_migration_rate_limit},
        {fusion_sync_rate_limit, fusion_sync_rate_limit},
        {dcp_consumer_max_marker_version, dcp_consumer_max_marker_version},
        {dcp_snapshot_marker_hps_enabled, dcp_snapshot_marker_hps_enabled},
        {dcp_snapshot_marker_purge_seqno_enabled,
         dcp_snapshot_marker_purge_seqno_enabled}
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
       {alerts, menelaus_alert:alert_keys() --
            %% Disabled by default:
            menelaus_alert:alert_keys_disabled_by_default()},
       %% The alerts which should produce UI pop-ups.
       {pop_up_alerts, menelaus_alert:alert_keys() --
            %% Disabled by default:
            menelaus_alert:alert_keys_disabled_by_default()}
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

     {service_orchestrator_weight, ?DEFAULT_SERVICE_WEIGHTS},
     {health_monitor_refresh_interval, []},
     {password_policy, [{min_length, 6}, {must_present, []}]}] ++
        service_ports:default_config(IsEnterprise) ++
        rebalance_quirks:default_config() ++
        auto_rebalance_settings:default_config() ++
        menelaus_web_auto_failover:default_config(IsEnterprise) ++
        ns_storage_conf:default_config() ++
        [{resource_management,
          menelaus_web_guardrails:default_for_ns_config()}] ++
        [{user_activity, menelaus_web_activity:default()}].

stop_if_memcached_buckets_in_use() ->
    ?log_debug("Checking to see if memcached buckets are being used."),
    case ns_bucket:memcached_buckets_in_use() of
        true ->
            %% Stop running before anything is changed which would prevent
            %% rebooting the older release that still supports memcached
            %% buckets.
            do_remote_stop(),
            exit(memcached_buckets_present);
        false ->
            ok
    end.

%% Do a remote_stop on a separate process in order to not deadlock with
%% the shutdown.
do_remote_stop() ->
    BabysitterNode = ns_server:get_babysitter_node(),
    Msg = "Unable to start when memcached buckets are configured.  Node is "
          "being stopped. The prior, older, version of Couchbase server can "
          "still be used.",
    ?log_error(Msg),
    Pid = proc_lib:spawn_link(
            fun () ->
                    rpc:call(BabysitterNode, io, put_chars,
                             [standard_error, Msg]),
                    ns_babysitter_bootstrap:remote_stop(BabysitterNode)
            end),
    ?log_error("Started remote_stop process: ~p", [Pid]).

%% returns list of changes to config to upgrade it to current version.
%% This will be invoked repeatedly by ns_config until list is empty.
%%
%% NOTE: API-wise we could return new config but that would require us
%% to handle vclock updates
-spec upgrade_config([[{term(), term()}]]) -> [{set, term(), term()}].
upgrade_config(Config) ->
    CurrentVersion = get_current_version(),
    UnsupportedVersion = {0, 0},
    ConfigVersion = ns_config:search_node_with_default(node(), Config,
                                                       config_version,
                                                       UnsupportedVersion),
    assert_not_developer_preview(CurrentVersion, ConfigVersion, Config),
    MinVersion = get_min_supported_version(),

    case CurrentVersion =:= ConfigVersion of
        true ->
            ok;
        false ->
            %% If there's memcached buckets in use we don't want to come up
            %% and we don't want to change anything that would prevent the
            %% older release from coming up.
            stop_if_memcached_buckets_in_use()
    end,

    case ConfigVersion of
        CurrentVersion ->
            [];
        MinVersion ->
            [{set, {node, node(), config_version}, {7,2}} |
             upgrade_config_from_7_1_to_7_2(Config)];
        {7,2} ->
            [{set, {node, node(), config_version}, {7,6}} |
             upgrade_config_from_7_2_to_76(Config)];
        {7,6} ->
            [{set, {node, node(), config_version}, {7,9}} |
             upgrade_config_from_76_to_79(Config)];
        {7,9} ->
            %% When upgrading to the latest config_version always upgrade
            %% service_ports.
            service_ports:offline_upgrade(Config) ++
                %% Note, we explicitly set the config version to the actual next
                %% version, not CurrentVersion, to ensure that if we forget to
                %% upgrade this function, it causes a test failure.
                %% Otherwise, when we update get_current_version/0, the unit
                %% test will still pass, despite the fact that offline upgrades
                %% from the version immediately prior to CurrentVersion would
                %% not actually be allowed.
                [{set, {node, node(), config_version}, {8,0}}];
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

-compile([{nowarn_unused_function, [{upgrade_sub_keys, 4},
                                    {do_upgrade_sub_keys, 3}]}]).
%% we use it to upgrade memcached key. it just happens that we don't
%% need to upgrade this key in latest upgrades
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

upgrade_config_from_7_1_to_7_2(Config) ->
    DefaultConfig = default(?VERSION_72),
    do_upgrade_config_from_7_1_to_7_2(Config, DefaultConfig).

do_upgrade_config_from_7_1_to_7_2(_Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig),
     upgrade_key(database_dir, DefaultConfig),
     upgrade_key(index_dir, DefaultConfig)].

upgrade_config_from_7_2_to_76(Config) ->
    DefaultConfig = default(?VERSION_76),
    do_upgrade_config_from_7_2_to_76(Config, DefaultConfig).

do_upgrade_config_from_7_2_to_76(_Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig)].

upgrade_config_from_76_to_79(Config) ->
    DefaultConfig = default(?VERSION_79),
    do_upgrade_config_from_76_to_79(Config, DefaultConfig).

do_upgrade_config_from_76_to_79(_Config, DefaultConfig) ->
    [upgrade_key(memcached_config, DefaultConfig),
     upgrade_key(memcached_defaults, DefaultConfig)].

encrypt_and_save(Config, DekSnapshot) ->
    {value, DirPath} = ns_config:search(Config, directory),
    Dynamic = ns_config:get_kv_list_with_config(Config),
    ns_config:save_config_sync([Dynamic], DirPath, DekSnapshot).

decrypt(Config) ->
    misc:rewrite_tuples(fun ({encrypted, Val}) when is_binary(Val) ->
                                {ok, Decrypted} = encryption_service:decrypt(Val),
                                {stop, binary_to_term(Decrypted)};
                            (_) ->
                                continue
                        end, Config).

fixup(KV) ->
    dist_manager:fixup_config(KV).

generate_internal_pass() ->
    binary_to_list(couch_uuids:random()).

-ifdef(TEST).
no_upgrade_on_current_version_test() ->
    ?assertEqual([], upgrade_config([[{{node, node(), config_version}, get_current_version()}]])).

ns_config_default_mock_setup() ->
    ns_config:mock_tombstone_agent(),
    config_profile:load_default_profile_for_test(),
    meck:new(sigar),
    meck:expect(sigar, get_cgroups_info,
                fun () ->  #{supported => false} end),
    meck:new(ns_storage_conf),
    meck:expect(ns_storage_conf, default_config,
                fun () -> mock_ns_storage_conf_default_config() end),
    meck:new(ns_bucket),
    meck:expect(ns_bucket, memcached_buckets_in_use,
                fun () -> false end).

mock_ns_storage_conf_default_config() ->
    [{{node, node(), database_dir}, "DbTestDir"},
     {{node, node(), index_dir}, "IxTestDir"}].

ns_config_default_mock_teardown(_) ->
    meck:unload(sigar),
    ns_config:unmock_tombstone_agent(),
    meck:unload(ns_storage_conf),
    config_profile:unload_profile_for_test(),
    meck:unload(ns_bucket).

all_upgrades_test_() ->
    {setup,
     fun ns_config_default_mock_setup/0,
     fun ns_config_default_mock_teardown/1,
     ?_test(test_all_upgrades())}.

test_all_upgrades() ->
    Default = default(?LATEST_VERSION_NUM),
    KVs = misc:update_proplist(Default, [{{node, node(), config_version},
                                          get_min_supported_version()}]),
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
    true = not detect_enterprise_version(<<"1.8.0r-9-ga083a1e-comm">>),
    true = detect_enterprise_version(
        <<"1.8.0r-9-ga083a1e-enterprise-analytics">>),
    false = detect_enterprise_version(
        <<"1.8.0r-9-ga083a1e-enterprise-wombat">>).
-endif.
