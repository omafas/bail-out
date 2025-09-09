%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Macros used all over the place.
%%

-ifndef(_NS_COMMON__HRL_).
-define(_NS_COMMON__HRL_,).

-type bucket_name() :: nonempty_string().
-type bucket_type() :: memcached | membase.
-type vbucket_map() :: [[atom(), ...], ...].
-type mc_error_atom() :: key_enoent | key_eexists | e2big | einval |
                         not_stored | delta_badval | not_my_vbucket |
                         unknown_command | enomem | not_supported | internal |
                         ebusy | etmpfail | auth_error | auth_continue.
-type mc_error() :: {memcached_error, mc_error_atom(), binary() | undefined}.
-type vbucket_id() :: non_neg_integer().
-type vbucket_state() :: active | dead | replica | pending.
-type rev_id() :: <<_:128>>.
-type seq_no() :: non_neg_integer().
-type rev() :: {seq_no(), rev_id()}.
-type rebalance_vbucket_state() :: passive | undefined | paused.
-type janitor_item() :: services | {bucket, bucket_name()}.
-type data_ingress_status() :: ok | resident_ratio | data_size | disk_usage.

-type ext_bucket_name() :: bucket_name() | binary().
%% ext vbucket id is vbucket id (potentially as binary) or <<"master">>
-type ext_vbucket_id() :: vbucket_id() | binary().

-type version() :: {list(integer()), candidate | release, integer()}.

-type dcp_error() :: {dcp_error, mc_error_atom(), binary()}.
-type dcp_conn_name() :: nonempty_string().
-type dcp_conn_type() :: consumer | producer | notifier.

-type service() :: kv | index | n1ql | fts | eventing | cbas | backup.
-type tcp_port() :: 0..65535.

-type auth_audit_props() :: [{atom(), binary() | [binary()] | integer()}].

-define(MULTICALL_DEFAULT_TIMEOUT, 30000).

-define(MIB, 1048576).

-define(MAX_BUCKETS_SUPPORTED, 30).

-define(MAX_SCOPES_SUPPORTED, 1200).
-define(MAX_COLLECTIONS_SUPPORTED, 1200).

-define(MIN_REPLICAS_SUPPORTED, 0).

-define(MAX_DCP_CONNECTION_NAME, 200).
-define(DEFAULT_DCP_CONNECTIONS, 1).

-define(DEFAULT_LOG_FILENAME, "info.log").
-define(ERRORS_LOG_FILENAME, "error.log").
-define(VIEWS_LOG_FILENAME, "views.log").
-define(MAPREDUCE_ERRORS_LOG_FILENAME, "mapreduce_errors.log").
-define(COUCHDB_LOG_FILENAME, "couchdb.log").
-define(DEBUG_LOG_FILENAME, "debug.log").
-define(XDCR_TARGET_LOG_FILENAME, "xdcr_target.log").
-define(STATS_LOG_FILENAME, "stats.log").
-define(BABYSITTER_LOG_FILENAME, "babysitter.log").
-define(NS_COUCHDB_LOG_FILENAME, "ns_couchdb.log").
-define(REPORTS_LOG_FILENAME, "reports.log").
-define(TRACE_LOG_FILENAME, "trace.log").
-define(ACCESS_LOG_FILENAME, "http_access.log").
-define(INT_ACCESS_LOG_FILENAME, "http_access_internal.log").
-define(GOXDCR_LOG_FILENAME, "goxdcr.log").
-define(QUERY_LOG_FILENAME, "query.log").
-define(PROJECTOR_LOG_FILENAME, "projector.log").
-define(INDEXER_LOG_FILENAME, "indexer.log").
-define(METAKV_LOG_FILENAME, "metakv.log").
-define(FTS_LOG_FILENAME, "fts.log").
-define(JSON_RPC_LOG_FILENAME, "json_rpc.log").
-define(EVENTING_LOG_FILENAME, "eventing.log").
-define(CBAS_LOG_FILENAME, "analytics_info.log").
-define(BACKUP_LOG_FILENAME, "backup_service.log").
-define(TLS_KEY_LOG_FILENAME, "key.log").
-define(CONT_BACKUP_LOG_FILENAME, "cont_backup.log").

-define(NS_SERVER_LOGGER, ns_server).
-define(NS_SERVER_TRACE_LOGGER, ns_server_trace).
-define(COUCHDB_LOGGER, couchdb).
-define(USER_LOGGER, user).
-define(MENELAUS_LOGGER, menelaus).
-define(NS_DOCTOR_LOGGER, ns_doctor).
-define(STATS_LOGGER, stats).
-define(REBALANCE_LOGGER, rebalance).
-define(CLUSTER_LOGGER, cluster).
-define(VIEWS_LOGGER, views).
%% The mapreduce logger is used by the couchdb component, hence don't wonder
%% if you can't find any calls to it in ns_server
-define(MAPREDUCE_ERRORS_LOGGER, mapreduce_errors).
-define(XDCR_LOGGER, xdcr).
-define(ACCESS_LOGGER, access).
-define(METAKV_LOGGER, metakv).
-define(JSON_RPC_LOGGER, json_rpc).
-define(CHRONICLE_ALE_LOGGER, chronicle).
-define(TLS_KEY_LOGGER, tls_key).

-define(LOGGERS, [?NS_SERVER_LOGGER,
                  ?USER_LOGGER, ?MENELAUS_LOGGER,
                  ?NS_DOCTOR_LOGGER, ?STATS_LOGGER,
                  ?REBALANCE_LOGGER, ?CLUSTER_LOGGER,
                  ?XDCR_LOGGER, ?METAKV_LOGGER,
                  ?JSON_RPC_LOGGER]).

-define(NS_COUCHDB_LOGGERS, [?NS_SERVER_LOGGER,
                             ?COUCHDB_LOGGER,
                             ?VIEWS_LOGGER,
                             ?MAPREDUCE_ERRORS_LOGGER,
                             ?XDCR_LOGGER]).

-define(ALE_LOG(Level, Format, Args),
        ale:log(?NS_SERVER_LOGGER, Level, Format, Args)).

%% ale:{log, error, debug, warning, critical} are parse transformed to
%% add the Module, Function, Line of the caller.
%%
%% If new definitions are added to the loggers defined below, make sure
%% the parse transform logic works on those too.

-define(log_debug(Format, Args, Opts), ale:debug(?NS_SERVER_LOGGER, Format, Args, Opts)).
-define(log_debug(Format, Args), ale:debug(?NS_SERVER_LOGGER, Format, Args)).
-define(log_debug(Msg), ale:debug(?NS_SERVER_LOGGER, Msg)).

-define(log_info(Format, Args, Opts), ale:info(?NS_SERVER_LOGGER, Format, Args, Opts)).
-define(log_info(Format, Args), ale:info(?NS_SERVER_LOGGER, Format, Args)).
-define(log_info(Msg), ale:info(?NS_SERVER_LOGGER, Msg)).

-define(log_warning(Format, Args, Opts), ale:warn(?NS_SERVER_LOGGER, Format, Args, Opts)).
-define(log_warning(Format, Args), ale:warn(?NS_SERVER_LOGGER, Format, Args)).
-define(log_warning(Msg), ale:warn(?NS_SERVER_LOGGER, Msg)).

-define(log_error(Format, Args, Opts), ale:error(?NS_SERVER_LOGGER, Format, Args, Opts)).
-define(log_error(Format, Args), ale:error(?NS_SERVER_LOGGER, Format, Args)).
-define(log_error(Msg), ale:error(?NS_SERVER_LOGGER, Msg)).

%% Log to user visible logs using combination of ns_log and ale routines.
-define(user_log(Code, Msg), ?user_log_mod(?MODULE, Code, Msg)).
-define(user_log_mod(Module, Code, Msg),
        ale:xlog(?USER_LOGGER, ns_log_sink:get_loglevel(Module, Code),
                 {Module, Code}, Msg)).

-define(user_log(Code, Fmt, Args), ?user_log_mod(?MODULE, Code, Fmt, Args)).
-define(user_log_mod(Module, Code, Fmt, Args),
        ale:xlog(?USER_LOGGER, ns_log_sink:get_loglevel(Module, Code),
                 {Module, Code}, Fmt, Args)).

-define(rebalance_debug(Format, Args),
        ale:debug(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_debug(Msg), ale:debug(?REBALANCE_LOGGER, Msg)).

-define(rebalance_info(Format, Args),
        ale:info(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_info(Msg), ale:info(?REBALANCE_LOGGER, Msg)).

-define(rebalance_warning(Format, Args),
        ale:warn(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_warning(Msg), ale:warn(?REBALANCE_LOGGER, Msg)).

-define(rebalance_error(Format, Args),
        ale:error(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_error(Msg), ale:error(?REBALANCE_LOGGER, Msg)).

-define(views_debug(Format, Args), ale:debug(?VIEWS_LOGGER, Format, Args)).
-define(views_debug(Msg), ale:debug(?VIEWS_LOGGER, Msg)).

-define(views_info(Format, Args), ale:info(?VIEWS_LOGGER, Format, Args)).
-define(views_info(Msg), ale:info(?VIEWS_LOGGER, Msg)).

-define(views_warning(Format, Args), ale:warn(?VIEWS_LOGGER, Format, Args)).
-define(views_warning(Msg), ale:warn(?VIEWS_LOGGER, Msg)).

-define(views_error(Format, Args), ale:error(?VIEWS_LOGGER, Format, Args)).
-define(views_error(Msg), ale:error(?VIEWS_LOGGER, Msg)).

-define(xdcr_debug(Format, Args), ale:debug(?XDCR_LOGGER, Format, Args)).
-define(xdcr_debug(Msg), ale:debug(?XDCR_LOGGER, Msg)).

-define(xdcr_info(Format, Args), ale:info(?XDCR_LOGGER, Format, Args)).
-define(xdcr_info(Msg), ale:info(?XDCR_LOGGER, Msg)).

-define(xdcr_warning(Format, Args), ale:warn(?XDCR_LOGGER, Format, Args)).
-define(xdcr_warning(Msg), ale:warn(?XDCR_LOGGER, Msg)).

-define(xdcr_error(Format, Args), ale:error(?XDCR_LOGGER, Format, Args)).
-define(xdcr_error(Msg), ale:error(?XDCR_LOGGER, Msg)).

-define(metakv_debug(Format, Args), ale:debug(?METAKV_LOGGER, Format, Args)).
-define(metakv_debug(Msg), ale:debug(?METAKV_LOGGER, Msg)).

%% Log important but potentially spammy info into trace.log
-define(log_trace(Format, Args, Opts),
        ale:debug(?NS_SERVER_TRACE_LOGGER, Format, Args, Opts)).
-define(log_trace(Format, Args),
        ale:debug(?NS_SERVER_TRACE_LOGGER, Format, Args)).
-define(log_trace(Msg), ale:debug(?NS_SERVER_TRACE_LOGGER, Msg)).

-define(tls_key_log(Msg), ale:debug(?TLS_KEY_LOGGER, Msg)).

-define(get_timeout(Op, Default), ns_config:get_timeout({?MODULE, Op}, Default)).
-define(get_param(Param, Default),
        ns_config:search_node_with_default({?MODULE, Param}, Default)).

-define(REBALANCE_OBSERVER_TASK_DEFAULT_TIMEOUT,
        ?get_timeout(observer_task, 10000)).

-define(i2l(V), integer_to_list(V)).

-define(UI_AUTH_EXPIRATION_SECONDS, 600).

%% XDCR_CHECKPOINT_STORE is the name of the simple-store where
%% metakv stores XDCR checkpoints.
-define(XDCR_CHECKPOINT_STORE, xdcr_ckpt_data).

%% Pattern used to identify XDCR checkpoints.
-define(XDCR_CHECKPOINT_PATTERN, list_to_binary("/ckpt/")).

%% Metakv tag for values storing sensitive information
%% If this tag is changed to something else, then do not forget
%% to change its value in ns_server/scripts/dump-guts as well.
-define(METAKV_SENSITIVE, metakv_sensitive).

-define(MIN_FREE_RAM, misc:get_env_default(quota_min_free_ram, 1024)).
-define(MIN_FREE_RAM_PERCENT, 80).

-define(DEFAULT_EPHEMERAL_PURGE_INTERVAL_DAYS, 1).

%% Default quota is 5GiB but the unit is MiB.
-define(QUERY_TMP_SPACE_DEF_SIZE, 5120).

%% Index storage mode values.
-define(INDEX_STORAGE_MODE_MEMORY_OPTIMIZED, <<"memory_optimized">>).
-define(INDEX_STORAGE_MODE_FORESTDB, <<"forestdb">>).
-define(INDEX_STORAGE_MODE_PLASMA, <<"plasma">>).
-define(DEFAULT_MAX_ROLLBACK_PTS_PLASMA, 2).
-define(DEFAULT_MAX_ROLLBACK_PTS_FORESTDB, 5).

%% common memcached settings are ints which is usually 32-bits wide
-define(MAX_32BIT_SIGNED_INT, 16#7FFFFFFF).

%% Many memcached bucket parameters use size_t which will almost certainly be a
%% 64 bit unsigned integer. We should prevent larger values than this being
%% passed to memcached as it will likely throw an error.
-define(MAX_64BIT_UNSIGNED_INT, 16#FFFFFFFFFFFFFFFF).

-define(VERSION_72, [7, 2]).
-define(VERSION_76, [7, 6]).
-define(VERSION_79, [7, 9]).
-define(VERSION_80, [8, 0]).

-define(MIN_SUPPORTED_VERSION, ?VERSION_72).

-define(version_string(Version),
        lists:flatten(lists:join(".", lists:map(fun erlang:integer_to_list/1,
                                                Version)))).

%% This require coordination with the UI to update the version.
-define(LATEST_UI_COMPAT_VERSION, ?VERSION_70).

-define(LATEST_VERSION_NUM, ?VERSION_80).
-define(MASTER_ADVERTISED_VERSION, [8, 0, 0]).

%% We use 'prod' and 'prod_name' during compatibility checks when validating
%% joining nodes. 'prod' is a short string that is used to identify the
%% product, whereas 'prod_name' is a longer "pretty" string that is used where
%% we want to display the full name of the product, including error messages &
%% certificate generation. See cluster_compat_mode:get_prod[_name]/0
-define(DEFAULT_PROD, "server").
-define(DEFAULT_PROD_NAME, "Couchbase Server").
%% These need to be kept in sync with 'prod[_name]' in the analytics profiles
-define(ANALYTICS_PROD, "analytics").
-define(ANALYTICS_PROD_NAME, "Enterprise Analytics").

-define(MIN_OF_MAX_MOVES_PER_NODE, 1).
-define(MAX_OF_MAX_MOVES_PER_NODE, 64).
-define(DEFAULT_MAX_MOVES_PER_NODE, 4).

-define(LOWEST_ALLOWED_MAX_DOCS_SKIP, 0).
%% Maximum allowed value when changing in /internalSettings
-define(HIGHEST_ALLOWED_MAX_DOCS_SKIP, 5000).
%% Unless changed in /internalSettings this is the maximum 'skip'
%% value permitted.
-define(DEFAULT_MAX_DOCS_SKIP, 1000).
%% Value to use if not specified
-define(DEFAULT_DOCS_SKIP, 0).

-define(LOWEST_ALLOWED_MAX_DOCS_LIMIT, 1).
%% Maximum allowed value when changing in /internalSettings
-define(HIGHEST_ALLOWED_MAX_DOCS_LIMIT, 5000).
%% Unless changed in /internalSettings this is the maximum 'limit'
%% value permitted.
-define(DEFAULT_MAX_DOCS_LIMIT, 1000).
-define(DEFAULT_DOCS_LIMIT, 1000).

-define(flush(Pattern),
        misc:letrec([0],
                    fun (Rec, I) ->
                            receive
                                Pattern ->
                                    Rec(Rec, I+1)
                            after
                                0 ->
                                    I
                            end
                    end)).

-define(must_flush(Pattern), ?must_flush(Pattern, 1, 15000)).
-define(must_flush(Pattern, N, Timeout),
        misc:letrec(
          [0],
          fun (_Rec, I)
                when I =:= N ->
                  ok;
              (Rec, I) ->
                  receive
                      Pattern ->
                          Rec(Rec, I + 1)
                  after
                      Timeout ->
                          throw({error, {no_messages, Timeout, ??Pattern}})
                  end
          end)).

%% Maximum number of non-self-issued intermediate certificates that can follow
%% the peer certificate in a valid certification path. So, if depth is 0 the
%% PEER must be signed by the trusted ROOT-CA directly; if 1 the path can be
%% PEER, CA, ROOT-CA; if 2 the path can be PEER, CA, CA, ROOT-CA, and so on.
-define(ALLOWED_CERT_CHAIN_LENGTH, 10).

-define(INTERNAL_CERT_USER, "@internal").
-define(INTERNAL_CERT_EMAIL_DOMAIN, "internal.couchbase.com").
%% Window for internal client certs to be rotated prior to expiry, 60 days by
%% default. This is double the default cert_expiration_warning_days to ensure
%% that expiration is performed prior to alerting.
%% If the cert_expiration_warning_days gets re-configured to 60 days or longer,
%% the alert will be triggered prior to rotation
-define(INTERNAL_CERT_REGEN_WINDOW_SEC,
        ?get_timeout(client_cert_regen_window_sec, 60 * 24 * 60 * 60)).
%% testSSL.sh complains when certificate validity is longer than 824 days
%% This apparently comes from Apple's 825 day restriction
%% (https://support.apple.com/en-us/103769)
-define(INTERNAL_CERT_MAX_NOT_AFTER_DURATION, 824 * 24 * 60 * 60).
%% Since we automatically regenerate the client cert prior to expiry, setting
%% the NotAfter duration to that window duration or smaller will cause any
%% generated cert to be immediately regenerated as soon as it was generated,
%% which will get repeatedly triggered indefinitely, until the setting is set
%% to a larger number again
-define(INTERNAL_CERT_MIN_NOT_AFTER_DURATION,
        ?INTERNAL_CERT_REGEN_WINDOW_SEC).

-define(CPU_COUNT_VAR, "COUCHBASE_CPU_COUNT").

%% Known profiles
-define(SERVERLESS_PROFILE, serverless).
-define(SERVERLESS_PROFILE_STR, "serverless").
-define(PROVISIONED_PROFILE_STR, "provisioned").
-define(ANALYTICS_PROFILE_STR, "analytics").

%% Default profile macros/constants
%% DEFAULT_PROFILE_STR is provided by rebar.config at build time
-define(DEFAULT_EMPTY_PROFILE_FOR_TESTS, [{name, "default"}]).

-define(SYSTEM_SCOPE_NAME, "_system").

-define(DEFAULT_PWHASH, ?ARGON2ID_HASH).
-define(ARGON2ID_HASH, <<"argon2id">>).
-define(PBKDF2_HASH, <<"pbkdf2-hmac-sha512">>).
-define(SHA1_HASH, <<"SHA-1">>).

-define(DEFAULT_ARG2ID_TIME, 3).
-define(DEFAULT_ARG2ID_MEM, 524288). %% in bytes
-define(DEFAULT_PBKDF2_ITER, 10000).
-define(DEFAULT_SCRAM_ITER, 15000).

-define(HASHES_KEY,           <<"hashes">>).
-define(SALT_KEY,             <<"salt">>).
-define(HASH_ALG_KEY,         <<"algorithm">>).
-define(ARGON_TIME_KEY,       <<"time">>).
-define(ARGON_MEM_KEY,        <<"memory">>).
-define(ARGON_THREADS_KEY,    <<"parallelism">>).
-define(PBKDF2_ITER_KEY,      <<"iterations">>).
-define(SCRAM_STORED_KEY_KEY, <<"stored_key">>).
-define(SCRAM_SERVER_KEY_KEY, <<"server_key">>).
-define(SCRAM_SALT_KEY,       <<"salt">>).
-define(SCRAM_ITERATIONS_KEY, <<"iterations">>).

-define(ARGON_MEM_MIN, 8_192).
-define(ARGON_MEM_MAX, 1_073_741_824).
-define(ARGON_TIME_MIN, 1).
-define(ARGON_TIME_MAX, 4_294_967_295).
-define(DEFAULT_ARG2ID_MAX_EXEC_TIME, 5000). %% in ms
-define(DEFAULT_ARG2ID_MAX_PRODUCT, 20000000000). %% MAX for (MEM * TIME)

-define(PBKDF2_ITER_MIN, 1).
-define(PBKDF2_ITER_MAX, 10_000_000).

%% Pre-7.6 keys (do not change these values):
-define(OLD_HASH_KEY,             <<"h">>).
-define(OLD_SCRAM_SALT_KEY,       <<"s">>).
-define(OLD_SCRAM_HASH_KEY,       <<"h">>).
-define(OLD_SCRAM_ITERATIONS_KEY, <<"i">>).

-define(COOKIE_LEN, 32).
-define(COOKIE_HEX_LEN, (?COOKIE_LEN * 2)).
%% Hide/Unhide macros to make it easier to hide secrets from stack-traces and
%% error reports. It is generally good practice to rename the variable(s) in
%% between hide/unhide points so that it's obvious that the value has been
%% wrapped and cannot be used directly.
-define(HIDE(__V), fun () -> __V end).
-define(UNHIDE(__V), __V()).

-define(COUCHDB_ENABLED(__TRUE, __FALSE),
        case config_profile:get_bool({couchdb, disabled}) of
            true ->
                __FALSE;
            false ->
                __TRUE
        end).

-define(INT_CREDS_ROTATION_INT_KEY, int_creds_rotation_interval).
-define(INT_CREDS_ROTATION_INT_DEFAULT, 1800000).
-define(INT_CREDS_PROTECTION_SLEEP_KEY, int_creds_protection_sleep).
-define(INT_CREDS_PROTECTION_SLEEP_DEFAULT, 30000).
-define(LDAP_GROUPS_CACHE_SIZE, 1000).

-define(SAML_METADATA_ENDPOINT_PATH, "metadata").
-define(SAML_CONSUME_ENDPOINT_PATH, "consume").
-define(SAML_LOGOUT_ENDPOINT_PATH, "logout").

-define(SECS_IN_DAY, 24*60*60).

%% Existing profile gets written to disk in the 'data' dir when ns_config first
%% starts.
-define(EXISTING_PROFILE_FILE, "existing_profile").

%% Weight values used in mb_master to determine orchestrator placement.
-define(DEFAULT_SERVICE_WEIGHTS, [{kv, 10000},
                                  {index, 1000},
                                  {fts, 1000},
                                  {cbas, 1000},
                                  {n1ql, 100},
                                  {eventing, 100},
                                  {backup, 10}]).

-define(MIN_BUCKET_RANK, 0).
-define(MAX_BUCKET_RANK, 1000).
-define(DEFAULT_BUCKET_RANK, ?MIN_BUCKET_RANK).

-define(KvResidentRatioQuery,
        ns_config:search_node_prop(
          ns_config:latest(), resource_promql_override, kv_resident_ratio,
          <<"100 * kv_ep_max_size / on(bucket) "
            "kv_logical_data_size_bytes{state=`active`}">>)).
-define(KvDataSizeRawQuery,
        ns_config:search_node_prop(
          ns_config:latest(), resource_promql_override, kv_data_size_raw,
          <<"kv_logical_data_size_bytes{state=`active`}">>)).
-define(KvDataSizeTBQuery,
        ns_config:search_node_prop(
          ns_config:latest(), resource_promql_override, kv_data_size_tb,
          <<"kv_logical_data_size_bytes{state=`active`} / 10^12">>)).

-define(IndexResidentRatioQuery,
        ns_config:search_node_prop(
          ns_config:latest(), resource_promql_override, index_resident_ratio,
          <<"index_avg_resident_percent{} AND ignoring(name) (
          (index_memory_rss / ignoring(name) index_memory_quota) > 0.9)">>)).

-define(REPLICATED_DETS_NORMAL_PRIORITY, 0).
-define(REPLICATED_DETS_HIGH_PRIORITY, 1).
-define(QUERY_NODE_QUOTA_DEFAULT, 0).

-define(APP_TELEMETRY_ENABLED, false).
-define(APP_TELEMETRY_PATH, "_appTelemetry").
-define(APP_TELEMETRY_MAX_CLIENTS_PER_NODE, 1024).
-define(APP_TELEMETRY_MIN_SCRAPE_INTERVAL_SECONDS,
        ns_config:search_node_with_default(
          app_telemetry_min_scrape_interval_seconds,
          60)).
-define(APP_TELEMETRY_MAX_SCRAPE_INTERVAL_SECONDS,
        ns_config:search_node_with_default(
          app_telemetry_max_scrape_interval_seconds,
          600)).
-define(APP_TELEMETRY_DEFAULT_SCRAPE_INTERVAL_SECONDS, 60).


-define(call_on_ns_server_node(Body, Args),
        case ns_node_disco:couchdb_node() == node() of
            false ->
                Body;
            true ->
                rpc:call(ns_node_disco:ns_server_node(), ?MODULE,
                         ?FUNCTION_NAME, Args)
        end).

-define(ALE_ENCR_CALLBACKS,
    #{create_no_deks_snapshot =>
          fun() ->
                  cb_crypto:create_deks_snapshot(undefined, [], undefined)
          end,
      file_encrypt_state_match =>
          fun(DS, EncrState) ->
                  cb_crypto:file_encrypt_state_match(DS, EncrState)
          end,
      validate_encr_file_with_ds =>
          fun(Path, DS) ->
                  cb_crypto:validate_encr_file_with_ds(Path, DS)
          end,
      is_file_encrypted =>
          fun(Path) ->
                  cb_crypto:is_file_encrypted(Path)
          end,
      file_encrypt_init =>
          fun(DS) ->
                  cb_crypto:file_encrypt_init(DS)
          end,
      file_encrypt_finish =>
          fun(EncrState) ->
                  cb_crypto:file_encrypt_finish(EncrState)
          end,
      file_encrypt_cont =>
          fun(Path, Offset, DS) ->
                  cb_crypto:file_encrypt_cont(Path, Offset, DS)
          end,
      file_encrypt_chunk =>
          fun(Data, EncrState) ->
                  cb_crypto:file_encrypt_chunk(Data, EncrState)
          end,
      reencrypt_file =>
          fun(FromPath, ToPath, DS, Opts) ->
                  cb_crypto:reencrypt_file(FromPath, ToPath, DS, Opts)
          end,
      get_in_use_deks =>
          fun(FilePaths) ->
                  cb_crypto:get_in_use_deks(FilePaths)
          end}).

-define(DEK_LIFETIME_ROTATION_MARGIN_SEC, (5 * 60)).

-define(MAX_PHASH2_RANGE, 4294967296).
-define(DEFAULT_SYSTEMD_CGROUP_ROOT,
        "/sys/fs/cgroup/system.slice/couchbase-server.service").

-type cgroup_val() :: max | non_neg_integer().
-record(limits, {hard = max :: cgroup_val(), soft = max :: cgroup_val()}).

%% NOTE: we use erlang:list_to_existing_atom/1 so we need to make sure all the
%% service names are valid atoms already loaded into the runtime system.
%% Otherwise this will cause a crash. Using list_to_atom would be a security
%% issue since you could easily run out of atoms by just spamming the API with
%% different atoms as the "service name".
-define(ALL_SERVICE_ATOMS, [n1ql, fts, prometheus, kv, projector, ns_server,
                            backup, index, cbas, goxdcr, eventing, cont_backup,
                            evaluator, saslauthd_port, ns_couchdb]).

-type service_name() :: n1ql | fts | prometheus | kv | projector | ns_server |
                        backup | index | cbas | goxdcr | eventing |
                        cont_backup | evaluator | saslauthd_port | ns_couchdb.

-record(cgroup_system_info,
        {v2 = false :: boolean(), controllers = false :: boolean()}).

-endif.
