%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_server).

-behavior(application).

-export([start/2, stop/1, get_loglevel/1, init_log_encryption/0,
         setup_node_names/0, get_babysitter_node/0, get_babysitter_cookie/0,
         get_babysitter_pid/0, read_cookie_file/1, start_disk_sink/2,
         get_disk_sink_rotation_opts/1, adjust_loglevel/2, set_tls_key_log/1,
         add_logger_stderr_sink/1]).

-include("ns_common.hrl").
-include_lib("ale/include/ale.hrl").

-define(BABYSITTER_NODE_PREFIX, "babysitter_of_").

log_pending() ->
    receive
        done ->
            ok;
        {LogLevel, Fmt, Args} ->
            ?ALE_LOG(LogLevel, Fmt, Args),
            log_pending()
    end.

start(_Type, _Args) ->
    setup_env(),
    setup_static_config(),
    init_logging(),
    ns_babysitter:setup_server_profile(),

    %% We load the config_profile in babysitter as well, but since we need to
    %% be able to restart ns_server, it makes more sense for the continuity
    %% checker to run on ns_server than babysitter. Running it on both also
    %% doesn't seem necessary and this maintains the same behavior as previous
    %% releases.
    config_profile_continuity_checker(
      path_config:component_path(data, ?EXISTING_PROFILE_FILE)),

    %% To initialize logging static config must be setup thus this weird
    %% machinery is required to log messages from setup_static_config().
    self() ! done,
    log_pending(),

    case misc:is_ipv6() of
        true ->
            case ns_config_default:init_is_enterprise() of
               true -> ok;
               false -> erlang:error("IPv6 not supported in community edition")
            end;
        false ->
            ok
    end,

    path_config:ensure_directories(),

    root_sup:start_link().

get_config_path() ->
    case application:get_env(ns_server, config_path) of
        {ok, V} -> V;
        _ ->
             erlang:error("config_path parameter for ns_server application is missing!")
    end.

setup_env() ->
    case os:getenv("CHILD_ERLANG_ENV_ARGS") of
        false ->
            ok;
        EnvArgsStr ->
            {ok, EnvArgs} = couch_util:parse_term(EnvArgsStr),
            lists:foreach(
              fun ({App, Values}) ->
                      lists:foreach(
                        fun ({K, V}) ->
                                application:set_env(App, K, V)
                        end, Values)
              end, EnvArgs)
    end.

config_profile_continuity_checker(File) ->
    CurrentProfile = config_profile:name(),
    case misc:read_marker(File) of
        {ok, CurrentProfile} ->
            ok;
        {ok, OtherProfile} ->
            erlang:error(
              lists:flatten(
                io_lib:format(
                  "Configuration profile does not match what the server "
                  "was started with. Changed from ~p to ~p. Exiting..",
                  [OtherProfile, config_profile:name()])));
        false ->
            %% file does not exist at all -- we maybe haven't created it yet.
            %% mark the config_profile on disk, so that we can detect if it
            %% changes.
            ?log_warning("Writing config_profile '~p' to disk.",
                         [CurrentProfile]),
            misc:create_marker(File, [CurrentProfile])
    end.

load_config(Path) ->
    case file:consult(Path) of
        {ok, T} when is_list(T) ->
            T;
        {error, Reason} ->
            Msg = io_lib:format("failed to read static config: ~s with error: ~p. It must be readable file with list of pairs~n",
                                [Path, Reason]),
            erlang:error(lists:flatten(Msg))
    end.

setup_static_config() ->
    Terms = load_config(get_config_path()),
    self() ! {info, "Static config terms:~n~p", [Terms]},
    lists:foreach(fun ({K,V}) ->
                          case application:get_env(ns_server, K) of
                              undefined ->
                                  application:set_env(ns_server, K, V);
                              _ ->
                                  self() ! {warn,
                                            "not overriding parameter ~p, which is given from command line",
                                            [K]}
                          end
                  end, Terms).

get_loglevel(LoggerName) ->
    {ok, DefaultLogLevel} = application:get_env(loglevel_default),
    LoggerNameStr = atom_to_list(LoggerName),
    Key = list_to_atom("loglevel_" ++ LoggerNameStr),
    misc:get_env_default(Key, DefaultLogLevel).

get_loglevel(App, LoggerName) ->
    {ok, DefaultLogLevel} = application:get_env(App, loglevel_default),
    LoggerNameStr = atom_to_list(LoggerName),
    Key = list_to_atom("loglevel_" ++ LoggerNameStr),
    misc:get_env_default(App, Key, DefaultLogLevel).

%% If LogLevel is less restricitve than ThresholdLogLevel (meaning that more
%% message would be printed with that LogLevel) then return ThresholdLogLevel.
%% Otherwise return LogLevel itself.
adjust_loglevel(LogLevel, ThresholdLogLevel) ->
    case ale_utils:loglevel_enabled(LogLevel, ThresholdLogLevel) of
        true ->
            LogLevel;
        false ->
            ThresholdLogLevel
    end.

init_logging() ->
    ale:with_configuration_batching(
      fun () ->
              do_init_logging()
      end),
    ale:info(?NS_SERVER_LOGGER, "Started & configured logging").

init_log_encryption() ->
    {ok, {ActiveDekId, Deks}} = application:get_env(ns_server, initial_log_deks),
    LogDS = cb_crypto:create_deks_snapshot(ActiveDekId, Deks, undefined),
    Res = ale:init_log_encryption_ds(LogDS),
    %% No need to keep the initial log deks in initial_log_deks anymore
    (Res == ok) andalso application:unset_env(ns_server, initial_log_deks),
    Res.

do_init_logging() ->
    StdLoggers = [?ALE_LOGGER, ?ERROR_LOGGER, ?TRACE_LOGGER],
    AllLoggers = [?TLS_KEY_LOGGER, ?CHRONICLE_ALE_LOGGER,
                  ?NS_SERVER_TRACE_LOGGER | StdLoggers] ++ ?LOGGERS,

    lists:foreach(
      fun (Logger) ->
              ale:stop_logger(Logger)
      end, ?LOGGERS ++ [?ACCESS_LOGGER, ?CHRONICLE_ALE_LOGGER, ?TLS_KEY_LOGGER,
                        ?NS_SERVER_TRACE_LOGGER]),

    ok = init_log_encryption(),

    ok = start_disk_sink(disk_default, ?DEFAULT_LOG_FILENAME),
    ok = start_disk_sink(disk_error, ?ERRORS_LOG_FILENAME),
    ok = start_disk_sink(disk_debug, ?DEBUG_LOG_FILENAME),
    ok = start_disk_sink(disk_xdcr, ?XDCR_TARGET_LOG_FILENAME),
    ok = start_disk_sink(disk_stats, ?STATS_LOG_FILENAME),
    ok = start_disk_sink(disk_reports, ?REPORTS_LOG_FILENAME),
    ok = start_disk_sink(disk_trace, ?TRACE_LOG_FILENAME),
    ok = start_disk_sink(disk_access, ?ACCESS_LOG_FILENAME),
    ok = start_disk_sink(disk_access_int, ?INT_ACCESS_LOG_FILENAME),
    ok = start_disk_sink(disk_metakv, ?METAKV_LOG_FILENAME),
    ok = start_disk_sink(disk_json_rpc, ?JSON_RPC_LOG_FILENAME),
    ok = start_disk_sink(disk_tls_key_log, ?TLS_KEY_LOG_FILENAME),

    ok = start_sink(ns_log, ns_log_sink, []),
    ok = start_sink(cb_log_counter, cb_log_counter_sink, []),

    lists:foreach(
      fun (Logger) ->
              ok = ale:start_logger(Logger, debug)
      end, ?LOGGERS),

    lists:foreach(
      fun (Logger) ->
              ok = ale:set_loglevel(Logger, debug)
      end,
      StdLoggers),
    ok = logger:set_primary_config(level, info),

    ok = ale:start_logger(?ACCESS_LOGGER, debug, menelaus_access_log_formatter),
    ok = ale:start_logger(?CHRONICLE_ALE_LOGGER, debug, chronicle_local),
    ok = ale:start_logger(?TLS_KEY_LOGGER, debug, ale_noop_formatter),
    ok = ale:start_logger(?NS_SERVER_TRACE_LOGGER, debug),


    OverrideLoglevels = [{?STATS_LOGGER, warn},
                         {?NS_DOCTOR_LOGGER, warn}],

    MainFilesLoggers = AllLoggers --
        [?XDCR_LOGGER, ?ERROR_LOGGER, ?TRACE_LOGGER,
         ?METAKV_LOGGER, ?JSON_RPC_LOGGER,
         ?TLS_KEY_LOGGER, ?NS_SERVER_TRACE_LOGGER],

    lists:foreach(
      fun (Logger) ->
              LogLevel = proplists:get_value(Logger, OverrideLoglevels,
                                             get_loglevel(Logger)),

              ok = ale:add_sink(Logger, disk_default,
                                adjust_loglevel(LogLevel, info)),

              ok = ale:add_sink(Logger, disk_error,
                                adjust_loglevel(LogLevel, error)),

              %% no need to adjust loglevel for debug log since 'debug' is
              %% already the least restrictive loglevel
              ok = ale:add_sink(Logger, disk_debug, LogLevel),
              ok = ale:add_sink(Logger, cb_log_counter, LogLevel)
      end, MainFilesLoggers),

    ok = ale:add_sink(?ERROR_LOGGER, disk_debug, get_loglevel(?ERROR_LOGGER)),
    ok = ale:add_sink(?ERROR_LOGGER, disk_reports, get_loglevel(?ERROR_LOGGER)),
    ok = ale:add_sink(?TRACE_LOGGER, disk_trace, get_loglevel(?TRACE_LOGGER)),

    ok = ale:add_sink(?USER_LOGGER, ns_log, info),
    ok = ale:add_sink(?MENELAUS_LOGGER, ns_log, info),
    ok = ale:add_sink(?CLUSTER_LOGGER, ns_log, info),
    ok = ale:add_sink(?REBALANCE_LOGGER, ns_log, error),
    ok = ale:add_sink(?XDCR_LOGGER, disk_xdcr, get_loglevel(?XDCR_LOGGER)),
    ok = ale:add_sink(?STATS_LOGGER, disk_stats, get_loglevel(?STATS_LOGGER)),
    ok = ale:add_sink(?NS_DOCTOR_LOGGER, disk_stats, get_loglevel(?NS_DOCTOR_LOGGER)),

    ok = ale:add_sink(?ACCESS_LOGGER, disk_access, info),
    ok = ale:add_sink(?ACCESS_LOGGER, disk_access_int, debug),

    ok = ale:add_sink(?METAKV_LOGGER, disk_metakv, get_loglevel(?METAKV_LOGGER)),

    ok = ale:add_sink(?JSON_RPC_LOGGER, disk_json_rpc, get_loglevel(?JSON_RPC_LOGGER)),
    ok = ale:add_sink(?TLS_KEY_LOGGER, disk_tls_key_log, get_loglevel(?TLS_KEY_LOGGER)),
    ok = ale:add_sink(?NS_SERVER_TRACE_LOGGER, disk_trace, debug),

    case misc:get_env_default(dont_suppress_stderr_logger, false) of
        true ->
            ok = start_sink(stderr, ale_stderr_sink, []),

            lists:foreach(
              fun (Logger) ->
                      ok = add_logger_stderr_sink(Logger)
              end, AllLoggers ++ [?ACCESS_LOGGER]);
        false ->
            ok
    end.

add_logger_stderr_sink(Logger) ->
    StderrLogLevel = get_loglevel(ns_server, stderr),
    LogLevel = get_loglevel(ns_server, Logger),
    ok = ale:add_sink(Logger, stderr,
                      adjust_loglevel(LogLevel, StderrLogLevel)).

start_sink(Name, Module, Args) ->
    ale:stop_sink(Name),
    ale:start_sink(Name, Module, Args).

start_disk_sink(Name, FileName) ->
    {ok, Dir} = application:get_env(ns_server, error_logger_mf_dir),
    DiskSinkOpts = get_disk_sink_opts(Name),

    Path = filename:join(Dir, FileName),
    start_sink(Name, ale_disk_sink, [Path, DiskSinkOpts]).

get_disk_sink_opts(Name) ->
  PerSinkOpts = misc:get_env_default(ns_server, list_to_atom("disk_sink_opts_" ++ atom_to_list(Name)), []),
  PerSinkOpts ++ misc:get_env_default(ns_server, disk_sink_opts, []).

get_disk_sink_rotation_opts(Name) ->
  DiskSinkOpts = get_disk_sink_opts(Name),
  proplists:get_value(rotation, DiskSinkOpts, []).

stop(_State) ->
    ok.

setup_node_names() ->
    Name =  misc:node_name_short(),
    Babysitter = list_to_atom(?BABYSITTER_NODE_PREFIX ++ Name ++ "@" ++
                                  misc:localhost_alias()),
    Couchdb = list_to_atom("couchdb_" ++ Name ++ "@" ++ misc:localhost_alias()),
    application:set_env(ns_server, ns_couchdb_node, Couchdb),
    application:set_env(ns_server, babysitter_node, Babysitter).

read_cookie_file(FilePath) ->
    {ok, Cookie0} = file:read_file(FilePath),
    Cookie1 = binary_to_list(Cookie0),
    Cookie2 = string:trim(Cookie1, trailing, "\n"),
    list_to_atom(Cookie2).

get_babysitter_cookie() ->
    {ok, Cookie} = application:get_env(ns_server, babysitter_cookie),
    Cookie.

get_babysitter_node() ->
    {ok, Node} = application:get_env(ns_server, babysitter_node),
    erlang:set_cookie(Node, get_babysitter_cookie()),
    Node.

get_babysitter_pid() ->
    list_to_integer(case atom_to_list(node()) of
                        ?BABYSITTER_NODE_PREFIX ++ _ -> os:getpid();
                        _ -> os:getenv("NS_SERVER_BABYSITTER_PID")
                    end).

%% To enable TLS key logging for debugging purposes, we use a diag/eval to
%% call this function, so this is not a dead function. Do not remove.
set_tls_key_log(Value) ->
    ns_config:set({node, node(), tls_key_log}, Value),
    netconfig_updater:apply_config([{keepSecrets, Value}]).
