%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(memcached_config_mgr).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").
-include_lib("ns_common/include/cut.hrl").

%% API
-export([start_link/0, trigger_tls_config_push/0,
         memcached_port_pid/0, push_config_encryption_key/1,
         drop_historical_deks/0,
         get_global_memcached_deks/0,
         get_key_ids_in_use/0,
         supported_tls_versions/0,
         supported_tls_security_levels/0]).

%% referenced from ns_config_default
-export([get_breakpad_enabled/2,
         get_minidump_dir/2, get_interfaces/2,
         client_cert_auth/2, is_snappy_enabled/2,
         is_snappy_enabled/0, get_fallback_salt/2,
         get_scram_fallback_iter_count/2,
         get_external_users_push_interval/2,
         get_external_auth_service/2,
         is_external_auth_service_enabled/0,
         prometheus_cfg/2,
         sasl_mechanisms/2,
         ssl_sasl_mechanisms/2,
         get_config_profile/2,
         get_fusion_rate/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TLS_VERSIONS, #{
                        'tlsv1.2' => <<"TLS 1.2">>,
                        'tlsv1.3' => <<"TLS 1.3">>
                       }).

-spec supported_tls_versions() -> [atom()].
supported_tls_versions() ->
    maps:keys(?TLS_VERSIONS).

-spec supported_tls_security_levels() -> [pos_integer()].
supported_tls_security_levels() ->
    [1, 2, 3, 4, 5].

-record(state, {
          port_pid :: pid(),
          memcached_config :: binary(),
          tls_config_timer :: undefined | reference(),
          keys_in_use :: #{cfg | prev := {ok, [cb_deks:dek_id()]} | {error, _}}
                         | undefined
         }).

start_link() ->
    proc_lib:start_link(?MODULE, init, [[]]).

trigger_tls_config_push() ->
    try
        ?MODULE ! upload_tls_config,
        ok
    catch
        error:badarg -> {error, no_proccess}
    end.

push_config_encryption_key(ReloadCfg) ->
    try
        gen_server:call(?MODULE, {push_config_encryption_key, ReloadCfg}, 60000)
    catch
        exit:{noproc, {gen_server, call,
                       [?MODULE, {push_config_encryption_key, _}, _]}} ->
            ?log_debug("Can't push config encryption key: ~p is not "
                       "started yet...", [?MODULE]),
            {error, retry}
    end.

drop_historical_deks() ->
    try
        gen_server:call(?MODULE, drop_historical_deks, 60000)
    catch
        exit:{noproc, {gen_server, call, [?MODULE, drop_historical_deks, _]}} ->
            ?log_debug("Can't drop hist keys: ~p is not started yet...",
                       [?MODULE]),
            {error, retry}
    end.

get_key_ids_in_use() ->
    try
        gen_server:call(?MODULE, get_key_ids_in_use, 60000)
    catch
        exit:{noproc, {gen_server, call, [?MODULE, get_key_ids_in_use, _]}} ->
            ?log_debug("Can't get key ids in use: ~p is not started yet...",
                       [?MODULE]),
            {error, retry}
    end.

init([]) ->
    register(?MODULE, self()),
    proc_lib:init_ack({ok, self()}),
    Pid = memcached_port_pid(),
    remote_monitors:monitor(Pid),
    Config = ns_config:get(),
    WantedMcdConfig = memcached_config(Config),
    ReadConfigResult = read_current_memcached_config(Pid),
    McdConfig  = case ReadConfigResult of
                     inactive ->
                         WantedMcdConfig;
                     {active, XMcdConfig} ->
                         XMcdConfig
                 end,
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun ({Key, _}) ->
                                     case is_notable_config_key(Key) of
                                         true ->
                                             Self ! do_check;
                                         _ ->
                                             []
                                     end,
                                     case is_notable_tls_config_key(Key) of
                                         true ->
                                             Self ! upload_tls_config;
                                         _ ->
                                             []
                                     end;
                                 (_Other) ->
                                     []
                             end),
    chronicle_compat_events:notify_if_key_changes(
      fun is_notable_chronicle_key/1, do_check),

    Self ! do_check,
    Self ! upload_tls_config,
    ActualMcdConfig =
        case ReadConfigResult of
            inactive ->
                delete_prev_config_file(),
                McdConfigPath = get_memcached_config_path(),
                {ok, CfgDeksSnapshot} = cb_crypto:fetch_deks_snapshot(configDek),
                ok = cb_crypto:atomic_write_file(McdConfigPath, WantedMcdConfig,
                                                 CfgDeksSnapshot),
                ?log_debug("wrote memcached config to ~s. Will activate "
                           "memcached port server",
                           [McdConfigPath]),
                BootstrapKeysData = prepare_bootstrap_keys(CfgDeksSnapshot),
                set_global_memcached_deks(CfgDeksSnapshot),
                ok = ns_port_server:activate(Pid, BootstrapKeysData),
                ?log_debug("activated memcached port server"),
                WantedMcdConfig;
            _ ->
                ?log_debug("found memcached port to be already active"),
                %% If ns_server vm gets restarted it can be the case that
                %% memcached is already running, so global memcached deks
                %% will not be set. In this case we need to push deks to
                %% memcached and then set global memcached deks.
                %% Note that we can't just call set_global_memcached_deks here
                %% because we don't know what deks are currently in memcached,
                %% while code assumes that global_memcached_deks is exactly
                %% what is in memcached.
                {ok, DS} = cb_crypto:fetch_deks_snapshot(configDek),
                {ok, _} = maybe_push_config_encryption_key(DS),
                McdConfig
        end,

    State = #state{port_pid = Pid,
                   memcached_config = ActualMcdConfig},
    gen_server:enter_loop(?MODULE, [], update_keys_in_use(State)).

update_keys_in_use(State) ->
    CfgPath = get_memcached_config_path(),
    PrevPath = get_memcached_prev_config_path(),
    State#state{keys_in_use = #{cfg => cb_crypto:get_file_dek_ids(CfgPath),
                                prev => cb_crypto:get_file_dek_ids(PrevPath)}}.

prepare_bootstrap_keys(CfgDeksSnapshot) ->
    FormatKeys =
        fun (DS, Name) ->
            {ActiveDekId, AllDeks} = cb_crypto:get_all_deks(DS),
            EncryptionStatusStr = case ActiveDekId of
                                      undefined -> "off";
                                      _ -> "on"
                                  end,
            ?log_debug("~p bootstrap ~s keys will be written to memcached's "
                       "stdin (~s encryption is ~s)",
                       [length(AllDeks), Name, Name, EncryptionStatusStr]),
            memcached_bucket_config:format_mcd_keys(ActiveDekId, AllDeks)
        end,
    {ok, LogDeksSnapshot} = cb_crypto:fetch_deks_snapshot(logDek),
    {ok, AuditDeksSnapshot} = cb_crypto:fetch_deks_snapshot(auditDek),
    ok = cb_crypto:all_keys_ok(CfgDeksSnapshot),
    ok = cb_crypto:active_key_ok(LogDeksSnapshot),
    ok = cb_crypto:active_key_ok(AuditDeksSnapshot),
    CfgDeksJson = FormatKeys(CfgDeksSnapshot, "config"),
    LogDeksJson = FormatKeys(LogDeksSnapshot, "log"),
    AuditDeksJson = FormatKeys(AuditDeksSnapshot, "audit"),
    BootstrapKeysJson = ejson:encode({[{<<"@config">>, CfgDeksJson},
                                       {<<"@logs">>, LogDeksJson},
                                       {<<"@audit">>, AuditDeksJson}]}),
    BootstrapData = <<"BOOTSTRAP_DEK=", BootstrapKeysJson/binary, "\nDONE\n">>,
    BootstrapData.

delete_prev_config_file() ->
    PrevMcdConfigPath = get_memcached_prev_config_path(),
    case file:delete(PrevMcdConfigPath) of
        ok -> ok;
        {error, enoent} -> ok;
        Other ->
            ?log_error("failed to delete ~s: ~p", [PrevMcdConfigPath, Other]),
            erlang:error({failed_to_delete_prev_config, Other})
    end.

is_notable_chronicle_key(rebalance_status) -> true;
is_notable_chronicle_key(jwt_settings) -> true;
is_notable_chronicle_key(?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY) -> true;
is_notable_chronicle_key(Key) ->
    case ns_bucket:sub_key_match(Key) of
        {true, _Bucket, props} ->
            true;
        _ ->
            false
    end.

is_notable_config_key({node, N, memcached}) ->
    N =:= node();
is_notable_config_key({node, N, memcached_defaults}) ->
    N =:= node();
is_notable_config_key({node, N, memcached_config}) ->
    N =:= node();
is_notable_config_key({node, N, memcached_config_extra}) ->
    N =:= node();
is_notable_config_key({node, N, address_family}) ->
    N =:= node();
is_notable_config_key({node, N, address_family_only}) ->
    N =:= node();
is_notable_config_key(memcached) -> true;
is_notable_config_key(memcached_config_extra) -> true;
is_notable_config_key(cluster_compat_version) -> true;
is_notable_config_key(developer_preview_enabled) -> true;
is_notable_config_key(client_cert_auth) -> true;
is_notable_config_key(scramsha_fallback_salt) -> true;
is_notable_config_key(external_auth_polling_interval) -> true;
is_notable_config_key(cluster_encryption_level) -> true;
is_notable_config_key({security_settings, kv}) -> true;
is_notable_config_key(ldap_settings) -> true;
is_notable_config_key(saslauthd_auth_settings) -> true;
is_notable_config_key(saml_settings) -> true;
is_notable_config_key(scram_sha1_enabled) -> true;
is_notable_config_key(scram_sha256_enabled) -> true;
is_notable_config_key(scram_sha512_enabled) -> true;
is_notable_config_key(oauthbearer_enabled) -> true;
is_notable_config_key(force_crash_dumps) -> true;
is_notable_config_key(_) ->
    false.

is_notable_tls_config_key(ssl_minimum_protocol) -> true;
is_notable_tls_config_key(ssl_security_level) -> true;
is_notable_tls_config_key(client_cert_auth) -> true;
is_notable_tls_config_key(cipher_suites) -> true;
is_notable_tls_config_key(honor_cipher_order) -> true;
is_notable_tls_config_key({security_settings, kv}) -> true;
is_notable_tls_config_key(_) -> false.

memcached_port_pid() ->
    ?log_debug("waiting for completion of initial ns_ports_setup round"),
    ns_ports_setup:sync(),
    ?log_debug("waiting for ns_ports_manager"),
    wait_for_ns_ports_manager(60, 1000),
    ?log_debug("ns_ports_setup seems to be ready"),
    find_port_pid_loop(100, 250).

wait_for_ns_ports_manager(Tries, Delay) when Tries > 0 ->
    case erpc:call(ns_server:get_babysitter_node(),
                  erlang, whereis, [ns_ports_manager]) of
        undefined ->
            %% This most likely is due to ns_ports_manager having shut
            %% down and our supervision tree is still running.
            ?log_debug("ns_ports_manager is not running. Will retry."),
            timer:sleep(Delay),
            wait_for_ns_ports_manager(Tries - 1, Delay);
        Pid when is_pid(Pid) ->
            ok
    end.

find_port_pid_loop(Tries, Delay) when Tries > 0 ->
    RV = ns_ports_manager:find_port(ns_server:get_babysitter_node(), kv),
    case RV of
        Pid when is_pid(Pid) ->
            Pid1 = supervisor_cushion:child_pid(Pid),
            case Pid1 of
                undefined -> %% it is already down, continue waiting
                    ?log_debug("Failed to obtain memcached port pid from "
                               "supervisor_cushion ~p. Will retry", [Pid]),
                    timer:sleep(Delay),
                    find_port_pid_loop(Tries - 1, Delay);
                _ when is_pid(Pid1) ->
                    ?log_debug("Found memcached port ~p", [Pid1]),
                    Pid1
            end;
        Other ->
            ?log_debug("Failed to obtain memcached port pid (~p). Will retry",
                       [Other]),
            timer:sleep(Delay),
            find_port_pid_loop(Tries - 1, Delay)
    end.

handle_call({push_config_encryption_key, NeedConfigReload}, _From,
            #state{memcached_config = CurrentMcdConfig} = State) ->
    maybe
        {ok, DeksSnapshot} ?= cb_crypto:fetch_deks_snapshot(configDek),
        ok ?= cb_crypto:all_keys_ok(DeksSnapshot),
        {ok, Changed} ?= maybe_push_config_encryption_key(DeksSnapshot),
        ok ?= case NeedConfigReload andalso (Changed == changed) of
                  true ->
                      hot_reload_config(CurrentMcdConfig, [inet, inet6],
                                        State, 10, []);
                  false ->
                      ok
              end,
        {reply, ok, update_keys_in_use(State)}
    else
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {memcached_error, _Status, _Msg} = Reason ->
            {reply, {error, Reason}, State}
    end;

handle_call(drop_historical_deks, _From, State) ->
    CurDS = get_global_memcached_deks(),
    DSWithoutHistDeks = cb_crypto:without_historical_deks(CurDS),
    Res = case maybe_push_config_encryption_key(DSWithoutHistDeks) of
              {ok, _} -> ok;
              {error, _} = Err -> Err
          end,
    {reply, Res, State};

handle_call(get_key_ids_in_use, _from, #state{keys_in_use = InUse} = State) ->
    Res = case InUse of
              #{cfg := {ok, K1}, prev := {ok, K2}} -> {ok, K1 ++ K2};
              #{cfg := {error, E}, prev := _} -> {error, {read_file_error, E}};
              #{cfg := _, prev := {error, E}} -> {error, {read_file_error, E}}
          end,
    {reply, Res, State};

handle_call(_, _From, _State) ->
    erlang:error(unsupported).

handle_cast(_, _State) ->
    erlang:error(unsupported).

handle_info(do_check, #state{memcached_config = CurrentMcdConfig} = State) ->
    Config = ns_config:get(),
    case memcached_config(Config) of
        %% NOTE: CurrentMcdConfig is bound above
        CurrentMcdConfig ->
            {noreply, State};
        DifferentConfig ->
            apply_changed_memcached_config(DifferentConfig, State)
    end;
handle_info(upload_tls_config, #state{} = State) ->
    NewState =
        case push_tls_config() of
            ok -> stop_tls_config_timer(State);
            {error, _} -> restart_tls_config_timer(State)
        end,
    {noreply, NewState};

handle_info({remote_monitor_down, Pid, Reason},
            #state{port_pid = Pid} = State) ->
    ?log_debug("Got DOWN with reason: ~p from memcached port server: ~p. "
               "Shutting down", [Reason, Pid]),
    {stop, {shutdown, {memcached_port_server_down, Pid, Reason}}, State};
handle_info(Other, State) ->
    ?log_debug("Got unknown message: ~p", [Other]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

restart_tls_config_timer(#state{tls_config_timer = undefined} = State) ->
    RetryTimeout = ?get_timeout(retry_tls_config_timeout, 1000),
    Ref = erlang:send_after(RetryTimeout, self(), upload_tls_config),
    State#state{tls_config_timer = Ref};
restart_tls_config_timer(State) ->
    restart_tls_config_timer(stop_tls_config_timer(State)).

stop_tls_config_timer(#state{tls_config_timer = Ref} = State) ->
    _ = (catch erlang:cancel_timer(Ref)),
    State#state{tls_config_timer = undefined}.

sanitize_tls_config({Cfg}) ->
    {lists:map(
       fun ({<<"password">>, _}) -> {<<"password">>, <<"********">>};
           ({K, V}) -> {K, V}
       end, Cfg)}.

push_tls_config() ->
    Config = ns_config:get(),
    case cluster_compat_mode:tls_supported(Config) of
        true ->
            TLSCfg = tls_config(memcached_params(Config)),
            ?log_info("Pushing TLS config to memcached:~n~p",
                      [sanitize_tls_config(TLSCfg)]),
            case ns_memcached:set_tls_config(TLSCfg) of
                ok ->
                    ?log_info("Successfully pushed TLS config to memcached"),
                    ok;
                {error, Reason} ->
                    ?log_error("Failed to push TLS config to memcached: ~p",
                               [Reason]),
                    {error, Reason}
            end;
        false ->
            ?log_info("Skip TLS config pushing to memcached in CE"),
            ok
    end.

apply_changed_memcached_config(DifferentConfig, State) ->
    %% We might have changed address family in ns_server and we don't want to
    %% fail after trying to connect to the address family which might not exist
    %% in memcached yet.
    %% Since we cannot know for sure which address family memcached is
    %% listening on, due to various error paths that can lead to mismatch in
    %% what this module perceives the memcached config is and what in fact is
    %% applied in memcached, it is simplest to attempt connection using both
    %% address family.
    AFamiliesToTry = [inet, inet6],

    case ns_memcached:config_validate(DifferentConfig, AFamiliesToTry) of
        ok ->
            ?log_debug("New memcached config is hot-reloadable."),
            hot_reload_config(DifferentConfig, AFamiliesToTry, State, 10, []),
            {noreply, update_keys_in_use(
                        State#state{memcached_config = DifferentConfig})};
        {memcached_error, einval, _} ->
            ?log_debug("Memcached config is not hot-reloadable"),
            RestartMemcached =
                case ns_config:search_node(node(), ns_config:latest(),
                                           auto_restart_memcached) of
                    {value, RestartMemcachedBool} ->
                        true = is_boolean(RestartMemcachedBool),
                        RestartMemcachedBool;
                    _ -> false
                end,
            ale:info(?USER_LOGGER, "Got memcached.json config change that's "
                     "not hot-reloadable. Changed keys: ~p",
                     [changed_keys(State#state.memcached_config,
                                   DifferentConfig)]),
            case RestartMemcached of
                false ->
                    ?log_debug("will not restart memcached because new config "
                               "isn't hot-reloadable & auto_restart_memcached "
                               "is not enabled"),
                    {noreply, State};
                _ ->
                    ?log_debug("will auto-restart memcached"),
                    ok = ns_ports_setup:restart_memcached(),
                    ale:info(?USER_LOGGER,
                             "auto-restarted memcached for config change"),
                    {stop, {shutdown, restarting_memcached}, State}
            end
    end.

changed_keys(BlobBefore, BlobAfter) ->
    try
        {Before0} = ejson:decode(BlobBefore),
        {After0} = ejson:decode(BlobAfter),
        Before = lists:sort(Before0),
        After = lists:sort(After0),
        Diffing = (Before -- After) ++ (After -- Before),
        lists:usort([K || {K, _} <- Diffing])
    catch _:_ ->
            unknown
    end.

hot_reload_config(NewMcdConfig, _, State, Tries, LastErr) when Tries < 1 ->
    ale:error(?USER_LOGGER,
              "Unable to apply memcached config update that was supposed to "
              "succeed. Error: ~p. Giving up. Restart memcached to apply that "
              "config change. Updated keys: ~p",
              [LastErr, changed_keys(State#state.memcached_config,
                                     NewMcdConfig)]),
    LastErr;
hot_reload_config(NewMcdConfig, AFamiliesToTry, State, Tries, _LastErr) ->
    FilePath = get_memcached_config_path(),
    PrevFilePath = get_memcached_prev_config_path(),

    %% lets double check everything
    {active, CurrentMcdConfig} =
        read_current_memcached_config(State#state.port_pid),
    true = (CurrentMcdConfig =:= State#state.memcached_config),

    maybe
        DeksSnapshot = get_global_memcached_deks(),
        %% now we save currently active config to .prev
        ok = cb_crypto:atomic_write_file(PrevFilePath, CurrentMcdConfig,
                                         DeksSnapshot),
        %% if we crash here, .prev has copy of active memcached config and
        %% we'll be able to retry hot or cold config update
        ok = cb_crypto:atomic_write_file(FilePath, NewMcdConfig,
                                         DeksSnapshot),

        ok ?= ns_memcached:config_reload(AFamiliesToTry),

        delete_prev_config_file(),
        ale:info(?USER_LOGGER,
                 "Hot-reloaded memcached.json for config change of the "
                 "following keys: ~p",
                 [changed_keys(CurrentMcdConfig, NewMcdConfig)]),
        ok
    else
        Error ->
            ?log_error("Failed to reload memcached config. "
                       "Will retry. Error: ~p", [Error]),
            timer:sleep(1000),
            hot_reload_config(NewMcdConfig, AFamiliesToTry, State,
                              Tries - 1, Error)
    end.

maybe_push_config_encryption_key(DeksSnapshot) ->
    ShouldUpdate =
        case get_global_memcached_deks() of
            undefined -> true;
            OldDeksSnapshot ->
                not cb_crypto:same_snapshots(OldDeksSnapshot, DeksSnapshot)
        end,
    case ShouldUpdate of
        true ->
            ?log_debug("Pushing new config encryption key to memcached: ~0p",
                       [cb_crypto:get_dek(DeksSnapshot)]),
            case ns_memcached:set_active_dek("@config", DeksSnapshot) of
                ok ->
                    set_global_memcached_deks(DeksSnapshot),
                    {ok, changed};
                {error, Err} ->
                    ?log_error("Failed to push config encryption key to "
                               "memcached: ~p", [Err]),
                    {error, Err}
            end;
        false ->
            %% memcached already knows about that key
            ?log_debug("No need to update config encryption key"),
            {ok, unchanged}
    end.

get_memcached_config_path() ->
    Path = ns_config:search_node_prop(ns_config:latest(), memcached,
                                      config_path),
    true = is_list(Path),
    Path.

get_memcached_prev_config_path() ->
    get_memcached_config_path() ++ ".prev".

read_current_memcached_config(McdPortServer) ->
    case ns_port_server:is_active(McdPortServer) of
        true ->
            FilePath = get_memcached_config_path(),
            PrevFilePath = get_memcached_prev_config_path(),
            {ok, Contents} = do_read_current_memcached_config([PrevFilePath,
                                                               FilePath]),
            {active, Contents};
        false ->
            inactive
    end.

do_read_current_memcached_config([]) ->
    ?log_debug("Failed to read any memcached config. Assuming it "
               "does not exist"),
    missing;
do_read_current_memcached_config([Path | Rest]) ->
    case cb_crypto:read_file(Path, configDek) of
        {decrypted, Contents} ->
            {ok, Contents};
        {raw, Contents} ->
            {ok, Contents};
        {error, Error} ->
            ?log_debug("Got ~p while trying to read active "
                       "memcached config from ~s", [Error, Path]),
            do_read_current_memcached_config(Rest)
    end.

memcached_params(Config) ->
    {value, McdParams0} = ns_config:search(Config, {node, node(), memcached}),
    GlobalMcdParams = ns_config:search(Config, memcached, []),
    DefaultMcdParams = ns_config:search(Config,
                                        {node, node(), memcached_defaults}, []),

    McdParams0 ++ GlobalMcdParams ++ DefaultMcdParams.

get_config_profile([], _Params) ->
    list_to_binary(config_profile:name()).

memcached_config(Config) ->
    {value, McdConf} = ns_config:search(Config, {node, node(),
                                                 memcached_config}),

    McdParams = memcached_params(Config),

    {Props} = expand_memcached_config(McdConf, McdParams),
    ExtraProps = ns_config:search(Config,
                                  {node, node(), memcached_config_extra}, []),
    ExtraPropsG = ns_config:search(Config, memcached_config_extra, []),

    BinPrefix = filename:dirname(path_config:component_path(bin)),
    RootProp = [{root, list_to_binary(BinPrefix)}],

    %% removes duplicates of properties making sure that local
    %% memcached_config_extra props overwrite global extra props and
    %% that memcached_config props overwrite them both.
    FinalProps =
        lists:foldl(
          fun (List, Acc) ->
                  normalize_memcached_props(List, Acc)
          end, [], [ExtraPropsG, ExtraProps, RootProp, Props]),

    misc:ejson_encode_pretty({lists:sort(FinalProps)}).

normalize_memcached_props([], Tail) -> Tail;
normalize_memcached_props([{Key, Value} | Rest], Tail) ->
    RestNormalized = normalize_memcached_props(Rest, Tail),
    [{Key, Value} | lists:keydelete(Key, 1, RestNormalized)].

expand_memcached_config({Props}, Params) when is_list(Props) ->
    {[{Key, expand_memcached_config(Value, Params)} || {Key, Value} <- Props]};
expand_memcached_config(Array, Params) when is_list(Array) ->
    [expand_memcached_config(Elem, Params) || Elem <- Array];
expand_memcached_config({M, F, A}, Params) ->
    M:F(A, Params);
expand_memcached_config({Fmt, Args}, Params) ->
    Args1 = [expand_memcached_config(A, Params) || A <- Args],
    iolist_to_binary(io_lib:format(Fmt, Args1));
expand_memcached_config(Param, Params)
  when is_atom(Param), Param =/= true, Param =/= false ->
    {Param, Value} = lists:keyfind(Param, 1, Params),
    Value;
expand_memcached_config(Verbatim, _Params) ->
    Verbatim.

get_breakpad_enabled([], Params) ->
    ForceCrashDumps =
        ns_config:search(ns_config:latest(), force_crash_dumps, false),
    {breakpad_enabled, CurrBreakpadEn} =
        lists:keyfind(breakpad_enabled, 1, Params),

    Snapshot = chronicle_compat:get_snapshot(
                 [cb_cluster_secrets:fetch_snapshot_in_txn(_)], #{}),
    BucketEncrEnabled = ns_bucket:any_bucket_encryption_enabled(Snapshot),
    {ok, LogEncrMethod} = cb_crypto:get_encryption_method(
                            log_encryption, cluster, Snapshot),
    LogEncrEnabled = LogEncrMethod =/= disabled,

    %% Breakpad is forced to false if log/data encryption is enabled, unless
    %% crash dumps are being forced
    EncrEnabled = LogEncrEnabled or BucketEncrEnabled,
    ForceCrashDumps or (CurrBreakpadEn and not EncrEnabled).

get_minidump_dir([], Params) ->
    list_to_binary(proplists:get_value(breakpad_minidump_dir_path, Params,
                                       proplists:get_value(log_path, Params))).

get_interfaces([], MCDParams) ->
    lists:filter(fun ({Props}) ->
                    proplists:get_value(port, Props) =/= undefined andalso
                        %% Either ipv4/ipv6 interface.
                        (proplists:get_value(ipv4, Props) =/= off orelse
                         proplists:get_value(ipv6, Props) =/= off)
                 end, generate_interfaces(MCDParams)).

client_cert_auth([], _Params) ->
    Val = ns_ssl_services_setup:client_cert_auth(),

    State = proplists:get_value(state, Val),
    Prefixes = [{[{K, list_to_binary(V)} || {K, V} <- Triple]} ||
                Triple <- proplists:get_value(prefixes, Val, [])],
    {[{state, list_to_binary(State)}, {prefixes, Prefixes}]}.

is_snappy_enabled([], _Params) ->
    is_snappy_enabled().

is_snappy_enabled() ->
    Cfg = ns_config:latest(),

    %% Local snappy config > global snappy config > default snappy value.
    Default = ns_config:search_prop(Cfg, {node, node(), memcached_defaults},
                                    datatype_snappy, false),

    ns_config:search_node_prop(Cfg, memcached, datatype_snappy, Default).

get_fallback_salt([], _Params) ->
    base64:encode(scram_sha:get_fallback_salt()).

get_scram_fallback_iter_count([], _Params) ->
    scram_sha:get_fallback_iteration_count().

get_external_users_push_interval([], _Params) ->
    max(menelaus_roles:external_auth_polling_interval() div 1000, 1).

get_external_auth_service([], _Params) ->
    is_external_auth_service_enabled().

get_fusion_rate(Name, Params) ->
    case rebalance:status() of
        running ->
            0;
        _ ->
            proplists:get_value(Name, Params)
    end.

is_external_auth_service_enabled() ->
    SaslauthdEnabled =
        proplists:get_value(enabled, saslauthd_auth:build_settings(), false),
    LDAPEnabled = ldap_util:get_setting(authentication_enabled),
    SamlEnabled = menelaus_web_saml:is_enabled(),
    JwtEnabled = menelaus_web_jwt:is_enabled(),

    SaslauthdEnabled or LDAPEnabled or SamlEnabled or JwtEnabled.

get_ssl_cipher_list([], Params) ->
    Cfg = ns_config:latest(),
    AllConfigured = ns_ssl_services_setup:configured_ciphers_names(kv, Cfg),
    {Ciphers12, Ciphers13} =
        case AllConfigured of
            [] ->
                %% Backward compatibility
                %% ssl_cipher_list is obsolete and should not be used in
                %% new installations
                {iolist_to_binary(proplists:get_value(ssl_cipher_list, Params,
                                                      "HIGH")),
                 format_ciphers(ciphers:all_tls13())};
            L ->
                {C13, C12} = lists:partition(fun ciphers:is_tls13_cipher/1, L),
                {format_ciphers(C12), format_ciphers(C13)}
        end,
    {[{<<"TLS 1.2">>, Ciphers12},
      {<<"TLS 1.3">>, Ciphers13}]}.

tls_config(Params) ->
    KeyPath = iolist_to_binary(ns_ssl_services_setup:pkey_file_path(node_cert)),
    ChainPath =
        iolist_to_binary(ns_ssl_services_setup:chain_file_path(node_cert)),
    CAPath = iolist_to_binary(ns_ssl_services_setup:ca_file_path()),
    MinVsn = maps:get(ns_ssl_services_setup:ssl_minimum_protocol(kv),
                      ?TLS_VERSIONS),
    SecurityLevel = ns_ssl_services_setup:ssl_security_level(kv),
    Ciphers = get_ssl_cipher_list([], Params),
    CipherOrder = ns_ssl_services_setup:honor_cipher_order(kv),
    Auth = proplists:get_value(state, ns_ssl_services_setup:client_cert_auth()),
    AuthBin = iolist_to_binary(Auth),
    AuthBinMcd = case AuthBin of
                     <<"disable">> -> <<"disabled">>;
                     <<"enable">> -> <<"enabled">>;
                     %% kv is only a server so hybrid is the same as enabled.
                     %% Some day when kv is a client (e.g. file-to-file
                     %% rebalance) ns_server will explicitly tell kv if TLS
                     %% is to be used or not, the cert to use, and any optional
                     %% username/passwords to use.
                     <<"hybrid">> -> <<"enabled">>;
                     <<"mandatory">> -> <<"mandatory">>
                 end,
    PKeyPassFun = ns_secrets:get_pkey_pass(node_cert),
    PasswordOpts = case PKeyPassFun() of
                       undefined -> [];
                       P -> [{<<"password">>, base64:encode(P)}]
                   end,
    {[{<<"private key">>, KeyPath},
      {<<"certificate chain">>, ChainPath},
      {<<"CA file">>, CAPath},
      {<<"minimum version">>, MinVsn},
      {<<"security level">>, SecurityLevel},
      {<<"cipher list">>, Ciphers},
      {<<"cipher order">>, CipherOrder},
      {<<"client cert auth">>, AuthBinMcd} |
      PasswordOpts]}.

format_ciphers(RFCCipherNames) ->
    OpenSSLNames = [Name || C <- RFCCipherNames,
                            Name <- [ciphers:openssl_name(C)],
                            Name =/= undefined],
    iolist_to_binary(lists:join(":", OpenSSLNames)).

prometheus_cfg([], _Params) ->
    {[{port, service_ports:get_port(memcached_prometheus)},
      {family, ns_config:read_key_fast({node, node(), address_family}, inet)}]}.

sasl_mechanisms([], _Params) ->
    list_to_binary(lists:join(" ", auth_mechs())).

ssl_sasl_mechanisms([], _Params) ->
    list_to_binary(lists:join(" ", ssl_auth_mechs())).

auth_mechs() ->
    ["SCRAM-SHA512" || ns_config:read_key_fast(scram_sha512_enabled, true)] ++
    ["SCRAM-SHA256" || ns_config:read_key_fast(scram_sha256_enabled, true)] ++
    ["SCRAM-SHA1"   || ns_config:read_key_fast(scram_sha1_enabled,   true)] ++
    ["PLAIN"].

ssl_auth_mechs() ->
    auth_mechs() ++
        ["OAUTHBEARER" ||
            cluster_compat_mode:is_cluster_totoro() andalso
                ns_config:read_key_fast(oauthbearer_enabled, true)].

generate_interfaces(MCDParams) ->
    GetPort = fun (Port) ->
                      {Port, Value} = lists:keyfind(Port, 1, MCDParams),
                      Value
              end,

    InterProps = [{[{port, GetPort(port)}]},

                  {[{port, GetPort(dedicated_port)},
                    {system, true}]},

                  {[{port, GetPort(ssl_port)},
                    {tls, true}]},

                  {[{port, GetPort(dedicated_ssl_port)},
                    {system, true},
                    {tls, true}]}] ++
        case config_profile:get_value({memcached, mirror_ssl_port},
                                      undefined) of
            undefined -> [];
            Port ->
                [{[{port, Port},
                   {tls, true}]}]
        end,

    IPv4Interfaces = lists:map(
                       fun ({Props}) ->
                               IsSSL = proplists:is_defined(tls, Props),
                               Extra = [{host, get_host(inet, IsSSL)},
                                        {ipv4, misc:get_afamily_type(inet)},
                                        {ipv6, off}],
                               {Props ++ Extra}
                       end, InterProps),
    IPv6Interfaces = lists:map(
                       fun ({Props}) ->
                               IsSSL = proplists:is_defined(tls, Props),
                               Extra = [{host, get_host(inet6, IsSSL)},
                                        {ipv4, off},
                                        {ipv6, misc:get_afamily_type(inet6)}],
                               {Props ++ Extra}
                       end, InterProps),
    IPv4Interfaces ++ IPv6Interfaces.

get_host(Proto, IsSSL) ->
    case (not IsSSL) andalso misc:disable_non_ssl_ports() of
        true ->
            list_to_binary(misc:localhost(Proto, []));
        false ->
            <<"*">>
    end.

set_global_memcached_deks(DeksSnapshot) ->
    persistent_term:put(memcached_native_encryption_deks, DeksSnapshot).

get_global_memcached_deks() ->
    persistent_term:get(memcached_native_encryption_deks, undefined).
