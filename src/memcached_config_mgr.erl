%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2018 Couchbase, Inc.
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
-module(memcached_config_mgr).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("cut.hrl").

%% API
-export([start_link/0]).

%% referenced from ns_config_default
-export([get_minidump_dir/2, get_interfaces/2, ssl_minimum_protocol/2,
         client_cert_auth/2, is_snappy_enabled/2,
         is_snappy_enabled/0, collections_enabled/2, get_fallback_salt/2,
         get_external_users_push_interval/2, get_ssl_cipher_list/2,
         get_ssl_cipher_order/2, get_external_auth_service/2,
         is_external_auth_service_enabled/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [[]]).

-record(state, {
          port_pid :: pid(),
          memcached_config :: binary()
         }).

init([]) ->
    register(?MODULE, self()),
    proc_lib:init_ack({ok, self()}),
    ?log_debug("waiting for completion of initial ns_ports_setup round"),
    ns_ports_setup:sync(),
    ?log_debug("ns_ports_setup seems to be ready"),
    Pid = find_port_pid_loop(100, 250),
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
                                     end;
                                 (_Other) ->
                                     []
                             end),
    Self ! do_check,
    ActualMcdConfig =
        case ReadConfigResult of
            inactive ->
                delete_prev_config_file(),
                McdConfigPath = get_memcached_config_path(),
                ok = misc:atomic_write_file(McdConfigPath, WantedMcdConfig),
                ?log_debug("wrote memcached config to ~s. Will activate "
                           "memcached port server",
                           [McdConfigPath]),
                ok = ns_port_server:activate(Pid),
                ?log_debug("activated memcached port server"),
                WantedMcdConfig;
            _ ->
                ?log_debug("found memcached port to be already active"),
                McdConfig
    end,
    State = #state{port_pid = Pid,
                   memcached_config = ActualMcdConfig},
    gen_server:enter_loop(?MODULE, [], State).

delete_prev_config_file() ->
    PrevMcdConfigPath = get_memcached_config_path() ++ ".prev",
    case file:delete(PrevMcdConfigPath) of
        ok -> ok;
        {error, enoent} -> ok;
        Other ->
            ?log_error("failed to delete ~s: ~p", [PrevMcdConfigPath, Other]),
            erlang:error({failed_to_delete_prev_config, Other})
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
is_notable_config_key(memcached) -> true;
is_notable_config_key(memcached_config_extra) -> true;
is_notable_config_key(ssl_minimum_protocol) -> true;
is_notable_config_key(cluster_compat_version) -> true;
is_notable_config_key(developer_preview_enabled) -> true;
is_notable_config_key(client_cert_auth) -> true;
is_notable_config_key(scramsha_fallback_salt) -> true;
is_notable_config_key(external_auth_polling_interval) -> true;
is_notable_config_key(cipher_suites) -> true;
is_notable_config_key(honor_cipher_order) -> true;
is_notable_config_key(cluster_encryption_level) -> true;
is_notable_config_key({security_settings, kv}) -> true;
is_notable_config_key(ldap_settings) -> true;
is_notable_config_key(saslauthd_auth_settings) -> true;
is_notable_config_key(_) ->
    false.

find_port_pid_loop(Tries, Delay) when Tries > 0 ->
    RV = ns_ports_manager:find_port(ns_server:get_babysitter_node(), memcached),
    case RV of
        Pid when is_pid(Pid) ->
            Pid1 = supervisor_cushion:child_pid(Pid),
            ?log_debug("Found memcached port ~p", [Pid1]),
            Pid1;
        Other ->
            ?log_debug("Failed to obtain memcached port pid (~p). Will retry",
                       [Other]),
            timer:sleep(Delay),
            find_port_pid_loop(Tries - 1, Delay)
    end.

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

apply_changed_memcached_config(DifferentConfig, State) ->
    case ns_memcached:config_validate(DifferentConfig) of
        ok ->
            ?log_debug("New memcached config is hot-reloadable."),
            hot_reload_config(DifferentConfig, State, 10, []),
            {noreply, State#state{memcached_config = DifferentConfig}};
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

hot_reload_config(NewMcdConfig, State, Tries, LastErr) when Tries < 1 ->
    ale:error(?USER_LOGGER,
              "Unable to apply memcached config update that was supposed to "
              "succeed. Error: ~p. Giving up. Restart memcached to apply that "
              "config change. Updated keys: ~p",
              [LastErr, changed_keys(State#state.memcached_config,
                                     NewMcdConfig)]);
hot_reload_config(NewMcdConfig, State, Tries, _LastErr) ->
    FilePath = get_memcached_config_path(),
    PrevFilePath = FilePath ++ ".prev",

    %% lets double check everything
    {active, CurrentMcdConfig} =
        read_current_memcached_config(State#state.port_pid),
    true = (CurrentMcdConfig =:= State#state.memcached_config),

    %% now we save currently active config to .prev
    ok = misc:atomic_write_file(PrevFilePath, CurrentMcdConfig),
    %% if we crash here, .prev has copy of active memcached config and
    %% we'll be able to retry hot or cold config update
    ok = misc:atomic_write_file(FilePath, NewMcdConfig),

    case ns_memcached:config_reload() of
        ok ->
            delete_prev_config_file(),
            ale:info(?USER_LOGGER,
                     "Hot-reloaded memcached.json for config change of the "
                     "following keys: ~p",
                     [changed_keys(CurrentMcdConfig, NewMcdConfig)]),
            ok;
        Err ->
            ?log_error("Failed to reload memcached config. "
                       "Will retry. Error: ~p", [Err]),
            timer:sleep(1000),
            hot_reload_config(NewMcdConfig, State, Tries - 1, Err)
    end.

get_memcached_config_path() ->
    Path = ns_config:search_node_prop(ns_config:latest(), memcached,
                                      config_path),
    true = is_list(Path),
    Path.

read_current_memcached_config(McdPortServer) ->
    case ns_port_server:is_active(McdPortServer) of
        true ->
            FilePath = get_memcached_config_path(),
            PrevFilePath = FilePath ++ ".prev",
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
    case file:read_file(Path) of
        {ok, Contents} ->
            {ok, Contents};
        {error, Error} ->
            ?log_debug("Got ~p while trying to read active "
                       "memcached config from ~s", [Error, Path]),
            do_read_current_memcached_config(Rest)
    end.

memcached_config(Config) ->
    {value, McdParams0} = ns_config:search(Config, {node, node(), memcached}),
    {value, McdConf} = ns_config:search(Config, {node, node(),
                                                 memcached_config}),

    GlobalMcdParams = ns_config:search(Config, memcached, []),

    DefaultMcdParams = ns_config:search(Config,
                                        {node, node(), memcached_defaults}, []),

    McdParams = McdParams0 ++ GlobalMcdParams ++ DefaultMcdParams,

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

get_minidump_dir([], Params) ->
    list_to_binary(proplists:get_value(breakpad_minidump_dir_path, Params,
                                       proplists:get_value(log_path, Params))).

get_interfaces([], MCDParams) ->
    lists:filter(fun ({Props}) ->
                    proplists:get_value(port, Props) =/= undefined
                 end, generate_interfaces(MCDParams)).

ssl_minimum_protocol([], _Params) ->
    ns_ssl_services_setup:ssl_minimum_protocol(kv).

client_cert_auth([], _Params) ->
    Val = ns_ssl_services_setup:client_cert_auth(),

    case cluster_compat_mode:is_cluster_51() of
        true ->
            State = proplists:get_value(state, Val),
            Prefixes = [{[{K, list_to_binary(V)} || {K, V} <- Triple]} ||
                           Triple <- proplists:get_value(prefixes, Val, [])],
            {[{state, list_to_binary(State)}, {prefixes, Prefixes}]};
        false ->
            {[{K, list_to_binary(V)} || {K,V} <- Val]}
    end.

is_snappy_enabled([], _Params) ->
    is_snappy_enabled().

is_snappy_enabled() ->
    is_snappy_enabled(?VERSION_55).

is_snappy_enabled(FeatureVersion) ->
    Cfg = ns_config:latest(),

    %% Local snappy config > global snappy config > default snappy value.
    Default = ns_config:search_prop(Cfg, {node, node(), memcached_defaults},
                                    datatype_snappy, false),

    cluster_compat_mode:is_enabled(FeatureVersion) andalso
        ns_config:search_node_prop(Cfg, memcached, datatype_snappy, Default).

collections_enabled([], _Params) ->
    collections:enabled().

get_fallback_salt([], _Params) ->
    base64:encode(scram_sha:get_fallback_salt()).

get_external_users_push_interval([], _Params) ->
    max(menelaus_roles:external_auth_polling_interval() div 1000, 1).

get_external_auth_service([], _Params) ->
    is_external_auth_service_enabled().

is_external_auth_service_enabled() ->
    SaslauthdEnabled =
        proplists:get_value(enabled, saslauthd_auth:build_settings(), false),
    LDAPEnabled = ldap_util:get_setting(authentication_enabled),
    SaslauthdEnabled or LDAPEnabled.

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
    {[{<<"tls 1.2">>, Ciphers12},
      {<<"tls 1.3">>, Ciphers13}]}.

format_ciphers(RFCCipherNames) ->
    OpenSSLNames = [Name || C <- RFCCipherNames,
                            Name <- [ciphers:openssl_name(C)],
                            Name =/= undefined],
    iolist_to_binary(lists:join(":", OpenSSLNames)).

get_ssl_cipher_order([], _Params) ->
    ns_ssl_services_setup:honor_cipher_order(kv).

get_afamily_type(AFamily) ->
    Required = ns_config:read_key_fast({node, node(), address_family}, inet),
    case AFamily of
        Required -> <<"required">>;
        _ -> <<"optional">>
    end.

generate_interfaces(MCDParams) ->
    SSL = {[{key, list_to_binary(ns_ssl_services_setup:memcached_key_path())},
            {cert,
             list_to_binary(ns_ssl_services_setup:memcached_cert_path())}]},
    GetPort = fun (Port) ->
                      {Port, Value} = lists:keyfind(Port, 1, MCDParams),
                      Value
              end,
    InterProps = [{[{port, GetPort(port)}]},

                  {[{port, GetPort(dedicated_port)},
                    {system, true}]},

                  {[{port, GetPort(ssl_port)},
                    {ssl, SSL}]},

                  {[{port, GetPort(dedicated_ssl_port)},
                    {system, true},
                    {ssl, SSL}]}],

    IPv4Interfaces = lists:map(
                       fun ({Props}) ->
                               IsSSL = proplists:is_defined(ssl, Props),
                               Additonal = [{host, get_host(inet, IsSSL)},
                                            {ipv4, get_afamily_type(inet)},
                                            {ipv6, <<"off">>}],
                               {Props ++ Additonal}
                       end, InterProps),
    IPv6Interfaces = lists:map(
                       fun ({Props}) ->
                               IsSSL = proplists:is_defined(ssl, Props),
                               Additonal = [{host, get_host(inet6, IsSSL)},
                                            {ipv4, <<"off">>},
                                            {ipv6, get_afamily_type(inet6)}],
                               {Props ++ Additonal}
                       end, InterProps),
    IPv4Interfaces ++ IPv6Interfaces.

get_host(Proto, IsSSL) ->
    case (not IsSSL) andalso misc:disable_non_ssl_ports() of
        true ->
            list_to_binary(misc:localhost(Proto, []));
        false ->
            <<"*">>
    end.
