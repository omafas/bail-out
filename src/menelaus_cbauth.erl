%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(menelaus_cbauth).

-export([handle_cbauth_post/1,
         handle_extract_user_from_cert_post/1,
         handle_rpc_connect/3]).

-behaviour(gen_server).

-export([start_link/0, sync/0, sync/1, stats/0]).


-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(worker, {pid :: pid(),
                 label :: string(),
                 version :: string() | internal,
                 mref :: reference(),
                 connection :: pid()}).

-record(state, {cbauth_info :: map(),
                workers :: list(),
                cert_version,
                client_cert_version,
                client_cert_auth_version}).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(VERSION_1, "v1").
-define(WORKER_SYNC_TIMEOUT, 5000).

handle_rpc_connect(?VERSION_1, Label, Req) ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            case ns_cluster_membership:get_cluster_membership(node()) of
                active ->
                    validator:handle(
                      fun (Params) ->
                              json_rpc_connection_sup:handle_rpc_connect(
                                Label ++ "-auth",
                                misc:update_proplist(
                                  Params,
                                  [{type, auth}, {version, ?VERSION_1}]), Req)
                      end, Req, qs,
                      [validator:integer(heartbeat, 1, infinity, _),
                       validator:no_duplicates(_),
                       validator:unsupported(_)]);
                Other ->
                    ?log_info(
                       "Reject the revrpc connection from ~s because node is "
                       "~p", [Label, Other]),
                    menelaus_util:reply_text(Req, "Node is not active", 503)
            end;
        false ->
            ?log_info("Reject the revrpc connection from ~s because the "
                      "cluster is not provisioned", [Label]),
            menelaus_util:reply_text(Req, "Cluster is not provisioned", 503)
    end;
handle_rpc_connect(_, _Label, Req) ->
    menelaus_util:reply_text(Req, "Version is not supported", 400).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

sync() ->
    sync(node()).

sync(Node) ->
    sync(Node, ?WORKER_SYNC_TIMEOUT).

sync(Node, WorkerSyncTimeout) ->
    InternalConnections =
        gen_server:call({?MODULE, Node}, get_internal_connections, infinity),
    %% If the above call succeeds, but a worker exits before we make the below
    %% sync call, then we would incorrectly crash the caller of this function.
    %% However, if we simply ignore all exceptions, then other cases like
    %% timeouts would incorrectly be ignored. As such, we need to catch any
    %% noproc errors, but allow all other exeptions to be propogated to the
    %% caller
    misc:parallel_map(
      fun (Pid) ->
              try menelaus_cbauth_worker:sync(Pid, WorkerSyncTimeout)
              catch exit:{noproc, _} ->
                      ?log_error("Process ~p no longer exists", [Pid])
              end
      end, InternalConnections, infinity).

stats() ->
    InternalConnections = gen_server:call(?MODULE, get_internal_connections),
    Results =
        misc:parallel_map(?cut(catch(menelaus_cbauth_worker:collect_stats(_))),
                          InternalConnections, infinity),
    lists:filtermap(fun({ok, Res}) ->
                            {true, Res};
                       (_) ->
                            false
                    end, Results).

init([]) ->
    erlang:process_flag(trap_exit, true),
    ns_pubsub:subscribe_link(json_rpc_events, fun json_rpc_event/1),
    ns_pubsub:subscribe_link(ns_node_disco_events, fun node_disco_event/1),
    chronicle_compat_events:subscribe(fun is_interesting/1,
                                      fun handle_config_event/1),
    ns_pubsub:subscribe_link(user_storage_events, fun user_storage_event/1),
    ns_pubsub:subscribe_link(ssl_service_events, fun ssl_service_event/1),
    json_rpc_connection_sup:reannounce(),
    {ok, #state{cert_version = new_cert_version(),
                client_cert_auth_version = client_cert_auth_version(),
                client_cert_version = new_cert_version(),
                cbauth_info = maps:new(),
                workers = []}}.

new_cert_version() ->
    misc:rand_uniform(0, 16#100000000).

json_rpc_event({_Msg, Label, Params, _Pid} = Event) ->
    case proplists:get_value(type, Params) of
        auth ->
            gen_server:cast(?MODULE, Event);
        _ ->
            case is_cbauth_connection(Label) of
                true ->
                    ok = gen_server:cast(?MODULE, Event);
                false ->
                    ok
            end
    end.

node_disco_event(_Event) ->
    ?MODULE ! maybe_notify_cbauth.

handle_config_event(client_cert_auth) ->
    ?MODULE ! client_cert_auth_event;
handle_config_event({node, Node, membership}) when Node =:= node() ->
    ?MODULE ! node_status_changed;
handle_config_event(_) ->
    ?MODULE ! maybe_notify_cbauth.

user_storage_event(_Event) ->
    ?MODULE ! maybe_notify_cbauth.

ssl_service_event(Event) ->
    ?MODULE ! {ssl_service_event, Event}.

terminate(_Reason, State = #state{workers = Workers}) ->
    WorkerPids = [Pid || #worker{pid = Pid} <- Workers],

    [erlang:demonitor(MRef, [flush]) || #worker{mref = MRef} <- Workers],

    misc:terminate_and_wait(WorkerPids, shutdown),

    %% Terminate all external revrpc connections
    %% This is needed because revrpc connections
    %% survive ns_server restart and therefore it can happen that external
    %% cbauth connection is still alive when users database is erased when
    %% node leaves the cluster.
    terminate_external_connections(State).

terminate_external_connections(#state{workers = Workers}) ->
    ToTerminate = [Pid || #worker{version = V, connection = Pid} <- Workers,
                          V =/= internal],
    ?log_info("External connections to be terminated: ~p", [ToTerminate]),
    misc:terminate_and_wait(ToTerminate, shutdown).

code_change(_OldVsn, State, _) -> {ok, State}.

is_interesting(client_cert_auth) -> true;
is_interesting({node, _, services}) -> true;
is_interesting({service_map, _}) -> true;
is_interesting({node, _, membership}) -> true;
is_interesting({node, _, memcached}) -> true;
is_interesting({node, _, capi_port}) -> true;
is_interesting({node, _, ssl_capi_port}) -> true;
is_interesting({node, _, ssl_rest_port}) -> true;
is_interesting({node, _, local_resource_statuses}) -> true;
is_interesting(rest) -> true;
is_interesting(rest_creds) -> true;
is_interesting(cluster_compat_version) -> true;
is_interesting({node, _, is_enterprise}) -> true;
is_interesting(user_roles) -> true;
is_interesting(buckets) -> true;
is_interesting(cipher_suites) -> true;
is_interesting(honor_cipher_order) -> true;
is_interesting(ssl_minimum_protocol) -> true;
is_interesting(cluster_encryption_level) -> true;
is_interesting({security_settings, _}) -> true;
is_interesting({cbauth_cache_size, _, _}) -> true;
is_interesting({node, N, prometheus_auth_info}) when N =:= node() -> true;
is_interesting({node, N, uuid}) when N =:= node() -> true;
is_interesting(uuid) -> true;
is_interesting(Key) -> collections:key_match(Key) =/= false.

handle_call(get_internal_connections, _From,
            #state{workers = Workers} = State) ->
    {reply, [Pid || #worker{version = internal, pid = Pid} <- Workers], State};

handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({Msg, Label, Params, ConnectionPid},
            #state{workers = Workers,
                   cbauth_info = CBAuthInfo} = State) ->
    Version = proplists:get_value(version, Params, internal),
    OldInfo = maps:get(Version, CBAuthInfo, undefined),

    ?log_debug("Observed json rpc process ~p ~p",
               [{Label, Params, ConnectionPid}, Msg]),
    {Info, NewCBAuthInfo} =
        case OldInfo of
            undefined ->
                I = build_auth_info(Version, build_auth_info_ctx(), State),
                {I, maps:put(Version, I, CBAuthInfo)};
            _ ->
                {OldInfo, CBAuthInfo}
        end,
    {WorkerPid, NewWorkers} =
        case lists:keyfind(ConnectionPid, #worker.connection, Workers) of
            false ->
                {ok, {Pid, MRef}} =
                    menelaus_cbauth_worker:start_monitor(
                      Label, Version, ConnectionPid, Params),
                {Pid, [#worker{label = Label, version = Version,
                               mref = MRef, connection = ConnectionPid,
                               pid = Pid} | Workers]};
            #worker{pid = Pid, label = L, version = V} when
                  L =:= Label andalso V =:= Version ->
                {Pid, Workers}
        end,
    menelaus_cbauth_worker:notify(WorkerPid,
                                  personalize_info(Version, Label, Info)),
    {noreply, State#state{workers = NewWorkers, cbauth_info = NewCBAuthInfo}}.

handle_info({ssl_service_event, client_cert_changed}, State) ->
    self() ! maybe_notify_cbauth,
    {noreply, State#state{client_cert_version = new_cert_version()}};
handle_info({ssl_service_event, _}, State) ->
    self() ! maybe_notify_cbauth,
    {noreply, State#state{cert_version = new_cert_version()}};
handle_info(client_cert_auth_event, State) ->
    self() ! maybe_notify_cbauth,
    {noreply, State#state{client_cert_auth_version =
                              client_cert_auth_version()}};
handle_info(node_status_changed, State) ->
    self() ! maybe_notify_cbauth,
    case ns_cluster_membership:get_cluster_membership(node()) of
        active ->
            ok;
        Other ->
            ?log_info("Killing all external connections due to node status"
                      " changing to ~p", [Other]),
            terminate_external_connections(State)
    end,
    {noreply, State};
handle_info(maybe_notify_cbauth, State) ->
    misc:flush(maybe_notify_cbauth),
    {noreply, maybe_notify_cbauth(State)};
handle_info({'DOWN', MRef, _, Pid, Reason},
            #state{workers = Workers} = State) ->
    {value, #worker{mref = MRef} = W, NewWorkers} =
        lists:keytake(Pid, #worker.pid, Workers),
    ?log_debug("Observed worker process ~p died with reason ~p", [W,  Reason]),
    {noreply, State#state{workers = NewWorkers}};
handle_info({'EXIT', Pid, Reason}, State) ->
    ?log_error("Linked process ~p exited with ~p. Exiting.", [Pid, Reason]),
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.

maybe_notify_cbauth(#state{workers = Workers,
                           cbauth_info = OldInfo} = State) ->
    NewInfo = build_auth_infos(State),
    maps:foreach(
      fun (Ver, Info) ->
              case maps:get(Ver, OldInfo, default) of
                  Info ->
                      ok;
                  _ ->
                      [menelaus_cbauth_worker:notify(
                         Pid, personalize_info(V, Label, Info)) ||
                          #worker{label = Label, pid = Pid,
                                  version = V} <- Workers]
              end
      end, NewInfo),
    State#state{cbauth_info = NewInfo}.

personalize_info(internal, Label, Info) ->
    MemcachedUser = [$@ | menelaus_cbauth_worker:strip_cbauth_suffix(Label)],

    TlsConfigLabel =
        case Label of
            "projector-cbauth" -> "index-cbauth";
            _ -> Label
        end,

    TLSConfig = proplists:get_value(TlsConfigLabel,
                                    proplists:get_value(tlsConfig, Info),
                                    {[{present, false}]}),

    CacheConfig =
        build_cache_config(Label, proplists:get_value(cacheConfig, Info)),

    Nodes = proplists:get_value(nodes, Info),
    NewNodes =
        lists:map(fun ({Node}) ->
                          OtherUsers = proplists:get_value(other_users, Node),
                          NewNode = case lists:member(MemcachedUser, OtherUsers) of
                                        true ->
                                            lists:keyreplace(user, 1, Node,
                                                             {user, list_to_binary(MemcachedUser)});
                                        false ->
                                            Node
                                    end,
                          {lists:keydelete(other_users, 1, NewNode)}
                  end, Nodes),

    %% Only report statuses applicable to the service
    GuardrailStatuses =
        lists:flatmap(
          fun ({Service, Statuses}) when Service =:= Label ->
                  Statuses;
              (_) ->
                  []
          end, proplists:get_value(guardrailStatuses, Info)),

    misc:update_proplist(Info,
                         [{specialUser, erlang:list_to_binary(MemcachedUser)},
                          {nodes, NewNodes},
                          {tlsConfig, TLSConfig},
                          {cacheConfig, CacheConfig},
                          {guardrailStatuses, GuardrailStatuses}]);
personalize_info(_Version, _Label, Info) ->
    Info.

build_node_info(N, Config, Snapshot) ->
    build_node_info(N, ns_config:search_node_prop(
                         N, Config, memcached, admin_user), Config, Snapshot).

build_node_info(_N, undefined, _Config, _Snapshot) ->
    undefined;
build_node_info(N, User, Config, Snapshot) ->
    IncludedServices =
        case config_profile:get_bool({cbauth, include_non_active_services}) of
            true ->
                ns_cluster_membership:node_services(Snapshot, N);
            false ->
                ns_cluster_membership:node_active_services(Snapshot, N)
        end,
    Services = [rest | IncludedServices],
    Ports0 = [Port || {_Key, Port} <- service_ports:get_ports_for_services(
                                        N, Config, Services)],

    Ports =
        case N =:= node() andalso not lists:member(kv, Services) of
            true ->
                [service_ports:get_port(memcached_port, Config) | Ports0];
            false ->
                Ports0
        end,

    Host = misc:extract_node_address(N),
    Local = case node() of
                N ->
                    [{local, true}];
                _ ->
                    []
            end,
    Password = ns_config_auth:get_password(N, Config, special),
    {[{host, erlang:list_to_binary(Host)},
      {user, erlang:list_to_binary(User)},
      {other_users, ns_config:search_node_prop(N, Config, memcached, other_users, [])},
      {password, erlang:list_to_binary(Password)},
      {ports, Ports}] ++ Local}.

-define(AUTH_CHECK_ENDPOINT, "/_cbauth").
-define(PERM_CHECK_ENDPOINT, "/_cbauth/checkPermission").
-define(EXTRACT_USER_ENDPOINT, "/_cbauth/extractUserFromCert").

versions() ->
    [internal, ?VERSION_1].

build_auth_infos(State = #state{workers = Workers}) ->
    Ctx = build_auth_info_ctx(),
    Versions = lists:usort([V || #worker{version = V} <- Workers]),
    maps:from_list(
      [{V, build_auth_info(V, Ctx, State)} || V <- versions(),
                                              lists:member(V, Versions)]).

build_auth_info_ctx() ->
    Config = ns_config:get(),
    Snapshot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(all, _),
           ns_cluster_membership:fetch_snapshot(_)], #{ns_config => Config}),

    AuthVersion = auth_version(Config),
    PermissionsVersion =
        menelaus_web_rbac:check_permissions_url_version(Snapshot),
    CcaState = ns_ssl_services_setup:client_cert_auth_state(),
    {AuthVersion, PermissionsVersion, CcaState, Config, Snapshot}.


%% this function promises to the external clients that any client with
%% corresponding version of cbauth would be compatible. in order to make
%% any changes, the version should be increased and ns_server should still
%% support all the previous versions.
build_auth_info(?VERSION_1, {AuthVersion, PermissionsVersion, CcaState, _Config,
                             _Snapshot},
                #state{client_cert_auth_version = ClientCertAuthVersion}) ->
    {value, NodeUuid} = ns_config:search_node(uuid),
    [{authCheckEndpoint, <<?AUTH_CHECK_ENDPOINT>>},
     {authVersion, AuthVersion},
     {permissionCheckEndpoint, <<?PERM_CHECK_ENDPOINT>>},
     {permissionsVersion, PermissionsVersion},
     {clientCertAuthVersion, ClientCertAuthVersion},
     {extractUserFromCertEndpoint, <<?EXTRACT_USER_ENDPOINT>>},
     {clientCertAuthState, list_to_binary(CcaState)},
     {nodeUUID, NodeUuid},
     {clusterUUID, menelaus_web:get_uuid()}];

%% we free to modify the output of this function as soon as the golang client
%% code is modified accordingly
build_auth_info(internal, {AuthVersion, PermissionsVersion, CcaState, Config,
                           Snapshot},
                #state{cert_version = CertVersion,
                       client_cert_version = ClientCertVersion,
                       client_cert_auth_version = ClientCertAuthVersion}) ->
    Nodes = lists:foldl(fun (Node, Acc) ->
                                case build_node_info(Node, Config, Snapshot) of
                                    undefined ->
                                        Acc;
                                    Info ->
                                        [Info | Acc]
                                end
                        end, [], ns_cluster_membership:nodes_wanted(Snapshot)),

    Port = service_ports:get_port(rest_port, Config),
    AuthCheckURL = misc:local_url(Port, ?AUTH_CHECK_ENDPOINT, []),
    PermissionCheckURL = misc:local_url(Port, ?PERM_CHECK_ENDPOINT, []),
    UuidCheckURL = misc:local_url(Port, "/_cbauth/getUserUuid", []),
    EUserFromCertURL = misc:local_url(Port, ?EXTRACT_USER_ENDPOINT, []),
    UserBucketsURL = misc:local_url(Port, "/_cbauth/getUserBuckets", []),
    ClusterDataEncrypt = misc:should_cluster_data_be_encrypted(),
    DisableNonSSLPorts = misc:disable_non_ssl_ports(),
    TLSServices = menelaus_web_settings:services_with_security_settings(),
    SpecialPasswords = ns_config_auth:get_special_passwords(
                         dist_manager:this_node(), Config),
    SpecialPasswordsBin = [list_to_binary(P) || P <- SpecialPasswords],
    [{nodes, Nodes},
     {authCheckURL, list_to_binary(AuthCheckURL)},
     {permissionCheckURL, list_to_binary(PermissionCheckURL)},
     {permissionsVersion, PermissionsVersion},
     {uuidCheckURL, list_to_binary(UuidCheckURL)},
     {userBucketsURL, list_to_binary(UserBucketsURL)},
     {userVersion, user_version()},
     {authVersion, AuthVersion},
     {certVersion, CertVersion},
     {clientCertVersion, ClientCertVersion},
     {extractUserFromCertURL, list_to_binary(EUserFromCertURL)},
     {clientCertAuthState, list_to_binary(CcaState)},
     {clientCertAuthVersion, ClientCertAuthVersion},
     {clusterEncryptionConfig, {[{encryptData, ClusterDataEncrypt},
                                 {disableNonSSLPorts, DisableNonSSLPorts}]}},
     {specialPasswords, SpecialPasswordsBin},
     {tlsConfig, [tls_config(S, Config) || S <- TLSServices]},
     {cacheConfig, build_cache_size_overrides(Config)},
     {guardrailStatuses, build_guardrail_statuses(Config)}].

tls_config(Service, Config) ->
    Label = service_to_label(Service),
    %% Golang TLS used by services index, ftx, n1ql, and eventing, doesn't allow
    %% configuring TLS 1.3 cipherSuites, see,
    %% https://golang.org/pkg/crypto/tls/#Config.
    %%
    %% This means that golang will,
    %% 1. Honor TLS 1.2 and TLS 1.1 cipherSuites if specified, i.e.,
    %%    only the TLS 1.2, and TLS 1.1 ciphers on this list are used.
    %% 2. If only TLS 1.3 cipher are specified in cipherSuites, TLS 1.2 and
    %%    TLS 1.1 ciphers are not used.
    %% 3. Allow all TLS 1.3 ciphers to be used, even if just a few/none are
    %%    specified.
    Ciphers = ciphers(Service, Config),
    Order = ns_ssl_services_setup:honor_cipher_order(Service, Config),
    CipherInts = lists:map(fun (<<I:16/unsigned-integer>>) -> I end,
                           [ciphers:code(N) || N <- Ciphers]),
    CipherOpenSSLNames = [N2 || N <- Ciphers, N2 <- [ciphers:openssl_name(N)],
                                N2 =/= undefined],
    MinTLSVsn = ns_ssl_services_setup:ssl_minimum_protocol(Service, Config),
    PassFun = ns_secrets:get_pkey_pass(node_cert),
    PassOpt = case PassFun() of
                  undefined -> [];
                  P -> [{privateKeyPassphrase, base64:encode(P)}]
              end,
    ClientPassFun = ns_secrets:get_pkey_pass(client_cert),
    ClientPassOpt = case ClientPassFun() of
                        undefined -> [];
                        P2 -> [{clientPrivateKeyPassphrase, base64:encode(P2)}]
                    end,
    {Label,
     {[{present, true},
       {minTLSVersion, MinTLSVsn},
       {cipherOrder, Order},
       {ciphers, CipherInts},
       {cipherNames, Ciphers},
       {cipherOpenSSLNames, CipherOpenSSLNames}] ++ PassOpt ++ ClientPassOpt}}.

ciphers(Service, Config) ->
    case ns_ssl_services_setup:configured_ciphers_names(Service, Config) of
        [] -> default_cbauth_ciphers();
        List -> List
    end.

default_cbauth_ciphers() ->
    %% Backward compatibility
    %% ssl_ciphers_strength is obsolete and should not be used in
    %% new installations
    Names = lists:flatmap(
              fun (high) -> ciphers:high();
                  (medium) -> ciphers:medium()
              end, ns_config:read_key_fast(ssl_ciphers_strength, [high])),
    ciphers:only_known(Names).

auth_version(Config) ->
    B = term_to_binary(
          [ns_config_auth:get_admin_creds(Config),
           menelaus_users:get_auth_version(),
           ns_config:search_node(Config, prometheus_auth_info)]),
    base64:encode(crypto:hash(sha, B)).

user_version() ->
    B = term_to_binary([menelaus_users:get_users_version()]),
    base64:encode(crypto:hash(sha, B)).

client_cert_auth_version() ->
    B = term_to_binary(ns_ssl_services_setup:client_cert_auth()),
    base64:encode(crypto:hash(sha, B)).

handle_cbauth_post(Req) ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            {User, Domain} = menelaus_auth:get_identity(Req),
            UUID = menelaus_users:get_user_uuid({User, Domain}),
            %% Services don't know anything about localtoken
            %% Make it look like a regular admin for them
            DomainForServices = case Domain == local_token of
                                    true -> admin;
                                    false -> Domain
                                end,
            menelaus_util:reply_json(
              Req, {[{user, erlang:list_to_binary(User)},
                     {domain, DomainForServices}] ++
                        [{uuid, UUID} || UUID =/= undefined]});
        false ->
            menelaus_util:require_auth(Req)
    end.

handle_extract_user_from_cert_post(Req) ->
    CertBin = mochiweb_request:recv_body(Req),
    try
        case menelaus_auth:extract_identity_from_cert(CertBin) of
            auth_failure ->
                ns_audit:auth_failure(Req),
                ns_server_stats:notify_counter(<<"rest_request_auth_failure">>),
                menelaus_util:reply_json(Req, <<"Auth failure">>, 401);
            temporary_failure ->
                Msg = <<"Temporary error occurred. Please try again later.">>,
                menelaus_util:reply_json(Req, Msg, 503);
            {User, Domain} ->
                UUID = menelaus_users:get_user_uuid({User, Domain}),
                menelaus_util:reply_json(
                  Req, {[{user, list_to_binary(User)},
                         {domain, Domain}] ++
                            [{uuid, UUID} || UUID =/= undefined]})
        end
    catch
        _:_ ->
            ns_audit:auth_failure(Req),
            ns_server_stats:notify_counter(<<"rest_request_auth_failure">>),
            menelaus_util:reply_json(Req, <<"Auth failure">>, 401)
    end.

is_cbauth_connection(Label) ->
    lists:suffix("-cbauth", Label).

build_cache_size_overrides(Config) ->
    ns_config:fold(
      fun ({cbauth_cache_size, Service, ConfigKey}, V, Map)
          when is_integer(V) andalso V >= 1 andalso V =< 65536 ->
              maps:put({service_to_label(Service), ConfigKey}, V, Map);
          (_, _ ,Map) ->
              Map
      end, maps:new(), Config).

build_cache_config(Label, Overrides) ->
    {lists:map(
       fun ({Key, JsonKey, Default}) ->
               {JsonKey, maps:get({Label, Key}, Overrides, Default)}
       end, cache_size_defaults())}.

cache_size_defaults() ->
    [{uuid_cache_size, uuidCacheSize, 256},
     {user_bkts_cache_size, userBktsCacheSize, 1024},
     {up_cache_size, upCacheSize, 1024},
     {auth_cache_size, authCacheSize, 256},
     {client_cert_cache_size, clientCertCacheSize, 256}].

build_guardrail_statuses(Config) ->
    lists:map(
      fun (Service) ->
              {service_to_label(Service),
               build_guardrail_statuses(Service, Config)}
      end,
      [index]).

build_guardrail_statuses(index, Config) ->
    lists:filtermap(
      fun (Resource) ->
              case guardrail_monitor:get_local_status(
                     {index, Resource}, Config, ok) of
                  ok ->
                      false;
                  Severity ->
                      {true,
                       {[{resource, Resource},
                         {severity, Severity}]}}
              end
      end, [disk_usage, resident_ratio]).

service_to_label(n1ql) ->
    "cbq-engine-cbauth";
service_to_label(xdcr) ->
    "goxdcr-cbauth";
service_to_label(Service) ->
    atom_to_list(Service) ++ "-cbauth".

-ifdef(TEST).
-define(SERVICE, "<service>").
-define(LABEL, "<service>-cbauth").
-define(HEARTBEAT_TIME_S, 1).
%% Short timeout for waiting to confirm that something doesn't immediately
%% happen, without waiting so long that it significantly increases test duration
-define(SHORT_TIMEOUT, 500).
%% Timeout for things that should eventually happen, but not necessarily
%% immediately
-define(LONG_TIMEOUT, 60_000).

setup_t() ->
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),

    fake_ns_config:setup(),
    fake_chronicle_kv:setup(),
    %% Test setups return a map of pids for later shutdown in the teardown
    PidMap = mock_helpers:setup_mocks([json_rpc_events,
                                       ns_node_disco_events,
                                       user_storage_events,
                                       ssl_service_events,
                                       json_rpc_connection_sup,
                                       ns_ssl_services_setup,
                                       menelaus_users,
                                       ns_secrets,
                                       testconditions]),

    %% Set config values for a few keys, since these are needed for greater
    %% coverage, and to avoid errors
    fake_chronicle_kv:update_snapshot(#{nodes_wanted => [node()],
                                        bucket_names => []}),
    fake_ns_config:update_snapshot([{rest, [{port, 8091}]},
                                    {rest_creds, placeholder},
                                    {memcached, [{admin_user, "user"},
                                                 {admin_pass, "pass"}]}]),

    meck:new(menelaus_cbauth_worker, [passthrough]),

    {ok, Pid} = menelaus_cbauth:start_link(),
    start_fake_json_rpc_connection(?LABEL),
    PidMap#{?MODULE => Pid}.

teardown_t(PidMap) ->
    Name = list_to_atom("json_rpc_connection-" ++ ?LABEL),
    erlang:unregister(Name),
    mock_helpers:shutdown_processes(PidMap),
    fake_chronicle_kv:teardown(),
    fake_ns_config:teardown(),
    meck:unload().

start_fake_json_rpc_connection(Label) ->
    Name = list_to_atom("json_rpc_connection-" ++ Label),
    Pid = self(),
    true = erlang:register(Name, Pid),
    meck:expect(json_rpc_connection, perform_call,
                fun (_Label, _Call, _EJsonArg, _Opts) ->
                        {ok, true}
                end),
    gen_event:notify(json_rpc_events,
                     {started, Label, [internal,
                                       {heartbeat, ?HEARTBEAT_TIME_S}],
                      self()}),
    %% Wait for initial notify to be handled, to avoid racy tests
    meck:wait(1, menelaus_cbauth_worker, notify, ['_', '_'], ?LONG_TIMEOUT).

cbauth_init_t() ->
    %% UpdateDB gets called once almost immediately
    meck:wait(json_rpc_connection, perform_call,
              ['_', "AuthCacheSvc.UpdateDB", '_', '_'], ?LONG_TIMEOUT),
    %% Heartbeat gets called once
    meck:wait(json_rpc_connection, perform_call,
              ['_', "AuthCacheSvc.Heartbeat", '_', '_'],
              2_000 * ?HEARTBEAT_TIME_S),
    %% Heartbeat gets called again
    meck:reset(json_rpc_connection),
    meck:wait(json_rpc_connection, perform_call,
              ['_', "AuthCacheSvc.Heartbeat", '_', '_'],
              2_000 * ?HEARTBEAT_TIME_S),
    %% UpdateDB isn't called immediately again
    ?assertError(timeout,
                 meck:wait(json_rpc_connection, perform_call,
                           ['_', "AuthCacheSvc.UpdateDB", '_', '_'],
                           ?SHORT_TIMEOUT)).

cbauth_sync_t() ->
    %% UpdateDB gets called once immediately
    meck:wait(json_rpc_connection, perform_call,
              ['_', "AuthCacheSvc.UpdateDB", '_', '_'],
              ?LONG_TIMEOUT),
    [ok] = sync(),
    %% Ensure that the next perform_call doesn't immediately return until after
    %% the sync call would time out
    meck:expect(json_rpc_connection, perform_call,
                fun (_, "AuthCacheSvc.UpdateDB", _, _) ->
                        timer:sleep(2 * ?SHORT_TIMEOUT);
                    (_, _, _, _) ->
                        ok
                end),
    meck:reset(menelaus_cbauth_worker),
    %% Force an update by updating the snapshot
    fake_ns_config:update_snapshot(rest, [{port, 8092}]),
    %% Wait for the update to start being handled
    meck:wait(menelaus_cbauth_worker, notify, ['_', '_'], ?LONG_TIMEOUT),
    %% Sync with timeout half the perform_call sleep time, to ensure it gets hit
    ?assertExit({timeout, _}, sync(node(), ?SHORT_TIMEOUT)).

cbauth_stats_t() ->
    meck:expect(json_rpc_connection, perform_call,
                fun (_Label, "AuthCacheSvc.GetStats", _EJsonArg, _Opts) ->
                        {ok, {[ok]}};
                    (_Label, _Call, _EJsonArg, _Opts) ->
                        {ok, true}
                end),
    [{<<?SERVICE>>, ok}] = stats(),
    meck:expect(json_rpc_connection, perform_call,
                fun (_Label, "AuthCacheSvc.GetStats", _EJsonArg, _Opts) ->
                        {error, error};
                    (_Label, _Call, _EJsonArg, _Opts) ->
                        {ok, true}
                end),
    [] = stats().

cbauth_many_notify_t() ->
    meck:wait(1, json_rpc_connection, perform_call,
              ['_', "AuthCacheSvc.UpdateDB", '_', '_'], ?LONG_TIMEOUT),
    %% Ensure that the next perform_call doesn't immediately return
    meck:expect(json_rpc_connection, perform_call,
                fun (_, "AuthCacheSvc.UpdateDB", _, _) ->
                        receive return_from_call -> {ok, true}
                        after ?LONG_TIMEOUT -> error(timeout) end;
                    (_Label, _Call, _EJsonArg, _Opts) ->
                        {ok, true}
                end),
    %% Send three updates, ensuring three notify calls with different values:
    %% - The first is to get the worker stuck in the above meck:expect
    %% - The second is to provide a value, which we want to become stale
    %% - The third will be the new value, which should get used, instead of the
    %%   stale value, after the first update is processed
    lists:foreach(
      fun (N) ->
              fake_ns_config:update_snapshot(rest, [{port, N}]),
              meck:wait(N, menelaus_cbauth_worker, notify, ['_', '_'],
                        ?LONG_TIMEOUT)
      end, [2, 3, 4]),
    %% Extract value from next update
    meck:expect(json_rpc_connection, perform_call,
                fun (_, "AuthCacheSvc.UpdateDB", {Info}, _) ->
                        [{Node}] = proplists:get_value(nodes, Info, []),
                        [undefined, Port] = proplists:get_value(ports, Node),
                        %% Expect the last value, not the stale value
                        ?assertEqual(4, Port),
                        {ok, true};
                    (_Label, _Call, _EJsonArg, _Opts) ->
                        {ok, true}
                end),
    %% Release worker
    WorkerPid = whereis(list_to_atom("menelaus_cbauth_worker-" ++ ?LABEL)),
    WorkerPid ! return_from_call,
    %% Wait for both the stuck and final call to return
    meck:wait(3, json_rpc_connection, perform_call,
              ['_', "AuthCacheSvc.UpdateDB", '_', '_'], ?LONG_TIMEOUT).

trigger_notification(ns_node_disco_events) ->
    %% To avoid needing to trick ns_node_disco into recognising fake nodes, just
    %% make the node list different by removing the only node.
    %% Note, this tests ns_node_disco_events as well as chronicle_kv, as the
    %% event handler for chronicle_compat_events in this module does not cover
    %% the nodes_wanted key (although the ns_node_disco handler does cover this
    %% key, hence how this tests the ns_node_disco_events handler)
    fake_chronicle_kv:update_snapshot(nodes_wanted, []);
trigger_notification(ns_config_events) ->
    %% This is just an arbitrary key in ns_config that we use for the cbauth
    %% info, that is covered by the chronicle_compat_events handler
    fake_ns_config:update_snapshot(rest, [{port, 8092}]);
trigger_notification(chronicle_kv) ->
    %% While the bucket_names key isn't subscribed to, the collections key is,
    %% and we need to update both for the info to be updated
    fake_chronicle_kv:update_snapshot(
      #{bucket_names => ["test"],
        {bucket, "test", collections} => [{uid, 0}],
        {bucket, "test", props} => [{type, membase}]});
trigger_notification(user_storage_events) ->
    meck:expect(menelaus_users, get_users_version, 0, {1, 0}),
    %% It isn't worth the complexity to trigger a user version change through
    %% less artificial means, so just manually trigger the event
    gen_event:notify(user_storage_events, event);
trigger_notification(ssl_service_events) ->
    %% It isn't worth the complexity to trigger an ssl_service_event through
    %% less artificial means, so just manually trigger the event
    gen_event:notify(ssl_service_events, client_cert_changed).

cbauth_notify_tests() ->
    %% Check that expected events cause notifications
    [{"cbauth notify " ++ atom_to_list(EventManager) ++ " test",
      fun () ->
              %% UpdateDB gets called once almost immediately
              meck:wait(json_rpc_connection, perform_call,
                        ['_', "AuthCacheSvc.UpdateDB", '_', '_'],
                        ?LONG_TIMEOUT),
              meck:reset(json_rpc_connection),
              trigger_notification(EventManager),
              meck:wait(json_rpc_connection, perform_call,
                        ['_', "AuthCacheSvc.UpdateDB", '_', '_'],
                        ?LONG_TIMEOUT)
      end} || EventManager <- [ns_node_disco_events,
                               ns_config_events,
                               chronicle_kv,
                               user_storage_events,
                               ssl_service_events]].

cbauth_test_() ->
    {foreach, fun setup_t/0, fun teardown_t/1,
     [{"cbauth init test", fun cbauth_init_t/0},
      {"cbauth sync test", fun cbauth_sync_t/0},
      {"cbauth stats test", fun cbauth_stats_t/0},
      {"cbauth many notify test", fun cbauth_many_notify_t/0}
     | cbauth_notify_tests()]}.
-endif.
