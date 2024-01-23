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

-export([start_link/0]).


-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(rpc_process, {label :: string(),
                      version :: string() | internal,
                      mref :: reference(),
                      heartbeat_interval :: integer() | undefined,
                      last_heartbeat :: integer()}).

-record(state, {cbauth_info :: map(),
                rpc_processes :: map(),
                cert_version,
                client_cert_auth_version,
                timer :: misc:timer()}).

-include("ns_common.hrl").
-include("cut.hrl").

-define(VERSION_1, "v1").

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
                       validator:unsupported(_)]);
                Other ->
                    ?log_debug(
                       "Reject the revrpc connection from ~s because node is "
                       "~p", [Label, Other]),
                    menelaus_util:reply_text(Req, "Node is not active", 503)
            end;
        false ->
            ?log_debug("Reject the revrpc connection from ~s because the "
                       "cluster is not provisioned", [Label]),
            menelaus_util:reply_text(Req, "Cluster is not provisioned", 503)
    end;
handle_rpc_connect(_, _Label, Req) ->
    menelaus_util:reply_text(Req, "Version is not supported", 400).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
                cbauth_info = maps:new(),
                rpc_processes = maps:new(),
                timer = misc:create_timer(heartbeat)}}.

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

ssl_service_event(_Event) ->
    ?MODULE ! ssl_service_event.

terminate(_Reason, State) ->
    terminate_external_connections(State),
    ok.

terminate_external_connections(State = #state{rpc_processes = Processes}) ->
    {NewProcesses, ToWait} =
        maps:fold(
          fun (Pid, #rpc_process{version = internal} = P, {Acc, ToWaitAcc}) ->
                  {maps:put(Pid, P, Acc), ToWaitAcc};
              (Pid, #rpc_process{label = Label, mref = MRef},
               {Acc, ToWaitAcc}) ->
                  ?log_debug("Killing connection ~p with pid = ~p",
                             [Label, Pid]),
                  true = erlang:demonitor(MRef, [flush]),
                  exit(Pid, shutdown),
                  {Acc, [Pid | ToWaitAcc]}
          end, {#{}, []}, Processes),
    [misc:wait_for_process(P, infinity) || P <- ToWait],
    State#state{rpc_processes = NewProcesses}.

code_change(_OldVsn, State, _) -> {ok, State}.

is_interesting(client_cert_auth) -> true;
is_interesting({node, _, services}) -> true;
is_interesting({service_map, _}) -> true;
is_interesting({node, _, membership}) -> true;
is_interesting({node, _, memcached}) -> true;
is_interesting({node, _, capi_port}) -> true;
is_interesting({node, _, ssl_capi_port}) -> true;
is_interesting({node, _, ssl_rest_port}) -> true;
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
is_interesting(enforce_limits) -> true;
is_interesting({security_settings, _}) -> true;
is_interesting({node, N, prometheus_auth_info}) when N =:= node() -> true;
is_interesting({node, N, uuid}) when N =:= node() -> true;
is_interesting(uuid) -> true;
is_interesting(Key) -> collections:key_match(Key) =/= false.

register_heartbeat(P) ->
    P#rpc_process{last_heartbeat = erlang:monotonic_time(millisecond)}.

new_process(Label, Version, Pid, Params) ->
    MRef = erlang:monitor(process, Pid),
    #rpc_process{
       label = Label, version = Version, mref = MRef,
       heartbeat_interval =
           case proplists:get_value(heartbeat, Params) of
               undefined -> undefined;
               I -> I * 1000
           end,
       last_heartbeat = erlang:monotonic_time(millisecond)}.

handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({Msg, Label, Params, Pid},
            #state{rpc_processes = Processes,
                   cbauth_info = CBAuthInfo} = State) ->
    Version = proplists:get_value(version, Params, internal),
    OldInfo = maps:get(Version, CBAuthInfo, undefined),

    ?log_debug("Observed json rpc process ~p ~p", [{Label, Params, Pid}, Msg]),
    {Info, NewCBAuthInfo} =
        case OldInfo of
            undefined ->
                I = build_auth_info(Version, build_auth_info_ctx(), State),
                {I, maps:put(Version, I, CBAuthInfo)};
            _ ->
                {OldInfo, CBAuthInfo}
        end,
    NewProcesses =
        case notify_cbauth(Label, Version, Pid, Info) of
            error ->
                Processes;
            ok ->
                case maps:find(Pid, Processes) of
                    {ok, P = #rpc_process{label = L, version = V}} when
                          L =:= Label andalso V =:= Version ->
                        maps:update(Pid, register_heartbeat(P), Processes);
                    error ->
                        maps:put(Pid, new_process(Label, Version, Pid, Params),
                                 Processes)
                end
        end,
    NewState = State#state{rpc_processes = NewProcesses,
                           cbauth_info = NewCBAuthInfo},
    {noreply, process_heartbeats(NewState)}.

handle_info(ssl_service_event, State) ->
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
            {noreply, State};
        Other ->
            ?log_debug("Killing all external connections due to node status"
                       " changing to ~p", [Other]),
            {noreply, terminate_external_connections(State)}
    end;
handle_info(maybe_notify_cbauth, State) ->
    misc:flush(maybe_notify_cbauth),
    {noreply, maybe_notify_cbauth(State)};
handle_info({'DOWN', MRef, _, Pid, Reason},
            #state{rpc_processes = Processes} = State) ->
    {#rpc_process{mref = MRef} = P, NewProcesses} = maps:take(Pid, Processes),
    ?log_debug("Observed json rpc process ~p died with reason ~p",
               [P,  Reason]),
    {noreply, State#state{rpc_processes = NewProcesses}};
handle_info(heartbeat, State) ->
    {noreply, process_heartbeats(State)};
handle_info({'EXIT', Pid, Reason}, State) ->
    ?log_debug("Linked process ~p exited with ~p. Exiting.", [Pid, Reason]),
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.

process_heartbeats(#state{rpc_processes = Processes,
                          timer = Timer} = State) ->
    misc:flush(heartbeat),
    Now = erlang:monotonic_time(millisecond),
    {ToSend, NextTime} =
        maps:fold(
          fun (_, #rpc_process{heartbeat_interval = undefined}, Acc) ->
                  Acc;
              (Pid, #rpc_process{heartbeat_interval = I,
                                 last_heartbeat = Last} = P,
               {AccToSend, AccNextTime}) ->
                  GetNextTime =
                      fun (N) ->
                              case AccNextTime of
                                  undefined ->
                                      N;
                                  _ ->
                                      min(N, AccNextTime)
                              end
                      end,

                  case Last + I of
                      T when T =< Now ->
                          {[{Pid, P} | AccToSend], GetNextTime(I)};
                      Future ->
                          {AccToSend, GetNextTime(Future - Now)}
                  end
          end, {[], undefined}, Processes),

    Results = async:map(fun ({Pid, #rpc_process{label = Label} = P}) ->
                                {send_heartbeat(Label, Pid), Pid, P}
                        end, ToSend),

    NewProcesses =
        lists:foldl(
          fun ({ok, Pid, P}, AccProcesses) ->
                  maps:update(Pid, register_heartbeat(P), AccProcesses);
              ({error, _, _}, AccProcesses) ->
                  AccProcesses
          end, Processes, Results),

    NextTimer = case NextTime of
                    undefined ->
                        Timer;
                    _ ->
                        misc:arm_timer(NextTime, Timer)
                end,
    State#state{rpc_processes = NewProcesses, timer = NextTimer}.

maybe_notify_cbauth(#state{rpc_processes = Processes,
                           cbauth_info = OldInfo} = State) ->
    NewInfo = build_auth_infos(State),
    NewProcesses =
        maps:fold(
          fun (Ver, I, Acc) ->
                  case maps:get(Ver, OldInfo, default) of
                      I ->
                          Acc;
                      _ ->
                          notify_version(Ver, Acc, I)
                  end
          end, Processes, NewInfo),
    State#state{cbauth_info = NewInfo, rpc_processes = NewProcesses}.

notify_version(Ver, Processes, Info) ->
    maps:map(
      fun (Pid, #rpc_process{label = Label, version = V} = P) when V =:= Ver ->
              case notify_cbauth(Label, Ver, Pid, Info) of
                  ok ->
                      register_heartbeat(P);
                  error ->
                      P
              end;
          (_, P) ->
              P
      end, Processes).

personalize_info(Label, Info) ->
    "htuabc-" ++ ReversedTrimmedLabel = lists:reverse(Label),
    MemcachedUser = [$@ | lists:reverse(ReversedTrimmedLabel)],

    TlsConfigLabel =
        case Label of
            "projector-cbauth" -> "index-cbauth";
            _ -> Label
        end,

    TLSConfig = proplists:get_value(TlsConfigLabel,
                                    proplists:get_value(tlsConfig, Info),
                                    {[{present, false}]}),

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

    misc:update_proplist(Info,
                         [{specialUser, erlang:list_to_binary(MemcachedUser)},
                          {nodes, NewNodes},
                          {tlsConfig, TLSConfig}]).

notify_cbauth(Label, internal, Pid, Info) ->
    invoke_method(Label, "AuthCacheSvc.UpdateDB", Pid,
                  personalize_info(Label, Info));
notify_cbauth(Label, _, Pid, Info) ->
    invoke_method(Label, "AuthCacheSvc.UpdateDBExt", Pid, Info).

send_heartbeat(Label, Pid) ->
    TestCondition = list_to_atom(atom_to_list(?MODULE) ++ "_skip_heartbeats"),
    case testconditions:get(TestCondition) of
        false ->
            invoke_method(Label, "AuthCacheSvc.Heartbeat", Pid, []);
        true ->
            ?log_debug("Skip heartbeat for label ~p", [Label])
    end.

invoke_method(Label, Method, Pid, Info) ->
    try json_rpc_connection:perform_call(Label, Method, {Info}) of
        {error, method_not_found} ->
            error;
        {error, {rpc_error, _}} ->
            error;
        {error, Error} ->
            ?log_error("Error returned from go component ~p: ~p. "
                       "This shouldn't happen but crash it just in case.",
                       [{Label, Pid}, Error]),
            exit(Pid, Error),
            error;
        {ok, true} ->
            ok;
        {ok, null} ->
            ok
    catch exit:{noproc, _} ->
            ?log_debug("Process ~p is already dead", [{Label, Pid}]),
            error;
          exit:{Reason, _} ->
            ?log_debug("Process ~p has exited during the call with reason ~p",
                       [{Label, Pid}, Reason]),
            error
    end.

build_node_info(N, Config, Snapshot) ->
    build_node_info(N, ns_config:search_node_prop(
                         N, Config, memcached, admin_user), Config, Snapshot).

build_node_info(_N, undefined, _Config, _Snapshot) ->
    undefined;
build_node_info(N, User, Config, Snapshot) ->
    ActiveServices = [rest |
                      ns_cluster_membership:node_active_services(Snapshot, N)],
    Ports0 = [Port || {_Key, Port} <- service_ports:get_ports_for_services(
                                        N, Config, ActiveServices)],

    Ports =
        case N =:= node() andalso not lists:member(kv, ActiveServices) of
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
    {[{host, erlang:list_to_binary(Host)},
      {user, erlang:list_to_binary(User)},
      {other_users, ns_config:search_node_prop(N, Config, memcached, other_users, [])},
      {password,
       erlang:list_to_binary(ns_config:search_node_prop(N, Config, memcached, admin_pass))},
      {ports, Ports}] ++ Local}.

-define(AUTH_CHECK_ENDPOINT, "/_cbauth").
-define(PERM_CHECK_ENDPOINT, "/_cbauth/checkPermission").
-define(EXTRACT_USER_ENDPOINT, "/_cbauth/extractUserFromCert").

versions() ->
    [internal, ?VERSION_1].

build_auth_infos(State = #state{rpc_processes = Processes}) ->
    Ctx = build_auth_info_ctx(),
    Versions =
        lists:usort(
          [V || {_, #rpc_process{version = V}} <- maps:to_list(Processes)]),
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
    LimitsCheckURL = misc:local_url(Port, "/_cbauth/getUserLimits", []),
    UuidCheckURL = misc:local_url(Port, "/_cbauth/getUserUuid", []),
    EUserFromCertURL = misc:local_url(Port, ?EXTRACT_USER_ENDPOINT, []),
    ClusterDataEncrypt = misc:should_cluster_data_be_encrypted(),
    DisableNonSSLPorts = misc:disable_non_ssl_ports(),
    TLSServices = menelaus_web_settings:services_with_security_settings(),
    LimitsConfig = {[{enforceLimits,
                      cluster_compat_mode:should_enforce_limits()},
                     {userLimitsVersion,
                      menelaus_web_rbac:check_user_limits_version()}]},

    [{nodes, Nodes},
     {authCheckURL, list_to_binary(AuthCheckURL)},
     {permissionCheckURL, list_to_binary(PermissionCheckURL)},
     {permissionsVersion, PermissionsVersion},
     {limitsCheckURL, list_to_binary(LimitsCheckURL)},
     {uuidCheckURL, list_to_binary(UuidCheckURL)},
     {userVersion, user_version()},
     {authVersion, AuthVersion},
     {certVersion, CertVersion},
     {limitsConfig, LimitsConfig},
     {extractUserFromCertURL, list_to_binary(EUserFromCertURL)},
     {clientCertAuthState, list_to_binary(CcaState)},
     {clientCertAuthVersion, ClientCertAuthVersion},
     {clusterEncryptionConfig, {[{encryptData, ClusterDataEncrypt},
                                 {disableNonSSLPorts, DisableNonSSLPorts}]}},
     {tlsConfig, [tls_config(S, Config) || S <- TLSServices]}].

tls_config(Service, Config) ->
    Label = case Service of
                n1ql -> "cbq-engine-cbauth";
                _ -> atom_to_list(Service) ++ "-cbauth"
            end,
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
    PassFun = ns_secrets:get_pkey_pass(),
    PassOpt = case PassFun() of
                  undefined -> [];
                  P -> [{privateKeyPassphrase, base64:encode(P)}]
              end,
    {Label,
     {[{present, true},
       {minTLSVersion, MinTLSVsn},
       {cipherOrder, Order},
       {ciphers, CipherInts},
       {cipherNames, Ciphers},
       {cipherOpenSSLNames, CipherOpenSSLNames}] ++ PassOpt}}.

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
            menelaus_util:reply_json(
              Req, {[{user, erlang:list_to_binary(User)},
                     {domain, Domain}] ++
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
            menelaus_util:reply_json(Req, <<"Auth failure">>, 401)
    end.

is_cbauth_connection(Label) ->
    lists:suffix("-cbauth", Label).
