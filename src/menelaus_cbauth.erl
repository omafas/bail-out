%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2019 Couchbase, Inc.
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

-module(menelaus_cbauth).

-export([handle_cbauth_post/1,
         handle_extract_user_from_cert_post/1]).
-behaviour(gen_server).

-export([start_link/0]).


-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {cbauth_info = undefined,
                rpc_processes = [],
                cert_version,
                client_cert_auth_version}).

-include("ns_common.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ns_pubsub:subscribe_link(json_rpc_events, fun json_rpc_event/1),
    ns_pubsub:subscribe_link(ns_node_disco_events, fun node_disco_event/1),
    ns_pubsub:subscribe_link(ns_config_events, fun ns_config_event/1),
    ns_pubsub:subscribe_link(user_storage_events, fun user_storage_event/1),
    ns_pubsub:subscribe_link(ssl_service_events, fun ssl_service_event/1),
    json_rpc_connection_sup:reannounce(),
    {ok, #state{cert_version = new_cert_version(),
                client_cert_auth_version = client_cert_auth_version()}}.

new_cert_version() ->
    misc:rand_uniform(0, 16#100000000).

json_rpc_event({_, Label, _} = Event) ->
    case is_cbauth_connection(Label) of
        true ->
            ok = gen_server:cast(?MODULE, Event);
        false ->
            ok
    end.

node_disco_event(_Event) ->
    ?MODULE ! maybe_notify_cbauth.

ns_config_event({client_cert_auth, _}) ->
    ?MODULE ! client_cert_auth_event;
ns_config_event(Event) ->
    case is_interesting(Event) of
        true ->
            ?MODULE ! maybe_notify_cbauth;
        _ ->
            ok
    end.

user_storage_event(_Event) ->
    ?MODULE ! maybe_notify_cbauth.

ssl_service_event(_Event) ->
    ?MODULE ! ssl_service_event.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

is_interesting({{node, _, services}, _}) -> true;
is_interesting({{service_map, _}, _}) -> true;
is_interesting({{node, _, membership}, _}) -> true;
is_interesting({{node, _, memcached}, _}) -> true;
is_interesting({{node, _, capi_port}, _}) -> true;
is_interesting({{node, _, ssl_capi_port}, _}) -> true;
is_interesting({{node, _, ssl_rest_port}, _}) -> true;
is_interesting({rest, _}) -> true;
is_interesting({rest_creds, _}) -> true;
is_interesting({cluster_compat_version, _}) -> true;
is_interesting({{node, _, is_enterprise}, _}) -> true;
is_interesting({user_roles, _}) -> true;
is_interesting({buckets, _}) -> true;
is_interesting({cipher_suites, _}) -> true;
is_interesting({honor_cipher_order, _}) -> true;
is_interesting({ssl_minimum_protocol, _}) -> true;
is_interesting({cluster_encryption_level, _}) -> true;
is_interesting({{security_settings, _}, _}) -> true;
is_interesting(_) -> false.

handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({Msg, Label, Pid}, #state{rpc_processes = Processes,
                                      cbauth_info = CBAuthInfo} = State) ->
    ?log_debug("Observed json rpc process ~p ~p", [{Label, Pid}, Msg]),
    Info = case CBAuthInfo of
               undefined ->
                   build_auth_info(State);
               _ ->
                   CBAuthInfo
           end,
    NewProcesses = case notify_cbauth(Label, Pid, Info) of
                       error ->
                           Processes;
                       ok ->
                           case lists:keyfind({Label, Pid}, 2, Processes) of
                               false ->
                                   MRef = erlang:monitor(process, Pid),
                                   [{MRef, {Label, Pid}} | Processes];
                               _ ->
                                   Processes
                           end
                   end,
    {noreply, State#state{rpc_processes = NewProcesses,
                          cbauth_info = Info}}.

handle_info(ssl_service_event, State) ->
    self() ! maybe_notify_cbauth,
    {noreply, State#state{cert_version = new_cert_version()}};
handle_info(client_cert_auth_event, State) ->
    self() ! maybe_notify_cbauth,
    {noreply, State#state{client_cert_auth_version =
                              client_cert_auth_version()}};
handle_info(maybe_notify_cbauth, State) ->
    misc:flush(maybe_notify_cbauth),
    {noreply, maybe_notify_cbauth(State)};
handle_info({'DOWN', MRef, _, Pid, Reason},
            #state{rpc_processes = Processes} = State) ->
    {value, {MRef, {Label, Pid}}, NewProcesses} = lists:keytake(MRef, 1, Processes),
    ?log_debug("Observed json rpc process ~p died with reason ~p", [{Label, Pid}, Reason]),
    {noreply, State#state{rpc_processes = NewProcesses}};

handle_info(_Info, State) ->
    {noreply, State}.

maybe_notify_cbauth(#state{rpc_processes = Processes,
                           cbauth_info = CBAuthInfo} = State) ->
    case build_auth_info(State) of
        CBAuthInfo ->
            State;
        Info ->
            [notify_cbauth(Label, Pid, Info) || {_, {Label, Pid}} <- Processes],
            State#state{cbauth_info = Info}
    end.

personalize_info(Label, Info) ->
    SpecialUser = ns_config_auth:get_user(special) ++ Label,
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
                         [{specialUser, erlang:list_to_binary(SpecialUser)},
                          {nodes, NewNodes},
                          {tlsConfig, TLSConfig}]).

notify_cbauth(Label, Pid, Info) ->
    Method = "AuthCacheSvc.UpdateDB",
    NewInfo = {personalize_info(Label, Info)},

    try json_rpc_connection:perform_call(Label, Method, NewInfo) of
        {error, method_not_found} ->
            error;
        {error, {rpc_error, _}} ->
            error;
        {error, Error} ->
            ?log_error("Error returned from go component ~p: ~p. This shouldn't happen but crash it just in case.",
                       [{Label, Pid}, Error]),
            exit(Pid, Error),
            error;
        {ok, true} ->
            ok
    catch exit:{noproc, _} ->
            ?log_debug("Process ~p is already dead", [{Label, Pid}]),
            error;
          exit:{Reason, _} ->
            ?log_debug("Process ~p has exited during the call with reason ~p",
                       [{Label, Pid}, Reason]),
            error
    end.

build_node_info(N, Config) ->
    build_node_info(N, ns_config:search_node_prop(N, Config, memcached, admin_user), Config).

build_node_info(_N, undefined, _Config) ->
    undefined;
build_node_info(N, User, Config) ->
    ActiveServices = [rest |
                      ns_cluster_membership:node_active_services(Config, N)],
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

build_auth_info(#state{cert_version = CertVersion,
                       client_cert_auth_version = ClientCertAuthVersion}) ->
    Config = ns_config:get(),
    Nodes = lists:foldl(fun (Node, Acc) ->
                                case build_node_info(Node, Config) of
                                    undefined ->
                                        Acc;
                                    Info ->
                                        [Info | Acc]
                                end
                        end, [], ns_node_disco:nodes_wanted(Config)),

    CcaState = ns_ssl_services_setup:client_cert_auth_state(),
    Port = service_ports:get_port(rest_port, Config),
    AuthCheckURL = misc:local_url(Port, "/_cbauth", []),
    PermissionCheckURL = misc:local_url(Port, "/_cbauth/checkPermission", []),
    PermissionsVersion = menelaus_web_rbac:check_permissions_url_version(Config),
    EUserFromCertURL = misc:local_url(Port, "/_cbauth/extractUserFromCert", []),
    ClusterDataEncrypt = misc:should_cluster_data_be_encrypted(),
    DisableNonSSLPorts = misc:disable_non_ssl_ports(),
    TLSServices = menelaus_web_settings:services_with_security_settings(),

    [{nodes, Nodes},
     {authCheckURL, list_to_binary(AuthCheckURL)},
     {permissionCheckURL, list_to_binary(PermissionCheckURL)},
     {permissionsVersion, PermissionsVersion},
     {authVersion, auth_version(Config)},
     {certVersion, CertVersion},
     {extractUserFromCertURL, list_to_binary(EUserFromCertURL)},
     {clientCertAuthState, list_to_binary(CcaState)},
     {clientCertAuthVersion, ClientCertAuthVersion},
     {clusterEncryptionConfig, {[{encryptData, ClusterDataEncrypt},
                                 {disableNonSSLPorts, DisableNonSSLPorts}]}},
     {tlsConfig, [tls_config(S, Config) ||
                  S <- [N || {N, _} <- TLSServices]]}].

tls_config(Service, Config) ->
    Label = case Service of
                n1ql -> "cbq-engine-cbauth";
                _ -> atom_to_list(Service) ++ "-cbauth"
            end,
    Ciphers = ciphers(Service, Config),
    Order = ns_ssl_services_setup:honor_cipher_order(Service, Config),
    CipherInts = lists:map(fun (<<I:16/unsigned-integer>>) -> I end,
                           [ciphers:code(N) || N <- Ciphers]),
    CipherOpenSSLNames = [N2 || N <- Ciphers, N2 <- [ciphers:openssl_name(N)],
                                N2 =/= undefined],
    MinTLSVsn = ns_ssl_services_setup:ssl_minimum_protocol(Service, Config),
    {Label,
     {[{present, true},
       {minTLSVersion, MinTLSVsn},
       {cipherOrder, Order},
       {ciphers, CipherInts},
       {cipherNames, Ciphers},
       {cipherOpenSSLNames, CipherOpenSSLNames}]}}.

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
           menelaus_users:get_auth_version()]),
    base64:encode(crypto:hash(sha, B)).

client_cert_auth_version() ->
    B = term_to_binary(ns_ssl_services_setup:client_cert_auth()),
    base64:encode(crypto:hash(sha, B)).

handle_cbauth_post(Req) ->
    {User, Domain} = menelaus_auth:get_identity(Req),
    menelaus_util:reply_json(Req, {[{user, erlang:list_to_binary(User)},
                                    {domain, Domain}]}).

handle_extract_user_from_cert_post(Req) ->
    CertBin = mochiweb_request:recv_body(Req),
    try
        case menelaus_auth:extract_identity_from_cert(CertBin) of
            {User, Domain} ->
                menelaus_util:reply_json(Req,
                                         {[{user, list_to_binary(User)},
                                           {domain, Domain}]});
            auth_failure ->
                menelaus_util:reply_json(Req, <<"Auth failure">>, 401)
        end
    catch
        _:_ -> menelaus_util:reply_json(Req, <<"Auth failure">>, 401)
    end.

is_cbauth_connection(Label) ->
    lists:suffix("-cbauth", Label).
