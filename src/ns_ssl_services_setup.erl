%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2019 Couchbase, Inc.
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

-module(ns_ssl_services_setup).

-include("ns_common.hrl").
-include_lib("public_key/include/public_key.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         start_link_capi_service/0,
         start_link_rest_service/0,
         ssl_cert_key_path/0,
         ssl_cacert_key_path/0,
         memcached_cert_path/0,
         memcached_key_path/0,
         sync/0,
         ssl_minimum_protocol/1,
         ssl_minimum_protocol/2,
         client_cert_auth/0,
         client_cert_auth_state/0,
         client_cert_auth_state/1,
         get_user_name_from_client_cert/1,
         set_node_certificate_chain/4,
         upgrade_client_cert_auth_to_51/1,
         ssl_client_opts/0,
         configured_ciphers_names/2,
         honor_cipher_order/1,
         honor_cipher_order/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% exported for debugging purposes
-export([low_security_ciphers/0]).

-import(couch_httpd, [make_arity_1_fun/1,
                      make_arity_2_fun/1,
                      make_arity_3_fun/1]).

-behavior(gen_server).

-record(state, {hash,
                reload_state,
                client_cert_auth,
                sec_settings_state}).

start_link() ->
    case cluster_compat_mode:tls_supported() of
        true ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, [], []);
        false ->
            ignore
    end.

start_link_capi_service() ->
    ok = ns_couchdb_config_rep:pull(),
    case service_ports:get_port(ssl_capi_port, ns_config:latest(),
                                ns_node_disco:ns_server_node()) of
        undefined ->
            ignore;
        SSLPort ->
            do_start_link_capi_service(SSLPort)
    end.

do_start_link_capi_service(SSLPort) ->
    ok = ssl:clear_pem_cache(),

    Options = [{port, SSLPort},
               {ssl, true},
               {ssl_opts, ssl_server_opts()},
               {ip, misc:inaddr_any()}],

    %% the following is copied almost verbatim from couch_httpd.  The
    %% difference is that we don't touch "ssl" key of couch config and
    %% that we don't register on config changes.
    %%
    %% Not touching ssl key is important because otherwise main capi
    %% http service will restart itself. And I decided it's better to
    %% localize capi code here for now rather than change somewhat
    %% fossilized couchdb code.
    %%
    %% Also couchdb code is reformatted for ns_server formatting
    DefaultSpec = "{couch_httpd_db, handle_request}",
    DefaultFun = make_arity_1_fun(
                   couch_config:get("httpd", "default_handler", DefaultSpec)),

    UrlHandlersList =
        lists:map(
          fun({UrlKey, SpecStr}) ->
                  {list_to_binary(UrlKey), make_arity_1_fun(SpecStr)}
          end, couch_config:get("httpd_global_handlers")),

    DbUrlHandlersList =
        lists:map(
          fun({UrlKey, SpecStr}) ->
                  {list_to_binary(UrlKey), make_arity_2_fun(SpecStr)}
          end, couch_config:get("httpd_db_handlers")),

    DesignUrlHandlersList =
        lists:map(
          fun({UrlKey, SpecStr}) ->
                  {list_to_binary(UrlKey), make_arity_3_fun(SpecStr)}
          end, couch_config:get("httpd_design_handlers")),

    UrlHandlers = dict:from_list(UrlHandlersList),
    DbUrlHandlers = dict:from_list(DbUrlHandlersList),
    DesignUrlHandlers = dict:from_list(DesignUrlHandlersList),
    {ok, ServerOptions} = couch_util:parse_term(
                            couch_config:get("httpd", "server_options", "[]")),
    {ok, SocketOptions} = couch_util:parse_term(
                            couch_config:get("httpd", "socket_options", "[]")),

    DbFrontendModule = list_to_atom(couch_config:get("httpd", "db_frontend", "couch_db_frontend")),

    ExtraHeaders = menelaus_util:compute_sec_headers(),

    Loop =
        fun(Req)->
                case SocketOptions of
                    [] ->
                        ok;
                    _ ->
                        ok = mochiweb_socket:setopts(mochiweb_request:get(socket, Req), SocketOptions)
                end,
                apply(couch_httpd, handle_request,
                      [Req, DbFrontendModule, DefaultFun, UrlHandlers,
                       DbUrlHandlers, DesignUrlHandlers, ExtraHeaders])
        end,

    %% set mochiweb options
    FinalOptions = lists:append([Options, ServerOptions,
                                 [{loop, Loop},
                                  {name, https}]]),

    %% launch mochiweb
    {ok, _Pid} = case mochiweb_http:start(FinalOptions) of
                     {ok, MochiPid} ->
                         {ok, MochiPid};
                     {error, Reason} ->
                         io:format("Failure to start Mochiweb: ~s~n",[Reason]),
                         throw({error, Reason})
                 end.

%% generated using "openssl dhparam -outform DER 2048"
dh_params_der() ->
    <<48,130,1,8,2,130,1,1,0,152,202,99,248,92,201,35,238,246,
      5,77,93,120,10,118,129,36,52,111,193,167,220,49,229,106,
      105,152,133,121,157,73,158,232,153,197,197,21,171,140,
      30,207,52,165,45,8,221,162,21,199,183,66,211,247,51,224,
      102,214,190,130,96,253,218,193,35,43,139,145,89,200,250,
      145,92,50,80,134,135,188,205,254,148,122,136,237,220,
      186,147,187,104,159,36,147,217,117,74,35,163,145,249,
      175,242,18,221,124,54,140,16,246,169,84,252,45,47,99,
      136,30,60,189,203,61,86,225,117,255,4,91,46,110,167,173,
      106,51,65,10,248,94,225,223,73,40,232,140,26,11,67,170,
      118,190,67,31,127,233,39,68,88,132,171,224,62,187,207,
      160,189,209,101,74,8,205,174,146,173,80,105,144,246,25,
      153,86,36,24,178,163,64,202,221,95,184,110,244,32,226,
      217,34,55,188,230,55,16,216,247,173,246,139,76,187,66,
      211,159,17,46,20,18,48,80,27,250,96,189,29,214,234,241,
      34,69,254,147,103,220,133,40,164,84,8,44,241,61,164,151,
      9,135,41,60,75,4,202,133,173,72,6,69,167,89,112,174,40,
      229,171,2,1,2>>.

supported_versions(MinVer) ->
    case application:get_env(ssl_versions) of
        {ok, Versions} ->
            Versions;
        undefined ->
            Patches = proplists:get_value(couchbase_patches,
                                          ssl:versions(), []),
            Versions0 = ['tlsv1.1', 'tlsv1.2'],

            Versions1 = case lists:member(tls_padding_check, Patches) of
                            true ->
                                ['tlsv1' | Versions0];
                            false ->
                                Versions0
                        end,
            case lists:dropwhile(fun (Ver) -> Ver < MinVer end, Versions1) of
                [] ->
                    ?log_warning("Incorrect ssl_minimum_protocol ~p was ignored.", [MinVer]),
                    Versions1;
                Versions ->
                    Versions
            end
    end.

ssl_minimum_protocol(Service) ->
    ssl_minimum_protocol(Service, ns_config:latest()).

ssl_minimum_protocol(Service, Config) ->
    get_sec_setting(Service, ssl_minimum_protocol, Config, 'tlsv1').


get_sec_setting(Service, Setting, Config, Default) ->
    case ns_config:search_prop(Config, {security_settings, Service}, Setting) of
        undefined ->
            ns_config:search(Config, Setting, Default);
        Val ->
            Val
    end.

client_cert_auth(Cfg) ->
    DefaultValue = case cluster_compat_mode:is_cluster_51() of
                       true -> [{state, "disable"}, {prefixes, []}];
                       false -> [{state, "disable"}]
                   end,
    ns_config:search(Cfg, client_cert_auth, DefaultValue).

client_cert_auth() ->
    client_cert_auth(ns_config:latest()).

client_cert_auth_state(Cfg) ->
    proplists:get_value(state, client_cert_auth(Cfg)).

client_cert_auth_state() ->
    client_cert_auth_state(ns_config:latest()).

upgrade_client_cert_auth_to_51(Config) ->
    Cca = ns_config:search(Config, client_cert_auth, []),
    State = proplists:get_value(state, Cca, "disable"),
    NewCca = case proplists:get_value(path, Cca) of
                 undefined ->
                     [{state, State}, {prefixes, []}];
                 Path ->
                     Prefix = proplists:get_value(prefix, Cca, ""),
                     Delimiters = proplists:get_value(delimiter, Cca, ""),
                     [{state, State}, {prefixes,
                                       [[{path, Path}, {prefix, Prefix}, {delimiter, Delimiters}]]}]
             end,
    [{set, client_cert_auth, NewCca}].


%% The list is obtained by running the following openssl command:
%%
%%   openssl ciphers LOW:RC4 | tr ':' '\n'
%%
low_security_ciphers_openssl() ->
    ["EDH-RSA-DES-CBC-SHA",
     "EDH-DSS-DES-CBC-SHA",
     "DH-RSA-DES-CBC-SHA",
     "DH-DSS-DES-CBC-SHA",
     "ADH-DES-CBC-SHA",
     "DES-CBC-SHA",
     "DES-CBC-MD5",
     "ECDHE-RSA-RC4-SHA",
     "ECDHE-ECDSA-RC4-SHA",
     "AECDH-RC4-SHA",
     "ADH-RC4-MD5",
     "ECDH-RSA-RC4-SHA",
     "ECDH-ECDSA-RC4-SHA",
     "RC4-SHA",
     "RC4-MD5",
     "RC4-MD5",
     "PSK-RC4-SHA",
     "EXP-ADH-RC4-MD5",
     "EXP-RC4-MD5",
     "EXP-RC4-MD5"].

openssl_cipher_to_erlang(Cipher) ->
    try ssl_cipher:erl_suite_definition(ssl_cipher:openssl_suite(Cipher)) of
        V ->
            {ok, V}
    catch _:_ ->
            %% erlang is bad at reporting errors here; on R16B03 it just fails
            %% with function_clause error so I need to catch all here
            {error, unsupported}
    end.

low_security_ciphers() ->
    Ciphers = low_security_ciphers_openssl(),
    [EC || C <- Ciphers, {ok, EC} <- [openssl_cipher_to_erlang(C)]].

ns_server_ciphers() ->
    Config = ns_config:latest(),
    case configured_ciphers(ns_server, Config) of
        [] ->
            %% Backward compatibility
            %% ssl_ciphers is obsolete and should not be used in
            %% new installations
            case application:get_env(ssl_ciphers) of
                {ok, Ciphers} -> Ciphers;
                undefined -> ssl:cipher_suites() -- low_security_ciphers()
            end;
        List -> List
    end.

configured_ciphers_names(Service, Config) ->
    ciphers:only_known(get_sec_setting(Service, cipher_suites, Config, [])).

configured_ciphers(Service, Config) ->
    [ciphers:code(N) || N <- configured_ciphers_names(Service, Config)].


honor_cipher_order(Service) -> honor_cipher_order(Service, ns_config:latest()).
honor_cipher_order(Service, Config) ->
    get_sec_setting(Service, honor_cipher_order, Config, true).

ssl_auth_options() ->
    Val = list_to_atom(proplists:get_value(state, client_cert_auth())),
    case Val of
        disable ->
            [];
        enable ->
            [{verify, verify_peer}, {depth, ?ALLOWED_CERT_CHAIN_LENGTH}];
        mandatory ->
            [{fail_if_no_peer_cert, true},
             {verify, verify_peer}, {depth, ?ALLOWED_CERT_CHAIN_LENGTH}]
    end.

ssl_server_opts() ->
    Path = ssl_cert_key_path(),
    CipherSuites = ns_server_ciphers(),
    Order = honor_cipher_order(ns_server),
    ClientReneg = ns_config:read_key_fast(client_renegotiation_allowed, false),
    %% Pass CA as cacerts opt (instead of cacertfile) in order to
    %% work around unknown bug in erlang ssl application that leads to
    %% the following behavior:
    %% web server doesn't load new CA (after cert rotation) until
    %% all connections to the server are closed
    ssl_auth_options() ++
        [{keyfile, Path},
         {certfile, Path},
         {versions, supported_versions(ssl_minimum_protocol(ns_server))},
         {cacerts, read_ca_certs(ssl_cacert_key_path())},
         {dh, dh_params_der()},
         {ciphers, CipherSuites},
         {honor_cipher_order, Order},
         {secure_renegotiate, true},
         {client_renegotiation, ClientReneg}].

read_ca_certs(undefined) -> [];
read_ca_certs(File) ->
    {ok, CAPemBin} = file:read_file(File),
    {ok, Certs} = ns_server_cert:decode_cert_chain(CAPemBin),
    Certs.

ssl_client_opts() ->
    [{cacertfile, ssl_cacert_key_path()},
     {verify, verify_peer},
     {depth, ?ALLOWED_CERT_CHAIN_LENGTH}].

start_link_rest_service() ->
    case service_ports:get_port(ssl_rest_port) of
        undefined ->
            ignore;
        SSLPort ->
            Config3 = [{ssl, true},
                       {name, menelaus_web_ssl},
                       {ssl_opts, ssl_server_opts()},
                       {port, SSLPort}],
            menelaus_web:start_link(Config3)
    end.

ssl_cert_key_path() ->
    filename:join(path_config:component_path(data, "config"), "ssl-cert-key.pem").


raw_ssl_cacert_key_path() ->
    ssl_cert_key_path() ++ "-ca".

ssl_cacert_key_path() ->
    Path = raw_ssl_cacert_key_path(),
    case filelib:is_file(Path) of
        true ->
            Path;
        false ->
            undefined
    end.

local_cert_path_prefix() ->
    filename:join(path_config:component_path(data, "config"), "local-ssl-").

memcached_cert_path() ->
    filename:join(path_config:component_path(data, "config"), "memcached-cert.pem").

memcached_key_path() ->
    filename:join(path_config:component_path(data, "config"), "memcached-key.pem").

marker_path() ->
    filename:join(path_config:component_path(data, "config"), "reload_marker").

user_set_cert_path() ->
    filename:join(path_config:component_path(data, "config"), "user-set-cert.pem").

user_set_key_path() ->
    filename:join(path_config:component_path(data, "config"), "user-set-key.pem").

user_set_ca_chain_path() ->
    filename:join(path_config:component_path(data, "config"), "user-set-ca.pem").

check_local_cert_and_pkey(ClusterCertPEM, Node) ->
    true = is_binary(ClusterCertPEM),
    try
        do_check_local_cert_and_pkey(ClusterCertPEM, Node)
    catch T:E ->
            {T, E}
    end.

do_check_local_cert_and_pkey(ClusterCertPEM, Node) ->
    {ok, MetaBin} = file:read_file(local_cert_path_prefix() ++ "meta"),
    Meta = erlang:binary_to_term(MetaBin),
    {ok, LocalCert} = file:read_file(local_cert_path_prefix() ++ "cert.pem"),
    {ok, LocalPKey} = file:read_file(local_cert_path_prefix() ++ "pkey.pem"),
    Digest = erlang:md5(term_to_binary({Node, ClusterCertPEM, LocalCert, LocalPKey})),
    {proplists:get_value(digest, Meta) =:= Digest, LocalCert, LocalPKey}.

sync() ->
    ns_config:sync_announcements(),
    %% First ping guarantees that trigger_ssl_reload has sent
    %% the notify_services message
    ok = gen_server:call(?MODULE, ping, infinity),
    %% Second ping guarantees that the notify_services message message
    %% has been handled
    ok = gen_server:call(?MODULE, ping, infinity).

set_node_certificate_chain(Props, CAChain, Cert, PKey) ->
    gen_server:call(?MODULE, {set_node_certificate_chain, Props, CAChain, Cert, PKey}, infinity).

build_hash(Data) ->
    crypto:hash(sha256, term_to_binary(Data)).

get_node_cert_data() ->
    Node = node(),
    case ns_server_cert:cluster_ca() of
        {GeneratedCert, GeneratedKey} ->
            {generated, GeneratedCert, GeneratedKey, Node};
        {_UploadedCAProps, GeneratedCert, GeneratedKey} ->
            case ns_config:search(ns_config:latest(), {node, node(), cert}) of
                {value, _Props} ->
                    {ok, Cert} = file:read_file(user_set_cert_path()),
                    {ok, PKey} = file:read_file(user_set_key_path()),
                    {ok, CAChain} = file:read_file(user_set_ca_chain_path()),
                    {user_set, Cert, PKey, CAChain};
                false ->
                    {generated, GeneratedCert, GeneratedKey, Node}
            end
    end.

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events, fun config_change_detector_loop/2, Self),

    Data = get_node_cert_data(),
    apply_node_cert_data(Data),
    RetrySvc = case misc:marker_exists(marker_path()) of
                   true ->
                       Self ! notify_services,
                       all_services() -- [ssl_service];
                   false ->
                       []
               end,
    {ok, #state{hash = build_hash(Data),
                reload_state = RetrySvc,
                sec_settings_state = security_settings_state(),
                client_cert_auth = client_cert_auth()}}.

config_change_detector_loop({cert_and_pkey, _}, Parent) ->
    Parent ! cert_and_pkey_changed,
    Parent;
%% we're using this key to detect change of node() name
config_change_detector_loop({{node, _Node, capi_port}, _}, Parent) ->
    Parent ! cert_and_pkey_changed,
    Parent;
config_change_detector_loop({ssl_minimum_protocol, _}, Parent) ->
    Parent ! security_settings_changed,
    Parent;
config_change_detector_loop({client_cert_auth, _}, Parent) ->
    Parent ! client_cert_auth_changed,
    Parent;
config_change_detector_loop({cipher_suites, _}, Parent) ->
    Parent ! security_settings_changed,
    Parent;
config_change_detector_loop({honor_cipher_order, _}, Parent) ->
    Parent ! security_settings_changed,
    Parent;
config_change_detector_loop({secure_headers, _}, Parent) ->
    Parent ! secure_headers_changed,
    Parent;
config_change_detector_loop({{security_settings, ns_server}, _}, Parent) ->
    Parent ! security_settings_changed,
    Parent;
config_change_detector_loop(_OtherEvent, Parent) ->
    Parent.

handle_call({set_node_certificate_chain, Props, CAChain, Cert, PKey}, _From, State) ->
    CAChainFile = user_set_ca_chain_path(),
    CanUpdateChain =
        case file:read_file(CAChainFile) of
            {ok, CAChain} -> true;
            _ ->  misc:is_cluster_encryption_fully_disabled()
        end,

    case CanUpdateChain of
        true ->
            ns_config:delete({node, node(), cert}),

            ok = misc:atomic_write_file(CAChainFile, CAChain),
            ok = misc:atomic_write_file(user_set_cert_path(), Cert),
            ok = misc:atomic_write_file(user_set_key_path(), PKey),

            ns_config:set({node, node(), cert}, Props),
            self() ! cert_and_pkey_changed,
            {reply, ok, State};
        false ->
            {reply, {error, n2n_enabled}, State}
    end;
handle_call(ping, _From, State) ->
    {reply, ok, State};
handle_call(_, _From, State) ->
    {reply, unknown_call, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(cert_and_pkey_changed, #state{hash = OldHash} = State) ->
    misc:flush(cert_and_pkey_changed),

    Data = get_node_cert_data(),
    NewHash = build_hash(Data),
    case OldHash =:= NewHash of
        true ->
            {noreply, State};
        false ->
            ?log_info("Got certificate and pkey change"),
            misc:create_marker(marker_path()),
            apply_node_cert_data(Data),
            ?log_info("Wrote new pem file"),
            NewState = State#state{hash = NewHash,
                                   reload_state = all_services()},
            {noreply, notify_services(NewState)}
    end;
handle_info(security_settings_changed,
            #state{sec_settings_state = Current} = State) ->
    misc:flush(security_settings_changed),
    case security_settings_state() of
        Current ->
            {noreply, State};
        NewValue ->
            {noreply, trigger_ssl_reload(cipher_suites,
                                         [ssl_service, capi_ssl_service],
                                          State#state{
                                              sec_settings_state = NewValue})}
    end;
handle_info(client_cert_auth_changed,
            #state{client_cert_auth = Auth} = State) ->
    misc:flush(client_cert_auth_changed),
    case client_cert_auth() of
        Auth ->
            {noreply, State};
        Other ->
            {noreply, trigger_ssl_reload(client_cert_auth,
                                         [ssl_service, capi_ssl_service],
                                         State#state{client_cert_auth = Other})}
    end;
handle_info(secure_headers_changed, State) ->
    misc:flush(secure_headers_changed),
    {noreply, trigger_ssl_reload(secure_headers_changed, [capi_ssl_service],
                                 State)};

handle_info(notify_services, State) ->
    misc:flush(notify_services),
    {noreply, notify_services(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

trigger_ssl_reload(Event, Services, #state{reload_state = ReloadState} = State) ->
    misc:create_marker(marker_path()),
    self() ! notify_services,
    ?log_debug("Notify services ~p about ~p change", [Services, Event]),
    State#state{reload_state = lists:usort(Services ++ ReloadState)}.

do_generate_local_cert(CertPEM, PKeyPEM, Node) ->
    Host = misc:extract_node_address(node()),
    SANArg =
        case misc:is_raw_ip(Host) of
            true -> "--san-ip-addrs=" ++ Host;
            false -> "--san-dns-names=" ++ Host
        end,

    %% CN can't be longer than 64 characters. Since it will be used for
    %% displaying purposing only, it doesn't make sense to make it even
    %% that long
    HostShortened = case string:slice(Host, 0, 20) of
                        Host -> Host;
                        Shortened -> Shortened ++ "..."
                    end,
    Args = ["--generate-leaf",
            "--common-name=Couchbase Server Node (" ++ HostShortened ++ ")",
            SANArg],
    Env = [{"CACERT", binary_to_list(CertPEM)},
           {"CAPKEY", binary_to_list(PKeyPEM)}],
    {LocalCert, LocalPKey} = ns_server_cert:do_generate_cert_and_pkey(Args, Env),
    ok = misc:atomic_write_file(local_cert_path_prefix() ++ "pkey.pem", LocalPKey),
    ok = misc:atomic_write_file(local_cert_path_prefix() ++ "cert.pem", LocalCert),
    Meta = [{digest, erlang:md5(term_to_binary({Node, CertPEM, LocalCert, LocalPKey}))}],
    ok = misc:atomic_write_file(local_cert_path_prefix() ++ "meta", term_to_binary(Meta)),
    ?log_info("Saved local cert for node ~p", [Node]),
    {true, LocalCert, LocalPKey} = check_local_cert_and_pkey(CertPEM, Node),
    {LocalCert, LocalPKey}.

maybe_generate_local_cert(CertPEM, PKeyPEM, Node) ->
    case check_local_cert_and_pkey(CertPEM, Node) of
        {true, LocalCert, LocalPKey} ->
            {LocalCert, LocalPKey};
        {false, _, _} ->
            ?log_info("Detected existing node certificate that did not match cluster certificate. Will re-generate"),
            do_generate_local_cert(CertPEM, PKeyPEM, Node);
        Error ->
            ?log_info("Failed to read node certificate. Perhaps it wasn't created yet. Error: ~p", [Error]),
            do_generate_local_cert(CertPEM, PKeyPEM, Node)
    end.

apply_node_cert_data(Data) ->
    Path = ssl_cert_key_path(),
    apply_node_cert_data(Data, Path),
    ok = ssl:clear_pem_cache(),
    cb_dist:restart_tls().

apply_node_cert_data({generated, CertPEM, undefined, _Node}, _Path) ->
    ?log_info("No private key provided. This happens when node joins the "
              "cluster. Just store the new CA"),
    ok = misc:atomic_write_file(raw_ssl_cacert_key_path(), CertPEM);
apply_node_cert_data({generated, CertPEM, PKeyPEM, Node}, Path) ->
    {LocalCert, LocalPKey} = maybe_generate_local_cert(CertPEM, PKeyPEM, Node),
    ok = misc:atomic_write_file(Path, [LocalCert, LocalPKey]),
    ok = misc:atomic_write_file(raw_ssl_cacert_key_path(), CertPEM),
    ok = misc:atomic_write_file(memcached_cert_path(), [LocalCert, CertPEM]),
    ok = misc:atomic_write_file(memcached_key_path(), LocalPKey);
apply_node_cert_data({user_set, Cert, PKey, CAChain}, Path) ->
    ok = misc:atomic_write_file(Path, [Cert, PKey]),
    ok = misc:atomic_write_file(raw_ssl_cacert_key_path(), CAChain),
    ok = misc:atomic_write_file(memcached_cert_path(), [Cert, CAChain]),
    ok = misc:atomic_write_file(memcached_key_path(), PKey).

-spec get_user_name_from_client_cert(term()) -> string() | undefined | failed.
get_user_name_from_client_cert(Val) ->
    case cluster_compat_mode:is_cluster_55() of
        true ->
            ClientAuth = ns_ssl_services_setup:client_cert_auth(),
            {state, State} = lists:keyfind(state, 1, ClientAuth),
            case Val of
                {ssl, SSLSock} ->
                    case {ssl:peercert(SSLSock), State} of
                        {_, "disable"} ->
                            undefined;
                        {{ok, Cert}, _} ->
                            get_user_name_from_client_cert(Cert, ClientAuth);
                        {{error, no_peercert}, "enable"} ->
                            undefined;
                        {{error, R}, _} ->
                            ?log_debug("Error getting client certificate: ~p",
                                       [R]),
                            failed
                    end;
                Cert when is_binary(Cert) ->
                    get_user_name_from_client_cert(Cert, ClientAuth);
                _ ->
                    undefined
            end;
        false ->
            undefined
    end.

get_user_name_from_client_cert(Cert, ClientAuth) ->
    Triples = proplists:get_value(prefixes, ClientAuth),
    case get_user_name_from_client_cert_inner(Cert, Triples) of
        {error, _} ->
            failed;
        Username ->
            Username
    end.

get_user_name_from_client_cert_inner(_Cert, []) ->
    {error, not_found};
get_user_name_from_client_cert_inner(Cert, [Triple | Rest]) ->
    {path, Path} = lists:keyfind(path, 1, Triple),
    [Category, Field] = string:tokens(Path, "."),
    case get_fields_from_cert(Cert, Category, Field) of
        {error, _} ->
            get_user_name_from_client_cert_inner(Cert, Rest);
        Values ->
            Prefix = proplists:get_value(prefix, Triple),
            Delimiters = proplists:get_value(delimiter, Triple),
            case extract_user_name(Values, Prefix, Delimiters) of
                {error, _} ->
                    get_user_name_from_client_cert_inner(Cert, Rest);
                UName ->
                    UName
            end
    end.

get_fields_from_cert(Cert, "subject", "cn") ->
    ns_server_cert:get_subject_fields_by_type(Cert, ?'id-at-commonName');
get_fields_from_cert(Cert, "san", Field) ->
    Type = san_field_to_type(Field),
    ns_server_cert:get_sub_alt_names_by_type(Cert, Type).

extract_user_name([], _Prefix, _Delimiters) ->
    {error, not_found};
extract_user_name([Val | Rest], Prefix, Delimiters) ->
    case do_extract_user_name(Val, Prefix, Delimiters) of
        {error, not_found} ->
            extract_user_name(Rest, Prefix, Delimiters);
        Username ->
            Username
    end.

do_extract_user_name(Name, Prefix, Delimiters) ->
    Name1 = string:prefix(Name, Prefix),

    case Name1 of
        nomatch ->
            {error, not_found};
        _ ->
            lists:takewhile(fun(C) ->
                                    not lists:member(C, Delimiters)
                            end, Name1)
    end.

san_field_to_type("dnsname") -> dNSName;
san_field_to_type("uri") -> uniformResourceIdentifier;
san_field_to_type("email") -> rfc822Name.

all_services() ->
    [ssl_service, capi_ssl_service, xdcr_proxy, memcached, event].

notify_services(#state{reload_state = []} = State) -> State;
notify_services(#state{reload_state = Reloads} = State) ->
    ?log_debug("Going to notify following services: ~p", [Reloads]),

    RVs = diag_handler:diagnosing_timeouts(
            fun () ->
                    misc:parallel_map(fun notify_service/1, Reloads, 60000)
            end),
    ResultPairs = lists:zip(RVs, Reloads),
    {Good, Bad} = lists:foldl(fun ({ok, Svc}, {AccGood, AccBad}) ->
                                      {[Svc | AccGood], AccBad};
                                  (ErrorPair, {AccGood, AccBad}) ->
                                      {AccGood, [ErrorPair | AccBad]}
                              end, {[], []}, ResultPairs),
    case Good of
        [] ->
            ok;
        _ ->
            ?log_info("Succesfully notified services ~p", [Good])
    end,
    case Bad of
        [] ->
            misc:remove_marker(marker_path()),
            ok;
        _ ->
            ?log_info("Failed to notify some services. Will retry in 5 sec, ~p", [Bad]),
            timer:send_after(5000, notify_services)
    end,
    State#state{reload_state = [Svc || {_, Svc} <- Bad]}.

notify_service(Service) ->
    RV = (catch do_notify_service(Service)),
    case RV of
        ok ->
            ?log_info("Successfully notified service ~p", [Service]);
        Other ->
            ?log_warning("Failed to notify service ~p: ~p", [Service, Other])
    end,

    RV.

do_notify_service(ssl_service) ->
    %% NOTE: We're going to talk to our supervisor so if we do it
    %% synchronously there's chance of deadlock if supervisor is about
    %% to shutdown us.
    %%
    %% We're not trapping exits and that makes this interaction safe.
    case ns_ssl_services_sup:restart_ssl_service() of
        ok ->
            ok;
        {error, not_running} ->
            ?log_info("Did not restart ssl rest service because it wasn't running"),
            ok
    end;
do_notify_service(capi_ssl_service) ->
    case ns_couchdb_api:restart_capi_ssl_service() of
        ok ->
            ok;
        {error, not_running} ->
            ?log_info("Did not restart capi ssl service because is wasn't running"),
            ok
    end;
do_notify_service(xdcr_proxy) ->
    ns_ports_setup:restart_xdcr_proxy();
do_notify_service(memcached) ->
    memcached_refresh:refresh(ssl_certs);
do_notify_service(event) ->
    gen_event:notify(ssl_service_events, cert_changed).

security_settings_state() ->
    {ssl_minimum_protocol(ns_server),
     honor_cipher_order(ns_server),
     ns_server_ciphers()}.

-ifdef(TEST).
extract_user_name_test() ->
    ?assertEqual(extract_user_name(["www.abc.com"], "www.", ";,."), "abc"),
    ?assertEqual(extract_user_name(["xyz.abc.com", "qwerty", "www.abc.com"],
                                   "www.", "."), "abc"),
    ?assertEqual(extract_user_name(["xyz.com", "www.abc.com"], "", "."), "xyz"),
    ?assertEqual(extract_user_name(["abc", "xyz"], "", ""), "abc"),
    ?assertEqual(extract_user_name(["xyz.abc.com"],
                                   "www.", "."), {error, not_found}),
    ?assertEqual(extract_user_name(["xyz.abc.com"], "", "-"), "xyz.abc.com").
-endif.
