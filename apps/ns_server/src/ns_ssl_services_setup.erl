%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ns_ssl_services_setup).

-include("ns_common.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PKEY_REVALIDATION_INTERVAL, 5000).
-define(WRITE_RETRIES, 7).

-export([start_link/0,
         start_link_capi_service/0,
         start_link_rest_service/0,
         pkey_file_path/1,
         chain_file_path/1,
         ca_file_path/0,
         sync/0,
         ssl_minimum_protocol/1,
         internal_ssl_minimum_protocol/0,
         ssl_minimum_protocol/2,
         client_cert_auth/0,
         client_cert_auth_state/0,
         client_cert_auth_state/1,
         get_user_name_from_client_cert/1,
         set_certificate_chain/6,
         tls_client_opts/2,
         merge_ns_config_tls_options/3,
         tls_client_certs_opts/0,
         tls_peer_verification_client_opts/0,
         tls_no_peer_verification_client_opts/0,
         configured_ciphers_names/2,
         honor_cipher_order/1,
         honor_cipher_order/2,
         set_certs/6,
         get_tls_version_map/0,
         get_supported_tls_versions/2,
         remove_node_certs/0,
         update_certs_epoch/0,
         get_key_ids_in_use/0,
         resave_encrypted_files/0,
         config_upgrade_to_76/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% exported for debugging purposes
-export([low_security_ciphers/0]).

-import(couch_httpd, [make_arity_1_fun/1,
                      make_arity_2_fun/1,
                      make_arity_3_fun/1]).

-behavior(gen_server).

-define(TIMEOUT, ?get_timeout(default, 300000)).

-record(state, {reload_state,
                client_cert_auth,
                sec_settings_state,
                afamily_requirement,
                pkey_validation_timer,
                client_cert_regenerate_timer}).

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
            case config_profile:get_bool({couchdb, disabled}) of
                false ->
                    do_start_link_capi_service(SSLPort);
                true ->
                    ignore
            end
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

max_tls_version() ->
    sorted_map_head(get_tls_version_map(), fun(V1, V2) -> V1 >= V2 end).

min_tls_version() ->
    sorted_map_head(get_tls_version_map(), fun(V1, V2) -> V1 =< V2 end).

sorted_map_head(Map, PredicateFun) ->
    hd(lists:usort(
         fun({_, V1}, {_, V2}) ->
                 PredicateFun(V1, V2)
         end, maps:to_list(Map))).

%% Map of available TLS versions:
%% You should be able to simply update this to include or remove tls versions
%% that we support or no longer support and a number of different validation
%% functions should "just work".
get_tls_version_map() ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            #{'tlsv1.3' => 3, 'tlsv1.2' => 2};
        false  ->
            #{'tlsv1.3'=> 3, 'tlsv1.2' => 2, 'tlsv1.1' => 1, tlsv1 => 0}
    end.

%% Valid TLS versions: ['tlsv1.2', 'tlsv1.3']
%% 'none' can be used as an unbounded end for a range
-spec(get_supported_tls_versions(atom(), atom()) -> [atom()]).
get_supported_tls_versions(none, MaxVsn) when is_atom(MaxVsn) ->
    get_supported_tls_versions(element(1, min_tls_version()), MaxVsn);
get_supported_tls_versions(MinVsn, none) when is_atom(MinVsn) ->
    get_supported_tls_versions(MinVsn, element(1, max_tls_version()));
get_supported_tls_versions(MinVsn, MaxVsn) when is_atom(MinVsn),
                                                is_atom(MaxVsn) ->
    Min = case maps:get(MinVsn, get_tls_version_map(), not_found) of
              not_found -> erlang:error(badarg);
              VMin -> VMin
          end,
    Max = case maps:get(MaxVsn, get_tls_version_map(), not_found) of
              not_found -> erlang:error(badarg);
              VMax -> VMax
          end,
    Allowed = maps:filter(fun (_, V) ->
                                  V =< Max andalso V >= Min
                          end, get_tls_version_map()),
    lists:sort(maps:keys(Allowed)).

supported_versions(MinVer) ->
    get_supported_tls_versions(MinVer, none).

ssl_minimum_protocol(Service) ->
    ssl_minimum_protocol(Service, ns_config:latest()).

ssl_minimum_protocol(Service, Config) ->
    MinVsn = 'tlsv1.2',
    Version = get_sec_setting(Service, ssl_minimum_protocol, Config, MinVsn),
    case Service of
        kv ->
            SupportedVersions = memcached_config_mgr:supported_tls_versions(),
            case lists:member(Version, SupportedVersions) of
                true ->
                    Version;
                false ->
                    ?log_warning("Unsupported TLS version ~p for service ~p. "
                                 "Using default ~p",
                                 [Version, Service, MinVsn]),
                    MinVsn
            end;
        _ ->
            Version
    end.

internal_ssl_minimum_protocol() ->
    internal_ssl_minimum_protocol(ns_config:latest()).

internal_ssl_minimum_protocol(Config) ->
    ns_config:search(Config, internal_ssl_minimum_protocol, 'tlsv1.3').

get_sec_setting(Service, Setting, Config, Default) ->
    case ns_config:search_prop(Config, {security_settings, Service}, Setting) of
        undefined ->
            ns_config:search(Config, Setting, Default);
        Val ->
            Val
    end.

client_cert_auth(Cfg) ->
    DefaultValue = [{state, "disable"}, {prefixes, []}],
    ns_config:search(Cfg, client_cert_auth, DefaultValue).

client_cert_auth() ->
    client_cert_auth(ns_config:latest()).

client_cert_auth_state(Cfg) ->
    proplists:get_value(state, client_cert_auth(Cfg)).

client_cert_auth_state() ->
    client_cert_auth_state(ns_config:latest()).

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
     "RSA-PSK-RC4-SHA",
     "ECDHE-ECDSA-3DES-EDE-CBC-SHA",
     "ECDHE-RSA-3DES-EDE-CBC-SHA",
     "DHE-RSA-3DES-EDE-CBC-SHA",
     "DHE-DSS-3DES-EDE-CBC-SHA",
     "ECDH-ECDSA-3DES-EDE-CBC-SHA",
     "ECDH-RSA-3DES-EDE-CBC-SHA",
     "ECDHE-ECDSA-AES-256-CBC-SHA384",
     "ECDHE-RSA-AES-256-CBC-SHA384",
     "ECDH-ECDSA-AES-256-CBC-SHA384",
     "ECDH-RSA-AES-256-CBC-SHA384",
     "ECDHE-ECDSA-AES-128-CBC-SHA256",
     "ECDHE-RSA-AES-128-CBC-SHA256",
     "ECDH-ECDSA-AES-128-CBC-SHA256",
     "ECDH-RSA-AES-128-CBC-SHA256",
     "DHE-RSA-AES-256-CBC-SHA256",
     "DHE-DSS-AES-256-CBC-SHA256",
     "DHE-RSA-AES-128-CBC-SHA256",
     "DHE-DSS-AES-128-CBC-SHA256",
     "ECDHE-ECDSA-AES-256-CBC-SHA",
     "ECDHE-RSA-AES-256-CBC-SHA",
     "ECDH-ECDSA-AES-256-CBC-SHA",
     "ECDH-RSA-AES-256-CBC-SHA",
     "ECDHE-ECDSA-AES-128-CBC-SHA",
     "ECDHE-RSA-AES-128-CBC-SHA",
     "ECDH-ECDSA-AES-128-CBC-SHA",
     "ECDH-RSA-AES-128-CBC-SHA",
     "DHE-RSA-AES-256-CBC-SHA",
     "DHE-DSS-AES-256-CBC-SHA",
     "DHE-RSA-AES-128-CBC-SHA",
     "DHE-DSS-AES-128-CBC-SHA",
     "RSA-PSK-AES-256-CBC-SHA384",
     "RSA-PSK-AES-128-CBC-SHA256",
     "RSA-PSK-AES-256-CBC-SHA",
     "RSA-PSK-AES-128-CBC-SHA",
     "SRP-RSA-AES-256-CBC-SHA",
     "SRP-DSS-AES-256-CBC-SHA",
     "SRP-RSA-AES-128-CBC-SHA",
     "SRP-DSS-AES-128-CBC-SHA",
     "RSA-AES-256-CBC-SHA256",
     "RSA-AES-128-CBC-SHA256",
     "EXP-ADH-RC4-MD5",
     "EXP-RC4-MD5",
     "EXP-RC4-MD5"].

%% Security vulnerabily with weak ciphers: https://sweet32.info/
%%
%% Excludes two kerberos ciphers that are also considered unsafe, because they
%% are rarely included in openssl installations, and has a more niche use
%% case(s). Including them in this comment for completeness.
%%
%% Excludes:
%%  | OpenSSL           | IANA                           |
%%  +-------------------+--------------------------------+
%%  | KRB5-DES-CBC3-MD5 | TLS_KRB5_WITH_3DES_EDE_CBC_MD5 |
%%  | KRB5-DES-CBC3-SHA | TLS_KRB5_WITH_3DES_EDE_CBC_SHA |
%%
%% Originally added in RFC: https://www.ietf.org/rfc/rfc2712.txt
sweet32_ciphers() ->
    ["DES-CBC3-SHA",
     "DH-DSS-DES-CBC-SHA",
     "DH-RSA-DES-CBC-SHA",
     "EDH-DSS-DES-CBC-SHA",
     "EDH-RSA-DES-CBC-SHA",
     "ADH-DES-CBC-SHA",
     "PSK-3DES-EDE-CBC-SHA",
     "ECDH-ECDSA-DES-CBC3-SHA",
     "ECDHE-ECDSA-DES-CBC3-SHA",
     "ECDH-RSA-DES-CBC3-SHA",
     "ECDHE-RSA-DES-CBC3-SHA",
     "AECDH-DES-CBC3-SHA",
     "SRP-3DES-EDE-CBC-SHA",
     "SRP-RSA-3DES-EDE-CBC-SHA",
     "SRP-DSS-3DES-EDE-CBC-SHA"].

lucky13_ciphers() ->
    ["DHE-RSA-AES128-GCM-SHA256",
     "AES256-GCM-SHA384",
     "AES128-GCM-SHA256",
     "AES256-SHA",
     "AES128-SHA"].

openssl_cipher_to_erlang(Cipher) ->
    try ssl_cipher_format:suite_openssl_str_to_map(Cipher) of
        V ->
            {ok, V}
    catch _:_ ->
            %% erlang is bad at reporting errors here; on R16B03 it just fails
            %% with function_clause error so I need to catch all here
            {error, unsupported}
    end.

%% Right now this list includes ciphers we already know aren't secure as well
%% as the 'SWEET32' and 'LUCKY13' specific ciphers.
low_security_ciphers() ->
    List = low_security_ciphers_openssl() ++ sweet32_ciphers() ++
        lucky13_ciphers(),
    [EC || C <- List, {ok, EC} <- [openssl_cipher_to_erlang(C)]].


ns_server_ciphers() ->
    Config = ns_config:latest(),
    case configured_ciphers(ns_server, Config) of
        [] ->
            %% Backward compatibility
            %% ssl_ciphers is obsolete and should not be used in
            %% new installations
            case application:get_env(ssl_ciphers) of
                {ok, Ciphers} -> Ciphers;
                undefined ->
                    Ciphers = ssl:cipher_suites(all, 'tlsv1.3'),
                    LowList = low_security_ciphers(),

                    %% Filter out any ciphers that aren't compatible with the
                    %% current cryptolib.
                    %% https://www.erlang.org/doc/man/ssl.html#cipher_suites-2
                    %%
                    ssl:filter_cipher_suites(Ciphers -- LowList, [])
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
            [{fail_if_no_peer_cert, false},
             {verify, verify_peer}, {depth, ?ALLOWED_CERT_CHAIN_LENGTH}];
        hybrid ->
            [{fail_if_no_peer_cert, false},
             {verify, verify_peer}, {depth, ?ALLOWED_CERT_CHAIN_LENGTH}];
        mandatory ->
            [{fail_if_no_peer_cert, true},
             {verify, verify_peer}, {depth, ?ALLOWED_CERT_CHAIN_LENGTH}]
    end.

-spec merge_ns_config_tls_options(client|server, atom(), list()) -> list().

merge_ns_config_tls_options(OptionKey, Mod, TLSOptions) ->
    NsConfigKey = {tls_options, OptionKey, Mod},
    NsConfigOptions =
        ns_config:read_key_fast(NsConfigKey, []),

    NsConfigOptPasswordRemoved =
        lists:map(
            fun ({K, {password, V}}) -> {K, V};
                (KV) -> KV
            end, NsConfigOptions),

    TLSOptions2 = misc:update_proplist(TLSOptions, NsConfigOptPasswordRemoved),
    cleanup_options(TLSOptions2).

cleanup_options(Opts) ->
    %% ssl.erl fails if some option dependencies are not satisfied. Since
    %% we build options dynamically and they come from different sources,
    %% it may happen that some options contradict each other. In most cases,
    %% it is safe to just drop one of the options as it doesn't make any sense
    %% when another option is present. For example, there is no need to honor
    %% the reuse_sessions option when tls version is strictly 1.3, so it's ok to
    %% just drop it.
    %% This is not a comprehensive cleaning, for a full list of dependencies see
    %% assert_option_dependency in ssl.erl.
    SupportedVersions = proplists:get_value(supported, ssl:versions()),
    VersionsSet = sets:from_list(
                    proplists:get_value(versions, Opts, SupportedVersions),
                    [{version, 2}]),
    CheckVsn = fun (all) -> true;
                   (NeededVersions) ->
                       not sets:is_disjoint(sets:from_list(NeededVersions,
                                                           [{version, 2}]),
                                            VersionsSet)
               end,
    lists:filter(fun ({K, _}) -> CheckVsn(tls_option_versions(K)) end, Opts).

ssl_server_opts() ->
    PassphraseFun =
        case ns_node_disco:couchdb_node() == node() of
            true ->
                rpc:call(ns_node_disco:ns_server_node(), ns_secrets,
                         get_pkey_pass, [node_cert]);
            false ->
                ns_secrets:get_pkey_pass(node_cert)
        end,
    CipherSuites = ns_server_ciphers(),
    Order = honor_cipher_order(ns_server),
    ClientReneg = ns_config:read_key_fast(client_renegotiation_allowed, false),
    %% Pass CA as cacerts opt (instead of cacertfile) in order to
    %% work around unknown bug in erlang ssl application that leads to
    %% the following behavior:
    %% web server doesn't load new CA (after cert rotation) until
    %% all connections to the server are closed
    Versions = supported_versions(ssl_minimum_protocol(ns_server)),
    RawTLSOptions =
        ssl_auth_options() ++
            [{keyfile, pkey_file_path(node_cert)},
             {certfile, chain_file_path(node_cert)},
             {versions, Versions},
             {cacerts, read_ca_certs(ca_file_path())},
             {dh, dh_params_der()},
             {ciphers, CipherSuites},
             {honor_cipher_order, Order},
             {secure_renegotiate, true},
             {client_renegotiation, ClientReneg},
             {password, PassphraseFun}],
    merge_ns_config_tls_options(server, ?MODULE, RawTLSOptions).

tls_option_versions(anti_replay) -> ['tlsv1.3'];
tls_option_versions(beast_mitigation) -> ['tlsv1'];
tls_option_versions(client_renegotiation) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(early_data) -> ['tlsv1.3'];
tls_option_versions(cookie) -> ['tlsv1.3'];
tls_option_versions(key_update_at) -> ['tlsv1.3'];
tls_option_versions(next_protocols_advertised) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(padding_check) -> ['tlsv1'];
tls_option_versions(psk_identity) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(secure_renegotiate) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(reuse_session) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(reuse_sessions) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(session_tickets) -> ['tlsv1.3'];
tls_option_versions(srp_identity) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(supported_groups) -> ['tlsv1.3'];
tls_option_versions(use_ticket) -> ['tlsv1.3'];
tls_option_versions(user_lookup_fun) -> ['tlsv1','tlsv1.1','tlsv1.2'];
tls_option_versions(_) -> all.

read_ca_certs(File) ->
    case file:read_file(File) of
        {ok, CAPemBin} ->
            {ok, Certs} = ns_server_cert:decode_cert_chain(CAPemBin),
            Certs;
        {error, enoent} ->
            []
    end.

tls_client_opts(Config, PresetOpts) ->
    RawTLSOptions =
        tls_peer_verification_client_opts() ++
        case client_cert_auth_state(Config) of
            "mandatory" -> tls_client_certs_opts();
            "hybrid" -> tls_client_certs_opts();
            _ -> []
        end,
    RawTLSOptions2 =
        case cluster_compat_mode:is_cluster_72() of
            true ->
                IntVsn = internal_ssl_minimum_protocol(),
                IntVsns = lists:reverse(supported_versions(IntVsn)),
                [{versions, IntVsns} | RawTLSOptions];
            false ->
                RawTLSOptions
        end,
    RawTLSOptions3 = misc:update_proplist(RawTLSOptions2, PresetOpts),
    merge_ns_config_tls_options(client, ?MODULE, RawTLSOptions3).

tls_client_certs_opts() ->
    PassphraseFun =
        case ns_node_disco:couchdb_node() == node() of
            true ->
                rpc:call(ns_node_disco:ns_server_node(), ns_secrets,
                         get_pkey_pass, [client_cert]);
            false ->
                ns_secrets:get_pkey_pass(client_cert)
        end,
    [{certfile, chain_file_path(client_cert)},
     {keyfile, pkey_file_path(client_cert)},
     {password, PassphraseFun}].

tls_peer_verification_client_opts() ->
    [{cacertfile, ca_file_path()},
     {verify, verify_peer},
     {depth, ?ALLOWED_CERT_CHAIN_LENGTH},
     {reuse_sessions, false}].

tls_no_peer_verification_client_opts() ->
    [{verify, verify_none}].

start_link_rest_service() ->
    case service_ports:get_port(ssl_rest_port) of
        undefined ->
            ignore;
        SSLPort ->
            Config3 = [{ssl, true},
                       {name, menelaus_web_ssl},
                       %% Make it a fun to avoid printing of sensitive stuff
                       %% like pkey password in progress reports
                       {ssl_opts_fun, fun () -> ssl_server_opts() end},
                       {port, SSLPort}],
            menelaus_web:start_link(Config3)
    end.

marker_path() ->
    filename:join(path_config:component_path(data, "config"), "reload_marker").

ca_file_path() ->
    filename:join(certs_dir(), "ca.pem").
chain_file_path(node_cert) ->
    filename:join(certs_dir(), "chain.pem");
chain_file_path(client_cert) ->
    filename:join(certs_dir(), "client_chain.pem").
pkey_file_path(node_cert) ->
    filename:join(certs_dir(), "pkey.pem");
pkey_file_path(client_cert) ->
    filename:join(certs_dir(), "client_pkey.pem").
tmp_certs_and_key_file(node_cert) ->
    filename:join(certs_dir(), "certs.tmp");
tmp_certs_and_key_file(client_cert) ->
    filename:join(certs_dir(), "client_certs.tmp").
cert_info_file(node_cert) ->
    filename:join(certs_dir(), "certs.info");
cert_info_file(client_cert) ->
    filename:join(certs_dir(), "client_certs.info").
certs_dir() ->
    filename:join(path_config:component_path(data, "config"), "certs").

sync() ->
    chronicle_compat_events:sync(),
    %% First ping guarantees that async_ssl_reload has sent
    %% the notify_services message
    ok = gen_server:call(?MODULE, ping, ?TIMEOUT),
    %% Second ping guarantees that the notify_services message message
    %% has been handled
    ok = gen_server:call(?MODULE, ping, ?TIMEOUT).

set_certificate_chain(Type, CAEntry, Chain, PKey, PassphraseSettings,
                      ForceReload) ->
    ?log_debug("Setting node certificate chain"),
    gen_server:call(?MODULE, {set_certificate_chain, Type, CAEntry, Chain,
                              fun () -> PKey end,
                              fun () -> PassphraseSettings end,
                              ForceReload}, ?TIMEOUT).

set_certs(Host, CA, NodeCert, NodeKey, ClientCert, ClientKey) ->
    ?log_debug("Setting certificates"),
    gen_server:call(?MODULE,
                    {set_certs, Host, CA, NodeCert, ?cut(NodeKey), ClientCert,
                     ?cut(ClientKey)}, ?TIMEOUT).

get_key_ids_in_use() ->
    try
        gen_server:call(?MODULE, get_key_ids_in_use, ?TIMEOUT)
    catch
        exit:{noproc, {gen_server, call, [?MODULE, get_key_ids_in_use, _]}} ->
            {error, retry}
    end.

resave_encrypted_files() ->
    try
        gen_server:call(?MODULE, resave_encrypted_files, ?TIMEOUT)
    catch
        exit:{noproc, {gen_server, call,
                       [?MODULE, resave_encrypted_files, _]}} ->
            {error, retry}
    end.

init([]) ->
    Self = self(),
    chronicle_compat_events:subscribe(handle_config_change(_, Self)),

    CertsDir = certs_dir(),
    case misc:mkdir_p(CertsDir) of
        ok -> ok;
        {error, Reason} ->
            ?log_error("Cannot create certificates directory ~s, reason: ~p",
                       [CertsDir, Reason]),
            exit({certs_dir, CertsDir, Reason})
    end,
    %% In case a failure occurred while certs were being updated there will
    %% be a temp file which must be processed.
    _ = save_certs_phase2(node_cert),
    _ = save_certs_phase2(client_cert),
    maybe_store_ca_certs(),
    maybe_generate_node_certs(),
    maybe_generate_client_certs(),
    reload_pkey_passphrase(node_cert),
    reload_pkey_passphrase(client_cert),
    self() ! {validate_pkey, node_cert},
    self() ! {validate_pkey, client_cert},
    %% Note that it should do nothing if "auto-generated"
    %% certs are in order (not regenerated).
    RetrySvc = read_services_from_marker() -- [ssl_service],
    case RetrySvc of
        [] -> ok;
        _ -> Self ! notify_services
    end,
    {ok, restart_regenerate_client_cert_timer(
           #state{reload_state = RetrySvc,
                  sec_settings_state = security_settings_state(),
                  afamily_requirement = misc:address_family_requirement(),
                  client_cert_auth = client_cert_auth()})}.

handle_config_change(ca_certificates, Parent) ->
    Parent ! ca_certificates_updated;
handle_config_change(root_cert_and_pkey, Parent) ->
    Parent ! cert_and_pkey_changed;
%% we're using this key to detect change of node() name
handle_config_change({node, _Node, is_enterprise}, Parent) ->
    Parent ! cert_and_pkey_changed;
handle_config_change(ssl_minimum_protocol, Parent) ->
    Parent ! security_settings_changed;
handle_config_change(client_cert_auth, Parent) ->
    Parent ! client_cert_auth_changed;
handle_config_change(cipher_suites, Parent) ->
    Parent ! security_settings_changed;
handle_config_change(honor_cipher_order, Parent) ->
    Parent ! security_settings_changed;
handle_config_change(secure_headers, Parent) ->
    Parent ! secure_headers_changed;
handle_config_change({security_settings, ns_server}, Parent) ->
    Parent ! security_settings_changed;
handle_config_change({node, _Node, address_family}, Parent) ->
    Parent ! afamily_requirement_changed;
handle_config_change({node, _Node, address_family_only}, Parent) ->
    Parent ! afamily_requirement_changed;
handle_config_change(cluster_compat_version, Parent) ->
    Parent ! ca_certificates_updated;
handle_config_change(cluster_certs_epoch, Parent) ->
    Parent ! cert_and_pkey_changed;
handle_config_change(_OtherEvent, _Parent) ->
    ok.

handle_call({set_certificate_chain, Type, CAEntry, Chain, PKeyFun,
             PassphraseSettingsFun, ForceReload}, _From, State) ->
    NewPKey = PKeyFun(),
    NewPassphraseSettings = PassphraseSettingsFun(),
    {value, SavedProps} = ns_config:search(ns_config:latest(),
                                           {node, node(), node_cert}),
    Pem = proplists:get_value(pem, SavedProps),
    PassphraseSettings = proplists:get_value(pkey_passphrase_settings,
                                             SavedProps, []),
    CA = proplists:get_value(ca, SavedProps),
    OriginalPassphraseSettings =
        %% If auto_generated is true, this password was generated by ns_server,
        %% which means user didn't provide any password.
        case proplists:get_bool(auto_generated, PassphraseSettings) of
            true -> [];
            false -> PassphraseSettings
        end,

    %% If current cert is OOTB, we should not short circuit the reload
    IsUploaded = (proplists:get_value(type, SavedProps) =:= uploaded),

    Reload =
        case Chain =:= Pem andalso CAEntry =:= CA andalso
             NewPassphraseSettings =:= OriginalPassphraseSettings andalso
             IsUploaded of
            true ->
                case ForceReload of
                    false ->
                        %% Nothing changed so just return.
                        ?log_debug("Certs unchanged so not reloading."),
                        false;
                    true ->
                        %% User wants to force a reload
                        ?log_debug("Unchanged certs being forcibly reloaded."),
                        true
                end;
            false ->
                %% Certs have changed.
                true
        end,

    case Reload of
        true ->
            Props = save_uploaded_certs(Type, CAEntry, Chain, NewPKey,
                                        NewPassphraseSettings),
            {reply, {ok, Props}, read_marker_and_reload_ssl(State)};
        false ->
            {reply, {ok, SavedProps}, State}
    end;

%% This is used in the case when this node is added to a cluster
%% and that cluster pushes generated node certs to us (only in the case when
%% certs are self-generated)
%% Note: During node addition this certificates are generated by another
%% cluster (by another CA)
handle_call({set_certs, Host, CA, NodeCert, NodeKeyFun, ClientCert,
             ClientKeyFun}, _From, State) ->
    NodeKey = NodeKeyFun(),
    ClientKey = ClientKeyFun(),
    ns_server_cert:set_generated_ca(CA),
    maybe_store_ca_certs(),
    if
        (NodeCert =/= undefined) andalso (NodeKey =/= undefined) ->
            save_generated_certs(node_cert, CA, NodeCert, NodeKey, Host);
        true ->
            ?log_warning("Set certs: Node certs are not present. Ignoring")
    end,
    if
        (ClientCert =/= undefined) andalso (ClientKey =/= undefined) ->
            save_generated_certs(client_cert, CA, ClientCert, ClientKey,
                                 ?INTERNAL_CERT_USER);
        true ->
            ?log_warning("Set certs: Client certs are not present. Ignoring")
    end,
    {reply, ok, read_marker_and_reload_ssl(State)};
handle_call(ping, _From, State) ->
    {reply, ok, State};
handle_call(resave_encrypted_files, _From, State) ->
    lists:foreach(
        fun (Type) ->
            resave_cert_info(Type)
        end, [node_cert, client_cert]),
    {reply, ok, State};
handle_call(get_key_ids_in_use, _From, State) ->
    %% normally tmp_certs_and_key_file will never exist here because
    %% if it exists this process will be crashing and restarting, so we will
    %% never get here. Anyway, I think it makes sense to mention this file here
    %% to make fewer assumptions in the code.
    {reply,
     {ok, cb_crypto:get_in_use_deks([tmp_certs_and_key_file(node_cert),
                                     tmp_certs_and_key_file(client_cert),
                                     cert_info_file(node_cert),
                                     cert_info_file(client_cert)])},
     State};
handle_call(_, _From, State) ->
    {reply, unknown_call, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({validate_pkey, Type}, State) ->
    misc:flush({validate_pkey, Type}),
    {_, NewState} = validate_pkey(Type, State),
    {noreply, NewState};

handle_info({revalidate_pkey, Type}, State) ->
    ?log_info("Revalidation of ~p private key is triggered", [Type]),
    reload_pkey_passphrase(Type),
    {ShouldNotify, NewState} = validate_pkey(Type, State),
    case ShouldNotify of
        true ->
            {noreply, async_ssl_reload(pkey_passphrase_updated,
                                       services_to_reload(Type), NewState)};
        false -> {noreply, NewState}
    end;

handle_info(ca_certificates_updated, #state{} = State) ->
    misc:flush(ca_certificates_updated),
    maybe_store_ca_certs(),
    {noreply, read_marker_and_reload_ssl(State)};

%% It means either we generated new cluster CA cert or hostname changed
handle_info(cert_and_pkey_changed, #state{} = State) ->
    ?log_info("cert_and_pkey changed"),
    misc:flush(cert_and_pkey_changed),
    maybe_generate_node_certs(),
    maybe_generate_client_certs(),
    {noreply, read_marker_and_reload_ssl(State)};

handle_info(afamily_requirement_changed,
            #state{afamily_requirement = Current} = State) ->
    misc:flush(afamily_requirement_changed),
    case misc:address_family_requirement() of
        Current ->
            {noreply, State};
        NewValue ->
            {noreply, async_ssl_reload(
                        address_family_requirement, [ssl_service],
                        State#state{afamily_requirement = NewValue})}
    end;
handle_info(security_settings_changed,
            #state{sec_settings_state = Current} = State) ->
    misc:flush(security_settings_changed),
    case security_settings_state() of
        Current ->
            {noreply, State};
        NewValue ->
            {noreply, async_ssl_reload(cipher_suites,
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
            {noreply, async_ssl_reload(client_cert_auth,
                                       [ssl_service, capi_ssl_service],
                                       State#state{client_cert_auth = Other})}
    end;
handle_info(secure_headers_changed, State) ->
    misc:flush(secure_headers_changed),
    {noreply, async_ssl_reload(secure_headers_changed, [capi_ssl_service],
                               State)};

handle_info(notify_services, State) ->
    misc:flush(notify_services),
    {noreply, notify_services(State)};

handle_info(regenerate_client_cert, State) ->
    ?log_info("Received regenerate_client_cert message"),
    maybe_generate_client_certs(),
    {noreply, read_marker_and_reload_ssl(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

prepare_ca_file_content() ->
    CAs = [string:trim(P) || P <- ns_server_cert:trusted_CAs(pem)],
    {length(CAs), iolist_to_binary(lists:join(io_lib:nl(), CAs))}.

maybe_store_ca_certs() ->
    ?log_debug("Considering to store CA certs"),
    %% just to trigger generation of ca cert if it's not generated yet
    _ = ns_server_cert:self_generated_ca(),
    {N, NewContent} = prepare_ca_file_content(),
    Path = ca_file_path(),

    ShouldUpdate =
        case file:read_file(Path) of
            {ok, NewContent} -> false;
            {ok, _} -> true;
            {error, enoent} -> true
        end,

    case ShouldUpdate of
        true ->
            ?log_debug("Updating CA file with ~p certificates", [N]),
            create_marker(all_services()),
            ok = atomic_write_file_with_retry(Path, NewContent, ?WRITE_RETRIES),
            ?log_info("CA file updated: ~b cert(s) written", [N]),
            ok = ssl:clear_pem_cache();
        false ->
            ok
    end,

    ShouldUpdate.

generated_cert_version(ClusterCA, Hostname) ->
    base64:encode(erlang:md5(term_to_binary({ClusterCA, Hostname}))).

maybe_generate_client_certs() ->
    maybe_generate_certs(client_cert, ?INTERNAL_CERT_USER).

should_regenerate_certs(CertType, Name) ->
    %% We can't keep info for certs regeneration in node_cert key because during
    %% rename it may extract wrong info by wrong nodename from ns_config.
    %% Based on this wrong info it might decide to regenerate certs when it
    %% should not.
    CurCertsEpoch = certs_epoch(),
    case read_cert_info(CertType) of
        {ok, {uploaded, CertsEpoch}} when CertsEpoch < CurCertsEpoch ->
            ?log_info("Should regenerate ~p because epoch has changed "
                      "(~p -> ~p)", [CertType, CertsEpoch, CurCertsEpoch]),
            true;
        {ok, {uploaded, _CertsEpoch}} ->
            false;
        {ok, {generated, OldVsn}} ->
            ClusterCA = ns_server_cert:self_generated_ca(),
            case generated_cert_version(ClusterCA, Name) of
                OldVsn when CertType == client_cert ->
                    case time_left_to_client_cert_regen() of
                        infinity ->
                            false;
                        N when N < 10000 ->
                            ?log_info("Should regerate ~p because time has "
                                      "come", [CertType]),
                            true;
                        _ ->
                            false
                    end;
                OldVsn ->
                    false;
                _ ->
                    ?log_info("Should regenerate ~p because CA or name in "
                              "the certificate has changed", [CertType]),
                    true
            end;
        {error, enoent} ->
            ?log_info("Should regenerate ~p because there are no certs on "
                      "this node", [CertType]),
            true
    end.

maybe_generate_node_certs() ->
    maybe_generate_certs(node_cert, misc:extract_node_address(node())).

maybe_generate_certs(Type, Name) ->
    ShouldGenerate = should_regenerate_certs(Type, Name),
    case ShouldGenerate of
        true ->
            case ns_server_cert:generate_certs(Type, Name) of
                no_private_key ->
                    ?log_warning("Node doesn't have private key, "
                                 "skipping ~p generation", [Type]),
                    false;
                {CA, CertChain, NodeKey} ->
                    save_generated_certs(Type, CA, CertChain, NodeKey, Name),
                    true
            end;
        false ->
            false
    end.

async_ssl_reload(Event, Services, #state{reload_state = ReloadState} = State) ->
    ReloadServices = lists:usort(Services ++ ReloadState),
    create_marker(ReloadServices),
    self() ! notify_services,
    ?log_debug("Notify services ~p about ~p change", [Services, Event]),
    restart_regenerate_client_cert_timer(
      State#state{reload_state = ReloadServices}).

read_marker_and_reload_ssl(#state{reload_state = ReloadServices} = State) ->
    NewReloadServices =
        lists:usort(read_services_from_marker() ++ ReloadServices),
    NewState = State#state{reload_state = NewReloadServices},
    restart_regenerate_client_cert_timer(notify_services(NewState)).

save_generated_certs(node_cert = Type, CA, Chain, PKey, Hostname) ->
    save_certs(Type, CA, Chain, PKey, [], [{type, generated}, {hostname, Hostname}]);
save_generated_certs(client_cert = Type, CA, Chain, PKey, Name) ->
    save_certs(Type, CA, Chain, PKey, [], [{type, generated}, {name, Name}]).
save_uploaded_certs(Type, CA, Chain, PKey, PassphraseSettings) ->
    save_certs(Type, CA, Chain, PKey, PassphraseSettings, [{type, uploaded}]).

%% CA, PKey and Chain are pem encoded
%% Chain :: [NodeCert, IntermediateCert, ...]
save_certs(Type, CA, Chain, PKey, PassphraseSettings, Extra)
                                                        when is_binary(CA),
                                                             is_binary(Chain),
                                                             is_binary(PKey) ->
    {EncryptedPKey, NonEmptyPassphraseSettings} =
        case ns_secrets:is_pkey_pass_set(PassphraseSettings) orelse
             (not ns_config:read_key_fast(automatically_encrypt_pkeys, true)) of
            true ->
                {PKey, PassphraseSettings};
            false ->
                PassBin = misc:hexify(crypto:strong_rand_bytes(32)),
                Pass = ?HIDE(binary_to_list(PassBin)),
                PSettings = [{type, plain}, {password, PassBin},
                             {auto_generated, true}],
                {ns_server_cert:encrypt_pkey(PKey, Pass), PSettings}
        end,
    {Subject, Expiration} =
        ns_server_cert:get_chain_info(Chain, CA),
    UTCTime = calendar:universal_time(),
    LoadTime = calendar:datetime_to_gregorian_seconds(UTCTime),
    CertsEpoch = certs_epoch(),
    Props = [{subject, unicode:characters_to_binary(Subject)},
             {not_after, Expiration},
             {verified_with, erlang:md5(CA)},
             {load_timestamp, LoadTime},
             {ca, CA},
             {pem, Chain},
             {pkey_passphrase_settings, NonEmptyPassphraseSettings},
             {certs_epoch, CertsEpoch} | Extra],

    Data = term_to_binary({Type, Props, Chain, EncryptedPKey}),

    {ok, CfgDeksSnapshot} = cb_crypto:fetch_deks_snapshot(configDek),
    ok = cb_crypto:atomic_write_file(tmp_certs_and_key_file(Type), Data,
                                     CfgDeksSnapshot),
    ?log_info("New ~p and pkey are written to tmp file", [Type]),
    ok = save_certs_phase2(Type),
    Props.

save_certs_phase2(Type) ->
    TmpFile = tmp_certs_and_key_file(Type),
    case cb_crypto:read_file(TmpFile, configDek) of
        {Res, Bin} when Res == decrypted; Res == raw ->
            {_, Props, Chain, PKey} = binary_to_term(Bin),
            CertsEpoch = proplists:get_value(certs_epoch, Props),
            ok = atomic_write_file_with_retry(chain_file_path(Type), Chain,
                                              ?WRITE_RETRIES),
            ok = atomic_write_file_with_retry(pkey_file_path(Type), PKey,
                                              ?WRITE_RETRIES),
            CertsInfo = case proplists:get_value(type, Props) of
                            generated when Type == node_cert ->
                                Host = proplists:get_value(hostname, Props),
                                CA = proplists:get_value(ca, Props),
                                {generated, generated_cert_version(CA, Host)};
                            generated when Type == client_cert ->
                                Name = proplists:get_value(name, Props),
                                CA = proplists:get_value(ca, Props),
                                {generated, generated_cert_version(CA, Name)};
                            uploaded ->
                                {uploaded, CertsEpoch}
                        end,
            save_cert_info(Type, CertsInfo),
            ?log_info("~p cert and pkey files updated", [Type]),
            ns_config:set({node, node(), Type}, Props),
            reload_pkey_passphrase(Type),
            self() ! {validate_pkey, Type},
            ok = ssl:clear_pem_cache(),
            %% No need to reload all the services when client cert is updated
            add_services_to_marker(services_to_reload(Type)),
            ok = file:delete(TmpFile);
        {error, enoent} -> file_not_found
    end.

save_cert_info(Type, CertsInfo) ->
    CertsInfoBin = term_to_binary(CertsInfo),
    {ok, CfgDeksSnapshot} = cb_crypto:fetch_deks_snapshot(configDek),
    ok = cb_crypto:atomic_write_file(cert_info_file(Type), CertsInfoBin,
                                     CfgDeksSnapshot).

read_cert_info(Type) ->
    CertInfoFile = cert_info_file(Type),
    case cb_crypto:read_file(CertInfoFile, configDek) of
        {Res, Bin} when Res == decrypted; Res == raw ->
            try
                {ok, binary_to_term(Bin)}
            catch
                _:_ ->
                    %% It is possible that the file is in pre-7.9 format:
                    case file:consult(CertInfoFile) of
                        {ok, [CertsInfo]} -> {ok, CertsInfo};
                        {error, _} = Error -> Error
                    end
            end;
        {error, _} = Error ->
            Error
    end.

resave_cert_info(Type) ->
    case read_cert_info(Type) of
        {ok, Info} -> save_cert_info(Type, Info);
        {error, enoent} -> ok
    end.

reload_pkey_passphrase(Type) ->
    CertProps = ns_config:read_key_fast({node, node(), Type}, []),
    PassSettings = proplists:get_value(pkey_passphrase_settings, CertProps, []),
    ns_secrets:load_passphrase(Type, PassSettings).

validate_pkey(Type, #state{pkey_validation_timer = TimerRef} = State) ->
    catch erlang:cancel_timer(TimerRef),
    KeyFile = pkey_file_path(Type),
    Res =
        case file:read_file(KeyFile) of
            {ok, Key} ->
                PassFun = ns_secrets:get_pkey_pass(Type),
                case ns_server_cert:validate_pkey(Key, PassFun) of
                    {ok, _} -> ok;
                    {error, Error} -> {error, Error}
                end;
            {error, Error} ->
                {error, {cannot_read_keyfile, KeyFile, Error}}
        end,

    case Res of
        ok ->
            ?log_info("Private key (~p) passphrase validation suceeded",
                      [Type]),
            {true, State#state{pkey_validation_timer = undefined}};
        {error, Reason} ->
            ?log_error("Private key (~p) passphrase validation failed: ~p",
                       [Type, Reason]),
            NewTimerRef = erlang:send_after(?PKEY_REVALIDATION_INTERVAL,
                                            self(), {revalidate_pkey, Type}),
            {false, State#state{pkey_validation_timer = NewTimerRef}}
    end.

certs_epoch() ->
    case chronicle_kv:get(kv, cluster_certs_epoch) of
        {ok, {N, _}} -> N;
        {error, not_found} -> 0
    end.

-spec get_user_name_from_client_cert(term()) -> string() | undefined | failed.
get_user_name_from_client_cert(Val) ->
    ClientAuth = client_cert_auth(),
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
                {{error, no_peercert}, "hybrid"} ->
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
    end.

get_user_name_from_client_cert(Cert, ClientAuth) ->
    Triples = proplists:get_value(prefixes, ClientAuth),
    case ns_server_cert:extract_internal_client_cert_user(Cert) of
        {ok, User} -> User;
        {error, not_found} ->
            case get_user_name_from_client_cert_inner(Cert, Triples) of
                {error, _} ->
                    failed;
                Username ->
                    Username
            end
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
            %% Assert that the user exists. Must be a 'local' (or 'admin') user.
            %% See MB-62413 / MB-63001
            case menelaus_users:user_exists({Username, local})
                orelse Username =:= ns_config_auth:get_user(admin) of
                true ->
                    Username;
                false ->
                    %% Rest could contain more user names
                    extract_user_name(Rest, Prefix, Delimiters)
            end
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
    [cb_dist_tls, ssl_service, capi_ssl_service, memcached, server_cert_event,
     client_cert_event].

notify_services(#state{reload_state = []} = State) ->
    catch misc:remove_marker(marker_path()),
    State;
%% Note: This function assumes that the reload marker exists on disk
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

    NotNotifiedServices = [Svc || {_, Svc} <- Bad],

    case NotNotifiedServices of
        [] ->
            misc:remove_marker(marker_path()),
            ok;
        _ ->
            create_marker(NotNotifiedServices),
            ?log_info("Failed to notify some services. Will retry in 5 sec, ~p", [Bad]),
            erlang:send_after(5000, self(), notify_services)
    end,
    State#state{reload_state = NotNotifiedServices}.

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
do_notify_service(memcached) ->
    memcached_config_mgr:trigger_tls_config_push();
do_notify_service(server_cert_event) ->
    gen_event:notify(ssl_service_events, cert_changed);
do_notify_service(client_cert_event) ->
    gen_event:notify(ssl_service_events, client_cert_changed);
do_notify_service(cb_dist_tls) ->
    cb_dist:restart_tls().

security_settings_state() ->
    {ssl_minimum_protocol(ns_server),
     honor_cipher_order(ns_server),
     ns_server_ciphers()}.

-ifdef(TEST).
extract_user_name_test() ->
    meck:new(menelaus_users, [passthrough]),
    meck:expect(menelaus_users, user_exists, fun (_) -> true end),
    ?assertEqual(extract_user_name(["www.abc.com"], "www.", ";,."), "abc"),
    ?assertEqual(extract_user_name(["xyz.abc.com", "qwerty", "www.abc.com"],
                                   "www.", "."), "abc"),
    ?assertEqual(extract_user_name(["xyz.com", "www.abc.com"], "", "."), "xyz"),
    ?assertEqual(extract_user_name(["abc", "xyz"], "", ""), "abc"),
    ?assertEqual(extract_user_name(["xyz.abc.com"],
                                   "www.", "."), {error, not_found}),
    ?assertEqual(extract_user_name(["xyz.abc.com"], "", "-"), "xyz.abc.com"),
    meck:unload(menelaus_users).
-endif.

security_config_update_warning(Version, MinVersion) ->
    ale:info(?USER_LOGGER,
             "Deleting global security setting: tlsMinVersion. ~p is no longer "
             "supported; the minimum supported version is ~p.",
             [Version, MinVersion]).

security_config_update_warning(Version, MinVersion, Service) ->
    ale:info(?USER_LOGGER,
             "Deleting security setting: tlsMinVersion for ~p. ~p is no longer "
             "supported; the minimum supported version is ~p.",
             [ns_cluster_membership:user_friendly_service_name(Service),
              Version, MinVersion]).

tls_version_not_supported(Ver) ->
    case maps:find(Ver, get_tls_version_map()) of
        error -> true;
        _ -> false
    end.

service_security_config_needs_update(Service, Config, MinVersion) ->
    case ns_config:search_prop(Config, {security_settings, Service},
                               ssl_minimum_protocol) of
        undefined -> false;
        Ver ->
            case tls_version_not_supported(Ver) of
                true ->
                    security_config_update_warning(Ver, MinVersion, Service),
                    true;
                _ -> false
            end
    end.

global_security_config_needs_update(Config, Key, MinVersion, Warn) ->
    case ns_config:search(Config, Key) of
        {value, Version} ->
            case tls_version_not_supported(Version) of
                true ->
                    case Warn of
                        true ->
                            security_config_update_warning(Version, MinVersion);
                        _ -> ok
                    end,
                    true;
                _ -> false
            end;
        _ -> false
    end.

update_service_security_config(Service, Config) ->
    {value, OldProps} = ns_config:search(Config, {security_settings, Service}),
    NewProps = proplists:delete(ssl_minimum_protocol, OldProps),
    case NewProps of
        [] -> {delete, {security_settings, Service}};
        _ -> {set, {security_settings, Service}, NewProps}
    end.

config_upgrade_to_76(Config) ->
    {MinVersion, _} = min_tls_version(),
    GlobalKeys = [{ssl_minimum_protocol, true},
                  {internal_ssl_minimum_protocol, false}],
    [{delete, cert_and_pkey} |
     [{delete, Key} ||
         {Key, Warn} <- GlobalKeys,
         global_security_config_needs_update(Config, Key, MinVersion, Warn)] ++
         [update_service_security_config(Service, Config) ||
             Service <- menelaus_web_settings:services_with_security_settings(),
             service_security_config_needs_update(Service, Config, MinVersion)]].

remove_node_certs() ->
    ?log_warning("Removing node certificate and private key"),
    file:delete(cert_info_file(node_cert)),
    file:delete(cert_info_file(client_cert)),
    file:delete(chain_file_path(node_cert)),
    file:delete(chain_file_path(client_cert)),
    file:delete(pkey_file_path(node_cert)),
    file:delete(pkey_file_path(client_cert)).

update_certs_epoch() ->
    update_cert_epoch(node_cert),
    update_cert_epoch(client_cert).

update_cert_epoch(Type) ->
    Epoch = certs_epoch(),
    RV = ns_config:run_txn(
           fun (Config, SetFn) ->
               Key = {node, node(), Type},
               case ns_config:search(Config, Key) of
                   false -> {abort, no_cert};
                   {value, Props} ->
                       NewProps = misc:update_proplist(Props,
                                                       [{certs_epoch, Epoch}]),
                       GeneratedOrUploaded = proplists:get_value(type, Props),
                       {commit, SetFn(Key, NewProps, Config),
                        GeneratedOrUploaded}
               end
           end),
    case RV of
        {commit, _, generated} ->
            ?log_info("Updated generated ~p epoch: ~p", [Type, Epoch]);
        {commit, _, uploaded} ->
            save_cert_info(node_cert, {uploaded, Epoch}),
            ?log_info("Updated uploaded ~p epoch: ~p", [Type, Epoch]);
        {abort, Reason} ->
            ?log_info("Skipped ~p epoch update: ~p", [Type, Reason])
    end,
    ok.

create_marker(Services) ->
    Data = iolist_to_binary(io_lib:format("~p.", [Services])),
    misc:create_marker(marker_path(), Data).

add_services_to_marker(Services) ->
    create_marker(lists:usort(read_services_from_marker() ++ Services)).

%% It's possible that the target file is open while we are trying to write to it.
%% On windows it leads to {error, eacces}. If we assume that consumers of that
%% file don't keep it open for long, simple retry should solve this problem.
atomic_write_file_with_retry(Path, Bytes, Retries) when Retries >= 0,
                                                        is_binary(Bytes) ->
    atomic_write_file_with_retry(Path, Bytes, Retries, _FirstTimeout = 10).

atomic_write_file_with_retry(Path, Bytes, Retries, Timeout) ->
    case misc:atomic_write_file(Path, Bytes) of
        {error, eacces} = Error when Retries > 0 ->
            ?log_warning("Got ~p when writing to ~p, will retry after ~bms",
                         [Error, Path, Timeout]),
            timer:sleep(Timeout),
            atomic_write_file_with_retry(Path, Bytes, Retries - 1, Timeout * 2);
        Other ->
            Other
    end.

-ifdef(TEST).
atomic_write_file_with_retry_test() ->
    meck:new(timer, [passthrough, unstick]),
    meck:new(misc, [passthrough]),
    try
        Self = self(),
        Path = "/path/",
        Body = <<"abc">>,
        Ref = make_ref(),
        meck:expect(timer, sleep, fun (T) -> Self ! {Ref, {sleep, T}}  end),
        meck:expect(misc, atomic_write_file,
                    fun (P, B) ->
                        P = Path,
                        B = Body,
                        case get({retries, Ref}) of
                            N when N =< 0 -> ok;
                            N ->
                               put({retries, Ref}, N - 1),
                               {error, eacces}
                        end
                    end),
        AssertSleeps = fun AS([]) ->
                               receive
                                  {Ref, {sleep, T}} ->
                                      error({unexpected_sleep, T})
                               after
                                   0 -> ok
                               end;
                           AS([Next | Tail]) ->
                               receive
                                   {Ref, {sleep, T}} ->
                                       case Next == T of
                                           true -> AS(Tail);
                                           false ->
                                               error({wrong_sleep, T, Next})
                                       end
                               after
                                   0 ->
                                       error({missing_sleep, Next})
                               end
                       end,
        Test = fun (RetriesNeeded, RetriesHave, ExpectedSleeps) ->
                   put({retries, Ref}, RetriesNeeded),
                   Res = atomic_write_file_with_retry(Path, Body, RetriesHave),
                   case RetriesNeeded =< RetriesHave of
                       true -> ?assertEqual(ok, Res);
                       false -> ?assertEqual({error, eacces}, Res)
                   end,
                   ?assertEqual(ok, AssertSleeps(ExpectedSleeps))
               end,
        Test(1, 0, []),
        Test(0, 0, []),
        Test(4, 1, [10]),
        Test(4, 2, [10, 20]),
        Test(4, 3, [10, 20, 40]),
        Test(3, 3, [10, 20, 40]),
        Test(3, 4, [10, 20, 40]),
        Test(8, 8, [10, 20, 40, 80, 160, 320, 640, 1280]),
        ok
    after
        meck:unload(misc),
        meck:unload(timer)
    end.
-endif.

read_services_from_marker() ->
    case misc:consult_marker(marker_path()) of
        {ok, []} ->
            %% Treat it as all_services() for backward compat
            all_services();
        {ok, [Services]} ->
            Services;
        false ->
            []
    end.

restart_regenerate_client_cert_timer(
                    #state{client_cert_regenerate_timer = undefined} = State) ->
    TimeLeft = time_left_to_client_cert_regen(),
    ?log_debug("Time left before client cert regeneration: ~p", [TimeLeft]),
    case TimeLeft of
        infinity -> State;
        Timeout ->
            Ref = erlang:send_after(max(Timeout, 10000), self(),
                                    regenerate_client_cert),
            State#state{client_cert_regenerate_timer = Ref}
    end;
restart_regenerate_client_cert_timer(
                    #state{client_cert_regenerate_timer = OldRef} = State) ->
    catch erlang:cancel_timer(OldRef),
    misc:flush(regenerate_client_cert),
    restart_regenerate_client_cert_timer(
      State#state{client_cert_regenerate_timer = undefined}).

time_left_to_client_cert_regen() ->
    Props = ns_config:read_key_fast({node, node(), client_cert}, []),
    case proplists:get_value(type, Props) of
        generated ->
            case proplists:get_value(not_after, Props) of
                undefined -> infinity;
                NotAfterGregSec ->
                    Window = ?INTERNAL_CERT_REGEN_WINDOW_SEC,
                    RegenGregSec = NotAfterGregSec - Window,
                    GMTDateTime = calendar:universal_time(),
                    CurGregSec =
                        calendar:datetime_to_gregorian_seconds(GMTDateTime),
                    max(RegenGregSec - CurGregSec, 0) * 1000
            end;
        _ ->
            infinity
    end.

services_to_reload(node_cert) -> all_services() -- [client_cert_event];
services_to_reload(client_cert) -> [cb_dist_tls, client_cert_event].

-ifdef(TEST).

check_tls_min_max_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode,
                is_cluster_76,
                fun() ->
                        true
                end),

    try
        Versions1 = get_supported_tls_versions('tlsv1.2', 'tlsv1.3'),
        ?assertEqual(Versions1, ['tlsv1.2', 'tlsv1.3']),
        Versions2 = get_supported_tls_versions(none, 'tlsv1.2'),
        ?assertEqual(Versions2, ['tlsv1.2']),
        Versions3 = get_supported_tls_versions('tlsv1.2', none),
        ?assertEqual(Versions3, ['tlsv1.2', 'tlsv1.3']),
        Versions4 = get_supported_tls_versions('tlsv1.3', none),
        ?assertEqual(Versions4, ['tlsv1.3']),
        Versions5 = get_supported_tls_versions(none, 'tlsv1.3'),
        ?assertEqual(Versions5, ['tlsv1.2', 'tlsv1.3']),
        Versions6 = get_supported_tls_versions(none, none),
        ?assertEqual(Versions6, ['tlsv1.2', 'tlsv1.3']),
        ?assertError(badarg, get_supported_tls_versions(tlsv1, 'tlsv1.2')),
        ?assertError(badarg, get_supported_tls_versions('tlsv1.1', 'tlsv1.2')),
        ?assertError(badarg, get_supported_tls_versions('tlsv1.2', 'tlsv1.4')),
        Vsn = max_tls_version(),
        ?assertEqual(Vsn, {'tlsv1.3', 3}),
        Vsn2 = min_tls_version(),
        ?assertEqual(Vsn2, {'tlsv1.2', 2}),
        ok
    after
        meck:unload(cluster_compat_mode)
    end,

    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode,
                is_cluster_76,
                fun() ->
                        false
                end),

    try
        Versions7 = get_supported_tls_versions('tlsv1.1', 'tlsv1.2'),
        ?assertEqual(Versions7, ['tlsv1.1', 'tlsv1.2']),
        Versions8 = get_supported_tls_versions('tlsv1.1', 'tlsv1.3'),
        ?assertEqual(Versions8, ['tlsv1.1', 'tlsv1.2', 'tlsv1.3']),
        Versions9 = get_supported_tls_versions(none, 'tlsv1.3'),
        ?assertEqual(Versions9, [tlsv1, 'tlsv1.1', 'tlsv1.2', 'tlsv1.3']),
        Versions10 = get_supported_tls_versions('tlsv1.2', none),
        ?assertEqual(Versions10, ['tlsv1.2', 'tlsv1.3']),
        Versions11 = get_supported_tls_versions(none, none),
        ?assertEqual(Versions11, [tlsv1, 'tlsv1.1', 'tlsv1.2', 'tlsv1.3']),
        Versions12 = get_supported_tls_versions(none, tlsv1),
        ?assertEqual(Versions12, [tlsv1]),
        ?assertError(badarg, get_supported_tls_versions('tlsv1.4',
                                                        'another-atom')),
        ?assertError(badarg, get_supported_tls_versions(tlsv1, 'tlsv1.4')),
        Versions13 = get_supported_tls_versions(tlsv1, none),
        ?assertEqual(Versions13, [tlsv1, 'tlsv1.1', 'tlsv1.2', 'tlsv1.3']),
        Versions14 = get_supported_tls_versions(none, tlsv1),
        ?assertEqual(Versions14, [tlsv1]),
        Versions15 = get_supported_tls_versions(tlsv1, 'tlsv1.2'),
        ?assertEqual(Versions15, [tlsv1, 'tlsv1.1', 'tlsv1.2']),
        Vsn3 = max_tls_version(),
        ?assertEqual(Vsn3, {'tlsv1.3', 3}),
        Vsn4 = min_tls_version(),
        ?assertEqual(Vsn4, {tlsv1, 0}),
        ok
    after
        meck:unload(cluster_compat_mode)
    end.

test_upgrade_config(Config, Expected) ->
    Map = maps:from_list(Config),
    ?assertEqual(lists:sort(config_upgrade_to_76(Map)),
                 lists:sort(Expected)).

config_upgrade_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode,
                is_cluster_76,
                fun() ->
                        true
                end),

    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config,
                search,
                fun(Config, Key) ->
                        case maps:get(Key, Config, false) of
                            false -> false;
                            Version -> {value, Version}
                        end
                end),
    meck:expect(ns_config,
                search_prop,
                fun(Config, Key, SubKey) ->
                        case maps:get(Key, Config, false) of
                            false -> undefined;
                            Value ->
                                case proplists:get_value(SubKey, Value) of
                                    undefined -> undefined;
                                    V -> V
                                end
                        end
                end),
    try
        Config1 =
            [{ssl_minimum_protocol, tlsv1},
             {{security_settings, kv}, [{ssl_minimum_protocol, 'tlsv1.3'}]},
             {{security_settings, fts}, [{ssl_minimum_protocol, 'tlsv1.1'}]},
             {{security_settings, ns_server},
              [{ssl_minimum_protocol, 'tlsv1'}]}],
        Expected1 = [{delete, cert_and_pkey},
                     {delete, ssl_minimum_protocol},
                     {delete, {security_settings, fts}},
                     {delete, {security_settings, ns_server}}],

        test_upgrade_config(Config1, Expected1),

        Config2 =
            [{{security_settings, kv},
              [{ssl_minimum_protocol, 'tlsv1.3'}, {honorCipherOrder, false}]},
             {{security_settings, fts},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, true}]},
             {{security_settings, ns_server},
              [{ssl_minimum_protocol, tlsv1}, {honorCipherOrder, false}]}],
        Expected2 = [{delete, cert_and_pkey},
                     {set, {security_settings, fts},
                      [{honorCipherOrder, true}]},
                     {set, {security_settings, ns_server},
                      [{honorCipherOrder, false}]}],

        test_upgrade_config(Config2, Expected2),

        Config3 =
            [{ssl_minimum_protocol, 'tlsv1.1'},
             {{security_settings, kv},
              [{ssl_minimum_protocol, 'tlsv1.2'}, {honorCipherOrder, false}]},
             {{security_settings, fts},
              [{ssl_minimum_protocol, 'tlsv1.2'}, {honorCipherOrder, true}]},
             {{security_settings, ns_server},
              [{ssl_minimum_protocol, 'tlsv1.2'}, {honorCipherOrder, false}]}],
        Expected3 = [{delete, cert_and_pkey},
                     {delete, ssl_minimum_protocol}],

        test_upgrade_config(Config3, Expected3),

        Config4 =
            [{ssl_minimum_protocol, 'tlsv1.2'},
             {{security_settings, kv},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, false}]},
             {{security_settings, fts},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, true}]},
             {{security_settings, ns_server},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, false}]}],
        Expected4 = [{delete, cert_and_pkey},
                     {set, {security_settings, kv},
                      [{honorCipherOrder, false}]},
                     {set, {security_settings, fts},
                      [{honorCipherOrder, true}]},
                     {set, {security_settings, ns_server},
                      [{honorCipherOrder, false}]}],

        test_upgrade_config(Config4, Expected4),

        Config5 =
            [{ssl_minimum_protocol, 'tlsv1.2'},
             {internal_ssl_minimum_protocol, 'tlsv1.1'},
             {{security_settings, kv},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, false}]},
             {{security_settings, fts},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, true}]},
             {{security_settings, ns_server},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, false}]}],
        Expected5 = [{delete, cert_and_pkey},
                     {delete, internal_ssl_minimum_protocol},
                     {set, {security_settings, kv},
                      [{honorCipherOrder, false}]},
                     {set, {security_settings, fts},
                      [{honorCipherOrder, true}]},
                     {set, {security_settings, ns_server},
                      [{honorCipherOrder, false}]}],

        test_upgrade_config(Config5, Expected5),

        Config6 =
            [{ssl_minimum_protocol, tlsv1},
             {internal_ssl_minimum_protocol, 'tlsv1.1'},
             {{security_settings, kv},
              [{ssl_minimum_protocol, 'tlsv1.2'}, {honorCipherOrder, false}]},
             {{security_settings, fts},
              [{ssl_minimum_protocol, 'tlsv1.1'}, {honorCipherOrder, true}]},
             {{security_settings, ns_server},
              [{ssl_minimum_protocol, 'tlsv1.1'}]}],
        Expected6 = [{delete, cert_and_pkey},
                     {delete, ssl_minimum_protocol},
                     {delete, internal_ssl_minimum_protocol},
                     {set, {security_settings, fts},
                      [{honorCipherOrder, true}]},
                     {delete, {security_settings, ns_server}}],

        test_upgrade_config(Config6, Expected6),
        ok
    after
        meck:unload(cluster_compat_mode)
    end.

resave_encrypted_files_test_() ->
    {setup,
     fun () ->
         %% Create temp dir using path_config
         TmpDir = path_config:tempfile(".", "resave_encrypted_files_test"),

         meck:new(path_config, [passthrough]),
         meck:expect(path_config, component_path,
                     fun(data, Dir) -> filename:join(TmpDir, Dir) end),

         #{tmp_dir => TmpDir}
     end,
     fun (#{tmp_dir := TmpDir}) ->
         meck:unload(path_config),
         file:del_dir_r(TmpDir)
     end,
     fun (#{}) ->
         [
          %% Making sure read_cert_info() is backward compatible with
          %% pre-7.9 format
          ?_test(
            begin
                NodeCertInfo = {generated, <<"node-version-1">>},
                OldFormatPath = cert_info_file(node_cert),
                ok = filelib:ensure_dir(OldFormatPath),
                ok = file:write_file(OldFormatPath,
                                     io_lib:format("~p.", [NodeCertInfo])),
                ?assertEqual({ok, NodeCertInfo}, read_cert_info(node_cert))
            end),

          ?_test(
            begin
                ok = file:delete(cert_info_file(node_cert)),
                ?assertEqual(ok, resave_cert_info(node_cert)),
                ?assertEqual({error, enoent}, read_cert_info(node_cert))
            end)
         ]
     end}.

-endif.
