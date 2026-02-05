%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(menelaus_web_oidc).

-include("ns_common.hrl").
-include("rbac.hrl").
-include("jwt.hrl").
-include_lib("ns_common/include/cut.hrl").
-include_lib("oidcc/include/oidcc_provider_configuration.hrl").
-include_lib("oidcc/include/oidcc_client_context.hrl").

-export([handle_auth/1,
         handle_callback_get/1,
         handle_callback_post/1,
         handle_deauth/1]).

-define(RETRY_ATTEMPTS, 3).

auth_validators() ->
    [validator:required(issuer, _),
     validator:non_empty_string(issuer, _),
     validator:unsupported(_)].

%% The callback may have additional parameters, which are ignored. For
%% example, Keycloak returns "session_state", "iss" - we ignore any
%% additional parameters (so long as we don't use them).
callback_validators() ->
    [validator:required(code, _),
     validator:non_empty_string(code, _),
     validator:required(state, _),
     validator:non_empty_string(state, _)].

-spec get_issuer_config(IssuerName :: string()) ->
          {ok, map()} | {error, string()}.
get_issuer_config(IssuerName) ->
    case chronicle_kv:get(kv, jwt_settings) of
        {ok, {#{issuers := Issuers}, _}} ->
            case maps:get(IssuerName, Issuers, undefined) of
                undefined ->
                    {error, "Unknown issuer"};
                IssuerProps when is_map(IssuerProps) ->
                    case maps:is_key(oidc_settings, IssuerProps) of
                        true  -> {ok, IssuerProps#{name => IssuerName}};
                        false -> {error, "Issuer not OIDC-enabled"}
                    end
            end;
        _ ->
            {error, "JWT is not enabled"}
    end.

%% TODO: Handling multiple redirect bases may need to change - awaiting PM
%% feedback.
-spec select_uri_for_request(list(string()), mochiweb_request()) ->
          string() | {error, string()}.
select_uri_for_request([Base], _Req) ->
    Base;
select_uri_for_request(Allowed, Req) ->
    case get_request_host(Req) of
        undefined ->
            {error, "Request host missing; multiple redirect bases configured"};
        HostPortStr ->
            {ReqHost, ReqPortStr} = misc:split_host_port(HostPortStr, ""),

            ReqScheme = get_request_scheme(Req),

            ReqPort =
                case ReqPortStr of
                    "" -> default_port(ReqScheme);
                    _  -> list_to_integer(ReqPortStr)
                end,

            Parsed = [{Base, uri_string:parse(Base)} || Base <- Allowed],

            %% Attempt 1: Exact Match (Scheme + Host + Port)
            Exact =
                [Base || {Base, Map} <- Parsed,
                         maps:get(scheme, Map) =:= ReqScheme,
                         host_matches(Map, ReqHost),
                         port_matches(Map, ReqPort)],

            case Exact of
                [Base] ->
                    Base;
                _ ->
                    %% Attempt 2: Host-only Fallback
                    %% This handles scenarios like port-forwarding (Public:80 ->
                    %% Internal:8080) or SSL offloading (Public:HTTPS ->
                    %% Internal:HTTP).
                    %% The Scheme or Port might not align perfectly, but the
                    %% Host is unique.
                    HostMatches =
                        [Base || {Base, Map} <- Parsed,
                                 host_matches(Map, ReqHost)],
                    case HostMatches of
                        [Match] ->
                            Match;
                        [] ->
                            {error, "No configured redirect base matches host"};
                        _ ->
                            {error, "Ambiguous configuration; multiple bases "
                             "match host"}
                    end
            end
    end.

-spec get_request_host(mochiweb_request()) -> string() | undefined.
get_request_host(Req) ->
    %% If the app is behind a proxy, the standard 'Host' header often
    %% contains the internal upstream name/IP. Check 'X-Forwarded-Host' to
    %% find the original domain the user requested.
    case mochiweb_request:get_header_value("X-Forwarded-Host", Req) of
        undefined ->
            mochiweb_request:get_header_value("Host", Req);
        Host ->
            Host
    end.

-spec get_request_scheme(mochiweb_request()) -> string().
get_request_scheme(Req) ->
    %% If a load balancer handles the SSL handshake, the internal traffic to
    %% this node is often just HTTP. Check 'X-Forwarded-Proto' to know if the
    %% user is actually on HTTPS.
    case mochiweb_request:get_header_value("X-Forwarded-Proto", Req) of
        undefined ->
            case mochiweb_request:get(socket, Req) of
                {ssl, _} -> "https";
                _ -> "http"
            end;
        Proto ->
            string:lowercase(Proto)
    end.

-spec default_port(string()) -> integer().
default_port("http") -> 80;
default_port("https") -> 443;
default_port(_) -> 0.

-spec host_matches(map(), string()) -> boolean().
host_matches(Map, ReqHost) ->
    Host = maps:get(host, Map),
    string:lowercase(Host) =:= string:lowercase(ReqHost).

-spec port_matches(map(), integer()) -> boolean().
port_matches(Map, ReqPort) ->
    Scheme = maps:get(scheme, Map),
    BasePort = maps:get(port, Map, default_port(Scheme)),
    BasePort =:= ReqPort.

handle_auth(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Props) ->
              IssuerName = proplists:get_value(issuer, Props),
              case get_issuer_config(IssuerName) of
                  {ok, Config} ->
                      Redirect = build_auth_redirect(Config, Req),
                      menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                               [{"Location", Redirect}]);
                  {error, Reason} ->
                      menelaus_util:web_exception(400, Reason)
              end
      end, Req, qs, auth_validators()).

handle_callback_get(Req) ->
    handle_callback(Req, qs).

handle_callback_post(Req) ->
    handle_callback(Req, form).

handle_callback(Req, InputType) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Props) ->
              Code = proplists:get_value(code, Props),
              State = proplists:get_value(state, Props),

              Preauth =
                  case short_ttl_store:take(oidc_preauth_store,
                                            list_to_binary(State)) of
                      {ok, Val} -> Val;
                      not_found ->
                          %% Avoid logging the actual state value as it is a
                          %% short-lived secret used for CSRF protection.
                          ?log_debug("OIDC state not found or expired", []),
                          menelaus_util:web_exception(
                            400, "Invalid or expired state")
                  end,

              IssuerName = maps:get(name, Preauth),
              IssuerConfig =
                  case get_issuer_config(IssuerName) of
                      {ok, Cfg} -> Cfg;
                      {error, ErrMsg} -> menelaus_util:web_exception(400,
                                                                     ErrMsg)
                  end,

              OIDCConfig = maps:get(oidc_settings, IssuerConfig),
              PkceEnabled = maps:get(pkce_enabled, OIDCConfig, true),
              NonceValidation = maps:get(nonce_validation, OIDCConfig, true),

              Nonce = maps:get(nonce, Preauth, undefined),
              case {NonceValidation, Nonce} of
                  {true, undefined} ->
                      menelaus_util:web_exception(400, "Missing nonce");
                  _ ->
                      ok
              end,

              CodeVerifier = maps:get(code_verifier, Preauth, undefined),
              case {PkceEnabled, CodeVerifier} of
                  {true, undefined} ->
                      menelaus_util:web_exception(400, "Missing code_verifier");
                  _ ->
                      ok
              end,

              RedirectBaseFromState =
                  maps:get(redirect_base, Preauth, undefined),
              exchange_code_and_login(Req, IssuerConfig,
                                      Code, CodeVerifier, Nonce,
                                      RedirectBaseFromState)
      end, Req, InputType, callback_validators()).

build_auth_redirect(IssuerConfig, Req) ->
    IssuerName = maps:get(name, IssuerConfig),
    OIDCConfig = maps:get(oidc_settings, IssuerConfig),
    AllowedBases = maps:get(base_redirect_uris, OIDCConfig, []),
    RedirectBase =
        case select_uri_for_request(AllowedBases, Req) of
            {error, Msg} -> menelaus_util:web_exception(400, Msg);
            Y -> Y
        end,
    {Verifier, Nonce} = create_nonce_verifier(OIDCConfig),
    case generate_and_put_preauth(IssuerName,
                                  RedirectBase, Nonce, Verifier,
                                  ?RETRY_ATTEMPTS) of
        {ok, State, Nonce, Verifier} ->
            case create_provider_redirect(IssuerConfig, RedirectBase,
                                          State, Nonce, Verifier) of
                {ok, Url} -> Url;
                {error, E} ->
                    ?log_warning("OIDC redirect for ~p build failed: ~p",
                                 [IssuerName, E]),
                    "/oidc/callback?error=oidc_redirect_failed"
            end;
        {error, exists} ->
            ?log_warning("Failed to generate unique OIDC state after retries "
                         "for ~p", [IssuerName]),
            "/oidc/callback?error=oidc_state_generation_failed"
    end.

generate_and_put_preauth(_IssuerName, _RedirectBaseParam, _Nonce, _Verifier,
                         0) ->
    {error, exists};
generate_and_put_preauth(IssuerName, RedirectBaseParam, Nonce, Verifier,
                         AttemptsLeft) ->
    State = generate_state(),
    case short_ttl_store:put(oidc_preauth_store, State,
                             #{name => IssuerName,
                               state => State,
                               code_verifier => Verifier,
                               nonce => Nonce,
                               redirect_base => RedirectBaseParam},
                             ?OIDC_PREAUTH_STORE_TTL_SECONDS) of
        ok ->
            {ok, State, Nonce, Verifier};
        {error, exists} ->
            generate_and_put_preauth(IssuerName, RedirectBaseParam, Nonce,
                                     Verifier, AttemptsLeft - 1)
    end.

generate_random_url_safe_string(LengthBytes) ->
    base64:encode(crypto:strong_rand_bytes(LengthBytes),
                  #{mode => urlsafe, padding => false}).

generate_state() ->
    generate_random_url_safe_string(32).

create_nonce_verifier(OIDCConfig) ->
    Nonce =
        case maps:get(nonce_validation, OIDCConfig, true) of
            true ->
                generate_random_url_safe_string(32);
            false ->
                undefined
        end,
    Verifier =
        case maps:get(pkce_enabled, OIDCConfig, true) of
            true ->
                generate_random_url_safe_string(32);
            false ->
                undefined
        end,
    {Verifier, Nonce}.

get_redirect_uri(BaseRedirectUri) ->
    list_to_binary(BaseRedirectUri ++ "/oidc/callback").

-spec has_discovery_uri(map()) -> boolean().
has_discovery_uri(IssuerConfig) ->
    OidcSettings = maps:get(oidc_settings, IssuerConfig),
    maps:get(oidc_discovery_uri, OidcSettings, undefined) =/= undefined.

-spec build_manual_provider_configuration(map()) ->
          {ok, #oidcc_provider_configuration{}}.
build_manual_provider_configuration(IssuerConfig) ->
    IssuerName = maps:get(name, IssuerConfig),
    OidcSettings = maps:get(oidc_settings, IssuerConfig),
    AuthEndpoint = maps:get(authorization_endpoint, OidcSettings),
    TokenEndpoint = maps:get(token_endpoint, OidcSettings),
    Scopes = maps:get(scopes, OidcSettings),
    ScopesSupported = [list_to_binary(S) || S <- Scopes],
    SigningAlg = maps:get(signing_algorithm, IssuerConfig),
    SigningAlgBin = atom_to_binary(SigningAlg),
    TokenAuthMethod = maps:get(token_endpoint_auth_method, OidcSettings),
    TokenAuthMethodBin = atom_to_binary(TokenAuthMethod),
    EndSessionEndpoint = maps:get(end_session_endpoint, OidcSettings,
                                  undefined),
    IssuerBin = list_to_binary(IssuerName),
    AuthEndpointBin = list_to_binary(AuthEndpoint),
    TokenEndpointBin = list_to_binary(TokenEndpoint),
    EndSessionBin =
        case EndSessionEndpoint of
            undefined -> undefined;
            Url -> list_to_binary(Url)
        end,
    {ok,
     #oidcc_provider_configuration{
        issuer = IssuerBin,
        authorization_endpoint = AuthEndpointBin,
        token_endpoint = TokenEndpointBin,
        scopes_supported = ScopesSupported,
        response_types_supported = [<<"code">>],
        grant_types_supported =
            [<<"authorization_code">>, <<"refresh_token">>],
        subject_types_supported = [public],
        id_token_signing_alg_values_supported = [SigningAlgBin],
        code_challenge_methods_supported = [<<"S256">>],
        token_endpoint_auth_methods_supported = [TokenAuthMethodBin],
        end_session_endpoint = EndSessionBin
       }}.

-spec build_manual_client_context(map()) ->
          {ok, oidcc_client_context:t()} | {error, term()}.
build_manual_client_context(IssuerConfig) ->
    OidcSettings = maps:get(oidc_settings, IssuerConfig),
    {ok, ProviderCfg0} = build_manual_provider_configuration(IssuerConfig),
    case jwt_cache:get_jwks(IssuerConfig) of
        {error, _} = Error ->
            Error;
        {ok, Jwks} ->
            ClientId = maps:get(client_id, OidcSettings),
            ClientSecret = maps:get(client_secret, OidcSettings),
            ClientIdBin = list_to_binary(ClientId),
            ClientSecretBin = list_to_binary(ClientSecret),
            {ok,
             oidcc_client_context:from_manual(
               ProviderCfg0, Jwks, ClientIdBin,
               ClientSecretBin)}
    end.

-spec with_issuer_context(map(),
                          fun((discovery | oidcc_client_context:t()) -> term()),
                          iolist()) ->
          term().
with_issuer_context(IssuerConfig, OperationFun, LogPrefix) ->
    IssuerName = maps:get(name, IssuerConfig),
    try
        case has_discovery_uri(IssuerConfig) of
            true ->
                OperationFun(discovery);
            false ->
                case build_manual_client_context(IssuerConfig) of
                    {ok, ClientContext} ->
                        OperationFun(ClientContext);
                    {error, Reason} ->
                        {error, Reason}
                end
        end
    catch T:E:St ->
            ?log_warning("~s for ~p: ~p:~p~n~p",
                         [LogPrefix, IssuerName, T, E, St]),
            {error, {T, E}}
    end.

-spec create_provider_redirect(map(), string() | undefined,
                               binary(), binary() | undefined,
                               binary() | undefined) ->
          {ok, string()} | {error, term()}.
create_provider_redirect(IssuerConfig, RedirectBaseParam,
                         State, Nonce, Verifier) ->
    IssuerName = maps:get(name, IssuerConfig),
    OidcSettings = maps:get(oidc_settings, IssuerConfig),
    ClientId = maps:get(client_id, OidcSettings),
    ClientSecret = maps:get(client_secret, OidcSettings),
    Scopes = maps:get(scopes, OidcSettings),
    ScopesBin = [list_to_binary(S) || S <- Scopes],
    BaseOpts = #{redirect_uri => get_redirect_uri(RedirectBaseParam),
                 scopes => ScopesBin,
                 state => State},

    Opts1 = case Verifier of
                undefined ->
                    BaseOpts#{require_pkce => false};
                _ ->
                    BaseOpts#{pkce_verifier => Verifier,
                              require_pkce => true}
            end,

    Opts = case Nonce of
               undefined -> Opts1;
               _ -> Opts1#{nonce => Nonce}
           end,

    Res =
        with_issuer_context(
          IssuerConfig,
          fun
              (discovery) ->
                  oidcc:create_redirect_url(
                    list_to_atom(IssuerName),
                    list_to_binary(ClientId),
                    list_to_binary(ClientSecret),
                    Opts);
              (ClientContext) ->
                  oidcc_authorization:create_redirect_url(ClientContext, Opts)
          end,
          "OIDC redirect build failed"),
    case Res of
        {ok, Uri} -> {ok, binary_to_list(iolist_to_binary(Uri))};
        Error -> Error
    end.

-spec extract_oidc_connect_options(URL :: string(), OidcSettings :: map()) ->
          list().
extract_oidc_connect_options(URL, OidcSettings) ->
    AddressFamily = maps:get(address_family, OidcSettings, undefined),
    VerifyPeer = maps:get(tls_verify_peer, OidcSettings, true),
    {_, Certs} = maps:get(tls_ca, OidcSettings, {<<>>, []}),
    SNI = maps:get(tls_sni, OidcSettings, ""),
    misc:tls_connect_options(URL, AddressFamily, VerifyPeer, Certs, SNI, []).

%% Choose an OIDC URL whose scheme/host/port match the upstream IdP so that
%% tls_connect_options/6 can apply tls_verify_peer/tls_ca/tls_sni correctly.
-spec oidc_tls_url(map()) -> string().
oidc_tls_url(OidcSettings) ->
    case maps:get(oidc_discovery_uri, OidcSettings, undefined) of
        undefined ->
            maps:get(token_endpoint, OidcSettings);
        Disc ->
            Disc
    end.

exchange_code_and_login(Req, IssuerConfig, Code, Verifier, Nonce,
                        RedirectBase) ->
    IssuerName = maps:get(name, IssuerConfig),
    OidcSettings = maps:get(oidc_settings, IssuerConfig),
    ClientId = maps:get(client_id, OidcSettings),
    ClientSecret = maps:get(client_secret, OidcSettings),
    TokenEndpointAuthMethod =
        maps:get(token_endpoint_auth_method, OidcSettings),
    HttpTimeoutMs = maps:get(http_timeout_ms, OidcSettings),
    TlsUrl = oidc_tls_url(OidcSettings),
    SslOpts = extract_oidc_connect_options(TlsUrl, OidcSettings),
    RequestOpts = #{timeout => HttpTimeoutMs, ssl => SslOpts},
    BaseOpts = #{redirect_uri => get_redirect_uri(RedirectBase),
                 preferred_auth_methods => [TokenEndpointAuthMethod],
                 request_opts => RequestOpts},

    PkceEnabled = maps:get(pkce_enabled, OidcSettings, true),
    Opts1 = case PkceEnabled of
                true when Verifier =/= undefined ->
                    BaseOpts#{pkce_verifier => Verifier,
                              require_pkce => true};
                _ ->
                    BaseOpts
            end,

    Opts = case Nonce of
               undefined -> Opts1;
               _ -> Opts1#{nonce => Nonce}
           end,

    CodeBin = iolist_to_binary(Code),

    Res =
        with_issuer_context(
          IssuerConfig,
          fun
              (discovery) ->
                  oidcc:retrieve_token(
                    CodeBin,
                    list_to_atom(IssuerName),
                    list_to_binary(ClientId),
                    list_to_binary(ClientSecret),
                    Opts);
              (ClientContext) ->
                  oidcc_token:retrieve(CodeBin, ClientContext, Opts)
          end,
          "OIDC token exchange failed"),
    case Res of
        {ok, Token} ->
            handle_oidc_login_success(Req, IssuerName, Token);
        {error, Reason} ->
            menelaus_util:web_exception(
              400,
              io_lib:format("OIDC error: ~p", [Reason]))
    end.

handle_oidc_login_success(Req, IssuerName, Token) ->
    %% Use :: as delimiter since : appears in URIs (http://, :8080)
    SessionName = iolist_to_binary([IssuerName, "::",
                                    base64:encode(rand:bytes(6))]),
    %% Validate and map based on the ID token, then start a UI session
    case Token of
        {oidcc_token, {oidcc_token_id, IdTokenBin, _Claims}, _Access, _Refresh,
         _Scope} when is_binary(IdTokenBin) ->
            case jwt_auth:authenticate(binary_to_list(IdTokenBin)) of
                {ok, AuthnResJWT, _Audit} ->
                    SessionId = menelaus_auth:new_session_id(),
                    %% Store the original ID token in the authn result so that
                    %% it can be used later as id_token_hint during RP-initiated
                    %% logout. Wrap the value with ?HIDE/1 to avoid accidental
                    %% disclosure in stack traces or logs.
                    AuthnRes = AuthnResJWT#authn_res{type = ui,
                                                     session_id = SessionId,
                                                     id_token =
                                                         ?HIDE(IdTokenBin)},
                    case menelaus_auth:uilogin_phase2(Req, oidc,
                                                      SessionName, AuthnRes) of
                        {ok, Headers} ->
                            menelaus_util:reply_text(Req, <<"Redirecting...">>,
                                                     302,
                                                     [{"Location", "/"} |
                                                      Headers]);
                        {error, Reason} ->
                            menelaus_util:web_exception(
                              403,
                              io_lib:format("Access denied: ~p", [Reason]))
                    end;
                {error, _Audit} ->
                    menelaus_util:web_exception(403, "Invalid ID token")
            end;
        _ ->
            menelaus_util:web_exception(400, "Missing ID token")
    end.

%% RP-initiated logout for OIDC UI sessions
handle_deauth(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),

    %% Authenticate the request first since serve_ui doesn't do it
    Auth = menelaus_auth:extract_auth(Req),
    Req1 = case menelaus_auth:authenticate(Auth) of
               {ok, AuthnRes, _, _} ->
                   mochiweb_request:set_meta(authn_res, AuthnRes, Req);
               _ ->
                   Req
           end,

    {Session, Headers} = menelaus_auth:complete_uilogout(Req1),
    case Session of
        #uisession{type = oidc,
                   session_name = SessionName,
                   authn_res = #authn_res{id_token = IdTokenHidden}} ->
            IssuerName = parse_issuer_from_session_name(SessionName),
            case get_issuer_config(IssuerName) of
                {ok, IssuerConfig} ->
                    OidcSettings = maps:get(oidc_settings, IssuerConfig),
                    ClientId = maps:get(client_id, OidcSettings),
                    Opts = build_logout_opts(Req, IssuerConfig),
                    IdTokenHint =
                        case IdTokenHidden of
                            undefined -> undefined;
                            _ -> ?UNHIDE(IdTokenHidden)
                        end,
                    Redirect = initiate_logout(IssuerConfig, ClientId,
                                               Opts, IdTokenHint),
                    menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                             [{"Location", Redirect} |
                                              Headers]);
                {error, _} ->
                    ?log_warning("OIDC logout failed: unknown issuer ~p",
                                 [IssuerName]),
                    menelaus_util:web_exception(404, "not found")
            end;
        #uisession{type = simple} ->
            ?log_debug("User is not an OIDC user, ignoring RP-initiated "
                       "logout"),
            menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                     [{"Location", "/"} | Headers]);
        undefined ->
            ?log_debug("User not authenticated"),
            menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                     [{"Location", "/"} | Headers])
    end.

parse_issuer_from_session_name(SessionName) ->
    try
        [IssuerStr, _Rest] = string:split(binary_to_list(SessionName), "::"),
        IssuerStr
    catch _:_ ->
            ""
    end.

build_logout_opts(Req, Cfg) ->
    OidcSettings = maps:get(oidc_settings, Cfg),
    Allowed = maps:get(post_logout_redirect_uris, OidcSettings, []),
    case select_uri_for_request(Allowed, Req) of
        {error, Msg} -> menelaus_util:web_exception(400, Msg);
        Uri -> #{post_logout_redirect_uri => list_to_binary(Uri)}
    end.

initiate_logout(IssuerConfig, ClientId, Opts, IdTokenHint) ->
    IssuerName = maps:get(name, IssuerConfig),
    Result =
        with_issuer_context(
          IssuerConfig,
          fun
              (discovery) ->
                  oidcc:initiate_logout_url(
                    IdTokenHint,
                    list_to_atom(IssuerName),
                    list_to_binary(ClientId),
                    Opts);
              (ClientContext) ->
                  oidcc_logout:initiate_url(IdTokenHint, ClientContext, Opts)
          end,
          "OIDC logout failed"),
    case Result of
        {ok, Uri} ->
            binary_to_list(iolist_to_binary(Uri));
        {error, Error} ->
            ?log_warning("OIDC logout failed for ~p: ~p",
                         [IssuerName, Error]),
            "/"
    end.
