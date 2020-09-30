%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2018 Couchbase, Inc.
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
%% @doc Web server for menelaus.

-module(menelaus_auth).
-author('Northscale <info@northscale.com>').

-include("ns_common.hrl").
-include("rbac.hrl").

-export([has_permission/2,
         is_internal/1,
         get_accessible_buckets/2,
         extract_auth/1,
         extract_identity_from_cert/1,
         extract_ui_auth_token/1,
         uilogin/2,
         can_use_cert_for_auth/1,
         complete_uilogout/1,
         maybe_refresh_token/1,
         get_identity/1,
         get_user_id/1,
         get_token/1,
         assert_no_meta_headers/1,
         is_meta_header/1,
         verify_rest_auth/2,
         verify_local_token/1,
         apply_headers/2]).

%% rpc from ns_couchdb node
-export([authenticate/1,
         authenticate_external/2]).

%% External API

-spec get_accessible_buckets(fun ((bucket_name()) -> rbac_permission()), mochiweb_request()) ->
                                    [bucket_name()].
get_accessible_buckets(Fun, Req) ->
    Identity = get_identity(Req),
    Roles = menelaus_roles:get_compiled_roles(Identity),

    [BucketName ||
        {BucketName, _Config} <- ns_bucket:get_buckets(),
        menelaus_roles:is_allowed(Fun(BucketName), Roles)].

-spec get_cookies(mochiweb_request()) -> [{string(), string()}].
get_cookies(Req) ->
    case mochiweb_request:get_header_value("Cookie", Req) of
        undefined -> [];
        RawCookies ->
            RV = mochiweb_cookies:parse_cookie(RawCookies),
            RV
    end.

-spec lookup_cookie(mochiweb_request(), string()) -> string() | undefined.
lookup_cookie(Req, Cookie) ->
    proplists:get_value(Cookie, get_cookies(Req)).

-spec ui_auth_cookie_name(mochiweb_request()) -> string().
ui_auth_cookie_name(Req) ->
    %% NOTE: cookies are _not_ per-port and in general quite
    %% unexpectedly a stupid piece of mess. In order to have working
    %% dev mode clusters where different nodes are at different ports
    %% we use different cookie names for different host:port
    %% combination.
    case mochiweb_request:get_header_value("host", Req) of
        undefined ->
            "ui-auth";
        Host ->
            "ui-auth-" ++ mochiweb_util:quote_plus(Host)
    end.

-spec extract_ui_auth_token(mochiweb_request()) -> auth_token() | undefined.
extract_ui_auth_token(Req) ->
    case mochiweb_request:get_header_value("ns-server-auth-token", Req) of
        undefined ->
            lookup_cookie(Req, ui_auth_cookie_name(Req));
        T ->
            T
    end.

-spec generate_auth_cookie(mochiweb_request(), auth_token()) -> {string(), string()}.
generate_auth_cookie(Req, Token) ->
    Options = [{path, "/"}, {http_only, true}],
    SslOptions = case mochiweb_request:get(socket, Req) of
                     {ssl, _} -> [{secure, true}];
                     _ -> ""
                 end,
    mochiweb_cookies:cookie(ui_auth_cookie_name(Req), Token, Options ++ SslOptions).

-spec kill_auth_cookie(mochiweb_request()) -> {string(), string()}.
kill_auth_cookie(Req) ->
    {Name, Content} = generate_auth_cookie(Req, ""),
    {Name, Content ++ "; expires=Thu, 01 Jan 1970 00:00:00 GMT"}.

-spec complete_uilogout(mochiweb_request()) -> mochiweb_response().
complete_uilogout(Req) ->
    ns_audit:logout(Req),
    CookieHeader = kill_auth_cookie(Req),
    menelaus_util:reply(Req, 200, [CookieHeader]).

-spec maybe_refresh_token(mochiweb_request()) -> [{string(), string()}].
maybe_refresh_token(Req) ->
    case get_token(Req) of
        undefined ->
            [];
        Token ->
            case menelaus_ui_auth:maybe_refresh(Token) of
                nothing ->
                    [];
                {new_token, NewToken} ->
                    [generate_auth_cookie(Req, NewToken)]
            end
    end.

meta_header_names() ->
    ["menelaus-auth-user", "menelaus-auth-domain", "menelaus-auth-token",
     scram_sha:meta_header()].

-spec is_meta_header(atom() | list()) -> boolean().
is_meta_header(Header) when is_atom(Header) ->
    is_meta_header(atom_to_list(Header));
is_meta_header(Header) when is_list(Header) ->
    lists:member(string:lowercase(Header), meta_header_names()).

-spec assert_no_meta_headers(mochiweb_request()) -> ok.
assert_no_meta_headers(Req) ->
    [undefined = mochiweb_request:get_header_value(N, Req) ||
        N <- meta_header_names()],
    ok.

apply_headers(Req, []) ->
    Req;
apply_headers(Req, Headers) ->
    AllHeaders =
        lists:foldl(
          fun ({Header, Val}, H) ->
                  mochiweb_headers:enter(Header, Val, H)
          end, mochiweb_request:get(headers, Req), Headers),
    mochiweb_request:new(mochiweb_request:get(socket, Req), mochiweb_request:get(method, Req), mochiweb_request:get(raw_path, Req),
                         mochiweb_request:get(version, Req), AllHeaders).

meta_headers(undefined) ->
    [];
meta_headers({User, Domain}) ->
    [{"menelaus-auth-user", User},
     {"menelaus-auth-domain", Domain}];
meta_headers(User) when is_list(User) ->
    meta_headers({User, rejected}).

meta_headers(Identity, undefined) ->
    meta_headers(Identity);
meta_headers(Identity, Token) ->
    [{"menelaus-auth-token", Token} | meta_headers(Identity)].


-spec get_identity(mochiweb_request()) -> rbac_identity() | undefined.
get_identity(Req) ->
    case {mochiweb_request:get_header_value("menelaus-auth-user", Req),
          mochiweb_request:get_header_value("menelaus-auth-domain", Req)} of
        {undefined, undefined} ->
            undefined;
        {User, Domain} ->
            {User, list_to_existing_atom(Domain)}
    end.

-spec get_user_id(mochiweb_request()) -> rbac_user_id() | undefined.
get_user_id(Req) ->
    mochiweb_request:get_header_value("menelaus-auth-user", Req).

-spec get_token(mochiweb_request()) -> auth_token() | undefined.
get_token(Req) ->
    mochiweb_request:get_header_value("menelaus-auth-token", Req).

-spec extract_auth(mochiweb_request()) -> {User :: string(), Passwd :: string()}
                                              | {scram_sha, string()}
                                              | {token, string()}
                                              | {client_cert_auth, string()}
                                              | undefined.
extract_auth(Req) ->
    case mochiweb_request:get_header_value("ns-server-ui", Req) of
        "yes" ->
            {token, extract_ui_auth_token(Req)};
        _ ->
            Sock = mochiweb_request:get(socket, Req),
            case ns_ssl_services_setup:get_user_name_from_client_cert(Sock) of
                undefined ->
                    case mochiweb_request:get_header_value("authorization", Req) of
                        "Basic " ++ Value ->
                            parse_basic_auth_header(Value);
                        "SCRAM-" ++ Value ->
                            {scram_sha, Value};
                        undefined ->
                            undefined;
                        _ ->
                            error
                    end;
                failed ->
                    error;
                UName ->
                    {client_cert_auth, UName}
            end
    end.

get_rejected_user(Auth) ->
    case Auth of
        {client_cert_auth, User} ->
            User;
        {User, _} when is_list(User) ->
            User;
        _ ->
            undefined
    end.

parse_basic_auth_header(Value) ->
    case (catch base64:decode_to_string(Value)) of
        UserPasswordStr when is_list(UserPasswordStr) ->
            case string:chr(UserPasswordStr, $:) of
                0 ->
                    case UserPasswordStr of
                        "" ->
                            undefined;
                        _ ->
                            {UserPasswordStr, ""}
                    end;
                I ->
                    {string:substr(UserPasswordStr, 1, I - 1),
                     string:substr(UserPasswordStr, I + 1)}
            end;
        _ ->
            error
    end.

-spec has_permission(rbac_permission(), mochiweb_request()) -> boolean().
has_permission(Permission, Req) ->
    menelaus_roles:is_allowed(Permission, get_identity(Req)).

-spec is_internal(mochiweb_request()) -> boolean().
is_internal(Req) ->
    case get_identity(Req) of
        {"@" ++ _, admin} ->
            true;
        _ ->
            false
    end.

-spec authenticate(error | undefined |
                   {token, auth_token()} |
                   {client_cert_auth, string()} |
                   {rbac_user_id(), rbac_password()}) ->
                          false | {ok, rbac_identity()}.
authenticate(error) ->
    false;
authenticate(undefined) ->
    {ok, {"", anonymous}};
authenticate({token, Token} = Param) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            case menelaus_ui_auth:check(Token) of
                false ->
                    %% this is needed so UI can get /pools on unprovisioned
                    %% system with leftover cookie
                    case ns_config_auth:is_system_provisioned() of
                        false ->
                            {ok, {"", wrong_token}};
                        true ->
                            false
                    end;
                {ok, Id} ->
                    {ok, Id}
            end;
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE, authenticate, [Param])
    end;
authenticate({client_cert_auth, Username} = Param) ->
    %% Just returning the username as the request is already authenticated based
    %% on the client certificate.
    case ns_node_disco:couchdb_node() == node() of
        false ->
            case ns_config_auth:get_user(admin) of
                Username ->
                    {ok, {Username, admin}};
                _ ->
                    Identity = {Username, local},
                    case menelaus_users:user_exists(Identity) of
                        true ->
                            {ok, Identity};
                        false ->
                            false
                    end
            end;
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE, authenticate,
                     [Param])
    end;
authenticate({Username, Password}) ->
    case ns_config_auth:authenticate(Username, Password) of
        false ->
            authenticate_external(Username, Password);
        {ok, Id} ->
            {ok, Id}
    end.

-spec authenticate_external(rbac_user_id(), rbac_password()) ->
                                    false | {ok, rbac_identity()}.
authenticate_external(Username, Password) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            case saslauthd_auth:authenticate(Username, Password) orelse
                 ldap_auth_cache:authenticate(Username, Password) of
                true -> {ok, {Username, external}};
                false -> false
            end;
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE,
                     authenticate_external, [Username, Password])
    end.

verify_login_creds(Auth) ->
    case authenticate(Auth) of
        {ok, Identity} ->
            UIPermission = {[ui], read},
            case check_permission(Identity, UIPermission) of
                allowed ->
                    {ok, Identity};
                _ ->
                    {forbidden, Identity, UIPermission}
            end;
        false ->
            auth_failure
    end.

-spec uilogin(mochiweb_request(), list()) -> mochiweb_response().
uilogin(Req, Params) ->
    CertAuth = proplists:get_value("use_cert_for_auth",
                                   mochiweb_request:parse_qs(Req)) =:= "1",
    {User, AuthStatus} =
        case CertAuth of
            true ->
                S = mochiweb_request:get(socket, Req),
                case ns_ssl_services_setup:get_user_name_from_client_cert(S) of
                    X when X =:= undefined; X =:= failed ->
                        {invalid_client_cert, auth_failure};
                    UName ->
                        {UName, verify_login_creds({client_cert_auth, UName})}
                end;
            false ->
                Usr = proplists:get_value("user", Params),
                Password = proplists:get_value("password", Params),
                {Usr, verify_login_creds({Usr, Password})}
        end,

    case AuthStatus of
        {ok, Identity} ->
            Token = menelaus_ui_auth:generate_token(Identity),
            CookieHeader = generate_auth_cookie(Req, Token),
            ns_audit:login_success(
              apply_headers(Req, meta_headers(Identity, Token))),
            menelaus_util:reply(Req, 200, [CookieHeader]);
        auth_failure ->
            ns_audit:login_failure(
              apply_headers(Req, meta_headers(User))),
            menelaus_util:reply(Req, 400);
        {forbidden, Identity, Permission} ->
            ns_audit:login_failure(
              apply_headers(Req, meta_headers(Identity))),
            menelaus_util:reply_json(
              Req, menelaus_web_rbac:forbidden_response(Permission), 403)
    end.

-spec can_use_cert_for_auth(mochiweb_request()) ->
                                   can_use | cannot_use | must_use.
can_use_cert_for_auth(Req) ->
    case mochiweb_request:get(socket, Req) of
        {ssl, SSLSock} ->
            CCAState = ns_ssl_services_setup:client_cert_auth_state(),
            case {ssl:peercert(SSLSock), CCAState} of
                {_, "mandatory"} ->
                    must_use;
                {{ok, _Cert}, "enable"} ->
                    can_use;
                _ ->
                    cannot_use
            end;
        _ ->
            cannot_use
    end.

-spec verify_rest_auth(mochiweb_request(), rbac_permission() | no_check) ->
                              {auth_failure | forbidden | allowed, [{_, _}]}.
verify_rest_auth(Req, Permission) ->
    Auth = extract_auth(Req),
    case Auth of
        {scram_sha, AuthHeader} ->
            case scram_sha:authenticate(AuthHeader) of
                {ok, Identity, MetaHeaders} ->
                    {check_permission(Identity, Permission),
                     MetaHeaders ++ meta_headers(Identity, undefined)};
                {first_step, Headers} ->
                    mochiweb_request:recv_body(Req),
                    {auth_failure, Headers};
                auth_failure ->
                    {auth_failure, []}
            end;
        _ ->
            case authenticate(Auth) of
                false ->
                    {auth_failure, meta_headers(get_rejected_user(Auth))};
                {ok, Identity} ->
                    Token = case Auth of
                                {token, T} ->
                                    T;
                                _ ->
                                    undefined
                            end,
                    {check_permission(Identity, Permission),
                     meta_headers(Identity, Token)}
            end
    end.

-spec extract_identity_from_cert(binary()) -> auth_failure | tuple().
extract_identity_from_cert(CertDer) ->
    case ns_ssl_services_setup:get_user_name_from_client_cert(CertDer) of
        undefined ->
            auth_failure;
        failed ->
            auth_failure;
        UName ->
            case authenticate({client_cert_auth, UName}) of
                false ->
                    auth_failure;
                {ok, Identity} ->
                    Identity
            end
    end.

-spec check_permission(rbac_identity(), rbac_permission() | no_check) ->
                              auth_failure | forbidden | allowed.
check_permission(_Identity, no_check) ->
    allowed;
check_permission(Identity, Permission) ->
    Roles = menelaus_roles:get_compiled_roles(Identity),
    case Roles of
        [] ->
            %% this can happen in case of expired token, or if LDAP
            %% server authenticates the user that has no roles assigned
            auth_failure;
        _ ->
            case menelaus_roles:is_allowed(Permission, Roles) of
                true ->
                    allowed;
                false ->
                    ?log_debug("Access denied.~nIdentity: ~p~nRoles: ~p~n"
                               "Permission: ~p~n",
                               [ns_config_log:tag_user_data(Identity),
                               Roles, Permission]),
                    case Identity of
                        {"", anonymous} ->
                            %% we do allow some api's for anonymous
                            %% under some circumstances, but we want to return 401 in case
                            %% if autorization for requests with no auth fails
                            auth_failure;
                        _ ->
                            forbidden
                    end
            end
    end.

-spec verify_local_token(mochiweb_request()) ->
                                {auth_failure | allowed, [{_, _}]}.
verify_local_token(Req) ->
    case extract_auth(Req) of
        {"@localtoken" = Username, Password} ->
            case menelaus_local_auth:check_token(Password) of
                true ->
                    {allowed, meta_headers({Username, local_token})};
                false ->
                    {auth_failure, meta_headers(Username)}
            end;
        Auth ->
            {auth_failure, meta_headers(get_rejected_user(Auth))}
    end.
