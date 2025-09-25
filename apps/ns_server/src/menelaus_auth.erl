%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Web server for menelaus.

-module(menelaus_auth).
-author('Northscale <info@northscale.com>').

-include("ns_common.hrl").
-include("rbac.hrl").
-include("jwt.hrl").
-include_lib("ns_common/include/cut.hrl").

-define(count_auth(Type, Res),
        ns_server_stats:notify_counter({<<"authentications">>,
                                        [{<<"type">>, <<Type>>},
                                         {<<"res">>, <<Res>>}]})).

-export([has_permission/2,
         is_internal/1,
         is_anonymous/1,
         filter_accessible_buckets/3,
         extract_auth/1,
         extract_identity_from_cert/1,
         extract_ui_auth_token/1,
         uilogin/2,
         uilogin_phase2/4,
         can_use_cert_for_auth/1,
         complete_uilogout/1,
         maybe_refresh_token/1,
         get_authn_res/1,
         get_identity/1,
         get_authenticated_identity/1,
         get_user_id/1,
         get_session_id/1,
         get_on_behalf_extras/1,
         is_UI_req/1,
         is_password_expired/1,
         verify_rest_auth/2,
         authenticate/1,
         new_session_id/0,
         get_resp_headers/1,
         acting_on_behalf/1,
         init_auth/1,
         on_behalf_extras/1,
         get_authn_res_from_on_behalf_of/3,
         is_external_auth_allowed/1,
         get_authn_res_audit_props/1,
         maybe_set_auth_audit_props/2,
         check_expiration/1]).

%% rpc from ns_couchdb node
-export([do_authenticate/1,
         authenticate_external/2]).

%% External API

new_session_id() ->
    base64:encode(crypto:strong_rand_bytes(16)).

filter_accessible_buckets(Fun, Buckets, Req) ->
    AuthnRes = get_authn_res(Req),
    Roles = menelaus_roles:get_compiled_roles(AuthnRes),
    lists:filter(?cut(menelaus_roles:is_allowed(Fun(_), Roles)), Buckets).

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

-spec extract_ui_auth_token(mochiweb_request()) ->
                                    {token, auth_token() | undefined} | not_ui.
extract_ui_auth_token(Req) ->
    %% /saml/deauth is called technically outside of UI so it doesn't have
    %% the ns-server-ui header, while it still needs to be authenticated
    %% to perform the logout
    case mochiweb_request:get_header_value("ns-server-ui", Req) == "yes" orelse
         mochiweb_request:get(raw_path, Req) == "/saml/deauth" of
        true ->
            Token =
                case mochiweb_request:get_header_value("ns-server-auth-token",
                                                       Req) of
                    undefined ->
                        lookup_cookie(Req, ui_auth_cookie_name(Req));
                    T ->
                        T
                end,
            {token, Token};
        false ->
            not_ui
    end.

maybe_get_username_for_ui_cookie(#authn_res{identity = {"", _}}) ->
    undefined;
maybe_get_username_for_ui_cookie(#authn_res{identity = {Name, _}}) ->
    case ns_config:read_key_fast(include_username_in_ui_cookie, false) of
        true -> Name;
        false -> undefined
    end.

maybe_add_username_to_ui_cookie(Token, Username) ->
    case Username of
        undefined ->
            Token;
        _ ->
            EncodedName = base64:encode(Username),
            io_lib:format("~s-~s", [Token, EncodedName])
    end.

-spec generate_auth_cookie(mochiweb_request(), auth_token(),
                           rbac_user_id() | undefined) ->
          {string(), string()}.
generate_auth_cookie(Req, Token0, Username) ->
    Options = [{path, "/"}, {http_only, true}],
    SslOptions = case mochiweb_request:get(socket, Req) of
                     {ssl, _} -> [{secure, true}];
                     _ -> ""
                 end,
    Token1 = maybe_add_username_to_ui_cookie(Token0, Username),
    mochiweb_cookies:cookie(ui_auth_cookie_name(Req), Token1,
                            Options ++ SslOptions).

-spec kill_auth_cookie(mochiweb_request()) -> {string(), string()}.
kill_auth_cookie(Req) ->
    {Name, Content} = generate_auth_cookie(Req, "", undefined),
    {Name, Content ++ "; expires=Thu, 01 Jan 1970 00:00:00 GMT"}.

-spec complete_uilogout(mochiweb_request()) ->
                {Session :: #uisession{} | undefined, [{string(), string()}]}.
complete_uilogout(Req) ->
    case get_authn_res(Req) of
        #authn_res{type = ui, session_id = SessionId} ->
            UISession = menelaus_ui_auth:logout(SessionId),
            ns_audit:logout(Req),
            {UISession, [kill_auth_cookie(Req)]};
        _ ->
            {undefined, []}
    end.

-spec maybe_refresh_token(mochiweb_request()) -> [{string(), string()}].
maybe_refresh_token(Req) ->
    case extract_ui_auth_token(Req) of
        not_ui -> [];
        {token, undefined} -> [];
        {token, Token} ->
            case menelaus_ui_auth:maybe_refresh(Token) of
                nothing ->
                    [];
                {new_token, NewToken} ->
                    Username = maybe_get_username_for_ui_cookie(
                        get_authn_res(Req)),
                    [generate_auth_cookie(Req, NewToken, Username)]
            end
    end.

maybe_store_rejected_user(undefined, Req) ->
    Req;
maybe_store_rejected_user(User, Req) ->
    store_authn_res(#authn_res{identity = {User, unknown}}, Req).

store_authn_res(#authn_res{} = AuthnRes, Req) ->
    mochiweb_request:set_meta(authn_res, AuthnRes, Req).

append_resp_headers(Headers, Req) ->
    CurHeaders = mochiweb_request:get_meta(resp_headers, [], Req),
    mochiweb_request:set_meta(resp_headers, CurHeaders ++ Headers, Req).

get_resp_headers(Req) ->
    mochiweb_request:get_meta(resp_headers, [], Req).

-spec get_authn_res(mochiweb_request()) -> #authn_res{} | undefined.
get_authn_res(Req) ->
    mochiweb_request:get_meta(authn_res, undefined, Req).

-spec get_identity(mochiweb_request()) -> rbac_identity() | undefined.
get_identity(Req) ->
    case get_authn_res(Req) of
        undefined -> undefined;
        #authn_res{identity = Id} -> Id
    end.

-spec get_authenticated_identity(mochiweb_request()) ->
          rbac_identity() | undefined.
get_authenticated_identity(Req) ->
    case get_authn_res(Req) of
        undefined -> undefined;
        #authn_res{authenticated_identity = Id} -> Id
    end.

-spec get_session_id(mochiweb_request()) -> binary() | undefined.
get_session_id(Req) ->
    case get_authn_res(Req) of
        undefined -> undefined;
        #authn_res{session_id = SessionId} -> SessionId
    end.

-spec get_user_id(mochiweb_request()) -> rbac_user_id() | undefined.
get_user_id(Req) ->
    case mochiweb_request:get_meta(authn_res, undefined, Req) of
        #authn_res{identity = {Name, _}} -> Name;
        undefined -> undefined
    end.

-spec get_on_behalf_extras(mochiweb_request()) -> string().
get_on_behalf_extras(Req) ->
    case mochiweb_request:get_meta(authn_res, undefined, Req) of
        undefined -> "";
        AuthnRes -> on_behalf_extras(AuthnRes)
    end.

is_UI_req(Req) ->
    case get_authn_res(Req) of
        undefined -> false;
        #authn_res{type = ui} -> true;
        #authn_res{} -> false
    end.

-spec is_password_expired(mochiweb_request()) -> boolean().
is_password_expired(Req) ->
    case get_authn_res(Req) of
        undefined -> false;
        #authn_res{password_expired = true} -> true;
        #authn_res{} -> false
    end.

-spec extract_auth(mochiweb_request()) -> {User :: string(), Passwd :: string()}
              | {scram_sha, string()}
              | {token, string() | undefined}
              | {client_cert_auth, string()}
              | {jwt, string()}
              | undefined.
extract_auth(Req) ->
    case extract_ui_auth_token(Req) of
        {token, Token} ->
            {token, Token};
        not_ui ->
            Sock = mochiweb_request:get(socket, Req),
            case ns_ssl_services_setup:get_user_name_from_client_cert(Sock) of
                undefined ->
                    case mochiweb_request:get_header_value("authorization", Req) of
                        "Basic " ++ Value ->
                            parse_basic_auth_header(Value);
                        "SCRAM-" ++ Value ->
                            {scram_sha, Value};
                        "Bearer " ++ Value ->
                            {jwt, Value};
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
    menelaus_roles:is_allowed(Permission, get_authn_res(Req)).

-spec is_internal(mochiweb_request()) -> boolean().
is_internal(Req) ->
    is_internal_identity(get_identity(Req)).

is_internal_identity({"@" ++ _, admin}) -> true;
is_internal_identity(_) -> false.

init_auth(Identity) ->
    #authn_res{identity = Identity,
               authenticated_identity = Identity}.

init_auth_password_expired(Identity) ->
    #authn_res{identity = Identity,
               authenticated_identity = Identity,
               password_expired = true}.

-spec authenticate(error | undefined |
                   {token, auth_token()} |
                   {scram_sha, string()} |
                   {client_cert_auth, string()} |
                   {jwt, string()} |
                   {rbac_user_id(), rbac_password()}) ->
          {ok, #authn_res{}, RespHeaders :: [RespHeader],
           AuthAuditProps :: auth_audit_props()} |
          {error, auth_failure | temporary_failure,
           AuthAuditProps :: auth_audit_props()} |
          {unfinished, RespHeaders :: [RespHeader]}
              when RespHeader :: {string(), string()}.
authenticate(Auth) ->
    case do_authenticate(Auth) of
        {ok, #authn_res{authenticated_identity = Identity,
                        session_id = SessionId} = AuthnRes, _, _} = Result ->
            case menelaus_users:is_user_locked(Identity) of
                false ->
                    activity_tracker:handle_activity(AuthnRes),
                    Result;
                true ->
                    case SessionId of
                        undefined ->
                            ok;
                        _ ->
                            %% Expire token
                            menelaus_ui_auth:logout(SessionId)
                    end,
                    ?count_auth("error", "locked"),
                    {error, auth_failure, []}
            end;
        Other ->
            Other
    end.

-spec do_authenticate(error | undefined |
                      {token, auth_token()} |
                      {scram_sha, string()} |
                      {client_cert_auth, string()} |
                      {jwt, string()} |
                      {rbac_user_id(), rbac_password()}) ->
          {ok, #authn_res{}, RespHeaders :: [RespHeader],
           AuthAuditProps :: auth_audit_props()} |
          {error, auth_failure | temporary_failure,
           AuthAuditProps :: auth_audit_props()} |
          {unfinished, RespHeaders :: [RespHeader]}
              when RespHeader :: {string(), string()}.
do_authenticate(error) ->
    ?count_auth("error", "failure"),
    {error, auth_failure, []};
do_authenticate(undefined) ->
    ?count_auth("anon", "succ"),
    {ok, init_auth(?ANONYMOUS_IDENTITY), [], []};
do_authenticate({token, Token} = Param) ->
    ?call_on_ns_server_node(
       case menelaus_ui_auth:check(Token) of
           false ->
               ?count_auth("token", "failure"),
               %% this is needed so UI can get /pools on unprovisioned
               %% system with leftover cookie
               case ns_config_auth:is_system_provisioned() of
                   false ->
                       {ok, init_auth({"", wrong_token}), [], []};
                   true ->
                       {error, auth_failure, []}
               end;
           {ok, AuthnRes} ->
               ?count_auth("token", "succ"),
               {ok, AuthnRes, [], []}
       end, [Param]);
do_authenticate({client_cert_auth, "@" ++ _ = Username}) ->
    ?count_auth("client_cert_int", "succ"),
    {ok, init_auth({Username, admin}), [], []};
do_authenticate({client_cert_auth, Username} = Param) ->
    %% Just returning the username as the request is already authenticated based
    %% on the client certificate.
    ?call_on_ns_server_node(
       case ns_config_auth:get_user(admin) of
           Username ->
               ?count_auth("client_cert", "succ"),
               {ok, init_auth({Username, admin}), [], []};
           _ ->
               Identity = {Username, local},
               case menelaus_users:user_exists(Identity) of
                   true ->
                       ?count_auth("client_cert", "succ"),
                       {ok, init_auth(Identity), [], []};
                   false ->
                       ?count_auth("client_cert", "failure"),
                       {error, auth_failure, []}
               end
       end, [Param]);
do_authenticate({scram_sha, AuthHeader}) ->
    case scram_sha:authenticate(AuthHeader) of
        {ok, Identity, RespHeaders} ->
            ?count_auth("scram_sha", "succ"),
            {ok, init_auth(Identity), RespHeaders, []};
        {first_step, RespHeaders} ->
            ?count_auth("scram_sha", "succ"),
            {unfinished, RespHeaders};
        auth_failure ->
            ?count_auth("scram_sha", "failure"),
            {error, auth_failure, []}
    end;
do_authenticate({jwt, Token} = Param) ->
    ?call_on_ns_server_node(
       case jwt_auth:authenticate(Token) of
           {error, AuthAuditProps} ->
               ?count_auth("jwt", "failure"),
               {error, auth_failure, AuthAuditProps};
           {ok, AuthnRes, AuthAuditProps} ->
               ?count_auth("jwt", "succ"),
               {ok, AuthnRes, [], AuthAuditProps}
       end,
       [Param]);
do_authenticate({Username, Password}) ->
    case ns_config_auth:authenticate(Username, Password) of
        {ok, Id} ->
            ?count_auth("local", "succ"),
            {ok, init_auth(Id), [], []};
        {expired, Id} ->
            %% Note, we also count the auth failure if we determine that the
            %% request can't be performed with an expired password
            %% (i.e. the permission is not 'no_check').
            %% While this does mean double counting the authentication, it's not
            %% worth the complexity to make sure we only count the successful
            %% auth when the request can be performed
            ?count_auth("local", "succ"),
            {ok, init_auth_password_expired(Id), [], []};
        {error, auth_failure} ->
            case menelaus_users:user_exists({Username, local}) of
                true ->
                    AllowFallback = allow_fallback_auth(),
                    case AllowFallback of
                        false ->
                            ?count_auth("local", "failure"),
                            {error, auth_failure, []};
                        true ->
                            Res = authenticate_external(Username, Password),
                            maybe_log_fallback_auth_success(Username, Res),
                            Res
                    end;
                false ->
                    authenticate_external(Username, Password)
            end;
        {error, Reason} ->
            ?count_auth("local", "failure"),
            {error, Reason, []}
    end.

-spec authenticate_external(rbac_user_id(), rbac_password()) ->
          {error, auth_failure, auth_audit_props()} |
          {ok, #authn_res{}, [RespHeader], auth_audit_props()} when
      RespHeader :: {string(), string()}.
authenticate_external(Username, Password) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            case is_external_auth_allowed(Username) andalso
                 (saslauthd_auth:authenticate(Username, Password) orelse
                  ldap_auth_cache:authenticate(Username, Password)) of
                true ->
                    ?count_auth("external", "succ"),
                    {ok, init_auth({Username, external}), [], []};
                false ->
                    ?count_auth("external", "failure"),
                    {error, auth_failure, []}
            end;
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE,
                     authenticate_external, [Username, Password])
    end.

is_external_auth_allowed("@" ++ _) -> false;
is_external_auth_allowed(Username) ->
    ns_config_auth:get_user(admin) /= Username.

-spec uilogin(mochiweb_request(), list()) -> mochiweb_response().
uilogin(Req, Params) ->
    CertAuth = proplists:get_value("use_cert_for_auth",
                                   mochiweb_request:parse_qs(Req)) =:= "1",
    {User, AuthnStatus} =
        case CertAuth of
            true ->
                S = mochiweb_request:get(socket, Req),
                case ns_ssl_services_setup:get_user_name_from_client_cert(S) of
                    X when X =:= undefined; X =:= failed ->
                        {invalid_client_cert, {error, auth_failure, []}};
                    UName ->
                        {UName, authenticate({client_cert_auth, UName})}
                end;
            false ->
                Usr = proplists:get_value("user", Params),
                case can_use_cert_for_auth(Req) of
                    must_use ->
                        %% client cert is mandatory, but user is trying
                        %% to use a password to login
                        {Usr, {error, auth_failure, []}};
                    _ ->
                        Password = proplists:get_value("password", Params),
                        {Usr, authenticate({Usr, Password})}
                end
        end,

    case AuthnStatus of
        {ok, #authn_res{type = tmp, identity = Identity} = AuthnRes,
         RespHeaders, _AuthAuditProps} ->
            AuthnRes2 = AuthnRes#authn_res{type = ui,
                                           session_id = new_session_id(),
                                           identity = Identity},
            RandomName = base64:encode(rand:bytes(6)),
            SessionName = <<"UI - ", RandomName/binary>>,
            Req2 = append_resp_headers(RespHeaders, Req),
            case uilogin_phase2(Req2, simple, SessionName, AuthnRes2) of
                {ok, Headers} ->
                    menelaus_util:reply(Req, 200, Headers);
                {error, internal} ->
                    ns_server_stats:notify_counter(
                      <<"rest_request_access_forbidden">>),
                    menelaus_util:reply_json(
                      Req,
                      {[{message, <<"Forbidden. Internal user">>}]},
                      403);
                {error, {access_denied, UIPermission}} ->
                    ns_server_stats:notify_counter(
                      <<"rest_request_access_forbidden">>),
                    menelaus_util:reply_json(
                      Req,
                      menelaus_web_rbac:forbidden_response([UIPermission]),
                      403);
                {error, password_expired} ->
                    menelaus_util:reply_password_expired(Req)

            end;
        {error, auth_failure, _} ->
            ns_audit:login_failure(
              maybe_store_rejected_user(User, Req)),
            menelaus_util:reply(Req, 400);
        {error, temporary_failure, _} ->
            ns_audit:login_failure(
              maybe_store_rejected_user(User, Req)),
            Msg = <<"Temporary error occurred. Please try again later.">>,
            menelaus_util:reply_json(Req, Msg, 503)
    end.

uilogin_phase2(Req, UISessionType, UISessionName,
               #authn_res{identity = Identity} = AuthnRes) ->
    UIPermission = {[ui], read},
    case is_internal_identity(Identity) of
        false ->
            case check_permission(AuthnRes, UIPermission) of
                allowed ->
                    Token = menelaus_ui_auth:start_ui_session(UISessionType,
                                                              UISessionName,
                                                              AuthnRes),
                    Username = maybe_get_username_for_ui_cookie(AuthnRes),
                    CookieHeader = generate_auth_cookie(Req, Token, Username),
                    ns_audit:login_success(store_authn_res(AuthnRes, Req)),
                    {ok, [CookieHeader]};
                AuthzRes when AuthzRes == forbidden; AuthzRes == auth_failure ->
                    ns_audit:login_failure(store_authn_res(AuthnRes, Req)),
                    {error, {access_denied, UIPermission}};
                password_expired ->
                    ns_audit:login_failure(store_authn_res(AuthnRes, Req)),
                    {error, password_expired}
            end;
        true ->
            {error, internal}
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

-spec verify_rest_auth(mochiweb_request(),
                       rbac_permission() | no_check | local) ->
          {auth_failure | forbidden | allowed
          | temporary_failure | password_expired, mochiweb_request()}.
verify_rest_auth(Req, Permission) ->
    Auth = extract_auth(Req),
    case authenticate(Auth) of
        {ok, #authn_res{} = AuthnRes, RespHeaders, AuthAuditProps} ->
            Req1 = append_resp_headers(RespHeaders, Req),
            Req2 = maybe_set_auth_audit_props(Req1, AuthAuditProps),

            case apply_on_behalf_of_authn_res(AuthnRes, Req2) of
                error ->
                    Req3 = maybe_store_rejected_user(
                             get_rejected_user(Auth), Req2),
                    {auth_failure, Req3};
                AuthnRes2 ->
                    Req3 = store_authn_res(AuthnRes2, Req2),
                    Identity = AuthnRes2#authn_res.identity,
                    %% Check on-behalf-of user is not locked
                    case menelaus_users:is_user_locked(Identity) of
                        false ->
                            case AuthnRes2#authn_res.identity =:=
                                AuthnRes#authn_res.identity of
                                true ->
                                    ok;
                                false ->
                                    %% Need to track on-behalf user's activity
                                    activity_tracker:handle_activity(AuthnRes2)
                            end,
                            {check_permission(AuthnRes2, Permission), Req3};
                        true ->
                            ?count_auth("error", "locked"),
                            {auth_failure, Req3}
                    end
            end;
        {error, auth_failure, AuthAuditProps} ->
            Req2 = maybe_store_rejected_user(get_rejected_user(Auth), Req),
            Req3 = maybe_set_auth_audit_props(Req2, AuthAuditProps),
            {auth_failure, Req3};
        {error, temporary_failure, AuthAuditProps} ->
            Req2 = maybe_set_auth_audit_props(Req, AuthAuditProps),
            {temporary_failure, Req2};
        {unfinished, RespHeaders} ->
            %% When mochiweb decides if it needs to close the connection
            %% it checks if body is "received" (and many other things)
            %% If body is not received it will close the connection
            %% but we don't want it to happen in this case
            %% because it is kind of "graceful" 401
            mochiweb_request:recv_body(Req),
            Req2 = append_resp_headers(RespHeaders, Req),
            {auth_failure, Req2}
    end.

on_behalf_session(#authn_res{session_id = Session}) when is_binary(Session) ->
    "session=" ++ binary_to_list(Session);
on_behalf_session(_) ->
    "".

%% SAML and JWT can specify extra groups and roles which must be transmitted
%% in cb-on-behalf-of requests. They cannot be determined from Identity alone.
-spec on_behalf_groups(#authn_res{}) -> string().
on_behalf_groups(#authn_res{extra_groups = []}) ->
    "";
on_behalf_groups(#authn_res{extra_groups = ExtraGroups}) ->
    "groups=" ++ lists:flatten(misc:intersperse(ExtraGroups, ",")).

-spec on_behalf_roles(#authn_res{}) -> string().
on_behalf_roles(#authn_res{extra_roles = []}) ->
    "";
on_behalf_roles(#authn_res{extra_roles = ExtraRoles}) ->
    RolesStr =
        [menelaus_web_rbac:role_to_string(R) || R <- ExtraRoles],
    "roles=" ++ lists:flatten(misc:intersperse(RolesStr, ",")).

%% SAML and JWT specify an expiration time when auth is valid. This time is
%% in Gregorian seconds unlike JWT specified exp time (seconds since epoch).
-spec on_behalf_expiry(#authn_res{}) -> string().
on_behalf_expiry(#authn_res{expiration_datetime_utc = undefined}) -> "";
on_behalf_expiry(#authn_res{expiration_datetime_utc = '_'}) -> "";
on_behalf_expiry(#authn_res{expiration_datetime_utc = Exp}) ->
    Seconds = calendar:datetime_to_gregorian_seconds(Exp),
    "expiry=" ++ integer_to_list(Seconds).

on_behalf_extras(AuthnRes) ->
    Entries = lists:filter(fun("") -> false;
                              (_) -> true
                           end,
                           [on_behalf_session(AuthnRes),
                            on_behalf_groups(AuthnRes),
                            on_behalf_roles(AuthnRes),
                            on_behalf_expiry(AuthnRes)]),
    lists:flatten(misc:intersperse(Entries, ";")).

parse_on_behalf_session(List) ->
    case proplists:get_value("session", List, undefined) of
        undefined -> undefined;
        SessionStr -> list_to_binary(SessionStr)
    end.

parse_on_behalf_groups(List) ->
    case proplists:get_value("groups", List, undefined) of
        undefined -> [];
        GroupStr -> string:lexemes(GroupStr, ",")
    end.

parse_on_behalf_roles(List) ->
    case proplists:get_value("roles", List, undefined) of
        undefined -> [];
        RolesStr ->
            Parsed = menelaus_web_rbac:parse_roles(RolesStr),
            {GoodRoles, BadRoles} = menelaus_roles:validate_roles(Parsed),
            BadRoles /= [] andalso
                ?log_warning("ignoring invalid roles in on-behalf-extras: ~p",
                             [BadRoles]),
            GoodRoles
    end.

parse_on_behalf_expiry(List) ->
    case proplists:get_value("expiry", List, undefined) of
        undefined -> undefined;
        SecondsStr ->
            Seconds = list_to_integer(SecondsStr),
            calendar:gregorian_seconds_to_datetime(Seconds)
    end.

-spec get_authn_res_from_on_behalf_of(User :: rbac_user_id(),
                                      Domain :: rbac_identity_type(),
                                      EncodedExtras :: string() | undefined) ->
          #authn_res{}.
get_authn_res_from_on_behalf_of(User, Domain, EncodedExtras) ->
    AuthnRes0 = #authn_res{identity = {User, Domain}},
    case EncodedExtras of
        undefined -> AuthnRes0;
        EncodedExtras when is_list(EncodedExtras) ->
            case (catch base64:decode_to_string(EncodedExtras)) of
                Decoded when is_list(EncodedExtras) ->
                    Extras =
                        lists:foldl(
                          fun(Token, Acc) ->
                                  {Key, [$=|Value]} =
                                      string:take(Token, "=", true),
                                  [{Key, Value} | Acc]
                          end, [], string:lexemes(Decoded, ";")),

                    Session = parse_on_behalf_session(Extras),
                    Groups = parse_on_behalf_groups(Extras),
                    Roles = parse_on_behalf_roles(Extras),
                    Expiry = parse_on_behalf_expiry(Extras),

                    AuthnRes0#authn_res{session_id = Session,
                                        extra_groups = Groups,
                                        extra_roles = Roles,
                                        expiration_datetime_utc = Expiry};
                _ -> AuthnRes0
            end;
        _ -> AuthnRes0
    end.

-spec apply_on_behalf_of_authn_res(#authn_res{}, mochiweb_request()) ->
          error | #authn_res{}.
apply_on_behalf_of_authn_res(AuthnRes, Req) ->
    case extract_on_behalf_of_authn_res(Req) of
        error ->
            error;
        undefined ->
            AuthnRes;
        {User, Domain, Extras} ->
            %% The permission is formed the way that it is currently granted
            %% to full admins only. We might consider to reformulate it
            %% like {[onbehalf], impersonate} or, such in the upcoming
            %% major release when we will be able to change roles
            %%
            %% Supporting on-behalf for user roles other than full admin
            %% is out of scope now, though it can be easily achived by checking
            %% each permission twice, against the authenticated user and against
            %% the impersonated one
            case menelaus_roles:is_allowed(
                   {[admin, security, admin], impersonate}, AuthnRes) of
                true ->
                    get_authn_res_from_on_behalf_of(User, Domain, Extras);
                false ->
                    error
            end
    end.

-spec acting_on_behalf(mochiweb_request()) -> boolean().
acting_on_behalf(Req) ->
    get_authenticated_identity(Req) =/= get_identity(Req).

-spec extract_on_behalf_of_authn_res(mochiweb_request()) ->
          error | undefined |
          {rbac_user_id(), rbac_identity_type(), string() | undefined}.
extract_on_behalf_of_authn_res(Req) ->
    case read_on_behalf_of_header(Req) of
        Header when is_list(Header) ->
            case parse_on_behalf_of_header(Header) of
                {User, Domain} ->
                    try list_to_existing_atom(Domain) of
                        ExistingDomain when ExistingDomain =:= local;
                                            ExistingDomain =:= external;
                                            ExistingDomain =:= admin ->
                            case parse_on_behalf_extras_header(Req) of
                                error ->
                                    ?log_debug("Invalid format of "
                                               "cb-on-behalf-extras:~s",
                                               [ns_config_log:tag_user_name(
                                                  Header)]),
                                    error;
                                Extras -> {User, ExistingDomain, Extras}
                            end;
                        _ ->
                            ?log_debug("Invalid domain in cb-on-behalf-of: ~s",
                                       [ns_config_log:tag_user_name(Header)]),
                            error
                    catch
                        error:badarg ->
                            ?log_debug("Invalid domain in cb-on-behalf-of: ~s",
                                       [ns_config_log:tag_user_name(Header)]),
                            error
                    end;
                _ ->
                    ?log_debug("Invalid format of cb-on-behalf-of: ~s",
                               [ns_config_log:tag_user_name(Header)]),
                    error
            end;
        undefined ->
            case read_on_behalf_extras(Req) of
                undefined -> undefined;
                Hdr ->
                    ?log_debug("Unexpected cb-on-behalf-extras: ~s",
                               [ns_config_log:tag_user_name(Hdr)]),
                    undefined
            end
    end.

read_on_behalf_of_header(Req) ->
    mochiweb_request:get_header_value("cb-on-behalf-of", Req).

read_on_behalf_extras(Req) ->
    mochiweb_request:get_header_value("cb-on-behalf-extras", Req).

parse_on_behalf_of_header(Header) ->
    case (catch base64:decode_to_string(Header)) of
        UserDomainStr when is_list(UserDomainStr) ->
            case string:chr(UserDomainStr, $:) of
                0 ->
                    error;
                I ->
                    {string:substr(UserDomainStr, 1, I - 1),
                     string:substr(UserDomainStr, I + 1)}
            end;
        _ ->
            error
    end.

parse_on_behalf_extras_header(Req) ->
    case read_on_behalf_extras(Req) of
        undefined -> undefined;
        Extras when is_list(Extras) -> Extras;
        _ -> error
    end.

-spec extract_identity_from_cert(binary()) ->
          tuple() | auth_failure | temporary_failure.
extract_identity_from_cert(CertDer) ->
    case ns_ssl_services_setup:get_user_name_from_client_cert(CertDer) of
        undefined ->
            auth_failure;
        failed ->
            auth_failure;
        UName ->
            case authenticate({client_cert_auth, UName}) of
                {ok, #authn_res{identity = Identity}, _, _} ->
                    Identity;
                {error, Type, _} ->
                    Type
            end
    end.

-spec is_anonymous(#authn_res{}) -> boolean().
is_anonymous(#authn_res{identity = ?ANONYMOUS_IDENTITY}) -> true;
is_anonymous(#authn_res{}) -> false;
%% For cases when authn_res is not in Req
%% (when called as is_anonymous(get_authn_res(Req)))
is_anonymous(undefined) -> true.

-spec check_permission(#authn_res{}, rbac_permission() | no_check | local) ->
          auth_failure | forbidden | allowed | password_expired.
check_permission(_AuthnRes, no_check) ->
    allowed;
check_permission(#authn_res{identity = {"@" ++ _, local_token}}, local) ->
    allowed;
check_permission(_, local) ->
    forbidden;
check_permission(#authn_res{} = AuthnRes, no_check_disallow_anonymous) ->
    case is_anonymous(AuthnRes) of
        true ->
            auth_failure;
        false ->
            allowed
    end;
check_permission(#authn_res{password_expired=true}, _) ->
    ?count_auth("error", "password_expired"),
    password_expired;
check_permission(#authn_res{identity = Identity} = AuthnRes,
                 Permission) ->
    Roles = menelaus_roles:get_compiled_roles(AuthnRes),
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
                    case is_anonymous(AuthnRes) of
                        true ->
                            %% we do allow some api's for anonymous
                            %% under some circumstances, but we want to return 401 in case
                            %% if autorization for requests with no auth fails
                            auth_failure;
                        false ->
                            forbidden
                    end
            end
    end.

-spec get_authn_res_audit_props(#authn_res{}) -> auth_audit_props().
get_authn_res_audit_props(#authn_res{extra_groups = ExtraGroups,
                                     extra_roles = ExtraRoles,
                                     expiration_datetime_utc = Expiration}) ->
    Props = [],
    Props1 = case ExtraGroups of
                 [] -> Props;
                 Groups ->
                     GroupsStr = lists:flatten(misc:intersperse(Groups, ",")),
                     [{mapped_groups, list_to_binary(GroupsStr)} | Props]
             end,
    Props2 = case ExtraRoles of
                 [] -> Props1;
                 Roles ->
                     RolesStr = lists:flatten(
                                  misc:intersperse(
                                    [menelaus_web_rbac:role_to_string(R) ||
                                        R <- Roles], ",")),
                     [{mapped_roles, list_to_binary(RolesStr)} | Props1]
             end,
    case Expiration of
        undefined -> Props2;
        Exp ->
            ExpiryWithLeeway = misc:iso_8601_fmt_datetime(Exp, "-", ":"),
            [{expiry_with_leeway, list_to_binary(ExpiryWithLeeway)} | Props2]
    end.

-spec maybe_set_auth_audit_props(mochiweb_request(),
                                 auth_audit_props()) ->
          mochiweb_request().
maybe_set_auth_audit_props(Req, []) ->
    Req;
maybe_set_auth_audit_props(Req, AuthAuditProps) ->
    mochiweb_request:set_meta(auth_audit_props, AuthAuditProps, Req).

-spec check_expiration(#authn_res{}) -> ok | {error, expired}.
check_expiration(#authn_res{expiration_datetime_utc = undefined}) ->
    ok;
check_expiration(#authn_res{expiration_datetime_utc = Expiration}) ->
    Now = calendar:universal_time(),
    case Now > Expiration of
        true -> {error, expired};
        false -> ok
    end.

-spec allow_fallback_auth() -> boolean().
allow_fallback_auth() ->
    %% Previously, we allowed fallback auth unconditionally.
    %% Now, we only allow it for users who have enable_legacy_fallback_auth
    %% set. This is a temporary measure to allow users to authenticate with
    %% their legacy credentials until duplicate credentials are resolved.
    %% Note that memcached doesn't support this.
    case cluster_compat_mode:is_cluster_79() of
        true ->
            ns_config:read_key_fast(enable_legacy_fallback_auth, false);
        false ->
            true
    end.

-spec maybe_log_fallback_auth_success(
        Username :: rbac_user_id(),
        Res :: {error, auth_failure, auth_audit_props()} |
               {ok, #authn_res{}, [RespHeader], auth_audit_props()}) -> ok when
      RespHeader :: {string(), string()}.
maybe_log_fallback_auth_success(Username, Res) ->
    case Res of
        {ok, _, _, _} ->
            ale:warn(?USER_LOGGER,
                     "Local authentication failed but external authentication "
                     "succeeded for user ~s. This indicates duplicate user "
                     "accounts. Please resolve the conflict.",
                [ns_config_log:tag_user_name(Username)]),
            ok;
        _ ->
            ok
    end.
