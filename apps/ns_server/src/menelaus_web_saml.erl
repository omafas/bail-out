%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Support for SSO

-module(menelaus_web_saml).

-export([handle_auth/1,
         handle_deauth/1,
         handle_saml_metadata/1,
         handle_get_saml_consume/1,
         handle_post_saml_consume/1,
         handle_get_saml_logout/1,
         handle_post_saml_logout/1,
         handle_get_settings/2,
         handle_post_settings/1,
         handle_delete_settings/1,
         handle_uilogout_post/1,
         handle_get_error/1,
         defaults/0,
         is_enabled/0]).

-include("ns_common.hrl").
-include("rbac.hrl").
-include_lib("ns_common/include/cut.hrl").
-include_lib("esaml/include/esaml.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PERSISTENT_NAMEID_FORMAT,
        "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent").

-define(SAML_RESPONSE_MAX_SIZE_MIN, 256 * 1024).
-define(SAML_RESPONSE_MAX_SIZE_MAX, 1024 * 1024).

%%%===================================================================
%%% API
%%%===================================================================

handle_get_settings(Path, Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOSettings = extract_saml_settings(),
    SSOSettingsUpdated =
        case proplists:get_value(enabled, SSOSettings) of
            true ->
                #esaml_sp{metadata_uri = MetadataURL,
                          consume_uri = ConsumeURL,
                          logout_uri = LogoutURL} =
                    build_sp_metadata(SSOSettings, Req),
                misc:update_proplist(SSOSettings,
                                     [{sp_md_url, MetadataURL},
                                      {sp_md_consume_url, ConsumeURL},
                                      {sp_md_logout_url, LogoutURL}]);
            false ->
                SSOSettings
        end,
    menelaus_web_settings2:handle_get(Path, params(), fun type_spec/1,
                                      SSOSettingsUpdated, Req).

handle_post_settings(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    CurrentProps = ns_config:read_key_fast(saml_settings, []),
    menelaus_web_settings2:handle_post(
      fun (Proplist, NewReq) ->
          SSOProps = lists:map(fun ({[K], V}) -> {K, V} end, Proplist),
          set_sso_options(SSOProps, CurrentProps),
          {AuditJson} = menelaus_web_settings2:prepare_json(
                          [], params(), fun type_spec/1, SSOProps),
          ns_audit:settings(Req, modify_saml, AuditJson),
          is_enabled() orelse menelaus_ui_auth:logout_by_session_type(saml),
          handle_get_settings([], NewReq)
      end, [], params(), fun type_spec/1, CurrentProps, defaults(), Req).

set_sso_options(NewProps, CurrentProps) ->
    CurPropsWithDefaults = misc:update_proplist(defaults(), CurrentProps),
    PropsToSet = misc:update_proplist(CurPropsWithDefaults, NewProps),
    Enabled = proplists:get_value(enabled, PropsToSet),
    case Enabled of
        false ->
            ns_config:set(saml_settings, PropsToSet),
            ok;
        true ->
            {PropsToSet2, ParsedMetadata} =
                case verify_metadata_settings(PropsToSet,
                                              CurPropsWithDefaults) of
                    {ok, {UpdatedProps, Metadata}} -> {UpdatedProps, Metadata};
                    {error, Msg} -> menelaus_util:global_error_exception(
                                      400, iolist_to_binary(Msg))
                end,
            Fingerprints = cb_saml:extract_fingerprints(ParsedMetadata,
                                                        PropsToSet2),
            NewUUID = misc:uuid_v4(),
            PropsToSetWithUuid = misc:update_proplist(PropsToSet2,
                                                      [{uuid, NewUUID}]),
            {commit, _} =
                ns_config:run_txn(
                  fun (OldCfg, SetFun) ->
                      {commit, functools:chain(
                                 OldCfg,
                                 [SetFun(saml_sign_fingerprints,
                                         Fingerprints, _),
                                  SetFun(saml_settings, PropsToSetWithUuid, _)])}
                  end),
            cb_saml:cache_idp_metadata(ParsedMetadata, PropsToSetWithUuid),
            ok
    end.

verify_metadata_settings(PropsToSet, CurPropsWithDefaults) ->
    PropsWithDefaults = misc:update_proplist(CurPropsWithDefaults, PropsToSet),
    true = proplists:get_value(enabled, PropsWithDefaults),
    Modified = PropsToSet -- CurPropsWithDefaults,
    IsUrlChanged = proplists:is_defined(enabled, Modified) orelse
                   proplists:is_defined(idp_metadata_origin, Modified) orelse
                   proplists:is_defined(idp_metadata_url, PropsToSet),
    case proplists:get_value(idp_metadata_origin, PropsWithDefaults) of
        upload ->
            {zip, MetaZipped} = proplists:get_value(idp_metadata,
                                                    PropsWithDefaults),
            MetaBin = zlib:unzip(MetaZipped),
            Parsed = cb_saml:try_parse_idp_metadata(MetaBin, false),
            {ok, {PropsToSet, Parsed}};
        http_one_time when not IsUrlChanged ->
            {zip, MetaZipped} = proplists:get_value(idp_metadata,
                                                    PropsWithDefaults),
            MetaBin = zlib:unzip(MetaZipped),
            Parsed = cb_saml:try_parse_idp_metadata(MetaBin, false),
            {ok, {PropsToSet, Parsed}};
        Origin when Origin == http_one_time; Origin == http ->
            URL = proplists:get_value(idp_metadata_url, PropsWithDefaults),
            {_, FPs} = proplists:get_value(trusted_fingerprints,
                                           PropsWithDefaults),
            case cb_saml:load_idp_metadata(URL, PropsWithDefaults,
                                           FPs) of
                {ok, {MetaStr, Parsed}} when Origin == http_one_time ->
                    %% We will never update it automatically, so
                    %% save it in esaml settings in ns_config
                    MetaZipped = {zip, zlib:zip(MetaStr)},
                    UpdatedProps = misc:update_proplist(
                                     PropsToSet,
                                     [{idp_metadata, MetaZipped}]),
                    {ok, {UpdatedProps, Parsed}};
                {ok, {_MetaStr, Parsed}} when Origin == http ->
                    {ok, {PropsToSet, Parsed}};
                {error, Reason} ->
                    {error, lists:flatten(cb_saml:format_error(Reason))}
            end
    end.

handle_delete_settings(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    ns_config:delete(saml_settings),
    cb_saml:cleanup_metadata(),
    is_enabled() orelse menelaus_ui_auth:logout_by_session_type(saml),
    menelaus_util:reply(Req, 200).

handle_auth(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOOpts = extract_saml_settings_if_enabled(),
    ?log_debug("Starting saml authentication "),
    IDPMetadata = try_get_idp_metadata(SSOOpts),
    SPMetadata = build_sp_metadata(SSOOpts, Req),
    Binding = proplists:get_value(authn_binding, SSOOpts),
    RelayState = <<"">>,
    %% Using "persistent" by default because this seems to be the only option
    %% that single logout works with
    NameIDFormat = proplists:get_value(authn_nameID_format, SSOOpts),
    SignedXml = esaml_sp:generate_authn_request(_, SPMetadata, NameIDFormat),
    case Binding of
        redirect ->
            IDPURL = IDPMetadata#esaml_idp_metadata.login_redirect_location,
            reply_via_redirect(IDPURL, SignedXml(IDPURL), RelayState, [], Req);
        post ->
            IDPURL = IDPMetadata#esaml_idp_metadata.login_post_location,
            reply_via_post(IDPURL, SignedXml(IDPURL), RelayState, [], Req)
    end.

%% Simply check if SLO is enabled and tell UI to redirect to /saml/deauth.
%% Note: We can't perform SLO from here because it's an ajax request.
%% Note: We can't use 302 or similar, because XMLHttpRequest will follow it
%% automatically, which we don't need.
handle_uilogout_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOOpts = extract_saml_settings(),
    case proplists:get_value(enabled, SSOOpts) andalso
         proplists:get_value(single_logout, SSOOpts) of
        true ->
            menelaus_util:reply_json(
              Req, {[{redirect, <<"/saml/deauth">>}]}, 400);
        false ->
            erlang:error(disabled)
    end.


handle_deauth(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOOpts = extract_saml_settings_if_enabled(),
    ?log_debug("Starting saml single logout"),
    case proplists:get_value(single_logout, SSOOpts) of
        true -> ok;
        false ->
            ?log_debug("Single logout is turned off"),
            menelaus_util:web_exception(404, "not found")
    end,

    {Session, Headers} = menelaus_auth:complete_uilogout(Req),

    case Session of
        #uisession{type = saml, session_name = NameID} ->
            handle_single_logout(SSOOpts, NameID, Headers, Req);
        #uisession{type = simple} ->
            ?log_debug("User is not a saml user, ignoring single logout"),
            menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                     [{"Location", "/"} | Headers]);
        undefined ->
            ?log_debug("User not authenticated"),
            menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                     [{"Location", "/"} | Headers])
    end.

handle_single_logout(SSOOpts, NameID, ExtraHeaders, Req) ->
    IDPMetadata = try_get_idp_metadata(SSOOpts),
    SPMetadata = build_sp_metadata(SSOOpts, Req),
    NameIDFormat = proplists:get_value(authn_nameID_format, SSOOpts),
    Subject = #esaml_subject{name = binary_to_list(NameID),
                             name_format = NameIDFormat},
    SignedXml = esaml_sp:generate_logout_request(_, "", Subject, SPMetadata),
    case proplists:get_value(logout_binding, SSOOpts) of
        redirect ->
            URL = IDPMetadata#esaml_idp_metadata.logout_redirect_location,
            reply_via_redirect(URL, SignedXml(URL), <<>>, ExtraHeaders, Req);
        post ->
            URL = IDPMetadata#esaml_idp_metadata.logout_post_location,
            reply_via_post(URL, SignedXml(URL), <<>>, ExtraHeaders, Req)
    end.

handle_saml_metadata(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOOpts = extract_saml_settings_if_enabled(),
    SPMetadata = build_sp_metadata(SSOOpts, Req),
    SignedXml = esaml_sp:generate_metadata(SPMetadata),
    Metadata = xmerl:export([SignedXml], xmerl_xml),
    menelaus_util:reply(
      Req, Metadata, 200, [{"Content-Type", "application/samlmetadata+xml"}]).

handle_get_saml_consume(Req) ->
    handle_saml_consume(Req, mochiweb_request:parse_qs(Req)).

handle_post_saml_consume(Req) ->
    handle_saml_consume(Req, mochiweb_request:parse_post(Req)).

handle_saml_consume(Req, UnvalidatedParams) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOOpts = extract_saml_settings_if_enabled(),
    ?log_debug("Starting saml consume"),
    %% Making sure metadata is up to date. By doing that we also update
    %% certificates that will be used for assertion verification
    IDPMetadata = try_get_idp_metadata(SSOOpts),
    ExpectedIssuer = case proplists:get_value(verify_issuer, SSOOpts) of
                         true -> IDPMetadata#esaml_idp_metadata.entity_id;
                         false -> any
                     end,
    SPMetadata = build_sp_metadata(SSOOpts, Req),
    DupeCheck = proplists:get_value(dupe_check, SSOOpts),
    validator:handle(
      fun (Params) ->
          MaybeAssertion = proplists:get_value('SAMLResponse', Params),
          ?log_debug("Decoded assertion: " ++ ns_config_log:tag_misc_item("~p"),
                     [MaybeAssertion]),
          handle_saml_assertion(Req, MaybeAssertion, SSOOpts)
      end, Req, UnvalidatedParams,
      [validator:string('SAMLEncoding', _),
       validator:default('SAMLEncoding', "", _),
       validate_authn_response('SAMLResponse', 'SAMLEncoding',
                               SPMetadata, ExpectedIssuer, DupeCheck, Req, _),
       validator:required('SAMLResponse', _),
       validator:string('RelayState', _)]).

handle_get_error(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    _SSOOpts = extract_saml_settings_if_enabled(),
    validator:handle(
      fun (Params) ->
          Id = iolist_to_binary(proplists:get_value(id, Params)),
          case cb_saml:get_error_msg(Id) of
              {ok, Msg} -> menelaus_util:reply_json(Req, {[{error, Msg}]}, 200);
              {error, not_found} -> menelaus_util:reply_not_found(Req)
          end
      end, Req, qs,
      [validator:string('id', _),
       validator:required('id', _)]).

handle_get_saml_logout(Req) ->
    handle_saml_logout(Req, mochiweb_request:parse_qs(Req)).

handle_post_saml_logout(Req) ->
    handle_saml_logout(Req, mochiweb_request:parse_post(Req)).

handle_saml_logout(Req, UnvalidatedParams) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_76(),
    SSOOpts = extract_saml_settings_if_enabled(),
    ?log_debug("Starting saml logout"),
    IDPMetadata = try_get_idp_metadata(SSOOpts),
    SPMetadata = build_sp_metadata(SSOOpts, Req),
    validator:handle(
      fun (Params) ->
          LogoutReq = proplists:get_value('SAMLRequest', Params),
          LogoutResp = proplists:get_value('SAMLResponse', Params),
          case {LogoutReq, LogoutResp} of
              {undefined, undefined} ->
                  ?log_debug("Empty saml message"),
                  menelaus_util:reply_text(Req, "Missing SAML message", 400);
              {{ok, #esaml_logoutreq{name = NameID}}, _} ->
                  SessionName = iolist_to_binary(NameID),
                  menelaus_ui_auth:logout_by_session_name(saml,
                                                          SessionName),
                  SignedXml = esaml_sp:generate_logout_response(_, success,
                                                                SPMetadata),
                  BindingToUse = proplists:get_value(logout_binding, SSOOpts),
                  case BindingToUse of
                      redirect ->
                          URL = IDPMetadata#esaml_idp_metadata.logout_redirect_location,
                          reply_via_redirect(URL, SignedXml(URL), <<>>, [],
                                             Req);
                      post ->
                          URL = IDPMetadata#esaml_idp_metadata.logout_post_location,
                          reply_via_post(URL, SignedXml(URL), <<>>, [],
                                         Req)
                  end;
              {_, {ok, #esaml_logoutresp{}}} ->
                  undefined = menelaus_auth:get_identity(Req),
                  ?log_debug("Successful logout response"),
                  menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                           [{"Location", "/"}]);
              {{error, Msg}, _} ->
                  redirect_to_ui_with_error(Req, Msg);
              {_, {error, Msg}} ->
                  redirect_to_ui_with_error(Req, Msg)
          end
      end, Req, UnvalidatedParams,
      [validator:string('SAMLEncoding', _),
       validator:default('SAMLEncoding', "", _),
       validate_logout_response('SAMLResponse', 'SAMLEncoding', SPMetadata, _),
       validate_logout_request('SAMLRequest', 'SAMLEncoding', SPMetadata, _)]).

is_enabled() ->
    proplists:get_bool(enabled, extract_saml_settings()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

extract_saml_settings() ->
    SSOSettings = ns_config:read_key_fast(saml_settings, []),
    misc:update_proplist(defaults(), SSOSettings).

extract_saml_settings_if_enabled() ->
    Opts = extract_saml_settings(),
    assert_saml_enabled(Opts),
    Opts.

assert_saml_enabled(Opts) ->
    %% Pretend that we don't have this endpoint when saml is disabled
    case proplists:get_value(enabled, Opts) of
        true -> ok;
        false -> menelaus_util:web_exception(404, "not found")
    end.

reply_via_redirect("", _, _, _, _) ->
    menelaus_util:web_exception(500,
                                <<"IDP doesn't support Redirect binding">>);
reply_via_redirect(IDPURL, SignedXml, RelayState, ExtraHeaders, Req)
                                     when is_list(IDPURL), length(IDPURL) > 0 ->
    Location = esaml_binding:encode_http_redirect(
                 IDPURL,
                 SignedXml,
                 _Username = undefined,
                 RelayState),
    LocationStr = binary_to_list(Location),
    ?log_debug("Redirecting user to ~s using HTTP code 302, full url: ~s",
               [IDPURL, ns_config_log:tag_misc_item(LocationStr)]),
    menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                             [{allow_cache, false},
                              {"Location", LocationStr} | ExtraHeaders]).

reply_via_post("", _, _, _, _) ->
    menelaus_util:web_exception(500, <<"IDP doesn't support POST binding">>);
reply_via_post(IDPURL, SignedXml, RelayState, ExtraHeaders, Req)
                                     when is_list(IDPURL), length(IDPURL) > 0 ->
    HTMLBin = esaml_binding:encode_http_post(IDPURL, SignedXml, RelayState),
    ?log_debug("Redirecting user to ~s using POST:~n~s",
               [IDPURL, ns_config_log:tag_misc_item(HTMLBin)]),
    menelaus_util:reply(Req, HTMLBin, 200,
                        [{allow_cache, false},
                         {"Content-Type", "text/html"} | ExtraHeaders]).

build_sp_metadata(Opts, Req) ->
    BaseURL = build_base_url(
                proplists:get_value(base_url, Opts),
                proplists:get_value(custom_base_url, Opts),
                proplists:get_value(base_url_scheme, Opts),
                Req) ++ "/saml",

    OrgName = proplists:get_value(org_name, Opts),
    OrgDispName = proplists:get_value(org_display_name, Opts),
    OrgUrl = proplists:get_value(org_url, Opts),

    ContactName = proplists:get_value(contact_name, Opts),
    ContactEmail = proplists:get_value(contact_email, Opts),

    IdpSignsAssertions = proplists:get_value(verify_assertion_sig, Opts),
    IdpSignsEnvelopes = proplists:get_value(verify_assertion_envelop_sig, Opts),
    IdpSignsLogoutReq = proplists:get_value(verify_logout_req_sig, Opts),
    CacheDuration = proplists:get_value(sp_md_cache_duration, Opts),
    ClockSkew = proplists:get_value(clock_skew, Opts),
    Recipient = case proplists:get_value(verify_recipient, Opts) of
                    false -> any;
                    consumeURL -> undefined;
                    custom -> proplists:get_value(verify_recipient_value, Opts)
                end,

    SP = #esaml_sp{
           idp_signs_assertions = IdpSignsAssertions,
           idp_signs_envelopes = IdpSignsEnvelopes,
           idp_signs_logout_requests = IdpSignsLogoutReq,
           consume_uri = BaseURL ++ "/"?SAML_CONSUME_ENDPOINT_PATH,
           metadata_uri = BaseURL ++ "/"?SAML_METADATA_ENDPOINT_PATH,
           logout_uri = BaseURL ++ "/"?SAML_LOGOUT_ENDPOINT_PATH,
           assertion_recipient = Recipient,
           org = #esaml_org{
                   name = [{en, OrgName}],
                   displayname = OrgDispName,
                   url = OrgUrl
                 },
           tech = #esaml_contact{
                    name = ContactName,
                    email = ContactEmail
                  },
           cache_duration = CacheDuration,
           clock_skew = ClockSkew
         },

    Cert = proplists:get_value(cert, Opts),
    Key = proplists:get_value(key, Opts),

    SignRequests = proplists:get_value(sign_requests, Opts),
    SignMetadata = proplists:get_value(sign_metadata, Opts),

    SP2 = case (Key =/= undefined) andalso (Cert =/= undefined) of
              true ->
                  {_, KeyEntry} = Key,
                  KeyEntryDecoded = public_key:pem_entry_decode(KeyEntry),
                  {_, Der} = Cert,
                  {_, CertChain} = proplists:get_value(chain, Opts),
                  SP#esaml_sp{key = KeyEntryDecoded,
                              certificate = Der,
                              cert_chain = CertChain,
                              sp_sign_requests = SignRequests,
                              sp_sign_metadata = SignMetadata};
              false ->
                  SP
          end,

    FPsUsage = proplists:get_value(fingerprints_usage, Opts),

    FPs = case FPsUsage of
              everything ->
                  {_, Parsed} = proplists:get_value(trusted_fingerprints, Opts),
                  Parsed;
              U when U =:= metadataInitialOnly; U =:= metadataOnly ->
                  case cb_saml:trusted_fingerprints_from_metadata() of
                      {ok, L} -> L;
                      %% It may happen that it is expired or not set
                      %% when we are building reply for /sso/<nam>/samlMetadata
                      %% In this case esaml doesn't need trusted fingerprints
                      %% because it is not verifying anything
                      {error, not_set} -> [];
                      {error, expired} -> []
                  end
          end,

    SP2#esaml_sp{entity_id = case proplists:get_value(entity_id, Opts) of
                                 "" -> undefined;
                                 S -> S
                             end,
                 trusted_fingerprints = FPs}.

try_get_idp_metadata(Opts) ->
    case cb_saml:get_idp_metadata(Opts) of
        {ok, Meta} -> Meta;
        {error, Reason} ->
            Msg = io_lib:format("Failed to get IDP metadata. "
                                "Reason: ~p", [Reason]),
            menelaus_util:web_exception(500, iolist_to_binary(Msg))
    end.

validate_assertion(SAMLResponse, SAMLEncoding, SPMetadata, ExpectedIssuer,
                   DupeCheck, Req) ->
    try esaml_binding:decode_response(SAMLEncoding, SAMLResponse) of
        Xml ->
            %% We can't use fun esaml_util:check_dupe_ets/2 because
            %% it makes rpc:call to all nodes with a fun which is dangerous
            DupeCheckFun =
            case DupeCheck of
                local -> fun cb_saml:check_dupe/2;
                global -> fun cb_saml:check_dupe_global/2;
                disabled -> fun (_, _) -> ok end
            end,
            case esaml_sp:validate_assertion(Xml, DupeCheckFun,
                                             SPMetadata,
                                             ExpectedIssuer) of
                {ok, Assertion} ->
                    ?log_debug("Assertion validated successfully"),
                    {value, {ok, Assertion}};
                {error, {decryption_problem, {E, ST}}} ->
                    ?log_debug("Assertion decryption failed: ~p~n~p",
                               [E, ST]),
                    ns_audit:login_failure(Req),
                    {value, {error, "Assertion decryption failed"}};
                {error, E} ->
                    ?log_debug("Assertion validation failed: ~p", [E]),
                    ns_audit:login_failure(Req),
                    Msg = cb_saml:format_error({validate_assertion, E}),
                    {value, {error, Msg}}
            end
    catch
        _:Reason ->
            ?log_debug("Failed to decode authn response:~n~p", [Reason]),
            Msg = io_lib:format("Assertion decode failed: ~p", [Reason]),
            {value, {error, Msg}}
    end.

validate_saml_response_size(SAMLResponse) ->
    Settings = extract_saml_settings(),
    MaxSize = proplists:get_value(sp_saml_response_max_size, Settings),
    Size = erlang:size(SAMLResponse),
    case Size > MaxSize of
        true ->
            Msg = io_lib:format("SAML response larger than max configured size."
                                " Max size - ~p bytes. Size ~p bytes",
                                [MaxSize, Size]),
            {error, Msg};
        false ->
            ok
    end.

validate_authn_response(NameResp, NameEnc, SPMetadata, ExpectedIssuer,
                        DupeCheck, Req, State) ->
    validator:validate_relative(
      fun (Resp, Enc) ->
          SAMLEncoding = list_to_binary(Enc),
          SAMLResponse = list_to_binary(Resp),
          ?log_debug("Received saml authn response: ~s~nEncoding: ~s",
                     [ns_config_log:tag_misc_item(SAMLResponse), SAMLEncoding]),
          case validate_saml_response_size(SAMLResponse) of
              ok ->
                  validate_assertion(
                    SAMLResponse, SAMLEncoding, SPMetadata,
                    ExpectedIssuer, DupeCheck, Req);
              {error, Msg} ->
                  {value, {error, Msg}}
          end
      end, NameResp, NameEnc, State).

validate_logout_request(NameReq, NameEnc, SPMetadata, State) ->
    validator:validate_relative(
      fun (LOReq, Enc) ->
          SAMLEncoding = list_to_binary(Enc),
          SAMLRequest = list_to_binary(LOReq),
          ?log_debug("Received saml logout request: ~s~nEncoding: ~s",
                     [ns_config_log:tag_misc_item(SAMLRequest), SAMLEncoding]),
          %% Seems like decode_response can actually decode requests as well
          try esaml_binding:decode_response(SAMLEncoding, SAMLRequest) of
              Xml ->
                  case esaml_sp:validate_logout_request(Xml, SPMetadata) of
                      {ok, LogoutReq} ->
                          {value, {ok, LogoutReq}};
                      {error, E} ->
                          ?log_debug("Logout req validation failed: ~p", [E]),
                          Msg = io_lib:format("Logout req validation failed:"
                                              " ~p", [E]),
                          {value, {error, Msg}}
                  end
          catch
              _:Reason ->
                  ?log_debug("Failed to decode logout request:~n~p", [Reason]),
                  Msg = io_lib:format("Logout request decode failed: ~p",
                                      [Reason]),
                  {value, {error, Msg}}
          end
      end, NameReq, NameEnc, State).

validate_logout_response(NameResp, NameEnc, SPMetadata, State) ->
    validator:validate_relative(
      fun (LOResp, Enc) ->
          SAMLEncoding = list_to_binary(Enc),
          SAMLResponse = list_to_binary(LOResp),
          ?log_debug("Received saml logout response: ~s~nEncoding: ~s",
                     [ns_config_log:tag_misc_item(SAMLResponse), SAMLEncoding]),
          %% Seems like decode_response can actually decode requests as well
          try esaml_binding:decode_response(SAMLEncoding, SAMLResponse) of
              Xml ->
                  case esaml_sp:validate_logout_response(Xml, SPMetadata) of
                      {ok, LogoutResp} ->
                          {value, {ok, LogoutResp}};
                      {error, {status, Status, SecondLevelStatus}} ->
                          Msg = io_lib:format("SAML IDP returned status: ~p "
                                              "(second level status: ~p) in "
                                              "logout response",
                                              [Status, SecondLevelStatus]),
                          {value, {error, Msg}};
                      {error, E} ->
                          ?log_debug("Logout resp validation failed: ~p", [E]),
                          Msg = io_lib:format("Logout resp validation failed:"
                                              " ~p", [E]),
                          {value, {error, Msg}}
                  end
          catch
              _:Reason ->
                  ?log_debug("Failed to decode logout response:~n~p", [Reason]),
                  Msg = io_lib:format("Logout response decode failed: ~p",
                                      [Reason]),
                  {value, {error, Msg}}
          end
      end, NameResp, NameEnc, State).

build_base_url(node, _, Scheme, Req) ->
    build_node_url(Scheme, undefined, undefined, Req);
build_base_url(alternate, _, Scheme, Req) ->
    Node = node(),
    {AltHostName, AltPorts} =
        service_ports:get_raw_external_host_and_ports(
          Node, ns_config:latest()),
    PortName = case Scheme of
                   http -> rest_port;
                   https -> ssl_rest_port
               end,
    AltPort = proplists:get_value(PortName, AltPorts),
    build_node_url(Scheme, AltHostName, AltPort, Req);
build_base_url(custom, URLBin, _Scheme, _Req) when is_list(URLBin) ->
    URLBin.

build_node_url(Scheme, Host, undefined, Req) ->
    PortName = case Scheme of
                   http -> rest_port;
                   https -> ssl_rest_port
               end,
    Port = service_ports:get_port(PortName),
    true = is_integer(Port),
    build_node_url(Scheme, Host, Port, Req);
build_node_url(Scheme, undefined, Port, Req) ->
    H = misc:extract_node_address(node()),
    Host = case misc:is_localhost(H) of
               true  -> menelaus_util:local_addr(Req);
               false -> H
           end,
    build_node_url(Scheme, Host, Port, Req);
build_node_url(Scheme, Host, Port, _Req) ->
    URL = io_lib:format("~p://~s:~b",
                        [Scheme, misc:maybe_add_brackets(Host), Port]),
    lists:flatten(URL).

params() ->
    [{"enabled",
      #{cfg_key => enabled,
        type => bool}},
     {"idpMetadataOrigin",
      #{cfg_key => idp_metadata_origin,
        type => {one_of, existing_atom, [upload, http_one_time, http]}}},
     {"idpMetadata",
      #{cfg_key => idp_metadata,
        type => saml_metadata,
        mandatory => fun (#{enabled := true,
                            idp_metadata_origin := upload}) -> true;
                         (_) -> false
                     end}},
     {"idpMetadataURL",
      #{cfg_key => idp_metadata_url,
        type => {url, [<<"http">>, <<"https">>]},
        mandatory => fun (#{enabled := true,
                            idp_metadata_origin := O}) -> O =/= upload;
                         (_) -> false
                     end}},
     {"idpMetadataHttpTimeoutMs",
      #{cfg_key => md_http_timeout,
        type => pos_int}},
     {"idpSignsMetadata",
      #{cfg_key => idp_signs_metadata,
        type => bool}},
     {"idpMetadataRefreshIntervalS",
      #{cfg_key => idp_metadata_refresh_interval,
        type => pos_int}},
     {"idpMetadataConnectAddressFamily",
      #{cfg_key => md_address_family,
        type => {one_of, existing_atom, [undefined, inet, inet6]}}},
     {"idpMetadataTLSVerifyPeer",
      #{cfg_key => md_tls_verify_peer,
        type => bool}},
     {"idpMetadataTLSCAs",
      #{cfg_key => md_tls_ca,
        type => certificate_chain}},
     {"idpMetadataTLSSNI",
      #{cfg_key => md_tls_sni,
        type => string}},
     {"idpMetadataTLSExtraOpts",
      #{cfg_key => md_tls_extra_opts,
        type => tls_opts}},
     {"idpAuthnBinding",
      #{cfg_key => authn_binding,
        type => {one_of, existing_atom, [post, redirect]}}},
     {"idpLogoutBinding",
      #{cfg_key => logout_binding,
        type => {one_of, existing_atom, [post, redirect]}}},

     %% See http://docs.oasis-open.org/security/saml/v2.0/saml-core-2.0-os.pdf
     %% section 8.3 for more info
     {"authnNameIDFormat",
      #{cfg_key => authn_nameID_format,
        type => string}},
     {"singleLogoutEnabled",
      #{cfg_key => single_logout, type => bool}},
    %% See http://docs.oasis-open.org/security/saml/v2.0/saml-core-2.0-os.pdf
    %% section 2.7.3.1
     {"usernameAttribute",
      #{cfg_key => username_attribute,
        type => string}},
     {"groupsAttribute",
      #{cfg_key => groups_attribute,
        type => string}},
     {"groupsAttributeSep",
      #{cfg_key => groups_attribute_sep,
        type => string}},
     {"groupsFilterRE",
      #{cfg_key => groups_filter_re,
        type => regex}},
     {"rolesAttribute",
      #{cfg_key => roles_attribute,
        type => string}},
     {"rolesAttributeSep",
      #{cfg_key => roles_attribute_sep,
        type => string}},
     {"rolesFilterRE",
      #{cfg_key => roles_filter_re,
        type => regex}},
     %% if empty, use Metadata URL as entity id
     {"spEntityId",
      #{cfg_key => entity_id,
        type => string}},
     {"spBaseURLType",
      #{cfg_key => base_url,
        type => {one_of, existing_atom, [node, alternate, custom]}}},
     {"spBaseURLScheme",
      #{cfg_key => base_url_scheme,
        type => {one_of, existing_atom, [https, http]}}},
     {"spCustomBaseURL",
      #{cfg_key => custom_base_url,
        type => {url, [<<"http">>, <<"https">>]},
        mandatory => fun (#{enabled := true, base_url := custom}) -> true;
                         (_) -> false
                     end}},
     {"spOrgName",
      #{cfg_key => org_name, type => string}},
     {"spOrgDisplayName",
      #{cfg_key => org_display_name, type => string}},
     {"spOrgURL",
      #{cfg_key => org_url, type => string}},
     {"spContactName",
      #{cfg_key => contact_name, type => string}},
     {"spContactEmail",
      #{cfg_key => contact_email, type => string}},
     {"spVerifyAssertionSig",
      #{cfg_key => verify_assertion_sig, type => bool}},
     {"spVerifyAssertionEnvelopSig",
      #{cfg_key => verify_assertion_envelop_sig, type => bool}},
     {"spVerifyLogoutReqSig",
      #{cfg_key => verify_logout_req_sig, type => bool}},
     {"spVerifyRecipient",
      #{cfg_key => verify_recipient,
        type => {one_of, existing_atom, [consumeURL, custom, false]}}},
     {"spVerifyRecipientValue",
      #{cfg_key => verify_recipient_value,
        type => string,
        mandatory => fun (#{enabled := true,
                            verify_recipient := custom}) -> true;
                         (_) -> false
                     end}},
     {"spCertificate",
      #{cfg_key => cert,
        type => certificate,
        mandatory => fun (#{enabled := true,
                            sign_requests := S1,
                            sign_metadata := S2}) -> S1 or S2;
                         (_) -> false
                     end}},
     {"spKey",
      #{cfg_key => key,
        type => pkey,
        mandatory => fun (#{enabled := true,
                            sign_requests := S1,
                            sign_metadata := S2}) -> S1 or S2;
                         (_) -> false
                     end}},
     {"spChain",
      #{cfg_key => chain,
        type => certificate_chain}},
     {"spSignRequests",
      #{cfg_key => sign_requests,
        type => bool}},
     {"spSignMetadata",
      #{cfg_key => sign_metadata,
        type => bool}},
     {"spTrustedFingerprints",
      #{cfg_key => trusted_fingerprints,
        type => fingerprint_list}},
     {"spTrustedFingerprintsUsage",
      #{cfg_key => fingerprints_usage,
        type => {one_of, existing_atom,
                 [everything, metadataOnly, metadataInitialOnly]}}},
     {"spSessionExpire",
      #{cfg_key => session_expire,
        type => {one_of, existing_atom, [false, 'SessionNotOnOrAfter']}}},
     {"spAssertionDupeCheck",
      #{cfg_key => dupe_check,
        type => {one_of, existing_atom, [local, global, disabled]}}},
     {"spMetadataCacheDuration",
      #{cfg_key => sp_md_cache_duration,
        type => iso8601_duration}},
     {"spMetadataURL",
      #{cfg_key => sp_md_url,
        type => {read_only, string}}},
     {"spConsumeURL",
      #{cfg_key => sp_md_consume_url,
        type => {read_only, string}}},
     {"spLogoutURL",
      #{cfg_key => sp_md_logout_url,
        type => {read_only, string}}},
     {"spClockSkewS",
      #{cfg_key => clock_skew,
        type => int}},
     {"spVerifyIssuer",
      #{cfg_key => verify_issuer,
        type => bool}},
     {"spSAMLResponseMaxSize",
      #{cfg_key => sp_saml_response_max_size,
        type => {int,
                 ?SAML_RESPONSE_MAX_SIZE_MIN,
                 ?SAML_RESPONSE_MAX_SIZE_MAX}}}].

defaults() ->
    [{enabled, false},
     {org_name, ""},
     {org_display_name, ""},
     {org_url, ""},
     {contact_name, ""},
     {contact_email, ""},
     {authn_binding, post},
     {logout_binding, post},
     {authn_nameID_format, ?PERSISTENT_NAMEID_FORMAT},
     {single_logout, true},
     {username_attribute, ""},
     {base_url, node},
     {base_url_scheme, https},
     {verify_assertion_sig, true},
     {verify_assertion_envelop_sig, true},
     {verify_logout_req_sig, true},
     {verify_recipient, consumeURL},
     {chain, {<<>>, []}},
     {sign_requests, true},
     {sign_metadata, true},
     {fingerprints_usage, metadataInitialOnly},
     {trusted_fingerprints, {<<>>, []}},
     {entity_id, ""},
     {md_address_family, undefined},
     {md_tls_verify_peer, true},
     {md_tls_ca, {<<>>, []}},
     {md_tls_sni, ""},
     {md_tls_extra_opts, []},
     {md_http_timeout, 5000},
     {idp_signs_metadata, true},
     {idp_metadata_refresh_interval, 3600},
     {groups_attribute, ""},
     {groups_attribute_sep, " ,"},
     {groups_filter_re, ".*"},
     {roles_attribute, ""},
     {roles_attribute_sep, " ,"},
     {roles_filter_re, ".*"},
     {idp_metadata_origin, http},
     {session_expire, 'SessionNotOnOrAfter'},
     {dupe_check, global},
     {sp_md_cache_duration, "P1M"},
     {sp_md_url, ""},
     {sp_md_consume_url, ""},
     {sp_md_logout_url, ""},
     {clock_skew, 180},
     {verify_issuer, true},
     {sp_saml_response_max_size, ?SAML_RESPONSE_MAX_SIZE_MIN}].

type_spec(saml_metadata) ->
    #{validators => [string, fun validate_saml_metadata/2],
      formatter => fun (undefined) -> {value, <<"">>};
                       ({zip, Zip}) -> {value, zlib:unzip(Zip)}
                   end};
type_spec(fingerprint_list) ->
    #{validators => [fun validate_fingerprint_list/2],
      formatter => fun ({Str, _}) -> {value, Str} end};
type_spec(iso8601_duration) ->
    #{validators => [string, fun validate_iso8601_duration/2],
      formatter => string};
type_spec(regex) ->
    #{validators => [string, fun validator:regex/2],
      formatter => string}.

validate_iso8601_duration(Name, State) ->
    validator:validate(
      fun (Str) ->
          try iso8601:parse_duration(Str) of
              _ -> ok
          catch
              _:_ -> {error, "invalid iso8601 duration"}
          end
      end, Name, State).

validate_saml_metadata(Name, State) ->
    validator:validate(
      fun ("") -> {value, undefined};
          (Str) ->
              try cb_saml:try_parse_idp_metadata(list_to_binary(Str), false) of
                  _ -> {value, {zip, zlib:zip(Str)}}
              catch
                  error:Reason ->
                      {error, lists:flatten(cb_saml:format_error(Reason))}
              end
      end, Name, State).

validate_fingerprint_list(Name, State) ->
    validator:validate(
      fun (Raw) ->
          RawBin = iolist_to_binary(Raw),
          Tokens = string:lexemes(string:trim(RawBin), "\r\n,"),
          ParseRes = lists:map(fun parse_fingerprint/1, Tokens),
          {FPs, Errors} = misc:partitionmap(fun ({ok, R}) -> {left, R};
                                                ({error, R}) -> {right, R}
                                            end, ParseRes),
          case Errors of
              [] -> {value, {RawBin, FPs}};
              [Error | _] -> {error, Error}
          end
      end, Name, State).

%% There are multiple formats for fingerprints. It is not really clear
%% which format we should support so we try to support more or less everything.
parse_fingerprint(BinStr) when is_binary(BinStr) ->
    TrimmedStr = string:trim(BinStr),
    try esaml_util:convert_fingerprints([binary_to_list(TrimmedStr)]) of
        [{md5, _Bin}] ->
            Msg = io_lib:format("MD5 fingerprints are not supported: ~s",
                                [TrimmedStr]),
            {error, lists:flatten(Msg)};
        [{Type, Bin}] when is_atom(Type), is_binary(Bin) ->
            {ok, {Type, Bin}};
        [Bin] when is_binary(Bin) ->
            case erlang:size(Bin) * 8 of
                160 -> {ok, {sha, Bin}};
                256 -> {ok, {sha256, Bin}};
                384 -> {ok, {sha384, Bin}};
                512 -> {ok, {sha512, Bin}};
                128 ->
                    Msg = io_lib:format("MD5 fingerprints are not supported: ~s",
                                        [TrimmedStr]),
                    {error, lists:flatten(Msg)};
                _ ->
                    Msg = io_lib:format("invalid fingerprint length: ~s",
                                        [TrimmedStr]),
                    {error, lists:flatten(Msg)}
            end
    catch
        error:_ ->
            Msg = io_lib:format("invalid fingerprint: ~s", [TrimmedStr]),
            {error, lists:flatten(Msg)}
    end.

get_all_attrs(AttrName, Attrs) ->
    lists:flatmap(
      fun ([]) -> [];
          %% Trying to distinguish between list of strings
          %% and a string
          ([[_|_] | _] = L) -> L;
          ([_|_] = Str) -> [Str]
      end, proplists:get_all_values(AttrName, Attrs)).

-ifdef(TEST).

get_all_attrs_test() ->
    ?assertEqual([], get_all_attrs(a, [])),
    ?assertEqual([], get_all_attrs(a, [{n, "n"}])),
    ?assertEqual([], get_all_attrs(a, [{n, "n"}, {a, []}])),
    ?assertEqual(["a"], get_all_attrs(a, [{n, "n"}, {a, "a"}])),
    ?assertEqual(["a", "b"], get_all_attrs(a, [{n, "n"}, {a, ["a", "b"]}])),
    ?assertEqual(["a", "b", "c"],
                 get_all_attrs(a, [{a, "a"}, {n, "n"}, {a, ["b", "c"]}])),
    ?assertEqual(["a", "b", "c", "d"],
                 get_all_attrs(a, [{a, ["a", "b"]}, {n, "n"},
                                   {a, ["c", "d"]}])),
    ?assertEqual(["a", "b", "c", "d"],
                 get_all_attrs(a, [{a, ["a", "b"]}, {n, "n"}, {a, ["c", "d"]},
                                   {a, []}])).

-endif.

handle_saml_assertion(Req, {error, Msg}, _SSOOpts) ->
    redirect_to_ui_with_error(Req, Msg);
handle_saml_assertion(Req, {ok, Assertion}, SSOOpts) ->
    Subject = Assertion#esaml_assertion.subject,
    NameID = Subject#esaml_subject.name,
    Username = extract_username(Assertion, SSOOpts),
    ExtraGroups = extract_groups(Assertion, SSOOpts),
    ExtraRoles = extract_roles(Assertion, SSOOpts),
    ClockSkew = proplists:get_value(clock_skew, SSOOpts),
    case is_list(Username) andalso length(Username) > 0 of
        true when NameID =/= undefined, length(NameID) > 0 ->
            ExpDatetimeUTC =
                case proplists:get_value(session_expire, SSOOpts) of
                    false ->
                        undefined;
                    'SessionNotOnOrAfter' ->
                        Authn = Assertion#esaml_assertion.authn,
                        case proplists:get_value(session_not_on_or_after,
                                                 Authn) of
                            undefined -> undefined;
                            DT -> misc:datetime_add(DT, ClockSkew)
                        end
                end,
            AuthnRes0 = menelaus_auth:init_auth({Username, external}),
            AuthnRes =
                AuthnRes0#authn_res{type = ui,
                                    session_id = menelaus_auth:new_session_id(),
                                    extra_groups = ExtraGroups,
                                    extra_roles = ExtraRoles,
                                    expiration_datetime_utc = ExpDatetimeUTC},
            SessionName = iolist_to_binary(NameID),
            LoginRes = menelaus_auth:uilogin_phase2(
                         Req,
                         saml,
                         SessionName,
                         AuthnRes),
            case LoginRes of
                {ok, Headers} ->
                    ?log_debug("Successful saml login: ~s",
                               [ns_config_log:tag_user_name(Username)]),
                    menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                                             [{"Location", "/"} | Headers]);
                {error, {access_denied, _}} ->
                    ?log_debug("Access denied for SAML user \"~s\"",
                               [ns_config_log:tag_user_name(Username)]),
                    Msg = format_access_denied_error(Username, ExtraGroups,
                                                     ExtraRoles, SSOOpts),
                    redirect_to_ui_with_error(Req, Msg)
            end;
        true ->
            ns_audit:login_failure(Req),
            ?log_debug("NameID is not set in SAML assertion"),
            redirect_to_ui_with_error(Req, "Missing NameID in SAML assertion");
        false ->
            ?log_debug("Could not extract identity from assertion for "
                       "NameID: ~s", [ns_config_log:tag_user_name(NameID)]),
            ns_audit:login_failure(Req),
            redirect_to_ui_with_error(
              Req, "Unable to extract username from SAML assertion")
    end.

extract_username(Assertion, SSOOpts) ->
    Subject = Assertion#esaml_assertion.subject,
    case proplists:get_value(username_attribute, SSOOpts) of
        "" ->
            NameID = Subject#esaml_subject.name,
            case Subject#esaml_subject.name_format of
                "urn:oasis:names:tc:SAML:2.0:"
                "nameid-format:transient" ->
                    ?log_warning(
                      "Using transient name as an identity: ~p",
                      [ns_config_log:tag_user_name(NameID)]);
                _ ->
                    ok
            end,
            NameID;
        AttrName ->
            Attrs = Assertion#esaml_assertion.attributes,
            AttrNameMapped = esaml:common_attrib_map(AttrName),
            proplists:get_value(AttrNameMapped, Attrs)
    end.

extract_groups(Assertion, SSOOpts) ->
    case proplists:get_value(groups_attribute, SSOOpts) of
        "" -> [];
        GroupsAttr when is_list(GroupsAttr) ->
            Attrs = Assertion#esaml_assertion.attributes,
            GroupsAttrMapped = esaml:common_attrib_map(GroupsAttr),
            GroupAttrs = get_all_attrs(GroupsAttrMapped, Attrs),
            GSep = proplists:get_value(groups_attribute_sep, SSOOpts),
            Grps = lists:flatmap(string:lexemes(_, GSep), GroupAttrs),
            GroupRe = proplists:get_value(groups_filter_re, SSOOpts),
            lists:filter(
              fun (G) ->
                  match == re:run(G, GroupRe,
                                  [{capture, none}, notempty])
              end, Grps)
    end.

extract_roles(Assertion, SSOOpts) ->
    case proplists:get_value(roles_attribute, SSOOpts) of
        "" -> [];
        RolesAttr when is_list(RolesAttr) ->
            Attrs = Assertion#esaml_assertion.attributes,
            RolesAttrMapped = esaml:common_attrib_map(RolesAttr),
            RolesAttrs = get_all_attrs(RolesAttrMapped, Attrs),
            RSep = proplists:get_value(roles_attribute_sep, SSOOpts),
            Rls = lists:flatmap(string:lexemes(_, RSep), RolesAttrs),
            RoleRe = proplists:get_value(roles_filter_re, SSOOpts),
            Filtered = lists:filter(
                         fun (R) ->
                             match == re:run(R, RoleRe,
                                             [{capture, none},
                                              notempty])
                         end, Rls),

            ParsedRoles =
                lists:filtermap(
                  fun (R) ->
                      case menelaus_web_rbac:parse_roles(R) of
                          [{error, _}] ->
                              ?log_warning("Ignoring invalid role: ~s",
                                           [R]),
                              false;
                          [ParsedRole] ->
                              {true, ParsedRole};
                          [_ | _] ->
                              ?log_warning("Ignoring invalid role: ~s",
                                           [R]),
                              false
                      end
                  end, Filtered),

              {GoodRoles, BadRoles} =
                  menelaus_roles:validate_roles(ParsedRoles),
              BadRoles /= [] andalso ?log_warning("ignoring invalid roles: ~p",
                                                  [BadRoles]),
              GoodRoles
    end.

redirect_to_ui_with_error(Req, Msg) ->
    IdStr = cb_saml:store_error_msg(iolist_to_binary(Msg)),
    Location = "/ui/index.html#/?samlErrorMsgId=" ++ IdStr,
    menelaus_util:reply_text(Req, <<"Redirecting...">>, 302,
                             [{"Location", Location}]).

format_access_denied_error(Username, ExtraGroups, ExtraRoles, SSOOpts) ->
    MainError = io_lib:format("Access denied for user \"~s\": "
                              "Insufficient Permissions",
                              [Username]),
    GroupsInfo =
        case proplists:get_value(groups_attribute, SSOOpts) of
            "" -> [];
            _ when ExtraGroups == [] ->
                ["Extracted groups: <empty>"];
            _ ->
                ["Extracted groups: " ++
                     misc:intersperse(ExtraGroups, ", ")]
        end,
    RolesInfo =
        case proplists:get_value(roles_attribute, SSOOpts) of
            "" -> [];
            _ when ExtraRoles == [] ->
                ["Extracted roles: <empty>"];
            _ ->
                ExtraRolesStr =
                    [menelaus_web_rbac:role_to_string(R)
                     || R <- ExtraRoles],
                ["Extracted roles: " ++
                     misc:intersperse(ExtraRolesStr, ", ")]
        end,
    misc:intersperse([MainError] ++ GroupsInfo ++ RolesInfo, ". ").
