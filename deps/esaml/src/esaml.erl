%% esaml - SAML for erlang
%%
%% Copyright (c) 2013, Alex Wilson and the University of Queensland
%% All rights reserved.
%%
%% Distributed subject to the terms of the 2-clause BSD license, see
%% the LICENSE file in the root of the distribution.

%% @doc SAML for Erlang
-module(esaml).
-behaviour(application).
-behaviour(supervisor).

-include_lib("xmerl/include/xmerl.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("esaml.hrl").

-export([start/2, stop/1, init/1]).
-export([stale_time/1]).
-export([config/2, config/1, to_xml/1, decode_response/1, decode_assertion/1, validate_assertion/5, common_attrib_map/1]).
-export([decode_logout_request/1, decode_logout_response/1, decode_idp_metadata/1]).

-type org() :: #esaml_org{}.
-type contact() :: #esaml_contact{}.
-type sp_metadata() :: #esaml_sp_metadata{}.
-type idp_metadata() :: #esaml_idp_metadata{}.
-type authnreq() :: #esaml_authnreq{}.
-type subject() :: #esaml_subject{}.
-type assertion() :: #esaml_assertion{}.
-type logoutreq() :: #esaml_logoutreq{}.
-type logoutresp() :: #esaml_logoutresp{}.
-type response() :: #esaml_response{}.
-type sp() :: #esaml_sp{}.
-type saml_record() :: org() | contact() | sp_metadata() | idp_metadata() | authnreq() | subject() | assertion() | logoutreq() | logoutresp() | response().

-export_type([org/0, contact/0, sp_metadata/0, idp_metadata/0,
    authnreq/0, subject/0, assertion/0, logoutreq/0,
    logoutresp/0, response/0, sp/0, saml_record/0]).

-type localized_string() :: string() | [{Locale :: atom(), LocalizedString :: string()}].
-type name_format() :: email | x509 | windows | krb | persistent | transient | unknown.
-type logout_reason() :: user | admin.
-type status_code() :: success | request_error | response_error | bad_version | authn_failed | bad_attr | denied | bad_binding | unknown.
-type version() :: string().
-type datetime() :: string() | binary().
-type condition() :: {not_before, esaml:datetime()} | {not_on_or_after, esaml:datetime()} | {audience, string()}.
-type conditions() :: [condition()].
-export_type([localized_string/0, name_format/0, logout_reason/0, status_code/0, version/0, datetime/0, conditions/0]).

%% @private
start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private
stop(_State) ->
    ok.

%% @private
init([]) ->
    DupeEts = {esaml_ets_table_owner,
        {esaml_util, start_ets, []},
        permanent, 5000, worker, [esaml]},
    {ok,
        {{one_for_one, 60, 600},
        [DupeEts]}}.

%% @doc Retrieve a config record
-spec config(Name :: atom()) -> term() | undefined.
config(N) -> config(N, undefined).
%% @doc Retrieve a config record with default
-spec config(Name :: atom(), Default :: term()) -> term().
config(N, D) ->
    case application:get_env(esaml, N) of
        {ok, V} -> V;
        _ -> D
    end.

-spec nameid_map(string()) -> name_format().
nameid_map("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress") -> email;
nameid_map("urn:oasis:names:tc:SAML:1.1:nameid-format:X509SubjectName") -> x509;
nameid_map("urn:oasis:names:tc:SAML:1.1:nameid-format:WindowsDomainQualifiedName") -> windows;
nameid_map("urn:oasis:names:tc:SAML:2.0:nameid-format:kerberos") -> krb;
nameid_map("urn:oasis:names:tc:SAML:2.0:nameid-format:persistent") -> persistent;
nameid_map("urn:oasis:names:tc:SAML:2.0:nameid-format:transient") -> transient;
nameid_map(S) when is_list(S) -> unknown.

-spec nameid_name_qualifier_map(string()) -> undefined | string().
nameid_name_qualifier_map("") -> undefined;
nameid_name_qualifier_map(S) when is_list(S) -> S.

-spec nameid_sp_name_qualifier_map(string()) -> undefined | string().
nameid_sp_name_qualifier_map("") -> undefined;
nameid_sp_name_qualifier_map(S) when is_list(S) -> S.

-spec nameid_format_map(string()) -> undefined | string().
nameid_format_map("") -> undefined;
nameid_format_map(S) when is_list(S) -> S.

-spec subject_method_map(string()) -> bearer | unknown.
subject_method_map("urn:oasis:names:tc:SAML:2.0:cm:bearer") -> bearer;
subject_method_map(_) -> unknown.

-spec status_code_map(string()) -> status_code() | atom().
status_code_map("urn:oasis:names:tc:SAML:2.0:status:Success") -> success;
status_code_map("urn:oasis:names:tc:SAML:2.0:status:VersionMismatch") -> bad_version;
status_code_map("urn:oasis:names:tc:SAML:2.0:status:AuthnFailed") -> authn_failed;
status_code_map("urn:oasis:names:tc:SAML:2.0:status:InvalidAttrNameOrValue") -> bad_attr;
status_code_map("urn:oasis:names:tc:SAML:2.0:status:RequestDenied") -> denied;
status_code_map("urn:oasis:names:tc:SAML:2.0:status:UnsupportedBinding") -> bad_binding;
status_code_map(Urn = "urn:" ++ _) -> list_to_atom(lists:last(string:tokens(Urn, ":")));
status_code_map(S) when is_list(S) -> unknown.

-spec rev_status_code_map(status_code()) -> string().
rev_status_code_map(success) -> "urn:oasis:names:tc:SAML:2.0:status:Success";
rev_status_code_map(bad_version) -> "urn:oasis:names:tc:SAML:2.0:status:VersionMismatch";
rev_status_code_map(authn_failed) -> "urn:oasis:names:tc:SAML:2.0:status:AuthnFailed";
rev_status_code_map(bad_attr) -> "urn:oasis:names:tc:SAML:2.0:status:InvalidAttrNameOrValue";
rev_status_code_map(denied) -> "urn:oasis:names:tc:SAML:2.0:status:RequestDenied";
rev_status_code_map(bad_binding) -> "urn:oasis:names:tc:SAML:2.0:status:UnsupportedBinding";
rev_status_code_map(_) -> error(bad_status_code).

-spec logout_reason_map(string()) -> logout_reason().
logout_reason_map("urn:oasis:names:tc:SAML:2.0:logout:user") -> user;
logout_reason_map("urn:oasis:names:tc:SAML:2.0:logout:admin") -> admin;
logout_reason_map(S) when is_list(S) -> unknown.

-spec rev_logout_reason_map(logout_reason()) -> string().
rev_logout_reason_map(user) -> "urn:oasis:names:tc:SAML:2.0:logout:user";
rev_logout_reason_map(admin) -> "urn:oasis:names:tc:SAML:2.0:logout:admin".

-spec common_attrib_map(string()) -> atom().
common_attrib_map("urn:oid:2.16.840.1.113730.3.1.3") -> employeeNumber;
common_attrib_map("urn:oid:1.3.6.1.4.1.5923.1.1.1.6") -> eduPersonPrincipalName;
common_attrib_map("urn:oid:0.9.2342.19200300.100.1.3") -> mail;
common_attrib_map("urn:oid:2.5.4.42") -> givenName;
common_attrib_map("urn:oid:2.16.840.1.113730.3.1.241") -> displayName;
common_attrib_map("urn:oid:2.5.4.3") -> commonName;
common_attrib_map("urn:oid:2.5.4.20") -> telephoneNumber;
common_attrib_map("urn:oid:2.5.4.10") -> organizationName;
common_attrib_map("urn:oid:2.5.4.11") -> organizationalUnitName;
common_attrib_map("urn:oid:1.3.6.1.4.1.5923.1.1.1.9") -> eduPersonScopedAffiliation;
common_attrib_map("urn:oid:2.16.840.1.113730.3.1.4") -> employeeType;
common_attrib_map("urn:oid:0.9.2342.19200300.100.1.1") -> uid;
common_attrib_map("urn:oid:2.5.4.4") -> surName;
common_attrib_map(Uri = "http://" ++ _) -> list_to_atom(lists:last(string:tokens(Uri, "/")));
common_attrib_map(Other) when is_list(Other) -> list_to_atom(Other).

-include("xmerl_xpath_macros.hrl").

%% @private
-spec decode_idp_metadata(Xml :: #xmlElement{}) -> {ok, #esaml_idp_metadata{}} | {error, term()}.
decode_idp_metadata(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'},
          {"md", 'urn:oasis:names:tc:SAML:2.0:metadata'},
          {"ds", 'http://www.w3.org/2000/09/xmldsig#'}],
    esaml_util:threaduntil([
        ?xpath_attr_required("/md:EntityDescriptor/@entityID", esaml_idp_metadata, entity_id, bad_entity),
        ?xpath_attr("/md:EntityDescriptor/@validUntil", esaml_idp_metadata, valid_until),
        fun (Meta = #esaml_idp_metadata{valid_until = undefined}) -> Meta;
            (Meta = #esaml_idp_metadata{valid_until = DTStr}) ->
                try Meta#esaml_idp_metadata{valid_until = iso8601:parse(DTStr)}
                catch
                    _:_ -> {error, {invalid_valid_until, DTStr}}
                end
        end,
        ?xpath_attr("/md:EntityDescriptor/@cacheDuration", esaml_idp_metadata, cache_duration),
        fun (Meta = #esaml_idp_metadata{cache_duration = undefined}) -> Meta;
            (Meta = #esaml_idp_metadata{cache_duration = DurStr}) ->
                try Meta#esaml_idp_metadata{cache_duration = iso8601:parse_duration(DurStr)}
                catch
                    _:_ -> {error, {invalid_cache_duration, DurStr}}
                end
        end,
        ?xpath_attr("/md:EntityDescriptor/md:IDPSSODescriptor/md:SingleSignOnService[@Binding='urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST']/@Location",
            esaml_idp_metadata, login_post_location),
        ?xpath_attr("/md:EntityDescriptor/md:IDPSSODescriptor/md:SingleSignOnService[@Binding='urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect']/@Location",
            esaml_idp_metadata, login_redirect_location),
        ?xpath_attr("/md:EntityDescriptor/md:IDPSSODescriptor/md:SingleLogoutService[@Binding='urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST']/@Location",
            esaml_idp_metadata, logout_post_location),
        ?xpath_attr("/md:EntityDescriptor/md:IDPSSODescriptor/md:SingleLogoutService[@Binding='urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect']/@Location",
            esaml_idp_metadata, logout_redirect_location),
        ?xpath_text("/md:EntityDescriptor/md:IDPSSODescriptor/md:NameIDFormat/text()",
            esaml_idp_metadata, name_format, fun nameid_map/1),
        ?xpath_text_many("/md:EntityDescriptor/md:IDPSSODescriptor/md:KeyDescriptor[@use='signing']/ds:KeyInfo/ds:X509Data/ds:X509Certificate/text()", esaml_idp_metadata, certificates, fun(X) -> base64:decode(list_to_binary(X)) end),
        ?xpath_recurse("/md:EntityDescriptor/md:ContactPerson[@contactType='technical']", esaml_idp_metadata, tech, decode_contact),
        ?xpath_recurse("/md:EntityDescriptor/md:Organization", esaml_idp_metadata, org, decode_org)
    ], #esaml_idp_metadata{}).

%% @private
-spec decode_org(Xml :: #xmlElement{}) -> {ok, #esaml_org{}} | {error, term()}.
decode_org(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'},
          {"md", 'urn:oasis:names:tc:SAML:2.0:metadata'}],
    esaml_util:threaduntil([
        ?xpath_text_required("/md:Organization/md:OrganizationName/text()", esaml_org, name, bad_org),
        ?xpath_text("/md:Organization/md:OrganizationDisplayName/text()", esaml_org, displayname),
        ?xpath_text("/md:Organization/md:OrganizationURL/text()", esaml_org, url)
    ], #esaml_org{}).

%% @private
-spec decode_contact(Xml :: #xmlElement{}) -> {ok, #esaml_contact{}} | {error, term()}.
decode_contact(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'},
          {"md", 'urn:oasis:names:tc:SAML:2.0:metadata'}],
    esaml_util:threaduntil([
        ?xpath_text_required("/md:ContactPerson/md:EmailAddress/text()", esaml_contact, email, bad_contact),
        ?xpath_text("/md:ContactPerson/md:GivenName/text()", esaml_contact, name),
        ?xpath_text_append("/md:ContactPerson/md:SurName/text()", esaml_contact, name, " ")
    ], #esaml_contact{}).

%% @private
-spec decode_logout_request(Xml :: #xmlElement{}) -> {ok, #esaml_logoutreq{}} | {error, term()}.
decode_logout_request(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        ?xpath_attr_required("/samlp:LogoutRequest/@Version", esaml_logoutreq, version, bad_version),
        ?xpath_attr_required("/samlp:LogoutRequest/@IssueInstant", esaml_logoutreq, issue_instant, bad_response),
        ?xpath_text_required("/samlp:LogoutRequest/saml:NameID/text()", esaml_logoutreq, name, bad_name),
        ?xpath_attr("/samlp:LogoutRequest/saml:NameID/@SPNameQualifier", esaml_logoutreq, sp_name_qualifier, fun nameid_sp_name_qualifier_map/1),
        ?xpath_attr("/samlp:LogoutRequest/saml:NameID/@Format", esaml_logoutreq, name_format, fun nameid_format_map/1),
        ?xpath_attr("/samlp:LogoutRequest/@Destination", esaml_logoutreq, destination),
        ?xpath_attr("/samlp:LogoutRequest/@Reason", esaml_logoutreq, reason, fun logout_reason_map/1),
        ?xpath_text("/samlp:LogoutRequest/saml:Issuer/text()", esaml_logoutreq, issuer)
    ], #esaml_logoutreq{}).

%% @private
-spec decode_logout_response(Xml :: #xmlElement{}) -> {ok, #esaml_logoutresp{}} | {error, term()}.
decode_logout_response(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        ?xpath_attr_required("/samlp:LogoutResponse/@Version", esaml_logoutresp, version, bad_version),
        ?xpath_attr_required("/samlp:LogoutResponse/@IssueInstant", esaml_logoutresp, issue_instant, bad_response),
        ?xpath_attr_required("/samlp:LogoutResponse/samlp:Status/samlp:StatusCode/@Value", esaml_logoutresp, status, fun status_code_map/1, bad_response),
        ?xpath_attr("/samlp:LogoutResponse/samlp:Status/samlp:StatusCode/samlp:StatusCode/@Value", esaml_logoutresp, status_second_level, fun status_code_map/1),
        ?xpath_attr("/samlp:LogoutResponse/@Destination", esaml_logoutresp, destination),
        ?xpath_text("/samlp:LogoutResponse/saml:Issuer/text()", esaml_logoutresp, issuer)
    ], #esaml_logoutresp{}).

%% @private
-spec decode_response(Xml :: #xmlElement{}) -> {ok, #esaml_response{}} | {error, term()}.
decode_response(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        ?xpath_attr_required("/samlp:Response/@Version", esaml_response, version, bad_version),
        ?xpath_attr_required("/samlp:Response/@IssueInstant", esaml_response, issue_instant, bad_response),
        ?xpath_attr("/samlp:Response/@Destination", esaml_response, destination),
        ?xpath_text("/samlp:Response/saml:Issuer/text()", esaml_response, issuer),
        ?xpath_attr("/samlp:Response/samlp:Status/samlp:StatusCode/@Value", esaml_response, status, fun status_code_map/1),
        ?xpath_recurse("/samlp:Response/saml:Assertion", esaml_response, assertion, decode_assertion)
    ], #esaml_response{}).

%% @private
-spec decode_assertion(Xml :: #xmlElement{}) -> {ok, #esaml_assertion{}} | {error, term()}.
decode_assertion(Xml) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        ?xpath_attr_required("/saml:Assertion/@Version", esaml_assertion, version, bad_version),
        ?xpath_attr_required("/saml:Assertion/@IssueInstant", esaml_assertion, issue_instant, bad_assertion),
        ?xpath_attr("/saml:Assertion/@ID", esaml_assertion, id),
        ?xpath_attr("/saml:Assertion/saml:Subject/saml:SubjectConfirmation/saml:SubjectConfirmationData/@Recipient", esaml_assertion, recipient),
        ?xpath_text("/saml:Assertion/saml:Issuer/text()", esaml_assertion, issuer),
        ?xpath_recurse("/saml:Assertion/saml:Subject", esaml_assertion, subject, decode_assertion_subject),
        ?xpath_recurse("/saml:Assertion/saml:Conditions", esaml_assertion, conditions, decode_assertion_conditions),
        ?xpath_recurse("/saml:Assertion/saml:AttributeStatement", esaml_assertion, attributes, decode_assertion_attributes),
        ?xpath_recurse("/saml:Assertion/saml:AuthnStatement", esaml_assertion, authn, decode_assertion_authn)
    ], #esaml_assertion{}).

-spec decode_assertion_subject(#xmlElement{}) -> {ok, #esaml_subject{}} | {error, term()}.
decode_assertion_subject(Xml) ->
    Ns = [{"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        ?xpath_text("/saml:Subject/saml:NameID/text()", esaml_subject, name),
        ?xpath_attr("/saml:Subject/saml:NameID/@NameQualifier", esaml_subject, name_qualifier, fun nameid_name_qualifier_map/1),
        ?xpath_attr("/saml:Subject/saml:NameID/@SPNameQualifier", esaml_subject, sp_name_qualifier, fun nameid_sp_name_qualifier_map/1),
        ?xpath_attr("/saml:Subject/saml:NameID/@Format", esaml_subject, name_format, fun nameid_format_map/1),
        ?xpath_attr("/saml:Subject/saml:SubjectConfirmation/@Method", esaml_subject, confirmation_method, fun subject_method_map/1),
        ?xpath_attr("/saml:Subject/saml:SubjectConfirmation/saml:SubjectConfirmationData/@NotOnOrAfter", esaml_subject, notonorafter),
        ?xpath_attr("/saml:Subject/saml:SubjectConfirmation/saml:SubjectConfirmationData/@InResponseTo", esaml_subject, in_response_to)
    ], #esaml_subject{}).

-spec decode_assertion_conditions(#xmlElement{}) -> {ok, conditions()} | {error, term()}.
decode_assertion_conditions(Xml) ->
    Ns = [{"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        fun(C) ->
            case xmerl_xpath:string("/saml:Conditions/@NotBefore", Xml, [{namespace, Ns}]) of
                [#xmlAttribute{value = V}] -> [{not_before, V} | C]; _ -> C
            end
        end,
        fun(C) ->
            case xmerl_xpath:string("/saml:Conditions/@NotOnOrAfter", Xml, [{namespace, Ns}]) of
                [#xmlAttribute{value = V}] -> [{not_on_or_after, V} | C]; _ -> C
            end
        end,
        fun(C) ->
            case xmerl_xpath:string("/saml:Conditions/saml:AudienceRestriction/saml:Audience/text()", Xml, [{namespace, Ns}]) of
                [#xmlText{value = V}] -> [{audience, V} | C]; _ -> C
            end
        end
    ], []).

-spec decode_assertion_attributes(#xmlElement{}) -> {ok, [{atom(), string()}]} | {error, term()}.
decode_assertion_attributes(Xml) ->
    Ns = [{"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    Attrs = xmerl_xpath:string("/saml:AttributeStatement/saml:Attribute", Xml, [{namespace, Ns}]),
    {ok, lists:foldl(fun(AttrElem, In) ->
        case [X#xmlAttribute.value || X <- AttrElem#xmlElement.attributes, X#xmlAttribute.name =:= 'Name'] of
            [Name] ->
                case xmerl_xpath:string("saml:AttributeValue/text()", AttrElem, [{namespace, Ns}]) of
                    [#xmlText{value = Value}] ->
                        [{common_attrib_map(Name), Value} | In];
                    List ->
                        if (length(List) > 0) ->
                            Value = [X#xmlText.value || X <- List, element(1, X) =:= xmlText],
                            [{common_attrib_map(Name), Value} | In];
                        true ->
                            In
                        end
                end;
            _ -> In
        end
    end, [], Attrs)}.

-spec decode_assertion_authn(#xmlElement{}) -> {ok, conditions()} | {error, term()}.
decode_assertion_authn(Xml) ->
    Ns = [{"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        fun(C) ->
            case xmerl_xpath:string("/saml:AuthnStatement/@AuthnInstant", Xml, [{namespace, Ns}]) of
                [#xmlAttribute{value = V}] -> [{authn_instant, V} | C]; _ -> C
            end
        end,
        fun(C) ->
            case xmerl_xpath:string("/saml:AuthnStatement/@SessionNotOnOrAfter", Xml, [{namespace, Ns}]) of
                [#xmlAttribute{value = V}] ->
                    try [{session_not_on_or_after, iso8601:parse(V)} | C]
                    catch
                        _:_ -> {error, {invalid_session_not_on_or_after, V}}
                    end;
                _ -> C
            end
        end,
        fun(C) ->
            case xmerl_xpath:string("/saml:AuthnStatement/@SessionIndex", Xml, [{namespace, Ns}]) of
                [#xmlAttribute{value = V}] -> [{session_index, V} | C]; _ -> C
            end
        end,
        fun(C) ->
            case xmerl_xpath:string("/saml:AuthnStatement/saml:AuthnContext/saml:AuthnContextClassRef/text()", Xml, [{namespace, Ns}]) of
                [#xmlText{value = V}] -> [{authn_context, V} | C]; _ -> C
            end
        end
    ], []).

%% @doc Returns the time at which an assertion is considered stale.
%% @private
-spec stale_time(#esaml_assertion{}) -> integer().
stale_time(A) ->
    esaml_util:thread([
        fun(T) ->
            case A#esaml_assertion.subject of
                #esaml_subject{notonorafter = ""} -> T;
                #esaml_subject{notonorafter = Restrict} ->
                    Secs = calendar:datetime_to_gregorian_seconds(
                        esaml_util:saml_to_datetime(Restrict)),
                    if (Secs < T) -> Secs; true -> T end
            end
        end,
        fun(T) ->
            Conds = A#esaml_assertion.conditions,
            case proplists:get_value(not_on_or_after, Conds) of
                undefined -> T;
                Restrict ->
                    Secs = calendar:datetime_to_gregorian_seconds(
                        esaml_util:saml_to_datetime(Restrict)),
                    if (Secs < T) -> Secs; true -> T end
            end
        end,
        fun(T) ->
            if (T =:= none) ->
                II = A#esaml_assertion.issue_instant,
                IISecs = calendar:datetime_to_gregorian_seconds(
                    esaml_util:saml_to_datetime(II)),
                IISecs + 5*60;
            true ->
                T
            end
        end
    ], none).

not_before_time(A) ->
    Conds = A#esaml_assertion.conditions,
    case proplists:get_value(not_before, Conds) of
        undefined ->
            IssueIsoTime = A#esaml_assertion.issue_instant,
            Secs = calendar:datetime_to_gregorian_seconds(
                     esaml_util:saml_to_datetime(IssueIsoTime)),
            Secs;
        IsoTime ->
            calendar:datetime_to_gregorian_seconds(
              esaml_util:saml_to_datetime(IsoTime))
    end.

check_stale(A, ClockSkewSecs) ->
    Now = erlang:localtime_to_universaltime(erlang:localtime()),
    NowSecs = calendar:datetime_to_gregorian_seconds(Now),
    Stale = stale_time(A),
    NotBefore = not_before_time(A),
    if
        NowSecs > Stale + ClockSkewSecs ->
            {error, stale_assertion};
        NowSecs < NotBefore - ClockSkewSecs ->
            {error, not_before};
        true ->
            A
    end.

%% @doc Parse and validate an assertion, returning it as a record
%% @private
-spec validate_assertion(AssertionXml :: #xmlElement{},
                         Recipient :: string() | any,
                         Audience :: string(),
                         Issuer :: string() | any,
                         ClockSkew :: integer()) ->
        {ok, #esaml_assertion{}} | {error, Reason :: term()}.
validate_assertion(AssertionXml, Recipient, Audience, Issuer, ClockSkew) ->
    case decode_assertion(AssertionXml) of
        {error, Reason} ->
            {error, Reason};
        {ok, Assertion} ->
            esaml_util:threaduntil([
                fun(A) -> case A of
                    #esaml_assertion{version = "2.0"} -> A;
                    _ -> {error, bad_version}
                end end,
                fun(A) -> case A of
                    #esaml_assertion{recipient = Recipient} -> A;
                    _ when Recipient == any -> A;
                    _ -> {error, bad_recipient}
                end end,
                fun(A) ->
                    case A of
                        #esaml_assertion{issuer = Issuer} -> A;
                        #esaml_assertion{} when Issuer == any -> A;
                        #esaml_assertion{issuer = BadIssuer} ->
                            {error, {bad_issuer, BadIssuer}}
                    end
                end,
                fun(A) -> case A of
                    #esaml_assertion{conditions = Conds} ->
                        case proplists:get_value(audience, Conds) of
                            undefined -> A;
                            Audience -> A;
                            _ -> {error, bad_audience}
                        end;
                    _ -> A
                end end,
                fun (A) -> check_stale(A, ClockSkew) end
            ], Assertion)
    end.

%% @doc Produce cloned elements with xml:lang set to represent
%%      multi-locale strings.
%% @private
-spec lang_elems(#xmlElement{}, localized_string()) -> [#xmlElement{}].
lang_elems(BaseTag, Vals = [{Lang, _} | _]) when is_atom(Lang) ->
    [BaseTag#xmlElement{
        attributes = BaseTag#xmlElement.attributes ++
            [#xmlAttribute{name = 'xml:lang', value = atom_to_list(L)}],
        content = BaseTag#xmlElement.content ++
            [#xmlText{value = V}]} || {L,V} <- Vals];
lang_elems(BaseTag, Val) ->
    [BaseTag#xmlElement{
        attributes = BaseTag#xmlElement.attributes ++
            [#xmlAttribute{name = 'xml:lang', value = "en"}],
        content = BaseTag#xmlElement.content ++
            [#xmlText{value = Val}]}].

%% @doc Convert a SAML request/metadata record into XML
%% @private
-spec to_xml(saml_record()) -> #xmlElement{}.
to_xml(#esaml_authnreq{version = V, issue_instant = Time, destination = Dest,
        issuer = Issuer, name_format = Format, consumer_location = Consumer}) ->
    Ns = #xmlNamespace{nodes = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
                                {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}]},

    esaml_util:build_nsinfo(Ns, #xmlElement{name = 'samlp:AuthnRequest',
        attributes = [#xmlAttribute{name = 'xmlns:samlp', value = proplists:get_value("samlp", Ns#xmlNamespace.nodes)},
                      #xmlAttribute{name = 'xmlns:saml', value = proplists:get_value("saml", Ns#xmlNamespace.nodes)},
                      #xmlAttribute{name = 'IssueInstant', value = Time},
                      #xmlAttribute{name = 'Version', value = V},
                      #xmlAttribute{name = 'Destination', value = Dest},
                      #xmlAttribute{name = 'AssertionConsumerServiceURL', value = Consumer},
                      #xmlAttribute{name = 'ProtocolBinding', value = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"}],
        content = [#xmlElement{name = 'saml:Issuer', content = [#xmlText{value = Issuer}]}] ++
              case is_list(Format) of
                true ->
                    [#xmlElement{name = 'samlp:NameIDPolicy',
                        attributes = [#xmlAttribute{name = 'Format', value = Format}]}];
                false ->
                    []
              end
    });

to_xml(#esaml_logoutreq{version = V, issue_instant = Time, destination = Dest, issuer = Issuer,
                        name = NameID, name_qualifier = NameQualifier,
                        sp_name_qualifier = SPNameQualifier, name_format = NameFormat,
                        session_index = SessionIndex, reason = Reason}) ->
    Ns = #xmlNamespace{nodes = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
                                {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}]},
    NameIDAttrs =
        case is_list(NameQualifier) of
            true -> [#xmlAttribute{name = 'NameQualifier', value = NameQualifier}];
            false -> []
        end ++
        case is_list(SPNameQualifier) of
            true -> [#xmlAttribute{name = 'SPNameQualifier', value = SPNameQualifier}];
            false -> []
        end ++
        case is_list(NameFormat) of
            true -> [#xmlAttribute{name = 'Format', value = NameFormat}];
            false -> []
        end,
    esaml_util:build_nsinfo(Ns, #xmlElement{name = 'samlp:LogoutRequest',
        attributes = [#xmlAttribute{name = 'xmlns:samlp', value = proplists:get_value("samlp", Ns#xmlNamespace.nodes)},
                      #xmlAttribute{name = 'xmlns:saml', value = proplists:get_value("saml", Ns#xmlNamespace.nodes)},
                      #xmlAttribute{name = 'IssueInstant', value = Time},
                      #xmlAttribute{name = 'Version', value = V},
                      #xmlAttribute{name = 'Destination', value = Dest},
                      #xmlAttribute{name = 'Reason', value = rev_logout_reason_map(Reason)}],
        content = [
            #xmlElement{name = 'saml:Issuer', content = [#xmlText{value = Issuer}]},
            #xmlElement{name = 'saml:NameID',
                attributes = NameIDAttrs,
                content = [#xmlText{value = NameID}]}
        ] ++ case is_list(SessionIndex) andalso length(SessionIndex) > 0 of
                 true ->
                    [#xmlElement{name = 'samlp:SessionIndex',
                                 content = [#xmlText{value = SessionIndex}]}];
                 false ->
                    []
             end
    });

to_xml(#esaml_logoutresp{version = V, issue_instant  = Time,
    destination = Dest, issuer = Issuer, status = Status}) ->
    Ns = #xmlNamespace{nodes = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
                                {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}]},
    esaml_util:build_nsinfo(Ns, #xmlElement{name = 'samlp:LogoutResponse',
        attributes = [#xmlAttribute{name = 'xmlns:samlp', value = proplists:get_value("samlp", Ns#xmlNamespace.nodes)},
                      #xmlAttribute{name = 'xmlns:saml', value = proplists:get_value("saml", Ns#xmlNamespace.nodes)},
                      #xmlAttribute{name = 'IssueInstant', value = Time},
                      #xmlAttribute{name = 'Version', value = V},
                      #xmlAttribute{name = 'Destination', value = Dest}],
        content = [
            #xmlElement{name = 'saml:Issuer', content = [#xmlText{value = Issuer}]},
            #xmlElement{name = 'samlp:Status', content = [
                    #xmlElement{name = 'samlp:StatusCode',
                                attributes =
                                    [#xmlAttribute{
                                        name = 'Value',
                                        value = rev_status_code_map(Status)}]}]}
        ]
    });

  to_xml(#esaml_sp_metadata{org = #esaml_org{name = OrgName, displayname = OrgDisplayName,
                                             url = OrgUrl },
                         tech = #esaml_contact{name = TechName, email = TechEmail},
                         signed_requests = SignReq, signed_assertions = SignAss,
                         certificate = CertBin, cert_chain = CertChain, entity_id = EntityID,
                         consumer_location = ConsumerLoc,
                         logout_location = SLOLoc,
                         cache_duration = CacheDuration}) ->

      Ns = #xmlNamespace{nodes = [{"md", 'urn:oasis:names:tc:SAML:2.0:metadata'},
                                  {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'},
                                  {"dsig", 'http://www.w3.org/2000/09/xmldsig#'}]},

      KeyDescriptorElems = case CertBin of
          undefined -> [];
          C when is_binary(C) -> [
              #xmlElement{name = 'md:KeyDescriptor',
                  attributes = [#xmlAttribute{name = 'use', value = "signing"}],
                  content = [#xmlElement{name = 'dsig:KeyInfo',
                      content = [#xmlElement{name = 'dsig:X509Data',
                          content =
                                  [#xmlElement{name = 'dsig:X509Certificate',
                              content = [#xmlText{value = base64:encode_to_string(CertBin)}]} |
                                  [#xmlElement{name = 'dsig:X509Certificate',
                              content = [#xmlText{value = base64:encode_to_string(CertChainBin)}]} || CertChainBin <- CertChain]]}]}]},

              #xmlElement{name = 'md:KeyDescriptor',
                  attributes = [#xmlAttribute{name = 'use', value = "encryption"}],
                  content = [#xmlElement{name = 'dsig:KeyInfo',
                      content = [#xmlElement{name = 'dsig:X509Data',
                          content =
                                  [#xmlElement{name = 'dsig:X509Certificate',
                              content = [#xmlText{value = base64:encode_to_string(CertBin)}]} |
                                  [#xmlElement{name = 'dsig:X509Certificate',
                              content = [#xmlText{value = base64:encode_to_string(CertChainBin)}]} || CertChainBin <- CertChain]]}]}]}

          ]
      end,

      SingleLogoutServiceElems = case SLOLoc of
          undefined -> [];
          _ -> [
              #xmlElement{name = 'md:SingleLogoutService',
                  attributes = [#xmlAttribute{name = 'Binding', value = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"},
                                #xmlAttribute{name = 'Location', value = SLOLoc}]},
              #xmlElement{name = 'md:SingleLogoutService',
                  attributes = [#xmlAttribute{name = 'Binding', value = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"},
                                #xmlAttribute{name = 'Location', value = SLOLoc}]}
          ]
      end,

      AssertionConsumerServiceElems = [
          #xmlElement{name = 'md:AssertionConsumerService',
              attributes = [#xmlAttribute{name = 'isDefault', value = "true"},
                            #xmlAttribute{name = 'index', value = "0"},
                            #xmlAttribute{name = 'Binding', value = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"},
                            #xmlAttribute{name = 'Location', value = ConsumerLoc}]},
          #xmlElement{name = 'md:AssertionConsumerService',
              attributes = [#xmlAttribute{name = 'index', value = "1"},
                            #xmlAttribute{name = 'Binding', value = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"},
                            #xmlAttribute{name = 'Location', value = ConsumerLoc}]}
      ],

      OrganizationElem = #xmlElement{name = 'md:Organization',
          content =
              lang_elems(#xmlElement{name = 'md:OrganizationName'}, OrgName) ++
              lang_elems(#xmlElement{name = 'md:OrganizationDisplayName'}, OrgDisplayName) ++
              lang_elems(#xmlElement{name = 'md:OrganizationURL'}, OrgUrl)
      },

      ContactElem = #xmlElement{name = 'md:ContactPerson',
          attributes = [#xmlAttribute{name = 'contactType', value = "technical"}],
          content = [
              #xmlElement{name = 'md:SurName', content = [#xmlText{value = TechName}]},
              #xmlElement{name = 'md:EmailAddress', content = [#xmlText{value = TechEmail}]}
          ]
      },

      SPSSODescriptorElem = #xmlElement{name = 'md:SPSSODescriptor',
          attributes = [#xmlAttribute{name = 'protocolSupportEnumeration', value = "urn:oasis:names:tc:SAML:2.0:protocol"},
                        #xmlAttribute{name = 'AuthnRequestsSigned', value = atom_to_list(SignReq)},
                        #xmlAttribute{name = 'WantAssertionsSigned', value = atom_to_list(SignAss)}],
          content = KeyDescriptorElems ++ SingleLogoutServiceElems ++ AssertionConsumerServiceElems
      },

      esaml_util:build_nsinfo(Ns, #xmlElement{
          name = 'md:EntityDescriptor',
          attributes = [
              #xmlAttribute{name = 'xmlns:md', value = atom_to_list(proplists:get_value("md", Ns#xmlNamespace.nodes))},
              #xmlAttribute{name = 'xmlns:saml', value = atom_to_list(proplists:get_value("saml", Ns#xmlNamespace.nodes))},
              #xmlAttribute{name = 'xmlns:dsig', value = atom_to_list(proplists:get_value("dsig", Ns#xmlNamespace.nodes))},
              #xmlAttribute{name = 'entityID', value = EntityID},
              #xmlAttribute{name = 'cacheDuration', value = CacheDuration}
          ], content = [
              SPSSODescriptorElem,
              OrganizationElem,
              ContactElem
          ]
      });

to_xml(_) -> error("unknown record").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

decode_response_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\" Destination=\"foo\"></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    ?assertMatch({ok, #esaml_response{issue_instant = "2013-01-01T01:01:01Z", destination = "foo", status = unknown}}, Resp).

decode_response_no_version_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" IssueInstant=\"2013-01-01T01:01:01Z\" Destination=\"foo\"></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {error, bad_version} = Resp.

decode_response_no_issue_instant_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" Destination=\"foo\"></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {error, bad_response} = Resp.

decode_response_destination_optional_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {ok, #esaml_response{issue_instant = "2013-01-01T01:01:01Z", status = unknown}} = Resp.

decode_response_status_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {ok, #esaml_response{issue_instant = "2013-01-01T01:01:01Z", status = success, issuer = "foo"}} = Resp.

decode_response_bad_assertion_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status><saml:Assertion></saml:Assertion></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {error, bad_version} = Resp.

decode_assertion_no_recipient_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status><saml:Assertion Version=\"2.0\" IssueInstant=\"test\"><saml:Issuer>foo</saml:Issuer><saml:Subject><saml:NameID>foobar</saml:NameID><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\" /></saml:Subject></saml:Assertion></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {error, bad_recipient} = Resp.

decode_assertion_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status><saml:Assertion Version=\"2.0\" IssueInstant=\"test\"><saml:Issuer>foo</saml:Issuer><saml:Subject><saml:NameID>foobar</saml:NameID><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"><saml:SubjectConfirmationData Recipient=\"foobar123\" /></saml:SubjectConfirmation></saml:Subject></saml:Assertion></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {ok, #esaml_response{issue_instant = "2013-01-01T01:01:01Z", issuer = "foo", status = success, assertion = #esaml_assertion{issue_instant = "test", issuer = "foo", recipient = "foobar123", subject = #esaml_subject{name = "foobar", confirmation_method = bearer}}}} = Resp.

decode_conditions_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status><saml:Assertion Version=\"2.0\" IssueInstant=\"test\"><saml:Issuer>foo</saml:Issuer><saml:Subject><saml:NameID>foobar</saml:NameID><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"><saml:SubjectConfirmationData Recipient=\"foobar123\" /></saml:SubjectConfirmation></saml:Subject><saml:Conditions NotBefore=\"before\" NotOnOrAfter=\"notafter\"><saml:AudienceRestriction><saml:Audience>foobaraudience</saml:Audience></saml:AudienceRestriction></saml:Conditions></saml:Assertion></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {ok, #esaml_response{assertion = #esaml_assertion{conditions = Conds}}} = Resp,
    [{audience, "foobaraudience"}, {not_before, "before"}, {not_on_or_after, "notafter"}] = lists:sort(Conds).

decode_attributes_test() ->
    {Doc, _} = xmerl_scan:string("<saml:Assertion xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"test\"><saml:Subject><saml:NameID>foobar</saml:NameID><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"><saml:SubjectConfirmationData Recipient=\"foobar123\" /></saml:SubjectConfirmation></saml:Subject><saml:AttributeStatement><saml:Attribute Name=\"urn:oid:0.9.2342.19200300.100.1.3\"><saml:AttributeValue>test@test.com</saml:AttributeValue></saml:Attribute><saml:Attribute Name=\"foo\"><saml:AttributeValue>george</saml:AttributeValue><saml:AttributeValue>bar</saml:AttributeValue></saml:Attribute><saml:Attribute Name=\"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress\"><saml:AttributeValue>test@test.com</saml:AttributeValue></saml:Attribute></saml:AttributeStatement></saml:Assertion>", [{namespace_conformant, true}]),
    Assertion = decode_assertion(Doc),
    {ok, #esaml_assertion{attributes = Attrs}} = Assertion,
    [{emailaddress, "test@test.com"}, {foo, ["george", "bar"]}, {mail, "test@test.com"}] = lists:sort(Attrs).

decode_solicited_in_response_to_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status><saml:Assertion Version=\"2.0\" IssueInstant=\"test\"><saml:Issuer>foo</saml:Issuer><saml:Subject><saml:NameID>foobar</saml:NameID><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"><saml:SubjectConfirmationData Recipient=\"foobar123\" InResponseTo=\"_1234567890\" /></saml:SubjectConfirmation></saml:Subject><saml:Conditions NotBefore=\"before\" NotOnOrAfter=\"notafter\"><saml:AudienceRestriction><saml:Audience>foobaraudience</saml:Audience></saml:AudienceRestriction></saml:Conditions></saml:Assertion></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {ok, #esaml_response{assertion = #esaml_assertion{subject = #esaml_subject{in_response_to = ReqId}}}} = Resp,
    "_1234567890" = ReqId.

decode_unsolicited_in_response_to_test() ->
    {Doc, _} = xmerl_scan:string("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" Version=\"2.0\" IssueInstant=\"2013-01-01T01:01:01Z\"><saml:Issuer>foo</saml:Issuer><samlp:Status><samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\" /></samlp:Status><saml:Assertion Version=\"2.0\" IssueInstant=\"test\"><saml:Issuer>foo</saml:Issuer><saml:Subject><saml:NameID>foobar</saml:NameID><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"><saml:SubjectConfirmationData Recipient=\"foobar123\" /></saml:SubjectConfirmation></saml:Subject><saml:Conditions NotBefore=\"before\" NotOnOrAfter=\"notafter\"><saml:AudienceRestriction><saml:Audience>foobaraudience</saml:Audience></saml:AudienceRestriction></saml:Conditions></saml:Assertion></samlp:Response>", [{namespace_conformant, true}]),
    Resp = decode_response(Doc),
    {ok, #esaml_response{assertion = #esaml_assertion{subject = #esaml_subject{in_response_to = ReqId}}}} = Resp,
    "" = ReqId.

validate_assertion_test() ->
    Now = erlang:localtime_to_universaltime(erlang:localtime()),
    DeathSecs = calendar:datetime_to_gregorian_seconds(Now) + 1,
    Death = esaml_util:datetime_to_saml(calendar:gregorian_seconds_to_datetime(DeathSecs)),

    Ns = #xmlNamespace{nodes = [{"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}]},

    E1 = esaml_util:build_nsinfo(Ns, #xmlElement{name = 'saml:Assertion',
        attributes = [#xmlAttribute{name = 'xmlns:saml', value = "urn:oasis:names:tc:SAML:2.0:assertion"}, #xmlAttribute{name = 'Version', value = "2.0"}, #xmlAttribute{name = 'IssueInstant', value = "now"}],
        content = [
            #xmlElement{name = 'saml:Issuer', content = [#xmlText{value = "issuer"}]},
            #xmlElement{name = 'saml:Subject', content = [
                #xmlElement{name = 'saml:SubjectConfirmation', content = [
                    #xmlElement{name = 'saml:SubjectConfirmationData',
                        attributes = [#xmlAttribute{name = 'Recipient', value = "foobar"},
                                      #xmlAttribute{name = 'NotOnOrAfter', value = Death}]
                    } ]} ]},
            #xmlElement{name = 'saml:Conditions', content = [
                #xmlElement{name = 'saml:AudienceRestriction', content = [
                    #xmlElement{name = 'saml:Audience', content = [#xmlText{value = "foo"}]}] }] } ]
    }),
    {ok, Assertion} = validate_assertion(E1, "foobar", "foo", "issuer", 0),
    #esaml_assertion{issue_instant = "now",
                     issuer = "issuer",
                     recipient = "foobar",
                     subject = #esaml_subject{notonorafter = Death},
                     conditions = [{audience, "foo"}]} = Assertion,
    {error, bad_recipient} = validate_assertion(E1, "foo", "something", 0),
    {error, bad_audience} = validate_assertion(E1, "foobar", "something", 0),
    {error, {bad_issuer, "issuer"}} =
        validate_assertion(E1, "foobar", "foo", "something", 0),
    {ok, _} = validate_assertion(E1, "foobar", "foo", any, 0),

    E2 = esaml_util:build_nsinfo(Ns, #xmlElement{name = 'saml:Assertion',
        attributes = [#xmlAttribute{name = 'xmlns:saml', value = "urn:oasis:names:tc:SAML:2.0:assertion"}, #xmlAttribute{name = 'Version', value = "2.0"}, #xmlAttribute{name = 'IssueInstant', value = "now"}],
        content = [
            #xmlElement{name = 'saml:Subject', content = [
                #xmlElement{name = 'saml:SubjectConfirmation', content = [ ]} ]},
            #xmlElement{name = 'saml:Conditions', content = [
                #xmlElement{name = 'saml:AudienceRestriction', content = [
                    #xmlElement{name = 'saml:Audience', content = [#xmlText{value = "foo"}]}] }] } ]
    }),
    {error, bad_recipient} = validate_assertion(E2, "", "", any, 0).

validate_stale_assertion_test() ->
    Ns = #xmlNamespace{nodes = [{"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}]},
    OldStamp = esaml_util:datetime_to_saml({{1990,1,1}, {1,1,1}}),
    E1 = esaml_util:build_nsinfo(Ns, #xmlElement{name = 'saml:Assertion',
        attributes = [#xmlAttribute{name = 'xmlns:saml', value = "urn:oasis:names:tc:SAML:2.0:assertion"}, #xmlAttribute{name = 'Version', value = "2.0"}, #xmlAttribute{name = 'IssueInstant', value = "now"}],
        content = [
            #xmlElement{name = 'saml:Subject', content = [
                #xmlElement{name = 'saml:SubjectConfirmation', content = [
                    #xmlElement{name = 'saml:SubjectConfirmationData',
                        attributes = [#xmlAttribute{name = 'Recipient', value = "foobar"},
                                      #xmlAttribute{name = 'NotOnOrAfter', value = OldStamp}]
                    } ]} ]} ]
    }),
    {error, stale_assertion} = validate_assertion(E1, "foobar", "foo", any, 0).

-endif.
