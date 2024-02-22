%% esaml - SAML for erlang
%%
%% Copyright (c) 2013, Alex Wilson and the University of Queensland
%% All rights reserved.
%%
%% Distributed subject to the terms of the 2-clause BSD license, see
%% the LICENSE file in the root of the distribution.

%% @doc SAML Service Provider (SP) routines
-module(esaml_sp).

-include("esaml.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-export([setup/1, generate_authn_request/2, generate_authn_request/3, generate_metadata/1]).
-export([validate_assertion/2, validate_assertion/3, validate_assertion/4]).
-export([generate_logout_request/3, generate_logout_request/4, generate_logout_response/3]).
-export([validate_logout_request/2, validate_logout_response/2]).

-type xml() :: #xmlElement{} | #xmlDocument{}.
-type dupe_fun() :: fun((esaml:assertion(), Digest :: binary()) -> ok | term()).
-type nameid_format() :: undefined | string().
-export_type([dupe_fun/0]).

%% @private
-spec add_xml_id(xml()) -> xml().
add_xml_id(Xml) ->
    Xml#xmlElement{attributes = Xml#xmlElement.attributes ++ [
        #xmlAttribute{name = 'ID',
            value = esaml_util:unique_id(),
            namespace = #xmlNamespace{}}
        ]}.

%% @private
-spec get_entity_id(esaml:sp()) -> string().
get_entity_id(#esaml_sp{entity_id = EntityID, metadata_uri = MetaURI}) ->
    if (EntityID =:= undefined) ->
        MetaURI;
    true ->
        EntityID
    end.

%% @private
-spec reorder_issuer(xml()) -> xml().
reorder_issuer(Elem) ->
    case lists:partition(fun(#xmlElement{name = N}) -> N == 'saml:Issuer' end, Elem#xmlElement.content) of
        {[Issuer], Other} -> Elem#xmlElement{content = [Issuer | Other]};
        _ -> Elem
    end.

%% @doc Return an AuthnRequest as an XML element
%% @deprecated Use generate_authn_request/3
-spec generate_authn_request(IdpURL :: string(), esaml:sp()) -> #xmlElement{}.
generate_authn_request(IdpURL, SP = #esaml_sp{}) ->
    generate_authn_request(IdpURL, SP, undefined).

%% @doc Return an AuthnRequest as an XML element
-spec generate_authn_request(IdpURL :: string(), esaml:sp(), Format :: nameid_format()) -> #xmlElement{}.
generate_authn_request(IdpURL,
        SP = #esaml_sp{metadata_uri = _MetaURI, consume_uri = ConsumeURI},
        Format) ->
    Now = erlang:localtime_to_universaltime(erlang:localtime()),
    Stamp = esaml_util:datetime_to_saml(Now),
    Issuer = get_entity_id(SP),

    Xml = esaml:to_xml(#esaml_authnreq{issue_instant = Stamp,
                                       destination = IdpURL,
                                       issuer = Issuer,
                                       name_format = Format,
                                       consumer_location = ConsumeURI}),
    if SP#esaml_sp.sp_sign_requests ->
        reorder_issuer(xmerl_dsig:sign(Xml, SP#esaml_sp.key, SP#esaml_sp.certificate));
    true ->
        add_xml_id(Xml)
    end.

%% @doc Return a LogoutRequest as an XML element
%% @deprecated Use generate_logout_request/4
-spec generate_logout_request(IdpURL :: string(), NameID :: string(), esaml:sp()) -> #xmlElement{}.
generate_logout_request(IdpURL, NameID, SP = #esaml_sp{}) ->
    SessionIndex = "",
    Subject = #esaml_subject{name = NameID},
    generate_logout_request(IdpURL, SessionIndex, Subject, SP).

%% @doc Return a LogoutRequest as an XML element
-spec generate_logout_request(IdpURL :: string(), SessionIndex :: string(), esaml:subject(), esaml:sp()) -> #xmlElement{}.
generate_logout_request(IdpURL, SessionIndex, Subject = #esaml_subject{}, SP = #esaml_sp{metadata_uri = _MetaURI})
        when is_record(Subject, esaml_subject) ->
    Now = erlang:localtime_to_universaltime(erlang:localtime()),
    Stamp = esaml_util:datetime_to_saml(Now),
    Issuer = get_entity_id(SP),

    Xml = esaml:to_xml(#esaml_logoutreq{issue_instant = Stamp,
                                       destination = IdpURL,
                                       issuer = Issuer,
                                       name = Subject#esaml_subject.name,
                                       name_qualifier = Subject#esaml_subject.name_qualifier,
                                       sp_name_qualifier = Subject#esaml_subject.sp_name_qualifier,
                                       name_format = Subject#esaml_subject.name_format,
                                       session_index = SessionIndex,
                                       reason = user}),
    if SP#esaml_sp.sp_sign_requests ->
        reorder_issuer(xmerl_dsig:sign(Xml, SP#esaml_sp.key, SP#esaml_sp.certificate));
    true ->
        add_xml_id(Xml)
    end.

%% @doc Return a LogoutResponse as an XML element
-spec generate_logout_response(IdpURL :: string(), esaml:status_code(), esaml:sp()) -> #xmlElement{}.
generate_logout_response(IdpURL, Status, SP = #esaml_sp{metadata_uri = _MetaURI}) ->
    Now = erlang:localtime_to_universaltime(erlang:localtime()),
    Stamp = esaml_util:datetime_to_saml(Now),
    Issuer = get_entity_id(SP),

    Xml = esaml:to_xml(#esaml_logoutresp{issue_instant = Stamp,
                                       destination = IdpURL,
                                       issuer = Issuer,
                                       status = Status}),
    if SP#esaml_sp.sp_sign_requests ->
        reorder_issuer(xmerl_dsig:sign(Xml, SP#esaml_sp.key, SP#esaml_sp.certificate));
    true ->
        add_xml_id(Xml)
    end.

%% @doc Return the SP metadata as an XML element
-spec generate_metadata(esaml:sp()) -> #xmlElement{}.
generate_metadata(SP = #esaml_sp{org = Org, tech = Tech}) ->
    EntityID = get_entity_id(SP),
    Xml = esaml:to_xml(#esaml_sp_metadata{
        org = Org,
        tech = Tech,
        signed_requests = SP#esaml_sp.sp_sign_requests,
        signed_assertions = SP#esaml_sp.idp_signs_assertions or SP#esaml_sp.idp_signs_envelopes,
        certificate = SP#esaml_sp.certificate,
        cert_chain = SP#esaml_sp.cert_chain,
        consumer_location = SP#esaml_sp.consume_uri,
        logout_location = SP#esaml_sp.logout_uri,
        entity_id = EntityID,
        cache_duration = SP#esaml_sp.cache_duration}),
    if SP#esaml_sp.sp_sign_metadata ->
        xmerl_dsig:sign(Xml, SP#esaml_sp.key, SP#esaml_sp.certificate);
    true ->
        add_xml_id(Xml)
    end.

%% @doc Initialize and validate an esaml_sp record
-spec setup(esaml:sp()) -> esaml:sp().
setup(SP = #esaml_sp{trusted_fingerprints = FPs, metadata_uri = MetaURI,
                     consume_uri = ConsumeURI}) ->
    Fingerprints = esaml_util:convert_fingerprints(FPs),
    case MetaURI of "" -> error("must specify metadata URI"); _ -> ok end,
    case ConsumeURI of "" -> error("must specify consume URI"); _ -> ok end,
    if (SP#esaml_sp.key =:= undefined) andalso (SP#esaml_sp.sp_sign_requests) ->
        error("must specify a key to sign requests");
    true -> ok
    end,
    if (not (SP#esaml_sp.key =:= undefined)) and (not (SP#esaml_sp.certificate =:= undefined)) ->
        SP#esaml_sp{sp_sign_requests = true, sp_sign_metadata = true, trusted_fingerprints = Fingerprints};
    true ->
        SP#esaml_sp{trusted_fingerprints = Fingerprints}
    end.

%% @doc Validate and parse a LogoutRequest element
-spec validate_logout_request(xml(), esaml:sp()) ->
        {ok, esaml:logoutreq()} | {error, Reason :: term()}.
validate_logout_request(Xml, SP = #esaml_sp{}) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    esaml_util:threaduntil([
        fun(X) ->
            case xmerl_xpath:string("/samlp:LogoutRequest", X, [{namespace, Ns}]) of
                [#xmlElement{}] -> X;
                _ -> {error, bad_assertion}
            end
        end,
        fun(X) ->
            if SP#esaml_sp.idp_signs_logout_requests ->
                case xmerl_dsig:verify(X, SP#esaml_sp.trusted_fingerprints) of
                    ok -> X;
                    OuterError -> {error, OuterError}
                end;
            true -> X
            end
        end,
        fun(X) ->
            case (catch esaml:decode_logout_request(X)) of
                {ok, LR} -> LR;
                {'EXIT', Reason} -> {error, Reason};
                Err -> Err
            end
        end
    ], Xml).

%% @doc Validate and parse a LogoutResponse element
-spec validate_logout_response(xml(), esaml:sp()) ->
        {ok, esaml:logoutresp()} | {error, Reason :: term()}.
validate_logout_response(Xml, SP = #esaml_sp{}) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'},
          {"ds", 'http://www.w3.org/2000/09/xmldsig#'}],
    esaml_util:threaduntil([
        fun(X) ->
            case xmerl_xpath:string("/samlp:LogoutResponse", X, [{namespace, Ns}]) of
                [#xmlElement{}] -> X;
                _ -> {error, bad_assertion}
            end
        end,
        fun(X) ->
            % Signature is optional on the logout_response. Verify it if we have it.
            case xmerl_xpath:string("/samlp:LogoutResponse/ds:Signature", X, [{namespace, Ns}]) of
                [#xmlElement{}] ->
                    case xmerl_dsig:verify(X, SP#esaml_sp.trusted_fingerprints) of
                        ok -> X;
                        OuterError -> {error, OuterError}
                    end;
                _ -> X
            end
        end,
        fun(X) ->
            case (catch esaml:decode_logout_response(X)) of
                {ok, LR} -> LR;
                {'EXIT', Reason} -> {error, Reason};
                Err -> Err
            end
        end,
        fun(LR = #esaml_logoutresp{status = success}) -> LR;
           (#esaml_logoutresp{status = S,
                              status_second_level = SecondStatus}) ->
               {error, {status, S, SecondStatus}}
        end
    ], Xml).

%% @doc Validate and decode an assertion envelope in parsed XML
-spec validate_assertion(xml(), esaml:sp()) ->
        {ok, esaml:assertion()} | {error, Reason :: term()}.
validate_assertion(Xml, SP = #esaml_sp{}) ->
    validate_assertion(Xml, fun(_A, _Digest) -> ok end, SP).

%% @doc Validate and decode an assertion envelope in parsed XML
%%
%% The dupe_fun argument is intended to detect duplicate assertions
%% in the case of a replay attack.
-spec validate_assertion(xml(), dupe_fun(), esaml:sp()) ->
        {ok, esaml:assertion()} | {error, Reason :: term()}.
validate_assertion(Xml, DuplicateFun, SP) ->
    validate_assertion(Xml, DuplicateFun, SP, any).

-spec validate_assertion(xml(), dupe_fun(), esaml:sp(), string() | any) ->
        {ok, esaml:assertion()} | {error, Reason :: term()}.
validate_assertion(Xml, DuplicateFun, SP = #esaml_sp{clock_skew = ClockSkew},
                   ExpectedIssuer) ->
    Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
          {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
    SuccessStatus = "urn:oasis:names:tc:SAML:2.0:status:Success",
    esaml_util:threaduntil([
        fun(X) ->
            case xmerl_xpath:string("/samlp:Response/samlp:Status/samlp:StatusCode/@Value", X, [{namespace, Ns}]) of
                [StatusCode] ->
                    case StatusCode#xmlAttribute.value of
                        SuccessStatus -> X;
                        ErrorStatus ->
                            ErrorMessage = case xmerl_xpath:string("/samlp:Response/samlp:Status/samlp:StatusMessage/text()", X, [{namespace, Ns}]) of
                                [] -> undefined;
                                [A] -> lists:flatten(xmerl_xs:value_of(A));
                                _ -> malformed
                            end,
                            {error, {saml_error, ErrorStatus, ErrorMessage}}
                    end;
                _ -> {error, bad_saml}
            end
        end,
        fun(X) ->
            case xmerl_xpath:string("/samlp:Response/saml:EncryptedAssertion", X, [{namespace, Ns}]) of
                [A1] ->
                    try
                        #xmlElement{} = DecryptedAssertion = decrypt_assertion(A1, SP),
                        xmerl_xpath:string("/saml:Assertion", DecryptedAssertion, [{namespace, Ns}]) of
                        [A2] -> A2
                    catch
                        _:Error:ST ->
                            {error, {decryption_problem, {Error, ST}}}
                    end;
                _ ->
                    case xmerl_xpath:string("/samlp:Response/saml:Assertion", X, [{namespace, Ns}]) of
                        [A3] -> A3;
                        _ -> {error, bad_assertion}
                    end
            end
        end,
        fun(A) ->
            if
                SP#esaml_sp.idp_signs_envelopes ->
                    case xmerl_dsig:verify(Xml, SP#esaml_sp.trusted_fingerprints) of
                        ok -> A;
                        OuterError -> {error, {envelope, OuterError}}
                    end;
                true -> A
            end
        end,
        fun(A) ->
            if SP#esaml_sp.idp_signs_assertions ->
                case xmerl_dsig:verify(A, SP#esaml_sp.trusted_fingerprints) of
                    ok -> A;
                    InnerError -> {error, {assertion, InnerError}}
                end;
            true -> A
            end
        end,
        fun(A) ->
            Recipient = case SP#esaml_sp.assertion_recipient of
                            undefined -> SP#esaml_sp.consume_uri;
                            R -> R
                        end,
            case esaml:validate_assertion(A, Recipient, get_entity_id(SP),
                                          ExpectedIssuer, ClockSkew) of
                {ok, AR} ->
                    case DuplicateFun(AR, xmerl_dsig:digest(A)) of
                        ok -> AR;
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} -> {error, Reason}
            end
        end
    ], Xml).


%% @doc Decrypts an encrypted assertion element.
decrypt_assertion(EncryptedAssertion, #esaml_sp{key = PrivateKey}) ->
    XencNs = [{"xenc", 'http://www.w3.org/2001/04/xmlenc#'}],
    [EncryptedData] = xmerl_xpath:string("./xenc:EncryptedData", EncryptedAssertion, [{namespace, XencNs}]),
    [#xmlText{value = CipherValue64}] = xmerl_xpath:string("xenc:CipherData/xenc:CipherValue/text()", EncryptedData, [{namespace, XencNs}]),
    CipherValue = base64:decode(CipherValue64),
    SymmetricKey = decrypt_key_info(EncryptedAssertion, EncryptedData, PrivateKey),
    [#xmlAttribute{value = Algorithm}] = xmerl_xpath:string("./xenc:EncryptionMethod/@Algorithm", EncryptedData, [{namespace, XencNs}]),
    AssertionXml = block_decrypt(Algorithm, SymmetricKey, CipherValue),
    {Assertion, _} = xmerl_scan:string(AssertionXml, [{namespace_conformant, true}]),
    Assertion.


decrypt_key_info(EncryptedAssertion, EncryptedData, Key) ->
    DsNs = [{"ds", 'http://www.w3.org/2000/09/xmldsig#'}],
    XencNs = [{"xenc", 'http://www.w3.org/2001/04/xmlenc#'}],
    [KeyInfo] = xmerl_xpath:string("./ds:KeyInfo", EncryptedData, [{namespace, DsNs}]),
    EKType = "http://www.w3.org/2001/04/xmlenc#EncryptedKey",
    [EncryptedKey] =
        case xmerl_xpath:string(
               "./ds:RetrievalMethod[@Type='" ++ EKType ++ "']/@URI",
               KeyInfo, [{namespace, DsNs}]) of
            [#xmlAttribute{value = "#" ++ URI}] ->
                xmerl_xpath:string("//xenc:EncryptedKey[@Id='" ++ URI ++ "']",
                                   EncryptedAssertion, [{namespace, XencNs}]);
            [] ->
                xmerl_xpath:string("./xenc:EncryptedKey",
                                   KeyInfo, [{namespace, XencNs}])
        end,
    [#xmlAttribute{value = Algorithm}] =
        xmerl_xpath:string("./xenc:EncryptionMethod/@Algorithm",
                           EncryptedKey, [{namespace, XencNs}]),
    [#xmlText{value = CipherValue64}] =
        xmerl_xpath:string("./xenc:CipherData/xenc:CipherValue/text()",
                           EncryptedKey, [{namespace, XencNs}]),
    CipherValue = base64:decode(CipherValue64),
    decrypt(CipherValue, Algorithm, Key).

decrypt(CipherValue, "http://www.w3.org/2001/04/xmlenc#rsa-1_5", Key) ->
    Opts = [
        {rsa_padding, rsa_pkcs1_padding},
        {rsa_pad, rsa_pkcs1_padding}
    ],
    public_key:decrypt_private(CipherValue, Key, Opts);

decrypt(CipherValue, "http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p", Key) ->
    Opts = [
        {rsa_padding, rsa_pkcs1_oaep_padding},
        {rsa_pad, rsa_pkcs1_oaep_padding}
    ],
    public_key:decrypt_private(CipherValue, Key, Opts).


block_decrypt("http://www.w3.org/2009/xmlenc11#aes128-gcm", SymmetricKey, CipherValue) ->
    %% IV: 12 bytes and Tag data: 16 bytes
    EncryptedDataSize = byte_size(CipherValue) - 12 - 16,
    <<IV:12/binary, EncryptedData:EncryptedDataSize/binary, Tag:16/binary>> = CipherValue,
    DecryptedData = crypto:crypto_one_time_aead(aes_128_gcm, SymmetricKey, IV, EncryptedData, <<>>, Tag, false),
    binary_to_list(DecryptedData);

block_decrypt("http://www.w3.org/2001/04/xmlenc#aes128-cbc", SymmetricKey, CipherValue) ->
    <<IV:16/binary, EncryptedData/binary>> = CipherValue,
    DecryptedData = crypto:crypto_one_time(aes_128_cbc, SymmetricKey, IV, EncryptedData, false),
    IsPadding = fun(X) -> X < 16 end,
    lists:reverse(lists:dropwhile(IsPadding, lists:reverse(binary_to_list(DecryptedData))));

block_decrypt("http://www.w3.org/2001/04/xmlenc#aes256-cbc", SymmetricKey, CipherValue) ->
    <<IV:16/binary, EncryptedData/binary>> = CipherValue,
    DecryptedData = crypto:crypto_one_time(aes_256_cbc, SymmetricKey, IV, EncryptedData, false),
    IsPadding = fun(X) -> X < 16 end,
    lists:reverse(lists:dropwhile(IsPadding, lists:reverse(binary_to_list(DecryptedData)))).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
