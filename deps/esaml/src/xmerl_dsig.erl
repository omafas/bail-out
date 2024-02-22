%% -*- coding: utf-8 -*-
%%
%% esaml - SAML for erlang
%%
%% Copyright (c) 2013, Alex Wilson and the University of Queensland
%% All rights reserved.
%%
%% Distributed subject to the terms of the 2-clause BSD license, see
%% the LICENSE file in the root of the distribution.

%% @doc XML digital signatures for xmerl
%%
%% Functions for performing XML digital signature generation and
%% verification, as specified at http://www.w3.org/TR/xmldsig-core/ .
%%
%% These routines work on xmerl data structures (see the xmerl user guide
%% for details).
%%
%% Currently only RSA + SHA1|SHA256 signatures are supported, in the typical
%% enveloped mode.
-module(xmerl_dsig).

-export([verify/1, verify/2, sign/3, sign/4, strip/1, digest/1]).

-include_lib("xmerl/include/xmerl.hrl").
-include_lib("public_key/include/public_key.hrl").

-type xml_thing() :: #xmlDocument{} | #xmlElement{} | #xmlAttribute{} | #xmlPI{} | #xmlText{} | #xmlComment{}.
-type sig_method() :: rsa_sha1 | rsa_sha256.
-type sig_method_uri() :: string().
-type fingerprint() :: binary() | {sha | sha256, binary()}.

%% @doc Returns an xmlelement without any ds:Signature elements that are inside it.
-spec strip(Element :: #xmlElement{} | #xmlDocument{}) -> #xmlElement{}.
strip(#xmlDocument{content = Kids} = Doc) ->
    NewKids = [if (element(1,K) =:= xmlElement) -> strip(K); true -> K end || K <- Kids],
    Doc#xmlDocument{content = NewKids};

strip(#xmlElement{content = Kids} = Elem) ->
    NewKids = [Kid || Kid <- Kids, is_valid_kid(Kid)],
    Elem#xmlElement{content = NewKids}.

is_valid_kid(Kid) when is_record(Kid, xmlAttribute); is_record(Kid, xmlElement) ->
    Canon_Name = xmerl_c14n:canon_name(Kid),
    Canon_Name =/= "http://www.w3.org/2000/09/xmldsig#Signature";
is_valid_kid(_Child) -> true.

%% @doc Signs the given XML element by creating a ds:Signature element within it, returning
%%      the element with the signature added.
%%
%% Don't use "ds" as a namespace prefix in the envelope document, or things will go baaaad.
-spec sign(Element :: #xmlElement{}, PrivateKey :: #'RSAPrivateKey'{}, CertBin :: binary()) -> #xmlElement{}.
sign(ElementIn, PrivateKey = #'RSAPrivateKey'{}, CertBin) when is_binary(CertBin) ->
    sign(ElementIn, PrivateKey, CertBin, "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256").

-spec sign(Element :: #xmlElement{}, PrivateKey :: #'RSAPrivateKey'{}, CertBin :: binary(), SignatureMethod :: sig_method() | sig_method_uri()) -> #xmlElement{}.
sign(ElementIn, PrivateKey = #'RSAPrivateKey'{}, CertBin, SigMethod) when is_binary(CertBin) ->
    % get rid of any previous signature
    ElementStrip = strip(ElementIn),

    % make sure the root element has an ID... if it doesn't yet, add one
    {Element, Id} = case lists:keyfind('ID', 2, ElementStrip#xmlElement.attributes) of
        #xmlAttribute{value = CapId} -> {ElementStrip, CapId};
        _ ->
            case lists:keyfind('id', 2, ElementStrip#xmlElement.attributes) of
                #xmlAttribute{value = LowId} -> {ElementStrip, LowId};
                _ ->
                    NewId = esaml_util:unique_id(),
                    Attr = #xmlAttribute{name = 'ID', value = NewId, namespace = #xmlNamespace{}},
                    NewAttrs = [Attr | ElementStrip#xmlElement.attributes],
                    Elem = ElementStrip#xmlElement{attributes = NewAttrs},
                    {Elem, NewId}
            end
    end,

    {HashFunction, DigestMethod, SignatureMethodAlgorithm} = signature_props(SigMethod),

    % first we need the digest, to generate our SignedInfo element
    CanonXml = xmerl_c14n:c14n(Element),
    DigestValue = base64:encode_to_string(
        crypto:hash(HashFunction, unicode:characters_to_binary(CanonXml, unicode, utf8))),

    Ns = #xmlNamespace{nodes = [{"ds", 'http://www.w3.org/2000/09/xmldsig#'}]},
    SigInfo = esaml_util:build_nsinfo(Ns, #xmlElement{
        name = 'ds:SignedInfo',
        content = [
            #xmlElement{name = 'ds:CanonicalizationMethod',
                attributes = [#xmlAttribute{name = 'Algorithm', value = "http://www.w3.org/2001/10/xml-exc-c14n#"}]},
            #xmlElement{name = 'ds:SignatureMethod',
                attributes = [#xmlAttribute{name = 'Algorithm', value = SignatureMethodAlgorithm}]},
            #xmlElement{name = 'ds:Reference',
                attributes = [#xmlAttribute{name = 'URI', value = lists:flatten(["#" | Id])}],
                content = [
                    #xmlElement{name = 'ds:Transforms', content = [
                        #xmlElement{name = 'ds:Transform',
                            attributes = [#xmlAttribute{name = 'Algorithm', value = "http://www.w3.org/2000/09/xmldsig#enveloped-signature"}]},
                        #xmlElement{name = 'ds:Transform',
                            attributes = [#xmlAttribute{name = 'Algorithm', value = "http://www.w3.org/2001/10/xml-exc-c14n#"}]}]},
                    #xmlElement{name = 'ds:DigestMethod',
                        attributes = [#xmlAttribute{name = 'Algorithm', value = DigestMethod}]},
                    #xmlElement{name = 'ds:DigestValue',
                        content = [#xmlText{value = DigestValue}]}
                ]}
        ]
    }),

    % now we sign the SignedInfo element...
    SigInfoCanon = xmerl_c14n:c14n(SigInfo),
    Data = unicode:characters_to_binary(SigInfoCanon, unicode, utf8),

    Signature = public_key:sign(Data, HashFunction, PrivateKey),
    Sig64 = base64:encode_to_string(Signature),
    Cert64 = base64:encode_to_string(CertBin),

    % and wrap it all up with the signature and certificate
    SigElem = esaml_util:build_nsinfo(Ns, #xmlElement{
        name = 'ds:Signature',
        attributes = [#xmlAttribute{name = 'xmlns:ds', value = "http://www.w3.org/2000/09/xmldsig#"}],
        content = [
            SigInfo,
            #xmlElement{name = 'ds:SignatureValue', content = [#xmlText{value = Sig64}]},
            #xmlElement{name = 'ds:KeyInfo', content = [
                #xmlElement{name = 'ds:X509Data', content = [
                    #xmlElement{name = 'ds:X509Certificate', content = [#xmlText{value = Cert64} ]}]}]}
        ]
    }),
    Element#xmlElement{content = [SigElem | Element#xmlElement.content]}.

%% @doc Returns the canonical digest of an (optionally signed) element
%%
%% Strips any XML digital signatures and applies any relevant InclusiveNamespaces
%% before generating the digest.
-spec digest(Element :: #xmlElement{}) -> binary().
digest(Element) -> digest(Element, sha256).

-spec digest(Element :: #xmlElement{}, HashFunction :: sha | sha256) -> binary().
digest(Element, HashFunction) ->
    DsNs = [{"ds", 'http://www.w3.org/2000/09/xmldsig#'},
        {"ec", 'http://www.w3.org/2001/10/xml-exc-c14n#'}],

    Txs = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:Reference/ds:Transforms/ds:Transform[@Algorithm='http://www.w3.org/2001/10/xml-exc-c14n#']", Element, [{namespace, DsNs}]),
    InclNs = case Txs of
        [C14nTx = #xmlElement{}] ->
            case xmerl_xpath:string("ec:InclusiveNamespaces/@PrefixList", C14nTx, [{namespace, DsNs}]) of
                [] -> [];
                [#xmlAttribute{value = NsList}] -> string:tokens(NsList, " ,")
            end;
        _ -> []
    end,

    CanonXml = xmerl_c14n:c14n(strip(Element), false, InclNs),
    CanonXmlUtf8 = unicode:characters_to_binary(CanonXml, unicode, utf8),
    crypto:hash(HashFunction, CanonXmlUtf8).

%% @doc Verifies an XML digital signature on the given element.
%%
%% Fingerprints is a list of valid cert fingerprints that can be
%% accepted.
%%
%% Will throw badmatch errors if you give it XML that is not signed
%% according to the xml-dsig spec. If you're using something other
%% than rsa+sha1 or sha256 this will asplode. Don't say I didn't warn you.
-spec verify(Element :: #xmlElement{}, Fingerprints :: [fingerprint()] | any) -> ok | {error, bad_digest | bad_signature | cert_not_accepted}.
verify(Element, Fingerprints) ->
    DsNs = [{"ds", 'http://www.w3.org/2000/09/xmldsig#'},
        {"ec", 'http://www.w3.org/2001/10/xml-exc-c14n#'}],

    case xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:SignatureMethod/@Algorithm", Element, [{namespace, DsNs}]) of
        [] ->
            {error, no_signature};
        [#xmlAttribute{value = SignatureMethodAlgorithm}] ->
            {HashFunction, _, _} = signature_props(SignatureMethodAlgorithm),

            [#xmlAttribute{value = "http://www.w3.org/2001/10/xml-exc-c14n#"}] = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:CanonicalizationMethod/@Algorithm", Element, [{namespace, DsNs}]),
            [#xmlAttribute{value = SignatureMethodAlgorithm}] = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:SignatureMethod/@Algorithm", Element, [{namespace, DsNs}]),
            [C14nTx = #xmlElement{}] = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:Reference/ds:Transforms/ds:Transform[@Algorithm='http://www.w3.org/2001/10/xml-exc-c14n#']", Element, [{namespace, DsNs}]),
            InclNs = case xmerl_xpath:string("ec:InclusiveNamespaces/@PrefixList", C14nTx, [{namespace, DsNs}]) of
                [] -> [];
                [#xmlAttribute{value = NsList}] -> string:tokens(NsList, " ,")
            end,

            CanonXml = xmerl_c14n:c14n(strip(Element), false, InclNs),
            CanonXmlUtf8 = unicode:characters_to_binary(CanonXml, unicode, utf8),
            CanonSha = crypto:hash(HashFunction, CanonXmlUtf8),

            [#xmlText{value = Sha64}] = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:Reference/ds:DigestValue/text()", Element, [{namespace, DsNs}]),
            CanonSha2 = base64:decode(Sha64),

            if not (CanonSha =:= CanonSha2) ->
                {error, bad_digest};

            true ->
                [SigInfo] = xmerl_xpath:string("ds:Signature/ds:SignedInfo", Element, [{namespace, DsNs}]),
                SigInfoCanon = xmerl_c14n:c14n(SigInfo),
                Data = list_to_binary(SigInfoCanon),

                [#xmlText{value = Sig64}] = xmerl_xpath:string("ds:Signature//ds:SignatureValue/text()", Element, [{namespace, DsNs}]),
                Sig = base64:decode(Sig64),

                [#xmlText{value = Cert64}] = xmerl_xpath:string("ds:Signature//ds:X509Certificate/text()", Element, [{namespace, DsNs}]),
                CertBin = base64:decode(Cert64),
                CertHash = crypto:hash(sha, CertBin),
                CertHash2 = crypto:hash(sha256, CertBin),
                CertHash384 = crypto:hash(sha384, CertBin),
                CertHash512 = crypto:hash(sha512, CertBin),

                Cert = public_key:pkix_decode_cert(CertBin, plain),
                KeyBin = case Cert#'Certificate'.tbsCertificate#'TBSCertificate'.subjectPublicKeyInfo#'SubjectPublicKeyInfo'.subjectPublicKey of
                  {_, KeyBin2} -> KeyBin2;
                  KeyBin3 -> KeyBin3
                end,
                Key = public_key:pem_entry_decode({'RSAPublicKey', KeyBin, not_encrypted}),

                case public_key:verify(Data, HashFunction, Sig, Key) of
                    true ->
                        case Fingerprints of
                            any ->
                                ok;
                            _ ->
                                CertHashes = [CertHash,
                                              {sha, CertHash},
                                              {sha256, CertHash2},
                                              {sha384, CertHash384},
                                              {sha512, CertHash512}],
                                case lists:any(fun (X) ->
                                                   lists:member(X, Fingerprints)
                                               end, CertHashes) of
                                    true ->
                                        ok;
                                    false ->
                                        {error, cert_not_accepted}
                                end
                        end;
                    false ->
                        {error, bad_signature}
                end
            end;
        _ ->
            {error, multiple_signatures}
    end.

%% @doc Verifies an XML digital signature, trusting any valid certificate.
%%
%% This is really not recommended for production use, but it's handy in
%% testing/development.
-spec verify(Element :: xml_thing()) -> ok | {error, bad_digest | bad_signature | cert_not_accepted}.
verify(Element) ->
    verify(Element, any).

-spec signature_props(atom() | string()) -> {HashFunction :: atom(), DigestMethodUrl :: string(), SignatureMethodUrl :: string()}.
signature_props("http://www.w3.org/2000/09/xmldsig#rsa-sha1") ->
    signature_props(rsa_sha1);
signature_props(rsa_sha1) ->
    HashFunction = sha,
    DigestMethod = "http://www.w3.org/2000/09/xmldsig#sha1",
    Url = "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
    {HashFunction, DigestMethod, Url};
signature_props("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256") ->
    signature_props(rsa_sha256);
signature_props(rsa_sha256) ->
    HashFunction = sha256,
    DigestMethod = "http://www.w3.org/2001/04/xmlenc#sha256",
    Url = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
    {HashFunction, DigestMethod, Url}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

verify_valid_sha1_test() ->
    {Doc, _} = xmerl_scan:string("<?xml version=\"1.0\"?><x:foo ID=\"9616e6c0-f525-11b7-afb7-5cf9dd711ed3\" xmlns:x=\"urn:foo:x:\"><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#rsa-sha1\"/><ds:Reference URI=\"#9616e6c0-f525-11b7-afb7-5cf9dd711ed3\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>xPVYXCs5uMMmIbfTiTZ5R5DVhTU=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>rYk+WAghakHfR9VtpLz3AkMD1xLD1wISfNgch9+i+PC72RqhmfeMCZMkBaw0EO+CTKEoFBQIQaJYlEj8rIG+XN+8HyBV75BrMKZs1rdN+459Rpn2FOOJuHVb2jLDPecC9Ok/DGaNu6lol60hG9di66EZkL8ErQCuCeZqiw9tiXMUPQyVa2GxqT2UeXvJ5YtkNMDweUc3HhEnTG3ovYt1vOZt679w4N0HAwUa9rk40Z12fOTx77BbMICZ9Q4N2m3UbaFU24YHYpHR+WUTiwzXcmdkrHiE5IF37h7rTKAEixD2bTojaefmrobAz0+mBhCqBPcbfNLhLrpT43xhMenjpA==</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIIDfTCCAmWgAwIBAgIJANCSQXrTqpDjMA0GCSqGSIb3DQEBBQUAMFUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApRdWVlbnNsYW5kMREwDwYDVQQHDAhCcmlzYmFuZTEMMAoGA1UECgwDRm9vMRAwDgYDVQQDDAdzYW1saWRwMB4XDTEzMDQyOTA2MTAyOVoXDTIzMDQyOTA2MTAyOVowVTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClF1ZWVuc2xhbmQxETAPBgNVBAcMCEJyaXNiYW5lMQwwCgYDVQQKDANGb28xEDAOBgNVBAMMB3NhbWxpZHAwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDFhBuEO3fX+FlyT2YYzozxmXNXEmQjksigJSKD4hvsgsyGyl1iLkqNT6IbkuMXoyJG6vXufMNVLoktcLBd6eu6LQwwRjSU62AVCWZhIJP8U6lHqVsxiP90h7/b1zM7Hm9uM9RHtG+nKB7W0xNRihG8BUQOocSaLIMZZXqDPW1h/UvUqmpEzCtT0kJyXX0UAmDHzTYWHt8dqOYdcO2RAlJX0UKnwG1bHjTAfw01lJeOZiF66kH777nStYSElrHXr0NmCO/2gt6ouEnnUqJWDWRzaLbzhMLmGj83lmPgwZCBbIbnbQWLYPQ438EWfEYELq9nSQrgfUmmDPb4rtsQOXqZAgMBAAGjUDBOMB0GA1UdDgQWBBT64y2JSqY96YTYv1QbFyCPp3To/zAfBgNVHSMEGDAWgBT64y2JSqY96YTYv1QbFyCPp3To/zAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQAecr+C4w3LYAU4pCbLAW2BbFWGZRqBAr6ZKZKQrrqSMUJUiRDoKc5FYJrkjl/sGHAe3b5vBrU3lb/hpSiKXVf4/zBP7uqAF75B6LwnMwYpPcXlnRyPngQcdTL5EyQT5vwqv+H3zB64TblMYbsvqm6+1ippRNq4IXQX+3NGTEkhh0xgH+e3wE8BjjiygDu0MqopaIVPemMVQIm3HI+4jmf60bz8GLD1J4dj5CvyW1jQCXu2K2fcS1xJS0FLrxh/QxR0+3prGkYiZeOWE/dHlTTvQLB+NftyamUthVxMFe8dvXMTix/egox+ps2NuO2XTkDaeeRFjUhPhS8SvZO9l0lZ</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><x:name>blah</x:name></x:foo>", [{namespace_conformant, true}]),
    ok = verify(Doc),
    ok = verify(Doc, [<<198,86,10,182,119,241,20,3,198,88,35,42,145,76,251,113,52,21,246,156>>]).

verify_valid_sha256_test() ->
    {Doc, _} = xmerl_scan:string("<?xml version=\"1.0\" encoding=\"UTF-8\"?><saml2p:Response xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\" Destination=\"https://api.kato.im/saml/v2/demo-okta/consume\" ID=\"id61268949075197931241532406\" IssueInstant=\"2015-01-09T01:57:56.021Z\" Version=\"2.0\"><saml2:Issuer xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" Format=\"urn:oasis:names:tc:SAML:2.0:nameid-format:entity\">http://www.okta.com/kzk0hhgeJEEBMWPZLFWI</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2001/04/xmldsig-more#rsa-sha256\"/><ds:Reference URI=\"#id61268949075197931241532406\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#sha256\"/><ds:DigestValue>jE916v/l6/Hh1+orj1OuounIq73STjkXd8ZjJdnm0sk=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>KFK1J0eP8jcnM+YPyiONtgZEUhCoKSTs9Md2tKWr+rZLq+RLxfuEVOBgeQeoWLzMIkbhrsOuKdk/w/FfgYxhlyO7EA3IoE87oQi98B3IFYA17qgsosSOXeNra68WuCmmSxFncWMkw/VkQxcUXa8vqaRgVBXL7BgTVYi++NdYdTg=</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIICmTCCAgKgAwIBAgIGAUjq/PsnMA0GCSqGSIb3DQEBBQUAMIGPMQswCQYDVQQGEwJVUzETMBEG
A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU
MBIGA1UECwwLU1NPUHJvdmlkZXIxEDAOBgNVBAMMB2thdG9faW0xHDAaBgkqhkiG9w0BCQEWDWlu
Zm9Ab2t0YS5jb20wHhcNMTQxMDA3MTQyMTAwWhcNNDQxMDA3MTQyMjAwWjCBjzELMAkGA1UEBhMC
VVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xDTALBgNVBAoM
BE9rdGExFDASBgNVBAsMC1NTT1Byb3ZpZGVyMRAwDgYDVQQDDAdrYXRvX2ltMRwwGgYJKoZIhvcN
AQkBFg1pbmZvQG9rdGEuY29tMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC2O65TnPiD1aJC
EDnT4d7PvUhZtIyEygxs8OYmVB4sPR6tfwDtXaoQ6SxC9egXNvZb9tBYdgkJ5+/R5fxuu+Rw2dJv
Fmt8+BffB6rS3fMDfyeUBpwDdOEHYV/8gwAkAOXCLwatNQW9awgfSjniHvMvWYclTfSwiOnnx422
qte8uwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBADy3G1EbTA+Af27Ci8DwbYlBOVezqpH+fak8Y0EY
2pYIoWQgIj2/E6mTEQHThk25qgaXwiaBGF9096/GxipgZe75Us9mFz2CUCGAHx8nGGiNtUDCeQFE
z+CClhkG4RiRcwuxMtkA9m0GmjEYh7TeDZJ3ntXaexH3s+IKFwEq2BsF</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><saml2p:Status xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\"><saml2p:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/></saml2p:Status><saml2:Assertion xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" ID=\"id61268949075925452141027347\" IssueInstant=\"2015-01-09T01:57:56.021Z\" Version=\"2.0\"><saml2:Issuer Format=\"urn:oasis:names:tc:SAML:2.0:nameid-format:entity\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">http://www.okta.com/kzk0hhgeJEEBMWPZLFWI</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2001/04/xmldsig-more#rsa-sha256\"/><ds:Reference URI=\"#id61268949075925452141027347\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#sha256\"/><ds:DigestValue>TbfbYyN9Gw/0hNL+ylMeYR5zKaN8GvppmCJcwHhrqso=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>gJhmBIQ1Yk1TRHDQRjZM4bPpJHAEw7pmOrQ1k76y3l4rGnuXflRtHoJ7VrsytBI5eYFVSuPD8ojmkFeokdYQYcMpOdl6gDmWskdFenPGP/jPR27sapf8AWhAjMQgmaA8AOAPbcZmfXxSbVO+Ljpo6NhSK7qVhydnLNFitwKw69s=</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIICmTCCAgKgAwIBAgIGAUjq/PsnMA0GCSqGSIb3DQEBBQUAMIGPMQswCQYDVQQGEwJVUzETMBEG
A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU
MBIGA1UECwwLU1NPUHJvdmlkZXIxEDAOBgNVBAMMB2thdG9faW0xHDAaBgkqhkiG9w0BCQEWDWlu
Zm9Ab2t0YS5jb20wHhcNMTQxMDA3MTQyMTAwWhcNNDQxMDA3MTQyMjAwWjCBjzELMAkGA1UEBhMC
VVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xDTALBgNVBAoM
BE9rdGExFDASBgNVBAsMC1NTT1Byb3ZpZGVyMRAwDgYDVQQDDAdrYXRvX2ltMRwwGgYJKoZIhvcN
AQkBFg1pbmZvQG9rdGEuY29tMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC2O65TnPiD1aJC
EDnT4d7PvUhZtIyEygxs8OYmVB4sPR6tfwDtXaoQ6SxC9egXNvZb9tBYdgkJ5+/R5fxuu+Rw2dJv
Fmt8+BffB6rS3fMDfyeUBpwDdOEHYV/8gwAkAOXCLwatNQW9awgfSjniHvMvWYclTfSwiOnnx422
qte8uwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBADy3G1EbTA+Af27Ci8DwbYlBOVezqpH+fak8Y0EY
2pYIoWQgIj2/E6mTEQHThk25qgaXwiaBGF9096/GxipgZe75Us9mFz2CUCGAHx8nGGiNtUDCeQFE
z+CClhkG4RiRcwuxMtkA9m0GmjEYh7TeDZJ3ntXaexH3s+IKFwEq2BsF</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><saml2:Subject xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\"><saml2:NameID Format=\"urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified\">yaroslav@kato.im</saml2:NameID><saml2:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"><saml2:SubjectConfirmationData NotOnOrAfter=\"2015-01-09T02:02:56.021Z\" Recipient=\"https://api.kato.im/saml/v2/demo-okta/consume\"/></saml2:SubjectConfirmation></saml2:Subject><saml2:Conditions NotBefore=\"2015-01-09T01:52:56.021Z\" NotOnOrAfter=\"2015-01-09T02:02:56.021Z\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\"><saml2:AudienceRestriction><saml2:Audience>https://api.kato.im/saml/v2/demo-okta/metadata</saml2:Audience></saml2:AudienceRestriction></saml2:Conditions><saml2:AuthnStatement AuthnInstant=\"2015-01-09T01:57:56.021Z\" SessionIndex=\"id1420768676021.697992970\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\"><saml2:AuthnContext><saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport</saml2:AuthnContextClassRef></saml2:AuthnContext></saml2:AuthnStatement></saml2:Assertion></saml2p:Response>", [{namespace_conformant, true}]),
    ok = verify(Doc),
    ok = verify(Doc, [<<219,7,85,249,71,184,75,241,1,217,88,92,235,58,17,143,84,113,64,215>>]).
    % ok.

verify_invalid_test() ->
    {Doc, _} = xmerl_scan:string("<x:foo xmlns:x=\"urn:foo:x:\"><x:name>blah</x:name></x:foo>", [{namespace_conformant, true}]),
    {'EXIT', _} = (catch verify(Doc)).

verify_unknown_cert_test() ->
    {Doc, _} = xmerl_scan:string("<?xml version=\"1.0\"?><x:foo ID=\"9616e6c0-f525-11b7-afb7-5cf9dd711ed3\" xmlns:x=\"urn:foo:x:\"><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#rsa-sha1\"/><ds:Reference URI=\"#9616e6c0-f525-11b7-afb7-5cf9dd711ed3\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>xPVYXCs5uMMmIbfTiTZ5R5DVhTU=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>rYk+WAghakHfR9VtpLz3AkMD1xLD1wISfNgch9+i+PC72RqhmfeMCZMkBaw0EO+CTKEoFBQIQaJYlEj8rIG+XN+8HyBV75BrMKZs1rdN+459Rpn2FOOJuHVb2jLDPecC9Ok/DGaNu6lol60hG9di66EZkL8ErQCuCeZqiw9tiXMUPQyVa2GxqT2UeXvJ5YtkNMDweUc3HhEnTG3ovYt1vOZt679w4N0HAwUa9rk40Z12fOTx77BbMICZ9Q4N2m3UbaFU24YHYpHR+WUTiwzXcmdkrHiE5IF37h7rTKAEixD2bTojaefmrobAz0+mBhCqBPcbfNLhLrpT43xhMenjpA==</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIIDfTCCAmWgAwIBAgIJANCSQXrTqpDjMA0GCSqGSIb3DQEBBQUAMFUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApRdWVlbnNsYW5kMREwDwYDVQQHDAhCcmlzYmFuZTEMMAoGA1UECgwDRm9vMRAwDgYDVQQDDAdzYW1saWRwMB4XDTEzMDQyOTA2MTAyOVoXDTIzMDQyOTA2MTAyOVowVTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClF1ZWVuc2xhbmQxETAPBgNVBAcMCEJyaXNiYW5lMQwwCgYDVQQKDANGb28xEDAOBgNVBAMMB3NhbWxpZHAwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDFhBuEO3fX+FlyT2YYzozxmXNXEmQjksigJSKD4hvsgsyGyl1iLkqNT6IbkuMXoyJG6vXufMNVLoktcLBd6eu6LQwwRjSU62AVCWZhIJP8U6lHqVsxiP90h7/b1zM7Hm9uM9RHtG+nKB7W0xNRihG8BUQOocSaLIMZZXqDPW1h/UvUqmpEzCtT0kJyXX0UAmDHzTYWHt8dqOYdcO2RAlJX0UKnwG1bHjTAfw01lJeOZiF66kH777nStYSElrHXr0NmCO/2gt6ouEnnUqJWDWRzaLbzhMLmGj83lmPgwZCBbIbnbQWLYPQ438EWfEYELq9nSQrgfUmmDPb4rtsQOXqZAgMBAAGjUDBOMB0GA1UdDgQWBBT64y2JSqY96YTYv1QbFyCPp3To/zAfBgNVHSMEGDAWgBT64y2JSqY96YTYv1QbFyCPp3To/zAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQAecr+C4w3LYAU4pCbLAW2BbFWGZRqBAr6ZKZKQrrqSMUJUiRDoKc5FYJrkjl/sGHAe3b5vBrU3lb/hpSiKXVf4/zBP7uqAF75B6LwnMwYpPcXlnRyPngQcdTL5EyQT5vwqv+H3zB64TblMYbsvqm6+1ippRNq4IXQX+3NGTEkhh0xgH+e3wE8BjjiygDu0MqopaIVPemMVQIm3HI+4jmf60bz8GLD1J4dj5CvyW1jQCXu2K2fcS1xJS0FLrxh/QxR0+3prGkYiZeOWE/dHlTTvQLB+NftyamUthVxMFe8dvXMTix/egox+ps2NuO2XTkDaeeRFjUhPhS8SvZO9l0lZ</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><x:name>blah</x:name></x:foo>", [{namespace_conformant, true}]),
    {error, cert_not_accepted} = verify(Doc, [<<198>>]).

verify_bad_digest_test() ->
    {Doc, _} = xmerl_scan:string("<?xml version=\"1.0\"?><x:foo ID=\"9616e6c0-f525-11b7-afb7-5cf9dd711ed3\" xmlns:x=\"urn:foo:x:\"><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#rsa-sha1\"/><ds:Reference URI=\"#9616e6c0-f525-11b7-afb7-5cf9dd711ed3\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>xPVYXCs5uMMmIbfTiTZ5R5DVhTU=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue></ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate></ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><x:name>b1ah</x:name></x:foo>", [{namespace_conformant, true}]),
    {error, bad_digest} = verify(Doc).

verify_bad_signature_test() ->
    {Doc, _} = xmerl_scan:string("<?xml version=\"1.0\"?><x:foo ID=\"9616e6c0-f525-11b7-afb7-5cf9dd711ed3\" xmlns:x=\"urn:foo:x:\"><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#rsa-sha1\"/><ds:Reference URI=\"#9616e6c0-f525-11b7-afb7-5cf9dd711ed3\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>FzMI9JNIp2IYjB5pnReqi+khe1k=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>rYk+WAghakHfR9VtpLz3AkMD1xLD1wISfNgch9+i+PC72RqhmfeMCZMkBaw0EO+CTKEoFBQIQaJYlEj8rIG+XN+8HyBV75BrMKZs1rdN+459Rpn2FOOJuHVb2jLDPecC9Ok/DGaNu6lol60hG9di66EZkL8ErQCuCeZqiw9tiXMUPQyVa2GxqT2UeXvJ5YtkNMDweUc3HhEnTG3ovYt1vOZt679w4N0HAwUa9rk40Z12fOTx77BbMICZ9Q4N2m3UbaFU24YHYpHR+WUTiwzXcmdkrHiE5IF37h7rTKAEixD2bTojaefmrobAz0+mBhCqBPcbfNLhLrpT43xhMenjpA==</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIIDfTCCAmWgAwIBAgIJANCSQXrTqpDjMA0GCSqGSIb3DQEBBQUAMFUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApRdWVlbnNsYW5kMREwDwYDVQQHDAhCcmlzYmFuZTEMMAoGA1UECgwDRm9vMRAwDgYDVQQDDAdzYW1saWRwMB4XDTEzMDQyOTA2MTAyOVoXDTIzMDQyOTA2MTAyOVowVTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClF1ZWVuc2xhbmQxETAPBgNVBAcMCEJyaXNiYW5lMQwwCgYDVQQKDANGb28xEDAOBgNVBAMMB3NhbWxpZHAwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDFhBuEO3fX+FlyT2YYzozxmXNXEmQjksigJSKD4hvsgsyGyl1iLkqNT6IbkuMXoyJG6vXufMNVLoktcLBd6eu6LQwwRjSU62AVCWZhIJP8U6lHqVsxiP90h7/b1zM7Hm9uM9RHtG+nKB7W0xNRihG8BUQOocSaLIMZZXqDPW1h/UvUqmpEzCtT0kJyXX0UAmDHzTYWHt8dqOYdcO2RAlJX0UKnwG1bHjTAfw01lJeOZiF66kH777nStYSElrHXr0NmCO/2gt6ouEnnUqJWDWRzaLbzhMLmGj83lmPgwZCBbIbnbQWLYPQ438EWfEYELq9nSQrgfUmmDPb4rtsQOXqZAgMBAAGjUDBOMB0GA1UdDgQWBBT64y2JSqY96YTYv1QbFyCPp3To/zAfBgNVHSMEGDAWgBT64y2JSqY96YTYv1QbFyCPp3To/zAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQAecr+C4w3LYAU4pCbLAW2BbFWGZRqBAr6ZKZKQrrqSMUJUiRDoKc5FYJrkjl/sGHAe3b5vBrU3lb/hpSiKXVf4/zBP7uqAF75B6LwnMwYpPcXlnRyPngQcdTL5EyQT5vwqv+H3zB64TblMYbsvqm6+1ippRNq4IXQX+3NGTEkhh0xgH+e3wE8BjjiygDu0MqopaIVPemMVQIm3HI+4jmf60bz8GLD1J4dj5CvyW1jQCXu2K2fcS1xJS0FLrxh/QxR0+3prGkYiZeOWE/dHlTTvQLB+NftyamUthVxMFe8dvXMTix/egox+ps2NuO2XTkDaeeRFjUhPhS8SvZO9l0lZ</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><x:name>b1ah</x:name></x:foo>", [{namespace_conformant, true}]),
    {error, bad_signature} = verify(Doc).

test_sign_key() ->
    CertBin = <<48,130,1,173,48,130,1,103,160,3,2,1,2,2,9,0,155,15,116,226,54,
                     209,145,118,48,13,6,9,42,134,72,134,247,13,1,1,5,5,0,48,66,49,
                     11,48,9,6,3,85,4,6,19,2,88,88,49,21,48,19,6,3,85,4,7,12,12,68,
                     101,102,97,117,108,116,32,67,105,116,121,49,28,48,26,6,3,85,4,
                     10,12,19,68,101,102,97,117,108,116,32,67,111,109,112,97,110,
                     121,32,76,116,100,48,30,23,13,49,51,48,53,48,50,48,54,48,48,51,
                     52,90,23,13,50,51,48,53,48,50,48,54,48,48,51,52,90,48,66,49,11,
                     48,9,6,3,85,4,6,19,2,88,88,49,21,48,19,6,3,85,4,7,12,12,68,101,
                     102,97,117,108,116,32,67,105,116,121,49,28,48,26,6,3,85,4,10,
                     12,19,68,101,102,97,117,108,116,32,67,111,109,112,97,110,121,
                     32,76,116,100,48,76,48,13,6,9,42,134,72,134,247,13,1,1,1,5,0,3,
                     59,0,48,56,2,49,0,205,22,207,74,179,213,185,209,141,250,249,
                     250,90,172,216,115,36,248,202,38,35,250,140,203,148,166,140,
                     157,135,4,125,142,129,148,170,140,171,183,154,14,45,63,60,99,
                     68,109,247,155,2,3,1,0,1,163,80,48,78,48,29,6,3,85,29,14,4,22,
                     4,20,217,116,226,255,194,252,218,129,177,246,103,26,72,200,32,
                     122,187,222,157,58,48,31,6,3,85,29,35,4,24,48,22,128,20,217,
                     116,226,255,194,252,218,129,177,246,103,26,72,200,32,122,187,
                     222,157,58,48,12,6,3,85,29,19,4,5,48,3,1,1,255,48,13,6,9,42,
                     134,72,134,247,13,1,1,5,5,0,3,49,0,66,238,235,142,200,32,210,
                     110,101,63,239,197,154,4,128,26,192,193,3,10,250,95,242,106,
                     110,98,1,100,8,229,143,141,180,42,219,11,94,149,187,74,164,45,
                     37,79,228,71,103,175>>,
    Key = {'RSAPrivateKey','two-prime',
                                    31566101599917470453416065772975030637050267921499643485243561060280673467204714198784209398028051515492879184033691,
                                    65537,
                                    18573989898799417322963879097353191425554564320258643998367520268996258880659389403428515182780052189009731243940089,
                                    6176779427556368800436097873318862403597526763704995657789,
                                    5110446628398630915379329225736384395133647699411033691319,
                                    3629707330424811560529090457257061337677158715287651140161,
                                    3337927863271614430989022488622788202360360154126504237157,
                                    3289563093010152325531764796397097457944832648507910197015,
                                    asn1_NOVALUE},
    {Key, CertBin}.

test_sign_256_key() ->
    CertBin = <<48,130,2,88,48,130,1,193,160,3,2,1,2,2,9,0,143,6,244,72,167,203,103,249,48,
                     13,6,9,42,134,72,134,247,13,1,1,11,5,0,48,69,49,11,48,9,6,3,85,4,6,19,2,65,
                     85,49,19,48,17,6,3,85,4,8,12,10,83,111,109,101,45,83,116,97,116,101,49,33,48,
                     31,6,3,85,4,10,12,24,73,110,116,101,114,110,101,116,32,87,105,100,103,105,
                     116,115,32,80,116,121,32,76,116,100,48,30,23,13,49,53,48,49,48,57,48,53,53,
                     56,50,56,90,23,13,49,56,48,49,48,56,48,53,53,56,50,56,90,48,69,49,11,48,9,6,
                     3,85,4,6,19,2,65,85,49,19,48,17,6,3,85,4,8,12,10,83,111,109,101,45,83,116,97,
                     116,101,49,33,48,31,6,3,85,4,10,12,24,73,110,116,101,114,110,101,116,32,87,
                     105,100,103,105,116,115,32,80,116,121,32,76,116,100,48,129,159,48,13,6,9,42,
                     134,72,134,247,13,1,1,1,5,0,3,129,141,0,48,129,137,2,129,129,0,226,96,97,235,
                     98,1,16,138,195,252,131,198,89,74,61,140,212,78,159,123,99,28,153,153,53,193,
                     67,109,72,5,148,219,215,43,114,158,115,146,245,138,110,187,86,167,232,15,75,
                     90,39,50,192,75,180,64,97,107,84,135,124,189,87,96,62,133,63,147,146,200,97,
                     209,193,17,186,23,41,243,247,94,51,116,64,104,108,253,157,152,31,189,28,67,
                     24,20,12,216,67,144,186,216,245,111,142,219,106,11,59,106,147,184,89,104,55,
                     80,79,112,40,181,99,211,254,130,151,2,109,137,153,40,216,255,2,3,1,0,1,163,
                     80,48,78,48,29,6,3,85,29,14,4,22,4,20,226,28,15,2,132,199,176,227,86,54,191,
                     35,102,122,246,50,138,160,135,239,48,31,6,3,85,29,35,4,24,48,22,128,20,226,
                     28,15,2,132,199,176,227,86,54,191,35,102,122,246,50,138,160,135,239,48,12,6,
                     3,85,29,19,4,5,48,3,1,1,255,48,13,6,9,42,134,72,134,247,13,1,1,11,5,0,3,129,
                     129,0,205,96,78,143,187,166,157,119,160,185,177,84,220,232,121,254,52,50,111,
                     54,114,42,132,147,98,202,12,7,194,120,234,67,26,218,126,193,245,72,75,95,224,
                     211,23,244,240,57,207,46,99,142,76,218,100,184,132,172,34,73,193,145,142,72,
                     53,165,23,144,255,102,86,99,42,254,82,107,53,119,240,62,200,212,83,220,57,80,
                     230,146,109,43,211,31,166,82,178,55,114,110,148,164,247,254,162,135,126,157,
                     123,185,30,146,185,60,125,234,98,188,205,109,134,74,58,230,84,245,87,233,232,
                     133,5,2>>,
    Key = {'RSAPrivateKey', 'two-prime',
                                    158966980232852666772927195913239826068125056530979279609712979168793279569950881734703825673400914686519075266453462906345312980842795804140929898282998881309114359443174166979208804324900933216050217378336424610098894747923637370129796798783736195833452722831496313972485597624172644388752444143966442019071,
                                    65537,
                                    81585278241787073666896657377387148477980168094656271566789692148593343582026914676392925775132211811359523575799353416465883426318681613016771856031686932947271317419547861320644294073546214321361245588222429356422579589512434099189282561422126611592192445638395200306602306031474495398876927483244443369593,
                                    12815152123986810526369994227491082588178787406540561310765978351462418958697931052574961306076834858513248417634296430722377133684866082077619514584491459,
                                    12404611251965211323458298415076779598256259333742031592133644354834252221601927657224330177651511823990769238743820731690160529549534378492093966021787669,
                                    12713470949925240093275522448216850277486308815036508762104942467263257296453352812079684136246663289377845680597663167924634849028624106358859697266275251,
                                    6810924077860081545742457087875899675964008664805732102649450821129373208143854079642954317600927742717607462760847234526126256852014054284747688684682049,
                                    4159324767638175662417764641421395971040638684938277905991804960733387537828956767796004537366153684030130407445292440219293856342103196426697248208199489,
                                    asn1_NOVALUE},
    {Key, CertBin}.


sign_and_verify_test() ->
    {Doc, _} = xmerl_scan:string("<x:foo id=\"test\" xmlns:x=\"urn:foo:x:\"><x:name>blah</x:name></x:foo>", [{namespace_conformant, true}]),
    {Key, CertBin} = test_sign_key(),
    SignedXml = sign(Doc, Key, CertBin, "http://www.w3.org/2000/09/xmldsig#rsa-sha1"),
    Doc = strip(SignedXml),
    false = (Doc =:= SignedXml),
    ok = verify(SignedXml, [crypto:hash(sha, CertBin)]).

sign_and_verify_sha256_test() ->
    {Doc, _} = xmerl_scan:string("<x:foo id=\"test\" xmlns:x=\"urn:foo:x:\"><x:name>blah</x:name></x:foo>", [{namespace_conformant, true}]),
    {Key, CertBin} = test_sign_256_key(),
    SignedXml = sign(Doc, Key, CertBin, rsa_sha256),
    Doc = strip(SignedXml),
    false = (Doc =:= SignedXml),
    ok = verify(SignedXml, [crypto:hash(sha, CertBin)]).

sign_generate_id_test() ->
    {Doc, _} = xmerl_scan:string("<x:foo xmlns:x=\"urn:foo:x:\"><x:name>blah</x:name></x:foo>", [{namespace_conformant, true}]),
    {Key, CertBin} = test_sign_key(),
    SignedXml = sign(Doc, Key, CertBin, "http://www.w3.org/2000/09/xmldsig#rsa-sha1"),
    Ns = [{"ds", 'http://www.w3.org/2000/09/xmldsig#'}],
    [#xmlAttribute{name = 'ID', value = RootId}] = xmerl_xpath:string("@ID", SignedXml, [{namespace, Ns}]),
    [#xmlAttribute{value = "#" ++ RootId}] = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:Reference/@URI", SignedXml, [{namespace, Ns}]).

utf8_test() ->
    Name = <<208,152,208,179,208,190,209,128,209,140,32,208,154,
      208,176,209,128,209,139,208,188,208,190,208,178,32>>,
    ThisPerson = <<227,129,157,227,129,174,228,186,186,10>>,
    XmlData = <<"<x:foo xmlns:x=\"urn:foo:x#\"><x:name attr=\"",Name/binary,"\">",ThisPerson/binary,"</x:name></x:foo>">>,
    {Doc, _} = xmerl_scan:string(binary_to_list(XmlData), [{namespace_conformant, true}]),
    {Key, CertBin} = test_sign_key(),
    SignedXml = sign(Doc, Key, CertBin, "http://www.w3.org/2000/09/xmldsig#rsa-sha1"),
    Ns = [{"ds", 'http://www.w3.org/2000/09/xmldsig#'}, {"x", 'urn:foo:x#'}],
    [#xmlAttribute{name = 'ID', value = RootId}] = xmerl_xpath:string("@ID", SignedXml, [{namespace, Ns}]),
    [#xmlAttribute{value = "#" ++ RootId}] = xmerl_xpath:string("ds:Signature/ds:SignedInfo/ds:Reference/@URI", SignedXml, [{namespace, Ns}]),
    AttrValue = unicode:characters_to_list(Name),
    [#xmlAttribute{name = 'attr', value = AttrValue}] = xmerl_xpath:string("x:name/@attr", SignedXml, [{namespace, Ns}]),
    TextValue = unicode:characters_to_list(ThisPerson),
    [#xmlText{value = TextValue}] = xmerl_xpath:string("x:name/text()", SignedXml, [{namespace, Ns}]),
    ok = verify(SignedXml, [crypto:hash(sha, CertBin)]).

-endif.
