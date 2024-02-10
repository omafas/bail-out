%% esaml - SAML for erlang
%%
%% Copyright (c) 2013, Alex Wilson and the University of Queensland
%% All rights reserved.
%%
%% Distributed subject to the terms of the 2-clause BSD license, see
%% the LICENSE file in the root of the distribution.

%% @doc Convenience functions for use with Cowboy handlers
%%
%% This module makes it easier to use esaml in your Cowboy-based web
%% application, by providing easy wrappers around the functions in
%% esaml_binding and esaml_sp.
-module(esaml_cowboy).

-include_lib("xmerl/include/xmerl.hrl").
-include("esaml.hrl").

-export([reply_with_authnreq/4, reply_with_authnreq/7, reply_with_metadata/2, reply_with_logoutreq/4, reply_with_logoutresp/5]).
-export([validate_assertion/2, validate_assertion/3, validate_assertion/5, validate_logout/2]).

-type uri() :: string().

%% @doc Reply to a Cowboy request with an AuthnRequest payload
%%
%% RelayState is an arbitrary blob up to 80 bytes long that will
%% be returned verbatim with any assertion that results from this
%% AuthnRequest.
-spec reply_with_authnreq(esaml:sp(), IdPSSOEndpoint :: uri(), RelayState :: binary(), Req) -> Req.
reply_with_authnreq(SP, IDP, RelayState, Req) ->
    reply_with_authnreq(SP, IDP, RelayState, Req, undefined, undefined, undefined).

%% @doc Reply to a Cowboy request with an AuthnRequest payload and calls the callback with the (signed?) XML
%%
%% Similar to reply_with_authnreq/4, but before replying - calls the callback with the (signed?) XML, allowing persistence and later validation.
%%
%% This function assumes redirect and POST bindings are the same
-type xml_callback_state()  :: any().
-type xml_callback_fun()    :: fun((#xmlElement{}, xml_callback_state()) -> any()).
-spec reply_with_authnreq(
    esaml:sp(),
    IdPSSOEndpoint :: uri(),
    RelayState :: binary(),
    Req,
    undefined | string(),
    undefined | xml_callback_fun(),
    undefined | xml_callback_state()) -> Req.
reply_with_authnreq(SP, IDP, RelayState, Req, User_Name_Id, Xml_Callback, Xml_Callback_State) ->
    SignedXml = esaml_sp:generate_authn_request(IDP, SP),
    is_function(Xml_Callback, 2) andalso Xml_Callback(SignedXml, Xml_Callback_State),
    reply_with_req(IDP, SignedXml, User_Name_Id, RelayState, Req).

%% @doc Reply to a Cowboy request with a LogoutRequest payload
%%
%% NameID should be the exact subject name from the assertion you
%% wish to log out.
%%
%% This function assumes redirect and POST bindings are the same
-spec reply_with_logoutreq(esaml:sp(), IdPSLOEndpoint :: uri(), NameID :: string(), Req) -> Req.
reply_with_logoutreq(SP, IDP, NameID, Req) ->
    SignedXml = esaml_sp:generate_logout_request(IDP, NameID, SP),
    reply_with_req(IDP, SignedXml, undefined, <<>>, Req).

%% @doc Reply to a Cowboy request with a LogoutResponse payload
%%
%% Be sure to keep the RelayState from the original LogoutRequest that you
%% received to allow the IdP to keep state.
%%
%% This function assumes redirect and POST bindings are the same
-spec reply_with_logoutresp(esaml:sp(), IdPSLOEndpoint :: uri(), esaml:status_code(), RelayState :: binary(), Req) -> Req.
reply_with_logoutresp(SP, IDP, Status, RelayState, Req) ->
    SignedXml = esaml_sp:generate_logout_response(IDP, Status, SP),
    reply_with_req(IDP, SignedXml, undefined, RelayState, Req).

%% @private
reply_with_req(IDP, SignedXml, Username, RelayState, Req) ->
    Target = esaml_binding:encode_http_redirect(IDP, SignedXml, Username, RelayState),
    UA = cowboy_req:header(<<"user-agent">>, Req, <<"">>),
    IsIE = not (binary:match(UA, <<"MSIE">>) =:= nomatch),
    if IsIE andalso (byte_size(Target) > 2042) ->
        Html = esaml_binding:encode_http_post(IDP, SignedXml, RelayState),
        cowboy_req:reply(200, #{
            <<"Cache-Control">> => <<"no-cache">>,
            <<"Pragma">> => <<"no-cache">>
        }, Html, Req);
    true ->
        cowboy_req:reply(302, #{
            <<"Cache-Control">> => <<"no-cache">>,
            <<"Pragma">> => <<"no-cache">>,
            <<"Location">> => Target
        }, <<"Redirecting...">>, Req)
    end.

%% @doc Validate and parse a LogoutRequest or LogoutResponse
%%
%% This function handles both REDIRECT and POST bindings.
-spec validate_logout(esaml:sp(), Req) ->
        {request, esaml:logoutreq(), RelayState::binary(), Req} |
        {response, esaml:logoutresp(), RelayState::binary(), Req} |
        {error, Reason :: term(), Req}.
validate_logout(SP, Req) ->
    Method = cowboy_req:method(Req),
    case Method of
        <<"POST">> ->
            {ok, PostVals, Req2} = cowboy_req:read_urlencoded_body(Req, #{length => 128000}),
            SAMLEncoding = proplists:get_value(<<"SAMLEncoding">>, PostVals),
            SAMLResponse = proplists:get_value(<<"SAMLResponse">>, PostVals,
                proplists:get_value(<<"SAMLRequest">>, PostVals)),
            RelayState = proplists:get_value(<<"RelayState">>, PostVals, <<>>),
            validate_logout(SP, SAMLEncoding, SAMLResponse, RelayState, Req2);
        <<"GET">> ->
            SAMLEncoding = cowboy_req:match_qs(['SAMLEncoding'], Req),
            SAMLResponse = cowboy_req:match_qs(
                [{'SAMLResponse', [], cowboy_req:match_qs(['SAMLRequest'], Req)}], Req),
            RelayState = cowboy_req:match_qs([{'RelayState', [], <<>>}], Req),
            validate_logout(SP, SAMLEncoding, SAMLResponse, RelayState, Req)
    end.

%% @private
validate_logout(SP, SAMLEncoding, SAMLResponse, RelayState, Req2) ->
    case (catch esaml_binding:decode_response(SAMLEncoding, SAMLResponse)) of
        {'EXIT', Reason} ->
            {error, {bad_decode, Reason}, Req2};
        Xml ->
            Ns = [{"samlp", 'urn:oasis:names:tc:SAML:2.0:protocol'},
                  {"saml", 'urn:oasis:names:tc:SAML:2.0:assertion'}],
            case xmerl_xpath:string("/samlp:LogoutRequest", Xml, [{namespace, Ns}]) of
                [#xmlElement{}] ->
                    case esaml_sp:validate_logout_request(Xml, SP) of
                        {ok, Reqq} -> {request, Reqq, RelayState, Req2};
                        Err -> Err
                    end;
                _ ->
                    case esaml_sp:validate_logout_response(Xml, SP) of
                        {ok, Resp} -> {response, Resp, RelayState, Req2};
                        Err -> Err
                    end
            end
    end.

%% @doc Reply to a Cowboy request with a Metadata payload
-spec reply_with_metadata(esaml:sp(), Req) -> Req.
reply_with_metadata(SP, Req) ->
    SignedXml = esaml_sp:generate_metadata(SP),
    Metadata = xmerl:export([SignedXml], xmerl_xml),
    cowboy_req:reply(200, #{<<"Content-Type">> => <<"text/xml">>}, Metadata, Req).

%% @doc Validate and parse an Assertion inside a SAMLResponse
%%
%% This function handles only POST bindings.
-spec validate_assertion(esaml:sp(), Req) ->
        {ok, esaml:assertion(), RelayState :: binary(), Req} |
        {error, Reason :: term(), Req}.
validate_assertion(SP, Req) ->
    validate_assertion(SP, fun(_A, _Digest) -> ok end, Req).

-spec validate_assertion(esaml:sp(), esaml_sp:dupe_fun(), Req) ->
    {ok, esaml:assertion(), RelayState :: binary(), Req} |
    {error, Reason :: term(), Req}.
validate_assertion(SP, DuplicateFun, Req) ->
    validate_assertion(SP, DuplicateFun, undefined, undefined, Req).

%% @doc Validate and parse an Assertion with duplicate detection
%%
%% This function handles only POST bindings.
%%
%% For the signature of DuplicateFun, see esaml_sp:validate_assertion/3
-type custom_security_callback() :: fun((#xmlElement{}, esaml:assertion(), custom_security_callback_state()) -> ok | {error, any()}).
-type custom_security_callback_state() :: any().

-spec validate_assertion(
    esaml:sp(),
    esaml_sp:dupe_fun(),
    undefined | custom_security_callback(),
    undefined | custom_security_callback_state(),
    Req) ->
    {ok, esaml:assertion(), RelayState :: binary(), Req} |
    {error, Reason :: term(), Req}.
validate_assertion(SP, DuplicateFun, Custom_Response_Security_Callback, Callback_State, Req) ->
    {ok, PostVals, Req2} = cowboy_req:read_urlencoded_body(Req, #{length => 128000}),
    SAMLEncoding = proplists:get_value(<<"SAMLEncoding">>, PostVals),
    SAMLResponse = proplists:get_value(<<"SAMLResponse">>, PostVals),
    RelayState = proplists:get_value(<<"RelayState">>, PostVals),

    case (catch esaml_binding:decode_response(SAMLEncoding, SAMLResponse)) of
        {'EXIT', Reason} ->
            {error, {bad_decode, Reason}, Req2};
        Xml ->
            case esaml_sp:validate_assertion(Xml, DuplicateFun, SP) of
                {ok, A}     -> perform_extra_security_if_applicable(Custom_Response_Security_Callback, Callback_State, Xml, A, RelayState, Req2);
                {error, E}  -> {error, E, Req2}
            end
    end.

perform_extra_security_if_applicable(undefined, _Callback_State, _Xml, Assertion, RelayState, Req) ->
    {ok, Assertion, RelayState, Req};
perform_extra_security_if_applicable(Callback,   Callback_State,  Xml, Assertion, RelayState, Req) when is_function(Callback, 3) ->
    case Callback(Xml, Assertion, Callback_State) of
        ok          -> {ok, Assertion, RelayState, Req};
        {error, E}  -> {error, E, Req}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
