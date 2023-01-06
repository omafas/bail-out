%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc REST client for the menelaus application.

-module(menelaus_rest).
-author('Northscale <info@northscale.com>').

-include("ns_common.hrl").

%% API

-export([rest_url/3,
         rest_url/4,
         json_request_hilevel/4,
         basic_auth_header/1,
         special_auth_header/0,
         special_auth_header/1,
         is_auth_header/1,
         if_none_match_header/1,
         on_behalf_header/1]).

-spec rest_url(string(), string() | integer(), string(), string() | atom()) -> string().
rest_url(Host, Port, Path, Scheme) when is_atom(Scheme) ->
    rest_url(Host, Port, Path, atom_to_list(Scheme));
rest_url(Host, Port, Path, Scheme) when is_integer(Port) ->
    rest_url(Host, integer_to_list(Port), Path, Scheme);
rest_url(Host, Port, Path, Scheme) ->
    Scheme ++ "://" ++ misc:join_host_port(Host, Port) ++ Path.

rest_url(Host, Port, Path) ->
    rest_url(Host, Port, Path, "http").

basic_auth_header(HiddenAuth) ->
    {User, Password} = ?UNHIDE(HiddenAuth),
    UserPassword = base64:encode_to_string(User ++ ":" ++ Password),
    {"Authorization", "Basic " ++ UserPassword}.

if_none_match_header(Etag) ->
    {"If-None-Match", Etag}.
special_auth_header() ->
    special_auth_header(node()).
special_auth_header(Node) when is_atom(Node) ->
    basic_auth_header(?HIDE({ns_config_auth:get_user(special),
                             ns_config_auth:get_password(special)})).

on_behalf_header({User, Domain}) ->
    {"cb-on-behalf-of",
     base64:encode_to_string(User ++ ":" ++ atom_to_list(Domain))}.

is_auth_header(Header) when is_atom(Header) ->
    is_auth_header(atom_to_list(Header));
is_auth_header(Header) when is_list(Header) ->
    is_auth_header_lc(string:lowercase(Header)).

is_auth_header_lc("authorization") ->
    true;
is_auth_header_lc("cb-on-behalf-of") ->
    true;
is_auth_header_lc("ns-server-auth-token") ->
    true;
is_auth_header_lc("ns-server-ui") ->
    true;
is_auth_header_lc(_) ->
    false.


rest_add_auth(Headers, HiddenAuth) when is_function(HiddenAuth) ->
    [basic_auth_header(HiddenAuth) | Headers];
rest_add_auth(Headers, undefined) ->
    Headers.

rest_add_mime_type(Headers, undefined) ->
    Headers;
rest_add_mime_type(Headers, MimeType) ->
    [{"Content-Type", MimeType} | Headers].

rest_request(Method, URL, Headers, MimeType, Body, HiddenAuth, HTTPOptions) ->
    NewHeaders0 = rest_add_auth(Headers, HiddenAuth),
    NewHeaders = rest_add_mime_type(NewHeaders0, MimeType),
    Timeout = proplists:get_value(timeout, HTTPOptions, 30000),
    HTTPOptions1 = lists:keydelete(timeout, 1, HTTPOptions),
    HTTPOptions2 = add_tls_options(URL, HTTPOptions1),
    lhttpc:request(URL, Method, NewHeaders, Body, Timeout, HTTPOptions2).

add_tls_options("https://" ++ _, Options) ->
    ConnectOptions = proplists:get_value(connect_options, Options, []),
    TLSOptions =
        case ns_server_cert:this_node_uses_self_generated_certs() of
            true -> [];
            false -> ns_ssl_services_setup:ssl_client_opts()
        end,
    NewConnectOptions = misc:update_proplist(TLSOptions, ConnectOptions),
    misc:update_proplist(Options, [{connect_options, NewConnectOptions}]);
add_tls_options("http://" ++ _, Options) -> Options.

decode_json_response_ext({ok, {{200 = _StatusCode, _} = _StatusLine,
                               _Headers, Body} = _Result},
                         _Method, _Request) ->
    try mochijson2:decode(Body) of
        X -> {ok, X}
    catch
        Type:What:Stack ->
            {error, bad_json, <<"Malformed JSON response">>,
             {Type, What, Stack}}
    end;

decode_json_response_ext({ok, {{400 = _StatusCode, _} = _StatusLine,
                               _Headers, Body} = _Result} = Response,
                         Method, Request) ->
    try mochijson2:decode(Body) of
        X -> {client_error, X}
    catch
        _:_ ->
            ns_error_messages:decode_json_response_error(Response, Method, Request)
    end;

decode_json_response_ext(Response, Method, Request) ->
    ns_error_messages:decode_json_response_error(Response, Method, Request).

-spec json_request_hilevel(atom(),
                           {atom(), string(), string() | integer(), string(), string(), iolist()}
                           | {atom(), string(), string() | integer(), string()},
                           undefined | {string(), string()} | fun(() -> {string(), string()}),
                           [any()]) ->
                                  %% json response payload
                                  {ok, any()} |
                                  %% json payload of 400
                                  {client_error, term()} |
                                  %% english error message and nested error
                                  {error, rest_error, binary(), {error, term()} | {bad_status, integer(), string()}}.
json_request_hilevel(Method, {Scheme, Host, Port, Path, MimeType, Payload} = R,
                     HiddenAuth, HTTPOptions) ->
    RealPayload = binary_to_list(iolist_to_binary(Payload)),
    URL = rest_url(Host, Port, Path, Scheme),
    HTTPOptions1 = set_default_request_opts(HTTPOptions),
    RV = rest_request(Method, URL, [], MimeType, RealPayload, HiddenAuth,
                      HTTPOptions1),
    decode_json_response_ext(RV, Method, setelement(6, R, RealPayload));
json_request_hilevel(Method, {Scheme, Host, Port, Path}, HiddenAuth, HTTPOptions) ->
    URL = rest_url(Host, Port, Path, Scheme),
    HTTPOptions1 = set_default_request_opts(HTTPOptions),
    RV = rest_request(Method, URL, [], undefined, [], HiddenAuth, HTTPOptions1),

    decode_json_response_ext(RV, Method, {Scheme, Host, Port, Path, [], []}).

set_default_request_opts(Opts) ->
    misc:update_proplist([{connect_timeout, 30000}], Opts).
