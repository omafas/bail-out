%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(rest_utils).

-include("ns_common.hrl").

-export([request/6,
         request/7,
         get_json_local/4,
         get_json_local/5]).

request(Type, URL, Method, Headers, Body, Timeout) ->
    request(Type, URL, Method, Headers, Body, Timeout, []).

request(Type, URL, Method, Headers, Body, Timeout, Options) ->
    Start = os:timestamp(),
    DefaultOptions = [{pool, rest_lhttpc_pool}],
    RV = lhttpc:request(URL, Method, Headers, Body, Timeout,
                        misc:update_proplist(DefaultOptions, Options)),
    case RV of
        {ok, {{Code, _}, _, _}} ->
            Diff = timer:now_diff(os:timestamp(), Start),
            ns_server_stats:notify_histogram(
              {<<"outgoing_http_requests">>, [{type, Type}]}, Diff div 1000),

            ns_server_stats:notify_counter(
              {<<"outgoing_http_requests">>, [{code, Code}, {type, Type}]});
        _ ->
            ns_server_stats:notify_counter(
              {<<"outgoing_http_requests">>, [{code, "error"}, {type, Type}]})
    end,

    RV.

request_local(Type, URL, Method, Headers, Body, Timeout) ->
    HeadersWithAuth = [menelaus_rest:special_auth_header() | Headers],

    request(Type, URL, Method, HeadersWithAuth, Body, Timeout).

get_json(Type, URL, Path, Timeout, ReqHeaders) ->
    RV = request_local(Type, URL, "GET", ReqHeaders, [], Timeout),
    case RV of
        {ok, {{200, _}, Headers, BodyRaw}} ->
            try
                {ok, Headers, ejson:decode(BodyRaw)}
            catch
                T:E ->
                    ?log_error("Received bad json in response from (~p) ~s: ~p",
                               [Type, Path, {T, E}]),
                    {error, bad_json}
            end;
        {ok, {{304, _}, Headers, <<>> = Body}} ->
            {ok, Headers, Body};
        _ ->
            ?log_error("Request to (~p) ~s with headers ~p failed: ~p",
                       [Type, Path, ReqHeaders, RV]),
            {error, RV}
    end.

-spec get_json_local(atom(), string(), integer(), integer()) ->
    {ok, [{any(), any()}], any()} | {error, any()}.
get_json_local(Type, Path, Port, Timeout) ->
    get_json_local(Type, Path, Port, Timeout, []).

-spec get_json_local(atom(), string(), integer(), integer(),
                     [{any(), any()}]) -> {ok, [{any(), any()}], any()} |
                                          {error, any()}.
get_json_local(Type, Path, Port, Timeout, Headers) ->
    URL = misc:local_url(Port, Path, []),
    get_json(Type, URL, Path, Timeout, Headers).
