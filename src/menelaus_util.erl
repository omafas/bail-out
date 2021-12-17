%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2020 Couchbase, Inc.
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

-module(menelaus_util).
-author('Northscale <info@northscale.com>').

-include("ns_common.hrl").
-include("menelaus_web.hrl").
-include("pipes.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([redirect_permanently/2,
         reply/2,
         reply/3,
         reply/4,
         reply_ok/3,
         reply_ok/4,
         reply_text/3,
         reply_text/4,
         reply_json/2,
         reply_json/3,
         reply_json/4,
         parse_json/1,
         reply_not_found/1,
         serve_file/3,
         serve_file/4,
         serve_static_file/4,
         parse_boolean/1,
         get_option/2,
         local_addr/1,
         concat_url_path/1,
         concat_url_path/2,
         bin_concat_path/1,
         bin_concat_path/2,
         parse_validate_boolean_field/3,
         parse_validate_number/3,
         parse_validate_number/4,
         parse_validate_port_number/1,
         validate_email_address/1,
         encode_json/1,
         is_valid_positive_integer/1,
         is_valid_positive_integer_in_range/3,
         format_server_time/1,
         format_server_time/2,
         ensure_local/1,
         reply_global_error/2,
         reply_error/3,
         require_auth/1,
         send_chunked/3,
         handle_streaming/2,
         assert_is_enterprise/0,
         assert_is_55/0,
         assert_is_65/0,
         assert_is_66/0,
         strip_json_struct/1,
         choose_node_consistently/2,
         compute_sec_headers/0]).

%% used by parse_validate_number
-export([list_to_integer/1, list_to_float/1]).

%% for hibernate
-export([handle_streaming_wakeup/4]).

%% External API

-define(CACHE_CONTROL, "Cache-Control").  %% TODO: Move to an HTTP header file.
-define(BASE_HEADERS, [{"Server", "Couchbase Server"}]).
-define(SEC_HEADERS,  [{"X-Content-Type-Options", "nosniff"},
                       {"X-Frame-Options", "DENY"},
                       {"X-Permitted-Cross-Domain-Policies", "none"},
                       {"X-XSS-Protection", "1; mode=block"}]).
-define(NO_CACHE_HEADERS,
        [{?CACHE_CONTROL, "no-cache,no-store,must-revalidate"},
         {"Expires", "Thu, 01 Jan 1970 00:00:00 GMT"},
         {"Pragma", "no-cache"}]).

compute_sec_headers() ->
    compute_sec_headers(ns_config:read_key_fast(secure_headers, [])).

compute_sec_headers(Headers) ->
    case proplists:get_value(enabled, Headers, true) of
        false ->
            [];
        true ->
            H1 = misc:update_proplist(?SEC_HEADERS, Headers),
            [{K, V} || {K, V} <- H1, V =/= disable, K =/= enabled]
    end.

response_headers(Req, Headers) ->
    response_headers(
      scram_sha:get_resp_headers_from_req(Req) ++ Headers).

%% response_header takes a proplist of headers or pseudo-header
%% descripts and augments it with response specific headers.
%% Since any given header can only be specified once, headers at the front
%% of the proplist have priority over the same header later in the
%% proplist.
%% The following pseudo-headers are supported:
%%   {allow_cache, true}  -- Enables long duration caching
%%   {allow_cache, false} -- Disables cache via multiple headers
%% If neither allow_cache or the "Cache-Control" header are specified
%% {allow_cache, false} is applied.
-spec response_headers([{string(),string()}|{atom(), atom()}]) ->
                              [{string(), string()}].
response_headers(Headers) ->
    {Expanded, _} =
        lists:foldl(
          fun({allow_cache, _}, {Acc, _CacheControl = true}) ->
                  {Acc, true};
             ({allow_cache, _Value = true}, {Acc, _}) ->
                  {[{?CACHE_CONTROL, "max-age=30000000"} | Acc], true};
             ({allow_cache, _Value = false}, {Acc, _}) ->
                  {?NO_CACHE_HEADERS ++ Acc, true};
             ({Header = ?CACHE_CONTROL, Value}, {Acc, _}) ->
                  {[{Header, Value} | Acc], true};
             ({Header, Value}, {Acc, CacheControl}) when is_list(Header) ->
                  {[{Header, Value} | Acc], CacheControl}
          end, {[], false},
          Headers ++ [{allow_cache, false} | ?BASE_HEADERS] ++
              compute_sec_headers()),
    lists:ukeysort(1, lists:reverse(Expanded)).

%% mostly extracted from mochiweb_request:maybe_redirect/3
redirect_permanently(Path, Req) ->
    Scheme = case mochiweb_request:get(socket, Req) of
                 {ssl, _} ->
                     "https://";
                 _ ->
                     "http://"
             end,
    Location =
        case mochiweb_request:get_header_value("host", Req) of
            undefined -> Path;
            X -> Scheme ++ X ++ Path
        end,
    LocationBin = list_to_binary(Location),
    Top = <<"<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">"
           "<html><head>"
           "<title>301 Moved Permanently</title>"
           "</head><body>"
           "<h1>Moved Permanently</h1>"
           "<p>The document has moved <a href=\"">>,
    Bottom = <<">here</a>.</p></body></html>\n">>,
    Body = <<Top/binary, LocationBin/binary, Bottom/binary>>,
    reply(Req, Body, 301, [{"Location", Location},
                           {"Content-Type", "text/html"}]).

reply_not_found(Req) ->
    reply_not_found(Req, []).

reply_not_found(Req, ExtraHeaders) ->
    reply_text(Req, "Requested resource not found.\r\n", 404, ExtraHeaders).

reply_text(Req, Message, Code) ->
    reply_text(Req, Message, Code, []).

reply_text(Req, Message, Code, ExtraHeaders) ->
    reply(Req, Message, Code, [{"Content-Type", "text/plain"} | ExtraHeaders]).

reply_json(Req, Body) ->
    reply_ok(Req, "application/json", encode_json(Body)).

reply_json(Req, Body, Code) ->
    reply(Req, encode_json(Body), Code, [{"Content-Type", "application/json"}]).

reply_json(Req, Body, Code, ExtraHeaders) ->
    reply(Req, encode_json(Body), Code,
          [{"Content-Type", "application/json"} | ExtraHeaders]).

log_web_hit(Peer, Req, Resp) ->
    Level = case menelaus_auth:get_user_id(Req) of
                [$@ | _] ->
                    debug;
                _ ->
                    info
            end,
    try
        ale:xlog(?ACCESS_LOGGER, Level, {Peer, Req, Resp}, "", [])
    catch
        error:undef ->
            ok
    end.

reply_ok(Req, ContentType, Body) ->
    reply_ok(Req, ContentType, Body, []).

reply_ok(Req, ContentType, Body, ExtraHeaders) ->
    Peer = mochiweb_request:get(peer, Req),
    Resp = mochiweb_request:ok(
             {ContentType, response_headers(Req, ExtraHeaders), Body}, Req),
    log_web_hit(Peer, Req, Resp),
    Resp.

reply(Req, Code) ->
    reply(Req, [], Code, []).

reply(Req, Code, ExtraHeaders) ->
    reply(Req, [], Code, ExtraHeaders).

reply(Req, Body, Code, ExtraHeaders) ->
    respond(Req, {Code, response_headers(Req, ExtraHeaders), Body}).

respond(Req, RespTuple) ->
    Peer = mochiweb_request:get(peer, Req),
    Resp = mochiweb_request:respond(RespTuple, Req),
    log_web_hit(Peer, Req, Resp),
    Resp.

-include_lib("kernel/include/file.hrl").

%% Originally from mochiweb_request.erl maybe_serve_file/2
%% and modified to handle user-defined content-type
serve_static_file(Req, {DocRoot, Path}, ContentType, ExtraHeaders) ->
    serve_static_file(Req, filename:join(DocRoot, Path),
                      ContentType, ExtraHeaders);
serve_static_file(Req, File, ContentType, ExtraHeaders) ->
    case file:read_file_info(File) of
        {ok, FileInfo} ->
            LastModified = httpd_util:rfc1123_date(FileInfo#file_info.mtime),
            case mochiweb_request:get_header_value("if-modified-since", Req) of
                LastModified ->
                    reply(Req, 304, ExtraHeaders);
                _ ->
                    case file:open(File, [raw, binary]) of
                        {ok, IoDevice} ->
                            Res = reply_ok(Req, ContentType,
                                           {file, IoDevice},
                                           [{"last-modified", LastModified}
                                            | ExtraHeaders]),
                            file:close(IoDevice),
                            Res;
                        _ ->
                            reply_not_found(Req, ExtraHeaders)
                    end
            end;
        {error, _} ->
            reply_not_found(Req, ExtraHeaders)
    end.

serve_file(Req, File, Root) ->
    serve_file(Req, File, Root, []).

serve_file(Req, File, Root, ExtraHeaders) ->
    Peer = mochiweb_request:get(peer, Req),
    Resp = mochiweb_request:serve_file(
             File, Root,
             response_headers(Req, ExtraHeaders ++ [{allow_cache, true}]), Req),
    log_web_hit(Peer, Req, Resp),
    Resp.

get_option(Option, Options) ->
    {proplists:get_value(Option, Options),
     proplists:delete(Option, Options)}.

parse_json(Req) ->
    mochijson2:decode(mochiweb_request:recv_body(Req)).

parse_boolean(Value) ->
    case Value of
        true -> true;
        false -> false;
        <<"true">> -> true;
        <<"false">> -> false;
        <<"1">> -> true;
        <<"0">> -> false;
        1 -> true;
        0 -> false
    end.

url_path_iolist(Path, Props) when is_binary(Path) ->
    do_url_path_iolist(Path, Props);
url_path_iolist(Segments, Props) ->
    Path = [[$/, mochiweb_util:quote_plus(S)] || S <- Segments],
    do_url_path_iolist(Path, Props).

do_url_path_iolist(Path, Props) ->
    case Props of
        [] ->
            Path;
        _ ->
            QS = mochiweb_util:urlencode(Props),
            [Path, $?, QS]
    end.

concat_url_path(Segments) ->
    concat_url_path(Segments, []).
concat_url_path(Segments, Props) ->
    lists:flatten(url_path_iolist(Segments, Props)).

bin_concat_path(Segments) ->
    bin_concat_path(Segments, []).
bin_concat_path(Segments, Props) ->
    iolist_to_binary(url_path_iolist(Segments, Props)).

parse_validate_boolean_field(JSONName, CfgName, Params) ->
    case proplists:get_value(JSONName, Params) of
        undefined -> [];
        "true" -> [{ok, CfgName, true}];
        "false" -> [{ok, CfgName, false}];
        _ -> [{error, JSONName,
               iolist_to_binary(io_lib:format("~s is invalid", [JSONName]))}]
    end.

-spec parse_validate_number(string(), (integer() | undefined),
                            (integer() | undefined)) ->
                                   invalid | too_small | too_large |
                                   {ok, integer()}.
parse_validate_number(String, Min, Max) ->
    parse_validate_number(String, Min, Max, list_to_integer).

list_to_integer(A) -> erlang:list_to_integer(A).

list_to_float(A) -> try erlang:list_to_integer(A)
                    catch _:_ ->
                            erlang:list_to_float(A)
                    end.

-spec parse_validate_number(string(), (number() | undefined),
                            (number() | undefined),
                            list_to_integer | list_to_float) ->
                                   invalid | too_small | too_large |
                                   {ok, integer()}.
parse_validate_number(String, Min, Max, Fun) ->
    Parsed = (catch menelaus_util:Fun(string:strip(String))),
    if
        is_number(Parsed) ->
            if
                Min =/= undefined andalso Parsed < Min -> too_small;
                Max =/= undefined andalso Max =/= infinity andalso
                  Parsed > Max -> too_large;
                true -> {ok, Parsed}
            end;
       true -> invalid
    end.

parse_validate_port_number(StringPort) ->
    case parse_validate_number(StringPort, 1024, 65535) of
        {ok, Port} ->
            Port;
        invalid ->
            throw({error, [<<"Port must be a number.">>]});
        _ ->
            throw({error,
                   [<<"The port number must be greater than 1023 and less "
                      "than 65536.">>]})
    end.

%% does a simple email address validation
validate_email_address(Address) ->
    %%" "hm, even erlang-mode is buggy :("),
    {ok, RE} = re:compile("^[^@]+@.+$", [multiline]),
    RV = re:run(Address, RE),
    case RV of
        {match, _} -> true;
        _ -> false
    end.

%% Extract the local address of the socket used for the request
local_addr(Req) ->
    Socket = mochiweb_request:get(socket, Req),
    Address = case Socket of
                  {ssl, SSLSock} ->
                      {ok, {AV, _Port}} = ssl:sockname(SSLSock),
                      AV;
                  _ ->
                      {ok, {AV, _Port}} = inet:sockname(Socket),
                      AV
              end,
    misc:maybe_add_brackets(inet:ntoa(Address)).

strip_json_struct({struct, Pairs}) -> {strip_json_struct(Pairs)};
strip_json_struct(List) when is_list(List) ->
    [strip_json_struct(E) || E <- List];
strip_json_struct({Key, Value}) -> {Key, strip_json_struct(Value)};
strip_json_struct(Other) -> Other.

encode_json(JSON) ->
    Stripped = try strip_json_struct(JSON)
               catch T1:E1 ->
                       ?log_debug("errored while stripping:~n~p", [JSON]),
                       Stack1 = erlang:get_stacktrace(),
                       erlang:raise(T1, E1, Stack1)
               end,

    try
        ejson:encode(Stripped)
    catch T:E ->
            ?log_debug("errored while sending:~n~p~n->~n~p", [JSON, Stripped]),
            Stack = erlang:get_stacktrace(),
            erlang:raise(T, E, Stack)
    end.

is_valid_positive_integer(String) ->
    Int = (catch erlang:list_to_integer(String)),
    (is_integer(Int) andalso (Int > 0)).

is_valid_positive_integer_in_range(String, Min, Max) ->
    Int = (catch erlang:list_to_integer(String)),
    (is_integer(Int) andalso (Int >= Min) andalso (Int =< Max)).

format_server_time(DateTime) ->
    format_server_time(DateTime, 0).

format_server_time({{YYYY, MM, DD}, {Hour, Min, Sec}}, MicroSecs) ->
    list_to_binary(
      io_lib:format("~4.4.0w-~2.2.0w-~2.2.0wT~2.2.0w:~2.2.0w:~2.2.0w.~3.3.0wZ",
                    [YYYY, MM, DD, Hour, Min, Sec, MicroSecs div 1000])).

ensure_local(Req) ->
    Host = mochiweb_request:get(peer, Req),
    case misc:is_localhost(Host) of
        true ->
            ok;
        false ->
            erlang:throw({web_exception, 400,
                          <<"API is accessible from localhost only">>, []})
    end.

reply_global_error(Req, Error) ->
    reply_error(Req, "_", Error).

reply_error(Req, Field, Error) ->
    reply_json(
      Req, {struct, [{errors, {struct, [{iolist_to_binary([Field]),
                                         iolist_to_binary([Error])}]}}]}, 400).

require_auth(Req) ->
    case {mochiweb_request:get_header_value("invalid-auth-response", Req),
          mochiweb_request:get_header_value(scram_sha:meta_header(), Req)} of
        {"on", _} ->
            %% We need this for browsers that display auth
            %% dialog when faced with 401 with
            %% WWW-Authenticate header response, even via XHR
            reply(Req, 401);
        {_, undefined} ->
            case ns_config:read_key_fast(disable_www_authenticate, false) of
                false ->
                    reply(Req, 401,
                          [{"WWW-Authenticate",
                            "Basic realm=\"Couchbase Server Admin / REST\""}]);
                true ->
                    reply(Req, 401)
            end;
        _ ->
            %% scram sha meta header will be converted later on to scram sha
            %% related auth headers
            reply(Req, 401)
    end.

send_chunked(Req, StatusCode, ExtraHeaders) ->
    ?make_consumer(
       begin
           Resp = respond(
                    Req, {StatusCode, response_headers(Req, ExtraHeaders),
                          chunked}),
           pipes:foreach(?producer(),
                         fun (Part) ->
                                 mochiweb_response:write_chunk(Part, Resp)
                         end),
           mochiweb_response:write_chunk(<<>>, Resp)
       end).

handle_streaming(F, Req) ->
    HTTPRes = reply_ok(Req, "application/json; charset=utf-8", chunked),
    %% Register to get config state change messages.
    menelaus_event:register_watcher(self()),
    Sock = mochiweb_request:get(socket, Req),
    mochiweb_socket:setopts(Sock, [{active, true}]),
    handle_streaming(F, Req, HTTPRes, undefined, undefined).

streaming_inner(F, HTTPRes, LastRes, UpdateID) ->
    Res = F(stable, UpdateID),
    case Res =:= LastRes of
        true ->
            ok;
        false ->
            ResNormal = case Res of
                            {just_write, Stuff} ->
                                Stuff;
                            _ ->
                                F(unstable, UpdateID)
                        end,
            Encoded = case ResNormal of
                          {write, Bin} -> Bin;
                          _ -> encode_json(ResNormal)
                      end,
            mochiweb_response:write_chunk(Encoded, HTTPRes),
            mochiweb_response:write_chunk("\n\n\n\n", HTTPRes)
    end,
    Res.

handle_streaming(F, Req, HTTPRes, LastRes, UpdateID) ->
    Res =
        try streaming_inner(F, HTTPRes, LastRes, UpdateID)
        catch exit:normal ->
                mochiweb_response:write_chunk("", HTTPRes),
                exit(normal)
        end,
    request_throttler:hibernate(?MODULE, handle_streaming_wakeup,
                                [F, Req, HTTPRes, Res]).

handle_streaming_wakeup(F, Req, HTTPRes, Res) ->
    UpdateID =
        receive
            {notify_watcher, ID} ->
                timer:sleep(50),
                menelaus_event:flush_watcher_notifications(ID);
            _ ->
                exit(normal)
        after 25000 ->
                timeout
        end,
    handle_streaming(F, Req, HTTPRes, Res, UpdateID).

assert_is_enterprise() ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            ok;
        _ ->
            erlang:throw({web_exception,
                          400,
                          "This http API endpoint requires enterprise edition",
                          [{"X-enterprise-edition-needed", 1}]})
    end.

assert_is_55() ->
    assert_cluster_version(fun cluster_compat_mode:is_cluster_55/0).

assert_is_65() ->
    assert_cluster_version(fun cluster_compat_mode:is_cluster_65/0).

assert_is_66() ->
    assert_cluster_version(fun cluster_compat_mode:is_cluster_66/0).

assert_cluster_version(Fun) ->
    case Fun() of
        true ->
            ok;
        false ->
            erlang:throw(
              {web_exception,
               400,
               "This http API endpoint isn't supported in mixed version "
               "clusters",
               []})
    end.

choose_node_consistently(Req, Nodes) ->
    Token = menelaus_auth:extract_ui_auth_token(Req),
    Memo = menelaus_ui_auth:check(Token),
    Peer = mochiweb_request:get(peer, Req),
    N = erlang:phash2({Memo, Peer}, length(Nodes)) + 1,
    lists:nth(N, Nodes).


-ifdef(TEST).
compute_sec_headers_test() ->
    ?assertEqual(lists:sort(?SEC_HEADERS), lists:sort(compute_sec_headers([]))),
    ?assertEqual(lists:sort([{"hdr", "val"} | ?SEC_HEADERS]),
                 lists:sort(compute_sec_headers([{"hdr", "val"}]))),
    ?assertEqual(lists:sort(
                   lists:keystore("X-Frame-Options", 1, ?SEC_HEADERS,
                                  {"X-Frame-Options", "OTHER"})),
                 lists:sort(
                   compute_sec_headers([{"X-Frame-Options", "OTHER"}]))),
    ?assertEqual(lists:sort(
                   lists:keydelete("X-Frame-Options", 1, ?SEC_HEADERS)),
                 lists:sort(
                   compute_sec_headers([{"X-Frame-Options", disable}]))),
    ?assertEqual([], compute_sec_headers([{enabled, false}])),
    ?assertEqual(lists:sort(?SEC_HEADERS),
                 lists:sort(compute_sec_headers([{enabled, true}]))).

response_headers_test() ->
    BaseSecHeaders = ?BASE_HEADERS ++ ?SEC_HEADERS,
    AllHeaders = ?NO_CACHE_HEADERS ++ BaseSecHeaders,
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, latest, fun() -> [] end),
    meck:expect(ns_config, read_key_fast, fun(secure_headers, []) -> [] end),
    ?assertEqual(lists:keysort(1, AllHeaders),
                 response_headers([])),
    ?assertEqual(lists:keysort(1, AllHeaders),
                 response_headers([{allow_cache, false}])),
    ?assertEqual(lists:keysort(1, [{"Extra", "header"}, {"Foo", "bar"}] ++
                                   AllHeaders),
                 response_headers([{"Foo", "bar"}, {"Extra", "header"}])),
    ?assertEqual(lists:keysort(1, [{"Cache-Control", "max-age=30000000"}] ++
                                   BaseSecHeaders),
                 response_headers([{allow_cache, true}])),
    ?assertEqual(lists:keysort( 1, [{"Cache-Control", "max-age=10"}] ++
                                    BaseSecHeaders),
                 response_headers([{?CACHE_CONTROL, "max-age=10"}])),
    ?assertEqual(lists:keysort(1, [{"Cache-Control", "max-age=10"}] ++
                                   BaseSecHeaders),
                 response_headers([{?CACHE_CONTROL, "max-age=10"},
                                   {allow_cache, true}])),
    ?assertEqual(lists:keysort( 1, [{"Duplicate", "first"}] ++ AllHeaders),
                 response_headers([{"Duplicate", "first"},
                                   {"Duplicate", "second"}])),
    meck:expect(ns_config, read_key_fast,
                fun(secure_headers, []) -> [{enabled, false}] end),
    ?assertEqual(lists:keysort( 1, [{"Duplicate", "first"}] ++
                                    ?NO_CACHE_HEADERS ++ ?BASE_HEADERS),
                 response_headers([{"Duplicate", "first"},
                                   {"Duplicate", "second"}])),
    true = meck:validate(ns_config),
    meck:unload(ns_config).
-endif.
