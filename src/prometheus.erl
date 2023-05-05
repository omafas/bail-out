%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(prometheus).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([query_range/6, query_range_async/7, query/4,
         create_snapshot/2, reload/1, quit/2,
         delete_series/5, clean_tombstones/2]).

-type metrics_data() :: [JSONObject :: {[{binary(), term()}]}].
-type error() :: {error, timeout | binary()}.
-type prom_query() :: string() | binary().
-type prom_timestamp() :: number().
-type prom_step() :: pos_integer() | string().
-type http_timeout() :: non_neg_integer().
-type successful_post() :: {ok, json, {[JSONObject :: term()]}} |
                           {ok, text, BinString :: binary()} |
                           {ok, no_content, BinString :: binary()}.

-spec quit(http_timeout(), prometheus_cfg:stats_settings()) -> ok | error().
quit(Timeout, Settings) ->
    case post("/-/quit", [], Timeout, Settings) of
        {ok, text, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec reload(prometheus_cfg:stats_settings()) -> ok | error().
reload(Settings) ->
    case post("/-/reload", [], infinity, Settings) of
        {ok, text, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec create_snapshot(http_timeout(), prometheus_cfg:stats_settings()) ->
                                    {ok, PathToSnapshot :: string()} | error().
create_snapshot(Timeout, Settings) ->
    case post("/api/v1/admin/tsdb/snapshot", [], Timeout, Settings) of
        {ok, json, {Data}} ->
            SnapshotName = proplists:get_value(<<"name">>, Data, {[]}),
            StoragePath = prometheus_cfg:storage_path(Settings),
            {ok, filename:join([StoragePath, "snapshots", SnapshotName])};
        {error, Reason} ->
            {error, Reason}
    end.

delete_series(MatchPatterns, Start, End, Timeout, Settings) ->
    Body = case Start of
               min_possible_time ->
                   [];
               Start ->
                   [{"start", Start}]
           end ++ [{"end", End}] ++ [{"match[]", P} || P <- MatchPatterns],

    case post("/api/v1/admin/tsdb/delete_series", Body, Timeout, Settings) of
        {ok, no_content, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

clean_tombstones(Timeout, Settings) ->
    case post("/api/v1/admin/tsdb/clean_tombstones", [], Timeout, Settings) of
        {ok, no_content, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec query_range(Query :: prom_query(),
                  Start :: prom_timestamp(),
                  End :: prom_timestamp(),
                  Step :: prom_step(),
                  Timeout :: http_timeout(),
                  prometheus_cfg:stats_settings()) ->
                                                {ok, metrics_data()} | error().
query_range(Query, Start, End, Step, Timeout, Settings) ->
    wait_async(
      fun (H) ->
          query_range_async(Query, Start, End, Step, Timeout, Settings, H)
      end, Timeout).

-spec query_range_async(Query :: prom_query(),
                        Start :: prom_timestamp(),
                        End :: prom_timestamp(),
                        Step :: prom_step(),
                        Timeout :: http_timeout(),
                        prometheus_cfg:stats_settings(),
                        fun(({ok, metrics_data()} | error()) -> ok)) -> ok.
query_range_async(Query, Start, End, Step, Timeout, Settings, Handler)
                  when is_integer(Step) ->
    StepStr = integer_to_list(Step) ++ "s",
    query_range_async(Query, Start, End, StepStr, Timeout, Settings, Handler);
query_range_async(Query, Start, End, Step, Timeout, Settings, Handler) ->
    proplists:get_bool(log_queries, Settings) andalso
        ?log_debug("Query range: ~s Start: ~p End: ~p Step: ~p Timeout: ~p",
                   [Query, Start, End, Step, Timeout]),
    TimeoutStr = integer_to_list(max(Timeout div 1000, 1)) ++ "s",
    Body = [{query, Query}, {start, Start}, {'end', End}, {step, Step},
            {timeout, TimeoutStr}],
    HandlerWrap =
        fun ({ok, json, {Data}}) ->
                Res = proplists:get_value(<<"result">>, Data, {[]}),
                Handler({ok, Res});
            ({error, Reason}) ->
                Handler({error, Reason})
        end,
    post_async("/api/v1/query_range", Body, Timeout, Settings, HandlerWrap).

-spec query(Query :: prom_query(),
            Time :: prom_timestamp() | undefined,
            Timeout :: http_timeout(),
            prometheus_cfg:stats_settings()) -> {ok, metrics_data()} | error().
query(Query, Time, Timeout, Settings) ->
    wait_async(fun (H) -> query_async(Query, Time, Timeout, Settings, H) end,
               Timeout).

-spec query_async(Query :: prom_query(),
                  Time :: prom_timestamp() | undefined,
                  Timeout :: http_timeout(),
                  prometheus_cfg:stats_settings(),
                  fun(({ok, metrics_data()} | error()) -> ok)) -> ok.
query_async(Query, Time, Timeout, Settings, Handler) ->
    proplists:get_bool(log_queries, Settings) andalso
        ?log_debug("Query: ~s Time: ~p Timeout: ~p ms",
                   [Query, Time, Timeout]),
    TimeoutStr = integer_to_list(max(Timeout div 1000, 1)) ++ "s",
    Body = [{query, Query}, {timeout, TimeoutStr}] ++
           [{time, Time} || Time =/= undefined],
    HandlerWrap =
        fun ({ok, json, {Data}}) ->
                Res = proplists:get_value(<<"result">>, Data, {[]}),
                Handler({ok, Res});
            ({error, Reason}) ->
                Handler({error, Reason})
        end,
    post_async("/api/v1/query", Body, Timeout, Settings, HandlerWrap).

-spec post(Path :: string(),
           Body :: proplists:proplist(),
           Timeout :: http_timeout() | infinity,
           prometheus_cfg:stats_settings()) ->
                successful_post() | error().
post(Path, Body, Timeout, Settings) ->
    wait_async(fun (H) -> post_async(Path, Body, Timeout, Settings, H) end,
               Timeout).

%% Wait in a separate process in order to correctly handle (ignore)
%% late replies in case when request times out.
wait_async(F, Timeout) ->
    misc:executing_on_new_process(
      fun () ->
          Self = self(),
          Ref = make_ref(),
          F(fun (Res) -> Self ! {Ref, Res} end),
          receive
              {Ref, Res} -> Res
          after Timeout ->
              {error, timeout}
          end
      end).

-spec post_async(Path :: string(),
                 Body :: proplists:proplist(),
                 Timeout :: http_timeout(),
                 prometheus_cfg:stats_settings(),
                 fun((successful_post() | error()) -> ok)) -> ok.
post_async(Path, Body, Timeout, Settings, Handler) ->
    case proplists:get_value(enabled, Settings) of
        true ->
            Addr = proplists:get_value(addr, Settings),
            URL = lists:flatten(io_lib:format("http://~s~s", [Addr, Path])),
            BodyEncoded = mochiweb_util:urlencode(Body),
            {Username, Password} = proplists:get_value(prometheus_creds,
                                                       Settings),
            Headers = [menelaus_rest:basic_auth_header(?HIDE({Username, Password}))],
            AFamily = proplists:get_value(afamily, Settings),
            StartTime = erlang:monotonic_time(millisecond),
            Receiver =
                fun (Res) ->
                    try
                        case handle_post_async_reply(Res, StartTime) of
                            {ok, _, _} = Reply -> Handler(Reply);
                            {error, Reason} = Reply ->
                                ?log_error("Prometheus http request failed:~n"
                                           "URL: ~s~nBody: ~s~nReason: ~p",
                                           [URL, BodyEncoded, Reason]),
                                Handler(Reply)
                        end
                    catch
                        Class:Error:ST ->
                            ?log_error("Exception in httpc receiver ~p:~p~n"
                                       "Stacktrace: ~p~nRes: ~w",
                                       [Class, Error, ST, Res])
                    end
                end,
            HttpOptions = [{timeout, Timeout}, {connect_timeout, Timeout}],
            Options = [{sync, false}, {receiver, Receiver},
                       {socket_opts, [{ipfamily, AFamily}]}],
            Req = {URL, Headers, "application/x-www-form-urlencoded",
                   BodyEncoded},
            {ok, _} = httpc:request('post', Req, HttpOptions, Options);
        false ->
            Handler({error, <<"Stats backend is disabled">>})
    end,
    ok.

handle_post_async_reply({_Ref, {error, R}}, _) ->
    ns_server_stats:notify_counter(
        {<<"outgoing_http_requests">>, [{code, "error"}, {type, prometheus}]}),
    {error, R};
handle_post_async_reply({_Ref, {{_, Code, CodeText}, Headers, Reply}},
                        StartTime) ->
    TimeDiff = erlang:monotonic_time(millisecond) - StartTime,
    ns_server_stats:notify_histogram(
        {<<"outgoing_http_requests">>, [{type, prometheus}]}, TimeDiff),
    ns_server_stats:notify_counter(
        {<<"outgoing_http_requests">>, [{code, Code}, {type, prometheus}]}),
    case proplists:get_value("content-type", Headers) of
        "application/json" ->
            try ejson:decode(Reply) of
                {JSON} ->
                    case proplists:get_value(<<"status">>, JSON) of
                        <<"success">> ->
                            R = proplists:get_value(<<"data">>, JSON),
                            {ok, json, R};
                        <<"error">> ->
                            E = proplists:get_value(<<"error">>, JSON),
                            {error, E}
                    end
            catch
                _:_ ->
                    R = misc:format_bin("Invalid json in reply: ~s", [Reply]),
                    {error, R}
            end;
        _ ->
            case Code of
                200 -> {ok, text, Reply};
                204 -> {ok, no_content, Reply};
                _ when Reply =/= <<>> -> {error, Reply};
                _ -> {error, CodeText}
            end
    end;
handle_post_async_reply(Unhandled, _) ->
    ?log_error("Unhandled response from httpc: ~p", [Unhandled]),
    ns_server_stats:notify_counter(
        {<<"outgoing_http_requests">>, [{code, "error"}, {type, prometheus}]}),
    {error, {unexpected, Unhandled}}.

-ifdef(TEST).

post_timeout_test() ->
    meck:new(httpc, [passthrough]),
    ResData = iolist_to_binary(io_lib:format("~p", [erlang:make_ref()])),
    ReqHandler =
        fun('post',
            {"http://127.0.0.1:9900/test/path", _,
             "application/x-www-form-urlencoded", _Body},
            _HttpOptions, Options) ->
                Receiver = proplists:get_value(receiver, Options),
                spawn(fun () ->
                          Res = {{"HTTP/1.1", 200, ""}, [], ResData},
                          Receiver({erlang:make_ref(), Res})
                      end),
                {ok, undefined}
        end,
    meck:expect(httpc, request, ReqHandler),
    try
        Settings = [{enabled, true},
                    {addr, "127.0.0.1:9900"},
                    {prometheus_creds, {"user", "pass"}},
                    {afamily, "inet"}],

        ?assertEqual({ok, text, ResData},
                     post("/test/path", [], 10000, Settings)),

        Results = [post("/test/path", [], 0, Settings)
                       || _ <- lists:seq(1, 100)],
        ?assert(lists:member({error, timeout}, lists:usort(Results))),
        timer:sleep(1000),
        CountRepliesInMainbox =
            fun C(N) ->
                receive
                    {_, {ok, text, ResData}} -> C(N + 1)
                after
                    0 -> N
                end
            end,
        ?assertEqual(0, CountRepliesInMainbox(0))
    after
        meck:unload(httpc)
    end.

-endif.
