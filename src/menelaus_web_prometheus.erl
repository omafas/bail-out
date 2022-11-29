%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(menelaus_web_prometheus).

%% API
-export([handle_get_local_metrics/2, handle_create_snapshot/1,
         handle_get_metrics/1, handle_sd_config/1,
         proxy_prometheus_api/2]).

-include("ns_common.hrl").

-define(METRICS_TIMEOUT, 5000).
-define(PART_SIZE, 1024).
-define(WINDOW_SIZE, 3).

%%%===================================================================
%%% API
%%%===================================================================

handle_get_metrics(Req) ->
    Resp = menelaus_util:respond(Req, {200, [], chunked}),
    ns_server_stats:report_prom_stats(
      fun (M) -> report_metric(Req, M, Resp) end, false),
    Settings = prometheus_cfg:settings(),
    Services = proplists:get_value(external_prometheus_services, Settings),
    NsServerProps = proplists:get_value(ns_server, Services, []),
    case proplists:get_bool(high_cardinality_enabled, NsServerProps) of
        true ->
            ns_server_stats:report_prom_stats(
              fun (M) -> report_metric(Req, M, Resp) end, true);
        false -> ok
    end,
    AllTargets = proplists:get_value(targets, Settings),
    AlmostAllTargets = proplists:delete(ns_server, AllTargets),
    HighCardTargets = lists:filter(
                        fun ({Name, _Addr}) ->
                            Props = proplists:get_value(Name, Services, []),
                            proplists:get_bool(high_cardinality_enabled, Props)
                        end, AlmostAllTargets),
    %% Addr is supposed to be a loopback address (127.0.0.1 or [::1] depending
    %% on configured ip family) with port, hence no need to use https
    MakeURL = fun (Addr, Path, kv) ->
                      "http://" ++ Addr ++ Path ++ "NoTS";
                  (Addr, Path, _) ->
                      "http://" ++ Addr ++ Path
              end,
    Config = ns_config:latest(),
    AuthHeader =
        fun (kv) ->
                menelaus_rest:basic_auth_header(
                  ns_config:search_node_prop(Config, memcached, admin_user),
                  ns_config:search_node_prop(Config, memcached, admin_pass));
            (_) ->
                menelaus_rest:special_auth_header()
        end,
    URLs = [{MakeURL(Addr, "/_prometheusMetrics", Type), AuthHeader(Type)}
            || {Type, Addr} <- AlmostAllTargets] ++
        [{MakeURL(Addr, "/_prometheusMetricsHigh", Type), AuthHeader(Type)}
         || {Type, Addr} <- HighCardTargets],
    [proxy_chunks_from_url(URL, Req, Resp) || URL <- URLs],
    menelaus_util:write_chunk(Req, <<>>, Resp).

handle_sd_config(Req) ->
    Nodes = menelaus_web_node:get_hostnames(Req, any),
    Yaml = [#{targets => [HostPort || {_, HostPort} <- Nodes]}],
    YamlBin = yaml:encode(Yaml),
    ClusterName = menelaus_web_pools:get_cluster_name(),
    Filename = io_lib:format("couchbase_sd_config_~s.yaml", [ClusterName]),
    ContentDisp = io_lib:format("attachment; filename=\"~s\"", [Filename]),
    ExtraHeaders = [{"Content-Disposition", lists:flatten(ContentDisp)}],
    menelaus_util:reply_ok(Req, "text/yaml", YamlBin, ExtraHeaders).

proxy_chunks_from_url({URL, AuthHeader}, Req, Resp) ->
    Options = [{connect_timeout, ?METRICS_TIMEOUT},
               {partial_download, [{window_size, ?WINDOW_SIZE},
                                   {part_size, ?PART_SIZE}]}],
    Headers = [AuthHeader],
    case lhttpc:request(URL, 'GET', Headers, [], ?METRICS_TIMEOUT, Options) of
        {ok, {{200, _}, _Hdrs, LHttpReqPid}} when is_pid(LHttpReqPid) ->
            case proxy_chunks(LHttpReqPid, Req, Resp) of
                ok -> ok;
                {error, Error} ->
                    ?log_error("Got error while reading chunks from ~s:~n~p",
                               [URL, Error])
            end;
        Bad ->
            ?log_error("Http get for ~s returned:~n~p", [URL, Bad]),
            ok
    end.

proxy_chunks(LHttpReqPid, Req, Resp) ->
    case lhttpc:get_body_part(LHttpReqPid, ?METRICS_TIMEOUT) of
        {ok, {http_eob, _Trailers}} -> ok;
        {ok, Bin} when is_binary(Bin) ->
            menelaus_util:write_chunk(Req, Bin, Resp),
            proxy_chunks(LHttpReqPid, Req, Resp);
        {error, _} = Error ->
            Error
    end.

%% It is supposed to be used by local prometheus to collect ns_server metrics
handle_get_local_metrics(IsHighCard, Req) ->
    Timeout =
        case proplists:get_value("timeout", mochiweb_request:parse_qs(Req)) of
            undefined -> undefined;
            Str ->
                %% Reduce the timeout a little bit just to have some time left
                %% to send the final chunk
                ceil(erlang:list_to_integer(Str)*0.95)
        end,

    Resp = menelaus_util:respond(Req, {200, [], chunked}),
    ns_server_stats:report_prom_stats(
      fun (M) -> report_metric(Req, M, Resp) end, IsHighCard, Timeout),
    menelaus_util:write_chunk(Req, <<>>, Resp).

handle_create_snapshot(Req) ->
    menelaus_util:ensure_local(Req),
    Settings = prometheus_cfg:settings(),
    Timeout = proplists:get_value(snapshot_timeout_msecs, Settings),
    case prometheus:create_snapshot(Timeout, Settings) of
        {ok, Response} ->
            menelaus_util:reply_text(Req, Response, 200);
        {error, Reason} ->
            menelaus_util:reply_text(Req, Reason, 500)
    end.

proxy_prometheus_api(RawPath, Req) ->
    ensure_allowed_prom_req(RawPath),
    Settings = prometheus_cfg:settings(),
    {Username, Password} = proplists:get_value(prometheus_creds, Settings),
    Method = mochiweb_request:get(method, Req),
    Headers = [menelaus_rest:basic_auth_header(Username, Password)] ++
              [{"Content-Type", "application/x-www-form-urlencoded"} ||
                Method /= 'GET'],
    {Addr, PortStr} = misc:split_host_port(proplists:get_value(addr, Settings),
                                           ""),
    Port = list_to_integer(PortStr),
    AFamily = proplists:get_value(afamily, Settings),
    %% Since we are not using https, we need to make sure we communicate
    %% with prometheus over loopback only (to comply with the strict TLS policy)
    true = lists:member(Addr, ["::1", "127.0.0.1"]),
    menelaus_util:proxy_req({http, Addr, Port, AFamily}, RawPath, Headers,
                            ?METRICS_TIMEOUT, fun (Hs) -> Hs end, [], Req).

%%%===================================================================
%%% Internal functions
%%%===================================================================

ensure_allowed_prom_req("/api/v1/query_range" ++ _) -> ok;
ensure_allowed_prom_req("/api/v1/query" ++ _) -> ok;
ensure_allowed_prom_req("/api/v1/series" ++ _) -> ok;
ensure_allowed_prom_req("/api/v1/labels" ++ _) -> ok;
ensure_allowed_prom_req("/api/v1/label/" ++ _) -> ok;
ensure_allowed_prom_req("/api/v1/metadata" ++ _) -> ok;
ensure_allowed_prom_req("/federate" ++ _) -> ok;
ensure_allowed_prom_req(_) ->
    menelaus_util:web_exception(404, "not found").

report_metric(Req, {Metric, Labels, Value}, Resp) ->
    LabelsIOList = [[name_to_iolist(K), <<"=\"">>, format_label_value(V),
                     <<"\"">>] || {K, V} <- Labels],
    Line =
        [name_to_iolist(Metric), <<"{">>, lists:join(<<",">>, LabelsIOList),
         <<"} ">>, promQL:format_value(Value), <<"\n">>],
    menelaus_util:write_chunk(Req, Line, Resp);
report_metric(Req, {Prefix, Metric, Labels, Value}, Resp) ->
    Prefixed = [Prefix, <<"_">>, name_to_iolist(Metric)],
    report_metric(Req, {Prefixed, Labels, Value}, Resp).

name_to_iolist(A) when is_atom(A) -> atom_to_binary(A, latin1);
name_to_iolist(A) -> A.

format_label_value(Val) ->
    ValBin = label_val_to_bin(Val),
    re:replace(ValBin, <<"\"">>, <<"\\\\\"">>, [global, {return, binary}]).

label_val_to_bin(N) when is_integer(N) -> integer_to_binary(N);
label_val_to_bin(F) when is_float(F) -> float_to_binary(F);
label_val_to_bin(A) when is_atom(A) -> atom_to_binary(A, latin1);
label_val_to_bin(Bin) when is_binary(Bin) -> Bin;
label_val_to_bin(Str) when is_list(Str) -> list_to_binary(Str).
