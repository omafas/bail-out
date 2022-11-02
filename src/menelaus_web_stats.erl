%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc rest api for stats
-module(menelaus_web_stats).

-export([handle_range_post/1, handle_range_get/2,
         handle_get_settings/2,
         handle_get_internal_settings/2,
         handle_post_settings/2,
         handle_post_internal_settings/2,
         aggregate/2]).

-include("ns_common.hrl").
-include("rbac.hrl").
-include("cut.hrl").
-include("ns_test.hrl").

-define(MAX_TS, 9999999999999).
-define(MIN_TS, -?MAX_TS).
-define(DEFAULT_PROMETHEUS_QUERY_TIMEOUT, 60000).
-define(DERIVED_PARAM_LABEL, <<"__derived_param_name_label__">>).
-define(NOT_EXISTING_LABEL, undefined).
-define(MAX_PARALLEL_QUERIES_IN_POST, 128).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

params() ->
    [{"prometheus.queryMaxSamples",
      #{cfg_key => query_max_samples, type => pos_int}},
     {"prometheus.retentionSize",
      #{cfg_key => retention_size, type => pos_int}},
     {"prometheus.retentionTime",
      #{cfg_key => retention_time, type => pos_int}}].

params_internal() ->
    Services = all_services(),

    [{"enabled",
      #{type => bool}},
     {"scrapeInterval",
      #{cfg_key => scrape_interval,
        type => {int, 1, prometheus_cfg:max_scrape_int()}}},
     {"scrapeTimeout",
      #{cfg_key => scrape_timeout,
        type => {int, 1, prometheus_cfg:max_scrape_int()}}},
     {"snapshotCreationTimeout",
      #{cfg_key => snapshot_timeout_msecs, type => pos_int}},
     {"scrapeIntervalsCalculationPeriod",
      #{cfg_key => intervals_calculation_period, type => pos_int_or_minus_one}},
     {"cbcollect.statsMaxSize",
      #{cfg_key => cbcollect_stats_dump_max_size, type => pos_int}},
     {"cbcollect.statsMinPeriod",
      #{cfg_key => cbcollect_stats_min_period, type => pos_int}},
     {"expectedAvgSampleSize",
      #{cfg => average_sample_size, type => pos_int}},
     {"logExecutedQueries",
      #{cfg_key => log_queries, type => bool}},
     {"derivedMetricsFilter",
      #{cfg_key => derived_metrics_filter, type => derived_metrics_filter}},
     {"derivedMetricsCalculationInterval",
      #{cfg_key => derived_metrics_interval, type => pos_int_or_minus_one}},
     {"prometheus.walCompression",
      #{cfg_key => wal_compression, type => bool}},
     {"prometheus.authEnabled",
      #{cfg_key => prometheus_auth_enabled, type => bool}},
     {"prometheus.logLevel",
      #{cfg_key => log_level,
        type => {one_of, string, ["debug", "info", "warn", "error"]}}},
     {"prometheus.maxBlockDuration",
      #{cfg_key => max_block_duration, type => pos_int}},
     {"prometheus.loopbackDelta",
      #{cfg_key => loopback_delta, type => int}},
     {"prometheus.reportMetrics",
      #{cfg_key => prometheus_metrics_enabled, type => bool}},
     {"prometheus.reportMetricsInterval",
      #{cfg_key => prometheus_metrics_scrape_interval, type => pos_int}},
     {"prometheus.listenAddr",
      #{cfg_key => listen_addr_type,
        type => {one_of, existing_atom, [loopback, any]}}},
     {"pruningInterval",
      #{cfg_key => pruning_interval, type => pos_int}},
     {"decimation.enabled",
      #{cfg_key => decimation_enabled, type => bool}},
     {"decimation.matchPatterns",
      #{cfg_key => decimation_match_patterns, type => prom_patterns}},
     {"truncation.enabled",
      #{cfg_key => truncation_enabled, type => bool}},
     {"truncation.matchPatterns",
      #{cfg_key => truncation_match_patterns, type => prom_patterns}},
     {"truncation.age",
      #{cfg_key => truncate_max_age, type => pos_int}},
     {"truncation.minInterval",
      #{cfg_key => min_truncation_interval, type => non_neg_int}},
     {"tombstonesCleaningEnabled",
      #{cfg_key => clean_tombstones_enabled, type => bool}}] ++
    params() ++
    [{"services." ++ N ++ ".highCardEnabled",
      #{cfg_key => [services, S, high_cardinality_enabled], type => bool}}
     || {S, N} <- Services] ++
    [{"services." ++ N ++ ".highCardScrapeInterval",
      #{cfg_key => [services, S, high_cardinality_scrape_interval],
        type => high_card_scrape_interval}}
     || {S, N} <- Services] ++
    [{"services." ++ N ++ ".highCardScrapeTimeout",
      #{cfg_key => [services, S, high_cardinality_scrape_timeout],
        type => high_card_scrape_interval}}
     || {S, N} <- Services] ++
    [{"statsExport." ++ N ++ ".highCardEnabled",
      #{cfg_key => [external_prometheus_services, S, high_cardinality_enabled],
        type => bool}}
     || {S, N} <- Services].

type_spec(derived_metrics_filter) ->
    #{validators => [fun derived_metrics_filter/2],
      formatter => fun format_derived_metrics_filter/1};
type_spec(prom_patterns) ->
    #{validators => [{string_list, ","},
                     validate_list(fun prom_pattern/1, _, _)],
      formatter => {string_list, ","}};
type_spec(pos_int_or_minus_one) ->
    #{validators => [int, validator:validate(
                            fun (-1) -> ok;
                                (N) when N > 0 -> ok;
                                (_) ->
                                    {error, "must be a positive integer or -1"}
                            end, _, _)],
      formatter => int};
type_spec(high_card_scrape_interval) ->
    MaxInt = prometheus_cfg:max_scrape_int(),
    #{validators => [int, validator:validate(
                            fun (-1) -> ok;
                                (N) when N > 0, N =< MaxInt -> ok;
                                (_) ->
                                    Err = io_lib:format(
                                            "must be an integer in range from "
                                            "1 to ~b, or -1", [MaxInt]),
                                    {error, Err}
                            end, _, _)],
      formatter => int}.

format_derived_metrics_filter(all) -> {value, <<"all">>};
format_derived_metrics_filter(L) -> {value, [list_to_binary(M) || M <- L]}.

derived_metrics_filter(Name, State) ->
    Err = "must be either \"all\" or list of strings",
    validator:validate(
        case validator:is_json(State) of
            true ->
                fun (<<"all">>) -> {value, all};
                    (List) when is_list(List) ->
                        case lists:all(fun is_binary/1, List) of
                            true ->
                                Metrics = [binary_to_list(B) || B <- List],
                                verify_derived_metrics(Metrics);
                            false -> {error, Err}
                        end;
                    (_NotList) ->
                        {error, Err}
                end;
            false ->
                fun ("all") -> {value, all};
                    (Str) ->
                        Tokens = string:lexemes(Str, ","),
                        Metrics = [string:trim(T) || T <- Tokens],
                        verify_derived_metrics(Metrics)
                end
        end, Name, State).

validate_list(Fun, Name, State) ->
    validator:validate(
      fun (List) ->
          try
             {value, lists:map(
                       fun (E) ->
                           case Fun(E) of
                               {value, V} -> V;
                               {error, Err} -> throw(Err)
                           end
                       end, List)}
          catch
              throw:Err -> {error, Err}
          end
      end, Name, State).

prom_pattern(Str) ->
    Trimmed = string:trim(Str),
    Msg = <<"bad prometheus patterns">>,
    case re:run(Trimmed, <<"^{(.*)}$">>, [{capture, all_but_first, list}]) of
        {match, _} -> {value, Trimmed};
        nomatch -> {error, Msg}
    end.

handle_get_internal_settings(Path, Req) ->
    Settings = prometheus_cfg:with_applied_defaults(
                 ns_config:read_key_fast(stats_settings, [])),
    menelaus_web_settings2:handle_get(Path, params_internal(), fun type_spec/1,
                                      Settings, Req).

handle_get_settings(Path, Req) ->
    Settings = prometheus_cfg:with_applied_defaults(
                 ns_config:read_key_fast(stats_settings, [])),
    menelaus_web_settings2:handle_get(Path, params(), fun type_spec/1,
                                      Settings, Req).

handle_post_internal_settings(Path, Req) ->
    menelaus_web_settings2:handle_post(
      apply_props(Path, _, _, internal), Path, params_internal(),
      fun type_spec/1, Req).

handle_post_settings(Path, Req) ->
    menelaus_web_settings2:handle_post(
      apply_props(Path, _, _, external), Path, params(), fun type_spec/1, Req).

apply_props(Path, PropList, Req, Type) ->
    OldProps = ns_config:read_key_fast(stats_settings, []),
    NewProps = lists:foldl(
                 fun ({KeyTokens, Value}, Acc) ->
                     apply_value(KeyTokens, Value, Acc)
                 end, OldProps, PropList),
    validate_metrics_settings(prometheus_cfg:with_applied_defaults(NewProps)),
    ns_config:set(stats_settings, NewProps),
    case Type of
        internal -> handle_get_internal_settings(Path, Req);
        external -> handle_get_settings(Path, Req)
    end.

validate_metrics_settings(Settings) ->
    case proplists:get_value(scrape_interval, Settings) <
         proplists:get_value(scrape_timeout, Settings) of
        true ->
            Msg = <<"scrapeInterval must be greater than or equal to "
                    "scrapeTimeout">>,
            menelaus_util:global_error_exception(400, Msg);
        false -> ok
    end,
    lists:foreach(
      fun ({Name, Props}) ->
          Int = proplists:get_value(high_cardinality_scrape_interval, Props),
          Timeout = proplists:get_value(high_cardinality_scrape_timeout, Props),
          if
              Int == -1 -> ok;
              Int < Timeout ->
                  F = "highCardScrapeInterval for service ~p must be "
                      "greater than or equal to highCardScrapeTimeout",
                  S = ns_cluster_membership:json_service_name(Name),
                  Error = iolist_to_binary(io_lib:format(F, [S])),
                  menelaus_util:global_error_exception(400, Error);
              true -> ok
          end
      end, proplists:get_value(services, Settings, [])).

apply_value([], Value, _PropList) -> Value;
apply_value([Key | Tail], Value, PropList) ->
    Res = misc:key_update(Key, PropList,
                          fun (SubProplist) ->
                              apply_value(Tail, Value, SubProplist)
                          end),
    case Res of
        false -> [{Key, apply_value(Tail, Value, [])} | PropList];
        _ -> Res
    end.

handle_range_post(Req) ->
    PermFilters =
        case promql_filters_for_identity(menelaus_auth:get_identity(Req)) of
            [] ->
                ns_audit:auth_failure(Req),
                menelaus_util:web_exception(403, "Forbidden");
            F -> F
        end,
    Now = os:system_time(millisecond),
    validator:handle(
      fun (List) ->
          %% New process is needed to avoid leaving response messages in
          %% mochiweb handler process's mailbox in case of timeout or other
          %% problems
          misc:executing_on_new_process(
            fun () ->
                handle_range_post_validated(List, PermFilters, Now, Req)
            end)
      end, Req, json_array, post_validators(Now, Req)).

handle_range_post_validated(List, PermFilters, Now, Req) ->
    Monitors = start_node_extractors_monitoring(List),
    Groups = misc:split(?MAX_PARALLEL_QUERIES_IN_POST, List),
    reply_with_chunked_json_array(
      fun (Reply, InitState) ->
          lists:foldl(
            fun (Sublist, Acc1) ->
                Requests = lists:map(send_metrics_request(_, PermFilters),
                                     Sublist),
                lists:foldl(
                  fun ({Ref, Props}, {Acc2, DownHosts}) ->
                      try read_metrics_response(Ref, Props, Now, DownHosts) of
                          {Res, NewDownHosts} ->
                              NewAcc2 = Reply(Res, Acc2),
                              {NewAcc2, NewDownHosts}
                      catch
                          Type:What:Stack ->
                              {Msg, Report} = menelaus_util:server_error_report(
                                                Req, Type, What, Stack),
                              ?log_error("Server error during processing: ~p",
                                         [Report]),
                              ErrorReply = {[{data, []},
                                             {errors, [{[{node, node()},
                                                         {error, Msg}]}]}]},
                              NewAcc2 = Reply(ErrorReply, Acc2),
                              {NewAcc2, DownHosts}
                      end
                  end, Acc1, lists:zip(Requests, Sublist))
            end, {InitState, []}, Groups)
      end, Req),
    stop_node_extractors_monitoring(Monitors).

reply_with_chunked_json_array(Fun, Req) ->
    HTTPResp = menelaus_util:reply_ok(
                 Req, "application/json; charset=utf-8", chunked),
    Write = menelaus_util:write_chunk(Req, _, HTTPResp),
    Write(<<"[">>),
    ReportFun = fun (JsonToWrite, IsFirst) ->
                    case IsFirst of
                         true -> ok;
                         false -> Write(<<",">>)
                    end,
                    Write(ejson:encode(JsonToWrite)),
                    false
                end,
    Fun(ReportFun, true),
    Write(<<"]">>),
    Write(<<>>).

handle_range_get([], _Req) ->
    menelaus_util:web_exception(404, "not found");
handle_range_get([MetricName | NotvalidatedFunctions], Req) ->
    PermFilters =
        case promql_filters_for_identity(menelaus_auth:get_identity(Req)) of
            [] ->
                ns_audit:auth_failure(Req),
                menelaus_util:web_exception(403, "Forbidden");
            F -> F
        end,
    Functions = try validate_functions(NotvalidatedFunctions)
                catch
                    error:invalid_function ->
                        menelaus_util:web_exception(404, "not found")
                end,
    Now = os:system_time(millisecond),
    validator:handle(
      fun (Props) ->
          Labels = lists:filter(fun ({K, _V}) ->
                                     not lists:member(K, [timeWindow, step,
                                                          start, 'end', nodes,
                                                          nodesAggregation,
                                                          timeout,
                                                          alignTimestamps])
                                end, Props),
          AllLabels = [{<<"name">>, iolist_to_binary(MetricName)} | Labels],
          Metric = lists:map(
                     fun ({Name, Value}) ->
                         {[{<<"label">>, Name}, {<<"value">>, Value}]}
                     end, AllLabels),
          NewProps = [{metric, Metric}, {applyFunctions, Functions} | Props],
          %% New process is needed to avoid leaving response messages in
          %% mochiweb handler process's mailbox in case of timeout or other
          %% problems
          misc:executing_on_new_process(
            fun () ->
                Monitors = start_node_extractors_monitoring([Props]),
                Ref = send_metrics_request(NewProps, PermFilters),
                {Res, _} = read_metrics_response(Ref, NewProps, Now, []),
                stop_node_extractors_monitoring(Monitors),
                menelaus_util:reply_json(Req, Res)
            end)
      end, Req, qs, get_validators(Now, MetricName, Req)).

post_validators(Now, Req) ->
    [validate_metric_json(metric, _),
     validator:required(metric, _),
     validator:string_array(applyFunctions, _),
     validate_functions(applyFunctions, _),
     validator:string_array(nodes, _) | validators(Now, Req)] ++
    [validator:validate_relative(
       fun (special, Metric) ->
               case derived_metrics:is_metric(extract_metric_name(Metric)) of
                   false ->
                       {error, <<"'special' aggregation is not available for "
                                 "non-derived metric">>};
                   _ -> ok
               end;
           (_, _) -> ok
       end, nodesAggregation, metric, _),
     validator:unsupported(_)].

get_validators(Now, MetricName, Req) ->
    [validator:token_list(nodes, ", ", _) | validators(Now, Req)] ++
    [validator:validate(
       fun (special) ->
               case derived_metrics:is_metric(iolist_to_binary(MetricName)) of
                   false ->
                       {error, <<"'special' aggregation is not available for "
                                 "non-derived metric">>};
                   _ -> ok
               end;
           (_) -> ok
       end, nodesAggregation, _)].

validators(Now, Req) ->
    NowSec = Now div 1000,
    [validate_time_duration(timeWindow, _),
     validator:default(timeWindow,
                       fun () ->
                           MaxScrapeInt = prometheus_cfg:max_scrape_int(),
                           %% Use 2 * MaxScrapeInterval by default
                           %% because most of the functions require at least 2
                           %% samples to be evaluated
                           integer_to_list(2 * MaxScrapeInt) ++ "s"
                       end, _),
     validator:boolean(alignTimestamps, _),
     validate_nodes_v2(nodes, _, Req),
     validator:default(nodes,
                       ?cut(menelaus_web_node:get_hostnames(Req, any)), _),
     validator:validate(
       fun Parse(V) when is_binary(V) -> Parse(binary_to_list(V));
           Parse("max") -> {value, max};
           Parse("min") -> {value, min};
           Parse("avg") -> {value, avg};
           Parse("sum") -> {value, sum};
           Parse("none") -> {value, none};
           Parse("special") -> {value, special};
           Parse("p" ++ PStr) ->
               try {value, {histogram_quantile, validate_percentile(PStr)}}
               catch _ -> {error, "invalid aggregation function"} end;
           Parse(_) ->
               {error, "invalid aggregation function"}
       end, nodesAggregation, _),
     validate_time_duration(step, _),
     validator:default(step, "10s", _),
     validator:integer(start, ?MIN_TS, ?MAX_TS, _),
     validator:integer('end', ?MIN_TS, ?MAX_TS, _),
     validate_negative_ts(start, NowSec, _),
     validate_negative_ts('end', NowSec, _),
     validator:greater_or_equal('end', start, _),
     validator:default(start, NowSec - 60, _),
     validator:default('end', NowSec, _),
     maybe_align_start(_),
     validator:integer(timeout, 1, 60*5*1000, _),
     validator:default(timeout, ?DEFAULT_PROMETHEUS_QUERY_TIMEOUT, _)].

start_node_extractors_monitoring(List) ->
    AllNodes = lists:foldl(
                 fun (Props, Acc) ->
                     Nodes = [N || {N, _} <- proplists:get_value(nodes, Props)],
                     lists:umerge(lists:sort(Nodes), Acc)
                 end, [], List),
    lists:map(fun (N) -> erlang:monitor(process, {ns_server_stats, N}) end,
              AllNodes).

stop_node_extractors_monitoring(Refs) ->
    lists:map(
      fun (Ref) ->
          erlang:demonitor(Ref, [flush])
      end, Refs).

%% If sum is used inside quantile, we should sum by le. The reason is
%% because the histogram_quantile function needs 'le' label to be present.
%% The same logic works for other aggregative functions, like min, max, etc.
%% For example:
%% "http_request_duration_bucket/sum/p90" should be converted
%%   histogram_quantile(0.9, sum by (le) (http_request_duration_bucket))
%% instead of
%%   histogram_quantile(0.9, sum(http_request_duration_bucket))
%% which will not work
maybe_prevent_le_aggregation(Functions, Props) ->
    NodesAggregation = proplists:get_value(nodesAggregation, Props),
    PreserveLE = case NodesAggregation of
                      {histogram_quantile, _} -> true;
                      _ -> false
                 end,
    {_, NewFunctions} =
        lists:foldr(
          fun ({histogram_quantile, _} = F, {_, Acc}) -> {true, [F | Acc]};
              (F, {false, Acc}) -> {false, [F | Acc]};
              (F, {true, Acc}) ->
                  case promQL:is_aggregation_op(F) of
                      true ->
                          {true, [{F, by, ["le"]} | Acc]};
                      false ->
                          {true, [F | Acc]}
                  end
          end, {PreserveLE, []}, Functions),
    NewFunctions.

send_metrics_request(Props, PermFilters) ->
    Functions = proplists:get_value(applyFunctions, Props, []),
    Functions2 = maybe_prevent_le_aggregation(Functions, Props),
    Window = proplists:get_value(timeWindow, Props),
    Labels = proplists:get_value(metric, Props),
    Query =
        case derived_metrics:is_metric(extract_metric_name(Labels)) of
            false ->
                construct_promql_query(Labels, Functions2, Window, PermFilters);
            true ->
                derived_metric_query(Labels, Functions2, Window, PermFilters)
        end,
    Start = proplists:get_value(start, Props),
    End = proplists:get_value('end', Props),
    Step = proplists:get_value(step, Props),
    Timeout = proplists:get_value(timeout, Props),
    Nodes = proplists:get_value(nodes, Props),
    NodesToPoll = lists:usort([N || {N, _} <- Nodes]),
    Ref = make_ref(),
    lists:foreach(
      fun (N) ->
          Req = {extract, {self(), {Ref, N}}, Query, Start, End, Step, Timeout},
          gen_server:cast({ns_server_stats, N}, Req)
      end, NodesToPoll),
    Ref.

read_metrics_response(Ref, Props, StartTimestampMs, DownHosts) ->
    Nodes = proplists:get_value(nodes, Props),
    Timeout = proplists:get_value(timeout, Props),
    {BadRes, GoodRes} =
        misc:partitionmap(
          fun ({Node, HostPort}) ->
              case lists:member(HostPort, DownHosts) of
                  true -> {left, {HostPort, down}};
                  false ->
                      TimeLeft = max(StartTimestampMs + Timeout -
                                     os:system_time(millisecond), 0),
                      receive
                          {{Ref, Node}, {ok, R}} -> {right, {HostPort, R}};
                          {{Ref, Node}, {error, R}} -> {left, {HostPort, R}};
                          {'DOWN', _, _, {ns_server_stats, Node}, _} ->
                              {left, {HostPort, down}}
                      after TimeLeft ->
                          {left, {HostPort, timeout}}
                      end
              end
          end, Nodes),
    AggFunction = proplists:get_value(nodesAggregation, Props, none),
    MetricName = extract_metric_name(proplists:get_value(metric, Props, [])),
    DerivedMetricName = (derived_metrics:is_metric(MetricName)
                         andalso MetricName),
    CleanedRes = [{N, clean_metric_props(R)} || {N, R} <- GoodRes],
    Data = merge_metrics(CleanedRes, DerivedMetricName, AggFunction),
    Errors = [{[{node, N},
                {error, format_error(R)}]}
                 || {N, R} <- BadRes],
    NewDownHosts = lists:usort(DownHosts ++ [H || {H, down} <- BadRes]),
    Start = proplists:get_value(start, Props),
    End = proplists:get_value('end', Props),
    {{[{data, Data}, {errors, Errors}, {startTimestamp, Start},
       {endTimestamp, End}]}, NewDownHosts}.

validate_metric_json(Name, State) ->
    validator:validate(
      fun ([]) ->
              {error, "must not be empty"};
          (Labels) when is_list(Labels) ->
              try
                  lists:foreach(
                    fun ({Props}) -> validate_label_props(Props);
                        (_) -> throw("must be a list of JSON objects")
                    end, Labels),
                  ok
              catch
                  throw:Msg -> {error, Msg}
              end;
          (_) ->
              {error, "must be a list of JSON objects"}
      end, Name, State).

validate_label_props(LabelProps) ->
    lists:foreach(
      fun ({N, _}) ->
          case lists:member(N, [<<"label">>, <<"value">>, <<"operator">>]) of
              true -> ok;
              false -> throw(io_lib:format("unknown label prop: ~s", [N]))
          end
      end, LabelProps),
    case proplists:is_defined(<<"label">>, LabelProps) and
         proplists:is_defined(<<"value">>, LabelProps) of
        true -> ok;
        false -> throw("all metric labels must contain "
                       "'label' and 'value' keys")
    end,
    Op = proplists:get_value(<<"operator">>, LabelProps, <<"=">>),
    case lists:member(Op, [<<"=">>, <<"!=">>, <<"=~">>, <<"!~">>, <<"any">>,
                           <<"not_any">>]) of
        true -> ok;
        false -> throw(io_lib:format("invalid operator: ~p", [Op]))
    end,
    Name = proplists:get_value(<<"label">>, LabelProps),
    case is_binary(Name) of
        true -> ok;
        false -> throw("Label name must be a string")
    end,
    Val = proplists:get_value(<<"value">>, LabelProps),
    case lists:member(Op, [<<"any">>, <<"not_any">>]) of
        true ->
            (is_list(Val) andalso lists:all(fun is_binary/1, Val)) orelse
                throw(io_lib:format("label value for ~s must be a list of "
                                    "strings when used with '~s' operation",
                                    [Name, Op]));
        false ->
            is_binary(Val) orelse
                throw(io_lib:format("label value for ~s must be a string",
                                    [Name]))
    end.

validate_functions(Name, State) ->
    validator:validate(
      fun (L) ->
          try
              {value, validate_functions(L)}
          catch
              error:invalid_function ->
                  {error, "invalid list of functions"}
          end
      end, Name, State).

clean_metric_props(Metrics) ->
    lists:map(
      fun ({MetricProps}) ->
          {misc:key_update(<<"metric">>, MetricProps,
                           fun ({P}) ->
                               {functools:chain(
                                  P, [proplists:delete(<<"job">>, _),
                                      proplists:delete(<<"__name__">>, _)])}
                           end)}
      end, Metrics).

aggregate_results(Results, AggParamsLabel, AggregationFun) ->
    UnpackedResults = [{maps:from_list(Metric), Values}
                        || {Props} <- Results,
                           [{<<"metric">>, {Metric}},
                            {<<"values">>, Values}] <- [lists:sort(Props)]],
    lists:map(
      fun ({NamePropsMap, List}) ->
          {[{<<"metric">>, {maps:to_list(NamePropsMap)}},
            {<<"values">>, aggregate_values(List, AggregationFun)}]}
      end, misc:groupby_map(
             fun ({M, V}) ->
                PName = maps:get(AggParamsLabel, M, default_param),
                %% if this metric is part of a derived metric,
                %% the name label will lead to incorrect groupping (we will
                %% set other name for it after aggregation anyway).
                LabelsToRemove = [AggParamsLabel] ++
                                 [<<"name">> || PName =/= default_param],
                {maps:without(LabelsToRemove, M), {PName, V}}
             end, UnpackedResults)).

%% List :: [{ParamName, list([Timestamp, ValueAsStr])}]
%% Example:
%% [{param1, [[16243234, "1"], [16243235, "1"], [16243236, "1"]]},
%% [{param1, [[16243234, "2"],                  [16243236, "2"]]},
%% [{param2, [[16243234, "3"], [16243235, "3"], [16243236, "3"]]},
%% [{param2, [[16243234, "4"], [16243235, "4"]                 ]}]
aggregate_values(List, AggregationFun) ->
      %% List2 :: [{ParamName, list(list([Timestamp, ValueAsStr]))}]
      %% Example:
      %% [{param1, [[[16243234, "1"], [16243235, "1"], [16243236, "1"]],
      %%            [[16243234, "2"],                  [16243236, "2"]]]},
      %%  {param2, [[[16243234, "3"], [16243235, "3"], [16243236, "3"]],
      %%            [[16243234, "4"], [16243235, "4"],                ]]}]
      List2 = misc:groupby_map(fun functools:id/1, List),

      Timestamps = lists:umerge(lists:map(?cut([TS || [TS, _V] <- _1]),
                                          [L || {_, L} <- List])),
      Normalize = normalize_datapoints(Timestamps, _, []),

      %% List4 :: [[[ValueAsStr]]]
      %% Example:
      %%        Node1              Node2
      %% [ [["1", "1", "1"], ["2", "NaN", "2"]],   <- param1
      %%   [["3", "3", "3"], ["4", "4", "NaN"]] ]  <- param2
      {AggregationParams, List3} = lists:unzip(List2),
      List4 = [lists:map(Normalize, L) || L <- List3],

      %% List5 :: [[[ValueAsStr]]]
      %% Example: [ [["1", "2"], ["1", "NaN"], ["1", "2"]],
      %%            [["3", "4"], ["3", "4"], ["3", "NaN"]] ]
      List5 = lists:map(
                fun ([]) ->
                        [[] || _ <- Timestamps];
                    (ValueLists) ->
                        misc:zipwithN(fun (L) -> L end, ValueLists)
                end, List4),

      %% List6 :: [AggregatedValueAsStr]
      %% Example: [AggregationFun([1, 2], [3, 4]),
      %%           AggregationFun([1, undefined], [3, 4]),
      %%           AggregationFun([1, 2], [3, undefined])]
      %% and if AggregationFun is sum, List6 will be [10, 8, 6]
      List6 = misc:zipwithN(
                fun (ValuesLists) ->
                   ParsedValuesMap =
                     lists:zip(AggregationParams,
                               [[promQL:parse_value(V) || V <- Values]
                                || Values <- ValuesLists]),
                   Res = AggregationFun(maps:from_list(ParsedValuesMap)),
                   promQL:format_value(Res)
                end, List5),

      %% Values :: [ [Timestamp, AggregatedValueAsStr] ]
      %% Example: [[16243234, 10], [16243235, 8], [16243236, 6]]
      lists:zipwith(?cut([_1, _2]), Timestamps, List6).

-ifdef(TEST).

aggregate_results_test() ->
    ?assertEqual([], aggregate_results([], [<<"op1">>], fun (_) -> 42 end)),

    Values = [
        %% From node 1
        {[{<<"metric">>, {[{<<"name">>, <<"m1">>}, {<<"b">>, <<"b1">>},
                           {?DERIVED_PARAM_LABEL, <<"op1">>}]}},
          {<<"values">>, [[1, <<"2">>], [2, <<"3">>], [3, <<"5">>]]}]},
        {[{<<"metric">>, {[{<<"name">>, <<"m1">>}, {<<"b">>, <<"b2">>},
                           {?DERIVED_PARAM_LABEL, <<"op1">>}]}},
          {<<"values">>, [[1, <<"7">>], [2, <<"11">>], [3, <<"13">>]]}]},
        {[{<<"metric">>, {[{<<"name">>, <<"m2">>}, {<<"b">>, <<"b1">>},
                           {?DERIVED_PARAM_LABEL, <<"op2">>}]}},
          {<<"values">>, [[1, <<"17">>], [2, <<"19">>], [3, <<"23">>]]}]},
        {[{<<"metric">>, {[{<<"name">>, <<"m2">>}, {<<"b">>, <<"b2">>},
                           {?DERIVED_PARAM_LABEL, <<"op2">>}]}},
          {<<"values">>, [               [2, <<"31">>], [3, <<"37">>]]}]},
        %% From node 2
        {[{<<"metric">>, {[{<<"name">>, <<"m1">>}, {<<"b">>, <<"b1">>},
                           {?DERIVED_PARAM_LABEL, <<"op1">>}]}},
          {<<"values">>, [[1, <<"41">>], [2, <<"43">>], [3, <<"47">>]]}]},
        {[{<<"metric">>, {[{<<"name">>, <<"m1">>}, {<<"b">>, <<"b2">>},
                           {?DERIVED_PARAM_LABEL, <<"op1">>}]}},
          {<<"values">>, [[1, <<"53">>], [2, <<"59">>], [3, <<"61">>]]}]},
        {[{<<"metric">>, {[{<<"name">>, <<"m2">>}, {<<"b">>, <<"b1">>},
                           {?DERIVED_PARAM_LABEL, <<"op2">>}]}},
          {<<"values">>, [[1, <<"67">>], [2, <<"71">>], [3, <<"73">>]]}]},
        {[{<<"metric">>, {[{<<"name">>, <<"m2">>}, {<<"b">>, <<"b2">>},
                           {?DERIVED_PARAM_LABEL, <<"op2">>}]}},
          {<<"values">>, [[1, <<"79">>], [2, <<"83">>], [3, <<"89">>]]}]}
    ],
    Res = aggregate_results(Values, ?DERIVED_PARAM_LABEL,
                            fun (#{<<"op1">> := Op1, <<"op2">> := Op2} = M) ->
                                 ?assertEqual(2, maps:size(M)),
                                 %% Need +1 to add some asymmetry
                                 aggregate(sum, Op1) * (aggregate(sum, Op2) + 1)
                            end),
    ?assertEqual([{[{<<"metric">>, {[{<<"b">>, <<"b1">>}]}},
                   {<<"values">>,
                    [[1, <<"3655">>], %% (2 + 41) * (17 + 67 + 1)
                     [2, <<"4186">>], %% (3 + 43) * (19 + 71 + 1)
                     [3, <<"5044">>]]}]}, %% (5 + 47) * (23 + 73 + 1)
                  {[{<<"metric">>, {[{<<"b">>, <<"b2">>}]}},
                    {<<"values">>,
                     [[1, <<"4800">>], %% (7 + 53) * ((miss) + 79 + 1)
                      [2, <<"8050">>], %% (11 + 59) * (31 + 83 + 1)
                      [3, <<"9398">>]]}]}], %% (13 + 61) * (37 + 89 + 1)
                 Res),

    DeleteParamLabel =
        fun ({P}) -> {proplists:delete(?DERIVED_PARAM_LABEL, P)} end,
    Values2 = [{misc:key_update(<<"metric">>, M, DeleteParamLabel)}
                   || {M} <- Values],

    {PName, AggFun} = regular_aggregation_fun(sum),
    Res2 = aggregate_results(Values2, PName, AggFun),

    ?assertEqual(
      [{[{<<"metric">>, {[{<<"b">>, <<"b1">>}, {<<"name">>, <<"m1">>}]}},
         {<<"values">>, [[1, <<"43">>], %% 2 + 41
                         [2, <<"46">>], %% 3 + 43
                         [3, <<"52">>]]}]}, %% 5 + 47
       {[{<<"metric">>, {[{<<"b">>, <<"b1">>}, {<<"name">>, <<"m2">>}]}},
         {<<"values">>, [[1, <<"84">>], %% 17 + 67
                         [2, <<"90">>], %% 19 + 71
                         [3, <<"96">>]]}]}, %% 23 + 73
       {[{<<"metric">>, {[{<<"b">>, <<"b2">>}, {<<"name">>, <<"m1">>}]}},
         {<<"values">>, [[1, <<"60">>], %% 7 + 53
                         [2, <<"70">>], %% 11 + 59
                         [3, <<"74">>]]}]}, %% 13 + 61
       {[{<<"metric">>, {[{<<"b">>, <<"b2">>}, {<<"name">>, <<"m2">>}]}},
         {<<"values">>, [[1, <<"79">>], %% (miss) + 79
                         [2, <<"114">>], %% 31 + 83
                         [3, <<"126">>]]}]} %% 37 + 89
      ], Res2).
-endif.

metrics_add_label(Res, Labels) ->
    lists:map(
      fun ({Props}) ->
          {misc:key_update(<<"metric">>, Props,
                           fun ({P}) -> {Labels ++ P} end)}
      end, Res).

merge_metrics(NodesResults, false, none) ->
    lists:flatmap(
      fun ({Node, List}) -> metrics_add_label(List, [{nodes, [Node]}]) end,
      NodesResults);
merge_metrics(NodesResults, false, AggFunctionName) ->
    {Nodes, ResultsLists} = lists:unzip(NodesResults),
    FlatResults = lists:flatten(ResultsLists),
    {AggLabel, AggFun} = regular_aggregation_fun(AggFunctionName),
    %% When request is not supposed to contain any named parameters we use
    %% 'default_param' atom to point to all the metrics without any param name
    %% label
    FlatAggregated = aggregate_results(FlatResults, AggLabel, AggFun),
    metrics_add_label(FlatAggregated, [{nodes, Nodes}]);
merge_metrics(NodesResults, DerivedMetricName, none) ->
    AggFun = derived_metrics:get_metric(DerivedMetricName, aggregation_fun),
    NodesResults2 = [{N, aggregate_results(R, ?DERIVED_PARAM_LABEL, AggFun)}
                         || {N, R} <- NodesResults],
    lists:flatmap(
      fun ({Node, List}) ->
          metrics_add_label(List, [{nodes, [Node]}, {name, DerivedMetricName}])
      end, NodesResults2);
merge_metrics(NodesResults, DerivedMetricName, special) ->
    AggFun = derived_metrics:get_metric(DerivedMetricName, aggregation_fun),
    {Nodes, ResultsLists} = lists:unzip(NodesResults),
    FlatResults = lists:flatten(ResultsLists),
    FlatAggregated = aggregate_results(FlatResults, ?DERIVED_PARAM_LABEL, AggFun),
    metrics_add_label(FlatAggregated, [{nodes, Nodes},
                                       {name, DerivedMetricName}]);

merge_metrics(NodesResults, DerivedMetricName, AggFunctionName) ->
    AggFun = derived_metrics:get_metric(DerivedMetricName, aggregation_fun),
    NodesResults2 = [{N, aggregate_results(R, ?DERIVED_PARAM_LABEL, AggFun)}
                         || {N, R} <- NodesResults],
    {Nodes, ResultsLists} = lists:unzip(NodesResults2),
    FlatResults = lists:flatten(ResultsLists),
    {AggLabel2, AggFun2} = regular_aggregation_fun(AggFunctionName),
    FlatAggregated = aggregate_results(FlatResults, AggLabel2, AggFun2),
    metrics_add_label(FlatAggregated, [{nodes, Nodes},
                                       {name, DerivedMetricName}]).

regular_aggregation_fun({histogram_quantile, Q}) ->
    {<<"le">>, aggregate_percentile(_, Q)};
regular_aggregation_fun(Name) ->
    {?NOT_EXISTING_LABEL,
     fun (#{default_param := P}) -> aggregate(Name, P) end}.

-ifdef(TEST).

merge_regular_metrics_test() ->
    Data = [{node1, [{[{<<"metric">>, {[{<<"name">>, <<"m1">>}]}},
                       {<<"values">>, [[1, <<"2">>]]}]},
                     {[{<<"metric">>, {[{<<"name">>, <<"m2">>}]}},
                       {<<"values">>, [[1, <<"5">>]]}]}]},
            {node2, [{[{<<"metric">>, {[{<<"name">>, <<"m1">>}]}},
                       {<<"values">>, [[1, <<"7">>]]}]},
                     {[{<<"metric">>, {[{<<"name">>, <<"m2">>}]}},
                       {<<"values">>, [[1, <<"11">>]]}]}]}],

    ?assertEqual(
     [{[{<<"metric">>, {[{nodes, [node1]}, {<<"name">>, <<"m1">>}]}},
        {<<"values">>, [[1, <<"2">>]]}]},
      {[{<<"metric">>, {[{nodes, [node1]}, {<<"name">>, <<"m2">>}]}},
        {<<"values">>, [[1, <<"5">>]]}]},
      {[{<<"metric">>, {[{nodes, [node2]}, {<<"name">>, <<"m1">>}]}},
        {<<"values">>, [[1, <<"7">>]]}]},
      {[{<<"metric">>, {[{nodes, [node2]}, {<<"name">>, <<"m2">>}]}},
        {<<"values">>, [[1, <<"11">>]]}]}],
     merge_metrics(Data, false, none)),

    ?assertEqual(
     [{[{<<"metric">>, {[{nodes, [node1, node2]}, {<<"name">>, <<"m1">>}]}},
        {<<"values">>, [[1, <<"9">>]]}]},
      {[{<<"metric">>, {[{nodes, [node1, node2]}, {<<"name">>, <<"m2">>}]}},
        {<<"values">>, [[1, <<"16">>]]}]}],
     merge_metrics(Data, false, sum)).


merge_derived_metrics_test() ->
    Data = [{node1, [{[{<<"metric">>, {[{<<"name">>, <<"m1">>},
                                        {?DERIVED_PARAM_LABEL, <<"p1">>}]}},
                       {<<"values">>, [[1, <<"11">>]]}]},
                     {[{<<"metric">>, {[%% that's ok if some names are missing
                                       {?DERIVED_PARAM_LABEL, <<"p1">>}]}},
                       {<<"values">>, [[1, <<"13">>]]}]},
                     {[{<<"metric">>, {[%% that's ok if some names are missing
                                       {?DERIVED_PARAM_LABEL, <<"p2">>}]}},
                       {<<"values">>, [[1, <<"17">>]]}]}]},
            {node2, [{[{<<"metric">>, {[{<<"name">>, <<"m1">>},
                                        {?DERIVED_PARAM_LABEL, <<"p1">>}]}},
                       {<<"values">>, [[1, <<"19">>]]}]},
                     {[{<<"metric">>, {[{<<"name">>, <<"m2">>},
                                        {?DERIVED_PARAM_LABEL, <<"p2">>}]}},
                       {<<"values">>, [[1, <<"23">>]]}]}]}],

    ?assertEqual(
     [{[{<<"metric">>, {[{nodes, [node1]}, {name, <<"test_derived_metric">>}]}},
        {<<"values">>, [[1, <<"432">>]]}]}, %% (11 + 13) * (17 + 1)
      {[{<<"metric">>, {[{nodes, [node2]}, {name, <<"test_derived_metric">>}]}},
        {<<"values">>, [[1, <<"456">>]]}]}], %% 19 * (23 + 1)
     merge_metrics(Data, <<"test_derived_metric">>, none)),

    ?assertEqual(
     [{[{<<"metric">>, {[{nodes, [node1, node2]},
                         {name, <<"test_derived_metric">>}]}},
        {<<"values">>, [[1, <<"888">>]]}]}], %% 432 + 456
     merge_metrics(Data, <<"test_derived_metric">>, sum)),

    ?assertEqual(
     [{[{<<"metric">>, {[{nodes, [node1, node2]},
                         {name, <<"test_derived_metric">>}]}},
        {<<"values">>, [[1, <<"1763">>]]}]}], %% (11 + 13 + 19) * (17 + 23 + 1)
     merge_metrics(Data, <<"test_derived_metric">>, special)).

-endif.


construct_promql_query(Labels, Functions, Window, PermFilters) ->
    {RangeVFunctions, InstantVFunctions} =
        lists:splitwith(fun is_range_vector_function/1, Functions),
    functools:chain(
      Labels,
      [lists:map(fun construct_promql_labels_ast/1, _),
       ?cut(lists:map(fun ({PermFilter}) -> {_ ++ PermFilter} end,
                      PermFilters)),
       case RangeVFunctions of
           [] -> fun functools:id/1;
           _ -> lists:map(fun (Ast) -> range_vector(Ast, Window) end, _)
       end,
       lists:map(fun (Ast) -> apply_functions(Ast, RangeVFunctions) end, _),
       {union, _},
       apply_functions(_, InstantVFunctions),
       promQL:format_promql(_)]).

derived_metric_query(Labels, Functions, Window, AuthorizationLabelsList) ->
    {RangeVFunctions, InstantVFunctions} =
        lists:splitwith(fun is_range_vector_function/1, Functions),

    LabelsAst = lists:map(fun construct_promql_labels_ast/1, Labels),

    {[{eq, <<"name">>, Name}], RestLabels} =
        lists:partition(
          fun ({eq, <<"name">>, _}) -> true;
              ({_, _, _}) -> false
          end, LabelsAst),

    Subqueries =
        lists:flatmap(
          fun ({AuthorizationLabels}) ->
              ExtraLabels = RestLabels ++ AuthorizationLabels,
              MetricFun =
                  fun MF(N) when is_binary(N) -> MF(promQL:metric(N));
                      MF({L}) ->
                          AST = {ExtraLabels ++ L},
                          case RangeVFunctions of
                              [] -> AST;
                              _ ->
                                  apply_functions(range_vector(AST, Window),
                                                  RangeVFunctions)
                          end
                  end,
              ParamAsts = (derived_metrics:get_metric(Name, query))(MetricFun),
              [promQL:with_label(?DERIVED_PARAM_LABEL, N,
                                 apply_functions(AST, InstantVFunctions))
               || {N, AST} <- maps:to_list(ParamAsts)]
          end, AuthorizationLabelsList),

    promQL:format_promql({union, Subqueries}).


-ifdef(TEST).

derived_metric_query_test() ->
    Labels = [{[{<<"label">>, <<"name">>},
                {<<"value">>, <<"test_derived_metric">>}]},
              {[{<<"label">>, <<"b">>},{<<"value">>, <<"b1">>}]}],
    ?assertEqual(
      <<"label_replace({b=`b1`,name=`m1`},"
                      "`__derived_param_name_label__`,`p1`,``,``) or "
        "label_replace({b=`b1`,name=`m2`},"
                      "`__derived_param_name_label__`,`p2`,``,``)">>,
      derived_metric_query(Labels, [], <<"1m">>, [{[]}])),
    ?assertEqual(
      <<"label_replace(sum(avg_over_time({b=`b1`,name=`m1`}[1m])),"
                      "`__derived_param_name_label__`,`p1`,``,``) or "
        "label_replace(sum(avg_over_time({b=`b1`,name=`m2`}[1m])),"
                      "`__derived_param_name_label__`,`p2`,``,``)">>,
      derived_metric_query(Labels, [avg_over_time, sum], <<"1m">>, [{[]}])),
    ?assertEqual(
      <<"label_replace("
          "sum(avg_over_time({b=`b1`,c=`c1`,d=`d1`,name=`m1`}[1m])),"
          "`__derived_param_name_label__`,`p1`,``,``) or "
        "label_replace("
          "sum(avg_over_time({b=`b1`,c=`c1`,d=`d1`,name=`m2`}[1m])),"
          "`__derived_param_name_label__`,`p2`,``,``) or "
        "label_replace("
          "sum(avg_over_time({b=`b1`,e=`e1`,name=`m1`}[1m])),"
          "`__derived_param_name_label__`,`p1`,``,``) or "
        "label_replace("
          "sum(avg_over_time({b=`b1`,e=`e1`,name=`m2`}[1m])),"
          "`__derived_param_name_label__`,`p2`,``,``)">>,
      derived_metric_query(Labels,
                           [avg_over_time, sum], <<"1m">>,
                           [{[{eq, <<"c">>, <<"c1">>},{eq, <<"d">>, <<"d1">>}]},
                            {[{eq, <<"e">>, <<"e1">>}]}])).

-endif.

construct_promql_labels_ast({MetricProps}) ->
    Label = proplists:get_value(<<"label">>, MetricProps),
    Value = proplists:get_value(<<"value">>, MetricProps),
    Op = case proplists:get_value(<<"operator">>, MetricProps, <<"=">>) of
             <<"=">> -> eq;
             <<"!=">> -> not_eq;
             <<"=~">> -> re;
             <<"!~">> -> not_re;
             <<"any">> -> eq_any;
             <<"not_any">> -> not_any
         end,
    {Op, Label, Value}.

range_vector(Ast, TimeWindow) -> {range_vector, Ast, TimeWindow}.

apply_functions(Ast, Functions) ->
    lists:foldl(
      fun ({histogram_quantile, Q}, AccAst) ->
              {call, histogram_quantile, none, [Q, AccAst]};
          ({F, by, Labels}, AccAst) ->
              {call, F, {by, Labels}, [AccAst]};
          (F, AccAst) ->
              {call, F, none, [AccAst]}
      end, Ast, Functions).

is_range_vector_function(Function) ->
    lists:member(Function, [rate, irate, increase, avg_over_time, min_over_time,
                            max_over_time, deriv, delta, idelta]).

validate_functions(Functions) ->
    lists:map(
      fun ("rate") -> rate;
          ("irate") -> irate;
          ("increase") -> increase;
          ("avg_over_time") -> avg_over_time;
          ("min_over_time") -> min_over_time;
          ("max_over_time") -> max_over_time;
          ("deriv") -> deriv;
          ("delta") -> delta;
          ("idelta") -> idelta;
          ("sum") -> sum;
          ("min") -> min;
          ("max") -> max;
          ("avg") -> avg;
          ("count") -> count;
          ("p" ++ PStr) ->
              {histogram_quantile, validate_percentile(PStr)};
          (_) -> error(invalid_function)
      end, Functions).

validate_percentile(PStr) ->
    try list_to_integer(PStr) of
        P when 0 < P; P =< 100  -> P/100;
        _ -> error(invalid_function)
    catch
        _:_ -> error(invalid_function)
    end.

%% If no unit is specified, we assume it's in seconds
validate_time_duration(Name, State) ->
    validator:validate(
      fun (Duration) when is_number(Duration), Duration =< 0.001 ->
              {error, <<"must be greater than 1ms">>};
          (Duration) when is_integer(Duration) ->
              {value, integer_to_list(Duration) ++ "s"};
          (Duration) when is_float(Duration) ->
              MS = round(Duration * 1000),
              {value, integer_to_list(MS) ++ "ms"};
          (Duration) ->
              DurationStr = case is_binary(Duration) of
                                true -> binary_to_list(Duration);
                                false -> Duration
                            end,
              try list_to_integer(DurationStr) of
                  Int -> {value, integer_to_list(Int) ++ "s"}
              catch _:_ ->
                  case promQL:parse_time_duration(DurationStr) of
                      {ok, _} -> {value, DurationStr};
                      {error, Error} -> {error, Error}
                  end
              end
      end, Name, State).

promql_filters_for_identity(Identity) ->
    Roles = menelaus_roles:get_roles(Identity),
    CompiledRoles = menelaus_roles:get_compiled_roles(Identity),
    promql_filters_for_roles(Roles, CompiledRoles).

promql_filters_for_roles(Roles, CompiledRoles) ->
    PermCheck =
        fun (Obj) ->
            menelaus_roles:is_allowed(collection_perm(Obj), CompiledRoles)
        end,
    case PermCheck([all, all, all]) of
        true -> [{[]}]; %% one empty filter
        false ->
            Definitions = menelaus_roles:get_definitions(all),
            %% Building a map that looks like the following:
            %% #{"bucket1" => true,
            %%   "bucket2" => #{"scope1" => true,
            %%                  "scope2" => #{"collection1" => true,
            %%                                "collection2" => true}}}
            PermMap = build_stats_perm_map(Roles, PermCheck, Definitions),
            convert_perm_map_to_promql_ast(PermMap)
    end.


-ifdef(TEST).

promql_filters_for_roles_test() ->
    Bucket1 = "test1",
    Bucket2 = "test2",
    UUID1 = <<"1bfe4b1738746c446747fa83b59930f1">>,
    UUID2 = <<"1bfe4b1738746c446747fa83b59930f1">>,
    ChronicleRev = {<<"A">>, 18},
    Manifest = [{uid,0},
                {next_uid,1},
                {next_scope_uid,8},
                {next_coll_uid,8},
                {num_scopes,0},
                {num_collections,0},
                {scopes,[{"_default",
                          [{uid,0},{collections,[{"_default",[{uid,0}]}]}]}]}],
    Snapshot = #{{bucket, Bucket1, collections} => {Manifest, ChronicleRev},
                 {bucket, Bucket2, collections} => {Manifest, ChronicleRev},
                 {bucket, Bucket1, uuid} => {UUID1, ChronicleRev},
                 {bucket, Bucket2, uuid} => {UUID2, ChronicleRev}},

    CompiledRoles = menelaus_roles:compile_roles(
                      _,
                      menelaus_roles:get_definitions(all),
                      Snapshot),

    Filters = fun (Roles) ->
                  AST = promql_filters_for_roles(Roles, CompiledRoles(Roles)),
                  promQL:format_promql({union, AST})
              end,

    meck:new(cluster_compat_mode, [passthrough]),
    try
        meck:expect(cluster_compat_mode, is_enterprise,
                    fun () -> true end),
        meck:expect(cluster_compat_mode, get_compat_version,
                    fun (_) -> ?LATEST_VERSION_NUM end),
        meck:expect(cluster_compat_mode, is_developer_preview,
                    fun () -> false end),

        ?assertBinStringsEqual(
          <<"{}">>,
          Filters(menelaus_roles:get_roles({"Admin", admin}))),

        ?assertBinStringsEqual(
          <<"{bucket=~`test1`} or {bucket!~`.+`}">>,
          Filters(menelaus_roles:get_roles({Bucket1, bucket}))),

        ?assertBinStringsEqual(
          <<"{}">>,
          Filters([{bucket_admin, [any]}])),

        ?assertBinStringsEqual(
          <<"{bucket=~`test1`} or {bucket!~`.+`}">>,
          Filters([{bucket_admin, [{Bucket1, UUID1}]}])),

        ?assertBinStringsEqual(
          <<"{bucket=~`test1|test2`} or {bucket!~`.+`}">>,
          Filters([{bucket_admin, [{Bucket1, UUID1}]},
                   {bucket_admin, [{Bucket2, UUID2}]}])),

        ?assertBinStringsEqual(
          <<"{}">>,
          Filters([{eventing_manage_functions, [any, any]}])),

        ?assertBinStringsEqual(
          <<"{bucket=~`test1`} or {bucket!~`.+`}">>,
          Filters([{eventing_manage_functions, [{Bucket1, UUID1}, any]}])),

        ?assertBinStringsEqual(
          <<"{bucket=`test1`,scope=~`_default`} or {bucket!~`.+`}">>,
          Filters([{eventing_manage_functions, [{Bucket1, UUID1},
                                                {"_default", 0}]}])),

        ?assertBinStringsEqual(
          <<"{bucket=`test1`,scope=~`_default`} or {bucket=~`test2`} or "
            "{bucket!~`.+`}">>,
          Filters([{eventing_manage_functions, [{Bucket1, UUID1},
                                                {"_default", 0}]},
                   {eventing_manage_functions, [{Bucket2, UUID2}, any]}])),

        ?assertBinStringsEqual(
          <<"{bucket=`test1`,scope=`_default`,collection=~`_default`} or "
            "{bucket=`test2`,scope=~`_default`} or {bucket!~`.+`}">>,
          Filters([{data_monitoring, [{Bucket1, UUID1}, {"_default", 0},
                                      {"_default", 0}]},
                   {data_monitoring, [{Bucket2, UUID2}, {"_default", 0},
                                      any]}])),

        ?assertBinStringsEqual(
          <<"{bucket=`test2`,scope=~`_default`} or {bucket=~`test1`} or "
            "{bucket!~`.+`}">>,
          Filters([{data_monitoring, [{Bucket1, UUID1}, {"_default", 0},
                                      {"_default", 0}]},
                   {data_monitoring, [{Bucket2, UUID2}, {"_default", 0}, any]},
                   {data_monitoring, [{Bucket1, UUID1}, any, any]}])),

        ?assertBinStringsEqual(
          <<"{bucket=~`test1`} or {bucket!~`.+`}">>,
          Filters([{data_reader, [any, any, any]},
                   {eventing_manage_functions, [{Bucket1, UUID1}, any]}])),

        ok
    after
        meck:unload(cluster_compat_mode)
    end.

-endif.

collection_perm([B, S, C]) -> {[{collection, [B, S, C]}, stats], read};
collection_perm([B, S]) ->    {[{collection, [B, S, all]}, stats], read};
collection_perm([B]) ->       {[{collection, [B, all, all]}, stats], read}.

build_stats_perm_map(UserRoles, PermCheckFun, RolesDefinitions) ->
    ParamDefs = menelaus_roles:get_param_defs(_, RolesDefinitions),
    StrippedParams =
        [{Defs, menelaus_roles:strip_ids(Defs, P)} || {R, P} <- UserRoles,
                                                      Defs <- [ParamDefs(R)]],

    %% Filtering out all the params that give full access to all buckets
    %% because: 1. the rest of the function assumes that bucket is a binary
    %%             string (not atom 'any');
    %%          2. perm filter is not needed in this case;
    %%          3. this case is handled outside of this function.
    FilteredParams =
        lists:filter(
          fun ({[bucket_name], [any]}) -> false;
              ({?RBAC_SCOPE_PARAMS, [any, any]}) -> false;
              ({?RBAC_COLLECTION_PARAMS, [any, any, any]}) -> false;
              (_) -> true
          end, StrippedParams),

    Params =
        misc:groupby_map(
          fun ({[bucket_name], B}) -> {buckets, B};
              ({?RBAC_SCOPE_PARAMS, [B, any]}) -> {buckets, [B]};
              ({?RBAC_COLLECTION_PARAMS, [B, any, any]}) -> {buckets, [B]};
              ({?RBAC_SCOPE_PARAMS, S}) -> {scopes, S};
              ({?RBAC_COLLECTION_PARAMS, [B, S, any]}) -> {scopes, [B, S]};
              ({?RBAC_COLLECTION_PARAMS, P}) -> {collections, P}
          end, FilteredParams),

    CheckParam =
        fun F([Obj], CheckPerm, Acc) ->
                case CheckPerm([Obj]) of
                    true -> Acc#{Obj => true};
                    false -> Acc
                end;
            F([Obj | T], CheckPerm, Acc) ->
                case maps:get(Obj, Acc, #{}) of
                    true -> Acc;
                    Map ->
                        NewCheckPerm = fun (E) -> CheckPerm([Obj | E]) end,
                        SubAcc = F(T, NewCheckPerm, Map),
                        Acc#{Obj => SubAcc}
                end
        end,

    CheckParamPerm = ?cut(CheckParam(_1, PermCheckFun, _2)),

    functools:chain(
      #{},
      [lists:foldl(CheckParamPerm, _,
                   proplists:get_value(buckets, Params, [])),
       lists:foldl(CheckParamPerm, _,
                   proplists:get_value(scopes, Params, [])),
       lists:foldl(CheckParamPerm, _,
                   proplists:get_value(collections, Params, []))]).

convert_perm_map_to_promql_ast(PermMap) ->
    AllowedBuckets = maps:keys(maps:filter(fun (_, V) -> V =:= true end,
                                           PermMap)),
    Filters =
        [{[{eq_any, "bucket", AllowedBuckets}]} || AllowedBuckets =/= []] ++
        maps:fold(
          fun (_Bucket, true, Acc) -> Acc;
              (Bucket, ScopePermMap, Acc) ->
                  AllowedScopes = maps:keys(
                                    maps:filter(fun (_, V) -> V =:= true end,
                                                ScopePermMap)),
                  [{[{eq, "bucket", Bucket},
                     {re, "scope", lists:join("|", AllowedScopes)}]}
                        || AllowedScopes =/= []] ++
                  maps:fold(
                    fun (_Scope, true, Acc2) -> Acc2;
                        (Scope, CollectionsPermMap, Acc2) ->
                            AllowedCols = maps:keys(
                                            maps:filter(
                                              fun (_, V) -> V =:= true end,
                                              CollectionsPermMap)),
                            [{[{eq, "bucket", Bucket},
                               {eq, "scope", Scope},
                               {re, "collection", lists:join("|", AllowedCols)}]}
                                    || AllowedCols =/= []] ++ Acc2
                    end, Acc, ScopePermMap)
          end, [], PermMap),
    case Filters of
        [] -> [];
        _ -> [{[{not_re, "bucket", ".+"}]} | Filters]
    end.


verify_derived_metrics(Metrics) ->
    AllDerivedMetrics = all_derived_metrics(),
    case [M || M <- Metrics, not lists:member(M, AllDerivedMetrics)] of
        [] -> {value, Metrics};
        InvalidMetrics ->
            {error, io_lib:format("Invalid derived metrics: ~p",
                                  [InvalidMetrics])}
    end.

all_derived_metrics() ->
    Services = [S || {S, _} <- all_services()],
    Settings = prometheus_cfg:settings(),
    [N || S <- Services, {N, _} <- prometheus_cfg:derived_metrics(S, Settings)].

all_services() ->
    [{S, atom_to_list(ns_cluster_membership:json_service_name(S))}
        || S <- [ns_server, xdcr |
                 ns_cluster_membership:supported_services()]].

validate_nodes_v2(Name, State, Req) ->
    validator:validate(
      fun (Nodes) ->
              {Right, Wrong} =
                  misc:partitionmap(
                    fun (HostName) ->
                            case menelaus_web_node:find_node_hostname(
                                   HostName, Req, any) of
                                {error, _} ->
                                    {right, HostName};
                                {ok, Node} ->
                                    {left, {Node, list_to_binary(HostName)}}
                            end
                    end, Nodes),
              case Wrong of
                  [] ->
                      {value, Right};
                  _ ->
                      {error, io_lib:format("Unknown hostnames: ~p", [Wrong])}
              end
      end, Name, State).

normalize_datapoints([], [], Acc) ->
    lists:reverse(Acc);
normalize_datapoints([T | Tail1], [[T, V] | Tail2], Acc) ->
    normalize_datapoints(Tail1, Tail2, [V | Acc]);
normalize_datapoints([_ | Tail1], Values, Acc) ->
    normalize_datapoints(Tail1, Values, [<<"NaN">> | Acc]).


%% Note: In order to be consistent with prometheus's quantile calculation, we
%% are making the same assumptions as they do, like the following:
%%  * samples are uniformly distributed within a bucket;
%%  * if the quantile falls into the most right bucket (N, +Inf), N is returned;
%%  * if there are less then 2 buckets, return NaN.
aggregate_percentile(_HistBucketsRaw, P) when P < 0 -> neg_infinity;
aggregate_percentile(_HistBucketsRaw, P) when P > 1 -> infinity;
aggregate_percentile(HistBucketsRaw, P) ->
    Buckets = lists:filtermap(
                fun ({default_param, _}) -> false;
                    ({Le, Values}) ->
                        {true, {promQL:parse_value(Le), aggregate(sum, Values)}}
                 end,  maps:to_list(HistBucketsRaw)),
    BucketsSorted = lists:sort(Buckets),
    TotalSamplesNum = proplists:get_value(infinity, BucketsSorted),
    case valid_hist_buckets(BucketsSorted) of
        false -> undefined;
        true ->
            PercentileSamplesNum = TotalSamplesNum * P,
            FindBucket = fun F(PrevLE, [{LE, Val} | Tail]) ->
                             case Val >= PercentileSamplesNum of
                                 true -> {PrevLE, LE};
                                 false -> F(LE, Tail)
                             end
                         end,
            case FindBucket(0, BucketsSorted) of
                {LeftLE, infinity} ->
                    %% Prometheus also returns the greatest LE in this case
                    proplists:get_value(LeftLE, BucketsSorted);
                {LeftLE, RightLE} ->
                    LeftValue = proplists:get_value(LeftLE, BucketsSorted, 0),
                    RightValue = proplists:get_value(RightLE, BucketsSorted),
                    BucketSamples = RightValue - LeftValue,
                    PercentileBucketSamples = PercentileSamplesNum - LeftValue,
                    LeftLE + (RightLE - LeftLE) * PercentileBucketSamples /
                                                  BucketSamples
            end
    end.


-ifdef(TEST).

generate_buckets(BucketsNum, SamplesNum, Mu, Sigma) ->
    V = Sigma*Sigma,
    Max = 2*Mu,
    BucketSize = Max / BucketsNum,
    Buckets =
        lists:foldl(
          fun (_, Acc) ->
              N = max(rand:normal(Mu, V), 1),
              Le = ceil(ceil(N / BucketSize) * BucketSize),
              maps:update_with(Le, fun (K) -> K + 1 end, 1, Acc)
          end, #{}, lists:seq(1, SamplesNum)),

    {Boundaries, _} = lists:mapfoldl(fun (_, B) ->
                                         {ceil(B + BucketSize), B + BucketSize}
                                     end, 0, lists:seq(1, BucketsNum)),

    {S, Res} = lists:foldl(
                 fun (Le, {Sum, Acc}) ->
                     Sum2 = Sum + maps:get(Le, Buckets, 0),
                     {Sum2, Acc#{integer_to_binary(Le) => [Sum2]}}
                 end, {0, #{}}, Boundaries),

    Res#{<<"+Inf">> => [S]}.

test_percentiles_with_random_normal_distribution() ->
    Mu = rand:uniform(100000) + 1000,
    Sigma = Mu * (rand:uniform()/2 + 0.5) / 4, %% not more than 4 sigmas to zero
    Buckets = generate_buckets(100, 100000, Mu, Sigma),
    CheckDeviation = fun (LeftBoundary, P, RightBoundary) ->
                         D = ((aggregate_percentile(Buckets, P) - Mu) / Sigma),
                         case LeftBoundary =< D andalso D =< RightBoundary of
                             true -> true;
                             false -> error({invalid_deviation, D, Mu, Sigma})
                         end
                     end,
    CheckDeviation(-3.3,  0.001, -2.8), %% should be -3*sigma from Mu
    CheckDeviation(-2.2,  0.023, -1.8), %% should be -2*sigma from Mu
    CheckDeviation(-1.2,  0.159, -0.8), %% should be -sigma from Mu
    CheckDeviation(-0.2,  0.5,    0.2), %% should be ~= Mu
    CheckDeviation( 0.8,  0.841,  1.2), %% should be sigma from Mu
    CheckDeviation( 1.8,  0.977,  2.2), %% should be 2*sigma from Mu
    CheckDeviation( 2.8,  0.999,  3.3). %% should be 3*sigma from Mu

aggregate_percentile_test() ->
    {timeout, 60,
     fun () ->
         [test_percentiles_with_random_normal_distribution()
            || _ <- lists:seq(0, 50)]
     end}.

-endif.

valid_hist_buckets(Buckets) when length(Buckets) < 2 -> false;
valid_hist_buckets(Buckets) -> valid_hist_buckets(0, Buckets).
valid_hist_buckets(_, []) -> true;
valid_hist_buckets(_, [{infinity, V} | _]) when V == 0 -> false;
valid_hist_buckets(_PrevVal, [{_Le, undefined} | _]) -> false;
valid_hist_buckets(_PrevVal, [{_Le, infinity} | _]) -> false;
valid_hist_buckets(_PrevVal, [{_Le, neg_infinity} | _]) -> false;
valid_hist_buckets(PrevVal, [{_Le, Val} | _]) when Val < PrevVal -> false;
valid_hist_buckets(_PrevVal, [{_Le, Val} | T]) -> valid_hist_buckets(Val, T).

aggregate(sum, List) -> foldl2(fun prometheus_sum/2, undefined, List);
aggregate(max, List) -> foldl2(fun prometheus_max/2, undefined, List);
aggregate(min, List) -> foldl2(fun prometheus_min/2, undefined, List);
aggregate(avg, List) ->
    case aggregate(sum, List) of
        undefined -> undefined;
        infinity -> infinity;
        neg_infinity -> neg_infinity;
        N -> N / length(List)
    end;
aggregate('div', [Op1, Op2]) ->
    if Op2 == 0 -> undefined;
       is_number(Op1) andalso is_number(Op2) -> Op1 / Op2;
       Op2 == undefined -> undefined;
       Op2 == infinity andalso is_number(Op1) -> 0;
       Op2 == neg_infinity andalso is_number(Op1) -> 0;
       Op1 == undefined -> undefined;
       Op1 == infinity andalso is_number(Op2) -> infinity;
       Op1 == neg_infinity andalso is_number(Op2) -> neg_infinity;
       is_atom(Op1) andalso is_atom(Op2) -> undefined
    end.

foldl2(_, Acc, []) -> Acc;
foldl2(F, Acc, [E | Tail]) ->
    case F(E, Acc) of
        {stop, Res} -> Res;
        {ok, NewAcc} -> foldl2(F, NewAcc, Tail)
    end.

-ifdef(TEST).

aggregate_test() ->
    ?assertEqual(undefined, aggregate(sum, [])),
    ?assertEqual(undefined, aggregate(max, [])),
    ?assertEqual(undefined, aggregate(min, [])),
    ?assertEqual(undefined, aggregate(avg, [])).

aggregate_randomized_test() ->
    RandomList = fun (N, K, Extra) ->
            L = lists:seq(1,N),
            Undefined = lists:duplicate(K, undefined),
            {lists:sum(L), misc:shuffle(L ++ Undefined ++ Extra)}
        end,
    lists:foreach(
      fun (_) ->
              N = rand:uniform(30),
              K = rand:uniform(20),
              {Sum, List} = RandomList(N, K, []),
              ?assertEqual(Sum, aggregate(sum, List)),
              ?assertEqual(N, aggregate(max, List)),
              ?assertEqual(1, aggregate(min, List)),
              ?assertEqual(Sum / length(List), aggregate(avg, List)),

              {_, List2} = RandomList(N, K, [infinity]),
              ?assertEqual(infinity, aggregate(sum, List2)),
              ?assertEqual(infinity, aggregate(max, List2)),
              ?assertEqual(1, aggregate(min, List2)),
              ?assertEqual(infinity, aggregate(avg, List2)),

              {_, List3} = RandomList(N, K, [neg_infinity]),
              ?assertEqual(neg_infinity, aggregate(sum, List3)),
              ?assertEqual(N, aggregate(max, List3)),
              ?assertEqual(neg_infinity, aggregate(min, List3)),
              ?assertEqual(neg_infinity, aggregate(avg, List3)),

              {_, List4} = RandomList(N, K, lists:duplicate(3, neg_infinity) ++
                                            lists:duplicate(3, infinity)),
              ?assertEqual(undefined, aggregate(sum, List4)),
              ?assertEqual(infinity, aggregate(max, List4)),
              ?assertEqual(neg_infinity, aggregate(min, List4)),
              ?assertEqual(undefined, aggregate(avg, List4))
      end, lists:seq(1,1000)).

aggregate_div_test() ->
    ?assertEqual(3.0, aggregate('div', [6, 2])),
    ?assertEqual(undefined, aggregate('div', [6, 0])),
    ?assertEqual(undefined, aggregate('div', [undefined, 123])),
    ?assertEqual(undefined, aggregate('div', [123, undefined])),
    ?assertEqual(undefined, aggregate('div', [undefined, infinity])),
    ?assertEqual(undefined, aggregate('div', [undefined, neg_infinity])),
    ?assertEqual(undefined, aggregate('div', [infinity, infinity])),
    ?assertEqual(undefined, aggregate('div', [neg_infinity, infinity])),
    ?assertEqual(undefined, aggregate('div', [infinity, neg_infinity])),
    ?assertEqual(undefined, aggregate('div', [infinity, undefined])),
    ?assertEqual(undefined, aggregate('div', [neg_infinity, undefined])),
    ?assertEqual(undefined, aggregate('div', [infinity, 0])),
    ?assertEqual(undefined, aggregate('div', [neg_infinity, 0])),
    ?assertEqual(0, aggregate('div', [2, infinity])),
    ?assertEqual(0, aggregate('div', [2, neg_infinity])),
    ?assertEqual(infinity, aggregate('div', [infinity, 2])),
    ?assertEqual(neg_infinity, aggregate('div', [neg_infinity, 2])).

-endif.

prometheus_sum(undefined, V) -> {ok, V};
prometheus_sum(V, undefined) -> {ok, V};
prometheus_sum(infinity, neg_infinity) -> {stop, undefined};
prometheus_sum(neg_infinity, infinity) -> {stop, undefined};
prometheus_sum(infinity, _) -> {ok, infinity};
prometheus_sum(_, infinity) -> {ok, infinity};
prometheus_sum(neg_infinity, _) -> {ok, neg_infinity};
prometheus_sum(_, neg_infinity) -> {ok, neg_infinity};
prometheus_sum(V1, V2) -> {ok, V1 + V2}.

prometheus_max(undefined, V) -> {ok, V};
prometheus_max(V, undefined) -> {ok, V};
prometheus_max(infinity, _) -> {stop, infinity};
prometheus_max(_, infinity) -> {stop, infinity};
prometheus_max(V, neg_infinity) -> {ok, V};
prometheus_max(neg_infinity, V) -> {ok, V};
prometheus_max(V1, V2) when V1 >= V2 -> {ok, V1};
prometheus_max(_V1, V2) -> {ok, V2}.

prometheus_min(undefined, V) -> {ok, V};
prometheus_min(V, undefined) -> {ok, V};
prometheus_min(neg_infinity, _) -> {stop, neg_infinity};
prometheus_min(_, neg_infinity) -> {stop, neg_infinity};
prometheus_min(V, infinity) -> {ok, V};
prometheus_min(infinity, V) -> {ok, V};
prometheus_min(V1, V2) when V1 >= V2 -> {ok, V2};
prometheus_min(V1, _V2) -> {ok, V1}.

validate_negative_ts(Name, Now, State) ->
    validator:validate(
      fun (TS) when TS < 0 ->
              {value, Now + TS};
          (_) ->
              ok
      end, Name, State).

maybe_align_start(State) ->
    validator:validate_relative(
      fun (_Start, false) -> ok;
          (Start, true) ->
              Step = validator:get_value(step, State),
              End = validator:get_value('end', State),
              case (Step =/= undefined) andalso (End =/= undefined) of
                  true ->
                      {ok, StepMs} = promQL:parse_time_duration(Step),
                      StartMs = Start * 1000,
                      Aligned = (math:ceil(StartMs / StepMs) * StepMs) / 1000,
                      case Aligned > End of
                          true -> {error, "[start, end] interval doesn't "
                                          "contain any aligned datapoints"};
                          false -> {value, Aligned}
                      end;
                  false ->
                      %% it doesn't make sense to do the check if 'end' or
                      %% 'step' is invalid
                      ok
              end
      end, start, alignTimestamps, State).

format_error(Bin) when is_binary(Bin) -> Bin;
format_error(timeout) -> <<"Request timed out">>;
format_error({exit, {{nodedown, _}, _}}) -> <<"Node is down">>;
format_error({exit, _}) -> <<"Unexpected server error">>;
format_error({failed_connect, _}) -> <<"Connect to stats backend failed">>;
format_error(down) -> <<"Node is not available">>;
format_error(Unknown) -> misc:format_bin("Unexpected error - ~10000p",
                                         [Unknown]).

extract_metric_name([]) -> undefined;
extract_metric_name([{Props} | Labels]) ->
    case proplists:get_value(<<"label">>, Props) of
        <<"name">> -> proplists:get_value(<<"value">>, Props);
        _ -> extract_metric_name(Labels)
    end.
