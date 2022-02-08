%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(derived_metrics).

-export([is_metric/1, get_metric/2]).

-include("cut.hrl").

is_metric(Name) ->
    get_metric(Name) =/= [].

get_metric(Name, Key) ->
    proplists:get_value(Key, get_metric(Name)).

get_metric(<<"n1ql_avg_req_time">>) ->
    ratio(_(<<"n1ql_request_time">>), _(<<"n1ql_requests">>));
get_metric(<<"n1ql_avg_svc_time">>) ->
    ratio(_(<<"n1ql_service_time">>), _(<<"n1ql_requests">>));
get_metric(<<"n1ql_avg_response_size">>) ->
    ratio(_(<<"n1ql_result_size">>), _(<<"n1ql_requests">>));
get_metric(<<"n1ql_avg_result_count">>) ->
    ratio(_(<<"n1ql_result_count">>), _(<<"n1ql_requests">>));
get_metric(<<"index_ram_percent">>) ->
    percent(_(<<"index_memory_used_total">>), _(<<"index_memory_quota">>), 100);
get_metric(<<"index_remaining_ram">>) ->
    sum_across_labels(
      fun (M) ->
          Diff = promQL:op('-', [M(<<"index_memory_quota">>),
                                 M(<<"index_memory_used_total">>)]),
          promQL:clamp_min(Diff, 0)
      end, []);
get_metric(<<"index_num_docs_pending_and_queued">>) ->
    sum([<<"index_num_docs_pending">>, <<"index_num_docs_queued">>]);
get_metric(<<"index_cache_miss_ratio">>) ->
    percent(_(<<"index_cache_misses">>),
            fun (M) ->
                promQL:op('+', [M(<<"index_cache_misses">>),
                                M(<<"index_cache_hits">>)])
            end, 0);
get_metric(<<"index_fragmentation">>) ->
    ratio(
      fun (M) ->
            promQL:sum_without([<<"index">>, <<"collection">>, <<"scope">>],
                               promQL:op('*', [M(<<"index_disk_size">>),
                                               M(<<"index_frag_percent">>)]))
      end,
      fun (M) ->
            promQL:sum_without([<<"index">>, <<"collection">>, <<"scope">>],
                               M(<<"index_disk_size">>))
      end, 100);
get_metric(<<"index_resident_percent">>) ->
    ratio(fun (M) ->
              promQL:op('*', [M(<<"index_resident_percent">>),
                              M(<<"index_data_size">>)])
          end,
          _(<<"index_data_size">>), 100);
get_metric(<<"couch_total_disk_size">>) ->
    sum([<<"couch_docs_actual_disk_size">>,
         <<"couch_views_actual_disk_size">>]);
get_metric(<<"couch_docs_fragmentation">>) ->
    opposite_percent(_(<<"kv_ep_db_data_size_bytes">>),
                     _(<<"kv_ep_db_file_size_bytes">>));
get_metric(<<"couch_views_fragmentation">>) ->
    opposite_percent(_(<<"couch_views_data_size">>),
                     _(<<"couch_views_disk_size">>));
get_metric(<<"kv_hit_ratio">>) ->
    percent(?cut(promQL:sum_without([<<"result">>, <<"op">>],
                                    _({[{eq, <<"name">>, <<"kv_ops">>},
                                        {eq, <<"op">>, <<"get">>},
                                        {eq, <<"result">>, <<"hit">>}]}))),
            ?cut(promQL:sum_without([<<"result">>, <<"op">>],
                                    _({[{eq, <<"name">>, <<"kv_ops">>},
                                        {eq, <<"op">>, <<"get">>}]}))), 100);
get_metric(<<"kv_ep_cache_miss_ratio">>) ->
    percent(_(<<"kv_ep_bg_fetched">>),
            ?cut(promQL:sum_without([<<"op">>, <<"result">>],
                                    _({[{eq, <<"name">>, <<"kv_ops">>},
                                        {eq, <<"op">>, <<"get">>}]}))), 0);
get_metric(<<"kv_ep_resident_items_ratio">>) ->
    opposite_percent(_(<<"kv_ep_num_non_resident">>),
                     _(<<"kv_curr_items_tot">>));
get_metric(<<"kv_vb_avg_queue_age_seconds">>) ->
    ratio(_(<<"kv_vb_queue_age_seconds">>), _(<<"kv_vb_queue_size">>));
get_metric(<<"kv_vb_avg_total_queue_age_seconds">>) ->
    ratio(?cut(promQL:sum_without([<<"state">>],
                                  _(<<"kv_vb_queue_age_seconds">>))),
          ?cut(promQL:sum_without([<<"state">>],
                                  _(<<"kv_vb_queue_size">>))));
get_metric(<<"kv_avg_disk_time_seconds">>) ->
    ratio(_(<<"kv_disk_seconds_sum">>), _(<<"kv_disk_seconds_count">>));
get_metric(<<"kv_avg_bg_wait_time_seconds">>) ->
    ratio(_(<<"kv_bg_wait_seconds_sum">>), _(<<"kv_bg_wait_seconds_count">>));
get_metric(<<"kv_avg_timestamp_drift_seconds">>) ->
    ratio(_(<<"kv_ep_hlc_drift_seconds">>), _(<<"kv_ep_hlc_drift_count">>));
get_metric(<<"kv_disk_write_queue">>) ->
    sum([<<"kv_ep_flusher_todo">>, <<"kv_ep_queue_size">>]);
get_metric(<<"kv_ep_ops_create">>) ->
    sum_across_labels(_(<<"kv_vb_ops_create">>), [<<"state">>]);
get_metric(<<"kv_ep_ops_update">>) ->
    sum_across_labels(_(<<"kv_vb_ops_update">>), [<<"state">>]);
get_metric(<<"kv_xdc_ops">>) ->
    sum_across_labels(
      _(promQL:re(<<"op">>, <<"del_meta|get_meta|set_meta">>,
                  promQL:metric(<<"kv_ops">>))),
      [<<"op">>, <<"result">>]);
get_metric(<<"kv_vb_resident_items_ratio">>) ->
    opposite_percent(_(<<"kv_vb_num_non_resident">>),
                     _(<<"kv_vb_curr_items">>));
get_metric(<<"xdcr_percent_completeness">>) ->
    percent(_(<<"xdcr_docs_processed_total">>),
            fun (M) ->
                promQL:sum_without([<<"name">>],
                                   {'or', [M(<<"xdcr_docs_processed_total">>),
                                           M(<<"xdcr_changes_left_total">>)]})
            end, 100);
get_metric(<<"eventing_processed_count">>) ->
    sum([<<"eventing_timer_callback_success">>,
         <<"eventing_on_delete_success">>,
         <<"eventing_on_update_success">>]);
get_metric(<<"eventing_failed_count">>) ->
    sum([<<"eventing_bucket_op_exception_count">>,
         <<"eventing_checkpoint_failure_count">>,
         <<"eventing_doc_timer_create_failure">>,
         <<"eventing_n1ql_op_exception_count">>,
         <<"eventing_non_doc_timer_create_failure">>,
         <<"eventing_on_delete_failure">>,
         <<"eventing_on_update_failure">>,
         <<"eventing_timer_callback_failure">>,
         <<"eventing_timeout_count">>]);
%% Used by unit tests:
get_metric(<<"test_derived_metric">>) ->
    [{aggregation_fun, fun (#{<<"p1">> := P1, <<"p2">> := P2}) ->
                           menelaus_web_stats:aggregate(sum, P1) *
                           (menelaus_web_stats:aggregate(sum, P2) + 1)
                       end},
     {query, fun (M) ->
                 #{<<"p1">> => M(<<"m1">>), <<"p2">> => M(<<"m2">>)}
             end}];
get_metric(_) -> [].

aggregated_ratio(Values1, Values2, DivisionByZeroDefault) ->
    case menelaus_web_stats:aggregate(sum, Values2) of
        0 -> DivisionByZeroDefault;
        Total ->
            menelaus_web_stats:aggregate(
              'div', [menelaus_web_stats:aggregate(sum, Values1), Total])
    end.

sum(MetricNames) ->
    sum_across_labels(_(promQL:eq_any(<<"name">>, MetricNames)), [<<"name">>]).

sum_across_labels(Metric, Labels) ->
    [{aggregation_fun, fun (#{<<"Param1">> := P1}) ->
                           menelaus_web_stats:aggregate(sum, P1)
                       end},
     {query, fun (M) ->
                 #{<<"Param1">> => promQL:sum_without(Labels, Metric(M))}
             end}].

ratio(Numerator, Denominator, Default) ->
    [{aggregation_fun, fun (#{<<"Param1">> := P1, <<"Param2">> := P2}) ->
                           aggregated_ratio(P1, P2, Default)
                       end},
     {query, fun (M) ->
                 #{<<"Param1">> => Numerator(M), <<"Param2">> => Denominator(M)}
             end}].

ratio(Numerator, Denominator) ->
    ratio(Numerator, Denominator, undefined).

percent(Numerator, Denominator, Default) ->
    ratio(?cut(promQL:multiply_by_scalar(Numerator(_), 100)), Denominator,
          Default).

opposite_percent(Numerator, Denominator) ->
    percent(fun (M) -> promQL:op('-', [Denominator(M), Numerator(M)]) end,
            Denominator, 100).
