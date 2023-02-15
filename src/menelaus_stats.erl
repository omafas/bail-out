%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Web server for menelaus.

-module(menelaus_stats).
-author('NorthScale <info@northscale.com>').

-include("ns_stats.hrl").
-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_bucket_stats/3,
         handle_stats_section/3,
         handle_bucket_node_stats/4,
         handle_stats_section_for_node/4,
         handle_specific_stat_for_buckets/4,
         basic_stats/1,
         bucket_disk_usage/1,
         bucket_ram_usage/1,
         build_bucket_stats_ops_response/4,
         serve_stats_directory/3,
         serve_ui_stats/1,
         handle_ui_stats_post/1]).


%% External API
bucket_disk_usage(BucketName) ->
    {_, _, _, _, DiskUsed, _, _}
        = last_membase_sample(BucketName, live_bucket_nodes(BucketName)),
    DiskUsed.

bucket_ram_usage(BucketName) ->
    %% NOTE: we're getting last membase sample, but first stat name is
    %% same in memcached buckets, so it works for them too.
    element(1, last_membase_sample(BucketName, live_bucket_nodes(BucketName))).

get_node_infos(NodeNames) ->
    NodesDict = ns_doctor:get_nodes(),
    lists:foldl(fun (N, A) ->
                        case dict:find(N, NodesDict) of
                            {ok, V} -> [{N, V} | A];
                            _ -> A
                        end
                end, [], NodeNames).

grab_latest_bucket_stats(BucketName, Nodes) ->
    NodeInfos = get_node_infos(Nodes),
    Stats = extract_interesting_buckets(BucketName, NodeInfos),
    {FoundNodes, _} = lists:unzip(Stats),
    RestNodes = Nodes -- FoundNodes,
    RestStats =
        case Nodes -- FoundNodes of
            [] -> [];
            RestNodes ->
                case cluster_compat_mode:is_cluster_70() of
                    true ->
                        stats_interface:buckets_interesting(RestNodes);
                    false ->
                        Entries = menelaus_stats_gatherer:invoke_archiver(
                                    BucketName, RestNodes, {1, minute, 1}),
                        [{N, S} || {N, [#stat_entry{values=S} | _]} <- Entries]
                end
        end,
    Stats ++ RestStats.

extract_interesting_stat(Key, Stats) ->
    case lists:keyfind(Key, 1, Stats) of
        false -> 0;
        {_, Stat} -> Stat
    end.

extract_interesting_buckets(BucketName, NodeInfos) ->
    Stats0 =
        lists:map(
          fun ({Node, NodeInfo}) ->
                  case proplists:get_value(per_bucket_interesting_stats, NodeInfo) of
                      undefined ->
                          [];
                      NodeStats ->
                          case [S || {B, S} <- NodeStats, B =:= BucketName] of
                              [BucketStats] ->
                                  [{Node, BucketStats}];
                              _ ->
                                  []
                          end
                  end
          end, NodeInfos),

    lists:append(Stats0).

last_membase_sample(BucketName, Nodes) ->
    lists:foldl(
      fun ({_, Stats},
           {AccMem, AccItems, AccOps, AccFetches, AccDisk, AccData, AccActiveNonRes}) ->
              {extract_interesting_stat(mem_used, Stats) + AccMem,
               extract_interesting_stat(curr_items, Stats) + AccItems,
               extract_interesting_stat(ops, Stats) + AccOps,
               extract_interesting_stat(ep_bg_fetched, Stats) + AccFetches,
               extract_interesting_stat(couch_docs_actual_disk_size, Stats) +
                   extract_interesting_stat(couch_spatial_disk_size, Stats) +
                   extract_interesting_stat(couch_views_actual_disk_size, Stats) + AccDisk,
               extract_interesting_stat(couch_docs_data_size, Stats) +
                   extract_interesting_stat(couch_views_data_size, Stats) +
                   extract_interesting_stat(couch_spatial_data_size, Stats) + AccData,
               extract_interesting_stat(vb_active_num_non_resident, Stats) + AccActiveNonRes}
      end, {0, 0, 0, 0, 0, 0, 0}, grab_latest_bucket_stats(BucketName, Nodes)).



last_memcached_sample(BucketName, Nodes) ->
    {MemUsed, CurrItems, Ops, CmdGet, GetHits}
        = lists:foldl(
            fun ({_, Stats},
                 {AccMem, AccItems, AccOps, AccGet, AccGetHits}) ->
                    {extract_interesting_stat(mem_used, Stats) + AccMem,
                     extract_interesting_stat(curr_items, Stats) + AccItems,
                     extract_interesting_stat(ops, Stats) + AccOps,
                     extract_interesting_stat(cmd_get, Stats) + AccGet,
                     extract_interesting_stat(get_hits, Stats) + AccGetHits}
            end, {0, 0, 0, 0, 0}, grab_latest_bucket_stats(BucketName, Nodes)),

    {MemUsed, CurrItems, Ops,
     case CmdGet == 0 of
         true -> 0;
         _ -> GetHits / CmdGet
     end}.

last_bucket_stats(membase, BucketName, Nodes) ->
    {MemUsed, ItemsCount, Ops, Fetches, Disk, Data, ActiveNonRes}
        = last_membase_sample(BucketName, Nodes),
    [{opsPerSec, Ops},
     {diskFetches, Fetches},
     {itemCount, ItemsCount},
     {diskUsed, Disk},
     {dataUsed, Data},
     {memUsed, MemUsed},
     {vbActiveNumNonResident, ActiveNonRes}];
last_bucket_stats(memcached, BucketName, Nodes) ->
    {MemUsed, ItemsCount, Ops, HitRatio} = last_memcached_sample(BucketName, Nodes),
    [{opsPerSec, Ops},
     {hitRatio, HitRatio},
     {itemCount, ItemsCount},
     {memUsed, MemUsed}].

basic_stats(BucketName) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    QuotaBytes = ns_bucket:ram_quota(BucketConfig),
    BucketType = ns_bucket:bucket_type(BucketConfig),
    BucketNodes = ns_bucket:live_bucket_nodes_from_config(BucketConfig),
    Stats = last_bucket_stats(BucketType, BucketName, BucketNodes),
    MemUsed = proplists:get_value(memUsed, Stats),
    QuotaPercent = try (MemUsed * 100.0 / QuotaBytes) of
                       X -> X
                   catch
                       error:badarith -> 0
                   end,
    [{quotaPercentUsed, lists:min([QuotaPercent, 100])}
     | Stats].

stats_read_permission(BucketName) ->
    {[{bucket, BucketName}, stats], read}.

%% GET /pools/{PoolID}/buckets/{Id}/stats
handle_bucket_stats(_PoolId, Id, Req) ->
    Params = mochiweb_request:parse_qs(Req),
    PropList1 = build_bucket_stats_ops_response(all, Id, Params, true),
    menelaus_util:reply_json(Req, {struct, PropList1}).

check_bucket(BucketName, Req) ->
    Permission = stats_read_permission(BucketName),
    case menelaus_auth:has_permission(Permission, Req) of
        true ->
            case lists:member(BucketName, ns_bucket:get_bucket_names()) of
                true ->
                    ok;
                _ ->
                    not_found
            end;
        false ->
            case ns_bucket:is_valid_bucket_name(BucketName) of
                true ->
                    {forbidden, Permission};
                {error, _} ->
                    not_found
            end
    end.

with_valid_bucket(Fun, Bucket, Req) ->
    case check_bucket(Bucket, Req) of
        ok ->
            Fun();
        not_found ->
            menelaus_util:reply_not_found(Req);
        {forbidden, Permission} ->
            ns_audit:auth_failure(Req),
            menelaus_util:reply_json(
              Req, menelaus_web_rbac:forbidden_response([Permission]), 403)
    end.

with_valid_section_id(Fun, Id, Req) ->
    case section_bucket(Id) of
        undefined ->
            Fun();
        BucketName ->
            with_valid_bucket(Fun, BucketName, Req)
    end.

handle_stats_section(_PoolId, Id, Req) ->
    with_valid_section_id(
      fun () ->
              Params = mochiweb_request:parse_qs(Req),
              PropList1 =
                  build_bucket_stats_ops_response(all, Id, Params, false),
              menelaus_util:reply_json(Req, {struct, PropList1})
      end, Id, Req).

%% Per-Node Stats
%% GET /pools/{PoolID}/buckets/{Id}/nodes/{NodeId}/stats
%%
%% Per-node stats match bucket stats with the addition of a 'hostname' key,
%% stats specific to the node (obviously), and removal of any cross-node stats
handle_bucket_node_stats(_PoolId, BucketName, HostName, Req) ->
    case menelaus_web_node:find_node_hostname(HostName, Req) of
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, {invalid_node, Reason}} ->
            menelaus_util:reply_text(Req, Reason, 400);
        {ok, Node} ->
            Params = mochiweb_request:parse_qs(Req),
            Ops = build_bucket_stats_ops_response([Node], BucketName, Params, true),
            menelaus_util:reply_json(
              Req,
              {struct, [{hostname, list_to_binary(HostName)}
                        | Ops]})
    end.

handle_stats_section_for_node(_PoolId, Id, HostName, Req) ->
    case menelaus_web_node:find_node_hostname(HostName, Req) of
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, {invalid_node, Reason}} ->
            menelaus_util:reply_text(Req, Reason, 400);
        {ok, Node} ->
            with_valid_section_id(
              fun () ->
                      case lists:member(Node, section_nodes(Id)) of
                          true ->
                              do_handle_stats_section_for_node(
                                Id, HostName, Node, Req);
                          false ->
                              menelaus_util:reply_not_found(Req)
                      end
              end, Id, Req)
    end.

do_handle_stats_section_for_node(Id, HostName, Node, Req) ->
    Params = mochiweb_request:parse_qs(Req),
    Ops = build_bucket_stats_ops_response([Node], Id, Params, false),
    menelaus_util:reply_json(
      Req,
      {struct, [{hostname, list_to_binary(HostName)}
                | Ops]}).

%% Specific Stat URL grouped by nodes
%% GET /pools/{PoolID}/buckets/{Id}/stats/{StatName}
%%
%% Req:ok({"application/json",
%%         menelaus_util:server_header(),
%%         <<"{
%%     \"timestamp\": [1,2,3,4,5],
%%     \"nodeStats\": [{\"127.0.0.1:9000\": [1,2,3,4,5]},
%%                     {\"127.0.0.1:9001\": [1,2,3,4,5]}]
%%   }">>}).
handle_specific_stat_for_buckets(_PoolId, BucketName, StatName, Req) ->
    Params = mochiweb_request:parse_qs(Req),
    menelaus_util:reply_json(
      Req,
      build_response_for_specific_stat(BucketName, StatName, Params, menelaus_util:local_addr(Req))).

%% Function to extract a simple stat from a list of stats.
build_simple_stat_extractor(StatAtom, StatBinary) ->
    fun (#stat_entry{timestamp = TS, values = VS}) ->
            V = case dict_safe_fetch(StatAtom, VS, undefined) of
                    undefined ->
                        dict_safe_fetch(StatBinary, VS, undefined);
                    V1 ->
                        V1
                end,

            {TS, V}
    end.

%% Function to extract a raw stat from a list of stats.
build_raw_stat_extractor(StatBinary) ->
    fun (#stat_entry{timestamp = TS, values = VS}) ->
            {TS, dict_safe_fetch(StatBinary, VS, undefined)}
    end.

%%
%% Some stats are computed using other stats.
%% Stats used in the computation are gathered and the ComputeFun is then
%% applied on them to extract the relevant stat.
%%
build_computed_stat_extractor(ComputeFun, Stats) ->
    fun (#stat_entry{timestamp = TS, values = VS}) ->
            Args = [dict_safe_fetch(Name, VS, undefined) || Name <- Stats],
            case lists:member(undefined, Args) of
                true ->
                    {TS, undefined};
                _ ->
                    {TS, erlang:apply(ComputeFun, Args)}
            end
    end.

%%
%% For the specified StatName, build the list of stats that need to be gathered
%% and the function to extract them.
%% E.g. for a computed stat, vb_active_resident_items_ratio, it will return
%% following:
%%      {[vb_active_num_non_resident, curr_items], <extractor_function>}.
%%
build_stat_list_and_extractor(BucketName, StatName) ->
    ExtraStats = computed_stats_lazy_proplist(BucketName),
    build_stat_list_and_extractor_inner(ExtraStats, StatName).

build_stat_list_and_extractor_inner(ExtraStats, StatName) ->
    StatBinary = list_to_binary(StatName),
    case lists:keyfind(StatBinary, 1, ExtraStats) of
        {_K, {F, Meta}} ->
            {Meta, build_computed_stat_extractor(F, Meta)};
        false ->
            Stat = try
                       {ok, list_to_existing_atom(StatName)}
                   catch
                       error:badarg ->
                           error
                   end,

            case Stat of
                {ok, StatAtom} ->
                    {[StatAtom, StatBinary],
                     build_simple_stat_extractor(StatAtom, StatBinary)};
                error ->
                    {[StatBinary],
                     build_raw_stat_extractor(StatBinary)}
            end
    end.

dict_safe_fetch(K, Dict, Default) ->
    case lists:keyfind(K, 1, Dict) of
        {_, V} -> V;
        _ -> Default
    end.

-record(gathered_stats, {kind, samples, nodes, extractor}).

are_samples_undefined(#gathered_stats{samples = Samples}) ->
    lists:all(
      fun (NodeSamples) ->
              lists:all(
                fun (#stat_entry{values = VS}) ->
                        lists:all(
                          fun ({_, undefined}) ->
                                  true;
                              ({_, _}) ->
                                  false
                          end, VS)
                end, NodeSamples)
      end, Samples).

calculate_stats(Extractor, keep_undefineds, Samples) ->
    lists:map(Extractor, Samples);
calculate_stats(Extractor, remove_undefineds, Samples) ->
    lists:filtermap(
      fun (StatEntry) ->
              case Extractor(StatEntry) of
                  {_, undefined} ->
                      false;
                  V ->
                      {true, V}
              end
      end, Samples).

calculate_stats(#gathered_stats{samples = Samples, nodes = Nodes,
                                extractor = Extractor}) ->
    {[calculate_stats(Extractor, keep_undefineds, NodeSamples)
      || NodeSamples <- Samples], Nodes}.

%%
%% Earlier we were gathering all stats from all nodes even if we are
%% interested in only one or some of them.
%% To optimize, we will gather only the stats as specified by the StatName.
%%

get_samples_for_stat(Kind, StatName, ForNodes, ClientTStamp, Window) ->
    {GatherStats, StatExtractor} =
        build_stat_list_and_extractor(Kind, StatName),

    {MainNode, MainSamples, RestSamplesRaw}
        = menelaus_stats_gatherer:gather_stats(Kind, ForNodes,
                                               ClientTStamp, Window,
                                               GatherStats),

    {RestNodes, RestSamples} = lists:unzip(RestSamplesRaw),
    #gathered_stats{kind = Kind, nodes = [MainNode | RestNodes],
                    samples = [lists:reverse(MainSamples) | RestSamples],
                    extractor = StatExtractor}.

nodes_to_try(Kind, all) ->
    section_nodes(Kind);
nodes_to_try(_Kind, Nodes) ->
    Nodes.

get_samples_from_one_of_kind([], _, _, _, _) ->
    %% We will reach here if samples are not available for a known stat.
    %% Samples may not be available at higher zoom level e.g. week, year etc.
    %% We do not know which Kind the stat belongs to and the
    %% nodes relevant for that Kind i.e. its section_nodes().
    #gathered_stats{samples = [[]], nodes = [node()],
                    extractor = fun functools:id/1};
get_samples_from_one_of_kind([Kind | RestKinds], StatName,
                             ClientTStamp, Window, Nodes) ->
    case nodes_to_try(Kind, Nodes) of
        [] ->
            get_samples_from_one_of_kind(RestKinds,
                                         StatName, ClientTStamp, Window, Nodes);
        ForNodes ->
            get_samples_for_kind(Kind, RestKinds, ForNodes,
                                 StatName, ClientTStamp, Window, Nodes)
    end.

get_samples_for_kind(Kind, RestKinds, ForNodes, StatName,
                     ClientTStamp, Window, Nodes) ->
    Stats = get_samples_for_stat(Kind, StatName, ForNodes,
                                 ClientTStamp, Window),
    case are_samples_undefined(Stats) of
        true ->
            get_samples_from_one_of_kind(RestKinds, StatName,
                                         ClientTStamp, Window, Nodes);
        false ->
            Stats
    end.

get_samples_for_system_or_bucket_stat(BucketName, StatName,
                                      ClientTStamp, Window) ->
    SearchList = get_stats_search_order(StatName, BucketName),
    Stats = get_samples_from_one_of_kind(SearchList, StatName,
                                         ClientTStamp, Window, all),
    calculate_stats(Stats).

%% For many stats, their section can be identified by their prefix.
guess_sections_by_prefix(StatName, BucketName) ->
    case StatName of
        "query_" ++ _Rest ->
            ["@query"];
        "replication" ++ _Rest ->
            ["@xdcr-" ++ BucketName || BucketName =/= undefined];
        "views" ++ _Rest ->
            [BucketName || BucketName =/= undefined];
        "spatial" ++ _Rest ->
            [BucketName || BucketName =/= undefined];
        "vb_" ++ _Rest ->
            [BucketName || BucketName =/= undefined];
        "ep_" ++ _Rest ->
            [BucketName || BucketName =/= undefined];
        Other ->
            case string:split(Other, "/") of
                [P, _] ->
                    Prefix = [$@ | P],
                    case lists:member(Prefix, services_sections(undefined)) of
                        true ->
                            [Prefix | [Prefix ++ "-" ++ BucketName ||
                                          BucketName =/= undefined]];
                        false ->
                            []
                    end;
                _ ->
                    []
            end
    end.

%%
%% Optimize the search order based on the stat prefix.
%% E.g. if a stat is query_..., then search for it in
%% query related stats first.
%%
get_stats_search_order(StatName, BucketName) ->
    GuessedSections = guess_sections_by_prefix(StatName, BucketName),
    AllSections = ["@system" | case BucketName of
                                   undefined ->
                                       [];
                                   _ ->
                                       [BucketName, "@xdcr-" ++ BucketName]
                               end ++ services_sections(BucketName)],
    GuessedSections ++ (AllSections -- GuessedSections).

build_response_for_specific_stat(BucketName, StatName, Params, LocalAddr) ->
    {ClientTStamp, {Step, _, Count} = Window} =
        parse_stats_params(Params),

    {NodesSamples, Nodes} =
        get_samples_for_system_or_bucket_stat(BucketName, StatName,
                                              ClientTStamp, Window),

    Config = ns_config:get(),
    Hostnames =
        [menelaus_web_node:build_node_hostname(Config, N, LocalAddr) ||
            N <- Nodes],

    Timestamps = [TS || {TS, _} <- hd(NodesSamples)],
    MainValues = [VS || {_, VS} <- hd(NodesSamples)],

    AllignedRestValues
        = lists:map(fun (undefined) -> [undefined || _ <- Timestamps];
                        (Samples) ->
                            Dict = orddict:from_list(Samples),
                            [dict_safe_fetch(T, Dict, 0) || T <- Timestamps]
                    end, tl(NodesSamples)),

    AllValues = [MainValues | AllignedRestValues],
    {struct, [{samplesCount, Count},
              {isPersistent, is_persistent(BucketName)},
              {lastTStamp, case Timestamps of
                               [] -> 0;
                               L -> lists:last(L)
                           end},
              {interval, Step * 1000},
              {timestamp, Timestamps},
              {nodeStats, {struct, lists:zipwith(fun (H, VS) ->
                                                         {H, VS}
                                                 end,
                                                 Hostnames, AllValues)}}]}.

%% Implementation

merge_all_samples_normally(MainSamples, ListOfLists) ->
    ETS = ets:new(ok, [{keypos, #stat_entry.timestamp}]),
    try do_merge_all_samples_normally(ETS, MainSamples, ListOfLists)
    after
        ets:delete(ETS)
    end.

do_merge_all_samples_normally(ETS, MainSamples, ListOfLists) ->
    ets:insert(ETS, MainSamples),
    lists:foreach(
      fun (OtherSamples) ->
              lists:foreach(
                fun (OtherS) ->
                        TS = OtherS#stat_entry.timestamp,
                        case ets:lookup(ETS, TS) of
                            [S|_] ->
                                ets:insert(ETS, aggregate_stat_entries(S, OtherS));
                            _ ->
                                nothing
                        end
                end, OtherSamples)
      end, ListOfLists),
    [hd(ets:lookup(ETS, T)) || #stat_entry{timestamp = T} <- MainSamples].

services_sections(undefined) ->
    ["@query",
     "@index",
     "@fts",
     "@cbas",
     "@eventing"];
services_sections(BucketName) ->
    services_sections(undefined) ++
        ["@index-" ++ BucketName,
         "@fts-" ++ BucketName,
         "@cbas-" ++ BucketName].

describe_section("@query") ->
    {n1ql, undefined};
describe_section("@index") ->
    {index, undefined};
describe_section("@index-" ++ Bucket) ->
    {index, Bucket};
describe_section("@fts") ->
    {fts, undefined};
describe_section("@fts-" ++ Bucket) ->
    {fts, Bucket};
describe_section("@cbas") ->
    {cbas, undefined};
describe_section("@cbas-" ++ Bucket) ->
    {cbas, Bucket};
describe_section("@eventing") ->
    {eventing, undefined};
describe_section(_) ->
    undefined.

services_add_params() ->
    [{n1ql, addq},
     {index, addi},
     {fts, addf},
     {cbas, adda},
     {eventing, adde}].

has_nodes(Service, ServiceNodes) ->
    proplists:get_value(Service, ServiceNodes) =/= [].

live_bucket_nodes(Bucket) ->
    case ns_bucket:get_bucket(Bucket) of
        not_present ->
            [];
        {ok, BucketConfig} ->
            ns_bucket:live_bucket_nodes_from_config(BucketConfig)
    end.

section_nodes("@system") ->
    ns_cluster_membership:actual_active_nodes();
section_nodes("@xdcr-" ++ Bucket) ->
    live_bucket_nodes(Bucket);
section_nodes(Section) ->
    case describe_section(Section) of
        {Service, _} ->
            ns_cluster_membership:service_actual_nodes(ns_config:latest(),
                                                       Service);
        undefined ->
            live_bucket_nodes(Section)
    end.

is_persistent("@"++_) ->
    false;
is_persistent(BucketName) ->
    ns_bucket:is_named_bucket_persistent(BucketName).

section_bucket("@system") ->
    undefined;
section_bucket("@xdcr-"++Bucket) ->
    Bucket;
section_bucket(Section) ->
    case describe_section(Section) of
        undefined ->
            Section;
        {_Service, Bucket} ->
            Bucket
    end.

grab_system_aggregate_op_stats(all, ClientTStamp, Window) ->
    grab_aggregate_op_stats("@system", section_nodes("@system"), ClientTStamp, Window);
grab_system_aggregate_op_stats([Node], ClientTStamp, Window) ->
    grab_aggregate_op_stats("@system", [Node], ClientTStamp, Window).

grab_aggregate_op_stats(Bucket, all, ClientTStamp, Window) ->
    grab_aggregate_op_stats(Bucket, section_nodes(Bucket), ClientTStamp, Window);
grab_aggregate_op_stats(Bucket, Nodes, ClientTStamp, Window) ->
    grab_aggregate_op_stats(Bucket, Nodes, ClientTStamp, Window, all).

grab_aggregate_op_stats(Bucket, all, ClientTStamp, Window, StatList) ->
    grab_aggregate_op_stats(Bucket, section_nodes(Bucket), ClientTStamp, Window, StatList);
grab_aggregate_op_stats(Bucket, Nodes, ClientTStamp, Window, StatList) ->
    {_MainNode, MainSamples, Replies} =
        menelaus_stats_gatherer:gather_stats(Bucket, Nodes, ClientTStamp, Window, StatList),
    RV = merge_all_samples_normally(MainSamples, [S || {_,S} <- Replies]),
    lists:reverse(RV).

find_zoom(ZoomStr) ->
    try
        Zoom = erlang:list_to_existing_atom(ZoomStr),
        case lists:keyfind(Zoom, 1, stats_archiver:archives()) of
            false ->
                undefined;
            T ->
                T
        end
    catch error:badarg ->
            undefined
    end.

parse_stats_params(Params) ->
    ClientTStamp = case proplists:get_value("haveTStamp", Params) of
                       undefined -> undefined;
                       X -> try list_to_integer(X) of
                                XI -> XI
                            catch
                                _:_ -> undefined
                            end
                   end,
    case find_zoom(proplists:get_value("zoom", Params, "minute")) of
        undefined ->
            menelaus_util:web_exception(400, "incorrect zoom value");
        {Period, _, Count} ->
            {ClientTStamp, {1, Period, Count}}
    end.

global_index_stat(StatName) ->
    service_stats_collector:global_stat(service_index, StatName).

per_index_stat(Index, Metric) ->
    service_stats_collector:per_item_stat(service_index, Index, Metric).

global_fts_stat(StatName) ->
    service_stats_collector:global_stat(service_fts, StatName).

per_fts_stat(Index, Metric) ->
    service_stats_collector:per_item_stat(service_fts, Index, Metric).

per_bucket_cbas_stat(StatName) ->
    service_stats_collector:global_stat(service_cbas, StatName).

per_fun_evening_stat(Id, Metric) ->
    service_stats_collector:per_item_stat(service_eventing, Id, Metric).

computed_stats_lazy_proplist("@system") ->
    [];
computed_stats_lazy_proplist("@index") ->
    [];
computed_stats_lazy_proplist("@query") ->
    Z2 = fun (StatNameA, StatNameB, Combiner) ->
                 {Combiner, [StatNameA, StatNameB]}
         end,
    QueryAvgRequestTime = Z2(query_request_time, query_requests,
                             fun (TimeNanos, Count) ->
                                     try TimeNanos * 1.0E-9 / Count
                                     catch error:badarith -> 0
                                     end
                             end),

    QueryAvgServiceTime = Z2(query_service_time, query_requests,
                             fun (TimeNanos, Count) ->
                                     try TimeNanos * 1.0E-9 / Count
                                     catch error:badarith -> 0
                                     end
                             end),

    QueryAvgResultSize = Z2(query_result_size, query_requests,
                            fun (Size, Count) ->
                                    try Size / Count
                                    catch error:badarith -> 0
                                    end
                            end),

    QueryAvgResultCount = Z2(query_result_count, query_requests,
                             fun (RCount, Count) ->
                                     try RCount / Count
                                     catch error:badarith -> 0
                                     end
                             end),


    [{<<"query_avg_req_time">>, QueryAvgRequestTime},
     {<<"query_avg_svc_time">>, QueryAvgServiceTime},
     {<<"query_avg_response_size">>, QueryAvgResultSize},
     {<<"query_avg_result_count">>, QueryAvgResultCount}];
computed_stats_lazy_proplist("@index-"++BucketId) ->
    Z2 = fun (StatNameA, StatNameB, Combiner) ->
                 {Combiner, [StatNameA, StatNameB]}
         end,


    ComputeFragmentation =
        fun (DiskOverhead, DiskSize) ->
                try
                    100 * (DiskOverhead / max(DiskOverhead, DiskSize))
                catch error:badarith ->
                        0
                end
        end,


    GlobalFragmentation = Z2(global_index_stat(<<"disk_overhead_estimate">>),
                             global_index_stat(<<"disk_size">>),
                             ComputeFragmentation),

    [{global_index_stat(<<"fragmentation">>), GlobalFragmentation}] ++
        lists:flatmap(
          fun (Index) ->
                  AvgItemSize = Z2(per_index_stat(Index, <<"raw_data_size">>),
                                   per_index_stat(Index, <<"items_count">>),
                                   fun (DataSize, Count) ->
                                           try
                                               DataSize / Count
                                           catch
                                               error:badarith ->
                                                   0
                                           end
                                   end),

                  AvgScanLatency = Z2(per_index_stat(Index, <<"total_scan_duration">>),
                                      per_index_stat(Index, <<"num_rows_returned">>),
                                      fun (ScanDuration, NumRows) ->
                                              try
                                                  ScanDuration / NumRows
                                              catch
                                                  error:badarith ->
                                                      0
                                              end
                                      end),

                  AllPendingDocs = Z2(per_index_stat(Index, <<"num_docs_pending">>),
                                      per_index_stat(Index, <<"num_docs_queued">>),
                                      fun (Pending, Queued) ->
                                              Pending + Queued
                                      end),

                  CacheMissRat = Z2(per_index_stat(Index, <<"cache_hits">>),
                                    per_index_stat(Index, <<"cache_misses">>),
                                    fun (Hits, Misses) ->
                                            try
                                                Misses * 100 / (Hits + Misses)
                                            catch
                                                error:badarith ->
                                                    0
                                            end
                                    end),

                  FragPercent = Z2(per_index_stat(Index, <<"data_size_on_disk">>),
                                   per_index_stat(Index, <<"log_space_on_disk">>),
                                   fun (Data, Log)
                                         when Log == 0 orelse Log < Data ->
                                           0;
                                       (Data, Log) ->
                                           try
                                               (Log - Data) * 100.00 / Log
                                           catch
                                               error:badarith ->
                                                   0
                                           end
                                   end),

                  ResPercent = Z2(per_index_stat(Index, <<"recs_in_mem">>),
                                  per_index_stat(Index, <<"recs_on_disk">>),
                                  fun (Mem, Disk) ->
                                          try
                                              Mem * 100 / (Mem + Disk)
                                          catch
                                              error:badarith ->
                                                  0
                                          end
                                  end),

                  [{per_index_stat(Index, <<"avg_item_size">>), AvgItemSize},
                   {per_index_stat(Index, <<"avg_scan_latency">>), AvgScanLatency},
                   {per_index_stat(Index, <<"num_docs_pending+queued">>), AllPendingDocs},
                   {per_index_stat(Index, <<"cache_miss_ratio">>), CacheMissRat},
                   {per_index_stat(Index, <<"index_frag_percent">>), FragPercent},
                   {per_index_stat(Index, <<"index_resident_percent">>), ResPercent}]
          end, get_indexes(service_index, BucketId));
computed_stats_lazy_proplist("@fts-"++BucketId) ->
    Z2 = fun (StatNameA, StatNameB, Combiner) ->
                 {Combiner, [StatNameA, StatNameB]}
         end,
    lists:flatmap(
      fun (Index) ->
              AvgQueriesLatency = Z2(per_fts_stat(Index, <<"total_request_time">>),
                                     per_fts_stat(Index, <<"total_queries">>),
                                     fun (TimeNanos, Count) ->
                                             try TimeNanos * 1.0E-6 / Count
                                             catch error:badarith -> 0
                                             end
                                     end),

              [{per_fts_stat(Index, <<"avg_queries_latency">>), AvgQueriesLatency}]
      end, get_indexes(service_fts, BucketId));
computed_stats_lazy_proplist("@fts") ->
    [];
computed_stats_lazy_proplist("@eventing") ->
    [];
computed_stats_lazy_proplist("@cbas") ->
    [];
computed_stats_lazy_proplist("@cbas-" ++ _BucketName) ->
    [];
computed_stats_lazy_proplist("@xdcr-"++BucketName) ->
    Z2 = fun (StatNameA, StatNameB, Combiner) ->
                 {Combiner, [StatNameA, StatNameB]}
         end,
    %% compute a list of per replication XDC stats
    Reps = goxdcr_status_keeper:get_replications(BucketName),
    lists:flatmap(fun (Id) ->
                          Prefix = <<"replications/", Id/binary,"/">>,

                          PercentCompleteness = Z2(<<Prefix/binary, "docs_processed">>,
                                                   <<Prefix/binary, "changes_left">>,
                                                   fun (Processed, Left) ->
                                                           try (100 * Processed) / (Processed + Left)
                                                           catch error:badarith -> 0
                                                           end
                                                   end),

                          [{<<Prefix/binary, "percent_completeness">>, PercentCompleteness}]
                  end,
                  Reps);
computed_stats_lazy_proplist(_) ->
    Z2 = fun (StatNameA, StatNameB, Combiner) ->
                 {Combiner, [StatNameA, StatNameB]}
         end,
    Z3 = fun (StatNameA, StatNameB, StatNameC, Combiner) ->
                 {Combiner, [StatNameA, StatNameB, StatNameC]}
         end,
    HitRatio = Z2(cmd_get, get_hits,
                  fun (Gets, _Hits) when Gets == 0 -> 0; % this handles int and float 0
                      (Gets, Hits) -> Hits * 100/Gets
                  end),
    EPCacheMissRatio = Z2(ep_bg_fetched, cmd_lookup,
                          fun (BGFetches, Gets) ->
                                  try BGFetches * 100 / Gets
                                  catch error:badarith -> 0
                                  end
                          end),
    ResidentItemsRatio = Z2(ep_num_non_resident, curr_items_tot,
                            fun (NonResident, CurrItems) ->
                                    try (CurrItems - NonResident) * 100 / CurrItems
                                    catch error:badarith -> 100
                                    end
                            end),
    AvgActiveQueueAge = Z2(vb_active_queue_age, vb_active_queue_size,
                           fun (ActiveAge, ActiveCount) ->
                                   try ActiveAge / ActiveCount / 1000
                                   catch error:badarith -> 0
                                   end
                           end),
    AvgReplicaQueueAge = Z2(vb_replica_queue_age, vb_replica_queue_size,
                            fun (ReplicaAge, ReplicaCount) ->
                                    try ReplicaAge / ReplicaCount / 1000
                                    catch error:badarith -> 0
                                    end
                            end),
    AvgPendingQueueAge = Z2(vb_pending_queue_age, vb_pending_queue_size,
                            fun (PendingAge, PendingCount) ->
                                    try PendingAge / PendingCount / 1000
                                    catch error:badarith -> 0
                                    end
                            end),
    AvgTotalQueueAge = Z2(vb_total_queue_age, vb_total_queue_size,
                          fun (TotalAge, TotalCount) ->
                                  try TotalAge / TotalCount / 1000
                                  catch error:badarith -> 0
                                  end
                          end),
    TotalDisk = Z2(couch_docs_actual_disk_size, couch_views_actual_disk_size,
                   fun (Views, Docs) ->
                           Views + Docs
                   end),

    ResidenceCalculator = fun (NonResident, Total) ->
                                  try (Total - NonResident) * 100 / Total
                                  catch error:badarith -> 100
                                  end
                          end,

    Fragmentation = fun (Data, Disk) ->
                            try
                                round((Disk - Data) / Disk * 100)
                            catch error:badarith ->
                                    0
                            end
                    end,

    DocsFragmentation = Z3(couch_docs_data_size, couch_docs_disk_size,
                           ep_db_history_file_size,
                           fun (Data, Disk, History) ->
                                   Fragmentation(Data, Disk - History)
                           end),
    ViewsFragmentation = Z2(couch_views_data_size, couch_views_disk_size,
                            Fragmentation),

    ActiveResRate = Z2(vb_active_num_non_resident, curr_items,
                       ResidenceCalculator),
    ReplicaResRate = Z2(vb_replica_num_non_resident, vb_replica_curr_items,
                        ResidenceCalculator),
    PendingResRate = Z2(vb_pending_num_non_resident, vb_pending_curr_items,
                        ResidenceCalculator),

    AverageDiskUpdateTime = Z2(disk_update_total, disk_update_count,
                               fun (Total, Count) ->
                                       try Total / Count
                                       catch error:badarith -> 0
                                       end
                               end),

    AverageCommitTime = Z2(disk_commit_total, disk_commit_count,
                           fun (Total, Count) ->
                                   try Total / Count / 1000000
                                   catch error:badarith -> 0
                                   end
                           end),

    AverageBgWait = Z2(bg_wait_total, bg_wait_count,
                       fun (Total, Count) ->
                               try Total / Count
                               catch error:badarith -> 0
                               end
                       end),

    AvgActiveTimestampDrift = Z2(ep_active_hlc_drift, ep_active_hlc_drift_count,
                                 fun (DriftTotal, Count) ->
                                         try DriftTotal / 1000000 / Count
                                         catch error:badarith -> 0
                                         end
                                 end),

    AvgReplicaTimestampDrift = Z2(ep_replica_hlc_drift, ep_replica_hlc_drift_count,
                                  fun (DriftTotal, Count) ->
                                          try DriftTotal / 1000000 / Count
                                          catch error:badarith -> 0
                                          end
                                  end),

    ViewsIndexesStats =
        [{Key, Z3(ViewKey, IndexKey, FtsKey, fun (A, B, C) -> A + B + C end)} ||
            {Key, ViewKey, IndexKey, FtsKey} <-
                [{<<"ep_dcp_views+indexes_count">>,
                  ep_dcp_views_count, ep_dcp_2i_count, ep_dcp_fts_count},
                 {<<"ep_dcp_views+indexes_items_remaining">>,
                  ep_dcp_views_items_remaining, ep_dcp_2i_items_remaining, ep_dcp_fts_items_remaining},
                 {<<"ep_dcp_views+indexes_producer_count">>,
                  ep_dcp_views_producer_count, ep_dcp_2i_producer_count, ep_dcp_fts_producer_count},
                 {<<"ep_dcp_views+indexes_total_backlog_size">>,
                  ep_dcp_views_total_backlog_size, ep_dcp_2i_total_backlog_size, ep_dcp_fts_total_backlog_size},
                 {<<"ep_dcp_views+indexes_items_sent">>,
                  ep_dcp_views_items_sent, ep_dcp_2i_items_sent, ep_dcp_fts_items_sent},
                 {<<"ep_dcp_views+indexes_total_bytes">>,
                  ep_dcp_views_total_bytes, ep_dcp_2i_total_bytes, ep_dcp_fts_total_bytes},
                 {<<"ep_dcp_views+indexes_backoff">>,
                  ep_dcp_views_backoff, ep_dcp_2i_backoff, ep_dcp_fts_backoff}]],

    [{<<"couch_total_disk_size">>, TotalDisk},
     {<<"couch_docs_fragmentation">>, DocsFragmentation},
     {<<"couch_views_fragmentation">>, ViewsFragmentation},
     {<<"hit_ratio">>, HitRatio},
     {<<"ep_cache_miss_rate">>, EPCacheMissRatio},
     {<<"ep_resident_items_rate">>, ResidentItemsRatio},
     {<<"vb_avg_active_queue_age">>, AvgActiveQueueAge},
     {<<"vb_avg_replica_queue_age">>, AvgReplicaQueueAge},
     {<<"vb_avg_pending_queue_age">>, AvgPendingQueueAge},
     {<<"vb_avg_total_queue_age">>, AvgTotalQueueAge},
     {<<"vb_active_resident_items_ratio">>, ActiveResRate},
     {<<"vb_replica_resident_items_ratio">>, ReplicaResRate},
     {<<"vb_pending_resident_items_ratio">>, PendingResRate},
     {<<"avg_disk_update_time">>, AverageDiskUpdateTime},
     {<<"avg_disk_commit_time">>, AverageCommitTime},
     {<<"avg_bg_wait_time">>, AverageBgWait},
     {<<"avg_active_timestamp_drift">>, AvgActiveTimestampDrift},
     {<<"avg_replica_timestamp_drift">>, AvgReplicaTimestampDrift}]
        ++ ViewsIndexesStats
        ++ computed_stats_lazy_proplist("@query").

combine_samples(Combiner, Dict, StatNames) ->
    case all_stats_defined(Dict, StatNames, []) of
        false ->
            undefined;
        Stats ->
            case length(Stats) of
                2 ->
                    combine_2_samples(Combiner, Stats);
                3 ->
                    combine_3_samples(Combiner, Stats)
            end
    end.

combine_2_samples(Combiner, [ValA, ValB]) ->
    lists:zipwith(
        fun (A, B) when A =/= null, B =/= null ->
                Combiner(A, B);
            (_, _) ->
                null
        end, ValA, ValB).

combine_3_samples(Combiner, [ValA, ValB, ValC]) ->
    lists:zipwith3(
        fun (A, B, C) when A =/= null, B =/= null, C =/= null ->
                Combiner(A, B, C);
            (_, _, _) ->
                null
        end, ValA, ValB, ValC).

all_stats_defined(_Dict, [], Acc) ->
    lists:reverse(Acc);
all_stats_defined(Dict, [First | Rest], Acc) ->
    case lists:keyfind(First, 1, Dict) of
        false ->
            false;
        {_, Val} ->
            all_stats_defined(Dict, Rest, [Val | Acc])
    end.

%% converts list of samples to proplist of stat values.
%%
%% null values should be uncommon, but they are not impossible. They
%% will be used for samples which lack some particular stat, for
%% example due to older membase version. I.e. when we upgrade we
%% sometimes add new kinds of stats and null values are used to mark
%% those past samples that don't have new stats gathered.
-spec samples_to_proplists([#stat_entry{}], list()) -> [{atom(), [null | number()]}].
samples_to_proplists([], _BucketName) -> [{timestamp, []}];
samples_to_proplists(Samples, BucketName) ->
    %% we're assuming that last sample has currently supported stats,
    %% that's why we are folding from backward and why we're ignoring
    %% other keys of other samples
    [LastSample | ReversedRest] = lists:reverse(Samples),
    InitialAcc0 = orddict:map(fun (_, V) -> [V] end, LastSample#stat_entry.values),
    InitialAcc = orddict:store(timestamp, [LastSample#stat_entry.timestamp], InitialAcc0),
    Dict = lists:foldl(fun (Sample, Acc) ->
                               orddict:map(fun (timestamp, AccValues) ->
                                                   [Sample#stat_entry.timestamp | AccValues];
                                               (K, AccValues) ->
                                                   case lists:keyfind(K, 1, Sample#stat_entry.values) of
                                                       {_, ThisValue} -> [ThisValue | AccValues];
                                                       _ -> [null | AccValues]
                                                   end
                                           end, Acc)
                       end, InitialAcc, ReversedRest),

    ExtraStats = lists:map(fun ({K, {F, StatNames}}) ->
                                   {K, combine_samples(F, Dict, StatNames)}
                           end, computed_stats_lazy_proplist(BucketName)),

    lists:filter(fun ({_, undefined}) -> false;
                     ({_, _}) -> true
                 end, ExtraStats)
        ++ orddict:to_list(Dict).

join_samples(A, B, Count) ->
    join_samples(lists:reverse(A), lists:reverse(B), [], Count).

join_samples([A | _] = ASamples, [B | TailB], Acc, Count) when A#stat_entry.timestamp < B#stat_entry.timestamp ->
    join_samples(ASamples, TailB, Acc, Count);
join_samples([A | TailA], [B | _] = BSamples, Acc, Count) when A#stat_entry.timestamp > B#stat_entry.timestamp ->
    join_samples(TailA, BSamples, Acc, Count);
join_samples(_, _, Acc, 0) ->
    Acc;
join_samples([A | TailA], [B | TailB], Acc, Count) ->
    NewAcc = [A#stat_entry{values = A#stat_entry.values ++ B#stat_entry.values} | Acc],
    join_samples(TailA, TailB, NewAcc, Count - 1);
join_samples(_, _, Acc, _) ->
    Acc.

build_bucket_stats_ops_response(Nodes, BucketName, Params, WithSystemStats) ->
    {ClientTStamp, {Step, Period, Count} = Window} = parse_stats_params(Params),

    Samples = case WithSystemStats of
                  true ->
                      W1 = {Step, Period, Count + 1},
                      BucketRawSamples = grab_aggregate_op_stats(BucketName, Nodes, ClientTStamp, W1),
                      SystemRawSamples = grab_system_aggregate_op_stats(Nodes, ClientTStamp, W1),

                      %% this will throw out all samples with timestamps that are not present
                      %% in both BucketRawSamples and SystemRawSamples
                      join_samples(BucketRawSamples, SystemRawSamples, Count);
                  false ->
                      grab_aggregate_op_stats(BucketName, Nodes, ClientTStamp, Window)
              end,

    StatsPropList = samples_to_proplists(Samples, BucketName),

    [{op, {struct,
           [{samples, {struct, StatsPropList}},
            {samplesCount, Count},
            {isPersistent, is_persistent(BucketName)},
            {lastTStamp, case proplists:get_value(timestamp, StatsPropList) of
                             [] -> 0;
                             L -> lists:last(L)
                         end},
            {interval, Step * 1000}]}}].

%% by default we aggregate stats between nodes using SUM
%% but in some cases other methods should be used
%% for example for couch_views_ops since view hits all the nodes
%% we use max to prevent the number of ops to be multiplied to the number of nodes
get_aggregate_method(Key) ->
    case Key of
        couch_views_ops ->
            max;
        cpu_utilization_rate ->
            max;
        <<"index_ram_percent">> ->
            max;
        <<"views/", S/binary>> ->
            case binary:match(S, <<"/accesses">>) of
                nomatch ->
                    sum;
                _ ->
                    max
            end;
        _ ->
            sum
    end.

aggregate_values(Key, AV, BV) ->
    case get_aggregate_method(Key) of
        sum ->
            try AV+BV
            catch error:badarith ->
                    case ([X || X <- [AV,BV],
                                X =/= undefined]) of
                        [] -> undefined;
                        [X|_] -> X
                    end
            end;
        max ->
            case {AV, BV} of
                {undefined, undefined} ->
                    undefined;
                {undefined, _} ->
                    BV;
                {_, undefined} ->
                    AV;
                _ ->
                    max(AV, BV)
            end
    end.

aggregate_stat_kv_pairs([], BPairs, Acc) ->
    lists:reverse(Acc, BPairs);
aggregate_stat_kv_pairs(APairs, [], Acc) ->
    lists:reverse(Acc, APairs);
aggregate_stat_kv_pairs([{AK, AV} = APair | ARest] = A,
                        [{BK, BV} = BPair | BRest] = B,
                        Acc) ->
    case AK of
        BK ->
            NewAcc = [{AK, aggregate_values(AK, AV, BV)} | Acc],
            aggregate_stat_kv_pairs(ARest, BRest, NewAcc);
        _ when AK < BK ->
            aggregate_stat_kv_pairs(ARest, B, [APair | Acc]);
        _ ->
            aggregate_stat_kv_pairs(A, BRest, [BPair | Acc])
    end.

aggregate_stat_entries(A, B) ->
    true = (B#stat_entry.timestamp =:= A#stat_entry.timestamp),
    NewValues = aggregate_stat_kv_pairs(A#stat_entry.values,
                                        B#stat_entry.values,
                                        []),
    A#stat_entry{values = NewValues}.

-define(SPACE_CHAR, 16#20).

simple_memoize(Key, Body, Expiration) ->
    menelaus_web_cache:lookup_or_compute_with_expiration(
      Key,
      fun () ->
              {Body(), Expiration, []}
      end,
      fun (_Key, _Value, []) ->
              false
      end).

proceed_if_has_nodes(Service, ServiceNodes, MemoizeKey, Fun) ->
    case proplists:get_value(Service, ServiceNodes) of
        [] ->
            [];
        NodesOrAll ->
            Nodes =
                case NodesOrAll of
                    all ->
                        ns_cluster_membership:service_actual_nodes(
                          ns_config:latest(), Service);
                    _ ->
                        NodesOrAll
                end,
            simple_memoize(MemoizeKey,
                           fun () ->
                                   Fun(Nodes)
                           end, 5000)
    end.

couchbase_goxdcr_stats_descriptions(BucketId) ->
    simple_memoize({stats_directory_goxdcr, BucketId},
                   fun () ->
                           do_couchbase_goxdcr_stats_descriptions(BucketId)
                   end, 5000).

do_couchbase_goxdcr_stats_descriptions(BucketId) ->
    Reps = goxdcr_status_keeper:get_replications_with_remote_info(BucketId),
    lists:map(
      fun ({Id, RemoteClusterName, RemoteBucket}) ->
              Prefix = <<"replications/", Id/binary,"/">>,

              BlockName = io_lib:format("Outbound XDCR Operations to bucket ~p "
                                                "on remote cluster ~p",[RemoteBucket, RemoteClusterName]),

              {struct,
               [{blockName, iolist_to_binary(BlockName)},
                {extraCSSClasses, <<"dynamic_closed">>},
                {stats,
                 [
                  {struct, [{title, <<"mutations">>},
                            {name, <<Prefix/binary, "changes_left">>},
                            {desc, <<"Number of mutations to be replicated to other clusters "
                                     "(measured from per-replication stat changes_left)">>}]},
                  {struct, [{title, <<"percent completed">>},
                            {maxY, 100},
                            {name, <<Prefix/binary, "percent_completeness">>},
                            {desc, <<"Percentage of checked items out of all checked and to-be-replicated items "
                                     "(measured from per-replication stat percent_completeness)">>}]},
                  {struct, [{title, <<"mutations replicated">>},
                            {name, <<Prefix/binary, "docs_written">>},
                            {desc, <<"Number of mutations that have been replicated to other clusters "
                                     "(measured from per-replication stat docs_written)">>}]},
                  {struct, [{title, <<"mutations filtered per sec.">>},
                            {name, <<Prefix/binary, "docs_filtered">>},
                            {desc, <<"Number of mutations per second that have been filtered out and have not been replicated to other clusters "
                                     "(measured from per-replication stat docs_filtered)">>}]},
                  {struct, [{title, <<"mutations skipped by resolution">>},
                            {name, <<Prefix/binary, "docs_failed_cr_source">>},
                            {desc, <<"Number of mutations that failed conflict resolution on the source side and hence have not been replicated to other clusters "
                                     "(measured from per-replication stat docs_failed_cr_source)">>}]},
                  {struct, [{title, <<"mutation replication rate">>},
                            {name, <<Prefix/binary, "rate_replicated">>},
                            {desc, <<"Rate of replication in terms of number of replicated mutations per second "
                                     "(measured from per-replication stat rate_replicated)">>}]},
                  {struct, [{isBytes, true},
                            {title, <<"data replication rate">>},
                            {name, <<Prefix/binary, "bandwidth_usage">>},
                            {desc, <<"Rate of replication in terms of bytes replicated per second "
                                     "(measured from per-replication stat bandwidth_usage)">>}]},
                  {struct, [{title, <<"opt. replication rate">>},
                            {name, <<Prefix/binary, "rate_doc_opt_repd">>},
                            {desc, <<"Rate of optimistic replications in terms of number of replicated mutations per second ">>}]},
                  {struct, [{title, <<"doc checks rate">>},
                            {name, <<Prefix/binary, "rate_doc_checks">>},
                            {desc, <<"Rate of doc checks per second ">>}]},
                  {struct, [{title, <<"ms meta batch latency">>},
                            {name, <<Prefix/binary, "wtavg_meta_latency">>},
                            {desc, <<"Weighted average latency in ms of sending getMeta and waiting for conflict solution result from remote cluster "
                                     "(measured from per-replication stat wtavg_meta_latency)">>}]},
                  {struct, [{title, <<"ms doc batch latency">>},
                            {name, <<Prefix/binary, "wtavg_docs_latency">>},
                            {desc, <<"Weighted average latency in ms of sending replicated mutations to remote cluster "
                                     "(measured from per-replication stat wtavg_docs_latency)">>}]},
                  {struct, [{title, <<"doc reception rate">>},
                            {name, <<Prefix/binary, "rate_received_from_dcp">>},
                            {desc, <<"Rate of mutations received from dcp in terms of number of mutations per second ">>}]}]}]}
      end, Reps).

couchbase_view_stats_descriptions(BucketId) ->
    simple_memoize({stats_directory_views, BucketId},
                   fun () ->
                           do_couchbase_view_stats_descriptions(BucketId)
                   end, 5000).

do_couchbase_view_stats_descriptions(BucketId) ->
    {MapReduceSignatures, SpatialSignatures} = ns_couchdb_api:get_design_doc_signatures(BucketId),
    do_couchbase_view_stats_descriptions(MapReduceSignatures, <<"views/">>, <<"Mapreduce View Stats">>) ++
        do_couchbase_view_stats_descriptions(SpatialSignatures, <<"spatial/">>, <<"Spatial View Stats">>).

do_couchbase_view_stats_descriptions(DictBySig, KeyPrefix, Title) ->
    dict:fold(
      fun(Sig, DDocIds0, Stats) ->
              Prefix = <<KeyPrefix/binary, Sig/binary, "/">>,
              DDocIds = lists:sort(DDocIds0),
              Ids = iolist_to_binary([hd(DDocIds) |
                                      [[?SPACE_CHAR | Id]
                                       || Id <- tl(DDocIds)]]),
              MyStats = {struct,[{blockName, <<Title/binary, ": ", Ids/binary>>},
                                 {extraCSSClasses, <<"dynamic_closed">>},
                                 {columns,
                                  [<<"Data">>, <<"Disk">>, <<"Read Ops">>]},
                                 {stats,
                                  [{struct, [{isBytes, true},
                                             {title, <<"data size">>},
                                             {name, <<Prefix/binary, "data_size">>},
                                             {desc, <<"How many bytes stored">>}]},
                                   {struct, [{isBytes, true},
                                             {title, <<"disk size">>},
                                             {name, <<Prefix/binary, "disk_size">>},
                                             {desc, <<"How much storage used">>}]},
                                   {struct, [{title, <<"view reads per sec.">>},
                                             {name, <<Prefix/binary, "accesses">>},
                                             {desc, <<"Traffic to the views in this design doc">>}]}
                                  ]}]},
              [MyStats | Stats]
      end, [], DictBySig).

couchbase_index_stats_descriptions(BucketId, ServiceNodes) ->
    proceed_if_has_nodes(
      index, ServiceNodes, {stats_directory_index, BucketId},
      fun (IndexNodes) ->
              do_couchbase_index_stats_descriptions(BucketId, IndexNodes)
      end).

do_couchbase_index_stats_descriptions(BucketId, Nodes) ->
    AllIndexes = do_get_indexes(service_index, BucketId, Nodes),
    [{struct, [{blockName, <<"Index Stats: ", Id/binary>>},
               {extraCSSClasses, <<"dynamic_closed">>},
               {stats,
                [{struct, [{title, <<"items scanned/sec">>},
                           {name, per_index_stat(Id, <<"num_rows_returned">>)},
                           {desc, <<"Number of index items scanned by the indexer per second">>}]},
                 {struct, [{isBytes, true},
                           {title, <<"disk size">>},
                           {name, per_index_stat(Id, <<"disk_size">>)},
                           {desc, <<"Total disk file size consumed by the index">>}]},
                 {struct, [{isBytes, true},
                           {title, <<"data size">>},
                           {name, per_index_stat(Id, <<"data_size">>)},
                           {desc, <<"Actual data size consumed by the index">>}]},
                 {struct, [{isBytes, true},
                           {title, <<"memory used">>},
                           {name, per_index_stat(Id, <<"memory_used">>)},
                           {desc, <<"Total memory consumed by the index storage">>}]},
                 {struct, [{title, <<"total mutations remaining">>},
                           {name, per_index_stat(Id, <<"num_docs_pending+queued">>)},
                           {desc, <<"Number of documents pending to be indexed">>}]},
                 {struct, [{title, <<"drain rate items/sec">>},
                           {name, per_index_stat(Id, <<"num_docs_indexed">>)},
                           {desc, <<"Number of documents indexed by the indexer per second">>}]},
                 {struct, [{title, <<"total indexed items">>},
                           {name, per_index_stat(Id, <<"items_count">>)},
                           {desc, <<"Current total indexed document count">>}]},
                 {struct, [{isBytes, true},
                           {title, <<"average item size">>},
                           {name, per_index_stat(Id, <<"avg_item_size">>)},
                           {desc, <<"Average size of each index item">>}]},
                 {struct, [{title, <<"% fragmentation">>},
                           {name, per_index_stat(Id, <<"index_frag_percent">>)},
                           {desc, <<"Percentage fragmentation of the index. Note: at small index sizes of less than a hundred kB, the static overhead of the index disk file will inflate the index fragmentation percentage">>}]},
                 {struct, [{title, <<"requests/sec">>},
                           {name, per_index_stat(Id, <<"num_requests">>)},
                           {desc, <<"Number of requests served by the indexer per second">>}]},
                 {struct, [{title, <<"bytes returned/sec">>},
                           {name, per_index_stat(Id, <<"scan_bytes_read">>)},
                           {desc, <<"Number of bytes per second read by a scan">>}]},
                 {struct, [{title, <<"avg scan latency(ns)">>},
                           {name, per_index_stat(Id, <<"avg_scan_latency">>)},
                           {desc, <<"Average time to serve a scan request (nanoseconds)">>}]},
                 {struct, [{title, <<"cache resident percent">>},
                           {name, per_index_stat(Id, <<"index_resident_percent">>)},
                           {desc, <<"Percentage of index data resident in memory">>}]},
                 {struct, [{title, <<"index cache miss ratio">>},
                           {name, per_index_stat(Id, <<"cache_miss_ratio">>)},
                           {desc, <<"Percentage of accesses to this index data"
                                    "from disk as opposed to RAM (measured from"
                                    "cache_misses * 100 / (cache_misses +"
                                    "cache_hits))">>}]}]}]}
     || Id <- AllIndexes].

couchbase_cbas_stats_descriptions(ServiceNodes) ->
    proceed_if_has_nodes(cbas, ServiceNodes, stats_directory_cbas,
                         fun (_) ->
                                 do_couchbase_cbas_stats_descriptions()
                         end).

do_couchbase_cbas_stats_descriptions() ->
    BlockName = "Analytics Stats",
    [{struct, [{blockName, list_to_binary(BlockName)},
               {extraCSSClasses, <<"dynamic_closed">>},
               {stats,
                [{struct, [{title, <<"ops/sec.">>},
                           {name, per_bucket_cbas_stat("incoming_records_count")},
                           {desc, <<"Operations (gets + sets + deletes) per second processed by Analytics for this bucket">>}]},
                 {struct, [{title, <<"sync failed records">>},
                           {name, per_bucket_cbas_stat("failed_at_parser_records_count_total")},
                           {desc, <<"Failed to parse records during bucket synchronization">>}]},
                 {struct, [{title, <<"total ops since bucket connect">>},
                           {name, per_bucket_cbas_stat("incoming_records_count_total")},
                           {desc, <<"Operations (gets + sets + deletes) processed by Analytics for this bucket since last connected">>}]}
                ]}]}].

couchbase_eventing_stats_descriptions(ServiceNodes) ->
    proceed_if_has_nodes(eventing, ServiceNodes, stats_directory_eventing,
                         fun (_) ->
                                 do_couchbase_eventing_stats_descriptions()
                         end).

do_couchbase_eventing_stats_descriptions() ->
    Functions = service_eventing:get_functions(),
    [{struct,
      [{blockName, <<"Eventing Stats: ", Id/binary>>},
       {warning, <<"Metrics for Eventing are not per bucket and will "
                   "not change if bucket dropdown above is changed">>},
       {extraCSSClasses, <<"dynamic_closed">>},
       {stats,
        [{struct,
          [{title, <<"Processed">>},
           {name, per_fun_evening_stat(Id, <<"processed_count">>)},
           {desc, <<"Successful function invocations.">>}]},
         {struct,
          [{title, <<"Failures">>},
           {name, per_fun_evening_stat(Id, <<"failed_count">>)},
           {desc, <<"Failed function invocations.">>}]},
         {struct,
          [{title, <<"Backlog">>},
           {name, per_fun_evening_stat(Id, <<"dcp_backlog">>)},
           {desc, <<"Mutations yet to be processed by the function">>}]},
         {struct,
          [{title, <<"Timeouts">>},
           {name, per_fun_evening_stat(Id, <<"timeout_count">>)},
           {desc, <<"Timed out function invocations.">>}]}]}]}
     || Id <- Functions].

couchbase_fts_stats_descriptions(BucketId, ServiceNodes) ->
    proceed_if_has_nodes(
      fts, ServiceNodes, {stats_directory_fts, BucketId},
      fun (FtsNodes) ->
              do_couchbase_fts_stats_descriptions(BucketId, FtsNodes)
      end).

do_couchbase_fts_stats_descriptions(BucketId, Nodes) ->
    AllIndexes = do_get_indexes(service_fts, BucketId, Nodes),
    [{struct, [{blockName, <<"Full Text Search Stats: ", Id/binary>>},
               {extraCSSClasses, <<"dynamic_closed">>},
               {stats,
                [{struct, [{title, <<"items">>},
                           {name, per_fts_stat(Id, <<"doc_count">>)},
                           {desc, <<"Number of documents examined"
                                    " (measured from doc_count of active and replica index partitions)">>}]},
                 {struct, [{title, <<"bytes indexed/sec">>},
                           {name, per_fts_stat(Id, <<"total_bytes_indexed">>)},
                           {desc, <<"Number of plain text bytes indexed per second"
                                    " (measured from total_bytes_indexed)">>}]},
                 {struct, [{title, <<"queries/sec">>},
                           {name, per_fts_stat(Id, <<"total_queries">>)},
                           {desc, <<"Number of queries per second"
                                    " (measured from total_queries)">>}]},
                 {struct, [{title, <<"error queries/sec">>},
                           {name, per_fts_stat(Id, <<"total_queries_error">>)},
                           {desc, <<"Number of queries that resulted in errors per second. Includes timeouts"
                                    " (measured from total_queries_error)">>}]},
                 {struct, [{title, <<"items remaining">>},
                           {name, per_fts_stat(Id, <<"num_mutations_to_index">>)},
                           {desc, <<"Number of mutations not yet indexed"
                                    " (measured from num_mutations_to_index)">>}]},
                 {struct, [{title, <<"compaction bytes written/sec">>},
                           {name, per_fts_stat(Id, <<"total_compaction_written_bytes">>)},
                           {desc, <<"Number of compaction bytes written per second"
                                    " (measured from total_compaction_written_bytes)">>}]},
                 {struct, [{title, <<"avg query latency(ms)">>},
                           {name, per_fts_stat(Id, <<"avg_queries_latency">>)},
                           {desc, <<"Average time to answer query"
                                    " (measured from avg_queries_latency)">>}]},
                 {struct, [{title, <<"timeout queries/sec">>},
                           {name, per_fts_stat(Id, <<"total_queries_timeout">>)},
                           {desc, <<"Number of queries that timeout per second"
                                    " (measured from total_queries_timeout)">>}]},
                 {struct, [{title, <<"records to persist">>},
                           {name, per_fts_stat(Id, <<"num_recs_to_persist">>)},
                           {desc, <<"Number of index records not yet persisted to disk"
                                    " (measured from num_recs_to_persist)">>}]},
                 {struct, [{title, <<"partitions actual">>},
                           {name, per_fts_stat(Id, <<"num_pindexes_actual">>)},
                           {desc, <<"Number of index partitions"
                                    " (including replica partitions, measured from"
                                    " num_pindexes_actual)">>}]},
                 {struct, [{title, <<"bytes returned/sec">>},
                           {name, per_fts_stat(Id, <<"total_bytes_query_results">>)},
                           {desc, <<"Number of bytes returned in results per second"
                                    " (measured from total_bytes_query_results)">>}]},
                 {struct, [{title, <<"slow queries/sec">>},
                           {name, per_fts_stat(Id, <<"total_queries_slow">>)},
                           {desc, <<"Number of slow queries per second"
                                    " (measured from total_queries_slow - those"
                                    " taking >5s to run)">>}]},
                 {struct, [{isBytes, true},
                           {title, <<"disk size">>},
                           {name, per_fts_stat(Id, <<"num_bytes_used_disk">>)},
                           {desc, <<"Total disk file size used by the index"
                                    " (measured from num_bytes_used_disk)">>}]},
                 {struct, [{title, <<"partitions target">>},
                           {name, per_fts_stat(Id, <<"num_pindexes_target">>)},
                           {desc, <<"Number of index partitions expected"
                                    " (including replica partitions, measured from"
                                    " num_pindexes_target)">>}]},
                 {struct, [{title, <<"files on disk">>},
                           {name, per_fts_stat(Id, <<"num_files_on_disk">>)},
                           {desc, <<"Number of files on disk across all"
                                    " partitions (measured from num_files_on_disk)">>}]},
                 {struct, [{title, <<"memory segments">>},
                           {name, per_fts_stat(Id, <<"num_root_memorysegments">>)},
                           {desc, <<"Number of memory segments across all partitions"
                                    " (measured from num_root_memorysegments)">>}]},
                 {struct, [{title, <<"file segments">>},
                           {name, per_fts_stat(Id, <<"num_root_filesegments">>)},
                           {desc, <<"Number of file segments across all partitions"
                                    " (measured from num_root_filesegments)">>}]},
                 {struct, [{title, <<"term searchers/sec">>},
                           {name, per_fts_stat(Id, <<"total_term_searchers">>)},
                           {desc, <<"Number of term searchers started per second"
                                    " (measured from total_term_searchers)">>}]}]}]}
     || Id <- AllIndexes].

couchbase_query_stats_descriptions() ->
    [{struct, [{blockName, <<"Query">>},
               {extraCSSClasses, <<"dynamic_closed">>},
               {warning, <<"Metrics for Query are not per bucket and will not "
                           "change if bucket dropdown above is changed">>},
               {stats,
                [{struct, [{title, <<"requests/sec">>},
                           {name, <<"query_requests">>},
                           {desc, <<"Number of N1QL requests processed per second">>}]},
                 {struct, [{title, <<"selects/sec">>},
                           {name, <<"query_selects">>},
                           {desc, <<"Number of N1QL selects processed per second">>}]},
                 {struct, [{title, <<"request time(sec)">>},
                           {name, <<"query_avg_req_time">>},
                           {desc, <<"Average end-to-end time to process a query (in seconds)">>}]},
                 {struct, [{title, <<"service time(sec)">>},
                           {name, <<"query_avg_svc_time">>},
                           {desc, <<"Average time to execute a query (in seconds)">>}]},
                 {struct, [{title, <<"result size">>},
                           {name, <<"query_avg_response_size">>},
                           {desc, <<"Average size (in bytes) of the data returned by a query">>}]},
                 {struct, [{title, <<"errors">>},
                           {name, <<"query_errors">>},
                           {desc, <<"Number of N1QL errors returned per second">>}]},
                 {struct, [{title, <<"warnings">>},
                           {name, <<"query_warnings">>},
                           {desc, <<"Number of N1QL errors returned per second">>}]},
                 {struct, [{title, <<"result count">>},
                           {name, <<"query_avg_result_count">>},
                           {desc, <<"Average number of results (documents) returned by a query">>}]},
                 {struct, [{title, <<"queries > 250ms">>},
                           {name, <<"query_requests_250ms">>},
                           {desc, <<"Number of queries that take longer than 250 ms per second">>}]},
                 {struct, [{title, <<"queries > 500ms">>},
                           {name, <<"query_requests_500ms">>},
                           {desc, <<"Number of queries that take longer than 500 ms per second">>}]},
                 {struct, [{title, <<"queries > 1000ms">>},
                           {name, <<"query_requests_1000ms">>},
                           {desc, <<"Number of queries that take longer than 1000 ms per second">>}]},
                 {struct, [{title, <<"queries > 5000ms">>},
                           {name, <<"query_requests_5000ms">>},
                           {desc, <<"Number of queries that take longer than 5000 ms per second">>}]},
                 {struct, [{title, <<"invalid requests/sec">>},
                           {name, <<"query_invalid_requests">>},
                           {desc, <<"Number of requests for unsupported endpoints per second, "
                                    "specifically HTTP requests for all endpoints not supported by "
                                    "the query engine. For example, a request for http://localhost"
                                    ":8093/foo will be included. Potentially useful in identifying "
                                    "DOS attacks.">>}]}]}]}].

membase_query_stats_description(false) ->
    [];
membase_query_stats_description(true) ->
    [{struct,[{title, <<"N1QL queries/sec">>},
              {name, <<"query_requests">>},
              {desc, <<"Number of N1QL requests processed per second">>}]}].

membase_index_stats_description(false) ->
    [];
membase_index_stats_description(true) ->
    [{struct, [{isBytes, true},
               {title, <<"index data size">>},
               {name, global_index_stat(<<"data_size">>)},
               {desc, <<"Actual data size consumed by the index">>}]},
     {struct, [{title, <<"index disk size">>},
               {name, global_index_stat(<<"disk_size">>)},
               {desc, <<"Total disk file size consumed by the index">>},
               {isBytes, true}]},
     {struct, [{title, <<"index fragmentation %">>},
               {name, global_index_stat(<<"fragmentation">>)},
               {desc, <<"Percentage fragmentation of the index. Note: at small index sizes of less "
                        "than a hundred kB, the static overhead of the index disk file will inflate "
                        "the index fragmentation percentage">>}]},
     {struct, [{title, <<"index scanned/sec">>},
               {name, global_index_stat(<<"num_rows_returned">>)},
               {desc, <<"Number of index items scanned by the indexer per second">>}]}].

membase_fts_stats_description(false) ->
    [];
membase_fts_stats_description(true) ->
    [{struct, [{title, <<"fts bytes indexed/sec">>},
               {name, global_fts_stat(<<"total_bytes_indexed">>)},
               {desc, <<"Number of fts bytes indexed per second">>}]},
     {struct, [{title, <<"fts queries/sec">>},
               {name, global_fts_stat(<<"total_queries">>)},
               {desc, <<"Number of fts queries per second">>}]},
     {struct, [{isBytes,true},
               {title, <<"fts disk size">>},
               {name, global_fts_stat(<<"num_bytes_used_disk">>)},
               {desc, <<"Total fts disk file size for this bucket">>}]}].

membase_drift_stats_description() ->
    [{struct, [{title, <<"avg active drift/mutation">>},
               {name, <<"avg_active_timestamp_drift">>},
               {desc, <<"Average drift (in seconds) per mutation on active vBuckets">>}]},
     {struct, [{title, <<"avg replica drift/mutation">>},
               {name, <<"avg_replica_timestamp_drift">>},
               {desc, <<"Average drift (in seconds) per mutation on replica vBuckets">>}]},
     {struct, [{title, <<"active ahead exceptions/sec">>},
               {name, <<"ep_active_ahead_exceptions">>},
               {desc, <<"Total number of ahead exceptions for  all active vBuckets">>}]},
     {struct, [{title, <<"replica ahead exceptions/sec">>},
               {name, <<"ep_replica_ahead_exceptions">>},
               {desc, <<"Total number of ahead exceptions for all replica vBuckets">>}]}].

membase_summary_stats_description(BucketId, ServiceNodes, IsEphemeral) ->
    [{struct,
      [{blockName, <<"Summary">>},
       {stats,
        [{struct,[{title, <<"ops per second">>},
                  {name, <<"ops">>},
                  {desc, <<"Total amount of operations per second "
                           "(including XDCR) to this bucket "
                           "(measured from cmd_lookup + cmd_set "
                           "+ incr_misses + incr_hits + decr_misses "
                           "+ decr_hits + delete_misses + delete_hits "
                           "+ ep_num_ops_del_meta + "
                           "ep_num_ops_get_meta + ep_num_ops_set_meta)">>},
                  {default, true}]},
         {struct, [{title, <<"cache miss ratio">>},
                   {name, <<"ep_cache_miss_rate">>},
                   {desc, <<"Percentage of reads per second to this bucket "
                            "from disk as opposed to RAM (measured from "
                            "ep_bg_fetches / cmd_lookup * 100)">>},
                   {maxY, 100}]},
         {struct, [{title, <<"gets per sec.">>},
                   {name, <<"cmd_get">>},
                   {desc, <<"Number of reads (get operations) per second from this bucket (measured from cmd_get)">>}]},
         {struct, [{title, <<"total gets per sec.">>},
                   {name, <<"cmd_lookup">>},
                   {desc, <<"Number of total get operations per second from "
                            "this bucket (measured from cmd_lookup). "
                            "This includes additional get operations such as "
                            "get locked that are not included in cmd_get">>}]},
         {struct, [{title, <<"sets per sec.">>},
                   {name, <<"cmd_set">>},
                   {desc, <<"Number of writes (set operations) per second to this bucket (measured from cmd_set)">>}]},
         {struct, [{title, <<"deletes per sec.">>},
                   {name, <<"delete_hits">>},
                   {desc, <<"Number of delete operations per second for this bucket (measured from delete_hits)">>}]},
         {struct, [{title, <<"CAS ops per sec.">>},
                   {name, <<"cas_hits">>},
                   {desc, <<"Number of operations with a CAS id per second for this bucket (measured from cas_hits)">>}]},
         {struct, [{title, <<"active docs resident %">>},
                   {name, <<"vb_active_resident_items_ratio">>},
                   {desc, <<"Percentage of active items cached in RAM in this bucket (measured from vb_active_resident_items_ratio)">>},
                   {maxY, 100}]},
         {struct, [{title, <<"items">>},
                   {name, <<"curr_items">>},
                   {desc, <<"Number of unique items in this bucket - only active items, not replica (measured from curr_items)">>}]},
         {struct, [{title, <<"temp OOM per sec.">>},
                   {name, <<"ep_tmp_oom_errors">>},
                   {desc, <<"Number of back-offs sent per second to client SDKs due to "
                            "\"out of memory\" situations from this bucket (measured from ep_tmp_oom_errors)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"low water mark">>},
                   {name, <<"ep_mem_low_wat">>},
                   {desc, <<"Low water mark for auto-evictions (measured from ep_mem_low_wat)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"high water mark">>},
                   {name, <<"ep_mem_high_wat">>},
                   {desc, <<"High water mark for auto-evictions (measured from ep_mem_high_wat)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"memory used">>},
                   {name, <<"mem_used">>},
                   {desc, <<"Memory used as measured from mem_used">>}]}]
        ++ case IsEphemeral of
               true -> [];
               false ->
                   [{struct, [{title, <<"disk creates per sec.">>},
                              {name, <<"ep_ops_create">>},
                              {desc, <<"Number of new items created on disk per second for this bucket "
                                       "(measured from vb_active_ops_create + vb_replica_ops_create "
                                       "+ vb_pending_ops_create)">>}]},
                    {struct, [{title, <<"disk updates per sec.">>},
                              {name, <<"ep_ops_update">>},
                              {desc, <<"Number of items updated on disk per second for this bucket "
                                       "(measured from vb_active_ops_update + vb_replica_ops_update "
                                       "+ vb_pending_ops_update)">>}]},
                    {struct, [{title, <<"disk reads per sec.">>},
                              {name, <<"ep_bg_fetched">>},
                              {desc, <<"Number of reads per second from disk for this bucket (measured from ep_bg_fetched)">>}]},
                    {struct, [{title, <<"disk write queue">>},
                              {name, <<"disk_write_queue">>},
                              {desc, <<"Number of items waiting to be written to disk in this bucket "
                                       "(measured from ep_queue_size+ep_flusher_todo)">>}]},
                    {struct, [{title, <<"disk read failures.">>},
                              {name, <<"ep_data_read_failed">>},
                              {desc, <<"Number of disk read failures (measured from ep_data_read_failed)">>}]},
                    {struct, [{title, <<"disk write failures.">>},
                              {name, <<"ep_data_write_failed">>},
                              {desc, <<"Number of disk write failures (measured from ep_data_write_failed)">>}]},
                    {struct, [{isBytes, true},
                              {name, <<"couch_docs_data_size">>},
                              {title, <<"docs data size">>},
                              {desc, <<"The size of active data in this bucket "
                                       "(measured from couch_docs_data_size)">>}]},
                    {struct,[{isBytes, true},
                             {name, <<"couch_docs_actual_disk_size">>},
                             {title, <<"docs total disk size">>},
                             {desc, <<"The size of all data files for this bucket, including the data "
                                      "itself, meta data and temporary files "
                                      "(measured from couch_docs_actual_disk_size)">>}]},
                    {struct, [{name, <<"couch_docs_fragmentation">>},
                              {title, <<"docs fragmentation %">>},
                              {desc, <<"How much fragmented data there is to be compacted compared "
                                       "to real data for the data files in this bucket "
                                       "(measured from couch_docs_fragmentation)">>}]},
                    {struct, [{isBytes, true},
                              {name, <<"couch_total_disk_size">>},
                              {title, <<"total disk size">>},
                              {desc, <<"The total size on disk of all data and view files for this bucket."
                                       "(measured from couch_total_disk_size)">>}]},
                    {struct, [{isBytes, true},
                              {name, <<"couch_views_data_size">>},
                              {title, <<"views data size">>},
                              {desc, <<"The size of active data on for all the indexes in this bucket"
                                       "(measured from couch_views_data_size)">>}]},
                    {struct, [{isBytes, true},
                              {name, <<"couch_views_actual_disk_size">>},
                              {title, <<"views total disk size">>},
                              {desc, <<"The size of all active items in all the indexes for this bucket on disk"
                                       "(measured from couch_views_actual_disk_size)">>}]},
                    {struct, [{name, <<"couch_views_fragmentation">>},
                              {title, <<"views fragmentation %">>},
                              {desc, <<"How much fragmented data there is to be compacted compared "
                                       "to real data for the view index files in this bucket"
                                       "(measured from couch_views_fragmentation)">>}]},
                    {struct, [{name, <<"couch_views_ops">>},
                              {title, <<"view reads per sec.">>},
                              {desc, <<"All the view reads for all design documents including scatter gather."
                                       "(measured from couch_views_ops)">>}]},
                    {struct, [{title, <<"disk update time">>},
                              {name, <<"avg_disk_update_time">>},
                              {hidden, true},
                              {desc, <<"Average disk update time in microseconds as from disk_update histogram of timings"
                                       "(measured from avg_disk_update_time)">>}]},
                    {struct, [{title, <<"disk commit time">>},
                              {name, <<"avg_disk_commit_time">>},
                              {hidden, true},
                              {desc, <<"Average disk commit time in seconds as from disk_update histogram of timings"
                                       "(measured from avg_disk_commit_time)">>}]}]
           end
        ++ [{struct, [{title, <<"bg wait time">>},
                      {hidden, true},
                      {name, <<"avg_bg_wait_time">>},
                      {desc, <<"Average background fetch time in microseconds"
                               "(measured from avg_bg_wait_time)">>}]},
            {struct, [{title, <<"incoming XDCR ops/sec.">>},
                      {name, <<"xdc_ops">>},
                      {desc, <<"Incoming XDCR operations per second for this bucket "
                               "(measured from xdc_ops)">>}]},
            {struct, [{title, <<"intra-replication queue">>},
                      {name, <<"ep_dcp_replica_items_remaining">>},
                      {desc,<<"Number of items remaining to be sent to consumer in this bucket "
                              "(measured from ep_dcp_replica_items_remaining)">>}]}]
        ++ case display_outbound_xdcr_mutations(BucketId) of
               true ->
                   [{struct, [{title, <<"outbound XDCR mutations">>},
                              {name, <<"replication_changes_left">>},
                              {desc, <<"Number of mutations to be replicated to other clusters"
                                       "(measured from replication_changes_left)">>}]}];
               false ->
                   []
           end
        ++ membase_query_stats_description(has_nodes(n1ql, ServiceNodes))
        ++ membase_index_stats_description(has_nodes(index, ServiceNodes))
        ++ membase_fts_stats_description(has_nodes(fts, ServiceNodes))
        ++ membase_drift_stats_description()
       }]}].

membase_vbucket_resources_stats_description() ->
    [{struct,
      [{blockName, <<"vBucket Resources">>},
       {extraCSSClasses, <<"dynamic_withtotal dynamic_closed">>},
       {columns,
        [<<"Active">>, <<"Replica">>, <<"Pending">>, <<"Total">>]},
       {stats,
        [{struct, [{title, <<"vBuckets">>},
                   {name, <<"vb_active_num">>},
                   {desc, <<"Number of vBuckets in the \"active\" state for this bucket (measured from vb_active_num)">>}]},
         {struct, [{title, <<"vBuckets">>},
                   {name, <<"vb_replica_num">>},
                   {desc, <<"Number of vBuckets in the \"replica\" state for this bucket (measured from vb_replica_num)">>}]},
         {struct, [{title, <<"vBuckets">>},
                   {name, <<"vb_pending_num">>},
                   {desc, <<"Number of vBuckets in the \"pending\" state for this bucket and should "
                            "be transient during rebalancing (measured from vb_pending_num)">>}]},
         {struct, [{title, <<"vBuckets">>},
                   {name, <<"ep_vb_total">>},
                   {desc, <<"Total number of vBuckets for this bucket (measured from ep_vb_total)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"curr_items">>},
                   {desc, <<"Number of items in \"active\" vBuckets in this bucket (measured from curr_items)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"vb_replica_curr_items">>},
                   {desc, <<"Number of items in \"replica\" vBuckets in this bucket (measured from vb_replica_curr_items)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"vb_pending_curr_items">>},
                   {desc, <<"Number of items in \"pending\" vBuckets in this bucket and should be "
                            "transient during rebalancing (measured from vb_pending_curr_items)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"curr_items_tot">>},
                   {desc, <<"Total number of items in this bucket (measured from curr_items_tot)">>}]},
         {struct, [{title, <<"resident %">>},
                   {name, <<"vb_active_resident_items_ratio">>},
                   {desc, <<"Percentage of active items cached in RAM in this bucket (measured from "
                            "vb_active_resident_items_ratio)">>},
                   {maxY, 100}]},
         {struct, [{title, <<"resident %">>},
                   {name, <<"vb_replica_resident_items_ratio">>},
                   {desc, <<"Percentage of replica items cached in RAM in this bucket "
                            "(measured from vb_replica_resident_items_ratio)">>},
                   {maxY, 100}]},
         {struct, [{title, <<"resident %">>},
                   {name, <<"vb_pending_resident_items_ratio">>},
                   {desc, <<"Percentage of items in pending state vbuckets cached in RAM in this bucket "
                            "(measured from vb_pending_resident_items_ratio)">>},
                   {maxY, 100}]},
         {struct, [{title, <<"resident %">>},
                   {name, <<"ep_resident_items_rate">>},
                   {desc, <<"Percentage of all items cached in RAM in this bucket (measured from ep_resident_items_rate)">>},
                   {maxY, 100}]},
         {struct, [{title, <<"new items per sec.">>},
                   {name, <<"vb_active_ops_create">>},
                   {desc, <<"New items per second being inserted into \"active\" vBuckets in this "
                            "bucket (measured from vb_active_ops_create)">>}]},
         {struct, [{title, <<"new items per sec.">>},
                   {name, <<"vb_replica_ops_create">>},
                   {desc, <<"New items per second being inserted into \"replica\" vBuckets in this "
                            "bucket (measured from vb_replica_ops_create)">>}]},
         {struct, [{title, <<"new items per sec.">>},
                   {name, <<"vb_pending_ops_create">>},
                   {desc, <<"New items per second being inserted into \"pending\" vBuckets in this "
                            "bucket and should be transient during rebalancing (measured from vb_pending_ops_create)">>}]},
         {struct, [{title, <<"new items per sec.">>},
                   {name, <<"ep_ops_create">>},
                   {desc, <<"Total number of new items being inserted into this bucket (measured from ep_ops_create)">>}]},
         {struct, [{title, <<"ejections per sec.">>},
                   {name, <<"vb_active_eject">>},
                   {desc, <<"Number of items per second being ejected to disk from \"active\" "
                            "vBuckets in this bucket (measured from vb_active_eject)">>}]},
         {struct, [{title, <<"ejections per sec.">>},
                   {name, <<"vb_replica_eject">>},
                   {desc, <<"Number of items per second being ejected to disk from \"replica\" "
                            "vBuckets in this bucket (measured from vb_replica_eject)">>}]},
         {struct, [{title, <<"ejections per sec.">>},
                   {name, <<"vb_pending_eject">>},
                   {desc, <<"Number of items per second being ejected to disk from \"pending\" "
                            "vBuckets in this bucket and should be transient during rebalancing "
                            "(measured from vb_pending_eject)">>}]},
         {struct, [{title, <<"ejections per sec.">>},
                   {name, <<"ep_num_value_ejects">>},
                   {desc, <<"Total number of items per second being ejected to disk in this bucket "
                            "(measured from ep_num_value_ejects)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"user data in RAM">>},
                   {name, <<"vb_active_itm_memory">>},
                   {desc, <<"Amount of active user data cached in RAM in this bucket "
                            "(measured from vb_active_itm_memory)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"user data in RAM">>},
                   {name, <<"vb_replica_itm_memory">>},
                   {desc, <<"Amount of replica user data cached in RAM in this bucket (measured from vb_replica_itm_memory)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"user data in RAM">>},
                   {name, <<"vb_pending_itm_memory">>},
                   {desc, <<"Amount of pending user data cached in RAM in this bucket and should be "
                            "transient during rebalancing (measured from vb_pending_itm_memory)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"user data in RAM">>},
                   {name, <<"ep_kv_size">>},
                   {desc, <<"Total amount of user data cached in RAM in this bucket (measured from ep_kv_size)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"metadata in RAM">>},
                   {name, <<"vb_active_meta_data_memory">>},
                   {desc, <<"Amount of active item metadata consuming RAM in this bucket "
                            "(measured from vb_active_meta_data_memory)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"metadata in RAM">>},
                   {name, <<"vb_replica_meta_data_memory">>},
                   {desc, <<"Amount of replica item metadata consuming in RAM in this bucket "
                            "(measured from vb_replica_meta_data_memory)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"metadata in RAM">>},
                   {name, <<"vb_pending_meta_data_memory">>},
                   {desc, <<"Amount of pending item metadata consuming RAM in this bucket and "
                            "should be transient during rebalancing (measured from vb_pending_meta_data_memory)">>}]},
         {struct, [{isBytes, true},
                   {title, <<"metadata in RAM">>},
                   {name, <<"ep_meta_data_memory">>},
                   {desc, <<"Total amount of item  metadata consuming RAM in this bucket "
                            "(measured from ep_meta_data_memory)">>}]}]}]}].

membase_disk_queues_stats_description() ->
    [{struct,
      [{blockName, <<"Disk Queues">>},
       {extraCSSClasses, <<"dynamic_withtotal dynamic_closed">>},
       {columns, [<<"Active">>, <<"Replica">>, <<"Pending">>, <<"Total">>]},
       {stats,
        [{struct, [{title, <<"items">>},
                   {name, <<"vb_active_queue_size">>},
                   {desc, <<"Number of active items waiting to be written to disk in this bucket "
                            "(measured from vb_active_queue_size)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"vb_replica_queue_size">>},
                   {desc, <<"Number of replica items waiting to be written to disk in this bucket "
                            "(measured from vb_replica_queue_size)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"vb_pending_queue_size">>},
                   {desc, <<"Number of pending items waiting to be written to disk in this bucket "
                            "and should be transient during rebalancing  (measured from vb_pending_queue_size)">>}]},
         {struct, [{title, <<"items">>},
                   {name, <<"ep_diskqueue_items">>},
                   {desc, <<"Total number of items waiting to be written to disk in this bucket "
                            "(measured from ep_diskqueue_items)">>}]},
         {struct, [{title, <<"fill rate">>},
                   {name, <<"vb_active_queue_fill">>},
                   {desc, <<"Number of active items per second being put on the active item disk "
                            "queue in this bucket (measured from vb_active_queue_fill)">>}]},
         {struct, [{title, <<"fill rate">>},
                   {name, <<"vb_replica_queue_fill">>},
                   {desc, <<"Number of replica items per second being put on the replica item disk "
                            "queue in this bucket (measured from vb_replica_queue_fill)">>}]},
         {struct, [{title, <<"fill rate">>},
                   {name, <<"vb_pending_queue_fill">>},
                   {desc, <<"Number of pending items per second being put on the pending item disk "
                            "queue in this bucket and should be transient during rebalancing "
                            "(measured from vb_pending_queue_fill)">>}]},
         {struct, [{title, <<"fill rate">>},
                   {name, <<"ep_diskqueue_fill">>},
                   {desc, <<"Total number of items per second being put on the disk queue in this "
                            "bucket (measured from ep_diskqueue_fill)">>}]},
         {struct, [{title, <<"drain rate">>},
                   {name, <<"vb_active_queue_drain">>},
                   {desc, <<"Number of active items per second being written to disk in this bucket "
                            "(measured from vb_active_queue_drain)">>}]},
         {struct, [{title, <<"drain rate">>},
                   {name, <<"vb_replica_queue_drain">>},
                   {desc, <<"Number of replica items per second being written to disk in this bucket "
                            "(measured from vb_replica_queue_drain)">>}]},
         {struct, [{title, <<"drain rate">>},
                   {name, <<"vb_pending_queue_drain">>},
                   {desc, <<"Number of pending items per second being written to disk in this bucket "
                            "and should be transient during rebalancing (measured from vb_pending_queue_drain)">>}]},
         {struct, [{title, <<"drain rate">>},
                   {name, <<"ep_diskqueue_drain">>},
                   {desc, <<"Total number of items per second being written to disk in this bucket "
                            "(measured from ep_diskqueue_drain)">>}]},
         {struct, [{title, <<"average age">>},
                   {name, <<"vb_avg_active_queue_age">>},
                   {desc, <<"Average age in seconds of active items in the active item queue for "
                            "this bucket (measured from vb_avg_active_queue_age)">>}]},
         {struct, [{title, <<"average age">>},
                   {name, <<"vb_avg_replica_queue_age">>},
                   {desc, <<"Average age in seconds of replica items in the replica item queue for "
                            "this bucket (measured from vb_avg_replica_queue_age)">>}]},
         {struct, [{title, <<"average age">>},
                   {name, <<"vb_avg_pending_queue_age">>},
                   {desc, <<"Average age in seconds of pending items in the pending item queue for "
                            "this bucket and should be transient during rebalancing (measured from "
                            "vb_avg_pending_queue_age)">>}]},
         {struct, [{title, <<"average age">>},
                   {name, <<"vb_avg_total_queue_age">>},
                   {desc, <<"Average age in seconds of all items in the disk write queue for this "
                            "bucket (measured from vb_avg_total_queue_age)">>}]}]}]}].

membase_dcp_queues_stats_description() ->
    [{struct,
      [{blockName,<<"DCP Queues">>},
       {extraCSSClasses,<<"dynamic_closed">>},
       {columns, [<<"Replication">>, <<"XDCR">>, <<"Views/Indexes">>, <<"Analytics">>, <<"Eventing">>, <<"Other">>]},
       {stats,
        [{struct, [{title, <<"DCP connections">>},
                   {name, <<"ep_dcp_replica_count">>},
                   {desc, <<"Number of internal replication DCP connections in this bucket "
                            "(measured from ep_dcp_replica_count)">>}]},
         {struct, [{title, <<"DCP connections">>},
                   {name, <<"ep_dcp_xdcr_count">>},
                   {desc, <<"Number of internal xdcr DCP connections in this bucket "
                            "(measured from ep_dcp_xdcr_count)">>}]},
         {struct, [{title, <<"DCP connections">>},
                   {name, <<"ep_dcp_views+indexes_count">>},
                   {desc, <<"Number of internal views/indexes DCP connections in this bucket "
                            "(measured from ep_dcp_views_count + ep_dcp_2i_count + ep_dcp_fts_count)">>}]},
         {struct, [{title, <<"DCP connections">>},
                   {name, <<"ep_dcp_cbas_count">>},
                   {desc, <<"Number of internal analytics DCP connections in this bucket "
                            "(measured from ep_dcp_cbas_count)">>}]},
         {struct, [{title, <<"DCP connections">>},
                   {name, <<"ep_dcp_eventing_count">>},
                   {desc, <<"Number of eventing DCP connections in this bucket (measured from ep_dcp_eventing_count)">>}]},
         {struct, [{title, <<"DCP connections">>},
                   {name, <<"ep_dcp_other_count">>},
                   {desc, <<"Number of other DCP connections in this bucket (measured from ep_dcp_other_count)">>}]},
         {struct, [{title, <<"DCP senders">>},
                   {name, <<"ep_dcp_replica_producer_count">>},
                   {desc, <<"Number of replication senders for this bucket (measured from ep_dcp_replica_producer_count)">>}]},
         {struct, [{title, <<"DCP senders">>},
                   {name, <<"ep_dcp_xdcr_producer_count">>},
                   {desc, <<"Number of xdcr senders for this bucket (measured from ep_dcp_xdcr_producer_count)">>}]},
         {struct, [{title, <<"DCP senders">>},
                   {name, <<"ep_dcp_views+indexes_producer_count">>},
                         {desc,<<"Number of views/indexes senders for this bucket (measured from "
                                 "ep_dcp_views_producer_count + ep_dcp_2i_producer_count + ep_dcp_fts_producer_count)">>}]},
         {struct, [{title, <<"DCP senders">>},
                   {name, <<"ep_dcp_cbas_producer_count">>},
                   {desc, <<"Number of analytics senders for this bucket (measured from ep_dcp_cbas_producer_count)">>}]},
         {struct, [{title, <<"DCP senders">>},
                   {name, <<"ep_dcp_eventing_producer_count">>},
                   {desc, <<"Number of eventing senders for this bucket (measured from ep_dcp_eventing_producer_count)">>}]},
         {struct, [{title, <<"DCP senders">>},
                   {name, <<"ep_dcp_other_producer_count">>},
                   {desc, <<"Number of other senders for this bucket (measured from ep_dcp_other_producer_count)">>}]},
         {struct, [{title, <<"items remaining">>},
                   {name, <<"ep_dcp_replica_items_remaining">>},
                   {desc, <<"Number of items remaining to be sent to consumer in this bucket "
                            "(measured from ep_dcp_replica_items_remaining)">>}]},
         {struct, [{title, <<"items remaining">>},
                   {name, <<"ep_dcp_xdcr_items_remaining">>},
                   {desc, <<"Number of items remaining to be sent to consumer in this bucket "
                            "(measured from ep_dcp_xdcr_items_remaining)">>}]},
         {struct, [{title, <<"items remaining">>},
                   {name, <<"ep_dcp_views+indexes_items_remaining">>},
                   {desc, <<"Number of items remaining to be sent to consumer in this bucket "
                            "(measured from ep_dcp_views_items_remaining + ep_dcp_2i_items_remaining "
                            "+ ep_dcp_fts_items_remaining)">>}]},
         {struct, [{title, <<"items remaining">>},
                   {name, <<"ep_dcp_cbas_items_remaining">>},
                   {desc, <<"Number of items remaining to be sent to consumer in this bucket "
                            "(measured from ep_dcp_cbas_items_remaining)">>}]},
         {struct, [{title, <<"items remaining">>},
                   {name, <<"ep_dcp_eventing_items_remaining">>},
                   {desc, <<"Number of items remaining to be sent to consumer in this bucket "
                            "(measured from ep_dcp_eventing_items_remaining)">>}]},
         {struct, [{title, <<"items remaining">>},
                   {name, <<"ep_dcp_other_items_remaining">>},
                   {desc, <<"Number of items remaining to be sent to consumer in this bucket "
                            "(measured from ep_dcp_other_items_remaining)">>}]},
         {struct, [{title, <<"drain rate items/sec">>},
                   {name, <<"ep_dcp_replica_items_sent">>},
                   {desc, <<"Number of items per second being sent for a producer for this bucket "
                            "(measured from ep_dcp_replica_items_sent)">>}]},
         {struct, [{title, <<"drain rate items/sec">>},
                   {name, <<"ep_dcp_xdcr_items_sent">>},
                   {desc, <<"Number of items per second being sent for a producer for this bucket "
                            "(measured from ep_dcp_xdcr_items_sent)">>}]},
         {struct, [{title, <<"drain rate items/sec">>},
                   {name, <<"ep_dcp_views+indexes_items_sent">>},
                   {desc, <<"Number of items per second being sent for a producer for this bucket "
                            "(measured from ep_dcp_views_items_sent + ep_dcp_2i_items_sent + ep_dcp_fts_items_sent)">>}]},
         {struct, [{title, <<"drain rate items/sec">>},
                   {name, <<"ep_dcp_cbas_items_sent">>},
                   {desc, <<"Number of items per second being sent for a producer for this bucket "
                             "(measured from ep_dcp_cbas_items_sent)">>}]},
         {struct, [{title, <<"drain rate items/sec">>},
                   {name, <<"ep_dcp_eventing_items_sent">>},
                   {desc, <<"Number of items per second being sent for a producer for this bucket "
                            "(measured from ep_dcp_eventing_items_sent)">>}]},
         {struct, [{title, <<"drain rate items/sec">>},
                   {name, <<"ep_dcp_other_items_sent">>},
                   {desc, <<"Number of items per second being sent for a producer for this bucket "
                            "(measured from ep_dcp_other_items_sent)">>}]},
         {struct, [{title, <<"drain rate bytes/sec">>},
                   {name, <<"ep_dcp_replica_total_bytes">>},
                   {desc, <<"Number of bytes per second being sent for replication DCP connections "
                            "for this bucket (measured from ep_dcp_replica_total_bytes)">>}]},
         {struct, [{title, <<"drain rate bytes/sec">>},
                   {name, <<"ep_dcp_xdcr_total_bytes">>},
                   {desc, <<"Number of bytes per second being sent for xdcr DCP connections for "
                            "this bucket (measured from ep_dcp_xdcr_total_bytes)">>}]},
         {struct, [{title, <<"drain rate bytes/sec">>},
                   {name, <<"ep_dcp_views+indexes_total_bytes">>},
                   {desc, <<"Number of bytes per second being sent for views/indexes DCP connections "
                            "for this bucket (measured from ep_dcp_views_total_bytes + "
                            "ep_dcp_2i_total_bytes + ep_dcp_fts_total_bytes)">>}]},
         {struct, [{title, <<"drain rate bytes/sec">>},
                   {name, <<"ep_dcp_cbas_total_bytes">>},
                   {desc, <<"Number of bytes per second being sent for analytics DCP connections for "
                            "this bucket (measured from ep_dcp_cbas_total_bytes)">>}]},
         {struct, [{title, <<"drain rate bytes/sec">>},
                   {name, <<"ep_dcp_eventing_total_bytes">>},
                   {desc, <<"Number of bytes per second being sent for eventing DCP connections for "
                            "this bucket (measured from ep_dcp_eventing_total_bytes)">>}]},
         {struct, [{title, <<"drain rate bytes/sec">>},
                   {name, <<"ep_dcp_other_total_bytes">>},
                   {desc, <<"Number of bytes per second being sent for other DCP connections for "
                            "this bucket (measured from ep_dcp_other_total_bytes)">>}]},
         {struct, [{title, <<"backoffs/sec">>},
                   {name, <<"ep_dcp_replica_backoff">>},
                   {desc, <<"Number of backoffs for replication DCP connections">>}]},
         {struct, [{title, <<"backoffs/sec">>},
                   {name, <<"ep_dcp_xdcr_backoff">>},
                   {desc,<<"Number of backoffs for xdcr DCP connections">>}]},
         {struct, [{title, <<"backoffs/sec">>},
                   {name, <<"ep_dcp_views+indexes_backoff">>},
                   {desc, <<"Number of backoffs for views/indexes DCP connections "
                            "(measured from ep_dcp_views_backoff + ep_dcp_2i_backoff + ep_dcp_fts_backoff)">>}]},
         {struct, [{title, <<"backoffs/sec">>},
                   {name, <<"ep_dcp_cbas_backoff">>},
                   {desc, <<"Number of backoffs for analytics DCP connections "
                            "(measured from ep_dcp_cbas_backoff)">>}]},
         {struct, [{title, <<"backoffs/sec">>},
                   {name, <<"ep_dcp_eventing_backoff">>},
                   {desc, <<"Number of backoffs for eventing DCP connections "
                            "(measured from ep_dcp_eventing_backoff)">>}]},
         {struct, [{title, <<"backoffs/sec">>},
                   {name, <<"ep_dcp_other_backoff">>},
                   {desc, <<"Number of backoffs for other DCP connections "
                            "(measured from ep_dcp_other_backoff)">>}]}
        ]}]}].

membase_incoming_xdcr_operations_stats_description() ->
    [{struct,
      [{blockName,<<"Incoming XDCR Operations">>},
       {bigTitlePrefix, <<"Incoming XDCR">>},
       {extraCSSClasses,<<"dynamic_closed">>},
       {stats,
        [{struct,[{title, <<"metadata reads per sec.">>},
                  {bigTitle, <<"Incoming XDCR metadata reads per sec.">>},
                  {name, <<"ep_num_ops_get_meta">>},
                  {desc, <<"Number of metadata read operations per second for this bucket as the target for XDCR "
                           "(measured from ep_num_ops_get_meta)">>}]},
         {struct, [{title, <<"sets per sec.">>},
                   {name, <<"ep_num_ops_set_meta">>},
                   {desc, <<"Number of set operations per second for this bucket as the target for XDCR"
                            "(measured from ep_num_ops_set_meta)">>}]},
         {struct, [{title, <<"deletes per sec.">>},
                   {name, <<"ep_num_ops_del_meta">>},
                   {desc, <<"Number of delete operations per second for this bucket as the target for XDCR "
                            "(measured from ep_num_ops_del_meta)">>}]},
         {struct, [{title, <<"total ops per sec.">>},
                   {bigTitle, <<"Incoming XDCR total ops/sec.">>},
                   {name, <<"xdc_ops">>},
                   {desc, <<"Total XDCR operations per second for this bucket "
                            "(measured from ep_num_ops_del_meta + ep_num_ops_get_meta + ep_num_ops_set_meta)">>}]}]}]}].

display_outbound_xdcr_mutations(BucketID) ->
    goxdcr_status_keeper:get_replications(BucketID) =/= [].

ephemeral_stats_description(BucketId, ServiceNodes) ->
    membase_summary_stats_description(BucketId, ServiceNodes, true)
        ++ membase_vbucket_resources_stats_description()
        ++ membase_dcp_queues_stats_description()
        ++ couchbase_index_stats_descriptions(BucketId, ServiceNodes)
        ++ couchbase_fts_stats_descriptions(BucketId, ServiceNodes)
        ++ couchbase_goxdcr_stats_descriptions(BucketId)
        ++ case has_nodes(n1ql, ServiceNodes) of
               true -> couchbase_query_stats_descriptions();
               false -> []
           end
        ++ couchbase_eventing_stats_descriptions(ServiceNodes)
        ++ membase_incoming_xdcr_operations_stats_description().


membase_stats_description(BucketId, ServiceNodes) ->
    membase_summary_stats_description(BucketId, ServiceNodes, false)
        ++ membase_vbucket_resources_stats_description()
        ++ membase_disk_queues_stats_description()
        ++ membase_dcp_queues_stats_description()
        ++ couchbase_view_stats_descriptions(BucketId)
        ++ couchbase_index_stats_descriptions(BucketId, ServiceNodes)
        ++ couchbase_cbas_stats_descriptions(ServiceNodes)
        ++ couchbase_fts_stats_descriptions(BucketId, ServiceNodes)
        ++ couchbase_goxdcr_stats_descriptions(BucketId)
        ++ case has_nodes(n1ql, ServiceNodes) of
               true -> couchbase_query_stats_descriptions();
               false -> []
           end
        ++ couchbase_eventing_stats_descriptions(ServiceNodes)
        ++ membase_incoming_xdcr_operations_stats_description().


memcached_stats_description() ->
    [{struct,
      [{blockName, <<"Memcached">>},
       {stats,
        [{struct, [{name, <<"ops">>},
                   {title, <<"ops per sec.">>},
                   {default, true},
                   {desc, <<"Total operations per second serviced by this bucket (measured from "
                            "cmd_get + cmd_set + incr_misses + incr_hits + decr_misses + decr_hits "
                            "+ delete_misses + delete_hits + get_meta + set_meta + delete_meta)">>}]},
         {struct, [{name, <<"hit_ratio">>},
                   {title, <<"hit ratio">>},
                   {maxY, 100},
                   {desc, <<"Percentage of get requests served with data from this bucket "
                            "(measured from get_hits * 100/cmd_get)">>}]},
         {struct, [{isBytes, true},
                   {name, <<"mem_used">>},
                   {title, <<"RAM used">>},
                   {desc, <<"Total amount of RAM used by this bucket (measured from mem_used)">>}]},
         {struct, [{name, <<"curr_items">>},
                   {title, <<"items">>},
                   {desc, <<"Number of items stored in this bucket (measured from curr_items)">>}]},
         {struct, [{name, <<"evictions">>},
                   {title, <<"evictions per sec.">>},
                   {desc, <<"Number of items per second evicted from this bucket (measured from evictions)">>}]},
         {struct, [{name, <<"cmd_set">>},
                   {title, <<"sets per sec.">>},
                   {desc, <<"Number of set operations serviced by this bucket (measured from cmd_set)">>}]},
         {struct, [{name, <<"cmd_get">>},
                   {title, <<"gets per sec.">>},
                   {desc, <<"Number of get operations serviced by this bucket (measured from cmd_get)">>}]},
         {struct, [{name, <<"bytes_written">>},
                   {title, <<"bytes TX per sec.">>},
                   {desc, <<"Number of bytes per second sent from this bucket (measured from bytes_written)">>}]},
         {struct, [{name, <<"bytes_read">>},
                   {title, <<"bytes RX per sec.">>},
                   {desc, <<"Number of bytes per second sent into this bucket (measured from bytes_read)">>}]},
         {struct, [{name, <<"get_hits">>},
                   {title, <<"get hits per sec.">>},
                   {desc, <<"Number of get operations per second for data that this bucket contains "
                            "(measured from get_hits)">>}]},
         {struct, [{name, <<"delete_hits">>},
                   {title, <<"delete hits per sec.">>},
                   {desc, <<"Number of delete operations per second for data that this bucket "
                            "contains (measured from delete_hits)">>}]},
         {struct,[{name, <<"incr_hits">>},
                  {title, <<"incr hits per sec.">>},
                  {desc, <<"Number of increment operations per second for data that this bucket "
                           "contains (measured from incr_hits)">>}]},
         {struct, [{name, <<"decr_hits">>},
                   {title, <<"decr hits per sec.">>},
                   {desc, <<"Number of decrement operations per second for data that this bucket "
                            "contains (measured from decr_hits)">>}]},
         {struct, [{name, <<"delete_misses">>},
                   {title, <<"delete misses per sec.">>},
                   {desc, <<"Number of delete operations per second for data that this bucket does "
                            "not contain (measured from delete_misses)">>}]},
         {struct, [{name, <<"decr_misses">>},
                   {title, <<"decr misses per sec.">>},
                   {desc, <<"Number of decr operations per second for data that this bucket does "
                            "not contain (measured from decr_misses)">>}]},
         {struct, [{name, <<"get_misses">>},
                   {title, <<"get misses per sec.">>},
                   {desc, <<"Number of get operations per second for data that this bucket does not "
                            "contain (measured from get_misses)">>}]},
         {struct, [{name, <<"incr_misses">>},
                   {title, <<"incr misses per sec.">>},
                   {desc, <<"Number of increment operations per second for data that this bucket "
                            "does not contain (measured from incr_misses)">>}]},
         {struct, [{name, <<"cas_hits">>},
                   {title, <<"CAS hits per sec.">>},
                   {desc, <<"Number of CAS operations per second for data that this bucket contains "
                            "(measured from cas_hits)">>}]},
         {struct, [{name, <<"cas_badval">>},
                   {title, <<"CAS badval per sec.">>},
                   {desc, <<"Number of CAS operations per second using an incorrect CAS ID for data "
                            "that this bucket contains (measured from cas_badval)">>}]},
         {struct, [{name, <<"cas_misses">>},
                   {title, <<"CAS misses per sec.">>},
                   {desc, <<"Number of CAS operations per second for data that this bucket does not "
                            "contain (measured from cas_misses)">>}]}]}]}].


index_server_resources_stats_description(false) ->
    [];
index_server_resources_stats_description(true) ->
    [{struct, [{name, <<"index_ram_percent">>},
               {title, <<"Max Index RAM Used %">>},
               {desc, <<"Percentage of Index RAM in use across all indexes on this server">>}]},
     {struct, [{name, <<"index_remaining_ram">>},
               {title, <<"remaining index ram">>},
               {desc, <<"Amount of index RAM available on this server">>}]}].

fts_server_resources_stats_description(false) ->
    [];
fts_server_resources_stats_description(true) ->
    [{struct, [{isBytes, true},
               {name, <<"fts_num_bytes_used_ram">>},
               {title, <<"fts RAM used">>},
               {desc, <<"Amount of RAM used by FTS on this server">>}]},
     {struct, [{title, <<"fts queries rejected">>},
               {name, <<"fts_total_queries_rejected_by_herder">>},
               {desc, <<"Number of fts queries rejected by the FTS throttler"
                        " due to high memory consumption">>}]},
     {struct, [{title, <<"fts blocked dcp batches">>},
               {name, <<"fts_curr_batches_blocked_by_herder">>},
               {desc, <<"Number of DCP batches blocked by the FTS throttler"
                        " due to high memory consumption">>}]}].

cbas_server_resources_stats_description(false) ->
    [];
cbas_server_resources_stats_description(true) ->
    [{struct, [{isBytes, true},
               {name, <<"cbas_heap_used">>},
               {title, <<"analytics heap used">>},
               {desc, <<"Amount of JVM heap used by Analytics on this server">>}]},
     {struct, [{name, <<"cbas_system_load_average">>},
               {title, <<"analytics system load">>},
               {desc, <<"System load for Analytics node">>}]},
     {struct, [{name, <<"cbas_thread_count">>},
               {title, <<"analytics thread count">>},
               {desc, <<"Number of threads for Analytics node">>}]},
     {struct, [{name, <<"cbas_gc_count">>},
               {title, <<"analytics gc count/sec">>},
               {desc, <<"Number of JVM garbage collections for Analytics node">>}]},
     {struct, [{name, <<"cbas_gc_time">>},
               {title, <<"analytics gc time (ms.)/sec">>},
               {desc, <<"The amount of time in milliseconds spent performing JVM garbage collections for Analytics node">>}]},
     {struct, [{name, <<"cbas_io_reads">>},
               {title, <<"analytics bytes read/sec">>},
               {desc, <<"Number of disk bytes read on Analytics node per second">>}]},
     {struct, [{name, <<"cbas_io_writes">>},
               {title, <<"analytics bytes written/sec">>},
               {desc, <<"Number of disk bytes written on Analytics node per second">>}]},
     {struct, [{isBytes, true},
               {name, <<"cbas_disk_used">>},
               {title, <<"analytics total disk size">>},
               {desc, <<"The total disk size used by Analytics">>}]}].

server_resources_stats_description(ServiceNodes) ->
    [{blockName, <<"Server Resources">>},
     {serverResources, true},
     {stats,
      [{struct, [{isBytes, true},
                 {name, <<"swap_used">>},
                 {title, <<"swap usage">>},
                 {desc, <<"Amount of swap space in use on this server">>}]},
       {struct, [{isBytes, true},
                 {name, <<"mem_actual_free">>},
                 {title, <<"free RAM">>},
                 {desc, <<"Amount of RAM available on this server">>}]},
       {struct, [{name, <<"cpu_utilization_rate">>},
                 {title, <<"Max CPU utilization %">>},
                 {desc, <<"Percentage of CPU in use across all available cores on this server">>},
                 {maxY, 100}]},
       {struct, [{name, <<"curr_connections">>},
                 {title, <<"connections">>},
                 {desc, <<"Number of connections to this server including "
                          "connections from external client SDKs, proxies, "
                          "DCP requests and internal statistic gathering "
                          "(measured from curr_connections)">>}]},
       {struct, [{name, <<"rest_requests">>},
                 {title, <<"Management port reqs/sec">>},
                 {desc, <<"Rate of http requests on management port (usually, 8091)">>}]},
       {struct, [{name, <<"hibernated_requests">>},
                 {title, <<"idle streaming requests">>},
                 {desc, <<"Number of streaming requests on management port (usually, 8091) now idle">>}]},
       {struct, [{name, <<"hibernated_waked">>},
                 {title, <<"streaming wakeups/sec">>},
                 {desc, <<"Rate of streaming request wakeups on management port (usually, 8091)">>}]}
       | index_server_resources_stats_description(
           has_nodes(index, ServiceNodes)) ++
           fts_server_resources_stats_description(
             has_nodes(fts, ServiceNodes)) ++
           cbas_server_resources_stats_description(
             has_nodes(cbas, ServiceNodes))]}].

base_stats_directory(BucketId, ServiceNodes) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketId),
    Base = case ns_bucket:external_bucket_type(BucketConfig) of
               membase -> membase_stats_description(BucketId, ServiceNodes);
               memcached -> memcached_stats_description();
               ephemeral -> ephemeral_stats_description(BucketId, ServiceNodes)
           end,
    [{struct, server_resources_stats_description(ServiceNodes)} | Base].

parse_add_param("addq", Params) ->
    case proplists:get_value("addq", Params, "") =/= "" of
        true ->
            all;
        false ->
            []
    end;
parse_add_param(Param, Params) ->
    case proplists:get_value(Param, Params) of
        undefined ->
            [];
        AddIndexX ->
            case ejson:decode(AddIndexX) of
                <<"all">> ->
                    all;
                AddIndexDecoded ->
                    [binary_to_existing_atom(N, latin1) || N <- AddIndexDecoded]
            end
    end.

serve_stats_directory(_PoolId, BucketId, Req) ->
    Params = mochiweb_request:parse_qs(Req),
    ServiceNodes =
        [{Service, parse_add_param(atom_to_list(Param), Params)} ||
            {Service, Param} <- services_add_params()],

    BaseDescription = base_stats_directory(BucketId, ServiceNodes),
    Prefix = menelaus_util:concat_url_path(["pools", "default", "buckets", BucketId, "stats"]),
    Desc = [{struct, add_specific_stats_url(BD, Prefix)} || {struct, BD} <- BaseDescription],
    menelaus_util:reply_json(Req, {struct, [{blocks, Desc}]}).

add_specific_stats_url(BlockDesc, Prefix) ->
    {stats, Infos} = lists:keyfind(stats, 1, BlockDesc),
    NewInfos =
        [{struct, [{specificStatsURL, begin
                                          {name, Name} = lists:keyfind(name, 1, KV),
                                          iolist_to_binary([Prefix, $/, mochiweb_util:quote_plus(Name)])
                                      end} |
                   KV]} || {struct, KV} <- Infos],
    lists:keyreplace(stats, 1, BlockDesc, {stats, NewInfos}).

grab_ui_stats(Kind, Nodes, HaveStamp, Wnd) ->
    TS = proplists:get_value(list_to_binary(Kind), HaveStamp),
    S = grab_aggregate_op_stats(Kind, Nodes, TS, Wnd),
    samples_to_proplists(S, Kind).

%% Returns multiple blocks of stats and other things convenient for
%% analytics UI.
%%
%% This is private and thus easily evolve-able API.
%%
%% It serves all stats that we're going to display on
%% analytics. Currently that is bucket stats, portion of system stats and
%% @query stats.
%%
%% Response format is roughly as follows:
%% {
%%   directory: {url: "/pools/default/buckets/default/statsDirectory?v=40989663&addq=1"},
%%   hot_keys: [],
%%   interval: 1000,
%%   isPersistent: true,
%%   lastTStamp: {default: 1424312639799, @system: 1424312639799, @query: 1424312640799},
%%   mainStatsBlock: "default",
%%   nextReqAfter: 0,
%%   samplesCount: 60,
%%   specificStatName: null,
%%   stats: {
%%     @query: {query_avg_req_time: [0, 0], query_avg_svc_time: [0, 0], query_avg_response_size: [0, 0],…},
%%     @system: {cpu_idle_ms: [14950, 15070], cpu_local_ms: [16310, 16090],…},
%%     default: {couch_total_disk_size: [578886, 578886], couch_docs_fragmentation: [0, 0],…},
%%     "@index-default": …
%% }}
%%
%%
%% Overall focus of this API was to pull as much logic as possible
%% from .js and into server, and to be able to handle displaying
%% multiple stats on single analytics page (i.e. @system, bucket,
%% @query, @index and possibly more (like @memcached-global)).
%%
%% Biggest differences from old-style (and public) stats are:
%%
%%  * independent "delta encoding" for different stats block. Which
%%  allows us to handle things such as missing samples in some stats
%%  without having to drop matching timestamps everywhere (like we
%%  still do between @system and bucket stats)
%%
%%  * automatically version-ed directory url. Thus UI doesn't have to
%%  have logic to "watch" served stats and reload directory
%%
%%  * maximally unified format and UI logic between aggregated and
%%  "specific" stats
%%
%%  * nextReqAfter tells ui when to send request for next sample
%%
%% * UI is not expected to be "hypertext" anymore, but to simply send
%% all parameters to _uistats and receive correct response back
%% (handling aggregate and specific stats in single place). We do
%% refer to directory separately, however, for efficiency (i.e. so
%% that ui does not have to receive/parse stats directory every time it receives
%% new stats samples).
%%
serve_ui_stats(Req) ->
    Params = mochiweb_request:parse_qs(Req),
    with_valid_bucket(
      fun () ->
              case proplists:get_value("statName", Params) of
                  undefined ->
                      serve_aggregated_ui_stats(Req, Params);
                  StatName ->
                      serve_specific_ui_stats(Req, StatName, Params)
              end
      end, proplists:get_value("bucket", Params), Req).

extract_ui_stats_params(Params) ->
    Bucket = proplists:get_value("bucket", Params),
    {HaveStamp} =
        ejson:decode(list_to_binary(
                       proplists:get_value("haveTStamp", Params, "{}"))),

    {Period, Seconds, Count} = find_zoom(proplists:get_value("zoom", Params)),
    {Bucket, HaveStamp, {Seconds * Count div 60, Period, 60}}.

should_grab_service_stats(all, ServiceNodes) ->
    ServiceNodes =/= [];
should_grab_service_stats([_|_] = Nodes, ServiceNodes) ->
    lists:any(fun (Node) ->
                      lists:member(Node, Nodes)
              end, ServiceNodes).

serve_aggregated_ui_stats(Req, Params) ->
    {Bucket, HaveStamp, Wnd} = extract_ui_stats_params(Params),
    Nodes = case proplists:get_value("node", Params, all) of
                all -> all;
                XHost ->
                    case menelaus_web_node:find_node_hostname(XHost, Req) of
                        {ok, N} -> [N];
                        _ ->
                            menelaus_util:web_exception(404, "not found")
                    end
            end,
    BS = grab_ui_stats(Bucket, Nodes, HaveStamp, Wnd),
    SS = grab_ui_stats("@system", Nodes, HaveStamp, Wnd),

    GoXDCRStats = [{iolist_to_binary([<<"@xdcr-">>, Bucket]),
                    {grab_ui_stats("@xdcr-" ++ Bucket, Nodes, HaveStamp, Wnd)}}],

    ServiceNodes =
        lists:map(
          fun ({Service, Param}) ->
                  AllNodes = ns_cluster_membership:service_actual_nodes(
                                ns_config:latest(), Service),
                  {Service,
                   {Param,
                    case should_grab_service_stats(Nodes, AllNodes) of
                        true ->
                            case Service of
                                n1ql ->
                                    all;
                                _ ->
                                    Nodes
                            end;
                        false ->
                            []
                    end}}
          end, services_add_params()),

    FullStats =
        lists:foldl(
          fun (Section, AccStats) ->
                  {Service, _Bucket} = describe_section(Section),
                  case proplists:get_value(Service, ServiceNodes) of
                      {_, []} ->
                          AccStats;
                      {_, N1} ->
                          SectionStats =
                              {list_to_binary(Section),
                               {grab_ui_stats(Section, N1, HaveStamp, Wnd)}},
                          [SectionStats | AccStats]
                  end
          end, GoXDCRStats, services_sections(Bucket)),

    Stats = [{list_to_binary("@kv-" ++ Bucket), {BS}}, {<<"@system">>, {SS}} | FullStats],
    NewHaveStamp = [case proplists:get_value(timestamp, S) of
                        [] -> {Name, 0};
                        L -> {Name, lists:last(L)}
                    end || {Name, {S}} <- Stats],

    StatsDirectoryV =
        erlang:phash2(
          base_stats_directory(
            Bucket,
            [{Service, N} || {Service, {_, N}} <- ServiceNodes])),

    DirAddParams =
        lists:foldl(
          fun ({_Service, {_Param, []}}, Acc) ->
                  Acc;
              ({n1ql, {Param, all}}, Acc) ->
                  [{Param, <<"1">>} | Acc];
              ({_Service, {Param, SNodes}}, Acc) ->
                  [{Param, iolist_to_binary(ejson:encode(SNodes))} | Acc]
          end, [], ServiceNodes),

    DirQS = [{v, integer_to_list(StatsDirectoryV)} | DirAddParams],
    DirURL = "/pools/default/buckets/" ++
        menelaus_util:concat_url_path([Bucket, "statsDirectory"], DirQS),

    output_ui_stats(Req, Stats,
                    {[{url, list_to_binary(DirURL)}]},
                    Wnd, Bucket, null, NewHaveStamp).

maybe_remove_port_8091(H) ->
    case lists:reverse(binary_to_list(H)) of
        "1908:" ++ RevPref ->
            list_to_binary(lists:reverse(RevPref));
        _ ->
            H
    end.

serve_specific_ui_stats(Req, StatName, Params) ->
    {Bucket, HaveStamp, Wnd} = extract_ui_stats_params(Params),
    ClientTStamp = proplists:get_value(<<"perNode">>, HaveStamp),

    AllNodes = [{Service, all} || {Service, _Param} <- services_add_params()],
    FullDirectory = base_stats_directory(Bucket, AllNodes),
    StatNameB = list_to_binary(StatName),
    MaybeStatDesc = [Desc
                     || Block <- FullDirectory,
                        XDesc <- case Block of
                                     {struct, BlockProps} ->
                                         {_, XStats} = lists:keyfind(stats, 1, BlockProps),
                                         XStats
                                 end,
                        Desc <- case XDesc of
                                    {struct, DescProps} ->
                                        {_, DescName} = lists:keyfind(name, 1, DescProps),
                                        case DescName =:= StatNameB of
                                            true ->
                                                [DescProps];
                                            false ->
                                                []
                                        end
                                end],

    case MaybeStatDesc of
        [] ->
            menelaus_util:web_exception(404, "not found");
        [_|_] ->
            ok
    end,

    StatDescProps = hd(MaybeStatDesc),

    {NodesSamples, Nodes} =
        get_samples_for_system_or_bucket_stat(Bucket, StatName, ClientTStamp, Wnd),

    Config = ns_config:get(),
    LocalAddr = menelaus_util:local_addr(Req),
    Hostnames =
        [menelaus_web_node:build_node_hostname(Config, N, LocalAddr)
         || N <- Nodes],
    StatKeys = [<<"@", H/binary>> || H <- Hostnames],

    Timestamps = [TS || {TS, _} <- hd(NodesSamples)],
    MainValues = [VS || {_, VS} <- hd(NodesSamples)],

    AllignedRestValues
        = lists:map(fun (undefined) -> [undefined || _ <- Timestamps];
                        (Samples) ->
                            Dict = orddict:from_list(Samples),
                            [dict_safe_fetch(T, Dict, 0) || T <- Timestamps]
                    end, tl(NodesSamples)),

    Stats = lists:zipwith(fun (H, VS) ->
                                  {H, VS}
                          end,
                          StatKeys, [MainValues | AllignedRestValues]),
    LastTStamp = case Timestamps of
                     [] -> 0;
                     L -> lists:last(L)
                 end,

    RestStatDescProps =
        [{K, V} || {K, V} <- StatDescProps,
                   K =/= name andalso K =/= title],

    StatInfos = [{[{title, maybe_remove_port_8091(H)}, {name, <<"@", H/binary>>}
                   | RestStatDescProps]} || H <- Hostnames],

    ServeDirectory = {[{value, {[{thisISSpecificStats, true},
                                 {blocks, [{[{blockName, <<"Specific Stats">>},
                                             {hideThis, true},
                                             {stats, StatInfos}]}]}]}},
                       {origTitle, misc:expect_prop_value(title, StatDescProps)},
                       {url, null}]},

    FullStats = [{timestamp, Timestamps} | Stats],

    output_ui_stats(Req,
                    [{perNode, {FullStats}}],
                    ServeDirectory,
                    Wnd, Bucket, list_to_binary(StatName),
                    [{perNode, LastTStamp}]).


output_ui_stats(Req, Stats, Directory, Wnd, Bucket, StatName, NewHaveStamp) ->
    Step = element(1, Wnd),
    J = [{stats, {Stats}},
         {directory, Directory},
         {samplesCount, 60},
         {interval, Step * 1000},
         {isPersistent, is_persistent(Bucket)},
         {nextReqAfter, case Step of
                            1 -> 0;
                            _ -> 30000
                        end},
         {mainStatsBlock, element(1, hd(Stats))},
         {specificStatName, StatName},
         {lastTStamp, {NewHaveStamp}}],
    menelaus_util:reply_json(Req, {J}).

get_indexes(Service, BucketId) ->
    simple_memoize(
      {indexes, Service:get_type(), BucketId},
      fun () ->
              Nodes =
                  section_nodes(service_stats_collector:service_prefix(Service)
                                ++ BucketId),
              do_get_indexes(Service, BucketId, Nodes)
      end, 5000).

do_get_indexes(Service, BucketId0, Nodes) ->
    WantedHosts0 =
        lists:flatmap(
          fun (N) ->
                  {_, Host} = misc:node_name_host(N),
                  Port = service_ports:get_port(rest_port, ns_config:latest(),
                                                N),
                  SslPort = service_ports:get_port(ssl_rest_port,
                                                   ns_config:latest(), N),
                  [list_to_binary(misc:join_host_port(Host, Port)),
                   list_to_binary(misc:join_host_port(Host, SslPort))]
          end, Nodes),
    WantedHosts = lists:usort(WantedHosts0),

    BucketId = list_to_binary(BucketId0),
    {ok, Indexes, _Stale, _Version} = service_status_keeper:get_items(Service),
    [begin
         {index, Name} = lists:keyfind(index, 1, I),
         Name
     end || I <- Indexes,
            proplists:get_value(bucket, I) =:= BucketId,
            not(ordsets:is_disjoint(WantedHosts,
                                    lists:usort(proplists:get_value(hosts, I))))].

-record(params, {bucket, start_ts, end_ts, step, nodes, aggregate}).

filter_samples(Samples, StartTS, EndTS) ->
    S1 = lists:dropwhile(fun (#stat_entry{timestamp = T}) -> T < StartTS end,
                         Samples),
    lists:takewhile(fun (#stat_entry{timestamp = T}) -> T < EndTS end, S1).

prepare_samples(Samples, StartTS, EndTS, Extractor) ->
    Filtered = filter_samples(Samples, StartTS, EndTS),
    calculate_stats(Extractor, remove_undefineds, Filtered).

merge_samples(Samples, undefined, StartTS, EndTS, Extractor) ->
    [prepare_samples(S, StartTS, EndTS, Extractor) || S <- Samples];
merge_samples(Samples, AccSamples, StartTS, EndTS, Extractor) ->
    [do_merge_samples(S, A, StartTS, EndTS, Extractor) ||
        {S, A} <- lists:zip(Samples, AccSamples)].

do_merge_samples(Samples, [], StartTS, EndTS, Extractor) ->
    prepare_samples(Samples, StartTS, EndTS, Extractor);
do_merge_samples(Samples, [{EndTS, _} | _] = AccSamples, StartTS, _, Extractor) ->
    prepare_samples(Samples, StartTS, EndTS, Extractor) ++ AccSamples.

prepare_aggregated_samples(Samples, StartTS, EndTS, Extractor) ->
    [FirstNodeSamples | Rest] =
        [filter_samples(S, StartTS, EndTS) || S <- Samples],
    Aggregated = merge_all_samples_normally(FirstNodeSamples, Rest),
    calculate_stats(Extractor, remove_undefineds, Aggregated).

aggregate_and_merge(Samples, [[{EndTS, _} | _]] = [AccSamples], StartTS, _,
                    Extractor) ->
    [prepare_aggregated_samples(Samples, StartTS, EndTS, Extractor) ++
         AccSamples];
aggregate_and_merge(Samples, _, StartTS, EndTS, Extractor) ->
    [prepare_aggregated_samples(Samples, StartTS, EndTS, Extractor)].

latest_start_timestamp(Samples, StartTS) ->
    lists:foldl(
      fun ([#stat_entry{timestamp = T} | _], LatestT) when T > LatestT ->
              T;
          (_, LatestT) ->
              LatestT
      end, StartTS, Samples).

archives(#params{step = Step}) ->
    %% skip more detailed archive if the next one is sufficient
    %% for the given step
    Archives = stats_archiver:archives(),
    [_ | Archives1] = Archives,
    ArchivesZipped =
        lists:dropwhile(
          fun ({_, {_, NextSeconds, _}}) ->
                  NextSeconds =< Step;
              ({_, undefined}) ->
                  false
          end, lists:zip(Archives, Archives1 ++ [undefined])),
    [A || {A, _} <- ArchivesZipped].

retrieve_samples_from_all_archives(Params, Stat) ->
    {S, N, _, _} =
        lists:foldl(?cut(retrieve_samples_from_archive(_1, Stat, Params, _2)),
                    {undefined, undefined, undefined, true}, archives(Params)),
    {S, N}.

retrieve_samples_from_archive(_Archive, _Stat, _Params,
                              {_AccSamples, _AccNodes, _Kind, false} = Acc) ->
    Acc;
retrieve_samples_from_archive(Archive, Stat,
                              Params = #params{start_ts = StartTS,
                                               end_ts = EndTS,
                                               aggregate = Aggregate},
                              {AccSamples, AccNodes, Kind, Continue}) ->
    case do_retrieve_samples_from_archive(Archive, Stat, Params, Kind) of
        #gathered_stats{samples = [[]]} ->
            %% no results for this stat in current archive
            %% no need to proceed to less detailed archives
            {AccSamples, AccNodes, Kind, false};
        #gathered_stats{nodes = Nodes, kind = NewKind, extractor = Extractor,
                        samples = Samples} ->
            if
                (AccNodes =:= undefined) orelse (Nodes =:= AccNodes) ->
                    NewContinue =
                        LatestStart = latest_start_timestamp(Samples, StartTS),
                        case LatestStart - StartTS > 1000 of
                            true ->
                                Continue;
                            false ->
                                %% we got all the samples we wanted, time to
                                %% stop retrieveing
                                false
                        end,

                    MergedSamples =
                        case Aggregate of
                            true ->
                                aggregate_and_merge(Samples, AccSamples,
                                                    StartTS, EndTS, Extractor);
                            false ->
                                merge_samples(Samples, AccSamples, StartTS,
                                              EndTS, Extractor)
                        end,
                    {MergedSamples, Nodes, NewKind, NewContinue};

                true -> %% Main node has changed. It means it doesn't have any
                        %% samples for this archive, which means we can stop
                        %% and ignore the last result
                    {AccSamples, AccNodes, Kind, false}
            end

    end.

do_retrieve_samples_from_archive({Period, Seconds, Count}, StatName,
                                #params{bucket = BucketName,
                                        start_ts = StartTS,
                                        step = Step,
                                        nodes = Nodes}, Kind) ->
    Wnd = case Step of
              1 ->
                  {1, Period, Count};
              _ ->
                  {1, Period, Seconds * Count div Step}
          end,

    case Kind of
        undefined ->
            SearchList = get_stats_search_order(StatName, BucketName),
            get_samples_from_one_of_kind(SearchList, StatName,
                                         StartTS, Wnd, Nodes);
        _ ->
            ForNodes = nodes_to_try(Kind, Nodes),
            get_samples_for_stat(Kind, StatName, ForNodes, StartTS, Wnd)
    end.

-define(MAX_TS, 9999999999999).
-define(MIN_TS, -?MAX_TS).

handle_ui_stats_post(Req) ->
    Permission = stats_read_permission(any),
    menelaus_util:require_permission(Req, Permission),
    validator:handle(
      fun (List) ->
              LocalAddr = menelaus_util:local_addr(Req),
              menelaus_util:reply_json(
                Req, [handle_ui_stats_post_section(LocalAddr, V) ||
                         V <- List])
      end, Req, json_array, ui_stats_post_validators(Req)).

jsonify_node(N, LocalAddr) ->
    menelaus_web_node:build_node_hostname(ns_config:latest(), N, LocalAddr).

build_one_stat_json([{{aggregate, Nodes}, Samples}], LocalAddr) ->
    {[{aggregate, Samples},
      {aggregateNodes, [jsonify_node(N, LocalAddr) || N <- Nodes]}]};
build_one_stat_json(SamplesForNodes, LocalAddr) ->
    {[{jsonify_node(N, LocalAddr), Samples} ||
         {N, Samples} <- SamplesForNodes]}.

handle_ui_stats_post_section(LocalAddr, Values) ->
    Bucket = proplists:get_value(bucket, Values),
    StatNames = proplists:get_value(stats, Values),
    Nodes = proplists:get_value(nodes, Values, all),
    Step = proplists:get_value(step, Values, 1),
    Aggregate = proplists:get_value(aggregate, Values, false),

    Params = #params{
                bucket = Bucket,
                start_ts = proplists:get_value(startTS, Values, 0),
                end_ts = proplists:get_value(endTS, Values, ?MAX_TS),
                step = Step,
                nodes = Nodes,
                aggregate = Aggregate},

    SamplesForAllStats =
        lists:filtermap(
          fun (Stat) ->
                  case retrieve_samples_from_all_archives(Params, Stat) of
                      {undefined, undefined} ->
                          false;
                      {[[]], _} ->
                          false;
                      {S, N} ->
                          {true, {Stat, case Aggregate of
                                            true ->
                                                {S, [{aggregate, N}]};
                                            false ->
                                                {S, N}
                                        end}}
                  end
          end, StatNames),

    {Timestamps, PreparedStats} = prepare_ui_stats(SamplesForAllStats),

    StatsJson =
        {[{list_to_binary(StatName), build_one_stat_json(Samples, LocalAddr)} ||
             {StatName, Samples} <- PreparedStats]},

    {[{timestamps, Timestamps},
      {step, Step},
      {stats, StatsJson}] ++
         [{bucket, list_to_binary(Bucket)} || Bucket =/= undefined]}.

build_timestamps(SamplesForStatsAndNodes) ->
    lists:umerge(
      lists:append(
        lists:map(
          fun ({_, {SamplesForNodes, _}}) ->
                  lists:map([TS || {TS, _} <- _], SamplesForNodes)
          end, SamplesForStatsAndNodes))).

normalize_samples(_, [], Acc) ->
    lists:reverse(Acc);
normalize_samples([{TS, V} | RestV], [TS | RestTS], Acc) ->
    normalize_samples(RestV, RestTS, [V | Acc]);
normalize_samples(Values, [_ | RestTS], Acc) ->
    normalize_samples(Values, RestTS, [null | Acc]).

prepare_ui_stats(SamplesForStatsAndNodes) ->
    Timestamps = build_timestamps(SamplesForStatsAndNodes),
    {Timestamps,
     lists:map(
       fun ({StatName, {SamplesForNodes, Nodes}}) ->
               {StatName,
                lists:zip(Nodes,
                          [normalize_samples(Samples, Timestamps, []) ||
                              Samples <- SamplesForNodes])}
       end, SamplesForStatsAndNodes)}.

ui_stats_post_validators(Req) ->
    Now = os:system_time(millisecond),
    [validator:string(bucket, _),
     validate_bucket(bucket, Req, _),
     validator:required(stats, _),
     validator:string_array(stats, _),
     validator:string(statName, _),
     validator:boolean(aggregate, _),
     validator:integer(startTS, ?MIN_TS, ?MAX_TS, _),
     validator:integer(endTS, ?MIN_TS, ?MAX_TS, _),
     validate_negative_ts(startTS, Now, _),
     validate_negative_ts(endTS, Now, _),
     validator:validate_relative(
       fun (StartTS, EndTS) when StartTS > EndTS ->
               {error,
                io_lib:format("should not be greater than ~p", [EndTS]) ++
                    case EndTS - Now of
                        N when N < 0 ->
                            io_lib:format(" or between ~p and 0", [N]);
                        _ ->
                            []
                    end};
           (_, _) ->
               ok
       end, startTS, endTS, _),
     validator:integer(step, 1, 60 * 60 * 24 * 366, _),
     validator:string_array(nodes, _),
     validate_nodes(nodes, _, Req),
     validator:unsupported(_)].

validate_negative_ts(Name, Now, State) ->
    validator:validate(
      fun (TS) when TS < 0 ->
              {value, Now + TS};
          (_) ->
              ok
      end, Name, State).

validate_bucket(Name, Req, State) ->
    validator:validate(
      fun (BucketName) ->
              case check_bucket(BucketName, Req) of
                  ok ->
                      ok;
                  not_found ->
                      {error, "Bucket not found"};
                  {forbidden, Permission} ->
                      ns_audit:auth_failure(Req),
                      {error, {403, Permission}}
              end
      end, Name, State).

validate_nodes(Name, State, Req) ->
    validator:validate(
      fun (Nodes) ->
              {Right, Wrong} =
                  misc:partitionmap(
                    fun (HostName) ->
                            case menelaus_web_node:find_node_hostname(
                                   HostName, Req) of
                                {error, _} ->
                                    {right, HostName};
                                {ok, Node} ->
                                    {left, Node}
                            end
                    end, Nodes),
              case Wrong of
                  [] ->
                      {value, Right};
                  _ ->
                      {error, io_lib:format("Unknown hostnames: ~p", [Wrong])}
              end
      end, Name, State).

-ifdef(TEST).
guess_sections_by_prefix_test() ->
    ?assertEqual(["@query"], guess_sections_by_prefix("query_blah", "test")),
    ?assertEqual(["@xdcr-test"], guess_sections_by_prefix("replicationblah",
                                                          "test")),
    ?assertEqual([], guess_sections_by_prefix("replicationblah", undefined)),
    lists:foreach(
      fun (StatName) ->
              ?assertEqual(["test"],
                           guess_sections_by_prefix(StatName, "test")),
              ?assertEqual([], guess_sections_by_prefix(StatName, undefined))
      end, ["viewsblah", "spatialblah", "vb_blah", "ep_blah"]),
    lists:foreach(
      fun ("@" ++ Prefix = Section) ->
              Stat = Prefix ++ "/blah/blah",
              ?assertEqual([Section, Section ++ "-test"],
                           guess_sections_by_prefix(Stat, "test")),
              ?assertEqual([Section],
                           guess_sections_by_prefix(Stat, undefined))
      end, services_sections(undefined) -- ["@query"]),
    ?assertEqual([], guess_sections_by_prefix("blah", "test")),
    ?assertEqual([], guess_sections_by_prefix("blah", undefined)).

join_samples_test() ->
    A = [
         {stat_entry, 1, [{key1, 1},
                          {key2, 2}]},
         {stat_entry, 2, [{key1, 3},
                          {key2, 4}]},
         {stat_entry, 3, [{key1, 5},
                          {key2, 6}]}],
    B = [
         {stat_entry, 2, [{key3, 1},
                          {key4, 2}]},
         {stat_entry, 3, [{key3, 3},
                          {key4, 4}]},
         {stat_entry, 4, [{key3, 5},
                          {key4, 6}]}],

    R1 = [
          {stat_entry, 2, [{key1, 3},
                           {key2, 4},
                           {key3, 1},
                           {key4, 2}]},
          {stat_entry, 3, [{key1, 5},
                           {key2, 6},
                           {key3, 3},
                           {key4, 4}]}],

    R2 = [
          {stat_entry, 2, [{key3, 1},
                           {key4, 2},
                           {key1, 3},
                           {key2, 4}]},
          {stat_entry, 3, [{key3, 3},
                           {key4, 4},
                           {key1, 5},
                           {key2, 6}]}],

    ?assertEqual(R1, join_samples(A, B, 2)),
    ?assertEqual(R2, join_samples(B, A, 2)),
    ?assertEqual(tl(R2), join_samples(B, A, 1)).

aggregate_stat_kv_pairs_test() ->
    ?assertEqual([{a, 3}, {b, undefined}, {c, 1}, {d,1}, {e, 1}],
                 aggregate_stat_kv_pairs([{a, 1}, {b, undefined}, {c,1}, {d, 1}],
                                         [{a, 2}, {b, undefined}, {d, undefined}, {e,1}],
                                         [])),
    ?assertEqual([{a, 3}, {b, undefined}, {ba, 123}, {c, 1}, {d,1}],
                 aggregate_stat_kv_pairs([{a, 1}, {b, undefined}, {c,1}, {d, 1}],
                                         [{a, 2}, {b, undefined}, {ba, 123}],
                                         [])),
    ?assertEqual([{a, 3}, {b, undefined}, {c, 1}, {d,1}, {e, 1}],
                 aggregate_stat_kv_pairs([{a, 1}, {b, undefined}, {c,1}, {d, 1}],
                                         [{a, 2}, {c,0}, {d, undefined}, {e,1}],
                                         [])),
    ?assertEqual([{couch_views_ops, 3},
                  {<<"views/A1/accesses">>, 4},
                  {<<"views/A1/blah">>, 3}],
                 aggregate_stat_kv_pairs([{couch_views_ops, 1},
                                          {<<"views/A1/accesses">>, 4},
                                          {<<"views/A1/blah">>, 1}],
                                         [{couch_views_ops, 3},
                                          {<<"views/A1/accesses">>, 2},
                                          {<<"views/A1/blah">>, 2}],
                                         [])).

prepare_ui_stats_test() ->
    ?assertEqual(
       {[0, 1, 2, 3, 4, 5, 6],
        [{stat1, [{n1, [null, a1, a2, null, a4, a5, null]},
                  {n2, [null, null, b2, b3, null, null, null]}]},
         {stat2, [{n1, [c0, null, c2, null, c4, null, c6]}]}]},
       prepare_ui_stats(
         [{stat1, {[[{1, a1}, {2, a2}, {4, a4}, {5, a5}], [{2, b2}, {3, b3}]],
                   [n1, n2]}},
          {stat2, {[[{0, c0}, {2, c2}, {4, c4}, {6, c6}]], [n1]}}])).

-endif.
