%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc this module contains memory quotas related code
%%
-module(memory_quota).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([this_node_memory_data/0,
         get_max_node_ram_quota/1,
         check_quotas/4,
         check_this_node_quotas/2,
         get_quota/1,
         get_quota/2,
         set_quotas/2,
         default_quotas/2,
         service_to_json_name/1,
         aware_services/0,
         aware_services/1,
         choose_limit/3]).

this_node_memory_data() ->
    case os:getenv("MEMBASE_RAM_MEGS") of
        false ->
            memory_data();
        X ->
            RAMBytes = list_to_integer(X) * ?MIB,
            {RAMBytes, 0, 0}
    end.

cgroup_memory_data() ->
    CGroupsInfo = sigar:get_cgroups_info(),
    MMax = case CGroupsInfo of
               #{<<"supported">> := true, <<"memory_max">> := MemMax}
                 when is_number(MemMax) ->
                   MemMax;
               _ -> undefined
           end,
    MCurr = case CGroupsInfo of
                #{<<"supported">> := true, <<"memory_current">> := MemCurrent}
                  when is_number(MemCurrent) ->
                    MemCurrent;
                _ -> undefined
            end,
    {MMax, MCurr}.

choose_limit(Limit, Usage, {undefined, _}) -> {Limit, Usage};
choose_limit(Limit, Usage, {_, undefined}) -> {Limit, Usage};
choose_limit(Limit, Usage, {0, _}) -> {Limit, Usage};
choose_limit(Limit, Usage, {CGroupLimit, _}) when Limit < CGroupLimit ->
    {Limit, Usage};
choose_limit(_, _, CGroupMemData) -> CGroupMemData.

memory_data() ->
    ns_bootstrap:ensure_os_mon(),
    {Total, Used, ProcInfo} = memsup:get_memory_data(),
    {TotalMemory, TotalUsed} = choose_limit(Total, Used,
                                            cgroup_memory_data()),
    {TotalMemory, TotalUsed, ProcInfo}.

add_quota_for_servers(Quota, Servers, NodesMap, Pos) ->
    lists:foldl(
      fun (S, NM) ->
              maps:update_with(
                S, ?cut(setelement(Pos, _1, element(Pos, _1) + Quota)), NM)
      end, NodesMap, Servers).

get_max_node_ram_quota(Snapshot) ->
    case bucket_placer:is_enabled() of
        false ->
            lists:foldl(
              fun ({_, BucketConfig}, RAMQuota) ->
                      ns_bucket:raw_ram_quota(BucketConfig) + RAMQuota
              end, 0, ns_bucket:get_buckets(Snapshot));
        true ->
            Nodes = ns_cluster_membership:nodes_wanted(Snapshot),
            NodesWithQuotas =
                lists:foldl(
                  fun ({_, BucketConfig}, NodesMap) ->
                          Quota = ns_bucket:raw_ram_quota(BucketConfig),
                          Servers = ns_bucket:get_servers(BucketConfig),
                          DesiredServers =
                              case ns_bucket:get_desired_servers(
                                     BucketConfig) of
                                  undefined -> [];
                                  DS -> DS
                              end,
                          functools:chain(
                            NodesMap,
                            [add_quota_for_servers(Quota, Servers, _, 1),
                             add_quota_for_servers(Quota, DesiredServers, _,
                                                   2)])
                  end, maps:from_keys(Nodes, {0, 0}),
                  ns_bucket:get_buckets(Snapshot)),
            lists:max(
              [max(X, Y) || {_, {X, Y}} <- maps:to_list(NodesWithQuotas)])
    end.

allowed_memory_usage_max(MemSupData) ->
    {MaxMemoryBytes0, _, _} = MemSupData,
    MinusMegs = ?MIN_FREE_RAM,

    MaxMemoryMBPercent = (MaxMemoryBytes0 * ?MIN_FREE_RAM_PERCENT) div (100 * ?MIB),
    MaxMemoryMB = lists:max([(MaxMemoryBytes0 div ?MIB) - MinusMegs, MaxMemoryMBPercent]),
    MaxMemoryMB.

-type quota_result() :: ok | {error, quota_error()}.
-type quota_error() ::
        {total_quota_too_high, node(), Value :: integer(), MaxAllowed :: pos_integer()} |
        {service_quota_too_low, service(), Value :: integer(), MinRequired :: pos_integer()}.
-type quotas() :: [{service(), integer()}].

-spec check_quotas([NodeInfo], ns_config(), map(), quotas()) ->
                          quota_result() when
      NodeInfo :: {node(), [service()], MemoryData :: term()}.
check_quotas(NodeInfos, Config, Snapshot, UpdatedQuotas) ->
    case check_service_quotas(UpdatedQuotas, Snapshot) of
        ok ->
            AllQuotas = get_all_quotas(Config, UpdatedQuotas),
            check_quotas_loop(NodeInfos, AllQuotas);
        Error ->
            Error
    end.

service_to_json_name(kv) ->
    memoryQuota;
service_to_json_name(index) ->
    indexMemoryQuota;
service_to_json_name(fts) ->
    ftsMemoryQuota;
service_to_json_name(cbas) ->
    cbasMemoryQuota;
service_to_json_name(n1ql) ->
    queryMemoryQuota;
service_to_json_name(eventing) ->
    eventingMemoryQuota.

services_ranking(Vsn) ->
    [kv, cbas, index, fts, eventing]
        ++
        case cluster_compat_mode:is_version_76(Vsn) of
            true ->
                [n1ql];
            false ->
                []
        end.

number_services(Vsn) ->
    length(services_ranking(Vsn)).

aware_services() ->
    aware_services(ns_config:latest()).

aware_services(Config) ->
    aware_services(
      cluster_compat_mode:get_compat_version(),
      cluster_compat_mode:is_enterprise(Config)).

aware_services(CompatVersion, IsEnterprise) ->
    [S || S <- ns_cluster_membership:supported_services_for_version(
                 CompatVersion, IsEnterprise),
          lists:member(S, services_ranking(CompatVersion))].

get_all_quotas(Config, UpdatedQuotas) ->
    Services = aware_services(Config),
    lists:map(
      fun (Service) ->
              Value =
                  case lists:keyfind(Service, 1, UpdatedQuotas) of
                      false ->
                          {ok, V} = get_quota(Config, Service),
                          V;
                      {_, V} ->
                          V
                  end,
              {Service, Value}
      end, Services).

check_quotas_loop([], _) ->
    ok;
check_quotas_loop([{Node, Services, MemoryData} | Rest], AllQuotas) ->
    TotalQuota = lists:sum([Q || {S, Q} <- AllQuotas, lists:member(S, Services)]),
    case check_node_total_quota(Node, TotalQuota, MemoryData) of
        ok ->
            check_quotas_loop(Rest, AllQuotas);
        Error ->
            Error
    end.

check_node_total_quota(Node, TotalQuota, MemoryData) ->
    Max = allowed_memory_usage_max(MemoryData),
    case TotalQuota =< Max of
        true ->
            ok;
        false ->
            {error, {total_quota_too_high, Node, TotalQuota, Max}}
    end.

check_service_quotas([], _) ->
    ok;
check_service_quotas([{Service, Quota} | Rest], Snapshot) ->
    case check_service_quota(Service, Quota, Snapshot) of
        ok ->
            check_service_quotas(Rest, Snapshot);
        Error ->
            Error
    end.

-define(MAX_DEFAULT_FTS_QUOTA, 512).

min_quota(kv) ->
    256;
min_quota(index) ->
    256;
min_quota(fts) ->
    256;
min_quota(cbas) ->
    1024;
min_quota(n1ql) ->
    0;
min_quota(eventing) ->
    256.

check_service_quota(kv, Quota, Snapshot) ->
    BucketsQuota = get_max_node_ram_quota(Snapshot) div ?MIB,
    MinMemoryMB = erlang:max(min_quota(kv), BucketsQuota),
    check_min_quota(kv, MinMemoryMB, Quota);
check_service_quota(Service, Quota, _) ->
    check_min_quota(Service, min_quota(Service), Quota).

check_min_quota(_Service, MinQuota, Quota) when Quota >= MinQuota ->
    ok;
check_min_quota(Service, MinQuota, Quota) ->
    {error, {service_quota_too_low, Service, Quota, MinQuota}}.

%% check that the node has enough memory for the quotas; note that we do not
%% validate service quota values because we expect them to be validated by the
%% calling side
-spec check_this_node_quotas([service()], quotas()) -> quota_result().
check_this_node_quotas(Services, Quotas0) ->
    Quotas = [{S, Q} || {S, Q} <- Quotas0, lists:member(S, Services)],
    MemoryData = this_node_memory_data(),
    TotalQuota = lists:sum([Q || {_, Q} <- Quotas]),

    check_node_total_quota(node(), TotalQuota, MemoryData).

service_to_store_method(kv) ->
    {key, memory_quota};
service_to_store_method(index) ->
    {manager, index_settings_manager};
service_to_store_method(fts) ->
    {key, fts_memory_quota};
service_to_store_method(cbas) ->
    {key, cbas_memory_quota};
service_to_store_method(eventing) ->
    {manager, eventing_settings_manager};
service_to_store_method(n1ql) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            {manager, query_settings_manager};
        false ->
            {not_yet_supported, ?VERSION_76}
    end.

get_quota(Service) ->
    get_quota(ns_config:latest(), Service).

get_quota(Config, Service) ->
    case service_to_store_method(Service) of
        {key, Key}->
            case ns_config:search(Config, Key) of
                {value, Quota} ->
                    {ok, Quota};
                false ->
                    not_found
            end;
        {manager, Manager} ->
            NotFound = make_ref(),
            case Manager:get_from_config(Config, memoryQuota, NotFound) of
                NotFound ->
                    not_found;
                Quota ->
                    {ok, Quota}
            end;
        {not_yet_supported, RequiredVsn} ->
            ?log_warning("Cannot get/set ~p memoryQuota before entire cluster"
                         " is upgraded to ~p.", [Service, RequiredVsn]),
            not_found
    end.

set_quotas(Config, Quotas) ->
    RV = ns_config:run_txn_with_config(
           Config,
           fun (Cfg, SetFn) ->
                   NewCfg =
                       lists:foldl(
                         fun ({Service, Quota}, Acc) ->
                                 do_set_memory_quota(Service, Quota, Acc, SetFn)
                         end, Cfg, Quotas),
                   {commit, NewCfg}
           end),

    case RV of
        {commit, _} ->
            ok;
        retry_needed ->
            retry_needed
    end.

do_set_memory_quota(Service, Quota, Cfg, SetFn) ->
    case service_to_store_method(Service) of
        {key, Key}->
            SetFn(Key, Quota, Cfg);
        {manager, Manager} ->
            Txn = Manager:update_txn([{memoryQuota, Quota}]),
            {commit, NewCfg, _} = Txn(Cfg, SetFn),
            NewCfg;
        {not_yet_supported, _} ->
            Cfg
    end.

remaining_default_quota(kv, Memory, NumServices) ->
    (Memory * 3) div NumServices;
remaining_default_quota(index, Memory, NumServices) ->
    (Memory * 3) div NumServices;
remaining_default_quota(n1ql, _Memory, _NumServices) ->
    %% This needs to always return 0 as the set remaining default quota because
    %% when the user initializes a cluster with setDefaultQuotas this function
    %% will get called it must be equal to the min_quota/1 value for consistency
    %% as well as the queryNodeQuota which the queryMemoryQuota shadows.
    ?QUERY_NODE_QUOTA_DEFAULT;
remaining_default_quota(fts, Memory, NumServices) ->
    min(Memory div NumServices, ?MAX_DEFAULT_FTS_QUOTA - min_quota(fts));
remaining_default_quota(cbas, Memory, NumServices) ->
    Memory div NumServices;
remaining_default_quota(eventing, Memory, NumServices) ->
    Memory div NumServices.

calculate_remaining_default_quota(Service, Memory, Vsn) ->
    remaining_default_quota(Service, Memory, number_services(Vsn)).

default_quotas(Services, Vsn) ->
    %% this is actually bogus, because nodes can be heterogeneous; but that's
    %% best we can do
    MemSupData = this_node_memory_data(),
    default_quotas(Services, MemSupData, Vsn).

default_quotas(Services, MemSupData, Vsn) ->
    {MemoryBytes, _, _} = MemSupData,
    Memory = MemoryBytes div ?MIB,
    MemoryMax = allowed_memory_usage_max(MemSupData),

    OrderedServices = [S || S <- services_ranking(Vsn),
                            lists:member(S, Services)],
    MinQuotas = [min_quota(S) || S <- OrderedServices],
    MinTotal = lists:sum(MinQuotas),
    MinQuotasServices = lists:zip(OrderedServices, MinQuotas),

    case MinTotal > MemoryMax of
        true ->
            %% we do not officialy support machines with that little memory
            %% but some people (MB-29290) insist that one still should be able
            %% to configure services manually on such machines. so we just
            %% return minimum quotas here, despite the sum of them being
            %% larger than available memory
            MinQuotasServices;
        false ->
            calculate_remaining_default_quotas(Memory, MemoryMax, MinTotal,
                                               MinQuotasServices, Vsn)
    end.

calculate_remaining_default_quotas(Memory, MemoryMax,
                                   MinTotal, MinQuotas, Vsn) ->
    {_, _, Result} =
        lists:foldl(
          fun ({Service, MinQ}, {AccMem, AccMax, AccResult}) ->
                  Quota =
                      case calculate_remaining_default_quota(Service,
                                                             AccMem, Vsn) of
                          Q when Q > AccMax ->
                              AccMax;
                          Q ->
                              Q
                      end,
                  AccMem1 = AccMem - Quota,
                  AccMax1 = AccMax - Quota,
                  AccResult1 = [{Service, Quota + MinQ} | AccResult],

                  {AccMem1, AccMax1, AccResult1}
          end,
          {Memory - MinTotal, MemoryMax - MinTotal, []}, MinQuotas),
    Result.


-ifdef(TEST).
default_quotas_test() ->
    MemSupData = {9822564352, undefined, undefined},
    Services = services_ranking(?LATEST_VERSION_NUM),
    Quotas = default_quotas(Services, MemSupData, ?LATEST_VERSION_NUM),
    TotalQuota = lists:sum([Q || {_, Q} <- Quotas]),
    ?assertEqual(true, allowed_memory_usage_max(MemSupData) >= TotalQuota).

%% Ensure our calculations are equal on 7.2, 7.6, and LATEST_VERSION_NUM.
default_quotas_by_version_test() ->
    MemSupData = {9822564352, undefined, undefined},
    Pre76Services = [kv, cbas, index, fts, eventing],
    %% 7.2 quotas (pre-n1ql introduction)
    Services72 = services_ranking(?VERSION_72),
    ?assertEqual(Pre76Services, Services72),
    TotalQuota72 =
        lists:sum([Q || {_, Q} <- default_quotas(Services72,
                                                 MemSupData, ?VERSION_72)]),
    ?assertEqual(true, allowed_memory_usage_max(MemSupData) >= TotalQuota72),
    %% 7.6 quotas
    SevenSixServices = [kv, cbas, index, fts, eventing, n1ql],
    Services76 = services_ranking(?VERSION_76),
    ?assertEqual(SevenSixServices, Services76),
    TotalQuota76 =
        lists:sum([Q || {_, Q} <-
                            default_quotas(Services76,
                                           MemSupData, ?VERSION_76)]),
    ?assertEqual(true,
                 allowed_memory_usage_max(MemSupData) >= TotalQuota76),

    %% 'latest_version_num' quotas (smoke test for new versions)
    ServicesLatestVsn = services_ranking(?LATEST_VERSION_NUM),
    ?assertEqual(SevenSixServices, ServicesLatestVsn),
    TotalQuotaLatestVsn =
        lists:sum([Q || {_, Q} <-
                            default_quotas(ServicesLatestVsn,
                                           MemSupData, ?LATEST_VERSION_NUM)]),
    ?assertEqual(true,
                 allowed_memory_usage_max(MemSupData) >= TotalQuotaLatestVsn),
    ?assertEqual(TotalQuota76, TotalQuotaLatestVsn).

-endif.
