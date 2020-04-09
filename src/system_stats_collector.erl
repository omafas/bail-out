%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-2019 Couchbase, Inc.
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
%% @doc grabs system-level stats portsigar
%%
-module(system_stats_collector).

-include("ns_common.hrl").
-include("ns_stats.hrl").

-define(ETS_LOG_INTVL, 180).

%% API
-export([start_link/0]).

%% callbacks
-export([init/1, grab_stats/1, process_stats/5]).

-export([increment_counter/1, increment_counter/2,
         get_ns_server_stats/0, set_counter/2,
         add_histo/2,
         cleanup_stale_epoch_histos/0, log_system_stats/1,
         stale_histo_epoch_cleaner/0]).

-type os_pid() :: integer().

-record(state, {
          port      :: port() | undefined,
          pid_names :: [{os_pid(), binary()}]
         }).

start_link() ->
    base_stats_collector:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ets:new(ns_server_system_stats, [public, named_table, set]),
    increment_counter({request_leaves, rest}, 0),
    increment_counter({request_enters, hibernate}, 0),
    increment_counter({request_leaves, hibernate}, 0),
    increment_counter(prev_request_leaves_rest, 0),
    increment_counter(prev_request_leaves_hibernate, 0),
    increment_counter(log_counter, 0),
    increment_counter(odp_report_failed, 0),
    _ = spawn_link(fun stale_histo_epoch_cleaner/0),
    Path = path_config:component_path(bin, "sigar_port"),

    BabysitterPid = ns_server:get_babysitter_pid(),
    Port = open_port({spawn_executable, Path},
                     [stream, use_stdio, exit_status,
                      binary, eof,
                      {arg0, lists:flatten(
                               io_lib:format("portsigar for ~s", [node()]))},
                      {args, [integer_to_list(BabysitterPid)]}]),
    spawn_ale_stats_collector(),

    State = #state{port = Port,
                   pid_names = grab_pid_names()},

    {ok, State}.

recv_data(Port) ->
    recv_data_loop(Port, <<"">>).

recv_data_loop(Port, <<Version:32/native,
                       StructSize:32/native, _/binary>> = Acc)
  when Version =:= 5 ->
    recv_data_with_length(Port, Acc, StructSize - erlang:size(Acc));
recv_data_loop(_, <<Version:32/native, _/binary>>) ->
    error({unsupported_portsigar_version, Version});
recv_data_loop(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            recv_data_loop(Port, <<Data/binary, Acc/binary>>)
    end.

recv_data_with_length(_Port, Acc, _WantedLength = 0) ->
    erlang:iolist_to_binary(Acc);
recv_data_with_length(Port, Acc, WantedLength) ->
    receive
        {Port, {data, Data}} ->
            Size = size(Data),
            if
                Size =< WantedLength ->
                    recv_data_with_length(Port, [Acc | Data], WantedLength - Size);
                Size > WantedLength ->
                    erlang:error({too_big_recv, Size, WantedLength, Data, Acc})
            end
    end.

unpack_data({Bin, LocalStats}, PrevSample, State) ->
    <<_Version:32/native,
      StructSize:32/native,
      CPUTotalMS:64/native,
      CPUIdleMS:64/native,
      CPUUserMS:64/native,
      CPUSysMS:64/native,
      CPUIrqMS:64/native,
      CPUStolenMS:64/native,
      SwapTotal:64/native,
      SwapUsed:64/native,
      MemTotal:64/native,
      MemUsed:64/native,
      MemActualUsed:64/native,
      MemActualFree:64/native,
      AllocStall:64/native,
      Rest/binary>> = Bin,

    StructSize = erlang:size(Bin),

    {PrevCounters, PrevSampleProcs} =
        case PrevSample of
            undefined ->
                {undefined, []};
            _ ->
                PrevSample
        end,

    {NowSamplesProcs0, PrevSampleProcs1} =
        unpack_processes(Rest, PrevSampleProcs, State),
    NowSamplesProcs =
        case NowSamplesProcs0 of
            [] ->
                undefined;
            _ ->
                NowSamplesProcs0
        end,

    #{cgroup_mem := CGroupMem, cpu_count := CPUcount} = LocalStats,
    {MemLimit, _} = memory_quota:choose_limit(MemTotal, MemUsed, CGroupMem),
    CoresAvailable = case CPUcount of
                         unknown -> 0;
                         N -> N
                     end,

    Counters = #{cpu_total_ms => CPUTotalMS,
                 cpu_idle_ms => CPUIdleMS,
                 cpu_user_ms => CPUUserMS,
                 cpu_sys_ms => CPUSysMS,
                 cpu_irq_ms => CPUIrqMS,
                 cpu_stolen_ms => CPUStolenMS},
    NowSamplesGlobal =
        case PrevCounters of
            undefined ->
                undefined;
            _ ->
                compute_cpu_stats(PrevCounters, Counters) ++
                    [{cpu_cores_available, CoresAvailable},
                     {swap_total, SwapTotal},
                     {swap_used, SwapUsed},
                     {mem_limit, MemLimit},
                     {mem_total, MemTotal},
                     {mem_used_sys, MemUsed},
                     {mem_actual_used, MemActualUsed},
                     {mem_actual_free, MemActualFree},
                     {mem_free, MemActualFree},
                     {allocstall, AllocStall}]
        end,

    {{NowSamplesGlobal, NowSamplesProcs}, {Counters, PrevSampleProcs1}}.

unpack_processes(Bin, PrevSample, State) ->
    {NewSample0, NewPrevSample0} =
        do_unpack_processes(Bin, {[], []}, PrevSample, State),

    {collapse_duplicates(NewSample0), collapse_duplicates(NewPrevSample0)}.

collapse_duplicates(Sample) ->
    Sorted = lists:keysort(1, Sample),
    lists:foldl(fun do_collapse_duplicates/2, [], Sorted).

do_collapse_duplicates({K, V1}, [{K, V2} | Acc]) ->
    [{K, V1 + V2} | Acc];
do_collapse_duplicates(KV, Acc) ->
    [KV | Acc].

do_unpack_processes(Bin, Acc, _, _) when size(Bin) =:= 0 ->
    Acc;
do_unpack_processes(Bin, {NewSampleAcc, NewPrevSampleAcc} = Acc, PrevSample, State) ->
    <<Name0:60/binary,
      CpuUtilization:32/native,
      Pid:64/native,
      _PPid:64/native,
      MemSize:64/native,
      MemResident:64/native,
      MemShare:64/native,
      MinorFaults:64/native,
      MajorFaults:64/native,
      PageFaults:64/native,
      Rest/binary>> = Bin,

    RawName = extract_string(Name0),
    case RawName of
        <<>> ->
            Acc;
        _ ->
            Name = adjust_process_name(Pid, RawName, State),

            OldMinorFaults = proc_stat(Name, minor_faults, PrevSample, 0),
            OldMajorFaults = proc_stat(Name, major_faults, PrevSample, 0),
            OldPageFaults = proc_stat(Name, page_faults, PrevSample, 0),

            MinorFaultsDiff = MinorFaults - OldMinorFaults,
            MajorFaultsDiff = MajorFaults - OldMajorFaults,
            PageFaultsDiff = PageFaults - OldPageFaults,

            NewSample =
                [{proc_stat_name(Name, major_faults), MajorFaultsDiff},
                 {proc_stat_name(Name, minor_faults), MinorFaultsDiff},
                 {proc_stat_name(Name, page_faults), PageFaultsDiff},
                 {proc_stat_name(Name, mem_size), MemSize},
                 {proc_stat_name(Name, mem_resident), MemResident},
                 {proc_stat_name(Name, mem_share), MemShare},
                 {proc_stat_name(Name, cpu_utilization), CpuUtilization},
                 {proc_stat_name(Name, minor_faults_raw), MinorFaults},
                 {proc_stat_name(Name, major_faults_raw), MajorFaults},
                 {proc_stat_name(Name, page_faults_raw), PageFaults}],

            NewPrevSampleAcc1 =
                [{proc_stat_name(Name, major_faults), MajorFaults},
                 {proc_stat_name(Name, minor_faults), MinorFaults},
                 {proc_stat_name(Name, page_faults), PageFaults}
                 | NewPrevSampleAcc],

            Acc1 = {NewSample ++ NewSampleAcc, NewPrevSampleAcc1},
            do_unpack_processes(Rest, Acc1, PrevSample, State)
    end.

extract_string(Bin) ->
    do_extract_string(Bin, size(Bin) - 1).

do_extract_string(_Bin, 0) ->
    <<>>;
do_extract_string(Bin, Pos) ->
    case binary:at(Bin, Pos) of
        0 ->
            do_extract_string(Bin, Pos - 1);
        _ ->
            binary:part(Bin, 0, Pos + 1)
    end.

proc_stat(Name, Stat, Sample, Default) ->
    case lists:keyfind(proc_stat_name(Name, Stat), 1, Sample) of
        {_, V} ->
            V;
        _ ->
            Default
    end.

proc_stat_name(Name, Stat) ->
    <<Name/binary, $/, (atom_to_binary(Stat, latin1))/binary>>.

add_ets_stats(Stats) ->
    [{_, NowRestLeaves}] = ets:lookup(ns_server_system_stats, {request_leaves, rest}),
    [{_, PrevRestLeaves}] = ets:lookup(ns_server_system_stats, prev_request_leaves_rest),
    ets:insert(ns_server_system_stats, {prev_request_leaves_rest, NowRestLeaves}),

    [{_, NowHibernateLeaves}] = ets:lookup(ns_server_system_stats, {request_leaves, hibernate}),
    [{_, PrevHibernateLeaves}] = ets:lookup(ns_server_system_stats, prev_request_leaves_hibernate),
    [{_, NowHibernateEnters}] = ets:lookup(ns_server_system_stats, {request_enters, hibernate}),
    ets:insert(ns_server_system_stats, {prev_request_leaves_hibernate, NowHibernateLeaves}),

    RestRate = NowRestLeaves - PrevRestLeaves,
    WakeupRate = NowHibernateLeaves - PrevHibernateLeaves,
    HibernatedCounter = NowHibernateEnters - NowHibernateLeaves,
    [{_, ODPReportFailed}] = ets:lookup(ns_server_system_stats,
                                        odp_report_failed),
    lists:umerge(Stats, lists:sort([{rest_requests, RestRate},
                                    {hibernated_requests, HibernatedCounter},
                                    {hibernated_waked, WakeupRate},
                                    {odp_report_failed, ODPReportFailed}])).

log_system_stats(TS) ->
    NSServerStats = lists:sort(ets:tab2list(ns_server_system_stats)),
    NSCouchDbStats = ns_couchdb_api:fetch_stats(),

    stats_collector:log_stats(TS, "@system", lists:keymerge(1, NSServerStats, NSCouchDbStats)).

grab_stats(#state{port = Port}) ->
    port_command(Port, <<0:32/native>>),
    {recv_data(Port), grab_local_stats()}.

grab_local_stats() ->
    #{cgroup_mem => memory_quota:cgroup_memory_data(),
      cpu_count => misc:cpu_count()}.

process_stats(TS, Binary, PrevSample, _, State) ->
    {{Stats0, ProcStats}, NewPrevSample} = unpack_data(Binary, PrevSample, State),
    RetStats =
        case Stats0 of
            undefined ->
                [];
            _ ->
                Stats = lists:sort(Stats0),
                Stats2 = add_ets_stats(Stats),
                case ets:update_counter(ns_server_system_stats, log_counter, {2, 1, ?ETS_LOG_INTVL, 0}) of
                    0 ->
                        log_system_stats(TS);
                    _ ->
                        ok
                end,
                [{"@system", Stats2}]
        end ++
        case ProcStats of
            undefined ->
                [];
            _ ->
                [{"@system-processes", ProcStats}]
        end,

    update_merger_rates(),
    sample_ns_memcached_queues(),
    {RetStats, NewPrevSample, State}.

increment_counter(Name) ->
    increment_counter(Name, 1).

increment_counter(Name, By) ->
    try
        do_increment_counter(Name, By)
    catch
        _:_ ->
            ok
    end.

do_increment_counter(Name, By) ->
    ets:insert_new(ns_server_system_stats, {Name, 0}),
    ets:update_counter(ns_server_system_stats, Name, By).

set_counter(Name, Value) ->
    (catch do_set_counter(Name, Value)).

do_set_counter(Name, Value) ->
    case ets:insert_new(ns_server_system_stats, {Name, Value}) of
        false ->
            ets:update_element(ns_server_system_stats, Name, {2, Value});
        true ->
            ok
    end.

get_ns_server_stats() ->
    ets:tab2list(ns_server_system_stats).

%% those constants are used to average config merger rates
%% exponentially. See
%% http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
-define(TEN_SEC_ALPHA, 0.0951625819640405).
-define(MIN_ALPHA, 0.0165285461783825).
-define(FIVE_MIN_ALPHA, 0.0799555853706767).

combine_avg_key(Key, Prefix) ->
    case is_tuple(Key) of
        true ->
            list_to_tuple([Prefix | tuple_to_list(Key)]);
        false ->
            {Prefix, Key}
    end.

update_avgs(Key, Value) ->
    [update_avg(combine_avg_key(Key, Prefix), Value, Alpha)
     || {Prefix, Alpha} <- [{avg_10s, ?TEN_SEC_ALPHA},
                            {avg_1m, ?MIN_ALPHA},
                            {avg_5m, ?FIVE_MIN_ALPHA}]],
    ok.

update_avg(Key, Value, Alpha) ->
    OldValue = case ets:lookup(ns_server_system_stats, Key) of
                   [] ->
                       0;
                   [{_, V}] ->
                       V
               end,
    NewValue = OldValue + (Value - OldValue) * Alpha,
    set_counter(Key, NewValue).

read_counter(Key) ->
    ets:insert_new(ns_server_system_stats, {Key, 0}),
    [{_, V}] = ets:lookup(ns_server_system_stats, Key),
    V.

read_and_dec_counter(Key) ->
    V = read_counter(Key),
    increment_counter(Key, -V),
    V.

update_merger_rates() ->
    SleepTime = read_and_dec_counter(total_config_merger_sleep_time),
    update_avgs(config_merger_sleep_time, SleepTime),

    RunTime = read_and_dec_counter(total_config_merger_run_time),
    update_avgs(config_merger_run_time, RunTime),

    Runs = read_and_dec_counter(total_config_merger_runs),
    update_avgs(config_merger_runs_rate, Runs),

    QL = read_counter(config_merger_queue_len),
    update_avgs(config_merger_queue_len, QL).

just_avg_counter(RawKey, AvgKey) ->
    V = read_and_dec_counter(RawKey),
    update_avgs(AvgKey, V).

just_avg_counter(RawKey) ->
    just_avg_counter(RawKey, RawKey).

sample_ns_memcached_queues() ->
    KnownsServices = case ets:lookup(ns_server_system_stats, tracked_ns_memcacheds) of
                         [] -> [];
                         [{_, V}] -> V
                     end,
    ActualServices = [ServiceName || ("ns_memcached-" ++ _) = ServiceName <-
                                         [atom_to_list(Name) || Name <- registered()]],
    ets:insert(ns_server_system_stats, {tracked_ns_memcacheds, ActualServices}),
    [begin
         [ets:delete(ns_server_system_stats, {Prefix, S, Stat})
          || Prefix <- [avg_10s, avg_1m, avg_5m]],
         ets:delete(ns_server_system_stats, {S, Stat})
     end
     || S <- KnownsServices -- ActualServices,
        Stat <- [qlen, call_time, calls, calls_rate,
                 long_call_time, long_calls, long_calls_rate,
                 e2e_call_time, e2e_calls, e2e_calls_rate]],
    [begin
         case (catch erlang:process_info(whereis(list_to_atom(S)), message_queue_len)) of
             {message_queue_len, QL} ->
                 QLenKey = {S, qlen},
                 update_avgs(QLenKey, QL),
                 set_counter(QLenKey, QL);
             _ -> ok
         end,

         just_avg_counter({S, call_time}),
         just_avg_counter({S, calls}, {S, calls_rate}),

         just_avg_counter({S, long_call_time}),
         just_avg_counter({S, long_calls}, {S, long_calls_rate}),

         just_avg_counter({S, e2e_call_time}),
         just_avg_counter({S, e2e_calls}, {S, e2e_calls_rate})
     end || S <- ["unknown" | ActualServices]],
    ok.

get_histo_bin(Value) when Value =< 0 -> 0;
get_histo_bin(Value) when Value > 64000000 -> infinity;
get_histo_bin(Value) when Value > 32000000 -> 64000000;
get_histo_bin(Value) when Value > 16000000 -> 32000000;
get_histo_bin(Value) when Value > 8000000 -> 16000000;
get_histo_bin(Value) when Value > 4000000 -> 8000000;
get_histo_bin(Value) when Value > 2000000 -> 4000000;
get_histo_bin(Value) when Value > 1000000 -> 2000000;
get_histo_bin(Value) ->
    Step = if
               Value < 100 -> 10;
               Value < 1000 -> 100;
               Value < 10000 -> 1000;
               Value =< 1000000 -> 10000
           end,
    ((Value + Step - 1) div Step) * Step.


-define(EPOCH_DURATION, 30).
-define(EPOCH_PRESERVE_COUNT, 5).

add_histo(Type, Value) ->
    BinV = get_histo_bin(Value),
    Epoch = erlang:monotonic_time(second) div ?EPOCH_DURATION,
    K = {h, Type, Epoch, BinV},
    increment_counter(K, 1),
    increment_counter({hg, Type, BinV}, 1).

cleanup_stale_epoch_histos() ->
    NowEpoch = erlang:monotonic_time(second) div ?EPOCH_DURATION,
    FirstStaleEpoch = NowEpoch - ?EPOCH_PRESERVE_COUNT,
    RV = ets:select_delete(ns_server_system_stats,
                           [{{{h, '_', '$1', '_'}, '_'},
                             [{'=<', '$1', {const, FirstStaleEpoch}}],
                             [true]}]),
    RV.

stale_histo_epoch_cleaner() ->
    erlang:register(system_stats_collector_stale_epoch_cleaner, self()),
    stale_histo_epoch_cleaner_loop().

stale_histo_epoch_cleaner_loop() ->
    cleanup_stale_epoch_histos(),
    timer:sleep(?EPOCH_DURATION * ?EPOCH_PRESERVE_COUNT * 1100),
    stale_histo_epoch_cleaner_loop().

spawn_ale_stats_collector() ->
    ns_pubsub:subscribe_link(
      ale_stats_events,
      fun ({{ale_disk_sink, Name}, StatName, Value}) ->
              add_histo({Name, StatName}, Value);
          (_) ->
              ok
      end).

grab_pid_names() ->
    OurPid = list_to_integer(os:getpid()),
    BabysitterPid = ns_server:get_babysitter_pid(),
    CouchdbPid = ns_couchdb_api:get_pid(),

    [{OurPid, <<"ns_server">>},
     {BabysitterPid, <<"babysitter">>},
     {CouchdbPid, <<"couchdb">>}].

adjust_process_name(Pid, Name, #state{pid_names = PidNames}) ->
    case lists:keyfind(Pid, 1, PidNames) of
        false ->
            Name;
        {Pid, BetterName} ->
            BetterName
    end.

compute_cpu_stats(OldCounters, Counters) ->
    Diffs = maps:map(fun (Key, Value) ->
                             OldValue = maps:get(Key, OldCounters),
                             Value - OldValue
                     end, Counters),

    #{cpu_idle_ms := Idle,
      cpu_user_ms := User,
      cpu_sys_ms := Sys,
      cpu_irq_ms := Irq,
      cpu_stolen_ms := Stolen,
      cpu_total_ms := Total} = Diffs,

    [{cpu_utilization_rate, compute_utilization(Total - Idle, Total)},
     {cpu_user_rate, compute_utilization(User, Total)},
     {cpu_sys_rate, compute_utilization(Sys, Total)},
     {cpu_irq_rate, compute_utilization(Irq, Total)},
     {cpu_stolen_rate, compute_utilization(Stolen, Total)}].

compute_utilization(Used, Total) ->
    try
        100 * Used / Total
    catch error:badarith ->
            0
    end.
