%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(sigar).

-behaviour(gen_server).

-include("ns_common.hrl").


%% API
-export([start_link/0,
         get_all/2,
         get_gauges/1,
         get_cgroups_info/0,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          port :: port() | undefined,
          most_recent_data :: binary() | undefined,
          most_recent_data_ts_usec :: integer() | undefined,
          most_recent_unpacked :: {#{atom() := number()},
                                   [{atom(), number()}],
                                   #{atom() := number() | boolean()}}
                                   | undefined
         }).

-define(SIGAR_CACHE_TIME_USEC, 1000000).
-define(CGROUPS_INFO_SIZE, 88).
-define(GLOBAL_STATS_SIZE, 112).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_all(Opaque, PidNames) ->
    gen_server:call(?MODULE, {get_all, Opaque, PidNames}).

get_gauges(Items) ->
    gen_server:call(?MODULE, {get_gauges, Items}).

get_cgroups_info() ->
    gen_server:call(?MODULE, get_cgroups_info).

stop() ->
    gen_server:call(?MODULE, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Name = lists:flatten(io_lib:format("portsigar for ~s", [node()])),
    Port = spawn_sigar(Name, ns_server:get_babysitter_pid()),
    {ok, #state{port = Port}}.

handle_call({get_all, undefined, PidNames}, _From, State) ->
    handle_call({get_all, {undefined, undefined, undefined}, PidNames}, _From,
                State);
handle_call({get_all, {PrevTS, PrevCounters, PrevCGroups}, PidNames}, _From,
            State) ->
    NewState = update_sigar_data(State),
    #state{most_recent_unpacked = {Counters, Gauges, CGroups},
           most_recent_data_ts_usec = TS} = NewState,
    HostCounters = compute_cpu_stats(PrevCounters, Counters),
    Cores = proplists:get_value(cpu_cores_available, Gauges),
    CGroupsCounters =
        case maps:get(supported, CGroups, false) of
            true -> compute_cgroups_counters(Cores, PrevTS, TS,
                                             PrevCGroups, CGroups);
            false when HostCounters == [] -> [];
            false -> default_cgroups_counters(HostCounters)
        end,
    ProcStats = unpack_processes(NewState#state.most_recent_data, PidNames),
    {reply, {{HostCounters ++ CGroupsCounters, Gauges, ProcStats},
     {TS, Counters, CGroups}}, NewState};

handle_call({get_gauges, Items}, _From, State) ->
    NewState = maybe_update_stats(State),
    {_, Gauges, _} = NewState#state.most_recent_unpacked,
    Res = [{I, proplists:get_value(I, Gauges)} || I <- Items],
    {reply, Res, NewState};

handle_call(get_cgroups_info, _From, State) ->
    NewState = maybe_update_stats(State),
    {_, _, CGroupsInfo} = NewState#state.most_recent_unpacked,
    {reply, CGroupsInfo, State};

handle_call(stop, _From, #state{port = Port} = State) ->
    catch port_close(Port),
    {stop, normal, ok, State#state{port = undefined}};

handle_call(Request, _From, State) ->
    ?log_error("Unhandled call: ~p", [Request]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?log_error("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info({Port, {exit_status, Status}}, #state{port = Port} = State) ->
    ?log_error("Received exit_status ~p from sigar", [Status]),
    {stop, {sigar, Status}, State};
handle_info({Port, eof}, #state{port = Port} = State) ->
    ?log_error("Received eof from sigar"),
    {stop, {sigar, eof}, State};
handle_info(Info, State) ->
    ?log_error("Unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

spawn_sigar(Name, BabysitterPid) ->
    Path = path_config:component_path(bin, "sigar_port"),
    ?log_info("Spawing sigar process '~s'(~p) with babysitter pid: ~p",
              [Name, Path, BabysitterPid]),
    open_port({spawn_executable, Path},
              [stream, use_stdio, exit_status, binary, eof,
               {arg0, Name},
               {args, [integer_to_list(BabysitterPid)]}]).

grab_stats(Port) ->
    port_command(Port, <<0:32/native>>),
    recv_data(Port).

unpack_global(Bin) ->
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
      _/binary>> = Bin,

    CgroupsInfo = unpack_cgroups_info(Bin),

    StructSize = erlang:size(Bin),

    CGroupMem = {maps:get(memory_max, CgroupsInfo, 0),
                 maps:get(memory_current, CgroupsInfo, 0)},
    {MemLimit, _} = memory_quota:choose_limit(MemTotal, MemUsed, CGroupMem),
    CoresAvailable = case maps:get(num_cpu_prc, CgroupsInfo, undefined) of
                         undefined ->
                             %% Suppressing dialyzer warning here
                             %% Dialyzer thinks that system_info can't return
                             %% 'unknown', while according to doc it seems like
                             %% it actually can. So, in order to avoid a warning
                             %% the value is compared with 0 insead of explicit
                             %% match to 'unknown'
                             case erlang:system_info(logical_processors) of
                                 N when is_number(N), N > 0 -> N;
                                 _ -> 0
                             end;
                         N -> N / 100 %% do not round it,
                                      %% cpu utilization will break
                     end,

    Counters = #{cpu_total_ms => CPUTotalMS,
                 cpu_idle_ms => CPUIdleMS,
                 cpu_user_ms => CPUUserMS,
                 cpu_sys_ms => CPUSysMS,
                 cpu_irq_ms => CPUIrqMS,
                 cpu_stolen_ms => CPUStolenMS},

    Gauges =
        [{cpu_cores_available, CoresAvailable},
         {swap_total, SwapTotal},
         {swap_used, SwapUsed},
         {mem_limit, MemLimit},
         {mem_total, MemTotal},
         {mem_used_sys, MemUsed},
         {mem_actual_used, MemActualUsed},
         {mem_actual_free, MemActualFree},
         {mem_free, MemActualFree},
         {allocstall, AllocStall}],

    {Counters, Gauges, CgroupsInfo}.

unpack_processes(Bin, ProcNames) ->
    ProcBinLength = byte_size(Bin) - ?GLOBAL_STATS_SIZE - ?CGROUPS_INFO_SIZE,
    ProcessesBin = binary:part(Bin, ?GLOBAL_STATS_SIZE, ProcBinLength),
    NewSample0 = do_unpack_processes(ProcessesBin, [], ProcNames),
    collapse_duplicates(NewSample0).

collapse_duplicates(Sample) ->
    Sorted = lists:keysort(1, Sample),
    lists:foldl(fun do_collapse_duplicates/2, [], Sorted).

do_collapse_duplicates({K, V1}, [{K, V2} | Acc]) ->
    [{K, V1 + V2} | Acc];
do_collapse_duplicates(KV, Acc) ->
    [KV | Acc].

do_unpack_processes(Bin, Acc, _) when size(Bin) =:= 0 ->
    Acc;
do_unpack_processes(Bin, NewSampleAcc, ProcNames) ->
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
            NewSampleAcc;
        _ ->
            Name = adjust_process_name(Pid, RawName, ProcNames),

            NewSample =
                [{proc_stat_name(Name, mem_size), MemSize},
                 {proc_stat_name(Name, mem_resident), MemResident},
                 {proc_stat_name(Name, mem_share), MemShare},
                 {proc_stat_name(Name, cpu_utilization), CpuUtilization},
                 {proc_stat_name(Name, minor_faults_raw), MinorFaults},
                 {proc_stat_name(Name, major_faults_raw), MajorFaults},
                 {proc_stat_name(Name, page_faults_raw), PageFaults}],

            Acc1 = NewSample ++ NewSampleAcc,
            do_unpack_processes(Rest, Acc1, ProcNames)
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

proc_stat_name(Name, Stat) ->
    <<Name/binary, $/, (atom_to_binary(Stat, latin1))/binary>>.

adjust_process_name(Pid, Name, PidNames) ->
    case lists:keyfind(Pid, 1, PidNames) of
        false ->
            Name;
        {Pid, BetterName} ->
            BetterName
    end.

compute_cpu_stats(undefined, _Counters) -> [];
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

    [{cpu_host_utilization_rate, compute_utilization(Total - Idle, Total)},
     {cpu_host_user_rate, compute_utilization(User, Total)},
     {cpu_host_sys_rate, compute_utilization(Sys, Total)},
     {cpu_irq_rate, compute_utilization(Irq, Total)},
     {cpu_stolen_rate, compute_utilization(Stolen, Total)}].

compute_cgroups_counters(Cores, PrevTS, TS,
                         #{supported := true} = Old,
                         #{supported := true} = New)
                                        when is_number(PrevTS), is_number(TS),
                                             is_number(Cores), Cores > 0 ->
    TimeDelta = TS - PrevTS,
    ComputeRate = fun (Key) ->
                      OldV = maps:get(Key, Old),
                      NewV = maps:get(Key, New),
                      compute_utilization(NewV - OldV, TimeDelta * Cores)
                  end,
    [{cpu_utilization_rate, ComputeRate(usage_usec)},
     {cpu_user_rate, ComputeRate(user_usec)},
     {cpu_sys_rate, ComputeRate(system_usec)},
     {cpu_throttled_rate, ComputeRate(throttled_usec)},
     {cpu_burst_rate, ComputeRate(burst_usec)}];
compute_cgroups_counters(_, _, _, _, _) ->
    [].

default_cgroups_counters(HostCounters) ->
    [{cpu_utilization_rate,
      proplists:get_value(cpu_host_utilization_rate, HostCounters)},
     {cpu_user_rate,
      proplists:get_value(cpu_host_user_rate, HostCounters)},
     {cpu_sys_rate,
      proplists:get_value(cpu_host_sys_rate, HostCounters)}].

compute_utilization(Used, Total) ->
    try
        100 * Used / Total
    catch error:badarith ->
            0
    end.

recv_data(Port) ->
    recv_data_loop(Port, <<"">>).

recv_data_loop(Port, <<Version:32/native,
                       StructSize:32/native, _/binary>> = Acc)
  when Version =:= 6 ->
    recv_data_with_length(Port, Acc, StructSize - erlang:size(Acc));
recv_data_loop(_, <<Version:32/native, _/binary>>) ->
    error({unsupported_portsigar_version, Version});
recv_data_loop(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            recv_data_loop(Port, <<Data/binary, Acc/binary>>);
        {Port, {exit_status, Status}} ->
            ?log_error("Received exit_status ~p from sigar", [Status]),
            exit({sigar, Status});
        {Port, eof} ->
            ?log_error("Received eof from sigar"),
            exit({sigar, eof})
    end.

recv_data_with_length(_Port, Acc, _WantedLength = 0) ->
    erlang:iolist_to_binary(Acc);
recv_data_with_length(Port, Acc, WantedLength) ->
    receive
        {Port, {data, Data}} ->
            Size = size(Data),
            if
                Size =< WantedLength ->
                    recv_data_with_length(Port, [Acc | Data],
                                          WantedLength - Size);
                Size > WantedLength ->
                    erlang:error({too_big_recv, Size, WantedLength, Data, Acc})
            end;
        {Port, {exit_status, Status}} ->
            ?log_error("Received exit_status ~p from sigar", [Status]),
            exit({sigar, Status});
        {Port, eof} ->
            ?log_error("Received eof from sigar"),
            exit({sigar, eof})
    end.

unpack_cgroups_info(Bin) ->
    CGroupsBin = binary:part(Bin, byte_size(Bin), -?CGROUPS_INFO_SIZE),
    unpack_cgroups(CGroupsBin).

unpack_cgroups(<<0:8/native, _/binary>>) ->
    #{supported => false};
unpack_cgroups(<<_:8/native,
                 CgroupsVsn:8/native,
                 NumCpuPrc:16/native,
                 _Padding:32,
                 MemMax:64/native,
                 MemCurr:64/native,
                 UsageUsec:64/native,
                 UserUsec:64/native,
                 SysUsec:64/native,
                 NrPeriods:64/native,
                 NrThrottled:64/native,
                 ThrottledUsec:64/native,
                 NrBursts:64/native,
                 BurstUsec:64/native>>) ->
    #{supported => true,
      cgroups_vsn => CgroupsVsn,
      num_cpu_prc => NumCpuPrc,
      memory_max => MemMax,
      memory_current => MemCurr,
      usage_usec => UsageUsec,
      user_usec => UserUsec,
      system_usec => SysUsec,
      nr_periods => NrPeriods,
      nr_throttled => NrThrottled,
      throttled_usec => ThrottledUsec,
      nr_bursts => NrBursts,
      burst_usec => BurstUsec}.

maybe_update_stats(#state{most_recent_data_ts_usec = TS} = State) ->
    case TS == undefined orelse timestamp() - TS >= ?SIGAR_CACHE_TIME_USEC of
        true -> update_sigar_data(State);
        false -> State
    end.

timestamp() -> erlang:monotonic_time(microsecond).

update_sigar_data(#state{port = Port} = State) ->
    Bin = grab_stats(Port),
    State#state{most_recent_data = Bin,
                most_recent_data_ts_usec = timestamp(),
                most_recent_unpacked = unpack_global(Bin)}.
