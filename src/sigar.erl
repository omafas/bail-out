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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0,
         get_all/1,
         get_gauges/1,
         get_cgroups_info/0,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          port :: port() | undefined,
          most_recent_data :: map() | undefined,
          most_recent_data_ts_usec :: integer() | undefined,
          most_recent_unpacked :: {#{bitstring() := number() | boolean()},
                                   [{atom(), number()}],
                                   #{bitstring() := number() | boolean()}}
                                   | undefined
         }).

-define(SIGAR_CACHE_TIME_USEC, 1000000).
-define(CGROUPS_INFO_SIZE, 96).
-define(GLOBAL_STATS_SIZE, 112).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_all(PidNames) ->
    gen_server:call(?MODULE, {get_all, PidNames}).

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

handle_call({get_all, PidNames}, _From, State) ->
    NewState = update_sigar_data(State),
    #state{most_recent_unpacked = {Counters, Gauges, CGroups}} = NewState,
    HostCounters = compute_cpu_stats(Counters),
    Cores = proplists:get_value(cpu_cores_available, Gauges),
    CGroupsCounters =
        case maps:get(<<"supported">>, CGroups, false) of
            true -> compute_cgroups_counters(Cores, CGroups);
            false -> []
        end,
    ProcStats = get_process_stats(NewState#state.most_recent_data, PidNames),
    DiskStats = get_disk_stats(NewState#state.most_recent_data),
    {reply, {HostCounters ++ CGroupsCounters, Gauges, ProcStats, DiskStats},
     NewState};

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
    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),
    LogFile = filename:join(LogDir, "sigar_port.log"),
    Path = path_config:component_path(bin, "sigar_port"),
    ?log_info("Spawning sigar process '~s'(~p) with babysitter pid:"
              " ~p and log file ~p", [Name, Path, BabysitterPid, LogFile]),
    open_port({spawn_executable, Path},
              [stream, use_stdio, exit_status, binary, eof,
               {arg0, Name},
               {args, ["--babysitter_pid", integer_to_list(BabysitterPid),
                       "--logfile", LogFile,
                       "--config", path_config:default_sigar_port_config_path()]}]).

grab_stats(Port) ->
    port_command(Port, <<"\n":8/native>>),
    recv_data(Port, <<"">>).

%% The first line contains the size of the JSON output to follow.
recv_data(Port, Acc) ->
    receive
        {Port, {data, Curr}} ->
            Data = <<Curr/binary, Acc/binary>>,
            case binary:split(Data, <<"\n">>) of
                [Length, Rest] ->
                    Remaining = binary_to_integer(Length) - erlang:size(Rest),
                    recv_data_with_length(Port, Rest, Remaining);
                _ -> recv_data(Port, Data)
            end;
        {Port, {exit_status, Status}} ->
            ?log_error("Received exit_status ~p from sigar", [Status]),
            exit({sigar, Status});
        {Port, eof} ->
            ?log_error("Received eof from sigar"),
            exit({sigar, eof})
    end.

recv_data_with_length(_Port, Acc, _WantedLength = 0) ->
    Bin = erlang:iolist_to_binary(Acc),
    {Decoded} = parse_json_elem(ejson:decode(Bin)),
    maps:from_list(Decoded);
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

%% Every JSON element is currently an integer/boolean/string. Most are strings
%% even if they represent numeric data. There is some contention on whether all
%% JSON libraries can represent 64-bit integers (or are limited to 2^53). So
%% they are transmitted as strings. Convert every bitstring to integer/float
%% when possible.
parse_json_elem({K, V}) -> {K, parse_json_elem(V)};
parse_json_elem({Obj}) -> {parse_json_elem(Obj)};
parse_json_elem(Data) when is_list(Data) ->
    lists:map(fun parse_json_elem/1, Data);
parse_json_elem(Data) when is_boolean(Data) -> Data;
parse_json_elem(Data) when is_integer(Data) -> Data;
parse_json_elem(Data) when is_float(Data) -> Data;
parse_json_elem(Data) when is_binary(Data) ->
    try binary_to_integer(Data) of
        Y -> Y
    catch _:_ ->
            try binary_to_float(Data) of
                X -> X
            catch _:_ ->
                    Data
            end
    end.

cpu_filter_counter(<<"cpu_", End/binary>>, Val) ->
    case binary:split(End, <<"_ms">>) of
        [_, <<>>] when is_number(Val) -> true;
        _ -> false
    end;
cpu_filter_counter(_, _) ->
     false.

get_number(StatsMap, Field) ->
    case maps:get(Field, StatsMap, undefined) of
        X when is_number(X) -> X;
        _ -> 0
end.

get_global_stats(StatsMap) ->
    CgroupsInfo =
        case maps:get(<<"control_group_info">>, StatsMap, undefined) of
            undefined -> #{<<"supported">> => false};
            {CgroupsStats} ->
                Map1 = maps:from_list(CgroupsStats),
                maps:put(<<"supported">>, true, Map1)
        end,
    CgroupsPressure =
        case CgroupsInfo of
            #{<<"supported">> := true, <<"pressure">> := CPressMap} ->
                get_pressure_stats(<<"pressure/cgroup">>, CPressMap, []);
            _ -> []
        end,
    {CGMemLimit, CGMemUsed, CGMemActual} =
        case CgroupsInfo of
            #{<<"supported">> := true, <<"memory_current">> := MCurr,
               <<"memory_cache">> := MCache, <<"memory_max">> := MMax}
              when is_number(MCurr), is_number(MCache), is_number(MMax) ->
                {MMax, MCurr, MCurr - MCache};
            _ -> {undefined, undefined, undefined}
        end,
    MemTotal = get_number(StatsMap, <<"mem_total">>),
    MemUsed = get_number(StatsMap, <<"mem_used">>),
    {MemLimit, _} = memory_quota:choose_limit(MemTotal, MemUsed,
                                              {CGMemLimit, CGMemUsed}),
    %% Suppressing dialyzer warning here
    %% Dialyzer thinks that system_info can't return 'unknown', while according
    %% to doc it seems like it actually can. So, in order to avoid a warning
    %% the value is compared with 0 insead of explicit match to 'unknown'
    HostCoresAvailable = case erlang:system_info(logical_processors_online) of
                             P when is_number(P), P > 0 -> P;
                             _ -> 0
                         end,
    CoresAvailable = case maps:get(<<"num_cpu_prc">>, CgroupsInfo, undefined) of
                         N when is_number(N) ->
                             %% do not round it, cpu utilization will break
                             N / 100;
                         _ -> HostCoresAvailable
                     end,
    Counters = case maps:get(<<"cpu_total_ms">>, StatsMap, undefined) of
                   X when is_number(X) ->
                       CMap = maps:filter(fun cpu_filter_counter/2, StatsMap),
                       maps:put(<<"supported">>, true, CMap);
                   _ -> #{<<"supported">> => false}
               end,
    HostPressure = case maps:get(<<"pressure">>, StatsMap, undefined) of
                       undefined -> [];
                       PressMap ->
                           get_pressure_stats(<<"pressure/host">>, PressMap, [])
                   end,
    Gauges =
        [{cpu_cores_available, CoresAvailable},
         {cpu_host_cores_available, HostCoresAvailable},
         {swap_total, get_number(StatsMap, <<"swap_total">>)},
         {swap_used, get_number(StatsMap, <<"swap_used">>)},
         {mem_limit, MemLimit},
         {mem_total, MemTotal},
         {mem_used_sys, MemUsed},
         {mem_actual_used, get_number(StatsMap, <<"mem_actual_used">>)},
         {mem_actual_free, get_number(StatsMap, <<"mem_actual_free">>)},
         {mem_free, get_number(StatsMap, <<"mem_actual_free">>)},
         {allocstall, get_number(StatsMap, <<"allocstall">>)}] ++
        [{mem_cgroup_limit, CGMemLimit} || CGMemLimit /= undefined] ++
        [{mem_cgroup_actual_used, CGMemActual} || CGMemActual /= undefined] ++
        [{mem_cgroup_used, CGMemUsed} || CGMemUsed /= undefined] ++
        HostPressure ++ CgroupsPressure,

    {Counters, Gauges, CgroupsInfo}.

get_pressure_stats(<<Prefix/binary>>, Elem, Acc) ->
    case Elem of
        Val when is_number(Val) ->
            [{Prefix, Val} | Acc];
        {<<Key/binary>>, Y} ->
            case binary:split(Key, <<"avg">>) of
                [<<>>, Rest] ->
                    get_pressure_stats(<<Prefix/binary, "/share_time_stalled/",
                                         Rest/binary>>, Y, Acc);
                _ ->
                    get_pressure_stats(<<Prefix/binary, $/, Key/binary>>, Y,
                                       Acc)
            end;
        {Obj} -> lists:foldl(
                   fun(X, AccIn) ->
                           get_pressure_stats(<<Prefix/binary>>, X, AccIn)
                   end, Acc, Obj);
        _ -> Acc
    end.

get_process_stats(StatsMap, ProcNames) ->
    collapse_duplicates(populate_processes(StatsMap, ProcNames)).

get_disk_stats(StatsMap) ->
    RawDiskStats = maps:get(<<"disks">>, StatsMap,
                            undefined),
    case RawDiskStats of
        undefined -> [];
        Val ->
            lists:flatten(
              lists:map(
                fun({Stats}) ->
                        populate_disk_stats(Stats)
                end, Val))
    end.

collapse_duplicates(Sample) ->
    Sorted = lists:keysort(1, Sample),
    lists:foldl(fun do_collapse_duplicates/2, [], Sorted).

do_collapse_duplicates({K, V1}, [{K, V2} | Acc]) ->
    [{K, V1 + V2} | Acc];
do_collapse_duplicates(KV, Acc) ->
    [KV | Acc].

%% The "_faults" stats are reported with suffix _raw.
%% cpu_user/sys will be reported in seconds.
fix_stat_name(Stat) ->
    case Stat of
        <<"minor_faults">> -> <<"minor_faults_raw">>;
        <<"major_faults">> -> <<"major_faults_raw">>;
        <<"page_faults">> -> <<"page_faults_raw">>;
        <<"cpu_user">> -> <<"cpu_seconds_total_user">>;
        <<"cpu_sys">> -> <<"cpu_seconds_total_sys">>;
        _ -> Stat
    end.

%% These stats are reported in msecs by the sigar program and are
%% converted to seconds for use by ns_server/prometheus.
stats_to_convert() ->
    [<<"cpu_seconds_total_user">>, <<"cpu_seconds_total_sys">>].

populate_disk_stat(DiskName, StatName, Value) ->
    case Value of
        X when is_number(X) ->
            {true, {proc_stat_name(DiskName, StatName), X}};
        _ -> false
    end.

populate_disk_stats(Stats) ->
    {<<"name">>, Name} = lists:keyfind(<<"name">>, 1, Stats),
    lists:filtermap(
      fun({Stat, Value}) ->
              populate_disk_stat(Name, Stat, Value)
      end, Stats).

populate_proc_stat(ProcName, Stat, Value) ->
    case Stat of
        <<"name">> -> false;
        <<"pid">> -> false;
        <<"ppid">> -> false;
        _ ->
            StatName = fix_stat_name(Stat),
            case Value of
                X0 when is_number(X0) ->
                    X = case lists:member(StatName, stats_to_convert()) of
                            false ->
                                X0;
                            true ->
                                %% Convert from msecs to seconds
                                X0 / 1000
                        end,
                    {true, {proc_stat_name(ProcName, StatName), X}};
                _ -> false
            end
    end.

populate_proc_stats(Stats, ProcNames) ->
    {<<"pid">>, Pid} = lists:keyfind(<<"pid">>, 1, Stats),
    {<<"name">>, Name} = lists:keyfind(<<"name">>, 1, Stats),
    ProcName =
        case lists:keyfind(Pid, 1, ProcNames) of
            false -> Name;
            {Pid, BetterName} -> BetterName
        end,
    lists:filtermap(fun({Stat, Value}) ->
                            populate_proc_stat(ProcName, Stat, Value) end,
                    Stats).

populate_processes(StatsMap, ProcNames) ->
    ProcStats = maps:get(<<"interesting_procs">>, StatsMap, undefined),
    case ProcStats of
        undefined -> [];
        Val -> lists:flatten(
                 lists:map(
                   fun({Stats}) -> populate_proc_stats(Stats, ProcNames) end,
                   Val))
    end.

proc_stat_name(ProcName, Stat) ->
    <<ProcName/binary, $/, Stat/binary>>.

compute_cpu_stats(#{<<"supported">> := true} = Counters) ->

    RawCpuTotal = get_raw_counter_msec_to_sec(<<"cpu_total_ms">>, Counters),
    RawCpuIdle = get_raw_counter_msec_to_sec(<<"cpu_idle_ms">>, Counters),
    RawCpuUser = get_raw_counter_msec_to_sec(<<"cpu_user_ms">>, Counters),
    RawCpuSys = get_raw_counter_msec_to_sec(<<"cpu_sys_ms">>, Counters),

    %% Raw counters so users can do their own computations using promql.
    [{cpu_host_seconds_total_idle, RawCpuIdle},
     {cpu_host_seconds_total_user, RawCpuUser},
     {cpu_host_seconds_total_sys, RawCpuSys}] ++
    case misc:is_linux() of
        false ->
            Other = RawCpuTotal - (RawCpuUser + RawCpuSys + RawCpuIdle),
            [{cpu_host_seconds_total_other, Other}];
        true ->
            RawCpuIrq = get_raw_counter_msec_to_sec(<<"cpu_irq_ms">>, Counters),
            RawCpuStolen = get_raw_counter_msec_to_sec(<<"cpu_stolen_ms">>,
                                                       Counters),
            Other = RawCpuTotal - (RawCpuUser + RawCpuSys + RawCpuIdle +
                                   RawCpuIrq + RawCpuStolen),
            %% Raw counters so users can do their own computations using
            %% promql.
            [{cpu_host_seconds_total_irq, RawCpuIrq},
             {cpu_host_seconds_total_stolen, RawCpuStolen},
             {cpu_host_seconds_total_other, Other}]
    end;
compute_cpu_stats(_) -> [].

%% The current measurement is returned as a raw counter in seconds.
%% The user can then use prometheus functions to do computations.
get_raw_counter_msec_to_sec(Stat, Counters) ->
    get_raw_counter_inner(Stat, Counters, 1000).

get_raw_counter_usec_to_sec(Stat, Counters) ->
    get_raw_counter_inner(Stat, Counters, 1000_000).

get_raw_counter_inner(Stat, Counters, Divisor) ->
    Value = maps:get(Stat, Counters, 0),
    Value / Divisor.

compute_cgroups_counters(Cores,
                         #{<<"supported">> := true} = New)
                                        when is_number(Cores), Cores > 0 ->
    RawCpuUsage = get_raw_counter_usec_to_sec(<<"usage_usec">>, New),
    RawCpuUser = get_raw_counter_usec_to_sec(<<"user_usec">>, New),
    RawCpuSys = get_raw_counter_usec_to_sec(<<"system_usec">>, New),
    RawCpuThrottled = get_raw_counter_usec_to_sec(<<"throttled_usec">>, New),
    RawCpuBurst = get_raw_counter_usec_to_sec(<<"burst_usec">>, New),

    %% Raw counters so users can do their own computations using promql
    [{cpu_cgroup_seconds_total_usage, RawCpuUsage},
     {cpu_cgroup_seconds_total_user, RawCpuUser},
     {cpu_cgroup_seconds_total_sys, RawCpuSys},
     {cpu_cgroup_seconds_total_throttled, RawCpuThrottled},
     {cpu_cgroup_seconds_total_burst, RawCpuBurst}];
compute_cgroups_counters(_, _) ->
    [].

maybe_update_stats(#state{most_recent_data_ts_usec = TS} = State) ->
    case TS == undefined orelse timestamp() - TS >= ?SIGAR_CACHE_TIME_USEC of
        true -> update_sigar_data(State);
        false -> State
    end.

timestamp() -> erlang:monotonic_time(microsecond).

update_sigar_data(#state{port = Port} = State) ->
    StatsMap = grab_stats(Port),
    State#state{most_recent_data = StatsMap,
                most_recent_data_ts_usec = timestamp(),
                most_recent_unpacked = get_global_stats(StatsMap)}.

-ifdef(TEST).
validate_results(Json, CountersExpected, GaugesExpected, CGExpected, PExpect,
                 PNames, DiskExpected) ->
    StatsMap = recv_data_with_length(31, Json, 0),
    {Counters, Gauges, CGroupsInfo} = get_global_stats(StatsMap),
    ?assertEqual(CGroupsInfo, CGExpected),
    ?assertEqual(Counters, CountersExpected),
    Result1 = [K1 || {K1, _} <- Gauges, {K2, _} <- GaugesExpected,
                     K1 =:= K2],
    ?assertEqual(length(GaugesExpected), length(Result1)),
    Result = [{K1, V1} || {K1, V1} <- Gauges, {K2, V2} <- GaugesExpected,
                          K1 =:= K2, V1 =/= V2],
    ?assertEqual(Result, []),
    OtherKeys = Gauges -- GaugesExpected,
    NonNumeric = [{K, V} || {K, V} <- OtherKeys, not is_number(V)],
    ?assertEqual(NonNumeric, []),
    CgroupsKeys = [mem_cgroup_limit, mem_cgroup_actual_used, mem_cgroup_used],
    CgroupList = [{K, V} || {K, V} <- OtherKeys, K1 <- CgroupsKeys, K =:= K1],
    case maps:get(<<"supported">>, CGExpected) of
        false -> ?assertEqual(CgroupList, []);
        true -> ?assertEqual(length(CgroupList), length(CgroupsKeys))
    end,
    ProcStats = get_process_stats(StatsMap, PNames),
    ?assertEqual(ProcStats, PExpect),
    DiskStats = get_disk_stats(StatsMap),
    ?assertEqual(DiskExpected, DiskStats).

sigar_json_test() ->
    Acc0 =
        <<"
           {
             \"cpu_idle_ms\": \"655676420\",
             \"cpu_irq_ms\": \"0\",
             \"cpu_stolen_ms\": \"0\",
             \"cpu_sys_ms\": \"25792540\",
             \"cpu_total_ms\": \"732003090\",
             \"cpu_user_ms\":\"50534130\",
             \"interesting_procs\":
              [
                 {
                   \"cpu_utilization\": \"4\",
                   \"major_faults\": \"3\",
                   \"minor_faults\": \"19\",
                   \"name\": \"beam.smp\",
                   \"page_faults\": \"21835\",
                   \"pid\": \"65595\",
                   \"ppid\": \"65587\"
                 },
                 {
                   \"cpu_utilization\": \"5\",
                   \"major_faults\": \"1\",
                   \"minor_faults\": \"2\",
                   \"name\":\"sigar_port\",
                   \"page_faults\": \"1298\",
                   \"pid\":\"65618\",
                   \"ppid\": \"65607\"
                 }
              ],
             \"mem_actual_free\":\"4063666176\",
             \"mem_actual_used\": \"30296072192\",
             \"mem_total\": \"34359738368\",
             \"mem_used\": \"33626083328\",
             \"swap_total\": \"1\",
             \"swap_used\": \"2\",
             \"pressure\":
              {
                \"cpu\":
                  {
                    \"full\":
                      {
                        \"avg10\":\"0.00\",
                        \"avg300\":\"0.00\",
                        \"avg60\":\"0.00\",
                        \"total_stall_time_usec\":\"42142\"
                      },
                    \"some\":
                      {
                        \"avg10\":\"0.00\",
                        \"avg300\":\"0.00\",
                        \"avg60\":\"0.00\",
                        \"total_stall_time_usec\":\"44472\"
                      }
                  },
                \"io\":
                  {
                    \"full\":
                      {
                        \"avg10\":\"1.86\",
                        \"avg300\":\"0.59\",
                        \"avg60\":\"2.13\",
                        \"total_stall_time_usec\":\"1939155\"
                      },
                    \"some\":
                      {
                        \"avg10\":\"1.86\",
                        \"avg300\":\"0.59\",
                        \"avg60\":\"2.13\",
                        \"total_stall_time_usec\":\"1939178\"
                      }
                  }
              },
            \"disks\":
              [
                {
                  \"name\": \"sdb\",
                  \"queue\": \"0\",
                  \"queue_depth\": \"25\",
                  \"read_bytes\": \"19476732416\",
                  \"read_time_ms\": \"6911980\",
                  \"reads\": \"1845269\",
                  \"time_ms\": \"2807312\",
                  \"write_bytes\": \"25794304000\",
                  \"write_time_ms\": \"2672373\",
                  \"writes\": \"356261\"
                }
              ]
           }">>,
    CountersExpected0 = #{<<"supported">> => true,
                          <<"cpu_idle_ms">> => 655676420,
                          <<"cpu_irq_ms">> => 0,
                          <<"cpu_stolen_ms">> => 0,
                          <<"cpu_sys_ms">> => 25792540,
                          <<"cpu_total_ms">> => 732003090,
                          <<"cpu_user_ms">> => 50534130},
    GaugesExpected0 = [{swap_total, 1},
                       {swap_used, 2},
                       {mem_total, 34359738368},
                       {mem_used_sys, 33626083328},
                       {mem_actual_used, 30296072192},
                       {mem_actual_free, 4063666176},
                       {mem_free, 4063666176},
                       {allocstall, 0},
                       {<<"pressure/host/cpu/full/share_time_stalled/10">>,
                        0.00},
                       {<<"pressure/host/cpu/full/share_time_stalled/300">>,
                        0.00},
                       {<<"pressure/host/cpu/full/share_time_stalled/60">>,
                        0.00},
                       {<<"pressure/host/cpu/full/total_stall_time_usec">>,
                        42142},
                       {<<"pressure/host/cpu/some/share_time_stalled/10">>,
                        0.00},
                       {<<"pressure/host/cpu/some/share_time_stalled/300">>,
                        0.00},
                       {<<"pressure/host/cpu/some/share_time_stalled/60">>,
                        0.00},
                       {<<"pressure/host/cpu/some/total_stall_time_usec">>,
                        44472},
                       {<<"pressure/host/io/full/share_time_stalled/10">>,
                        1.86},
                       {<<"pressure/host/io/full/share_time_stalled/300">>,
                        0.59},
                       {<<"pressure/host/io/full/share_time_stalled/60">>,
                        2.13},
                       {<<"pressure/host/io/full/total_stall_time_usec">>,
                        1939155},
                       {<<"pressure/host/io/some/share_time_stalled/10">>,
                        1.86},
                       {<<"pressure/host/io/some/share_time_stalled/300">>,
                        0.59},
                       {<<"pressure/host/io/some/share_time_stalled/60">>,
                        2.13},
                       {<<"pressure/host/io/some/total_stall_time_usec">>,
                        1939178}],
    Cgroups0 = #{<<"supported">> => false},
    PNames0 = [{65595, <<"Process0">>}, {65618, <<"Process1">>}],
    Proc0 = [{<<"Process1/page_faults_raw">>,1298},
             {<<"Process1/minor_faults_raw">>,2},
             {<<"Process1/major_faults_raw">>,1},
             {<<"Process1/cpu_utilization">>,5},
             {<<"Process0/page_faults_raw">>,21835},
             {<<"Process0/minor_faults_raw">>,19},
             {<<"Process0/major_faults_raw">>,3},
             {<<"Process0/cpu_utilization">>,4}],
    Disks0 = [{<<"sdb/queue">>,0},
              {<<"sdb/queue_depth">>,25},
              {<<"sdb/read_bytes">>,19476732416},
              {<<"sdb/read_time_ms">>,6911980},
              {<<"sdb/reads">>,1845269},
              {<<"sdb/time_ms">>,2807312},
              {<<"sdb/write_bytes">>,25794304000},
              {<<"sdb/write_time_ms">>,2672373},
              {<<"sdb/writes">>,356261}],
    validate_results(Acc0, CountersExpected0, GaugesExpected0, Cgroups0, Proc0,
                     PNames0, Disks0),
    Acc1 = <<"{\n\"allocstall\": \"1\",\n\"control_group_info\": {\n\""
             "num_cpu_prc\": 8,\n\"memory_current\": \"324\","
             "\n\"memory_cache\": \"123\"\n,\"memory_max\": \"491\"\n,\n\""
             "pressure\":\n{\n\"cpu\":\n{\n\"full\":\n{\n\"avg10\":\"0.00\""
             ",\n\"avg300\":\"0.00\",\n\"avg60\":\"0.00\",\n\""
             "total_stall_time_usec\":\"42142\"\n},\n\"some\":\n{\n\"avg10\""
             ":\"0.00\",\"avg300\":\"0.00\",\"avg60\":\"0.00\",\""
             "total_stall_time_usec\":\"44472\"\n}\n\},\"io\":\n{\n\"full\""
             ":\n{\"avg10\":\"1.86\",\"avg300\":\"0.59\",\"avg60\":\"2.13\","
             "\"total_stall_time_usec\":\"1939155\"\n},\"some\":\n\{\n\""
             "avg10\":\"1.86\",\"avg300\":\"0.59\",\"avg60\":\"2.13\","
             "\"total_stall_time_usec\":\"1939178\"\n}\n}\n}\n},\n\""
             "cpu_idle_ms\": \"655676420\",\n\"cpu_irq_ms\": \"0\",\n\""
             "cpu_stolen_ms\": \"0\",\n\"cpu_sys_ms\": \"25792540\",\n\""
             "cpu_user_ms\": \"50534130\",\n\"interesting_procs\": [\n{\n\""
             "cpu_utilization\": \"34\",\n\"major_faults\": \"10\",\n\""
             "minor_faults\": \"3\",\n\"name\": \"beam.smp\",\n\"page_faults\""
             ": \"23235\",\n\"pid\": \"35525\",\n\"ppid\": \"65587\"\n},\n{\n\""
             "cpu_utilization\": \"1\",\n\"major_faults\": \"0\",\n\""
             "minor_faults\": \"33\",\n\"name\": \"sigar_port\",\n\""
             "page_faults\": \"13398\",\n\"pid\": \"20618\",\n\"ppid\": \""
             "65607\"\n}\n],\n\"mem_actual_free\": \"4063666176\",\n\""
             "mem_actual_used\": \"30296072192\",\n\"mem_total\": \""
             "34359738368\",\n\"mem_used\": \"33626083328\",\n\"swap_total\": "
             "\"1\",\n\"swap_used\": \"2\"\n}">>,
    GaugesExpected1 = [{swap_total, 1},
                       {swap_used, 2},
                       {mem_total, 34359738368},
                       {mem_used_sys, 33626083328},
                       {mem_actual_used, 30296072192},
                       {mem_actual_free, 4063666176},
                       {mem_free, 4063666176},
                       {allocstall, 1},
                       {<<"pressure/cgroup/cpu/full/share_time_stalled/10">>,
                        0.00},
                       {<<"pressure/cgroup/cpu/full/share_time_stalled/300">>,
                        0.00},
                       {<<"pressure/cgroup/cpu/full/share_time_stalled/60">>,
                        0.00},
                       {<<"pressure/cgroup/cpu/full/total_stall_time_usec">>,
                        42142},
                       {<<"pressure/cgroup/cpu/some/share_time_stalled/10">>,
                        0.00},
                       {<<"pressure/cgroup/cpu/some/share_time_stalled/300">>,
                        0.00},
                       {<<"pressure/cgroup/cpu/some/share_time_stalled/60">>,
                        0.00},
                       {<<"pressure/cgroup/cpu/some/total_stall_time_usec">>,
                        44472},
                       {<<"pressure/cgroup/io/full/share_time_stalled/10">>,
                        1.86},
                       {<<"pressure/cgroup/io/full/share_time_stalled/300">>,
                        0.59},
                       {<<"pressure/cgroup/io/full/share_time_stalled/60">>,
                        2.13},
                       {<<"pressure/cgroup/io/full/total_stall_time_usec">>,
                        1939155},
                       {<<"pressure/cgroup/io/some/share_time_stalled/10">>,
                        1.86},
                       {<<"pressure/cgroup/io/some/share_time_stalled/300">>,
                        0.59},
                       {<<"pressure/cgroup/io/some/share_time_stalled/60">>,
                        2.13},
                       {<<"pressure/cgroup/io/some/total_stall_time_usec">>,
                        1939178}],
    Cgroups1 = #{<<"supported">> => true,
                 <<"num_cpu_prc">> => 8,
                 <<"memory_current">> => 324,
                 <<"memory_cache">> => 123,
                 <<"memory_max">> => 491,
                 <<"pressure">> =>
                     {[{<<"cpu">>,
                        {[{<<"full">>,
                           {[{<<"avg10">>, 0.00},
                             {<<"avg300">>, 0.00},
                             {<<"avg60">>, 0.00},
                             {<<"total_stall_time_usec">>, 42142}]}},
                          {<<"some">>,
                           {[{<<"avg10">>, 0.00},
                             {<<"avg300">>, 0.00},
                             {<<"avg60">>, 0.00},
                             {<<"total_stall_time_usec">>, 44472}]}}]}},
                       {<<"io">>,
                        {[{<<"full">>,
                           {[{<<"avg10">>, 1.86},
                             {<<"avg300">>, 0.59},
                             {<<"avg60">>, 2.13},
                             {<<"total_stall_time_usec">>, 1939155}]}},
                          {<<"some">>,
                           {[{<<"avg10">>, 1.86},
                             {<<"avg300">>, 0.59},
                             {<<"avg60">>, 2.13},
                             {<<"total_stall_time_usec">>, 1939178}]}}]}}
                      ]}},
    PNames1 = [{35525, <<"Process2">>}, {20618, <<"Process3">>}],
    Proc1 = [{<<"Process3/page_faults_raw">>,13398},
             {<<"Process3/minor_faults_raw">>,33},
             {<<"Process3/major_faults_raw">>,0},
             {<<"Process3/cpu_utilization">>,1},
             {<<"Process2/page_faults_raw">>,23235},
             {<<"Process2/minor_faults_raw">>,3},
             {<<"Process2/major_faults_raw">>,10},
             {<<"Process2/cpu_utilization">>,34}],
    CountersExpected1 = #{<<"supported">> => false},
    validate_results(Acc1, CountersExpected1, GaugesExpected1, Cgroups1, Proc1,
                     PNames1, []).
-endif.
