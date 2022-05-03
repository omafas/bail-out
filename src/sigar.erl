%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(sigar).

-export([stats/2, spawn/2, grab_stats/1, unpack_cgroups_info/1]).

-include("ns_common.hrl").

stats(Name, BabysitterPid) ->
    Port = ?MODULE:spawn(Name, BabysitterPid),
    try
        grab_stats(Port)
    after
        port_close(Port)
    end.

spawn(Name, BabysitterPid) ->
    Path = path_config:component_path(bin, "sigar_port"),
    open_port({spawn_executable, Path},
              [stream, use_stdio, exit_status, binary, eof,
               {arg0, Name},
               {args, [integer_to_list(BabysitterPid)]}]).

grab_stats(Port) ->
    port_command(Port, <<0:32/native>>),
    recv_data(Port).

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
