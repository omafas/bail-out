%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_info).

-export([version/0, version/1, runtime/0, basic_info/0]).

version() ->
    lists:map(fun({App, _, Version}) -> {App, Version} end,
              application:loaded_applications()).

version(App) ->
    proplists:get_value(App, version()).

runtime() ->
    {WallClockMSecs, _} = erlang:statistics(wall_clock),
    [{otp_release, erlang:system_info(otp_release)},
     {erl_version, erlang:system_info(version)},
     {erl_version_long, erlang:system_info(system_version)},
     {system_arch_raw, erlang:system_info(system_architecture)},
     {system_arch, system_arch()},
     {localtime, erlang:localtime()},
     {memory, misc:memory()},
     {loaded, erlang:loaded()},
     {applications, application:loaded_applications()},
     {pre_loaded, erlang:pre_loaded()},
     {process_count, erlang:system_info(process_count)},
     {node, erlang:node()},
     {nodes, erlang:nodes()},
     {registered, erlang:registered()},
     {cookie, erlang:get_cookie()},
     {wordsize, erlang:system_info(wordsize)},
     {wall_clock, trunc(WallClockMSecs / 1000)}].


basic_info() ->
    {WallClockMSecs, _} = erlang:statistics(wall_clock),
    {erlang:node(),
     [{version, version()},
      {supported_compat_version, cluster_compat_mode:supported_compat_version()},
      {advertised_version, cluster_compat_mode:mb_master_advertised_version()},
      {system_arch, system_arch()},
      {wall_clock, trunc(WallClockMSecs / 1000)},
      {memory_data, memory_quota:this_node_memory_data()},
      {disk_data, ns_disksup:get_disk_data()}]}.

system_arch() ->
    case erlang:system_info(system_architecture) of
        % Per bug 607, erlang R13B03 doesn't know it's on a 64-bit windows,
        % and always reports "win32".
        "win32" ->
            case erlang:system_info({wordsize, external}) of
                4 -> "win32";
                8 -> "win64"
            end;
        X -> X
    end.
