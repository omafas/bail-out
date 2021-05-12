%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_bootstrap).
-include_lib("kernel/include/logger.hrl").
-include("ns_common.hrl").

-export([start/0, stop/0, ensure_os_mon/0]).

start() ->
    try
        %% Check disk space every minute instead of every 30
        application:set_env(os_mon, disk_space_check_interval, 1),

        Apps = [ale, asn1, crypto, public_key, ssl,
                lhttpc, inets, sasl, os_mon, ns_server],
        lists:foreach(
          fun (os_mon = App) ->
                  ok = application:start(App);
              (App) ->
                  ok = application:start(App, permanent)
          end, Apps)
    catch T:E ->
            timer:sleep(500),
            erlang:T(E)
    end.

stop() ->
    ?log_info("Initiated server shutdown"),
    ?LOG_INFO("Initiated server shutdown"),
    RV = try
             ok = application:stop(ns_server),
             ?log_info("Successfully stopped ns_server"),
             ale:sync_all_sinks(),
             %% TODO: somehow shutdown of ale may take up to about 5
             %% seconds. So we're just doing sync above and exit
             %%
             %% ?log_info("Stopped ns_server application"),
             %% ?LOG_INFO("Stopped ns_server application"),
             %% application:stop(os_mon),
             %% application:stop(sasl),
             %% application:stop(ale),

             ok
         catch T:E:S ->
                 Msg = io_lib:format("Got error trying to stop applications~n~p",
                                     [{T, E, S}]),

                 (catch ?log_error(Msg)),
                 (catch ?LOG_ERROR(Msg)),
                 {T, E}
         end,
    RV.

ensure_os_mon() ->
    %% since os_mon is started as temporary application, if it
    %% terminates, it needs to be restarted before each call to
    %% disksup or memsup

    %% strictly speaking, the whereis(os_mon_sup) is not needed because
    %% application:ensure_started works both when application is started and
    %% not; but having this makes the common case much faster
    case whereis(os_mon_sup) of
        undefined ->
            ok = application:ensure_started(os_mon);
        Pid when is_pid(Pid) ->
            ok
    end.
