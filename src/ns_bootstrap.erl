%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2015 Couchbase, Inc.
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
-module(ns_bootstrap).
-include("ns_common.hrl").

-export([start/0, stop/0, remote_stop/1, ensure_os_mon/0]).

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
    error_logger:info_msg("Initiated server shutdown"),
    RV = try
             ok = application:stop(ns_server),
             ?log_info("Successfully stopped ns_server"),
             ale:sync_all_sinks(),
             %% TODO: somehow shutdown of ale may take up to about 5
             %% seconds. So we're just doing sync above and exit
             %%
             %% ?log_info("Stopped ns_server application"),
             %% error_logger:info_msg("Stopped ns_server application"),
             %% application:stop(os_mon),
             %% application:stop(sasl),
             %% application:stop(ale),

             ok
         catch T:E ->
                 Msg = io_lib:format("Got error trying to stop applications~n~p",
                                     [{T, E, erlang:get_stacktrace()}]),

                 (catch ?log_error(Msg)),
                 (catch error_logger:error_msg(Msg)),
                 {T, E}
         end,

    case RV of
        ok -> init:stop();
        X -> X
    end.

%% Call ns_bootstrap:stop on a remote node and exit with status indicating the
%% success of the call.
remote_stop(Node) ->
    RV = rpc:call(Node, ns_bootstrap, stop, []),
    ExitStatus = case RV of
                     ok -> 0;
                     Other ->
                         io:format("NOTE: shutdown failed~n~p~n", [Other]),
                         1
                 end,
    init:stop(ExitStatus).

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
