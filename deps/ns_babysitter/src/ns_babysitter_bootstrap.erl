%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ns_babysitter_bootstrap).

-export([start/0, stop/0, get_quick_stop/0, remote_stop/1]).

-include("ns_common.hrl").

start() ->
    try
        ok = application:start(ale),
        ok = application:start(sasl),
        ok = application:start(ns_babysitter, permanent),
        (catch ?log_info("~s: babysitter has started", [os:getpid()]))
    catch T:E ->
            timer:sleep(500),
            erlang:T(E)
    end.

stop() ->
    %% This is typically called via "couchbase-server -k" (most commonly
    %% when a user does "service couchbase-server stop").  The init:stop
    %% smoothly takes down all applications, unloads code, etc.
    (catch ?log_info("~s: got shutdown request. Terminating.", [os:getpid()])),
    init:stop().

remote_stop(Node) ->
    RV = rpc:call(Node, ns_babysitter_bootstrap, stop, []),
    ExitStatus = case RV of
                     ok -> 0;
                     Other ->
                         io:format("NOTE: shutdown failed~n~p~n", [Other]),
                         1
                 end,
    init:stop(ExitStatus).

get_quick_stop() ->
    fun quick_stop/0.

quick_stop() ->
    application:set_env(ns_babysitter, port_shutdown_command, "die!"),
    stop().
