%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc Supervisor for the menelaus application.

-module(menelaus_sup).
-author('Northscale <info@northscale.com>').

-behaviour(supervisor).

%% External exports
-export([start_link/0,
         barrier_spec/1, barrier_notify_spec/1,
         barrier_start_link/0, barrier_notify/0, barrier_wait/0]).

%% supervisor callbacks
-export([init/1]).

-include("ns_common.hrl").

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

barrier_spec(Id) ->
    {Id, {menelaus_sup, barrier_start_link, []},
     temporary, 1000, worker, [one_shot_barrier]}.

barrier_notify_spec(Id) ->
    {Id, {menelaus_sup, barrier_notify, []},
     temporary, 1000, worker, [menelaus_sup]}.

barrier_start_link() ->
    when_barrier_enabled(
      fun () ->
              one_shot_barrier:start_link(menelaus_barrier)
      end).

barrier_notify() ->
    when_barrier_enabled(
      fun () ->
              ok = one_shot_barrier:notify(menelaus_barrier),
              ignore
      end).

barrier_wait() ->
    ok = one_shot_barrier:wait(menelaus_barrier).

when_barrier_enabled(Fun) ->
    case ns_config:read_key_fast(menelaus_barrier_disabled, false) of
        true ->
            ignore;
        false ->
            Fun()
    end.

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    UIAuth = {menelaus_ui_auth,
              {menelaus_ui_auth, start_link, []},
              permanent, 5000, worker, dynamic},

    ScramSha = {scram_sha,
                {scram_sha, start_link, []},
                permanent, 5000, worker, dynamic},

    LocalAuth = {menelaus_local_auth,
                 {menelaus_local_auth, start_link, []},
                 permanent, 5000, worker, dynamic},

    Cache = {menelaus_web_cache,
             {menelaus_web_cache, start_link, []},
             permanent, 5000, worker, dynamic},

    StatsGatherer = {menelaus_stats_gatherer,
                     {menelaus_stats_gatherer, start_link, []},
                     permanent, 5000, worker, dynamic},

    RpcEvents = {json_rpc_events,
                 {gen_event, start_link, [{local, json_rpc_events}]},
                 permanent, 1000, worker, []},

    WebSup = {menelaus_web_sup,
              {menelaus_web_sup, start_link, []},
              permanent, infinity, supervisor, [menelaus_web_sup]},

    Alerts = {menelaus_web_alerts_srv,
              {menelaus_web_alerts_srv, start_link, []},
              permanent, 5000, worker, dynamic},

    HotKeysKeeper = {hot_keys_keeper,
                     {hot_keys_keeper, start_link, []},
                     permanent, 5000, worker, dynamic},

    CBAuth = {menelaus_cbauth,
              {menelaus_cbauth, start_link, []},
              permanent, 1000, worker, dynamic},

    Processes = [UIAuth, ScramSha, LocalAuth, Cache, StatsGatherer, RpcEvents,
                 WebSup, HotKeysKeeper, Alerts, CBAuth],
    {ok, {{one_for_one, 10, 10}, Processes}}.
