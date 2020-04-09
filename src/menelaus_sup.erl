%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2018 Couchbase, Inc.
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
%% @doc Supervisor for the menelaus application.

-module(menelaus_sup).
-author('Northscale <info@northscale.com>').

-behaviour(supervisor).
-behaviour(ns_log_categorizing).

-define(START_OK, 1).
-define(START_FAIL, 2).

%% External exports
-export([start_link/0,
         barrier_spec/1, barrier_notify_spec/1,
         barrier_start_link/0, barrier_notify/0, barrier_wait/0,
         restart_web_servers/0]).

%% supervisor callbacks
-export([init/1]).

-export([ns_log_cat/1, ns_log_code_string/1]).

-include("ns_common.hrl").

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    Result = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    WConfig = menelaus_web:webconfig(),
    Port = proplists:get_value(port, WConfig),
    case Result of
        {ok, _Pid} ->
            ?user_log(?START_OK,
                      "Couchbase Server has started on web port ~p on node ~p. Version: ~p.",
                      [Port, node(), ns_info:version(ns_server)]);
        _Err ->
            %% The exact error message is not logged here since this
            %% is a supervisor start, but a more helpful message
            %% should've been logged before.
            ?user_log(?START_FAIL,
                      "Couchbase Server has failed to start on web port ~p on node ~p. " ++
                          "Perhaps another process has taken port ~p already? " ++
                          "If so, please stop that process first before trying again.",
                      [Port, node(), Port])
    end,
    Result.

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

    Web = restartable:spec({menelaus_web,
                            {menelaus_web, start_link, []},
                            permanent, infinity, supervisor, dynamic}),

    Alerts = {menelaus_web_alerts_srv,
              {menelaus_web_alerts_srv, start_link, []},
              permanent, 5000, worker, dynamic},

    WebEvent = {menelaus_event,
                {menelaus_event, start_link, []},
                permanent, 5000, worker, dynamic},

    HotKeysKeeper = {hot_keys_keeper,
                     {hot_keys_keeper, start_link, []},
                     permanent, 5000, worker, dynamic},

    CBAuth = {menelaus_cbauth,
              {menelaus_cbauth, start_link, []},
              permanent, 1000, worker, dynamic},

    Processes = [UIAuth, ScramSha, LocalAuth, Cache, StatsGatherer, RpcEvents,
                 Web, WebEvent, HotKeysKeeper, Alerts, CBAuth],
    {ok, {{one_for_one, 10, 10}, Processes}}.

ns_log_cat(?START_OK) ->
    info;
ns_log_cat(?START_FAIL) ->
    crit.

ns_log_code_string(?START_OK) ->
    "web start ok";
ns_log_code_string(?START_FAIL) ->
    "web start fail".

restart_web_servers() ->
    restartable:restart(?MODULE, menelaus_web).
