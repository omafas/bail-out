%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(chronicle_local).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("cut.hrl").
-include_lib("ale/include/ale.hrl").

-export([start_link/0,
         init/1,
         handle_call/3,
         prepare_join/1,
         join_cluster/1,
         leave_cluster/0,
         rename/1,
         get_snapshot/1,
         sync/0]).

%% exported callbacks used by chronicle
-export([log/4, report_stats/1]).

%% exported for log formatting
-export([format_msg/2, format_time/1]).

-define(CALL_TIMEOUT, ?get_timeout(call, 180000)).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    application:set_env(chronicle, data_dir,
                        path_config:component_path(data, "config")),
    application:set_env(chronicle, logger_function, {?MODULE, log}),

    case misc:get_env_default(enable_chronicle_stats, true) of
        true ->
            application:set_env(chronicle,
                                stats_function, {?MODULE, report_stats});
        false ->
            ok
    end,

    ?log_debug("Ensure chronicle is started"),
    ok = application:ensure_started(chronicle, permanent),

    ChronicleState = chronicle:get_system_state(),
    ?log_debug("Chronicle state is: ~p", [ChronicleState]),

    case ChronicleState of
        not_provisioned ->
            ok = provision();
        _ ->
            ok
    end,

    case dist_manager:need_fixup() of
        {true, OldNode} ->
            ?log_info("Aborted rename from ~p was detected", [OldNode]),
            handle_rename(OldNode);
        false ->
            ok
    end,
    {ok, []}.

handle_call({prepare_join, Info}, _From, State) ->
    ?log_debug("Wiping chronicle before prepare join."),
    ok = chronicle:wipe(),
    case Info of
        undefined ->
            ?log_debug("Joining not chronicle enabled cluster"),
            ok = provision();
        _ ->
            ?log_debug("Prepare join. Info: ~p", [Info]),
            ok = chronicle:prepare_join(Info)
    end,
    {reply, ok, State};
handle_call({join_cluster, Info}, _From, State) ->
    ?log_debug("Joining cluster. Info: ~p", [Info]),
    ok = chronicle:join_cluster(Info),
    {reply, ok, State};
handle_call(leave_cluster, _From, State) ->
    handle_leave(),
    {reply, ok, State};
handle_call({rename, OldNode}, _From, State) ->
    handle_rename(OldNode),
    {reply, ok, State};
handle_call(get_snapshot, _From, Pid) ->
    RV =
        try chronicle_kv:get_full_snapshot(kv) of
            {ok, {Snapshot, _}} ->
                {ok, Snapshot}
        catch T:E:S ->
                ?log_debug("Unable to obtain chronicle snapshot:~n~p",
                           [{T, E, S}]),
                {error, cannot_get_snapshot}
        end,
    {reply, RV, Pid};
handle_call(sync, _From, State) ->
    {reply, ok, State}.

leave_cluster() ->
    gen_server2:call(?MODULE, leave_cluster, ?CALL_TIMEOUT).

prepare_join(Info) ->
    gen_server2:call(?MODULE, {prepare_join, Info}, ?CALL_TIMEOUT).

join_cluster(undefined) ->
    ok;
join_cluster(Info) ->
    gen_server2:call(?MODULE, {join_cluster, Info}, ?CALL_TIMEOUT).

rename(OldNode) ->
    gen_server2:call(?MODULE, {rename, OldNode}).

get_snapshot(Node) ->
    {ok, Snapshot} = gen_server2:call({?MODULE, Node}, get_snapshot,
                                      ?CALL_TIMEOUT),
    Snapshot.

sync() ->
    gen_server2:call(?MODULE, sync, ?CALL_TIMEOUT).

provision() ->
    ?log_debug("Provision chronicle on this node"),
    chronicle:provision([{kv, chronicle_kv, []}]).

handle_leave() ->
    ?log_debug("Leaving cluster"),
    ok = chronicle:wipe(),
    ok = provision().

handle_rename(OldNode) ->
    NewNode = node(),
    ?log_debug("Handle renaming from ~p to ~p", [OldNode, NewNode]),
    ok = chronicle:reprovision(),

    {ok, _} =
        chronicle_kv:rewrite(
          kv,
          fun (K, V) ->
                  case {misc:rewrite_value(OldNode, NewNode, K),
                        misc:rewrite_value(OldNode, NewNode, V)} of
                      {K, V} ->
                          keep;
                      {NewK, NewV} ->
                          {update, NewK, NewV}
                  end
          end).

log(Level, Fmt, Args, Info) ->
    AleLevel = case Level of
                   warning -> warn;
                   _ -> Level
               end,
    ale:xlog(?CHRONICLE_ALE_LOGGER, AleLevel, Info, Fmt, Args).

format_time(Time) ->
    ale_default_formatter:format_time(Time).

format_msg(#log_info{user_data = #{module := M, function := F, line := L}}
           = Info, UserMsg) ->
    ale_default_formatter:format_msg(
      Info#log_info{module = M, function = F, line = L}, UserMsg).

report_stats({histo, Metric, Max, Unit, Value}) ->
    ns_server_stats:notify_histogram(Metric, Max, Unit, Value);
report_stats({counter, Metric, By}) ->
    ns_server_stats:notify_counter(Metric, By);
report_stats({gauge, Metric, Value}) ->
    ns_server_stats:notify_gauge(Metric, Value).
