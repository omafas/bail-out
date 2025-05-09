%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(app_telemetry_aggregator).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-export([start_link/0, update_remote_cache/3, flush_remote_metrics/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% rpc called from other nodes
-export([flush_metrics/1]).

-define(SERVER, ?MODULE).
-define(REMOTE_TABLE, app_telemetry_remote_cache).

-define(FLUSH_INTERVAL, ?get_param(flush_interval, 60000)).
-define(FLUSH_TIMEOUT, ?get_timeout(flush_timeout, 60000)).

-record(state,
        {timer_ref :: undefined | reference()}).

update_remote_cache(Node, Metric, Value) ->
    Key = {Node, Metric},
    case ets:whereis(?REMOTE_TABLE) of
        undefined ->
            ok;
        Tid ->
            case ets:member(Tid, Key) of
                false ->
                    ets:insert(Tid, {Key, Value});
                true ->
                    ets:update_counter(Tid, Key, Value)
            end
    end.

-spec flush_remote_metrics() -> flush_remote_metrics.
flush_remote_metrics() ->
    ?SERVER ! flush_remote_metrics.

-spec flush_metrics([{{binary(), [{binary(), binary()}]}, integer()}]) -> ok.
flush_metrics(Metrics) ->
    lists:foreach(
        fun ({Metric, Value}) ->
            ns_server_stats:notify_counter_raw(Metric, Value)
        end, Metrics).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ets:new(?REMOTE_TABLE, [named_table, set, public]),
    {ok, restart_timer(#state{})}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(flush_remote_metrics, State0 = #state{}) ->
    ?flush(flush_remote_metrics),
    %% Flush again automatically if the scraper doesn't complete again within
    %% the flush interval
    State1 = restart_timer(State0),
    NodeMetrics =
        ets:foldl(
          fun ({{Node, Metric} = Key, _}, Acc) ->
                  [{Key, Value}] = ets:take(?REMOTE_TABLE, Key),
                  Entry = {Metric, Value},
                  maps:update_with(Node, [Entry | _], [Entry], Acc)
          end, #{}, ?REMOTE_TABLE),

    misc:parallel_map(
      fun ({Node, Metrics}) ->
              case rpc:call(Node, ?MODULE, flush_metrics, [Metrics],
                            ?FLUSH_TIMEOUT) of
                  ok ->
                      ok;
                  Error ->
                      ?log_warning("Failed to flush metrics to Node ~p, with "
                                   "error ~p", [Node, Error])
              end
      end,
      maps:to_list(NodeMetrics),
      %% No timeout as we already have per-node timeouts
      infinity),
    {noreply, State1};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% We need to make sure there is only one timer at any given moment, otherwise
%% the system would be fragile to future changes or diag/evals
restart_timer(#state{timer_ref = Ref} = State) when is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    restart_timer(State#state{timer_ref = undefined});
restart_timer(#state{timer_ref = undefined} = State) ->
    State#state{timer_ref = erlang:send_after(?FLUSH_INTERVAL, self(),
                                              flush_remote_metrics)}.
