%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(rebalance).

-export([running/0,
         running/1,
         type/0,
         status_uuid/0,
         status/0,
         reset_status/1,
         set_status/3,
         start/3,
         stop/1,
         progress/0,
         progress/1]).

-include("ns_common.hrl").

rebalancer(Config) ->
    chronicle_compat:get(Config, rebalancer_pid, #{default => undefined}).

running() ->
    running(direct).

running(Snapshot) ->
    rebalancer(Snapshot) =/= undefined.

type() ->
    chronicle_compat:get(rebalance_type, #{default => rebalance}).

status_uuid() ->
    chronicle_compat:get(rebalance_status_uuid, #{default => undefined}).

status() ->
    status(direct).

status(Snapshot) ->
    chronicle_compat:get(Snapshot, rebalance_status, #{default => undefined}).

reset_status(Fn) ->
    reset_status(Fn, chronicle_compat:backend()).

reset_status(Fn, ns_config) ->
    ok =
        ns_config:update(
          fun ({rebalance_status, Value}) ->
                  case Value of
                      running ->
                          NewValue = Fn(),
                          {update, {rebalance_status, NewValue}};
                      _ ->
                          skip
                  end;
              ({rebalancer_pid, Pid}) when is_pid(Pid) ->
                  {update, {rebalancer_pid, undefined}};
              (_Other) ->
                  skip
          end);
reset_status(Fn, chronicle) ->
    RV =
        chronicle_kv:transaction(
          kv, [rebalance_status],
          fun (Snapshot) ->
                  case status(Snapshot) of
                      running ->
                          {commit, [{set, rebalance_status, Fn()},
                                    {delete, rebalancer_pid}]};
                      _ ->
                          {abort, skip}
                  end
          end, #{}),
    case RV of
        skip ->
            ok;
        {ok, _} ->
            ok
    end.

set_status(Type, Status, Pid) ->
    ok = chronicle_compat:set_multiple(
           [{rebalance_status, Status},
            {rebalance_status_uuid, couch_uuids:random()},
            {rebalancer_pid, Pid},
            {rebalance_type, Type}]).

start(KnownNodes, EjectedNodes, DeltaRecoveryBuckets) ->
    ns_orchestrator:start_rebalance(KnownNodes, EjectedNodes,
                                    DeltaRecoveryBuckets).

stop(AllowUnsafe) ->
    %% NOTE: this is inherently raceful. But race is tiny and largely
    %% harmless. So we KISS instead.
    case can_stop(AllowUnsafe) of
        true ->
            ns_orchestrator:stop_rebalance();
        false ->
            unsafe
    end.

can_stop(true) ->
    true;
can_stop(false) ->
    case rebalancer(direct) of
        undefined ->
            true;
        Pid ->
            node(Pid) =:= mb_master:master_node()
    end.

-spec progress() -> {running, [{atom(), float()}]} |
                    not_running |
                    {error, timeout}.
progress() ->
    progress(?REBALANCE_OBSERVER_TASK_DEFAULT_TIMEOUT).

-spec progress(non_neg_integer()) -> {running, [{atom(), float()}]} |
                                     not_running |
                                     {error, timeout}.
progress(Timeout) ->
    case running() of
        false ->
            not_running;
        true ->
            case ns_rebalance_observer:get_aggregated_progress(Timeout) of
                {ok, Aggr} ->
                    {running, Aggr};
                Err ->
                    ?log_error("Couldn't reach ns_rebalance_observer"),
                    Err
            end
    end.
