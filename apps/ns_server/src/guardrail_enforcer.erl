%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(guardrail_enforcer).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("ns_test.hrl").
-endif.

-behaviour(gen_server).

-export([start_link/0, get_status/1, priority_order/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(NOTIFY_TIMEOUT, ?get_timeout(notify, 1000)).

%% Amount of time to wait between attempts to enforce the data ingress status
-define(RETRY_INTERVAL, ?get_param(retry_interval, 20000)).

-define(SERVER, ?MODULE).

-record(state, {
                statuses :: #{resource() => atom() | {atom(), retry}},
                timer_ref = undefined :: undefined | reference()
               }).

-type resource() :: guardrail_monitor:resource().
-type status() :: guardrail_monitor:status().

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get_status(resource()) -> undefined | ok | {error, atom()}.
get_status(Resource) ->
    case guardrail_monitor:is_enabled() of
        true ->
            gen_server:call(?SERVER, {status, Resource});
        false ->
            undefined
    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

init([]) ->
    ns_pubsub:subscribe_link(ns_config_events,
                             fun resource_status_change_callback/1),
    self() ! check_changes,
    {ok, #state{statuses = #{}}}.

handle_call({status, Resource}, _From, State) ->
    Status = case maps:get(Resource, State#state.statuses, ok) of
                 %% Ignore retry flag
                 {S, retry} -> S;
                 S -> S
             end,
    {reply, Status, State};
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(check_changes, #state{statuses = OldStatuses} = State0) ->
    %% Flush message and cancel any notify timer, to avoid unnecessarily
    %% checking and notifying
    ?flush(check_changes),

    State1 = State0#state{statuses = update_statuses(OldStatuses)},
    {noreply, maybe_retry(State1)};
handle_info(notify, #state{statuses = Statuses} = State) ->
    NewStatuses = maybe_notify_services(#{}, Statuses),
    {noreply, maybe_retry(State#state{statuses = NewStatuses})};
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
restart_timer(State0, Duration)->
    State1 = cancel_retry(State0),
    State1#state{timer_ref = erlang:send_after(Duration, self(), notify)}.

cancel_retry(#state{timer_ref = Ref} = State) when is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    %% Flush the message to avoid unnecessarily retrying when two messages come
    %% through close together
    ?flush(notify),
    State#state{timer_ref = undefined};
cancel_retry(State) ->
    State.

resource_status_change_callback({{node, _, resource_statuses}, _}) ->
    ?SERVER ! check_changes;
resource_status_change_callback(_) ->
    ok.

update_statuses(OldStatuses) ->
    GetNodeStatuses =
        fun (Node) ->
                ns_config:search_node_with_default(
                    Node, ns_config:latest(), resource_statuses, [])
        end,
    %% We are not interested in the status of inactive nodes, such as those that
    %% have failed over
    FlatStatuses = lists:flatmap(GetNodeStatuses,
                                 ns_cluster_membership:actual_active_nodes()),
    AggStatuses = maps:map(
                    fun (Resource, Values) ->
                            get_aggregated_status(Resource, Values)
                    end, maps:from_list(misc:keygroup(1, FlatStatuses))),
    maybe_notify_services(OldStatuses, AggStatuses).

%% Notify services of any changes
maybe_notify_services(AllOld, AllNew) ->
    ChangedStatuses = get_changes(AllOld, AllNew),
    ?log_debug("Changed Statuses: ~p", [ChangedStatuses]),
    maps:merge(AllNew, maps:map(fun notify_service/2, ChangedStatuses)).

get_changes(AllOld, AllNew) ->
    maps:filtermap(
      fun (_Key, {change, Old, Old}) ->
              %% Don't notify for unchanged
              false;
          (_Key, {change, _Old, {New, retry}}) ->
              %% Notify with new status if changed
              {true, New};
          (_Key, {change, _Old, New}) ->
              %% Notify with new status if changed
              {true, New};
          (Key, Status) ->
              case maps:get(Key, AllNew, undefined) of
                  undefined ->
                      %% Status has been removed, so we should notify with ok
                      {true, ok};
                  {New, retry} = Status ->
                      %% Notify with new status when there is no old status,
                      %% removing the 'retry' atom from the status
                      {true, New};
                  Status ->
                      %% Notify with new status when there is no old status
                      {true, Status}
              end
      end, maps:merge_with(
             fun (_Key, V1, V2) ->
                     {change, V1, V2}
             end, AllOld, AllNew)
     ).

%% To ensure that we keep trying to set the status after a failed call, we
%% will return {Status, retry} if we failed
-spec notify_service(resource(), status()) ->
          status() | {status(), retry}.
notify_service({bucket, Bucket}, Status) ->
    case ns_bucket:get_bucket(Bucket) of
        {ok, BucketConfig} ->
            ?log_debug("Notifying {bucket, ~p}: ~p", [Bucket, Status]),
            RV = janitor_agent:maybe_set_data_ingress(
                   Bucket, Status, ns_bucket:get_servers(BucketConfig)),
            case RV of
                ok ->
                    Status;
                {errors, BadReplies} ->
                    ?log_error("Failed to set ingress status for bucket ~p."
                               "~nBadReplies:~n~p", [Bucket, BadReplies]),
                    {Status, retry}
            end;
        not_present ->
            ?log_debug("Can't notify {bucket, ~p} (bucket not present): ~p",
                       [Bucket, Status]),
            ok
    end;
notify_service(_Resource, Status) ->
    Status.

-spec get_aggregated_status(resource(), [{node(), status()}]) -> status().
get_aggregated_status(Resource, NodeStatuses) ->
    resolve_status_conflict(Resource, lists:map(fun ({_, S}) -> S end,
                                                NodeStatuses)).

-spec priority_order(resource()) -> [status()].
priority_order({bucket, _}) ->
    [resident_ratio, data_size, disk_usage];
priority_order(disk) ->
    [maximum, critical, serious, warning];
priority_order(index) ->
    [critical, serious, warning].

-spec resolve_status_conflict([status()] | resource(), [status()]) ->
          status().
resolve_status_conflict(PriorityOrder, Reasons) when is_list(PriorityOrder) ->
    %% Return the highest priority reason
    lists:foldl(
      fun (Reason, ok) ->
              case lists:member(Reason, Reasons) of
                  true -> Reason;
                  false -> ok
              end;
          %% All other reasons are lower priority as we are folding in priority
          %% order
          (_, Reason) ->
              Reason
      end, ok, PriorityOrder);
resolve_status_conflict(Resource, Reasons) ->
    resolve_status_conflict(priority_order(Resource), Reasons).

maybe_retry(#state{statuses = NewStatuses} = State) ->
    case should_retry(NewStatuses) of
        false ->
            cancel_retry(State);
        true ->
            restart_timer(State, ?RETRY_INTERVAL)
    end.


-spec should_retry(#{resource() => atom() | {atom(), retry}}) -> boolean().
should_retry(Changes) ->
    lists:any(
      fun ({_Resource, {_Status, retry}}) -> true;
          (_) -> false
      end, maps:to_list(Changes)).

-ifdef(TEST).

test_handle_status(Response, Resource, Statuses) ->
    ?assertEqual({reply, Response, #state{statuses = Statuses}},
                 handle_call({status, Resource}, undefined,
                             #state{statuses = Statuses})).

get_status_test() ->
    test_handle_status(ok, {bucket, "bucket"}, #{}),

    test_handle_status(ok, {bucket, "bucket"}, #{{bucket, "bucket"} => ok}),

    %% Safely handle status awaiting retry
    test_handle_status(ok, {bucket, "bucket"},
                       #{{bucket, "bucket"} => {ok, retry}}),

    test_handle_status(resident_ratio, {bucket, "bucket"},
                       #{{bucket, "bucket"} => resident_ratio}),

    test_handle_status(data_size, {bucket, "bucket"},
                       #{{bucket, "bucket"} => data_size}).

get_changes_test() ->
    %% No statuses, no changes
    ?assertEqual(#{}, get_changes(#{}, #{})),
    %% Adding a status of ok
    ?assertEqual(#{test => ok}, get_changes(#{}, #{test => ok})),
    %% No change
    ?assertEqual(#{}, get_changes(#{test => ok}, #{test => ok})),
    %% Change from one status to another
    ?assertEqual(#{test => other},
                 get_changes(#{test => ok}, #{test => other})),
    %% Removing status should re-notify
    ?assertEqual(#{test => ok}, get_changes(#{test => other}, #{})),
    %% Removing ok will re-notify despite being unnecessary, to simplify logic
    ?assertEqual(#{test => ok}, get_changes(#{test => ok}, #{})),
    %% Changing the status to the same but with retry will count as a change
    ?assertEqual(#{test => ok},
                 get_changes(#{test => ok}, #{test => {ok, retry}})).

should_retry_test() ->
    ?assertEqual(false, should_retry(#{})),
    ?assertEqual(false, should_retry(#{test1 => ok, test2 => other})),
    ?assertEqual(true, should_retry(#{test1 => {ok, retry}, test2 => other})),
    ?assertEqual(true, should_retry(#{test1 => ok, test2 => {other, retry}})).

get_aggregated_status_test() ->
    ?assertEqual(ok,
                 get_aggregated_status(
                   {bucket, "bucket"}, [{node1, ok}])),

    ?assertEqual(resident_ratio,
                 get_aggregated_status({bucket, "bucket"},
                                       [{node1, resident_ratio}])),

    ?assertEqual(resident_ratio,
                 get_aggregated_status({bucket, "bucket"},
                                         [{node1, resident_ratio},
                                          {node2, ok}])),

    ?assertEqual(resident_ratio,
                 get_aggregated_status({bucket, "bucket"},
                                         [{node1, resident_ratio},
                                          {node2, data_size}])),

    ?assertEqual(data_size,
                 get_aggregated_status({bucket, "bucket"},
                                         [{node1, data_size},
                                          {node2, ok}])).

resolve_status_conflict_test() ->
    %% Generic status conflict logic
    ?assertEqual(ok, resolve_status_conflict([], [])),
    ?assertEqual(ok, resolve_status_conflict([], [ok])),
    ?assertEqual(ok, resolve_status_conflict([], [ok, other])),
    ?assertEqual(other1,
                 resolve_status_conflict([other1], [ok, other1])),
    ?assertEqual(other1,
                 resolve_status_conflict([other1, other2],
                                         [ok, other1, other2])),
    ?assertEqual(other2,
                 resolve_status_conflict([other1, other2],
                                         [ok, other2])),

    %% Bucket status conflicts
    ?assertEqual(ok, resolve_status_conflict({bucket, ""}, [])),
    ?assertEqual(ok, resolve_status_conflict({bucket, ""}, [ok])),
    ?assertEqual(resident_ratio,
                 resolve_status_conflict({bucket, ""}, [resident_ratio])),
    ?assertEqual(resident_ratio,
                 resolve_status_conflict({bucket, ""}, [ok, resident_ratio])),

    ?assertEqual(data_size,
                 resolve_status_conflict({bucket, ""},
                                           [data_size])),
    ?assertEqual(resident_ratio,
                 resolve_status_conflict({bucket, ""},
                                           [resident_ratio, data_size])).

-define(NODES, [node1, node2, node3]).

start_guardrail_enforcer() ->
    meck:expect(ns_cluster_membership, actual_active_nodes,
                fun () -> ?NODES end),
    meck:expect(ns_pubsub, subscribe_link,
                fun (ns_config_events, _) -> ok end),
    meck:expect(guardrail_monitor, is_enabled, ?cut(false)),
    %% Meck ns_config:search_node_with_default so that we don't have a race
    %% between start_link and test_update_statuses at the start of
    %% update_statuses_t
    meck:expect(ns_config, search_node_with_default,
                fun (_, _, resource_statuses, Default) ->
                        Default
                end),
    meck:expect(ns_config, search_node_with_default,
                fun ({guardrail_enforcer, retry_interval}, Default) ->
                        Default
                end),
    start_link().

start_janitor_agent() ->
    meck:expect(janitor_agent_sup, get_registry_pid,
                fun (_) -> self() end),
    BucketCfg = [{type, membase},
                 {storage_mode, magma},
                 {servers, ?NODES}],
    meck:expect(ns_bucket, get_bucket,
                fun ("bucket1") -> {ok, BucketCfg} end),
    meck:expect(ns_bucket, get_bucket,
                fun ("bucket1", _) -> {ok, BucketCfg} end),
    meck:expect(ns_bucket, get_snapshot,
                fun (_) -> fake_chronicle_snapshot end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) -> Default end),
    meck:expect(dcp_sup, nuke,
                fun (_) -> ok end),
    meck:expect(ns_storage_conf, this_node_bucket_dbdir,
                fun (_, _) -> "" end),
    janitor_agent:start_link("bucket1").

basic_test_setup() ->
    {ok, _} = start_guardrail_enforcer(),
    {ok, JanitorPid} = start_janitor_agent(),
    JanitorPid.

test_update_statuses(NewNodes) ->
    meck:expect(ns_config, search_node_with_default,
                fun (Node, _, resource_statuses, Default) ->
                        maps:get(Node, NewNodes, Default)
                end),
    ?SERVER ! check_changes.

%% Since we have three nodes that will all be called each time, we increment by
%% 3 each time we expect the data ingress status to be set
-define(NODE_CALLS(N), 3 * N).

update_statuses_t() ->
    test_update_statuses(#{}),
    ?assertEqual(undefined, get_status(test)),
    meck:expect(guardrail_monitor, is_enabled, ?cut(true)),
    ?assertEqual(ok, get_status(test)),

    meck:expect(janitor_agent, maybe_set_data_ingress,
                fun (Bucket, Status, S) ->
                        %% Replace fake nodes with actual node so that it can
                        %% be called
                        meck:passthrough([Bucket, Status,
                                          lists:map(fun (_) -> node() end, S)])
                end),
    meck:expect(ns_memcached, set_data_ingress,
                fun (Bucket, Status) ->
                        ?log_debug("Setting ingress status for '~p' to ~p",
                                   [Bucket, Status])
                end),
    ?assertEqual(?NODE_CALLS(0),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", ok])),

    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, ok}]}),
    ?assertEqual(ok, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", ok])),

    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, ok}]}),
    ?assertEqual(ok, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", ok])),

    ?assertEqual(?NODE_CALLS(0),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Don't try to set ingress when status unchanged
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Don't try to set ingress when status unchanged
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}],
                           node2 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Don't set ingress when only one becomes ok
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}],
                           node2 => [{{bucket, "bucket1"}, ok}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Update ingress when all nodes become ok
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, ok}],
                           node2 => [{{bucket, "bucket1"}, ok}]}),
    ?assertEqual(ok, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(2),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", ok])),

    %% Notify with ok when the status disappears
    test_update_statuses(#{node1 => []}),
    ?assertEqual(ok, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(3),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", ok])),

    meck:expect(ns_memcached, set_data_ingress,
                fun (Bucket, Status) ->
                        ?log_debug("Fail to set ingress status for '~p' to ~p",
                                   [Bucket, Status]),
                        error
                end),
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(2),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    meck:expect(ns_memcached, set_data_ingress,
                fun (Bucket, Status) ->
                        ?log_debug("Fail to set ingress status for '~p' to ~p",
                                   [Bucket, Status]),
                        throw(error)
                end),
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(3),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% When maybe_set_data_ingress has failed, we should retry even though the
    %% status has not changed
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(4),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    meck:expect(ns_memcached, set_data_ingress,
                fun (Bucket, Status) ->
                        ?log_debug("Setting ingress status for '~p' to ~p",
                                   [Bucket, Status])
                end),
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(5),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% When set data ingress starts succeeding again, we should not keep
    %% retrying
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(5),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Update status to data_size
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, data_size}]}),
    ?assertEqual(data_size, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", data_size])),

    %% Update both data_size and resident_ratio at same time
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, data_size}],
                           node2 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(6),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Update status to disk_usage
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, disk_usage}]}),
    ?assertEqual(disk_usage, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(1),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", disk_usage])),

    %% Update disk_usage, data_size and resident_ratio at same time
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, disk_usage}],
                           node2 => [{{bucket, "bucket1"}, data_size}],
                           node3 => [{{bucket, "bucket1"}, resident_ratio}]}),
    ?assertEqual(resident_ratio, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(7),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", resident_ratio])),

    %% Update disk_usage and data_size at same time
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, disk_usage}],
                           node2 => [{{bucket, "bucket1"}, data_size}]}),
    ?assertEqual(data_size, get_status({bucket, "bucket1"})),
    ?assertEqual(?NODE_CALLS(2),
                 meck:num_calls(ns_memcached, set_data_ingress,
                                ["bucket1", data_size])).

retry_notifying_service_t() ->
    meck:expect(ns_config, search_node_with_default,
                fun ({guardrail_enforcer, retry_interval}, _Default) ->
                        %% Immediately retry, so that we can immediately test
                        %% that the retry occurs
                        0
                end),
    %% Fail precisely 2 times to set the data ingress, then succeed
    meck:expect(janitor_agent, maybe_set_data_ingress,
                ["bucket1", ok, '_'],
                meck:seq([fun (_, _, _) -> {errors, []} end,
                          fun (_, _, _) -> {errors, []} end,
                          fun (_, _, _) -> ok end])),

    %% Update a resource status
    test_update_statuses(#{node1 => [{{bucket, "bucket1"}, ok}]}),

    %% Since the retry_interval is 0 and we expect to fail 2 times,
    %% we should see 3 attempt to set the data ingress
    meck:wait(3, janitor_agent, maybe_set_data_ingress,
              ["bucket1", ok, '_'], 60000),

    %% After the 3rd call, we should see the timer_ref undefined, as no further
    %% attempts are required
    ?assertEqual(undefined,
                 (sys:get_state(guardrail_enforcer))#state.timer_ref).

basic_test_teardown(JanitorPid) ->
    gen_server:stop(?SERVER),
    gen_server:stop(JanitorPid),
    meck:unload().

basic_test_() ->
    %% We use foreach to ensure that we have a new gen_server for each test
    {foreach,
     fun basic_test_setup/0,
     fun basic_test_teardown/1,
     [{"update statuses test", fun () -> update_statuses_t() end},
      {"retry notifying service test",
       fun () -> retry_notifying_service_t() end}]}.
-endif.
