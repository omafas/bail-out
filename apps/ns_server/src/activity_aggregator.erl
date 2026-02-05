%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(activity_aggregator).


-include("ns_common.hrl").
-include("rbac.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).


%% 120s should be enough that we never hit it when the 60s timeout for an
%% individual rpc call is reached
-define(PARALLEL_RPC_TIMEOUT, ?get_timeout(parallel_rpc_timeout, 120000)).
%% Amount of time to wait between refreshes (15 mins)
-define(CHECK_INTERVAL, ?get_param(check_interval, 15 * 60 * 1000)).
-define(SERVER, ?MODULE).
-define(CONFIG_KEY, user_activity).

-record(state, {refresh_timer_ref = undefined :: undefined | reference()}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ns_pubsub:subscribe_link(user_storage_events, fun user_storage_event/1),
    ns_pubsub:subscribe_link(ns_config_events, fun config_event/1),
    {ok, restart_refresh_timer(#state{})}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(refresh, State = #state{}) ->
    Config = menelaus_web_activity:get_config(),
    case menelaus_web_activity:is_enabled(Config) of
        false -> ok;
        true -> update_activity()
    end,
    %% Reminder to refresh again after the check interval
    {noreply, restart_refresh_timer(State)};
handle_info(clear_activity_for_untracked_users, State = #state{}) ->
    misc:flush(clear_activity_for_untracked_users),
    Config = menelaus_web_activity:get_config(),
    case menelaus_web_activity:is_enabled(Config) of
        false ->
            menelaus_users:delete_all_activity();
        true ->
            IsTracked = activity_tracker:is_user_covered(_, Config),
            menelaus_users:clear_activity_for_untracked_users(IsTracked)
    end,
    {noreply, State};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%% We need to make sure there is only one timer at any given moment, otherwise
%% the system would be fragile to future changes or diag/evals
restart_refresh_timer(#state{refresh_timer_ref = Ref} = State)
  when is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    restart_refresh_timer(State#state{refresh_timer_ref = undefined});
restart_refresh_timer(#state{refresh_timer_ref = undefined} = State) ->
    State#state{refresh_timer_ref = erlang:send_after(?CHECK_INTERVAL, self(),
                                                      refresh)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

user_storage_event({user_version, _}) ->
    ?SERVER ! clear_activity_for_untracked_users;
user_storage_event(_) ->
    ok.

config_event({Key, _}) ->
    case menelaus_web_activity:is_config_key(Key) of
        true -> ?SERVER ! clear_activity_for_untracked_users;
        false -> ok
    end;
config_event(_) ->
    ok.

update_activity() ->
    AllActivity =
        misc:parallel_map(
          fun (Node) ->
                  {Node, catch activity_tracker:get_activity_from_node(Node)}
          end, ns_node_disco:nodes_actual(), ?PARALLEL_RPC_TIMEOUT),
    AggregatedActivity = aggregate_activity(AllActivity),
    menelaus_users:store_activity(AggregatedActivity).

aggregate_activity(Activity) ->
    lists:foldl(
      fun ({_Node, NodeActivity}, Acc0) when is_list(NodeActivity) ->
              lists:foldl(
                fun ({Identity, T0}, Acc1) ->
                        Latest =
                            case maps:find(Identity, Acc1) of
                                {ok, T1} -> max(T0, T1);
                                error -> T0
                            end,
                        Acc1#{Identity => Latest}
                end, Acc0, NodeActivity);
          ({Node, Error}, Acc0) ->
              ?log_error("Couldn't get activity from node ~p, got ~p",
                         [Node, Error]),
              Acc0
      end, #{}, Activity).

%%%===================================================================
%%% Tests
%%%===================================================================


-ifdef(TEST).

-define(TEST_TIMEOUT, 5_000).

setup() ->
    fake_ns_config:setup(),

    %% Used by activity_tracker
    gen_event:start_link({local, user_storage_events}),

    %% Configure activity before starting activity_tracker, to immediately
    %% triggering
    fake_ns_config:update_snapshot(user_activity, [{enabled, true}]),

    activity_tracker:start_link(),

    %% Only have one node, to avoid unneeded complexity
    meck:expect(ns_node_disco, nodes_actual,
                fun () -> [node()] end),
    meck:expect(replicated_dets, change_multiple, fun (_, _) -> ok end),
    meck:expect(replicated_dets, get,
                fun(users_storage, {activity, _Id}) -> false end),

    start_link().

teardown(_) ->
    gen_server:stop(?SERVER),
    gen_server:stop(activity_tracker),
    fake_ns_config:teardown(),
    meck:unload().

aggregate_activity_test__() ->
    %% Reset meck module so that only calls from now on are counted
    meck:reset(replicated_dets),
    %% Add activity in activity_tracker
    ets:insert(activity_tracker, {user, time}),
    Pid = self(),
    ReportDone =
        fun() ->
                fake_ns_config:update_snapshot({?MODULE, check_interval},
                                               1_000_000),
                Pid ! done
        end,
    meck:expect(
      replicated_dets, change_multiple,
      fun (users_storage, Changes) ->
              ?assertEqual([{set, {activity, user}, time}], Changes),
              %% Update the meck so that we detect the second refresh, and
              %% ensure that the next refresh can detect that the activity
              %% time is the same
              meck:expect(replicated_dets, get,
                          fun(users_storage, {activity, user}) ->
                                  ReportDone(),
                                  {{activity, user}, time}
                          end),
              ok
      end),
    %% Set 0 timeout so next refresh is made immediately
    fake_ns_config:update_snapshot({?MODULE, check_interval}, 0),
    ?SERVER ! refresh,
    %% Wait for two refreshes
    receive
        done -> ok
    after ?TEST_TIMEOUT ->
            error(fail)
    end,

    %% Only the first refresh should cause an update to the activity entries
    ?assertEqual(1, meck:num_calls(replicated_dets, change_multiple,
                                   ['_', '_'])).

error_fetching_activity_test__() ->
    %% Reset meck module so that only calls from now on are counted
    meck:reset(replicated_dets),
    Pid = self(),
    meck:expect(
      activity_tracker, get_activity_from_node, ['_'],
      meck:seq(
        [fun (_Node) -> error(err) end,
         %% Reset check interval after second check, to make sure
         %% that the scheduled refresh message is sent/received
         fun (_Node) ->
                 fake_ns_config:update_snapshot({?MODULE, check_interval},
                                                1_000_000),
                 Pid ! done,
                 []
         end])),
    %% Set 0 timeout so next refresh is made immediately
    fake_ns_config:update_snapshot({?MODULE, check_interval}, 0),
    ?SERVER ! refresh,
    %% Wait for two refreshes
    receive
        done -> ok
    after ?TEST_TIMEOUT ->
            error(fail)
    end,

    ?assertEqual(2, meck:num_calls(activity_tracker, get_activity_from_node,
                                   ['_'])),

    %% Neither refresh should require updating any activity entries
    ?assertEqual(0, meck:num_calls(replicated_dets, change_multiple,
                                   ['_', '_'])).

all_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [fun aggregate_activity_test__/0,
      fun error_fetching_activity_test__/0]}.

-endif.
