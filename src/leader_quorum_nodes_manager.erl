%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
-module(leader_quorum_nodes_manager).

-behaviour(gen_server2).

%% API
-export([start_link/0]).
-export([set_quorum_nodes/2]).
-export([config_upgrade_to_55/1]).

-export([get_quorum_nodes_unsafe/0, set_quorum_nodes_unsafe/1]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_info/2]).

-include("cut.hrl").
-include("ns_common.hrl").

-define(SERVER, ?MODULE).

-record(state, { quorum_nodes :: sets:set(node()) }).

start_link() ->
    leader_utils:ignore_if_new_orchestraction_disabled(
      fun () ->
              proc_lib:start_link(?MODULE, init, [[]])
      end).

set_quorum_nodes(Pid, QuorumNodes) ->
    gen_server2:call(Pid, {set_quorum_nodes, QuorumNodes}, infinity).

config_upgrade_to_55(Config) ->
    [{set, quorum_nodes,
      lists:usort(ns_cluster_membership:active_nodes(Config))}].

%% The following two functions are only exported for use by leader_activities
%% when new orchestration is disabled.
get_quorum_nodes_unsafe() ->
    get_quorum_nodes_from_config().

set_quorum_nodes_unsafe(QuorumNodes) ->
    set_quorum_nodes_in_config(lists:usort(QuorumNodes)).

%% gen_server2 callbacks
init([]) ->
    register(?SERVER, self()),
    proc_lib:init_ack({ok, self()}),

    leader_utils:wait_cluster_is_55(),
    enter_loop().

enter_loop() ->
    process_flag(priority, high),
    process_flag(trap_exit, true),

    pull_config(),

    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events,
                             case _ of
                                 {quorum_nodes, _} ->
                                     Self ! quorum_nodes_updated;
                                 _ ->
                                     ok
                             end),

    QuorumNodes = get_quorum_nodes(),
    State       = #state{quorum_nodes = QuorumNodes},

    register_with_leader_activities(QuorumNodes),
    gen_server2:enter_loop(?MODULE, [], State, {local, ?SERVER}).

handle_call({set_quorum_nodes, QuorumNodes}, From, State) ->
    {noreply, handle_set_quorum_nodes(QuorumNodes, From, State)};
handle_call(Request, From, State) ->
    ?log_error("Received unexpected call ~p from ~p when state is~n~p",
               [Request, From, State]),
    {reply, nack, State}.

handle_info(quorum_nodes_updated, State) ->
    {noreply, handle_quorum_nodes_updated(State)};
handle_info({'EXIT', _Pid, _Reason} = Exit, State) ->
    ?log_error("Received unexpected exit message ~p. Exiting"),
    {stop, {unexpected_exit, Exit}, State};
handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    {stop, {leader_activities_died, Pid, Reason}, State};
handle_info(Info, State) ->
    ?log_error("Received unexpected message ~p when state is~n~p",
               [Info, State]),
    {noreply, State}.

%% internal
register_with_leader_activities(Nodes) ->
    ok        = misc:wait_for_local_name(leader_activities, 1000),
    {ok, Pid} = leader_activities:register_quorum_nodes_manager(self(), Nodes),
    erlang:monitor(process, Pid).

pull_config() ->
    OtherNodes = ns_node_disco:nodes_actual_other(),
    ?log_debug("Attempting to pull config from nodes:~n~p", [OtherNodes]),

    Timeout = ?get_timeout(pull_config, 5000),
    case ns_config_rep:pull_remotes(OtherNodes, Timeout) of
        ok ->
            ?log_debug("Pulled config successfully.");
        Error ->
            ?log_warning("Failed to pull "
                         "config from some nodes: ~p. Continuing anyway.",
                         [Error])
    end.

handle_set_quorum_nodes(NewQuorumNodes, From,
                        #state{quorum_nodes = QuorumNodes} = State)
  when NewQuorumNodes =:= QuorumNodes ->
    gen_server2:reply(From, ok),
    State;
handle_set_quorum_nodes(NewQuorumNodes, From,
                        #state{quorum_nodes = QuorumNodes} = State) ->
    QuorumNodesList    = sets:to_list(QuorumNodes),
    NewQuorumNodesList = sets:to_list(NewQuorumNodes),

    ?log_info("Updating quorum nodes.~n"
              "Old quorum nodes: ~p~n"
              "New quorum nodes: ~p", [QuorumNodesList, NewQuorumNodesList]),
    set_quorum_nodes_in_config(NewQuorumNodesList),
    push_config(QuorumNodes, NewQuorumNodes),

    gen_server2:reply(From, ok),
    State#state{quorum_nodes = NewQuorumNodes}.

push_config(QuorumNodes, NewQuorumNodes) ->
    OtherNodes = functools:chain(sets:union(QuorumNodes, NewQuorumNodes),
                                 [sets:del_element(node(), _),
                                  sets:to_list(_)]),

    ?log_debug("Attempting to synchronize config to ~p", [OtherNodes]),

    %% The timeout is pretty short because some nodes might be
    %% unhealthy. Incidentally, they are the onces that are most likely to
    %% give us trouble. So in the absence of better alternative to ns_config,
    %% this is just a best-effort, and it's hard to guarantee anything here.
    SyncTimeout = ?get_timeout(config_push, 2000),
    ns_config_rep:ensure_config_seen_by_nodes(OtherNodes, SyncTimeout).

handle_quorum_nodes_updated(#state{quorum_nodes = QuorumNodes} = State) ->
    ?flush(quorum_nodes_updated),

    NewQuorumNodes = get_quorum_nodes(),
    case QuorumNodes =:= NewQuorumNodes of
        true ->
            State;
        false ->
            QuorumNodesList    = sets:to_list(QuorumNodes),
            NewQuorumNodesList = sets:to_list(NewQuorumNodes),

            ?log_warning("Somebody else updated the quorum nodes "
                         "when we are the master node.~n"
                         "Our quorum nodes: ~p~n"
                         "Their quorum nodes: ~p",
                         [QuorumNodesList, NewQuorumNodesList]),
            exit({quorum_nodes_update_conflict,
                  QuorumNodesList, NewQuorumNodesList})
    end.

set_quorum_nodes_in_config(QuorumNodes) ->
    ns_config:set(quorum_nodes, QuorumNodes).

get_quorum_nodes_from_config() ->
    {value, QuorumNodes} = ns_config:search(quorum_nodes),
    QuorumNodes.

get_quorum_nodes() ->
    sets:from_list(get_quorum_nodes_from_config()).
