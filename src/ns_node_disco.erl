%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
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
-module(ns_node_disco).

-behaviour(gen_server).
-behavior(ns_log_categorizing).

-include("ns_common.hrl").

-define(PING_FREQ, 60000).
-define(SYNC_TIMEOUT, 5000).

-define(NODE_UP, 4).
-define(NODE_DOWN, 5).

%% API
-export([start_link/0,
         nodes_wanted/0, nodes_wanted/1,
         nodes_wanted_updated/0,
         erlang_visible_nodes/0,
         random_node/0,
         nodes_actual/0,
         nodes_actual_other/0,
         register_node_renaming_txn/1,
         couchdb_node/0,
         local_sub_nodes/0,
         ns_server_node/0,
         only_live_nodes/1]).

-export([ns_log_cat/1, ns_log_code_string/1]).

%% gen_server

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          nodes :: [node()],
          we_were_shunned = false :: boolean(),
          node_renaming_txn_mref :: undefined | reference()
         }).

% Node Discovery and monitoring.
%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Returns all nodes that erlang distribution system sees.
erlang_visible_nodes() ->
    lists:sort(nodes([this, visible])).

% Returns a subset of the nodes_wanted() that we see.
nodes_actual() ->
    only_live_nodes(nodes_wanted()).

only_live_nodes(Nodes) ->
    only_live_nodes(erlang_visible_nodes(), Nodes).

only_live_nodes(Current, Wanted) ->
    ordsets:intersection(ordsets:from_list(Current),
                         ordsets:from_list(Wanted)).

couchdb_node() ->
    case application:get_env(ns_server, ns_couchdb_node) of
        {ok, Node} ->
            Node;
        undefined ->
            node()
    end.

ns_server_node() ->
    case application:get_env(ns_couchdb, ns_server_node) of
        {ok, Node} ->
            Node;
        undefined ->
            node()
    end.

local_sub_nodes() ->
    only_live_nodes([couchdb_node()], lists:sort(nodes([hidden]))).

register_node_renaming_txn(Pid) ->
    gen_server:call(?MODULE, {register_node_renaming_txn, Pid}).

random_node() ->
    WorkingNodes = lists:filter(fun(N) ->
                                        net_adm:ping(N) == pong
                                end,
                                nodes_wanted() -- [node()]),

    case WorkingNodes of
        [] -> exit(nonode);
        [N|_] -> N
    end.

% Returns nodes_actual(), but with self node() filtered out.
nodes_actual_other() ->
    lists:subtract(nodes_actual(), [node()]).

nodes_wanted() ->
    nodes_wanted(ns_config:latest()).

nodes_wanted(Config) ->
    lists:usort(ns_config:search(Config, nodes_wanted, [])).

% API's used as callbacks that are invoked when ns_config
% keys have changed.

nodes_wanted_updated() ->
    gen_server:cast(?MODULE, nodes_wanted_updated).

%% gen_server implementation

init([]) ->
    ?log_debug("Initting ns_node_disco with ~p", [nodes()]),
    %% Proactively run one round of reconfiguration update.
    %% It may take longer than SYNC_TIMEOUT to complete if nodes are down.
    misc:wait_for_process(do_nodes_wanted_updated(nodes_wanted()),
                          ?SYNC_TIMEOUT),
    % Register for nodeup/down messages as handle_info callbacks.
    ok = net_kernel:monitor_nodes(true, [nodedown_reason]),
    timer2:send_interval(?PING_FREQ, ping_all),
    self() ! notify_clients,
    % Track the last list of actual ndoes.
    {ok, #state{nodes = []}}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_cast(nodes_wanted_updated, State) ->
    self() ! notify_clients,
    do_nodes_wanted_updated(nodes_wanted()),
    {noreply, State};

handle_cast(we_were_shunned, #state{we_were_shunned = true} = State) ->
    {noreply, State};

handle_cast({we_were_shunned, NodeList}, State) ->
    %% Must have been shunned while we were down. Leave the cluster.
    ?log_info("We've been shunned (nodes_wanted = ~p). "
              "Leaving cluster.", [NodeList]),
    ns_cluster:leave_async(),
    {noreply, State#state{we_were_shunned = true}};

handle_cast(_Msg, State)       -> {noreply, State}.

% Read from ns_config nodes_wanted, and add ourselves to the
% nodes_wanted list, if not already there.

handle_call({register_node_renaming_txn, Pid}, _From, State) ->
    case State of
        #state{node_renaming_txn_mref = undefined} ->
            MRef = erlang:monitor(process, Pid),
            NewState = State#state{node_renaming_txn_mref = MRef},
            {reply, ok, NewState};
        _ ->
            {reply, already_doing_renaming, State}
    end;

handle_call(Msg, _From, State) ->
    ?log_warning("Unhandled ~p call: ~p", [?MODULE, Msg]),
    {reply, error, State}.

handle_info({'DOWN', MRef, _, _, _}, #state{node_renaming_txn_mref = MRef} = State) ->
    self() ! notify_clients,
    {noreply, State#state{node_renaming_txn_mref = undefined}};
handle_info({nodeup, Node, InfoList}, State) ->
    ?user_log(?NODE_UP, "Node ~p saw that node ~p came up. Tags: ~p",
              [node(), Node, InfoList]),
    self() ! notify_clients,
    {noreply, State};

handle_info({nodedown, Node, InfoList}, State) ->
    ?user_log(?NODE_DOWN, "Node ~p saw that node ~p went down. Details: ~p",
              [node(), Node, InfoList]),
    self() ! notify_clients,
    {noreply, State};

handle_info(notify_clients, State) ->
    misc:flush(notify_clients),
    State2 = do_notify(State),
    {noreply, State2};

handle_info(ping_all, State) ->
    misc:flush(ping_all),
    spawn_link(fun ping_all/0),
    {noreply, State};

handle_info(Msg, State) ->
    ?log_warning("dropping unknown message on the floor:~n~p", [Msg]),
    {noreply, State}.

% -----------------------------------------------------------

%% The core of what happens when nodelists change
%% only used by do_nodes_wanted_updated
do_nodes_wanted_updated_fun(NodeListIn) ->
    {ok, _Cookie} = ns_cookie_manager:cookie_sync(),
    NodeList = lists:usort(NodeListIn),
    ?log_debug("ns_node_disco: nodes_wanted updated: ~p, with cookie: ~p",
               [NodeList, ns_cookie_manager:sanitize_cookie(erlang:get_cookie())]),
    PongList = lists:filter(fun(N) ->
                                    net_adm:ping(N) == pong
                            end,
                            NodeList),
    ?log_debug("ns_node_disco: nodes_wanted pong: ~p, with cookie: ~p",
               [PongList, ns_cookie_manager:sanitize_cookie(erlang:get_cookie())]),
    case lists:member(node(), NodeList) of
        true ->
            ok;
        false ->
            gen_server:cast(ns_node_disco, {we_were_shunned, NodeList})
    end,
    ok.

%% Run do_nodes_wanted_updated_fun in a process, return the Pid.
do_nodes_wanted_updated(NodeListIn) ->
    spawn(fun() -> do_nodes_wanted_updated_fun(NodeListIn) end).

do_notify(#state{node_renaming_txn_mref = MRef} = State) when MRef =/= undefined ->
    State;
do_notify(#state{nodes = NodesOld} = State) ->
    NodesNew = nodes_actual(),
    case NodesNew =:= NodesOld of
        true  -> State;
        false -> gen_event:notify(ns_node_disco_events,
                                  {ns_node_disco_events, NodesOld, NodesNew}),
                 State#state{nodes = NodesNew}
    end.

ping_all() ->
    lists:foreach(fun net_adm:ping/1, nodes_wanted()).

% -----------------------------------------------------------

ns_log_cat(?NODE_DOWN) ->
    warn;
ns_log_cat(_X) ->
    info.

ns_log_code_string(?NODE_UP) ->
    "node up";
ns_log_code_string(?NODE_DOWN) ->
    "node down".
