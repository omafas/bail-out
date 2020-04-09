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
%% ns_config_rep is a server responsible for all things configuration
%% synch related.
%%
%% NOTE: that this code tries to merge similar replication requests
%% before trying to perform them. That's beneficial because due to
%% some nodes going down some replications might take very long
%% time. Which will cause our mailbox to grow with easily mergable
%% requests.
%%
-module(ns_config_rep).

-behaviour(gen_server).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PULL_TIMEOUT, ?get_timeout(pull, 10000)).
-define(SELF_PULL_TIMEOUT, ?get_timeout(self_pull, 30000)).
-define(SYNCHRONIZE_TIMEOUT, ?get_timeout(sync, 30000)).

-define(MERGING_EMERGENCY_THRESHOLD, ?get_param(merge_mailbox_threshold, 2000)).

% How to launch the thing.
-export([start_link/0, start_link_merger/0]).

% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% API
-export([ensure_config_pushed/0,
         ensure_config_seen_by_nodes/0,
         ensure_config_seen_by_nodes/1, ensure_config_seen_by_nodes/2,
         pull_and_push/1, pull_from_one_node_directly/1]).

-export([get_remote/2, pull_remotes/1, pull_remotes/2, push_keys/1]).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link_merger() ->
    proc_lib:start_link(erlang, apply, [fun merger_init/0, []]).

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events_local,
                             fun (Keys) ->
                                     Self ! {push_keys, Keys}
                             end),
    % Start with startup config sync.
    ?log_debug("init pulling", []),
    pull_random_node(),
    ?log_debug("init pushing", []),
    do_push(),
    % Have ns_config reannounce its config for any synchronization that
    % may have occurred.
    ?log_debug("init reannouncing", []),
    ns_config:reannounce(),
    % Schedule some random config syncs.
    schedule_config_sync(),
    ok = ns_node_disco_rep_events:add_sup_handler(),
    {ok, #state{}}.

merger_init() ->
    erlang:register(ns_config_rep_merger, self()),
    proc_lib:init_ack({ok, self()}),
    merger_loop().

merger_loop() ->
    EnterTime = os:timestamp(),
    receive
        {merge_compressed, Blob} ->
            WakeTime = os:timestamp(),
            KVList = misc:decompress(Blob),
            system_stats_collector:increment_counter(total_config_merger_sleep_time, timer:now_diff(WakeTime, EnterTime)),
            merge_one_remote_config(KVList),
            system_stats_collector:increment_counter(total_config_merger_run_time, timer:now_diff(os:timestamp(), WakeTime)),
            system_stats_collector:increment_counter(total_config_merger_runs, 1),
            {message_queue_len, QL} = erlang:process_info(self(), message_queue_len),
            system_stats_collector:set_counter(config_merger_queue_len, QL),
            case QL > ?MERGING_EMERGENCY_THRESHOLD of
                true ->
                    ?log_warning("Queue size emergency state reached. "
                                 "Will kill myself and resync"),
                    exit(emergency_kill);
                false -> ok
            end;
        {'$gen_call', From, sync} ->
            gen_server:reply(From, sync_done)
    end,
    merger_loop().

handle_call(synchronize, _From, State) ->
    %% Need to sync with merger too because in case of incoming config change
    %% merger pushes changes to couchdb node
    sync_done = gen_server:call(ns_config_rep_merger, sync,
                                ?SYNCHRONIZE_TIMEOUT),
    {reply, ok, State};
handle_call(synchronize_everything, {Pid, _Tag} = _From,
            State) ->
    RemoteNode = node(Pid),
    ?log_debug("Got full synchronization request from ~p", [RemoteNode]),

    StartTS = os:timestamp(),
    sync_done = gen_server:call(ns_config_rep_merger, sync, ?SYNCHRONIZE_TIMEOUT),
    EndTS = os:timestamp(),
    Diff = timer:now_diff(EndTS, StartTS),
    ?log_debug("Fully synchronized config in ~p us", [Diff]),

    {reply, ok, State};
handle_call({pull_remotes, Nodes, Timeout}, _From, State) ->
    {reply, pull_from_all_nodes(Nodes, Timeout), State};
handle_call(Msg, _From, State) ->
    ?log_warning("Unhandled call: ~p", [Msg]),
    {reply, error, State}.

handle_cast({merge_compressed, _Blob} = Msg, State) ->
    ns_config_rep_merger ! Msg,
    {noreply, State};
handle_cast(Msg, State) ->
    ?log_error("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

accumulate_X(Acc, X) ->
    receive
        {X, Value} ->
            accumulate_X(lists:umerge(lists:sort(Value), Acc), X)
    after 0 ->
            Acc
    end.

accumulate_pull_and_push(Nodes) ->
    accumulate_X(lists:sort(Nodes), pull_and_push).

accumulate_push_keys(InitialKeys) ->
    accumulate_X(lists:sort(InitialKeys), push_keys).

accumulate_and_push_keys(_Keys0, 0) ->
    system_stats_collector:increment_counter(ns_config_rep_push_keys_retries_exceeded, 1),
    ale:warn(?USER_LOGGER,
             "Exceeded retries count trying to get consistent keys/values "
             "for config replication. This is minor bug. Everything is "
             "safe, but please file bug and attach logs"),
    KVs = lists:sort(ns_config:get_kv_list()),
    Keys = [K || {K, _} <- KVs],
    do_push_keys(Keys, KVs);
accumulate_and_push_keys(Keys0, RetriesLeft) ->
    Keys = accumulate_push_keys(Keys0),
    AllConfigKV = ns_config:get_kv_list(),
    %% the following ensures that all queued ns_config_events_local
    %% events are processed (and thus we've {push_keys, ...} in our
    %% mailbox if there were any local config mutations
    gen_event:which_handlers(ns_config_events_local),
    receive
        {push_keys, _} = Msg ->
            %% ok, yet another change is detected, we need to retry so
            %% that AllConfigKV is consistent with list of changed
            %% keys we have
            system_stats_collector:increment_counter(ns_config_rep_push_keys_retries, 1),
            system_stats_collector:increment_counter(ns_config_rep_push_keys_total_retries_left, RetriesLeft),
            %% ordering of these messages is irrelevant so we can
            %% resend and retry
            self() ! Msg,
            accumulate_and_push_keys(Keys, RetriesLeft-1)
    after 0 ->
            %% we know that AllConfigKV has exactly changes we've seen
            %% with {push_keys, ...}. I.e. there's no way config
            %% could've changed by local mutation before us getting it
            %% and us not detecting it here. Also we can see that
            %% we're reading values after we've seen keys.
            %%
            %% NOTE however that non-local mutation (i.e. incoming
            %% config replication) may have overriden some local
            %% mutations. And it's possible for us to see final value
            %% rather than produced by local mutation. It seems to be
            %% possible only when there's config conflict btw.
            %%
            %% So worst case seems to be that our node accidently
            %% replicates some value mutated on other node without
            %% replicating other change(s) by that other
            %% node. I.e. some third node may see partial config
            %% mutations of other node via config replication from
            %% this node. Given that we don't normally cause config
            %% conflicts and that in some foreseeble future we're
            %% going to make our config replication even less
            %% conflict-prone I think it should be ok. I.e. local
            %% mutation that is overwritten by conflicting incoming
            %% change is already bug.
            do_push_keys(Keys, AllConfigKV)
    end.

handle_info({push_keys, Keys0}, State) ->
    accumulate_and_push_keys(Keys0, 10),
    {noreply, State};
handle_info({pull_and_push, Nodes}, State) ->
    ?log_info("Replicating config to/from:~n~p", [Nodes]),
    FinalNodes = accumulate_pull_and_push(Nodes),
    pull_one_node(FinalNodes, length(FinalNodes)),
    RawKVList = ns_config:get_kv_list(?SELF_PULL_TIMEOUT),
    do_push(RawKVList, FinalNodes),
    ?log_debug("config pull_and_push done.", []),
    {noreply, State};
handle_info(sync_random, State) ->
    schedule_config_sync(),
    pull_random_node(1),
    {noreply, State};
handle_info({'EXIT', _From, Reason} = Msg, _State) ->
    ?log_warning("Got exit message. Exiting: ~p", [Msg]),
    {stop, Reason};
handle_info(Msg, State) ->
    ?log_debug("Unhandled msg: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%
% API methods
%

%% make sure that all outstanding changes are pushed out to other nodes
ensure_config_pushed() ->
    ns_config:sync_announcements(),
    synchronize_local().

%% push outstanding changes to other nodes and make sure that they merged the
%% changes in
ensure_config_seen_by_nodes() ->
    ensure_config_seen_by_nodes(ns_node_disco:nodes_actual_other()).

ensure_config_seen_by_nodes(Nodes) ->
    ensure_config_seen_by_nodes(Nodes, ?SYNCHRONIZE_TIMEOUT).

ensure_config_seen_by_nodes(Nodes, Timeout) ->
    ns_config:sync_announcements(),
    synchronize_remote(Nodes, Timeout).

pull_and_push([]) -> ok;
pull_and_push(Nodes) ->
    ?MODULE ! {pull_and_push, Nodes}.

get_remote(Node, Timeout) ->
    Blob = ns_config_replica:get_compressed(Node, Timeout),
    misc:decompress(Blob).

pull_remotes(Nodes) ->
    pull_remotes(Nodes, ?PULL_TIMEOUT).

pull_remotes(Nodes, PullTimeout) ->
    gen_server:call(?MODULE, {pull_remotes, Nodes, PullTimeout}, infinity).

push_keys(Keys) ->
    ?MODULE ! {push_keys, Keys}.

%
% Privates
%

% wait for completion of all previous requests
synchronize_local() ->
    gen_server:call(?MODULE, synchronize, ?SYNCHRONIZE_TIMEOUT).

synchronize_remote(Nodes, Timeout) ->
    ok = synchronize_local(),
    {_Replies, BadNodes} =
        misc:multi_call(Nodes, ?MODULE,
                        synchronize_everything, Timeout,
                        fun (R) ->
                                R =:= ok
                        end),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?log_error("Failed to synchronize config to some nodes: ~n~p",
                       [BadNodes]),
            {error, BadNodes}
    end.

schedule_config_sync() ->
    Frequency = 5000 + trunc(rand:uniform() * 55000),
    timer2:send_after(Frequency, self(), sync_random).

extract_kvs([], _KVs, Acc) ->
    Acc;
extract_kvs([K | Ks] = AllKs, [{CK,_} = KV | KVs], Acc) ->
    case K =:= CK of
        true ->
            extract_kvs(Ks, KVs, [KV | Acc]);
        _ ->
            %% we expect K to be present in kvs
            true = (K > CK),
            extract_kvs(AllKs, KVs, Acc)
    end.

do_push_keys(Keys, AllKVs) ->
    ?log_debug("Replicating some config keys (~p..)", [lists:sublist(Keys, 64)]),
    KVsToPush = extract_kvs(Keys, lists:sort(AllKVs), []),
    do_push(KVsToPush).

do_push() ->
    do_push(ns_config:get_kv_list(?SELF_PULL_TIMEOUT)).

do_push(RawKVList) ->
    do_push(RawKVList, ns_node_disco:nodes_actual_other() ++ ns_node_disco:local_sub_nodes()).

do_push(_RawKVList, []) ->
    ok;
do_push(RawKVList, OtherNodes) ->
    Blob = misc:compress(RawKVList),
    misc:parallel_map(fun(Node) ->
                              gen_server:cast({ns_config_rep, Node},
                                              {merge_compressed, Blob})
                      end,
                      OtherNodes, 2000).

pull_random_node()  -> pull_random_node(5).
pull_random_node(N) -> pull_one_node(misc:shuffle(ns_node_disco:nodes_actual_other()), N).

pull_one_node(Nodes, Tries) ->
    pull_one_node(Nodes, Tries, []).

pull_one_node([], _N, Errors) ->
    {error, Errors};
pull_one_node(_Nodes, 0, Errors) ->
    {error, Errors};
pull_one_node([Node | Rest], N, Errors) ->
    ?log_info("Pulling config from: ~p", [Node]),
    case (catch get_remote(Node, ?PULL_TIMEOUT)) of
        {'EXIT', _, _} = E ->
            pull_one_node(Rest, N - 1, [{Node, E} | Errors]);
        {'EXIT', _} = E ->
            pull_one_node(Rest, N - 1, [{Node, E} | Errors]);
        RemoteKVList ->
            merge_one_remote_config(RemoteKVList),
            ok
    end.

pull_from_one_node_directly(Node) ->
    pull_one_node([Node], 1).

pull_from_all_nodes(Nodes, Timeout) ->
    {Good, Bad} = ns_config_replica:get_compressed_many(Nodes, Timeout),

    KVLists     = [misc:decompress(Blob) || {_, Blob} <- Good],
    MergeResult = merge_remote_configs(KVLists),

    case Bad =:= [] of
        true ->
            MergeResult;
        false ->
            {error, {get_compressed_failed, Bad}}
    end.

merge_one_remote_config(KVList) ->
    merge_remote_configs([KVList]).

merge_remote_configs([]) ->
    ok;
merge_remote_configs(KVLists) ->
    Config = ns_config:get(),
    LocalKVList = ns_config:get_kv_list_with_config(Config),
    UUID = ns_config:uuid(Config),

    {NewKVList, TouchedKeys} =
        lists:foldl(
          fun (RemoteKVList, {AccKVList, AccTouched}) ->
                  do_merge_one_remote_config(UUID, RemoteKVList, AccKVList, AccTouched)
          end, {LocalKVList, []}, KVLists),

    case NewKVList =:= LocalKVList of
        true ->
            ok;
        false ->
            case ns_config:cas_remote_config(NewKVList, TouchedKeys, LocalKVList) of
                true ->
                    do_push(NewKVList -- LocalKVList, ns_node_disco:local_sub_nodes()),
                    ok;
                _ ->
                    ?log_warning("config cas failed. Retrying", []),
                    merge_remote_configs(KVLists)
            end
    end.

do_merge_one_remote_config(UUID, RemoteKVList, AccKVList, AccTouched) ->
    {Merged, Touched} = ns_config:merge_kv_pairs(RemoteKVList, AccKVList, UUID),
    {Merged, ordsets:union(AccTouched, Touched)}.


-ifdef(TEST).
accumulate_pull_and_push_test() ->
    receive
        {pull_and_push, _} -> exit(bad)
    after 0 -> ok
    end,

    L1 = [a,b],
    L2 = [b,c,e],
    L3 = [a,d],
    self() ! {pull_and_push, L2},
    self() ! {pull_and_push, L3},
    ?assertEqual([a,b,c,d,e],
                 accumulate_pull_and_push(L1)),
    receive
        {pull_and_push, _} -> exit(bad)
    after 0 -> ok
    end.
-endif.
