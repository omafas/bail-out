%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PULL_TIMEOUT, ?get_timeout(pull, 10000)).
-define(SELF_PULL_TIMEOUT, ?get_timeout(self_pull, 30000)).
-define(SYNCHRONIZE_TIMEOUT, ?get_timeout(sync, 30000)).
-define(QUORUM_FAILOVER_PULL_TIMEOUT, ?get_timeout(quorum_failover_pull, 5000)).

-define(MERGING_EMERGENCY_THRESHOLD, ?get_param(merge_mailbox_threshold, 2000)).
-define(MERGER_MAX_BLOBS_BATCH, ?get_param(merger_max_blobs_batch, 50)).

% How to launch the thing.
-export([start_link/0, start_link_merger/0]).

% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% API
-export([ensure_config_pushed/0, synchronize_local/0,
         ensure_config_seen_by_nodes/0,
         ensure_config_seen_by_nodes/1, ensure_config_seen_by_nodes/2,
         pull_and_push/1, pull_from_one_node_directly/1,
         get_timeout/1]).

-export([get_remote/2, pull_remotes/1, pull_remotes/2,
         push_keys/1, update_nodes/0]).

-record(state, { nodes,
                 nodes_rev }).

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

    chronicle_compat_events:notify_if_key_changes(
      lists:member(_, [cluster_compat_version, nodes_wanted]), update_nodes),

    ns_pubsub:subscribe_link(ns_node_disco_events,
                             handle_node_disco_event(Self, _)),

    State = update_nodes(#state{}),

    % Start with startup config sync.
    ?log_debug("init pulling", []),
    pull_random_node(State),
    ?log_debug("init pushing", []),
    do_push(State),
    % Schedule some random config syncs.
    schedule_config_sync(),
    {ok, State}.

merger_init() ->
    erlang:register(ns_config_rep_merger, self()),
    proc_lib:init_ack({ok, self()}),
    merger_loop([], ?MERGER_MAX_BLOBS_BATCH).

merger_loop([], MaxBatch) ->
    EnterTime = os:timestamp(),
    receive
        {merge_compressed, Blob} ->
            WakeTime = os:timestamp(),
            SleepTime = timer:now_diff(WakeTime, EnterTime) div 1000,
            ns_server_stats:notify_histogram(<<"ns_config_merger_sleep_time">>,
                                             SleepTime),
            merger_loop([Blob], MaxBatch);
        {sync, Pid, StartTS, From} ->
            merger_sync_reply(Pid, StartTS, From),
            merger_loop([], MaxBatch)
    end;
merger_loop(Changes, MaxBatch) when length(Changes) >= MaxBatch ->
    merge_changes(Changes),
    merger_loop([], ?MERGER_MAX_BLOBS_BATCH);
merger_loop(Changes, MaxBatch) ->
    receive
        {merge_compressed, Blob} ->
            merger_loop([Blob|Changes], MaxBatch);
        {sync, Pid, StartTS, From} ->
            merge_changes(Changes),
            merger_sync_reply(Pid, StartTS, From),
            merger_loop([], ?MERGER_MAX_BLOBS_BATCH)
    after 0 ->
        merge_changes(Changes),
        merger_loop([], ?MERGER_MAX_BLOBS_BATCH)
    end.

merger_sync_reply(Pid, StartTS, From) ->
    gen_server:cast(Pid, {sync_reply, StartTS, From}).

merge_changes(ListOfBlobs) ->
    MergeStart = os:timestamp(),
    merge_remote_configs(fun misc:decompress/1, lists:reverse(ListOfBlobs)),
    RunTime = timer:now_diff(os:timestamp(), MergeStart) div 1000,
    ns_server_stats:notify_histogram(<<"ns_config_merger_run_time">>, RunTime),
    {message_queue_len, QL} = erlang:process_info(self(), message_queue_len),
    ns_server_stats:notify_max(
      {<<"ns_config_merger_queue_len_1m_max">>, 60000, 1000}, QL),
    case QL > ?MERGING_EMERGENCY_THRESHOLD of
        true ->
            ?log_warning("Queue size emergency state reached. "
                         "Will kill myself and resync"),
            exit(emergency_kill);
        false -> ok
    end.

handle_call(synchronize, From, State) ->
    %% Need to sync with merger too because in case of incoming config change
    %% merger pushes changes to couchdb node
    merger_request_sync(From),
    {noreply, State};
handle_call(synchronize_everything, {Pid, _Tag} = From,
            State) ->
    RemoteNode = node(Pid),
    ?log_debug("Got full synchronization request from ~p", [RemoteNode]),
    merger_request_sync(From),
    {noreply, State};
handle_call({pull_remotes, Nodes, Timeout}, _From, State) ->
    {reply, pull_from_all_nodes(Nodes, Timeout), State};
handle_call(Msg, _From, State) ->
    ?log_warning("Unhandled call: ~p", [Msg]),
    {reply, error, State}.

handle_cast({merge_compressed, Node, Rev, Blob}, State) ->
    %% 7.1 and later.
    case accept_merge(Node, Rev, State) of
        true ->
            merge_blob(Blob);
        false ->
            ok
    end,
    {noreply, State};
handle_cast({merge_compressed, Blob}, State) ->
    %% Pre-7.1
    merge_blob(Blob),
    {noreply, State};
handle_cast({sync_reply, StartTS, From}, State) ->
    EndTS = erlang:monotonic_time(microsecond),
    ?log_debug("Synchronized with merger in ~p us", [EndTS - StartTS]),
    gen_server:reply(From, ok),
    {noreply, State};
handle_cast(Msg, State) ->
    ?log_error("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

merger_request_sync(From) ->
    Msg = {sync, self(), erlang:monotonic_time(microsecond), From},
    ns_config_rep_merger ! Msg.

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

accumulate_and_push_keys(_Keys0, 0, State) ->
    ns_server_stats:notify_counter(
      <<"ns_config_rep_push_keys_retries_exceeded">>),
    %% Exceeded retries count trying to get consistent keys/values for config
    %% replication. This can be caused when there are too many independent
    %% changes over a short time interval. Rather than try to accumulate more
    %% changes we'll just replicate the entire configuration.
    ?log_info("Exceeded retries count trying to get consistent keys/values "
              "for config replication. The full config will be replicated."),
    KVs = lists:sort(ns_config:get_kv_list()),
    Keys = [K || {K, _} <- KVs],
    do_push_keys(Keys, KVs, State);
accumulate_and_push_keys(Keys0, RetriesLeft, State) ->
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
            ns_server_stats:notify_counter(
              <<"ns_config_rep_push_keys_retries">>),
            %% ordering of these messages is irrelevant so we can
            %% resend and retry
            self() ! Msg,
            accumulate_and_push_keys(Keys, RetriesLeft-1, State)
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
            do_push_keys(Keys, AllConfigKV, State)
    end.

handle_info({push_keys, Keys0}, State) ->
    accumulate_and_push_keys(Keys0, 10, State),
    {noreply, State};
handle_info({pull_and_push, Nodes}, State) ->
    FinalNodes = accumulate_pull_and_push(Nodes),
    NewState = update_nodes(State),
    KnownNodes = [N || N <- FinalNodes, lists:member(N, NewState#state.nodes)],

    ?log_info("Replicating config to/from:~n~p", [KnownNodes]),
    pull_one_node(KnownNodes, length(KnownNodes)),
    RawKVList = ns_config:get_kv_list(?SELF_PULL_TIMEOUT),
    Blob = misc:compress(RawKVList),
    do_push(Blob, KnownNodes, NewState#state.nodes_rev),
    ?log_debug("config pull_and_push done.", []),
    {noreply, NewState};
handle_info(sync_random, State) ->
    schedule_config_sync(),
    pull_random_node(1, State),
    {noreply, State};
handle_info({'EXIT', _From, Reason} = Msg, _State) ->
    ?log_warning("Got exit message. Exiting: ~p", [Msg]),
    {stop, Reason};
handle_info(update_nodes, State) ->
    misc:flush(update_nodes),
    NewState = update_nodes(State),
    maybe_force_pull(NewState, State),
    {noreply, NewState};
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
get_timeout(pull) ->
    ?PULL_TIMEOUT;
get_timeout(push) ->
    ?SYNCHRONIZE_TIMEOUT.

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

pull_and_push(Nodes) ->
    pull_and_push(?MODULE, Nodes).

pull_and_push(_ServerRef, []) -> ok;
pull_and_push(ServerRef, Nodes) ->
    ServerRef ! {pull_and_push, Nodes}.

get_remote(Node, Timeout) ->
    Blob = ns_config_replica:get_compressed(Node, Timeout),
    misc:decompress(Blob).

pull_remotes(Nodes) ->
    pull_remotes(Nodes, ?PULL_TIMEOUT).

pull_remotes(Nodes, PullTimeout) ->
    gen_server:call(?MODULE, {pull_remotes, Nodes, PullTimeout}, infinity).

push_keys(Keys) ->
    ?MODULE ! {push_keys, Keys}.

update_nodes() ->
    ?MODULE ! update_nodes.

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
    erlang:send_after(Frequency, self(), sync_random).

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

do_push_keys(Keys, AllKVs, State) ->
    ?log_debug("Replicating some config keys (~p..)", [lists:sublist(Keys, 64)]),
    KVsToPush = extract_kvs(Keys, lists:sort(AllKVs), []),
    do_push(KVsToPush, State).

do_push(State) ->
    do_push(ns_config:get_kv_list(?SELF_PULL_TIMEOUT), State).

do_push(RawKVList, #state{nodes_rev = Revision} = State) ->
    Blob = misc:compress(RawKVList),
    do_push_local(Blob),
    LiveNodes = live_other_nodes(State),
    do_push(Blob, LiveNodes, Revision).

do_push(Blob, OtherNodes, Revision) ->
    misc:parallel_map(send_blob(_, Revision, Blob), OtherNodes, 2000).

send_blob(Node, undefined, Blob) ->
    gen_server:cast({ns_config_rep, Node}, {merge_compressed, Blob});
send_blob(Node, Revision, Blob) ->
    gen_server:cast({ns_config_rep, Node},
                    {merge_compressed, node(), Revision, Blob}).

do_push_local(Blob) ->
    do_push(Blob, ns_node_disco:local_sub_nodes(), undefined).

pull_random_node(State)  -> pull_random_node(5, State).
pull_random_node(N, State) ->
    LiveNodes = live_other_nodes(State),
    pull_one_node(misc:shuffle(LiveNodes), N).

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

    Blobs = [Blob || {_, Blob} <- Good],
    MergeResult = merge_remote_configs(fun misc:decompress/1, Blobs),

    case Bad =:= [] of
        true ->
            MergeResult;
        false ->
            {error, {get_compressed_failed, Bad}}
    end.

merge_one_remote_config(KVList) ->
    merge_remote_configs(fun (L) -> L end, [KVList]).

merge_remote_configs(_, []) ->
    ok;
merge_remote_configs(Fun, KVLists) ->
    Config = ns_config:get(),
    LocalKVList = ns_config:get_kv_list_with_config(Config),
    UUID = ns_config:uuid(Config),

    {NewKVList, TouchedKeys} =
        lists:foldl(
          fun (RemoteKVList, {AccKVList, AccTouched}) ->
                  do_merge_one_remote_config(UUID, Fun(RemoteKVList), AccKVList,
                                             AccTouched)
          end, {LocalKVList, []}, KVLists),

    case NewKVList =:= LocalKVList of
        true ->
            ok;
        false ->
            case ns_config:cas_remote_config(NewKVList, TouchedKeys, LocalKVList) of
                true ->
                    do_push_local(misc:compress(NewKVList -- LocalKVList)),
                    ok;
                _ ->
                    ?log_warning("config cas failed. Retrying", []),
                    merge_remote_configs(Fun, KVLists)
            end
    end.

do_merge_one_remote_config(UUID, RemoteKVList, AccKVList, AccTouched) ->
    %% Make sure that tombstones that we might have already purged don't get
    %% replicated to us again.
    PurgedKVList = tombstone_agent:purge_kvlist(RemoteKVList),
    {Merged, Touched} = ns_config:merge_kv_pairs(PurgedKVList, AccKVList, UUID),
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

handle_node_disco_event(Parent, {ns_node_disco_events, Old, New}) ->
    case New -- Old of
        [] ->
            ok;
        NewNodes ->
            ?log_debug("Detected new nodes (~p).  Moving config around.",
                       [NewNodes]),
            %% we know that new node will also try to replicate config
            %% to/from us. So we half our traffic by enforcing
            %% 'initiative' from higher node to lower node
            pull_and_push(Parent, [N || N <- NewNodes, N < node()])
    end;
handle_node_disco_event(_, _) ->
    ok.

merge_blob(Blob) ->
    ns_config_rep_merger ! {merge_compressed, Blob}.

update_nodes(State) ->
    case cluster_compat_mode:is_cluster_71() of
        true ->
            {ok, {Nodes, Rev}} = chronicle_kv:get(kv, nodes_wanted),
            State#state{nodes = Nodes, nodes_rev = Rev};
        false ->
            State#state{nodes = ns_node_disco:nodes_wanted(),
                        nodes_rev = undefined}
    end.

accept_merge(_Node, _OtherRev, #state{nodes_rev = undefined}) ->
    %% This node/process has not switched to 7.1 yet. Accept all incoming
    %% merge requests.
    true;
accept_merge(Node, OtherRev,
             #state{nodes = Nodes, nodes_rev = OurRev}) ->
    case chronicle:compare_revisions(OtherRev, OurRev) of
        incompatible ->
            %% Quorum failover happened. The other nodes is either part of the
            %% new topology and are slightly ahead of us (in which case we
            %% would want to accept the merge request), or its part of the
            %% failed over partition (and we don't want to accept the
            %% request). We could distinguish between these two situations if
            %% the merge request was coupled with the corresponding failover
            %% log. But that feels like too much trouble. Instead we'll resync
            %% with all nodes once we've observed the change to nodes_wanted
            %% that carries the new history id. Since quorum failover is
            %% reserved for smaller clusters and is generally discouraged,
            %% this feels ok to me.
            ?log_info("Ignoring a merge request from ~w "
                      "due to incompatible revisions. "
                      "Our revision: ~w. "
                      "Their revision: ~w",
                      [Node, OurRev, OtherRev]),
            false;
        lt ->
            %% The other node is potentially behind. Check that it's part of
            %% the topology known to us.
            Member = lists:member(Node, Nodes),
            case Member of
                false ->
                    ?log_info("Ignoring a merge request from ~w "
                              "which is not in peers.", [Node]);
                _ ->
                    ok
            end,
            Member;
        _ ->
            %% The other node is ahead. Even though it may not be a part of the
            %% topology known to us, we trust it.
            true
    end.

maybe_force_pull(#state{nodes_rev = NewRev} = NewState,
                 #state{nodes_rev = OldRev}) ->
    case NewRev =:= undefined orelse OldRev =:= undefined of
        true ->
            %% Still in pre-7.1 compat mode. Nothing to be done.
            ok;
        false ->
            case chronicle:compare_revisions(NewRev, OldRev) of
                incompatible ->
                    force_pull(NewState);
                _ ->
                    ok
            end
    end.

force_pull(State) ->
    OtherNodes = live_other_nodes(State),
    case OtherNodes of
        [] ->
            ok;
        _ ->
            ?log_info("Going to pull config from nodes ~w "
                      "after quorum failover", [OtherNodes]),
            case pull_from_all_nodes(OtherNodes,
                                     ?QUORUM_FAILOVER_PULL_TIMEOUT) of
                ok ->
                    ?log_info("Pulled config successfully");
                Other ->
                    %% Eventually our gossiping should propagate any
                    %% potentially missing changes back to us.
                    ?log_warning("Failed to pull config:~n~p", [Other])
            end
    end.

live_other_nodes(#state{nodes = Nodes}) ->
    ns_node_disco:only_live_nodes(Nodes -- [node()]).
