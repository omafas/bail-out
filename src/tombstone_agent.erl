%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(tombstone_agent).

-behavior(gen_server).

-include("ns_common.hrl").
-include("ns_config.hrl").

-export([start_link/0]).
-export([purge_ts/0, vclock_ts/0, purge_kvlist/1, purge_cluster/1,
         init_timestamps/1, refresh/0, wipe/0, refresh_timestamps/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(PURGER_KEY, ns_config_purger).
-define(PURGE_TS_KEY, ns_config_purge_ts).
-define(VCLOCK_TS_KEY, ns_config_vclock_ts).

-define(PREPARE_PURGE_TIMEOUT, ?get_timeout(prepare_purge, 20000)).
-define(SYNC_REVISION_TIMEOUT, ?get_timeout(sync_revision, 10000)).
-define(PUSH_TIMEOUT, ?get_timeout(push_timeout, 10000)).

-record(state, { purge_ts }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

purge_ts() ->
    tombstone_keeper:get(?PURGE_TS_KEY, 0).

vclock_ts() ->
    max(tombstone_keeper:get(?VCLOCK_TS_KEY, 0) + 1, ts()).

ts() ->
    calendar:datetime_to_gregorian_seconds(erlang:universaltime()).

purge_kvlist(KVList) ->
    purge_kvlist(KVList, purge_ts()).

purge_cluster(PurgeAge) ->
    OldPurgeTS =
        %% Get the value from chronicle, because the one published by the
        %% keeper may be a tiny bit out of date.
        case chronicle_kv:get(kv, ?PURGE_TS_KEY) of
            {ok, {V, _}} ->
                V;
            {error, not_found} ->
                0
        end,
    PurgeTS = ts() - PurgeAge,
    if
        PurgeTS > OldPurgeTS ->
            case have_tombstones(PurgeTS) of
                true ->
                    try
                        do_purge_cluster(PurgeTS)
                    catch
                        throw:Error ->
                            {error, Error}
                    end;
                false ->
                    ok
            end;
        PurgeTS =:= OldPurgeTS ->
            ok;
        true ->
            ?log_warning("Tentative purge ts ~b "
                         "is lower than the old purge ts ~b. "
                         "Time may be out of sync.",
                         [PurgeTS, OldPurgeTS]),
            {error, time_out_of_sync}
    end.

do_purge_cluster(PurgeTS) ->
    Rev = update_vclock_ts(PurgeTS),
    {Nodes, NodesRev} = get_nodes(),
    prepare_purge(Nodes, Rev),

    %% merge all the changes that were pushed by prepare_purge()
    ns_config_rep:synchronize_local(),

    KVList = ns_config:get_kv_list(),
    case find_tombstones(KVList, PurgeTS) of
        [] ->
            ok;
        Tombstones ->
            OtherNodes = Nodes -- [node()],
            push_keys(Tombstones, OtherNodes),
            update_purge_ts(PurgeTS, NodesRev)
    end.

prepare_purge(Nodes, Rev) ->
    {Results, Bad} = gen_server:multi_call(Nodes, ?MODULE,
                                           {prepare_purge, node(), Rev},
                                           ?PREPARE_PURGE_TIMEOUT),
    FailedNodes =
        [Pair || {_, R} = Pair <- Results, R =/= ok] ++ [{N, down} || N <- Bad],

    case FailedNodes of
        [] ->
            ok;
        _ ->
            throw({prepare_purge_failed, FailedNodes})
    end.

init_timestamps(Config) ->
    case cluster_compat_mode:is_cluster_70(Config) of
        true ->
            refresh();
        false ->
            ok
    end.

refresh() ->
    tombstone_keeper:refresh().

wipe() ->
    tombstone_keeper:wipe().

%% called by tombstone_keeper
refresh_timestamps() ->
    Keys = [?PURGE_TS_KEY, ?VCLOCK_TS_KEY],
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, Keys),
    lists:filtermap(
      fun (Key) ->
              case get_value(Key, Snapshot) of
                  {ok, Value} ->
                      {true, {Key, Value}};
                  error ->
                      false
              end
      end, Keys).

%% callbacks
init([]) ->
    chronicle_compat_events:notify_if_key_changes([?PURGE_TS_KEY], purge),
    {ok, maybe_purge(#state{purge_ts = 0})}.

handle_call({prepare_purge, Node, Rev}, _From, State) ->
    handle_prepare_purge(Node, Rev, State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_debug("Unexpected cast:~n~p", [Cast]),
    {noreply, State}.

handle_info(purge, State) ->
    _ = misc:flush(purge),
    {noreply, maybe_purge(State)};
handle_info(Msg, State) ->
    ?log_debug("Unexpected message:~n~p", [Msg]),
    {noreply, State}.

maybe_purge(#state{purge_ts = OldPurgeTS} = State) ->
    case cluster_compat_mode:is_cluster_70() of
        true ->
            refresh(),
            NewPurgeTS = purge_ts(),
            case OldPurgeTS =:= NewPurgeTS of
                true ->
                    State;
                false ->
                    purge(NewPurgeTS),
                    State#state{purge_ts = NewPurgeTS}
            end;
        false ->
            State
    end.

purge(PurgeTS) ->
    {ok, Purged} =
        ns_config:update_with_vclocks(
          fun (Key, Value, FullClock, Acc) ->
                  case purgeable(Value, FullClock, PurgeTS) of
                      true ->
                          {erase, [Key | Acc]};
                      false ->
                          {skip, Acc}
                  end
          end, []),

    case Purged of
        [] ->
            ok;
        _ ->
            N = length(Purged),
            ?log_debug("Purged ~b ns_config tombstone(s) up to timestamp ~b. "
                       "Tombstones:~n~200P",
                       [N, PurgeTS, Purged, 100])
    end.


have_tombstones(PurgeTS) ->
    find_tombstones(ns_config:get_kv_list(), PurgeTS) =/= [].

find_tombstones(KVList, PurgeTS) ->
    lists:filtermap(
      fun (KV) ->
              case purgeable(KV, PurgeTS) of
                  true ->
                      {Key, _} = KV,
                      {true, Key};
                  false ->
                      false
              end
      end, KVList).

purge_kvlist(KVList, PurgeTS) ->
    lists:filter(
      fun (KV) ->
              not purgeable(KV, PurgeTS)
      end, KVList).

purgeable({_Key, FullValue}, PurgeTS) ->
    Value = ns_config:strip_metadata(FullValue),
    FullClock = ns_config:extract_vclock(FullValue),
    purgeable(Value, FullClock, PurgeTS).

purgeable(Value, {ClockPurgeTS, Clock}, PurgeTS) ->
    case Value of
        ?DELETED_MARKER ->
            LatestTS = vclock:get_latest_timestamp(Clock),
            ClockPurgeTS =< PurgeTS andalso LatestTS =< PurgeTS;
        _ ->
            false
    end.

handle_prepare_purge(Node, Rev, State) ->
    chronicle_rsm:sync_revision(kv, Rev, ?SYNC_REVISION_TIMEOUT),
    refresh(),
    TS = vclock_ts(),
    LocalTS = ts(),

    case LocalTS < TS of
        true ->
            ?log_warning("Local timestamp ~b fell "
                         "behind the cluster timestamp ~b",
                         [LocalTS, TS]);
        false ->
            ok
    end,

    %% Make sure that ns_config_rep_merger has an up-to-date view of the
    %% cluster nodes. This is to prevent a race where a removed node
    %% replicates purged keys back to the cluster.
    ns_config_rep:update_nodes(),

    %% Make sure ns_config_rep_merger picks up the new timestamp and pushes
    %% everything to ns_config.
    ns_config_rep:synchronize_local(),

    case Node =:= node() of
        true ->
            %% This node is the orchestrator. No need to push the config. But
            %% the sync is neccessary to make sure that ns_config picks up the
            %% new timestamp.
            ns_config:sync();
        false ->
            %% get_kv_list() ensures that ns_config picked up the new
            %% timestamp implicitly.
            KVList = ns_config:get_kv_list(),
            Tombstones = find_tombstones(KVList, TS),
            ns_config_rep:push_keys(Tombstones),
            ns_config_rep:ensure_config_pushed()
    end,

    {reply, ok, State}.

get_revision(Key, Snapshot) ->
    case maps:find(Key, Snapshot) of
        {ok, {_, Rev}} ->
            Rev;
        error ->
            no_revision
    end.

get_value(Key, Snapshot) ->
    case maps:find(Key, Snapshot) of
        {ok, {Value, _}} ->
            {ok, Value};
        error ->
            error
    end.

get_value(Key, Snapshot, Default) ->
    case get_value(Key, Snapshot) of
        {ok, Value} ->
            Value;
        error ->
            Default
    end.

get_purger(Snapshot) ->
    get_value(?PURGER_KEY, Snapshot, unknown).

update_vclock_ts(TS) ->
    transaction(
      [?VCLOCK_TS_KEY, ?PURGER_KEY],
      fun (Snapshot) ->
              OldTS = get_value(?VCLOCK_TS_KEY, Snapshot, 0),
              case TS > OldTS of
                  true ->
                      {commit, [{set, ?VCLOCK_TS_KEY, TS},
                                {set, ?PURGER_KEY, node()}]};
                  false ->
                      {abort, {error, {conflict, get_purger(Snapshot)}}}
              end
      end).

update_purge_ts(PurgeTS, NodesRev) ->
    _ = transaction(
      [?VCLOCK_TS_KEY, ?PURGE_TS_KEY, ?PURGER_KEY, nodes_wanted],
      fun (Snapshot) ->
              case validate_purge_ts(PurgeTS, NodesRev, Snapshot) of
                  ok ->
                      {commit, [{set, ?PURGE_TS_KEY, PurgeTS}]};
                  {error, _} = Error ->
                      {abort, Error}
              end
      end),
    ok.

transaction(Keys, Body) ->
    try chronicle_kv:transaction(kv, Keys, Body) of
        {ok, Rev} ->
            Rev;
        {error, Error} ->
            throw(Error)
    catch
        exit:timeout ->
            throw(no_quorum)
    end.

validate_purge_ts(NewPurgeTS, NodesRev, Snapshot) ->
    Purger = get_purger(Snapshot),
    case Purger =:= node() of
        true ->
            case NodesRev =:= get_revision(nodes_wanted, Snapshot) of
                true ->
                    VClockTS = get_value(?VCLOCK_TS_KEY, Snapshot, 0),
                    PurgeTS = get_value(?PURGE_TS_KEY, Snapshot, 0),

                    case VClockTS =:= NewPurgeTS
                        andalso NewPurgeTS > PurgeTS of
                        true ->
                            ok;
                        false ->
                            {error, {inconsistent,
                                     [{purger, Purger},
                                      {vclock_ts, VClockTS},
                                      {purge_ts, PurgeTS},
                                      {new_purge_ts, NewPurgeTS}]}}
                    end;
                false ->
                    {error, nodes_changed}
            end;
        false ->
            {error, {conflict, Purger}}
    end.

push_keys(Keys, Nodes) ->
    ns_config_rep:push_keys(Keys),
    case ns_config_rep:ensure_config_seen_by_nodes(Nodes, ?PUSH_TIMEOUT) of
        ok ->
            ok;
        {error, BadNodes} ->
            throw({config_push_failed, BadNodes})
    end.

get_nodes() ->
    {ok, R} = chronicle_kv:get(kv, nodes_wanted),
    R.
