%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc A module that pretends to be chronicle_kv via meck which allows us to
%% test code paths that need to interact with chronicle_kv without mocking
%% all of the functions in each test.
%%
%% Two approaches could have been taken to solve this problem:
%%     1) Mocking chronicle_kv
%%     2) Starting chronicle up properly
%% This module takes approach 1 as starting chronicle isn't necessarily
%% trivial due to the extra features that chronicle has (i.e. writing to
%% files and replicating config).
%%
%% All config is stored in an ets table as a map (similarly to chronicle).
%%
%% This helper is minimal, it was written to solve an issue for a specific
%% test, so the ns_config interface that has been implemented here is not
%% complete. Hopefully the simplicity of the snapshot and functions here allows
%% future users to simply add the additional interface functions required.
%%
%% Use should be as follows:
%%     1) fake_chronicle_kv:setup(),
%%     2a) Merge a map into the snapshot
%%         fake_chronicle_kv:update_snapshot(#{}),
%%     2b) Upsert an individual key value pair to the snapshot
%%         fake_chronicle_kv:update_snapshot(auto_failover_cfg, []),
%%     3) Perform test
%%     4) fake_chronicle_kv:teardown(),
-module(fake_chronicle_kv).

-include("ns_test.hrl").

%% API
-export([setup/0,
         teardown/0,
         update_snapshot/1,
         update_snapshot/2]).

%% Helper function API
-export([setup_cluster_compat_version/1]).

-define(TABLE_NAME, fake_chronicle_kv).
-define(FAKE_HISTORY_REV, <<"fake">>).

%% --------------------
%% API - Setup/Teardown
%% --------------------
setup() ->
    ets:new(?TABLE_NAME, [public, named_table]),
    meck_setup(),

    fake_ns_config:setup_ns_config_events(),

    {ok, _} = gen_event:start_link({local, chronicle_kv:event_manager(kv)}),
    {ok, _} = chronicle_compat_events:start_link().

teardown() ->
    ets:delete(?TABLE_NAME),

    Pid = whereis(chronicle_compat_events),
    unlink(Pid),
    misc:terminate_and_wait(Pid, shutdown),

    P1 = whereis(chronicle_kv:event_manager(kv)),
    unlink(P1),
    misc:terminate_and_wait(P1, shutdown),

    fake_ns_config:teardown_ns_config_events(),

    ?meckUnload(ns_node_disco),
    ?meckUnload(chronicle_kv).

%% -------------------------
%% API - Snapshot Management
%% -------------------------
-spec update_snapshot(atom(), term()) -> true.
update_snapshot(Key, Value) ->
    OldSnapshot = get_ets_snapshot(),
    NewSnapshot = maps:put(Key, add_rev_to_value(Value), OldSnapshot),
    store_ets_snapshot(NewSnapshot).

-spec update_snapshot(map()) -> true.
update_snapshot(Map) when is_map(Map) ->
    NewKVs = maps:map(
               fun(_Key, Value) ->
                       add_rev_to_value(Value)
               end, Map),
    OldSnapshot = get_ets_snapshot(),
    NewSnapshot = maps:merge(OldSnapshot, NewKVs),
    store_ets_snapshot(NewSnapshot).

%% ----------------------
%% API - Helper Functions
%% ----------------------
-spec setup_cluster_compat_version(list()) -> true.
setup_cluster_compat_version(Version) ->
    update_snapshot(cluster_compat_version, Version).

%% ----------------------------------
%% Internal - chronicle_kv meck setup
%% ----------------------------------
meck_setup() ->
    %% chronicle_compat often tries to read from ns_couchdb_chronicle_dup
    %% if available, we will bypass that to hit chronicle_kv proper.
    meck:new(ns_node_disco, [passthrough]),
    meck:expect(ns_node_disco, couchdb_node, [], [not_this_node]),

    meck_setup_chronicle_kv().

meck_setup_chronicle_kv() ->
    %% No passthrough, causes headaches as local functions can't be
    %% intercepted by meck, so we must control all entry to these modules.
    meck:new(chronicle_kv),

    meck:expect(chronicle_kv, sync, 2, ok),

    meck_setup_chronicle_kv_getters(),
    meck_setup_chronicle_kv_setters().

meck_setup_chronicle_kv_getters() ->
    meck:expect(
      chronicle_kv, get_snapshot,
      fun (_Fetchers, _Opts) ->
              %% Don't care about fetchers, return the entire config. We
              %% should be using lookup functions to get specific keys
              %% anyways so this should just work, and we don't care about
              %% perf here.
              get_ets_snapshot()
      end),


    meck:expect(chronicle_kv, ro_txn,
                fun(_Name, Fun) ->
                        {ok,
                         {Fun(get_ets_snapshot()),
                          make_rev(get_snapshot_seqno())}}
                end),
    meck:expect(chronicle_kv, ro_txn,
                fun(_Name, Fun, _Opts) ->
                        {ok,
                         {Fun(get_ets_snapshot()),
                          make_rev(get_snapshot_seqno())}}
                end),


    meck:expect(chronicle_kv, txn_get,
                fun(Key, Txn) ->
                        fetch_from_snapshot(Txn, Key, [])
                end),

    meck:expect(chronicle_kv, txn_get_many,
                fun(Keys, Txn) ->
                        get_keys_for_txn(Keys, Txn)
                end),


    meck:expect(chronicle_kv, get,
                fun(_Name, Key) ->
                        fetch_from_latest_snapshot(Key)
                end),
    meck:expect(chronicle_kv, get,
                fun(_Name, Key, #{}) ->
                        fetch_from_latest_snapshot(Key)
                end),

    meck:expect(chronicle_kv, get_full_snapshot,
                fun(_Name) ->
                        {ok,
                         {get_ets_snapshot(), make_rev(get_snapshot_seqno())}}
                end),

    meck:expect(chronicle_kv, event_manager,
                fun(Name) ->
                        list_to_atom(atom_to_list(Name) ++ "-events")
                end).

meck_setup_chronicle_kv_setters() ->
    meck:expect(chronicle_kv, transaction,
                fun(Name, Keys, Fun) ->
                        transaction(Name, Keys, Fun, #{})
                end),

    meck:expect(chronicle_kv, transaction,
                fun(Name, Keys, Fun, Opts) ->
                        transaction(Name, Keys, Fun, Opts)
                end),

    meck:expect(chronicle_kv, txn,
                fun(Name, Fun, Opts) ->
                        transaction(Name, all, Fun, Opts)
                end),

    meck:expect(chronicle_kv, txn,
                fun(Name, Fun) ->
                        transaction(Name, all, Fun, [])
                end),

    meck:expect(chronicle_kv, set,
                fun(_Name, Key, Value) ->
                        update_snapshot(Key, Value),
                        {ok, 1}
                end).

%% ----------------------------------
%% Internal - getter/setter functions
%% ----------------------------------
get_ets_snapshot() ->
    case ets:lookup(?TABLE_NAME, snapshot) of
        [{snapshot, Snapshot}] -> Snapshot;
        [] -> maps:new()
    end.

get_snapshot_seqno() ->
    case ets:lookup(?TABLE_NAME, snapshot_seqno) of
        [{snapshot_seqno, Value}] -> Value;
        [] -> 0
    end.

store_ets_snapshot(Snapshot) ->
    Seqno = maps:fold(
              fun(_Key, {_Value, {_, Seqno}}, Acc) ->
                      max(Seqno, Acc)
              end, 0, Snapshot),
    store_ets_snapshot(Snapshot, Seqno).
store_ets_snapshot(Snapshot, Seqno) ->
    ets:insert(?TABLE_NAME, {snapshot, Snapshot}),
    ets:insert(?TABLE_NAME, {snapshot_seqno, Seqno}).

add_rev_to_value(Value, Seqno) ->
    {Value, make_rev(Seqno)}.

add_rev_to_value(Value) ->
    %% Chronicle stores revs, we won't bother with the actual history rev here,
    %% but we do use the seqno in some places/tests.
    {Value, make_rev(get_snapshot_seqno() + 1)}.

make_rev(Seqno) ->
    {?FAKE_HISTORY_REV, Seqno}.

fetch_from_latest_snapshot(Key) ->
    fetch_from_latest_snapshot(Key, #{}).

fetch_from_latest_snapshot(Key, Opts) ->
    fetch_from_snapshot(get_ets_snapshot(), Key, Opts).

fetch_from_snapshot(Snapshot, Key, _Opts) ->
    case maps:find(Key, Snapshot) of
        error -> {error, not_found};
        Other -> Other
    end.

get_keys_for_txn(Keys, _Snapshot) ->
    %% We /probably/ should not care about writing to same keys at
    %% the same time in such a test, so this is the simplest way to deal with
    %% this for now.
    maps:filter(fun(K, _) -> lists:member(K, Keys) end, get_ets_snapshot()).

do_commits(Commits) ->
    {NewMap, NewSeqno} = lists:foldl(
        fun({set, Key, Value}, {Snapshot, Seqno}) ->
                NewSeqno = Seqno + 1,
                {Snapshot#{Key => add_rev_to_value(Value, NewSeqno)}, NewSeqno};
           ({delete, Key}, {Snapshot, Seqno}) ->
               NewSeqno = Seqno + 1,
               {maps:remove(Key, Snapshot), NewSeqno}
        end, {get_ets_snapshot(), get_snapshot_seqno()}, Commits),

    store_ets_snapshot(NewMap, NewSeqno).

transaction(_Name, Keys, Fun, _Opts) ->
    Snapshot = case Keys of
                   all -> get_ets_snapshot();
                   _ -> get_keys_for_txn(Keys, {txn_slow, get_ets_snapshot()})
               end,
    %% Not correct or safe by any means, but should be acceptable for unit
    %% testing.
    case Fun(Snapshot) of
        %% Only handles commits and sets at the moment. Can be
        %% expanded in the future if necessary
        {commit, Commits} ->
            do_commits(Commits),
            {ok, {?FAKE_HISTORY_REV, get_snapshot_seqno()}};
        {commit, Commits, Results} ->
            do_commits(Commits),
            {ok, {?FAKE_HISTORY_REV, get_snapshot_seqno()}, Results};
        {abort, Error} ->
            Error
    end.
