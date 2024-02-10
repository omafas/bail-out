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
%% This module exposes a meck like interface to create and unload the
%% required mocks (i.e. fake_ns_config:new(), fake_ns_config:unload()).
%%
%% All config is stored in an ets table as a map (similarly to chronicle).
%%
%% This helper is minimal, it was written to solve an issue for a specific
%% test, so the ns_config interface that has been implemented here is not
%% complete. Hopefully the simplicity of the snapshot and functions here allows
%% future users to simply add the additional interface functions required.
%%
%% Use should be as follows:
%%     1) fake_chronicle_kv:new(),
%%     2a) Merge a map into the snapshot
%%         fake_chronicle_kv:update_snapshot(#{}),
%%     2b) Upsert an individual key value pair to the snapshot
%%         fake_chronicle_kv:update_snapshot(auto_failover_cfg, []),
%%     3) Perform test
%%     4) fake_chronicle_kv:unload(),
-module(fake_chronicle_kv).

-include("ns_test.hrl").

%% API
-export([new/0,
         unload/0,
         update_snapshot/1,
         update_snapshot/2]).

%% Helper function API
-export([setup_cluster_compat_version/1]).

-define(TABLE_NAME, fake_chronicle_kv).

%% --------------------
%% API - Setup/Teardown
%% --------------------
new() ->
    ets:new(?TABLE_NAME, [public, named_table]),
    meck_setup().

unload() ->
    ets:delete(?TABLE_NAME),
    meck:unload(ns_node_disco),
    meck:unload(chronicle_kv).

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
    meck:expect(chronicle_kv, get_snapshot,
        fun (_Fetchers, _Opts) ->
                %% Don't care about fetchers, return the entire config. We
                %% should be using lookup functions to get specific keys
                %% anyways so this should just work, and we don't care about
                %% perf here.
                get_ets_snapshot()
        end),


    meck:expect(chronicle_kv, ro_txn,
        fun(_Name, _Fun, _Opts) ->
                {ok, {get_ets_snapshot(), no_rev}}
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
            transaction(Name, [], Fun, Opts)
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

store_ets_snapshot(Snapshot) ->
    ets:insert(?TABLE_NAME, {snapshot, Snapshot}).

add_rev_to_value(Value) ->
    %% Chronicle stores revs, we won't bother with the actual values here,
    %% but we need the correct format.
    {Value, 1}.

fetch_from_latest_snapshot(Key) ->
    fetch_from_latest_snapshot(Key, #{}).

fetch_from_latest_snapshot(Key, Opts) ->
    fetch_from_snapshot(get_ets_snapshot(), Key, Opts).

fetch_from_snapshot(Snapshot, Key, _Opts) ->
    case maps:find(Key, Snapshot) of
        error -> {error, not_found};
        Other -> Other
    end.

get_keys_for_txn(_Keys, _Snapshot) ->
    %% Don't care about fetchers, return the entire config. We
    %% should be using lookup functions to get specific keys
    %% anyways so this should just work, and we don't care about
    %% perf here. We /probably/ should not care about writing to same keys at
    %% the same time in such a test, so this is the simplest way to deal with
    %% this for now.
    get_ets_snapshot().

transaction(_Name, Keys, Fun, _Opts) ->
    Snapshot = get_keys_for_txn(Keys, {txn_slow, get_ets_snapshot()}),
    %% Not correct or safe by any means, but should be acceptable for unit
    %% testing.
    case Fun(Snapshot) of
        %% Only handles commits and sets at the moment. Can be
        %% expanded in the future if necessary
        {commit, New} ->
            NewMap = lists:foldl(
                fun({set, Key, Value}, Acc) ->
                    Acc#{Key => Value}
                end, #{}, New),
            update_snapshot(NewMap),
            {ok, 1}
    end.
