%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc A module that pretends to be ns_config via meck which allows us to
%% test code paths that need to interact with ns_config without mocking all
%% of the functions in each test.
%%
%% Two approaches could have been taken to solve this problem:
%%     1) Mocking ns_config
%%     2) Starting ns_config up properly
%% This module takes approach 1 as starting ns_config isn't necessarily
%% trivial due to the extra features that ns_config has (i.e. writing to
%% files and replicating config).
%%
%% This module exposes a meck like interface to create and unload the
%% required mocks (i.e. fake_ns_config:new(), fake_ns_config:unload()).
%%
%% All config is stored in an ets table as a proplist (similarly to ns_config).
%%
%% This helper is minimal, it was written to solve an issue for a specific
%% test, so the ns_config interface that has been implemented here is not
%% complete. Hopefully the simplicity of the snapshot and functions here allows
%% future users to simply add the additional interface functions required.
%%
%% Use should be as follows:
%%     1) fake_ns_config:new(),
%%     2a) Merge a proplist to the snapshot
%%         fake_ns_config:update_snapshot([{auto_failover_cfg,
%%                                          [{enabled, true},
%%                                           {timeout, 1},
%%                                           {count, 0},
%%                                           {max_count, 5}]}]),
%%     2b) Upsert an individual key value pair to the snapshot
%%         fake_ns_config:update_snapshot(auto_failover_cfg, []),
%%     3) Perform test
%%     4) fake_ns_config:unload(),
-module(fake_ns_config).

-include("ns_config.hrl").

%% API
-export([new/0,
         unload/0,
         update_snapshot/1,
         update_snapshot/2]).

-define(TABLE_NAME, fake_ns_config).

%% --------------------
%% API - Setup/Teardown
%% --------------------
new() ->
    ets:new(?TABLE_NAME, [public, named_table]),
    meck_setup().

unload() ->
    ets:delete(?TABLE_NAME),
    meck:unload(ns_config).

%% -------------------------
%% API - Snapshot Management
%% -------------------------
-spec update_snapshot(atom(), term()) -> true.
update_snapshot(Key, Value) ->
    OldSnapshot = get_ets_snapshot(),
    StoreSnapshot = misc:update_proplist(OldSnapshot, [{Key, Value}]),
    store_ets_snapshot(StoreSnapshot).

-spec update_snapshot(proplists:proplist()) -> true.
update_snapshot(NewSnapshot) when is_list(NewSnapshot) ->
    OldSnapshot = get_ets_snapshot(),
    StoreSnapshot = misc:update_proplist(OldSnapshot, NewSnapshot),
    store_ets_snapshot(StoreSnapshot).

%% -------------------------------
%% Internal - ns_config meck setup
%% -------------------------------
meck_setup() ->
    %% No passthrough, causes headaches as local functions can't be
    %% intercepted by meck, so we must control all entry to ns_config.
    meck:new(ns_config),

    meck:expect(ns_config, latest, fun() -> ?NS_CONFIG_LATEST_MARKER end),

    meck_setup_getters(),
    meck_setup_setters(),

    %% This function is slightly interesting. It uses some "fold" function in
    %% ns_config that requires the snapshot to be in a specific format.
    %% The snapshot format is trivial to map to, so rather than re-implement
    %% (simple as the logic is) just map the snapshot to the expected format
    %% and pass it through to the base function, which should stop this from
    %% ever getting out of sync.
    meck:expect(ns_config, get_node_uuid_map,
        fun(?NS_CONFIG_LATEST_MARKER) ->
                meck:passthrough([[get_ets_snapshot()]]);
            (Snapshot) ->
                meck:passthrough([[Snapshot]])
        end).

meck_setup_getters() ->
    meck:expect(ns_config, get,
        fun() ->
            get_ets_snapshot()
        end),


    meck:expect(ns_config, get_timeout,
        fun(Key, Default) ->
            fetch_with_default_from_latest_snapshot(Key, Default)
        end),


    meck:expect(ns_config, read_key_fast,
        fun(Key, Default) ->
            fetch_with_default_from_latest_snapshot(Key, Default)
        end),


    meck:expect(ns_config, search,
        fun(Key) ->
            fetch_from_latest_snapshot(Key)
        end),
    meck:expect(ns_config, search,
        fun(?NS_CONFIG_LATEST_MARKER, Key) ->
                fetch_from_latest_snapshot(Key);
            (Snapshot, Key) ->
                fetch_from_snapshot(Snapshot, Key)
        end),
    meck:expect(ns_config, search,
        fun(?NS_CONFIG_LATEST_MARKER, Key, Default) ->
                %% search/3 remaps {value, Value} to Value.
                case fetch_with_default_from_latest_snapshot(Key, Default) of
                    {value, V} -> V;
                    Other -> Other
                end;
            (Snapshot, Key, Default) ->
                case fetch_with_default(Snapshot, Key, Default) of
                    {value, V} -> V;
                    Other -> Other
                end
        end),


    meck:expect(ns_config, search_node_with_default,
        fun(Key, Default) ->
            fetch_with_default_from_latest_snapshot(Key, Default)
        end).

meck_setup_setters() ->
    meck:expect(ns_config, update_key,
        fun(_Key, Fun) ->
            update_snapshot(Fun(get_ets_snapshot())),
            ok
        end),


    meck:expect(ns_config, set,
        fun(Key, Value) ->
            update_snapshot(Key, Value)
        end).

%% ----------------------------------
%% Internal - getter/setter functions
%% ----------------------------------
get_ets_snapshot() ->
    case ets:lookup(?TABLE_NAME, snapshot) of
        [{snapshot, Snapshot}] -> Snapshot;
        [] -> []
    end.

store_ets_snapshot(Snapshot) ->
    ets:insert(?TABLE_NAME, {snapshot, Snapshot}).

fetch_from_snapshot(Snapshot, Key)  ->
    {value, proplists:get_value(Key, Snapshot)}.

fetch_from_latest_snapshot(Key) ->
    fetch_from_snapshot(get_ets_snapshot(), Key).

fetch_with_default(Snapshot, Key, Default) ->
    case proplists:get_value(Key, Snapshot, undefined) of
        undefined -> Default;
        V -> {value, V}
    end.

fetch_with_default_from_latest_snapshot(Key, Default) ->
    fetch_with_default(get_ets_snapshot(), Key, Default).
