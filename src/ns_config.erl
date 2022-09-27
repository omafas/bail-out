% Copyright (c) 2009-2019, Couchbase, Inc.
% Copyright (c) 2008, Cliff Moon
% Copyright (c) 2008, Powerset, Inc
%
% Use of this software is governed by a BSD-style license that can be
% found in licenses/BSD-moon.txt.
%
% Original Author: Cliff Moon
%
% But very very heavily mutated by Couchbase and couchbase since
% then. All bugs are couchbase's.

-module(ns_config).

-behaviour(gen_server).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_TIMEOUT, 15000).
-define(TERMINATE_SAVE_TIMEOUT, 10000).
-define(UPGRADE_TIMEOUT, ?get_timeout(upgrade, 240000)).

-export([uuid/1,
         start_link/2, start_link/1,
         sync/0,
         get/0, get/1,
         set/2, set/1,
         cas_remote_config/3,
         set_initial/2,
         update/1, update_with_vclocks/1, update_with_vclocks/2,
         update_key/2, update_key/3,
         update_sub_key/3, set_sub/2,
         search_node/3, search_node/2, search_node/1,
         search_node_prop/3, search_node_prop/4,
         search_node_prop/5,
         search/3, search/2, search/1,
         search_prop/3, search_prop/4,
         search_raw/2,
         search_with_vclock/2,
         run_txn/1, run_txn_with_config/2,
         clear/1,
         merge_kv_pairs/3,
         sync_announcements/0,
         get_kv_list/0, get_kv_list/1, get_kv_list_with_config/1,
         upgrade_config_explicitly/1, config_version_token/0,
         fold/3, read_key_fast/2, get_timeout/2,
         delete/1,
         regenerate_node_uuid/0,
         get_node_uuid_map/1,
         strip_metadata/1, extract_vclock/1, build_vclock/2,
         latest/0,
         merge_dynamic_and_static/0,
         search_node_with_default/2,
         search_node_with_default/3,
         search_node_with_default/4,
         reload/0]).

-export([compute_global_rev/1]).

-export([save_config_sync/1, save_config_sync/2, do_not_save_config/1]).

% Exported for tests only
-ifdef(TEST).
-export([save_file/3, load_config/3,
         load_file/2, send_config/2,
         test_setup/1, upgrade_config/2]).
-export([mock_tombstone_agent/0, unmock_tombstone_agent/0]).
-endif.

% A static config file is often hand edited.
% potentially with in-line manual comments.
%
% A dynamic config file is system generated and modified,
% such as due to changes from UI/admin-screen operations, or
% nodes getting added/removed, and gossiping about config
% information.
%
-include("ns_config.hrl").

%% gen_server callbacks

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([stop/0, resave/0, reannounce/0]).

%% state sanitization
-export([format_status/2]).

format_status(_Opt, [_PDict, State]) ->
    ns_config_log:sanitize(State).

%% API

uuid(#config{uuid = UUID}) ->
    UUID.

start_link(Full) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Full, []).

start_link(ConfigPath, PolicyMod) -> start_link([ConfigPath, PolicyMod]).

stop()       -> gen_server:cast(?MODULE, stop).
resave()     -> gen_server:call(?MODULE, resave).
reannounce() -> gen_server:call(?MODULE, reannounce).

sync() ->
    gen_server:call(?MODULE, sync, ?DEFAULT_TIMEOUT).

% ----------------------------------------

% Set & get configuration KVList, or [{Key, Val}*].
%
% ----------------------------------------

%% Set a value that will be overridden by any merged config
set_initial(Key, Value) ->
    ok = update_with_changes(fun (Config, _) ->
                                     NewPair = {Key, Value},
                                     {[NewPair], [NewPair | lists:keydelete(Key, 1, Config)]}
                             end).

update_config_key_rec(Key, Value, Rest, UUID, AccList) ->
    case Rest of
        [{Key, OldValue} = OldPair | XX] ->
            NewPair = case strip_metadata(OldValue) =:= strip_metadata(Value) of
                          true ->
                              OldPair;
                          _ ->
                              {Key, increment_vclock(Value, OldValue, UUID)}
                      end,
            [NewPair | lists:reverse(AccList, XX)];
        [Pair | XX2] ->
            update_config_key_rec(Key, Value, XX2, UUID, [Pair | AccList]);
        [] ->
            none
    end.

%% updates KVList with {Key, Value}. Places new tuple at the beginning
%% of list and removes old version for rest of list
update_config_key(Key, Value, KVList, UUID) ->
    case update_config_key_rec(Key, Value, KVList, UUID, []) of
        none -> [{Key, attach_vclock(Value, UUID)} | KVList];
        NewList -> NewList
    end.

%% Replaces config key-value pairs by NewConfig if they're still equal
%% to OldConfig. Returns true on success.
cas_remote_config(NewConfig, TouchedKeys, OldConfig) ->
    gen_server:call(?MODULE, {cas_config, NewConfig, TouchedKeys, OldConfig, remote}).

cas_local_config(NewConfig, OldConfig) ->
    gen_server:call(?MODULE, {cas_config, NewConfig, [], OldConfig, local}).

set(Key, Value) ->
    ok = update_with_changes(fun (Config, UUID) ->
                                     NewList = update_config_key(Key, Value, Config, UUID),
                                     {[hd(NewList)], NewList}
                             end).

%% gets current config. Runs Body on it to get new config, then tries
%% to cas new config returning retry_needed if it fails
-spec run_txn(fun((ConfigKVList :: [[term()]],
                   UpdateFn :: fun((Key :: term(), Value :: term(), Cfg :: [term()]) -> NewConfig :: [[term()]]))
                  -> {commit, ConfigKVList :: [[term()]]} |
                     {commit, ConfigKVList :: [[term()]], term()} | {abort, any()})) ->
                     run_txn_return().
run_txn(Body) ->
    run_txn_loop(Body, 10).

run_txn_with_config(Config, Body) ->
    run_txn_iter(Config, Body).

run_txn_iter(FullConfig, Body) ->
    UUID = uuid(FullConfig),
    Cfg = [get_kv_list_with_config(FullConfig)],

    SetFun = fun (Key, Value, Config) ->
                     run_txn_set(Key, Value, Config, UUID)
             end,

    case Body(Cfg, SetFun) of
        {commit, [NewCfg]} ->
            case cas_local_config(NewCfg, hd(Cfg)) of
                true -> {commit, [NewCfg]};
                false -> retry_needed
            end;
        {commit, [NewCfg], Extra} ->
            case cas_local_config(NewCfg, hd(Cfg)) of
                true -> {commit, [NewCfg], Extra};
                false -> retry_needed
            end;
        {abort, _} = AbortRV ->
            AbortRV
    end.

run_txn_loop(_Body, 0) ->
    retry_needed;
run_txn_loop(Body, RetriesLeft) ->
    FullConfig = ns_config:get(),
    case run_txn_iter(FullConfig, Body) of
        retry_needed ->
            run_txn_loop(Body, RetriesLeft -1);
        Other ->
            Other
    end.

run_txn_set(Key, Value, [KVList], UUID) ->
    [update_config_key(Key, Value, KVList, UUID)].

%% Updates Config with list of {Key, Value} pairs. Places new pairs at
%% the beginning of new list and removes old occurences of that keys.
%% Returns pair: {NewPairs, NewConfig}, where NewPairs is list of
%% updated KV pairs (with updated vclocks, if needed).
%%
%% Last parameter is accumulator. It's appended to NewPairs list.
set_kvlist([], Config, _UUID, NewPairs) ->
    {NewPairs, Config};
set_kvlist([{Key, Value} | Rest], Config, UUID, NewPairs) ->
    NewList = update_config_key(Key, Value, Config, UUID),
    set_kvlist(Rest, NewList, UUID, [hd(NewList) | NewPairs]).

set([]) ->
    ok;
set(KVList) when is_list(KVList) ->
    ok = update_with_changes(fun (Config, UUID) ->
                                     set_kvlist(KVList, Config, UUID, [])
                             end).

delete(Keys) when is_list(Keys) ->
    set([{K, ?DELETED_MARKER} || K <- Keys]);
delete(Key) ->
    delete([Key]).

regenerate_node_uuid() ->
    gen_server:call(?MODULE, regenerate_node_uuid).

get_node_uuid_map(Config) ->
    fold(
      fun (Key, Value, Acc) ->
              case Key of
                  {node, Node, uuid} ->
                      dict:store(Node, Value, Acc);
                  _ ->
                      Acc
              end
      end, dict:new(), Config).


%% update config by applying Fun to it. Fun should return a pair
%% {NewPairs, NewConfig} where NewConfig is new config and NewPairs is
%% list of changed pairs. That list of changed pairs is announced via
%% ns_config_events.
update_with_changes(Fun) ->
    gen_server:call(?MODULE, {update_with_changes, Fun}).

%% updates config by applying Fun to every {Key, Value} pair. Fun should
%% return either new pair or one of deletion markers. In the former case the
%% pair is replaced with it's new value. In the latter case depending on the
%% marker the pair is either completely or softly removed from config.
%%
%% The order in which function is called on the kv pairs is undefined. If the
%% function renames certain key to the one that is already present in the
%% config, then the it will be overwritten and the function won't be called on
%% the overwritten key/value pair. This means that caller is not able to do
%% things like swapping two values using this primitive.
%%
%% Function returns a pair {NewPairs, NewConfig} where NewConfig is
%% new config and NewPairs is list of changed pairs.
do_update_rec(_Fun, Acc, [], _UUID, NewConfig, NewPairs, Erased) ->
    {NewPairs, Erased, lists:reverse(NewConfig), Acc};
do_update_rec(Fun, Acc, [Pair | Rest], UUID, NewConfig, NewPairs, Erased) ->
    {Key, Value} = Pair,
    {Action, NewAcc} =
        Fun(Key, strip_metadata(Value), extract_vclock(Value), Acc),

    case Action of
        skip ->
            do_update_rec(Fun, NewAcc, Rest, UUID,
                          [Pair | NewConfig], NewPairs, Erased);
        erase ->
            do_update_rec(Fun, NewAcc, Rest, UUID,
                          NewConfig, NewPairs, [Key | Erased]);
        delete ->
            NewPair = {Key, increment_vclock(?DELETED_MARKER, Value, UUID)},
            do_update_rec(Fun, NewAcc, Rest, UUID,
                          [NewPair | NewConfig], [NewPair | NewPairs], Erased);
        {update, {NewKey, NewValue}} ->
            NewPair = {NewKey, increment_vclock(NewValue, Value, UUID)},
            do_update(Fun, NewAcc, Rest, UUID,
                      NewConfig, NewPairs, Erased, Pair, NewPair);
        {set_initial, NewPair} ->
            do_update(Fun, NewAcc, Rest, UUID,
                      NewConfig, NewPairs, Erased, Pair, NewPair);
        {set_fresh, {NewKey, NewValue}} ->
            NewPair = {NewKey, attach_vclock(NewValue, UUID)},
            do_update(Fun, NewAcc, Rest, UUID,
                      NewConfig, NewPairs, Erased, Pair, NewPair)
    end.

do_update(Fun, Acc, Rest, UUID,
          NewConfig, NewPairs, Erased, OldPair, NewPair) ->
    {OldKey, _} = OldPair,
    {NewKey, _} = NewPair,

    {Rest1, NewConfig1, NewPairs1} =
        case NewKey =:= OldKey of
            true ->
                {Rest, NewConfig, NewPairs};
            false ->
                %% key has changed; so we need to remove potential
                %% duplicates from rest of the config or from already
                %% processed part of it
                {lists:keydelete(NewKey, 1, Rest),
                 lists:keydelete(NewKey, 1, NewConfig),
                 lists:keydelete(NewKey, 1, NewPairs)}
        end,

    do_update_rec(Fun, Acc, Rest1, UUID,
                  [NewPair | NewConfig1], [NewPair | NewPairs1], Erased).

update(Fun) ->
    update_with_vclocks(
      fun (Key, Value, _VClock) ->
              Fun({Key, Value})
      end).

update_with_vclocks(Fun) ->
    {ok, _} =
        update_with_vclocks(
          fun (Key, Value, VClock, Acc) ->
                  {Fun(Key, Value, VClock), Acc}
          end, unused),

    ok.

update_with_vclocks(Fun, Acc) ->
    update_with_changes(
      fun (Config, UUID) ->
              do_update_rec(Fun, Acc, Config, UUID, [], [], [])
      end).

%% Applies given Fun to value of given Key. The Key must exist.
-spec update_key(term(), fun((term()) -> term())) ->
                        ok | {error | exit | throw, any(), any()}.
update_key(Key, Fun) ->
    update_with_changes(fun (Config, UUID) ->
                                case update_key_inner(Config, UUID, Key, Fun) of
                                    false ->
                                        erlang:throw({config_key_not_found, Key});
                                    V ->
                                        V
                                end
                        end).

update_key(Key, Fun, Default) ->
    update_with_changes(
      fun (Config, UUID) ->
              case update_key_inner(Config, UUID, Key, Fun) of
                  false ->
                      case Default of
                          ?DELETED_MARKER ->
                              {[], Config};
                          _ ->
                              NewConfig = update_config_key(Key, Default, Config, UUID),
                              {[hd(NewConfig)], NewConfig}
                      end;
                  V ->
                      V
              end
      end).

update_key_inner(Config, UUID, Key, Fun) ->
    case lists:keyfind(Key, 1, Config) of
        false ->
            false;
        {_, OldValue} ->
            case strip_metadata(OldValue) of
                ?DELETED_MARKER ->
                    false;
                StrippedValue ->
                    case Fun(StrippedValue) of
                        StrippedValue ->
                            {[], Config};
                        NewValue ->
                            NewConfig = update_config_key(Key, NewValue, Config,
                                                          UUID),
                            {[hd(NewConfig)], NewConfig}
                    end
            end
    end.


-spec update_sub_key(term(), term(), fun((term()) -> term())) ->
                            ok | {error | exit | throw, any(), any()}.
update_sub_key(Key, SubKey, Fun) ->
    update_key(Key, fun (PList) ->
                            RV = misc:key_update(SubKey, PList, Fun),
                            case RV of
                                false -> PList;
                                _ -> RV
                            end
                    end).

%% Set subkeys of certain key in config. If some of the subkeys do not exist
%% they are created.
set_sub(Key, SubKVList) ->
    ok = update_key(Key,
                    fun (PList) ->
                            set_sub_kvlist(PList, SubKVList)
                    end, SubKVList).

set_sub_kvlist(PList, []) ->
    PList;
set_sub_kvlist(PList, [ {SubKey, Value} | Rest ]) ->
    Replace = fun (_) -> Value end,
    NewPList =
        case misc:key_update(SubKey, PList, Replace) of
            false ->
                [ {SubKey, Value} | PList ];
            RV -> RV
        end,
    set_sub_kvlist(NewPList, Rest).


clear(Keep) -> gen_server:call(?MODULE, {clear, Keep}).

reload() ->
    gen_server:call(?MODULE, reload).

% ----------------------------------------

% Returns an opaque Config object that's a snapshot of the configuration.
% The Config object can be passed to the search*() related set
% of functions.

get() ->
    diag_handler:diagnosing_timeouts(
      fun () -> ns_config:get(?DEFAULT_TIMEOUT) end).

get(Timeout) ->
    gen_server:call(?MODULE, get, Timeout).

-spec get_kv_list() -> [{term(), term()}].
get_kv_list() -> get_kv_list(?DEFAULT_TIMEOUT).

-spec get_kv_list(timeout()) -> [{term(), term()}].
get_kv_list(Timeout) -> get_kv_list_with_config(ns_config:get(Timeout)).

get_kv_list_with_config(Config) ->
    config_dynamic(Config).

% ----------------------------------------

search(Key) ->
    case ets:lookup(ns_config_ets_dup, Key) of
        [{_, ?DELETED_MARKER}] ->
            false;
        [{_, V}] ->
            {value, V};
        _ ->
            false
    end.

read_key_fast(Key, Default) ->
    case search(Key) of
        {value, V} ->
            V;
        false ->
            Default
    end.

get_timeout(Operation, Default) ->
    search_node_with_default({timeout, Operation}, Default).

search_node(Key) -> search_node(latest(), Key).

search('latest-config-marker', Key) ->
    search(Key);
search(Config, Key) ->
    case search_raw(Config, Key) of
        {value, X} ->
            case strip_metadata(X) of
                ?DELETED_MARKER ->
                    false;
                V ->
                    {value, V}
            end;
        false -> false
    end.

search(Config, Key, Default) ->
    case search(Config, Key) of
        {value, V} ->
            V;
        false ->
            Default
    end.

search_with_vclock_kvlist([], _Key) -> false;
search_with_vclock_kvlist([KVList | Rest], Key) ->
    case lists:keyfind(Key, 1, KVList) of
        {_, RawValue} ->
            Clock = extract_vclock(RawValue),
            Value = strip_metadata(RawValue),

            {value, Value, Clock};
        false ->
            search_with_vclock_kvlist(Rest, Key)
    end.

get_static_and_dynamic(#config{dynamic = DL, static = SL}) -> [hd(DL) | SL];
get_static_and_dynamic([DL]) -> [DL].

search_with_vclock(Config, Key) ->
    LL = get_static_and_dynamic(Config),
    search_with_vclock_kvlist(LL, Key).

search_node(Config, Key) ->
    search_node(node(), Config, Key).

search_node_with_default(Key, Default) ->
    search_node_with_default(latest(), Key, Default).

search_node_with_default(Config, Key, Default) ->
    search_node_with_default(node(), Config, Key, Default).

search_node_with_default(Node, Config, Key, Default) ->
    case search_node(Node, Config, Key) of
        {value, V} ->
            V;
        false ->
            Default
    end.

search_node(Node, Config, Key) ->
    case search(Config, {node, Node, Key}) of
        {value, _} = V -> V;
        false          -> search(Config, Key)
    end.

% Returns the Value or undefined.

search_prop(Config, Key, SubKey) ->
    search_prop(Config, Key, SubKey, undefined).

search_node_prop(Config, Key, SubKey) ->
    search_node_prop(node(), Config, Key, SubKey, undefined).

% Returns the Value or the DefaultSubVal.

search_prop(Config, Key, SubKey, DefaultSubVal) ->
    case search(Config, Key) of
        {value, PropList} ->
            proplists:get_value(SubKey, PropList, DefaultSubVal);
        false ->
            DefaultSubVal
    end.

search_node_prop('latest-config-marker', Key, SubKey, DefaultSubVal) ->
    search_node_prop(node(), 'latest-config-marker', Key, SubKey, DefaultSubVal);
search_node_prop(Node, Config, Key, SubKey) when is_atom(Node) ->
    search_node_prop(Node, Config, Key, SubKey, undefined);
search_node_prop(Config, Key, SubKey, DefaultSubVal) ->
    search_node_prop(node(), Config, Key, SubKey, DefaultSubVal).

search_node_prop(Node, Config, Key, SubKey, DefaultSubVal) ->
    case search_node(Node, Config, Key) of
        {value, PropList} ->
            case proplists:lookup(SubKey, PropList) of
                none ->
                    search_prop(Config, Key, SubKey, DefaultSubVal);
                {SubKey, Val} ->
                    Val
            end;
        false ->
            DefaultSubVal
    end.

% The search_raw API does not strip out metadata from results.

search_raw(undefined, _Key) -> false;
search_raw([], _Key)        -> false;
search_raw([KVList | Rest], Key) ->
    case lists:keysearch(Key, 1, KVList) of
        {value, {Key, V}} -> {value, V};
        _                 -> search_raw(Rest, Key)
    end;
search_raw(#config{dynamic = DL, static = SL}, Key) ->
    case search_raw(DL, Key) of
        {value, _} = R -> R;
        false          -> search_raw(SL, Key)
    end.

upgrade_config_explicitly(Upgrader) ->
    gen_server:call(?MODULE, {upgrade_config_explicitly, Upgrader},
                    ?UPGRADE_TIMEOUT).

config_version_token() ->
    {ets:lookup(ns_config_announces_counter, changes_counter), erlang:whereis(?MODULE)}.

fold(_Fun, Acc, undefined) ->
    Acc;
fold(_Fun, Acc, []) ->
    Acc;
fold(Fun, Acc0, [KVList | Rest]) ->
    Acc = lists:foldl(
            fun ({Key, Value}, Acc1) ->
                    case strip_metadata(Value) of
                        ?DELETED_MARKER ->
                            Acc1;
                        V ->
                            Fun(Key, V, Acc1)
                    end
            end, Acc0, KVList),
    fold(Fun, Acc, Rest);
fold(Fun, Acc, #config{dynamic = DL, static = SL}) ->
    fold(Fun, fold(Fun, Acc, SL), DL);
fold(Fun, Acc, 'latest-config-marker') ->
    fold(Fun, Acc, ns_config:get()).

%% Implementation

% Removes metadata like METADATA_VCLOCK from results.
strip_metadata([{?METADATA_VCLOCK, _} | Rest]) ->
    Rest;
strip_metadata([{?METADATA_VCLOCK, _, _} | Rest]) ->
    Rest;
strip_metadata(Value) ->
    Value.

extract_vclock([First | _]) ->
    case First of
        {?METADATA_VCLOCK, Clock} ->
            {0, Clock};
        {?METADATA_VCLOCK, PurgeTS, Clock} ->
            {PurgeTS, Clock};
        _ ->
            {0, []}
    end;
extract_vclock(_) ->
    {0, []}.

build_vclock(0, Clock) ->
    {?METADATA_VCLOCK, Clock};
build_vclock(PurgeTS, Clock) ->
    {?METADATA_VCLOCK, PurgeTS, Clock}.

%% Increment the vclock in V2 and replace the one in V1
increment_vclock(NewValue, OldValue, Node) ->
    TS = tombstone_agent:vclock_ts(),
    {PurgeTS, OldVClock} = extract_vclock(OldValue),
    NewVClock = lists:sort(vclock:increment(Node, TS, OldVClock)),
    [build_vclock(PurgeTS, NewVClock) | strip_metadata(NewValue)].

%% Set the vclock in NewValue to one that descends from both
merge_vclocks(NewValue, OldValue) ->
    {PurgeTS, NewValueVClock} = extract_vclock(NewValue),
    {_, OldValueVClock} = extract_vclock(OldValue),
    case NewValueVClock =:= [] andalso
        OldValueVClock =:= [] andalso PurgeTS =:= 0 of
        true ->
            NewValue;
        _ ->
            NewVClock = lists:sort(vclock:merge([OldValueVClock, NewValueVClock])),
            [build_vclock(PurgeTS, NewVClock) | strip_metadata(NewValue)]
    end.

attach_vclock(Value, Node) ->
    TS = tombstone_agent:vclock_ts(),
    VClock = lists:sort(vclock:increment(Node, TS, vclock:fresh())),
    PurgeTS = tombstone_agent:purge_ts(),
    [build_vclock(PurgeTS, VClock) | strip_metadata(Value)].

%% NOTE: this function is not supposed to be used widely. It won't
%% "scale" with size of config. It is ok with existing limits of
%% config, but before we're able to switch to newer config we might
%% have to adapt users of this function to use some other way to track
%% "revision" of config they see. (Or not, if other config has some
%% "natural" way to track revision of data, e.g. ZAB's/RAFT's txn ids
%% or equivalent multi-paxos thing)
compute_global_rev('latest-config-marker') ->
    compute_global_rev(ns_config:get());
compute_global_rev(Config) ->
    KVList = config_dynamic(Config),
    lists:foldl(
      fun ({{local_changes_count, _}, Value}, Acc) ->
              %% local_changes_count never gets deleted, so it should be safe
              %% to ignore the purge timestamp
              {_, VC} = extract_vclock(Value),
              Acc + vclock:count_changes(VC);
          (_, Acc) ->
              Acc
      end, 0, KVList).

%% gen_server callbacks

upgrade_config(Config) ->
    Upgrader = fun (Cfg) ->
                       (Cfg#config.policy_mod):upgrade_config(Cfg)
               end,
    upgrade_config(Config, Upgrader).

upgrade_config(Config, Upgrader) ->
    do_upgrade_config(Config, Upgrader(Config), Upgrader).

upgrade_vclock(V, OldV, UUID) when is_list(OldV) ->
    case lists:keyfind(?METADATA_VCLOCK, 1, OldV) of
        false ->
            %% we encountered plenty of upgrade problems coming from the fact
            %% that both old and new values miss vclock;
            %% in this case the new value can be reverted by the old value
            %% replicated from not yet updated node;
            %% we solve this by attaching vclock to new value;
            attach_vclock(V, UUID);
        _ ->
            increment_vclock(V, OldV, UUID)
    end;
upgrade_vclock(V, _, UUID) ->
    attach_vclock(V, UUID).

do_upgrade_config(Config, [], _Upgrader) -> Config;
do_upgrade_config(#config{uuid = UUID} = Config, Changes, Upgrader) ->
    ?log_info("Upgrading config by changes:~n~p~n",
              [ns_config_log:sanitize(Changes)]),
    ConfigList = config_dynamic(Config),
    NewList =
        lists:foldl(
          fun (Change, Acc) ->
                  {K, V} = case Change of
                               {set, K0, V0} ->
                                   {K0, V0};
                               {delete, K0} ->
                                   {K0, ?DELETED_MARKER}
                           end,

                  case lists:keyfind(K, 1, Acc) of
                      false ->
                          case V of
                              ?DELETED_MARKER ->
                                  Acc;
                              _ ->
                                  [{K, attach_vclock(V, UUID)} | Acc]
                          end;
                      {K, OldV} ->
                          NewV = upgrade_vclock(V, OldV, UUID),
                          lists:keyreplace(K, 1, Acc, {K, NewV})
                  end
          end,
          ConfigList,
          Changes),
    NewConfig = Config#config{dynamic=[NewList]},
    do_upgrade_config(NewConfig, Upgrader(NewConfig), Upgrader).

bump_local_changes_counter_full(#config{uuid = UUID, dynamic = [KVList]} = Config) ->
    {RevPrefix, Tail} = bump_counter_rec(UUID, KVList, []),
    [{{local_changes_count, UUID}, _} = NewCounterPair | _] = Tail,
    NewKVList = lists:reverse(RevPrefix, Tail),
    {Config#config{dynamic = [NewKVList]}, NewCounterPair}.

bump_local_changes_counter(Config) ->
    {NewCfg, _} = bump_local_changes_counter_full(Config),
    NewCfg.

bump_counter_rec(UUID, [], Acc) ->
    Pair = {{local_changes_count, UUID}, increment_vclock([], [], UUID)},
    {Acc, [Pair]};
bump_counter_rec(UUID, [{K, V} | KVRest], Acc) ->
    case K of
        %% NOTE: that UUID is bound
        {local_changes_count, UUID} ->
            {Acc, [{K, increment_vclock([], V, UUID)} | KVRest]};
        _ ->
            bump_counter_rec(UUID, KVRest, [{K, V} | Acc])
    end.

do_init(Config) ->
    erlang:process_flag(trap_exit, true),

    %% NOTE: catch is needed because init may be called more than once via
    %% handle_call(reload,...) path
    (catch ets:new(ns_config_ets_dup, [public, set, named_table])),
    ets:delete_all_objects(ns_config_ets_dup),
    (catch ets:new(ns_config_announces_counter, [set, named_table])),
    ets:insert_new(ns_config_announces_counter, {changes_counter, 0}),
    UpgradedConfig = (Config#config.upgrade_config_fun)(Config),
    InitialState =
        if
            UpgradedConfig =/= Config ->
                ?log_debug("Upgraded initial config:~n~p~n", [ns_config_log:sanitize(UpgradedConfig)]),
                initiate_save_config(bump_local_changes_counter(UpgradedConfig));
            true ->
                UpgradedConfig
        end,
    update_ets_dup(config_dynamic(InitialState)),
    {ok, InitialState}.

init({with_state, LoadedConfig} = Init) ->
    do_init(LoadedConfig#config{init = Init});
init({full, ConfigPath, DirPath, PolicyMod} = Init) ->
    erlang:process_flag(priority, high),
    case load_config(ConfigPath, DirPath, PolicyMod) of
        {ok, Config} ->
            tombstone_agent:init_timestamps(Config),
            do_init(Config#config{init = Init,
                                  saver_mfa = {PolicyMod, encrypt_and_save, []},
                                  upgrade_config_fun = fun upgrade_config/1});
        Error ->
            {stop, Error}
    end;
init({pull_from_node, Node} = Init) ->
    KVList0 = duplicate_node_keys(ns_config_rep:get_remote(Node, infinity),
                                  Node, node()),
    {_, KVList} = drop_deletes(KVList0),
    Cfg = #config{dynamic = [KVList],
                  policy_mod = ns_config_default,
                  saver_mfa = {?MODULE, do_not_save_config, []},
                  upgrade_config_fun = fun (C) -> C end,
                  init = Init},
    do_init(Cfg);
init([ConfigPath, PolicyMod]) ->
    init({full, ConfigPath, undefined, PolicyMod}).

duplicate_node_keys(KVList, FromNode, ToNode) ->
    lists:flatmap(fun ({{node, Node, Key}, Value} = Val) when Node =:= FromNode ->
                          [{{node, ToNode, Key}, Value}, Val];
                      (Other) ->
                          [Other]
                  end, KVList).

-spec wait_saver(#config{}, infinity | non_neg_integer()) -> {ok, #config{}} | timeout.
wait_saver(State, Timeout) ->
    case State#config.saver_pid of
        undefined ->
            ?log_debug("Done waiting for saver."),
            {ok, State};
        Pid ->
            ?log_debug("Waiting for running saver"),
            receive
                {'EXIT', Pid, _Reason} = X ->
                    ?log_debug("Got exit from saver: ~p", [X]),
                    {noreply, NewState} = handle_info(X, State),
                    wait_saver(NewState, Timeout)
            after Timeout ->
                    timeout
            end
    end.

terminate(Reason, State) ->
    ?log_debug("Config is terminating with reason ~p", [Reason]),
    case wait_saver(State, ?TERMINATE_SAVE_TIMEOUT) of
        timeout ->
            ale:warn(?USER_LOGGER,
                     "Termination wait for ns_config saver process timed out.");
        _ -> ok
    end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_cast(stop, State) ->
    {stop, shutdown, State}.

handle_info({'EXIT', Pid, Reason},
            #config{saver_pid = MyPid,
                    pending_more_save = NeedMore} = State) when MyPid =:= Pid ->
    NewState = State#config{saver_pid = undefined},
    case Reason of
        normal ->
            ok;
        _ ->
            ?log_error("Saving ns_config failed. Trying to ignore: ~p", [Reason])
    end,
    S = case NeedMore of
            true ->
                initiate_save_config(NewState);
            false ->
                NewState
        end,
    {noreply, S};
handle_info(Info, State) ->
    ?log_warning("Unhandled message: ~p", [Info]),
    {noreply, State}.

handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(reload, _From, State) ->
    ?log_debug("Reload config"),
    wait_saver(State, infinity),
    case init(State#config.init) of
        {ok, State2} ->
            {reply, ok, State2};
        {stop, Error} ->
            ale:warn(?USER_LOGGER, "reload failed: ~p", [Error]),
            {reply, {error, Error}, State}
    end;

handle_call(resave, _From, State) ->
    {reply, ok, initiate_save_config(State)};

handle_call(reannounce, _From, State) ->
    %% we have to assume those are all genuine just made local changes
    announce_locally_made_changes(config_dynamic(State)),
    {reply, ok, State};

handle_call(get, _From, State) ->
    {reply, State, State};

handle_call(regenerate_node_uuid, From, State) ->
    NewUUID = couch_uuids:random(),
    Key = {node, node(), uuid},
    NewPair = {Key, attach_vclock(NewUUID, NewUUID)},
    ?log_debug("Regenerated node UUID: ~p ~n", [NewUUID]),
    Fun =
        fun (Config, _) ->
                {[NewPair], [NewPair | lists:keydelete(Key, 1, Config)]}
        end,
    {reply, ok, NewState} = handle_call({update_with_changes, Fun}, From,
                                        State),
    {reply, ok, NewState#config{uuid=NewUUID}};

handle_call({update_with_changes, Fun}, _From, #config{uuid = UUID} = State) ->
    OldList = config_dynamic(State),
    case do_update_with_changes(Fun, OldList, UUID) of
        {ok, NewPairs, Erased, NewConfig, Reply} ->
            case {NewPairs, Erased} of
                {[], []} ->
                    {reply, Reply, State};
                {_, _} ->
                    NewState = State#config{dynamic=[NewConfig]},

                    {FinalState, FinalPairs} =
                        case NewPairs =/= [] of
                            true ->
                                %% Bump the counter only if there are real
                                %% (non-erase changes).
                                {NewState1, CounterPair} =
                                    bump_local_changes_counter_full(NewState),
                                {NewState1, [CounterPair | NewPairs]};
                            false ->
                                {NewState, NewPairs}
                        end,

                    erase_ets_dup(Erased),
                    update_ets_dup(FinalPairs),
                    announce_locally_made_changes(FinalPairs),
                    {reply, Reply, initiate_save_config(FinalState)}
            end;
        {error, Error} ->
            {reply, Error, State}
    end;

handle_call({clear, Keep}, From, State) ->
    false = lists:member({node, node(), uuid}, Keep),

    NewUUID = couch_uuids:random(),
    NewList0 = lists:filtermap(
                 fun({K, V}) ->
                         case lists:member(K, Keep) of
                             true ->
                                 {true, {K, attach_vclock(V, NewUUID)}};
                             false ->
                                 false
                         end
                 end,
                 config_dynamic(State)),
    NewList = [{{node, node(), uuid}, attach_vclock(NewUUID, NewUUID)} | NewList0],
    {reply, _, NewState} = handle_call(resave, From,
                                       State#config{dynamic=[NewList],
                                                    uuid=NewUUID}),
    RV = handle_call(reload, From, NewState),
    ?log_debug("Full result of clear:~n~p", [ns_config_log:sanitize(RV)]),
    RV;

handle_call({merge_ns_couchdb_config, NewKVList0, FromNode}, _From, State) ->
    NewKVList1 = lists:sort(duplicate_node_keys(NewKVList0, FromNode, node())),
    OldKVList = config_dynamic(State),
    NewKVList = misc:ukeymergewith(fun (New, _Old) -> New end,
                                   1, NewKVList1, lists:sort(OldKVList)),
    C = {cas_config, NewKVList, [], OldKVList, remote},
    {reply, true, NewState0} = handle_call(C, [], State),

    %% {cas_config, ..} above would have announced any deletions if anybody
    %% cares about them. Now we can drop them.
    #config{dynamic = [KVList]} = NewState0,
    {Deletes, FinalKVList} = drop_deletes(KVList),
    erase_ets_dup(Deletes),
    NewState = NewState0#config{dynamic = [FinalKVList]},

    {reply, ok, NewState};

handle_call(merge_dynamic_and_static, _From, State) ->
    OldDynamic = config_dynamic(State),
    NewDynamic = do_merge_dynamic_and_static([OldDynamic], State),
    C = {cas_config, NewDynamic, [], OldDynamic, remote},
    {reply, true, NewState} = handle_call(C, [], State),
    {reply, ok, NewState};

handle_call({cas_config, NewKVList, ExtraLocalChanges, OldKVList, Type},
            _From, State) ->
    case OldKVList =:= hd(State#config.dynamic) of
        true ->
            HaveExtraLocalChanges = (ExtraLocalChanges =/= []),

            NewState0 = State#config{dynamic = [NewKVList]},
            NewState =
                case {Type, HaveExtraLocalChanges} of
                    {local, _} ->
                        bump_local_changes_counter(NewState0);
                    {remote, true} ->
                        bump_local_changes_counter(NewState0);
                    {remote, false} ->
                        NewState0
                end,

            Diff = config_dynamic(NewState) -- OldKVList,
            update_ets_dup(Diff),

            {LocalDiff, RemoteDiff} =
                case {Type, HaveExtraLocalChanges} of
                    {local, _} ->
                        {Diff, []};
                    {remote, false} ->
                        {[], Diff};
                    {remote, true} ->
                        %% if we reach here, we definitely bumped local change
                        %% counter, so we need to make sure it's replicated
                        %% immediately too
                        ToReplicate =
                            [{local_changes_count, uuid(State)} |
                             ExtraLocalChanges],

                        lists:partition(
                          fun ({K, _}) ->
                                  lists:member(K, ToReplicate)
                          end, Diff)
                end,

            announce_locally_made_changes(LocalDiff),
            announce_changes(RemoteDiff),

            {reply, true, initiate_save_config(NewState)};
        _ ->
            {reply, false, State}
    end;

handle_call({upgrade_config_explicitly, Upgrader}, _From, State) ->
    OldKVList = config_dynamic(State),
    NewConfig0 = upgrade_config(State, Upgrader),

    case OldKVList =:= config_dynamic(NewConfig0) of
        true ->
            {reply, ok, State};
        false ->
            NewConfig = bump_local_changes_counter(NewConfig0),
            NewKVList = config_dynamic(NewConfig),
            Diff = NewKVList -- OldKVList,

            update_ets_dup(Diff),
            announce_locally_made_changes(Diff),
            {reply, ok, initiate_save_config(NewConfig)}
    end.


%%--------------------------------------------------------------------

% TODO: We're currently just taking the first dynamic KVList,
%       and should instead be smushing all the dynamic KVLists together?
config_dynamic(#config{dynamic = [X | _]}) -> X;
config_dynamic(#config{dynamic = []})      -> [].

%%--------------------------------------------------------------------

dynamic_config_path(DirPath) ->
    C = filename:join(DirPath, "config.dat"),
    ok = filelib:ensure_dir(C),
    C.

merge_dynamic_and_static() ->
    gen_server:call(?MODULE, merge_dynamic_and_static, infinity).

do_merge_dynamic_and_static(Dynamic, #config{static = [S, DefaultConfig], uuid = UUID}) ->
    DefaultConfigWithVClocks =
        lists:map(
          fun ({{node, Node, _} = K, V}) when Node =:= node() ->
                  {K, attach_vclock(V, UUID)};
              (Other) ->
                  Other
          end, DefaultConfig),

    {_, DynamicPropList} = lists:foldl(fun (Tuple, {Seen, Acc}) ->
                                               K = element(1, Tuple),
                                               case sets:is_element(K, Seen) of
                                                   true -> {Seen, Acc};
                                                   false -> {sets:add_element(K, Seen),
                                                             [Tuple | Acc]}
                                               end
                                       end,
                                       {sets:from_list([directory]), []},
                                       lists:append(Dynamic ++ [S, DefaultConfigWithVClocks])),
    DynamicPropList.

load_config(ConfigPath, DirPath, PolicyMod) ->
    DefaultConfig = PolicyMod:default(),
    % Static config file.
    ?log_info("Loading static config from ~p", [ConfigPath]),
    case load_file(txt, ConfigPath) of
        {ok, S} ->
            % Dynamic data directory.
            DirPath2 =
                case DirPath of
                    undefined ->
                        {value, DP} = search([S, DefaultConfig], directory),
                        DP;
                    _ -> DirPath
                end,
            % Dynamic config file.
            C = dynamic_config_path(DirPath2),
            ok = filelib:ensure_dir(C),
            ?log_info("Loading dynamic config from ~p", [C]),
            Dynamic0 = case load_file(bin, C) of
                           {ok, DRead} ->
                               PolicyMod:decrypt(DRead);
                           not_found ->
                               ?log_info("No dynamic config file found. Assuming we're brand new node"),
                               [[]]
                       end,
            ?log_debug("Here's full dynamic config we loaded:~n~p", [ns_config_log:sanitize(Dynamic0)]),

            {UUID, Dynamic1} =
                case search(Dynamic0, {node, node(), uuid}) of
                    false ->
                        UUID0 = couch_uuids:random(),
                        UUIDTuple = {{node, node(), uuid}, attach_vclock(UUID0, UUID0)},

                        [KVs | RestKVs] = Dynamic0,
                        KVs1 = [UUIDTuple | KVs],

                        {UUID0, [KVs1 | RestKVs]};
                    {value, UUID0} ->
                        {UUID0, Dynamic0}
                end,

            Config1 = #config{static = [S, DefaultConfig],
                              policy_mod = PolicyMod,
                              uuid = UUID},
            DynamicPropList = PolicyMod:fixup(
                                do_merge_dynamic_and_static(Dynamic1, Config1)),

            ?log_info("Here's full dynamic config we loaded + static & default config:~n~p",
                      [ns_config_log:sanitize(DynamicPropList)]),
            {ok, Config1#config{dynamic = [lists:keysort(1, DynamicPropList)]}};
        E ->
            ?log_error("Failed loading static config: ~p", [E]),
            E
    end.

save_config_sync(#config{dynamic = D}, DirPath) ->
    save_config_sync(D, DirPath);

save_config_sync(Dynamic, DirPath) when is_list(Dynamic) ->
    C = dynamic_config_path(DirPath),
    ok = save_file(bin, C, Dynamic),
    ok.

save_config_sync(Config) ->
    {value, DirPath} = search(Config, directory),
    save_config_sync(Config, DirPath).

do_not_save_config(_Config) ->
    ok.

initiate_save_config(Config) ->
    case Config#config.saver_pid of
        undefined ->
            {M, F, ASuffix} = Config#config.saver_mfa,
            A = [Config | ASuffix],
            Pid = proc_lib:spawn_link(M, F, A),
            Config#config{saver_pid = Pid,
                          pending_more_save = false};
        _ ->
            Config#config{pending_more_save = true}
    end.

announce_locally_made_changes([]) ->
    ok;
announce_locally_made_changes(KVList) ->
    announce_changes(KVList),
    gen_event:notify(ns_config_events_local, [K || {K, _} <- KVList]).

announce_changes([]) -> ok;
announce_changes(KVList) ->
    ets:update_counter(ns_config_announces_counter, changes_counter, 1),
    % Fire a event per changed key.
    lists:foreach(fun ({Key, Value}) ->
                          gen_event:notify(ns_config_events,
                                           {Key, strip_metadata(Value)})
                  end,
                  KVList),
    % Fire a generic event that 'something changed'.
    gen_event:notify(ns_config_events, KVList).

update_ets_dup(KVList) ->
    KVs = [{K, strip_metadata(V)} || {K, V} <- KVList],
    ets:insert(ns_config_ets_dup, KVs).

erase_ets_dup(Keys) ->
    lists:foreach(
      fun (Key) ->
              ets:delete(ns_config_ets_dup, Key)
      end, Keys).

load_file(txt, ConfigPath) -> read_includes(ConfigPath);

load_file(bin, ConfigPath) ->
    case file:read_file(ConfigPath) of
        {ok, <<>>} -> not_found;
        {ok, B}    -> {ok, binary_to_term(B)};
        _          -> not_found
    end.

save_file(bin, ConfigPath, X) ->
    TempFile = path_config:tempfile(filename:dirname(ConfigPath),
                                    filename:basename(ConfigPath),
                                    ".tmp"),
    {ok, F} = file:open(TempFile, [write, raw]),
    ok = file:write(F, term_to_binary(X)),
    ok = file:sync(F),
    ok = file:close(F),
    file:rename(TempFile, ConfigPath).

-define(TOUCHED_KEYS, touched_keys).
touch_key(Key) ->
    Current = erlang:get(?TOUCHED_KEYS),
    true = (Current =/= undefined),
    New = ordsets:add_element(Key, Current),
    erlang:put(?TOUCHED_KEYS, New),
    ok.

with_touched_keys(Body) ->
    try
        undefined = erlang:put(?TOUCHED_KEYS, ordsets:new()),
        RV = Body(),
        TouchedKeys = ordsets:to_list(erlang:get(?TOUCHED_KEYS)),
        {RV, TouchedKeys}
    after
        erlang:erase(?TOUCHED_KEYS)
    end.

-spec merge_kv_pairs(kvlist(), kvlist(), uuid()) -> {kvlist(), [key()]}.
merge_kv_pairs(RemoteKVList, LocalKVList, UUID) ->
    with_touched_keys(
      fun () ->
              do_merge_kv_pairs(RemoteKVList, LocalKVList, UUID)
      end).

-spec do_merge_kv_pairs(kvlist(), kvlist(), uuid()) -> kvlist().
do_merge_kv_pairs(RemoteKVList, LocalKVList, _UUID)
  when RemoteKVList =:= LocalKVList ->
    LocalKVList;
do_merge_kv_pairs(RemoteKVList, LocalKVList, UUID) ->
    RemoteKVList1 = lists:sort(RemoteKVList),
    LocalKVList1 = lists:sort(LocalKVList),
    Merger = fun (_, {directory, _} = LP) ->
                     LP;
                 ({_, [VClock | ?DELETED_MARKER]}, {{node, Node, _}, _LV} = LP)
                   when Node =:= node(), is_tuple(VClock),
                        element(1, VClock) =:= ?METADATA_VCLOCK ->
                     %% we don't allow incoming replications of
                     %% deletions of our per-node keys. This is
                     %% because they (deletions) are done as part of
                     %% ejecting us from cluster in which case we'll
                     %% detect that (via nodes_wanted) and leave
                     %% (resetting config).
                     %%
                     %% Allowing deletions in this case might break
                     %% things in this node preventing it from leaving
                     %% cluster.
                     LP;
                 ({_, RV} = RP, {{node, Node, Key} = K, LV} = LP) when Node =:= node() ->
                     %% we want to make sure that that no one is able to
                     %% modify our own UUID
                     Bounce = (Key =:= uuid),

                     case Bounce of
                         true ->
                             case RV =:= LV of
                                 true ->
                                     %% same values imply same vclocks
                                     %% so no real merge is needed
                                     LV = merge_vclocks(LV, RV),
                                     {K, LV};
                                 false ->
                                     ?log_debug("Special-casing incoming replication "
                                                "of my node key ~p and different value. "
                                                "Overriding remote with local:~n"
                                                "local = ~p~n"
                                                "remote = ~p", [K, LV, RV]),
                                     touch_key(K),
                                     {K, increment_vclock(LV, merge_vclocks(LV, RV), UUID)}
                             end;
                         false ->
                             merge_values(RP, LP)
                     end;
                 (RP, LP) ->
                     merge_values(RP, LP)
             end,
    misc:ukeymergewith(Merger, 1, RemoteKVList1, LocalKVList1).

-spec merge_values(kvpair(), kvpair()) -> kvpair().
merge_values({_K, RV} = RP, {_, LV} = _LP) when RV =:= LV -> RP;
merge_values({K, RV} = RP, {_, LV} = LP) ->
    {RPurgeTS, RClock} = extract_vclock(RV),
    {LPurgeTS, LClock} = extract_vclock(LV),

    case RPurgeTS =:= LPurgeTS of
        true ->
            case {vclock:descends(RClock, LClock),
                  vclock:descends(LClock, RClock)} of
                {X, X} ->
                    Merged =
                        case {strip_metadata(LV), strip_metadata(RV)} of
                            {X1, X1} ->
                                touch_key(K),
                                [Loser, Winner] = lists:sort([LV, RV]),
                                merge_vclocks(Winner, Loser);
                            {?DELETED_MARKER, _} ->
                                RV;
                            {_, ?DELETED_MARKER} ->
                                LV;
                            {_, _} ->
                                touch_key(K),
                                merge_values_using_timestamps(K,
                                                              LV, LClock,
                                                              RV, RClock)
                        end,
                    {K, Merged};
                {true, false} -> RP;
                {false, true} -> LP
            end;
        false ->
            %% Pick the value with the later timestamp, break ties using purge
            %% timestamps.
            RLatestTS = vclock:get_latest_timestamp(RClock),
            LLatestTS = vclock:get_latest_timestamp(LClock),

            [{_, Loser}, {_, Winner}] =
                lists:keysort(1,
                              [{{LLatestTS, LPurgeTS}, LV},
                               {{RLatestTS, RPurgeTS}, RV}]),

            ?log_debug("Purge timestamp conflict on field "
                       "~p:~n~p and~n~p, choosing the former.",
                       [K,
                        sanitize_just_value(K, Winner),
                        sanitize_just_value(K, Loser)]),

            {K, Winner}
    end.

sanitize_just_value(K, V) ->
    {_, Sanitized} = ns_config_log:sanitize({K, V}),
    Sanitized.

-spec merge_values_using_timestamps(key(),
                                    kvpair(), vclock(),
                                    kvpair(), vclock()) -> kvpair().
merge_values_using_timestamps(K, LV, LClock, RV, RClock) ->
    LocalTS = vclock:get_latest_timestamp(LClock),
    RemoteTS = vclock:get_latest_timestamp(RClock),

    case {LocalTS >= RemoteTS, RemoteTS >= LocalTS} of
        {X1, X1} ->
            [Winner, Loser] = lists:sort([LV, RV]),

            log_conflict(LV,
                         "Conflicting configuration changes to field "
                         "~p:~n~p and~n~p, choosing the former.",
                         [K,
                          sanitize_just_value(K, Winner),
                          sanitize_just_value(K, Loser)]),

            merge_vclocks(Winner, Loser);
        {LocalNewer, RemoteNewer} ->
            true = LocalNewer xor RemoteNewer,

            [Winner, Loser] =
                case LocalNewer of
                    true ->
                        [LV, RV];
                    false ->
                        [RV, LV]
                end,

            log_conflict(LV,
                         "Conflicting configuration changes to field "
                         "~p:~n~p and~n~p, choosing the former, "
                         "which looks newer.",
                         [K,
                          sanitize_just_value(K, Winner),
                          sanitize_just_value(K, Loser)]),

            merge_vclocks(Winner, Loser)
    end.

log_conflict(LocalValue, Fmt, Args) ->
    case strip_metadata(LocalValue) of
        ?DELETED_MARKER ->
            %% Historically we've been treating delete conflicts specially and
            %% didn't log anything. Since these sort of conflicts are very
            %% common when nodes are removed/readded, continue to treat
            %% this specially.
            ok;
        _ ->
            ?log_debug(Fmt, Args)
    end.

read_includes(Path) -> read_includes([{include, Path}], []).

read_includes([{include, Path} | Terms], Acc) ->
    case file:consult(Path) of
        {ok, IncTerms}  -> read_includes(IncTerms ++ Terms, Acc);
        {error, enoent} -> {error, {bad_config_path, Path}};
        Error           -> Error
    end;
read_includes([X | Rest], Acc) -> read_includes(Rest, [X | Acc]);
read_includes([], Result)      -> {ok, lists:reverse(Result)}.

%% waits till all config change notifications are processed by
%% ns_config_events
sync_announcements() ->
    Pid = spawn(
            fun () ->
                    gen_event:sync_notify(ns_config_events,
                                          barrier)
            end),
    %% we don't need return value, but because this request will be
    %% queued we'll receive reply only after all currently queued
    %% messages to ns_config_events_local are consumed
    gen_event:which_handlers(ns_config_events_local),
    misc:wait_for_process(Pid, infinity).

latest() ->
    'latest-config-marker'.


-ifdef(TEST).
mock_tombstone_agent() ->
    ok = meck:new(tombstone_agent),
    ok = meck:expect(tombstone_agent, init_timestamps, fun (_) -> ok end),
    ok = meck:expect(tombstone_agent, vclock_ts, fun() -> 0 end),
    ok = meck:expect(tombstone_agent, purge_ts, fun() -> 0 end).

unmock_tombstone_agent() ->
    ok = meck:unload(tombstone_agent).

%% used in test/ns_config_tests.erl
test_setup(KVPairs) ->
    (catch ets:new(ns_config_ets_dup, [public, set, named_table])),
    ets:delete_all_objects(ns_config_ets_dup),
    update_ets_dup(KVPairs).

all_test_() ->
    {setup,
     fun mock_tombstone_agent/0,
     fun (_) ->
             unmock_tombstone_agent()
     end,
     [{spawn, [{"test_update_config", fun test_update_config/0},
               {"test_set_kvlist", fun test_set_kvlist/0}]},
      {spawn,
       {foreach, fun setup_with_saver/0, fun teardown_with_saver/1,
        [{"test_with_saver_stop", fun test_with_saver_stop/0},
         {"test_clear", fun test_clear/0},
         {"test_with_saver_set_and_stop", fun test_with_saver_set_and_stop/0},
         {"test_clear_with_concurrent_save",
          fun test_clear_with_concurrent_save/0},
         {"test_local_changes_count", fun test_local_changes_count/0}]}},

      {spawn, ?_test(test_upgrade_config_with_many_upgrades())},
      {spawn, ?_test(test_upgrade_config_vclocks())},
      {spawn, make_upgrade_config_test_spec()}
     ]}.

-define(assertConfigEquals(A, B), ?assertEqual(lists:sort([{K, strip_metadata(V)} || {K,V} <- A]),
                                               lists:sort([{K, strip_metadata(V)} || {K,V} <- B]))).

test_update_config() ->
    ?assertConfigEquals([{test, 1}], update_config_key(test, 1, [], <<"uuid">>)),
    ?assertConfigEquals([{test, 1},
                         {foo, [{k, 1}, {v, 2}]},
                         {xar, true}],
                        update_config_key(test, 1,
                                          [{foo, [{k, 1}, {v, 2}]},
                                           {xar, true},
                                           {test, [{a, b}, {c, d}]}],
                                          <<"uuid">>)).

test_set_kvlist() ->
    {NewPairs, [{foo, FooVal},
                {bar, [{'_vclock', _} | false]},
                {baz, [{nothing, false}]}]} =
        set_kvlist([{bar, false},
                    {foo, [{suba, a}, {subb, b}]}],
                   [{baz, [{nothing, false}]},
                    {foo, [{suba, undefined}, {subb, unlimited}]}],
                   <<"uuid">>, []),
    ?assertConfigEquals(NewPairs, [{foo, FooVal}, {bar, false}]),
    ?assertMatch([{'_vclock', [{<<"uuid">>, _}]}, {suba, a}, {subb, b}],
                 FooVal).

send_config(Config, Pid) ->
    Ref = erlang:make_ref(),
    Pid ! {saving, Ref, Config, self()},
    receive
        {Ref, Reply} ->
            Reply
    end.

setup_with_saver() ->
    {ok, _} = gen_event:start_link({local, ns_config_events}),
    {ok, _} = gen_event:start_link({local, ns_config_events_local}),
    Parent = self(),
    %% we don't want to kill this process when ns_config server dies,
    %% but we wan't to kill ns_config process when this process dies
    proc_lib:start(
      erlang, apply,
      [fun () ->
               Cfg = #config{dynamic = [[{config_version, ns_config_default:get_current_version()},
                                         {a, [{b, 1}, {c, 2}]},
                                         {d, 3},
                                         {{local_changes_count, testuuid}, []}]],
                             policy_mod = ns_config_default,
                             saver_mfa = {?MODULE, send_config, [save_config_target]},
                             upgrade_config_fun = fun upgrade_config/1,
                             uuid = testuuid},
               {ok, _} = start_link({with_state, Cfg}),
               MRef = erlang:monitor(process, Parent),

               proc_lib:init_ack(self()),

               receive
                   {'DOWN', MRef, _, _, _} ->
                       ?debugFmt("Commiting suicide~n", []),
                       exit(death)
               end
       end, []]).

kill_and_wait(undefined) -> ok;
kill_and_wait(Pid) ->
    (catch erlang:unlink(Pid)),
    exit(Pid, kill),
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end.

teardown_with_saver(_) ->
    kill_and_wait(whereis(ns_config)),
    kill_and_wait(whereis(ns_config_events)),
    kill_and_wait(whereis(ns_config_events_local)),
    ok.

fail_on_incoming_message() ->
    receive
        X ->
            exit({i_dont_expect_anything, X})
    after
        0 -> ok
    end.

test_with_saver_stop() ->
    do_test_with_saver(fun (_Pid) ->
                               gen_server:cast(?MODULE, stop)
                       end,
                       fun () ->
                               ok
                       end).

test_with_saver_set_and_stop() ->
    do_test_with_saver(fun (_Pid) ->
                               %% check that pending_more_save is false
                               Cfg1 = ns_config:get(),
                               ?assertEqual(false, Cfg1#config.pending_more_save),

                               %% send last mutation
                               set(d, 10),

                               %% check that pending_more_save is false
                               Cfg2 = ns_config:get(),
                               ?assertEqual(true, Cfg2#config.pending_more_save),

                               %% and kill ns_config
                               gen_server:cast(?MODULE, stop)
                       end,
                       fun () ->
                               %% wait for last save request and confirm it
                               receive
                                   {saving, Ref, _Conf, Pid} ->
                                       Pid ! {Ref, ok};
                                   X ->
                                       exit({unexpected_message, X})
                               end,
                               ok
                       end).

do_test_with_saver(KillerFn, PostKillerFn) ->
    erlang:process_flag(trap_exit, true),
    true = erlang:register(save_config_target, self()),

    ?assertEqual({value, 3}, search(d)),
    ?assertEqual(2, search_prop(ns_config:get(), a, c)),

    set(d, 4),

    {NewConfig1, Ref1, Pid1} = receive
                                   {saving, R, C, P} -> {C, R, P}
                               end,

    fail_on_incoming_message(),

    ?assertEqual({value, 4}, search(NewConfig1, d)),

    set(d, 5),

    %% ensure that save request is not sent while first is not yet
    %% complete
    fail_on_incoming_message(),

    %% and actually check that pending_more_save is true
    Cfg1 = ns_config:get(),
    ?assertEqual(true, Cfg1#config.pending_more_save),

    %% now signal save completed
    Pid1 ! {Ref1, ok},

    %% expect second save request immediately
    {_, Ref2, Pid2} = receive
                          {saving, R1, C1, P1} -> {C1, R1, P1}
                      end,

    Cfg2 = ns_config:get(),
    ?assertEqual(false, Cfg2#config.pending_more_save),

    Pid = whereis(ns_config),
    erlang:monitor(process, Pid),

    %% send termination request, but before completing second save
    %% request
    KillerFn(Pid),

    fail_on_incoming_message(),

    %% now confirm second save
    Pid2 ! {Ref2, ok},

    PostKillerFn(),

    %% await ns_config death
    receive
        {'DOWN', _MRef, process, Pid, Reason} ->
            ?assertEqual(shutdown, Reason)
    end,

    %% make sure there are no unhandled messages
    fail_on_incoming_message(),

    ok.

test_clear() ->
    erlang:process_flag(trap_exit, true),
    true = erlang:register(save_config_target, self()),

    ?assertEqual({value, 3}, search(d)),
    ?assertEqual(2, search_prop(ns_config:get(), a, c)),

    set(d, 4),

    NewConfig1 = receive
                     {saving, Ref1, C, Pid1} ->
                         Pid1 ! {Ref1, ok},
                         C
                 end,

    fail_on_incoming_message(),

    ?assertEqual({value, 4}, search(NewConfig1, d)),

    %% clear/1 blocks on saver, so we need concurrency here
    Clearer = spawn_link(fun () -> clear([]) end),

    %% make sure we're saving correctly cleared config
    receive
        {saving, Ref2, NewConfig2, Pid2} ->
            Pid2 ! {Ref2, ok},
            ?assertMatch([{{node, _, uuid}, _}], config_dynamic(NewConfig2))
    end,

    receive
        {'EXIT', Clearer, normal} -> ok
    end,

    fail_on_incoming_message(),

    %% now verify that ns_config was re-inited. In our case this means
    %% returning to original config
    ?assertEqual({value, 3}, search(d)),
    ?assertEqual(2, search_prop(ns_config:get(), a, c)).

test_clear_with_concurrent_save() ->
    erlang:process_flag(trap_exit, true),
    true = erlang:register(save_config_target, self()),

    ?assertEqual({value, 3}, search(d)),
    ?assertEqual(2, search_prop(ns_config:get(), a, c)),

    set(d, 4),

    %% don't reply right now
    {NewConfig1, Pid1, Ref1} = receive
                                   {saving, R1, C, P1} ->
                                       {C, P1, R1}
                               end,

    fail_on_incoming_message(),

    ?assertEqual({value, 4}, search(NewConfig1, d)),

    %% clear/1 blocks on saver, so we need concurrency here
    Clearer = spawn_link(fun () -> clear([]) end),

    %% this is racy, but don't know how to test other process waiting
    %% on reply from us
    timer:sleep(300),

    %% now assuming ns_config is waiting on us already, reply on first
    %% save request
    fail_on_incoming_message(),
    Pid1 ! {Ref1, ok},

    %% make sure we're saving correctly cleared config
    receive
        {saving, Ref2, NewConfig2, Pid2} ->
            Pid2 ! {Ref2, ok},
            ?assertMatch([{{node, _, uuid}, _}], config_dynamic(NewConfig2))
    end,

    receive
        {'EXIT', Clearer, normal} -> ok
    end,

    fail_on_incoming_message(),

    %% now verify that ns_config was re-inited. In our case this means
    %% returning to original config
    ?assertEqual({value, 3}, search(d)),
    ?assertEqual(2, search_prop(ns_config:get(), a, c)).

test_local_changes_count() ->
    erlang:process_flag(trap_exit, true),
    true = erlang:register(save_config_target, self()),

    ?assertEqual({value, 3}, search(d)),
    ?assertEqual({value, 3}, search(ns_config:get(), d)),

    ?assertEqual(0, compute_global_rev(latest())),
    ?assertEqual(0, compute_global_rev(ns_config:get())),

    ?assertEqual([], read_key_fast({local_changes_count, testuuid}, undefined)),

    set(d, 4),

    receive
        {saving, Ref1, _C, Pid1} ->
            Pid1 ! {Ref1, ok}
    end,

    fail_on_incoming_message(),

    ?assertEqual(1, compute_global_rev(latest())),
    ?assertEqual(1, compute_global_rev(ns_config:get())),

    {value, [], {0, VC}} = search_with_vclock(ns_config:get(),
                                              {local_changes_count, testuuid}),
    ?assertEqual(1, vclock:count_changes(VC)),

    ok.

upgrade_config_case(InitialList, Changes, ExpectedList) ->
    Upgrader = fun (_) -> [] end,
    upgrade_config_case(InitialList, Changes, ExpectedList, Upgrader).

upgrade_config_case(InitialList, Changes, ExpectedList, Upgrader) ->
    Config = #config{dynamic=[InitialList]},
    UpgradedConfig = do_upgrade_config(Config,
                                       Changes,
                                       Upgrader),
    StrippedUpgradedConfig = lists:map(fun ({K, V}) ->
                                               {K, strip_metadata(V)}
                                       end, config_dynamic(UpgradedConfig)),
    ?assertEqual(lists:sort(ExpectedList),
                 lists:sort(StrippedUpgradedConfig)).

upgrade_config_testgen(InitialList, Changes, ExpectedList) ->
    Title = iolist_to_binary(io_lib:format("~p + ~p = ~p~n", [InitialList, Changes, ExpectedList])),
    {Title,
     fun () -> upgrade_config_case(InitialList, Changes, ExpectedList) end}.

make_upgrade_config_test_spec() ->
    T = [{[{a, 1}, {b, 2}], [{set, a, 2}, {set, c, 3}], [{a, 2}, {b, 2}, {c, 3}]},
         {[{b, 2}, {a, 1}], [{set, a, 2}, {set, c, 3}], [{a, 2}, {b, 2}, {c, 3}]},
         {[{b, 2}, {a, [{key1, "asd"}, {key2, "ffd"}]}, {c, 0}],
          [{set, a, [{key1, "new"}, {key2, "newff"}]}, {set, c, 3}],
          [{a, [{key1, "new"}, {key2, "newff"}]}, {b, 2}, {c, 3}]}
        ],
    [upgrade_config_testgen(I, C, E) || {I,C,E} <- T].

test_upgrade_config_vclocks() ->
    Config = #config{dynamic = [[{{node, node(), a}, 1},
                                 {unchanged, 2},
                                 {b, 2},
                                 {{node, node(), c}, attach_vclock(1, <<"uuid">>)}]],
                     uuid = <<"uuid">>},
    Changes = [{set, {node, node(), a}, 2},
               {set, b, 4},
               {set, {node, node(), c}, [3]},
               {set, d, [4]}],
    Upgrader = fun (_) -> [] end,
    UpgradedConfig = do_upgrade_config(Config, Changes, Upgrader),

    Get = fun (Config1, K) ->
                  {value, Value} = search_raw(Config1, K),
                  Value
          end,

    ?assertMatch({0, [{<<"uuid">>, {_, _}}]},
                 extract_vclock(Get(UpgradedConfig, {node, node(), a}))),
    ?assertMatch({0, []},
                 extract_vclock(Get(UpgradedConfig, unchanged))),
    ?assertMatch({0, [{<<"uuid">>, {_, _}}]},
                 extract_vclock(Get(UpgradedConfig, b))),
    ?assertMatch({0, [{<<"uuid">>, {_, _}}]},
                 extract_vclock(Get(UpgradedConfig, {node, node(), c}))),
    ?assertMatch({0, [{<<"uuid">>, {_, _}}]},
                 extract_vclock(Get(UpgradedConfig, d))).

test_upgrade_config_with_many_upgrades() ->
    Initial = [{a, 1}],
    Ref = make_ref(),
    self() ! {Ref, [{set, a, 2}]},
    self() ! {Ref, [{set, a, 3}]},
    self() ! {Ref, []},
    Upgrader = fun (_) ->
                       receive
                           {Ref, X} -> X
                       end
               end,
    upgrade_config_case(Initial, Upgrader(any), [{a, 3}], Upgrader),
    receive
        X ->
            erlang:error({unexpected_message, X})
    after 0 ->
            ok
    end.

merge_values_test_() ->
    {timeout, 100, fun merge_values_test__/0}.

merge_values_test__() ->
    mock_timestamp(
      fun () ->
              with_touched_keys(
                fun () ->
                        lists:foreach(
                          fun (_I) ->
                                  merge_values_test_iter()
                          end, lists:seq(1, 1000))
                end)
      end).

mock_timestamp(Body) ->
    Tid = ets:new(none, [public]),
    true = ets:insert_new(Tid, {counter, 0}),
    ok = meck:new(tombstone_agent),

    try
        ok = meck:expect(tombstone_agent, purge_ts, fun() -> 0 end),
        ok = meck:expect(tombstone_agent, vclock_ts,
                         fun () ->
                                 [{counter, Count}] = ets:lookup(Tid, counter),
                                 NewCount = case rand:uniform() < 0.3 of
                                                true ->
                                                    Count + 1;
                                                false ->
                                                    Count
                                            end,
                                 true = ets:insert(Tid, {counter, NewCount}),
                                 Count
                         end),
        Body()
    after
        meck:unload(),
        ets:delete(Tid)
    end.

mutate(Value, Nodes) ->
    N = length(Nodes),
    ManyNodes = lists:concat(lists:duplicate(N, Nodes)),
    Mutations = lists:sublist(misc:shuffle(ManyNodes), N),

    lists:foldl(
      fun (Node, V) ->
              increment_vclock(V, V, Node)
      end, Value, Mutations).

merge_values_helper(RP, LP) ->
    {_, V} = merge_values({key, RP}, {key, LP}),
    V.

merge_values_test_iter() ->
    Nodes = [a,b,c,d,e],

    LocalValue = mutate(rand:uniform(10), Nodes),
    RemoteValue = mutate(rand:uniform(10), Nodes),

    R0 = merge_values_helper(RemoteValue, LocalValue),
    R1 = merge_values_helper(LocalValue, RemoteValue),
    ?assertEqual(R0, R1),

    R2 = merge_values_helper(RemoteValue, LocalValue),
    R3 = merge_values_helper(LocalValue, RemoteValue),
    ?assertEqual(R2, R3),

    %% merge result is independent on the node and the time
    %% when merge was done
    ?assertEqual(R0, R2).
-endif.

do_update_with_changes(Fun, OldList, UUID) ->
    try Fun(OldList, UUID) of
        {Changed, Config} ->
            {ok, Changed, [], Config, ok};
        {Changed, Erased, Config} ->
            {ok, Changed, Erased, Config, ok};
        {Changed, Erased, Config, Acc} ->
            {ok, Changed, Erased, Config, {ok, Acc}}
    catch
        T:E:Stacktrace ->
            ?log_error("Failed to update config: ~p~nStacktrace: ~n~p",
                       [{T, E}, Stacktrace]),
            {error, {T, E, Stacktrace}}
    end.

drop_deletes(KVList) ->
    misc:partitionmap(
      fun ({Key, Value} = Pair) ->
              case strip_metadata(Value) of
                  ?DELETED_MARKER ->
                      {left, Key};
                  _ ->
                      {right, Pair}
              end
      end, KVList).
