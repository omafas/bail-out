%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(metakv).

-include("ns_common.hrl").
-include("ns_config.hrl").

-export([get/1,
         set/2, set/3,
         delete/1,
         delete_matching/1,
         mutate/2, mutate/3,
         iterate_matching/1, iterate_matching/2, iterate_matching/3,
         check_continuous_allowed/1]).

%% Exported APIs

get(Key) ->
    case which_store(Key) of
        simple_store ->
            simple_store_get(Key);
        ns_config ->
            ns_config_get(Key)
    end.

set(Key, Value) ->
    mutate(Key, Value).

set(Key, Value, Params) ->
    mutate(Key, Value, Params).

delete(Key) ->
    mutate(Key, ?DELETED_MARKER).

%% Create, update or delete the specified key.
%% For delete, Value is set to ?DELETED_MARKER.
mutate(Key, Value) ->
    mutate(Key, Value, []).

mutate(Key, Value, Params) ->
    case which_store(Key) of
        simple_store ->
            %% Today, Simple store does not support revisions and other params.
            simple_store_mutation(Key, Value);
        ns_config ->
            ns_config_mutation(Key, Value, Params)
    end.

%% Delete key with the matching prefix
delete_matching(KeyPrefix) ->
    case which_store(KeyPrefix) of
        simple_store ->
            simple_store:delete_matching(?XDCR_CHECKPOINT_STORE, KeyPrefix);
        ns_config ->
            ns_config_delete_matching(KeyPrefix)
    end.

%% Read keys from appropriate store and return KVs that match the prefix
iterate_matching(KeyPrefix) ->
    case which_store(KeyPrefix) of
        simple_store ->
            simple_store:iterate_matching(?XDCR_CHECKPOINT_STORE, KeyPrefix);
        ns_config ->
            ns_config_iterate_matching(KeyPrefix)
    end.

%% User has passed the full KV list, we need to return KVs that match the
%% prefix.
iterate_matching(KeyPrefix, KVList) ->
    ns_config_matching_kvs(KeyPrefix, KVList).

%% Read keys from appropriate store and run the Callback function on
%% KVs that match the prefix
iterate_matching(KeyPrefix, Continuous, Callback) ->
    case which_store(KeyPrefix) of
        simple_store ->
            simple_store_iterate_matching(KeyPrefix, Callback);
        ns_config ->
            ns_config_iterate_matching(KeyPrefix, Continuous, Callback)
    end.

%% Simple store does not support continuous iteration
check_continuous_allowed(Key) ->
    case which_store(Key) of
        simple_store ->
            ?metakv_debug("Continuous should not be set to true while iterating on XDCR Checkpoints."),
            false;
        ns_config ->
            true
    end.

%% Internal

%% Currently, we have only two storage options:
%% 1. simple store for XDCR checkpoints.
%% 2. ns_config for everything else.
%%
%% Today, metakv uses simple store only for XDCR checkpoints.
%% In future, if simple_store has other metakv consumers,
%% then this module will need to take that into account.
%% Note, non-metakv consumers can use simple-store directly by using
%% the APIs in simple_store module.

which_store(Key) ->
    case misc:is_prefix(?XDCR_CHECKPOINT_PATTERN, Key) of
        true ->
            simple_store;
        _ ->
            ns_config
    end.

%% Simple Store related functions

simple_store_get(K) ->
    case simple_store:get(?XDCR_CHECKPOINT_STORE, K) of
        false ->
            false;
        V ->
            {value, V}
    end.

simple_store_mutation(Key, Value) ->
    case Value =:= ?DELETED_MARKER of
        true ->
            simple_store:delete(?XDCR_CHECKPOINT_STORE, Key);
        false ->
            simple_store:set(?XDCR_CHECKPOINT_STORE, Key, Value)
    end.

simple_store_iterate_matching(KeyPrefix, Callback) ->
    KVs = simple_store:iterate_matching(?XDCR_CHECKPOINT_STORE, KeyPrefix),
    lists:foreach(
      fun({K, V}) ->
              Callback({K, V})
      end,
      KVs).

%% NS Config related functions

ns_config_get(Key) ->
    case ns_config:search_with_vclock(ns_config:get(), {metakv, Key}) of
        false ->
            false;
        {value, Val, VC} ->
            {_, RawVal} = strip_sensitive(Val),
            {value, RawVal, VC}
    end.

ns_config_mutation(Key, Value, Params) ->
    Rev = proplists:get_value(rev, Params),
    Sensitive = proplists:get_value(?METAKV_SENSITIVE, Params),
    K = {metakv, Key},
    work_queue:submit_sync_work(
      metakv_worker,
      fun () ->
              case Rev =:= undefined of
                  true ->
                      %% If key does not exist, then update_key will
                      %% set the value to DefaultValue.
                      DefaultValue = add_sensitive(Sensitive, undefined, Value),
                      ns_config:update_key(K,
                                           fun (OldValue) ->
                                               add_sensitive(Sensitive,
                                                             OldValue, Value)
                                           end,
                                           DefaultValue),
                      ok;
                  false ->
                      RV = ns_config:run_txn(
                             fun (Cfg, SetFn) ->
                                     OldData = ns_config:search_with_vclock(Cfg, K),
                                     OldValue = get_old_value(OldData),
                                     NewValue = add_sensitive(Sensitive,
                                                              OldValue, Value),
                                     OldVC = get_old_vclock(OldData),
                                     case Rev =:= OldVC of
                                         true ->
                                             {commit, SetFn(K, NewValue, Cfg)};
                                         false ->
                                             {abort, mismatch}
                                     end
                             end),
                      case RV of
                          {commit, _} ->
                              %% don't send whole config back
                              %% from worker
                              ok;
                          _ ->
                              RV
                      end
              end
      end).

%% Add sensitive tag to a value that is sensitive and return the new value.
%%
%% If user is deleting the entry then don't care whether it is senstive or not.
add_sensitive(_, _, ?DELETED_MARKER) ->
    ?DELETED_MARKER;

%% If Key does not exist (OldValue is undefined) or was previously deleted,
%% then add sensitive tag based on what the user passed in Sensitive.
%% Otherwise, if key already exists, then carry forward its sensitive value
%% irrespective of what user passed in Sensitive.
add_sensitive(Sensitive, ?DELETED_MARKER, NewValue) ->
    check_sensitive(Sensitive, NewValue);

add_sensitive(Sensitive, undefined, NewValue) ->
    check_sensitive(Sensitive, NewValue);

add_sensitive(_, OldValue, NewValue) ->
    case OldValue of
        {?METAKV_SENSITIVE, _} ->
            {?METAKV_SENSITIVE, NewValue};
        _ ->
            NewValue
    end.

check_sensitive(Sensitive, NewValue) ->
    case Sensitive of
        true ->
            {?METAKV_SENSITIVE, NewValue};
        _ ->
            NewValue
    end.

%% Strip senstive tag from the value
strip_sensitive({?METAKV_SENSITIVE, Value}) ->
    {true, Value};
strip_sensitive(Value) ->
    {false, Value}.

get_old_value(OldData) ->
    case OldData of
        {value, OldV, _OldVC} ->
            OldV;
        false ->
            undefined
    end.

get_old_vclock(OldData) ->
    case OldData of
        false ->
            missing;
        {value, OldV, OldVC} ->
            case OldV of
                ?DELETED_MARKER ->
                    missing;
                _ ->
                    OldVC
            end
    end.

ns_config_delete_matching(Key) ->
    Filter = mk_config_filter(Key),
    RV = ns_config:run_txn(
           fun (Cfg, SetFn) ->
                   KeysToDelete = [K || {K, V} <- hd(Cfg), Filter(K),
                                        ns_config:strip_metadata(V) =/= ?DELETED_MARKER],
                   NewCfg = lists:foldl(
                              fun (K, AccCfg) ->
                                      SetFn(K, ?DELETED_MARKER, AccCfg)
                              end,
                              Cfg, KeysToDelete),
                   {commit, NewCfg}
           end),
    case RV of
        {commit, _} ->
            ok;
        _ ->
            RV
    end.

mk_config_filter(KeyBin) ->
    KeyL = size(KeyBin),
    fun ({metakv, K}) when is_binary(K) ->
            case K of
                <<KeyBin:KeyL/binary, _/binary>> ->
                    true;
                _ ->
                    false
            end;
        (_K) ->
            false
    end.

ns_config_iterate_matching(Key) ->
    KVs = ns_config_matching_kvs(Key, ns_config:get_kv_list()),
    %% This function gets called during first iteration of
    %% menelaus_metakv:handle_iterate(). Skip deleted entries for
    %% the first iteration. This will retain the behaviour as it existed
    %% before this code was moved here from menelaus_metakv.erl.
    [{K, V} || {K, V} <- KVs, ns_config:strip_metadata(V) =/= ?DELETED_MARKER].

ns_config_iterate_matching(Key, Continuous, Callback) ->
    Self = self(),
    case Continuous of
        true ->
            ns_pubsub:subscribe_link(
              ns_config_events,
              fun ([_|_] = KVs) ->
                      %% we receive kvlist events because they include
                      %% vclocks
                      Self ! {config, KVs};
                  (_) ->
                      ok
              end);
        false ->
            ok
    end,
    ns_config_emit_values(ns_config_iterate_matching(Key), Callback),
    case Continuous of
        true ->
            ns_config_iterate_loop(Key, Callback);
        false ->
            ok
    end.

ns_config_iterate_loop(Key, Callback) ->
    receive
        {config, KVs} ->
            ns_config_emit_values(ns_config_matching_kvs(Key, KVs), Callback),
            ns_config_iterate_loop(Key, Callback);
        _ ->
            erlang:exit(normal)
    end.

ns_config_emit_values(KV, Callback) ->
    lists:foreach(
      fun({{metakv, K}, V0}) ->
              VC = ns_config:extract_vclock(V0),
              {Sensitive, V} = strip_sensitive(ns_config:strip_metadata(V0)),
              Callback({K, V, VC, Sensitive})
      end, KV).

ns_config_matching_kvs(Key, KVList) ->
    Filter = mk_config_filter(Key),
    %% This function gets called during subsequent iteration of
    %% menelaus_metakv:handle_iterate(). Do not skip deleted entries.
    %% This will retain the behaviour as it existed
    %% before this code was moved here from menelaus_metakv.erl.
    [{K, V} || {K, V} <- KVList, Filter(K)].
