%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(json_settings_manager).

-include("ns_common.hrl").

-export([start_link/1,
         get/3,
         build_settings_json/3,
         get_from_config/4,
         fetch_settings_json/2,
         decode_settings_json/1,
         id_lens/1,
         props_lens/1,
         update/2,
         update_txn/2,
         upgrade_existing_key/4,
         upgrade_existing_key/5
        ]).

-callback cfg_key() -> term().
-callback is_enabled() -> boolean().
-callback known_settings() -> term().
-callback on_update(atom(), term()) -> term().

start_link(Module) ->
    work_queue:start_link(Module, fun () -> init(Module) end).

get(Module, Key, Default) when is_atom(Key) ->
    case ets:lookup(Module, Key) of
        [{Key, Value}] ->
            Value;
        [] ->
            Default
    end.

get_from_config(M, Config, Key, Default) ->
    case Config =:= ns_config:latest() of
        true ->
            json_settings_manager:get(M, Key, Default);
        false ->
            case ns_config:search(Config, M:cfg_key()) of
                {value, JSON} ->
                    get_from_json(Key, JSON, M:known_settings());
                false ->
                    Default
            end
    end.

init(M) ->
    ets:new(M, [named_table, set, protected]),
    ModCfgKey = M:cfg_key(),
    Self = self(),
    chronicle_compat_events:subscribe(
      fun (Key) when (Key =:= ModCfgKey) ->
              submit_config_update(M, Self, Key);
          (cluster_compat_version) ->
              submit_full_refresh(M, Self);
          (_) ->
              ok
      end),
    populate_ets_table(M).

update(M, Props) ->
    work_queue:submit_sync_work(
      M,
      fun () ->
              do_update(M, Props)
      end).

get_from_json(Key, JSON, KSettings) ->
    {_, Lens} = lists:keyfind(Key, 1, KSettings),
    Settings = decode_settings_json(JSON),
    lens_get(Lens, Settings).

update_txn(M, Props) ->
    CfgKey = M:cfg_key(),
    fun (Config, SetFn) ->
            JSON = fetch_settings_json(Config, CfgKey),
            Current = decode_settings_json(JSON),

            New = build_settings_json(Props, Current, M:known_settings()),
            {commit, SetFn(CfgKey, New, Config), New}
    end.

upgrade_existing_key(M, Config, NewProps, KnownSettings) ->
    upgrade_existing_key(M, Config, NewProps, KnownSettings,
                         fun functools:id/1).

upgrade_existing_key(M, Config, NewProps, KnownSettings, Fun) ->
    JSON = fetch_settings_json(Config, M:cfg_key()),
    Current0 = decode_settings_json(JSON),
    Current = Fun(Current0),
    New = build_settings_json(NewProps, Current, KnownSettings),
    [{set, M:cfg_key(), New}].

do_update(M, Props) ->
    RV = ns_config:run_txn(update_txn(M, Props)),
    case RV of
        {commit, _, NewJSON} ->
            populate_ets_table(M, NewJSON),
            {ok, ets:tab2list(M)};
        _ ->
            RV
    end.

submit_full_refresh(M, Pid) ->
    work_queue:submit_work(
      Pid,
      fun () ->
              populate_ets_table(M)
      end).

submit_config_update(M, Pid, Key) ->
    work_queue:submit_work(
      Pid,
      fun () ->
              populate_ets_table(M, fetch_settings_json(Key))
      end).

fetch_settings_json(CfgKey) ->
    fetch_settings_json(ns_config:latest(), CfgKey).

fetch_settings_json(Config, CfgKey) ->
    ns_config:search(Config, CfgKey, <<"{}">>).

build_settings_json(Props, Dict, KnownSettings) ->
    NewDict = lens_set_many(KnownSettings, Props, Dict),
    ejson:encode({dict:to_list(NewDict)}).

decode_settings_json(JSON) ->
    {Props} = ejson:decode(JSON),
    dict:from_list(Props).

populate_ets_table(M) ->
    JSON = fetch_settings_json(M:cfg_key()),
    populate_ets_table(M, JSON).

populate_ets_table(M, JSON) ->
    case not M:is_enabled()
        orelse erlang:get(prev_json) =:= JSON of
        true ->
            ok;
        false ->
            do_populate_ets_table(M, JSON, M:known_settings())
    end.

do_populate_ets_table(M, JSON, Settings) ->
    Dict = decode_settings_json(JSON),
    NotFound = make_ref(),

    lists:foreach(
      fun ({Key, NewValue}) ->
              OldValue = json_settings_manager:get(M, Key, NotFound),
              case OldValue =:= NewValue of
                  true ->
                      ok;
                  false ->
                      ets:insert(M, {Key, NewValue}),
                      M:on_update(Key, NewValue)
              end
      end, lens_get_many(Settings, Dict)),

    erlang:put(prev_json, JSON).

id_lens(Key) ->
    Get = fun (Dict) ->
                  dict:fetch(Key, Dict)
          end,
    Set = fun (Value, Dict) ->
                  dict:store(Key, Value, Dict)
          end,
    {Get, Set}.

lens_get({Get, _}, Dict) ->
    Get(Dict).

lens_get_many(Lenses, Dict) ->
    [{Key, lens_get(L, Dict)} || {Key, L} <- Lenses].

lens_set(Value, {_, Set}, Dict) ->
    Set(Value, Dict).

lens_set_many(Lenses, Values, Dict) ->
    lists:foldl(
      fun ({Key, Value}, Acc) ->
              {Key, L} = lists:keyfind(Key, 1, Lenses),
              lens_set(Value, L, Acc)
      end, Dict, Values).

props_lens(Props) ->
    Get = fun (Dict) ->
                  lens_get_many(Props, Dict)
          end,
    Set = fun (Values, Dict) ->
                  lens_set_many(Props, Values, Dict)
          end,
    {Get, Set}.
