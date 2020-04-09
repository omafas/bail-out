%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2020 Couchbase, Inc.
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
%% @doc configuration parameters to create bucket and rules for updating
%%      these parameters in case if they change

-module(memcached_bucket_config).

-include("ns_common.hrl").
-include("cut.hrl").

-record(cfg, {type, name, config, engine_config, params}).

-export([get/2,
         ensure/2,
         start_params/1,
         ensure_collections/2,
         get_current_collections_uid/1]).

params(membase, BucketName, BucketConfig, MemQuota, UUID) ->
    StorageMode = ns_bucket:storage_mode(BucketConfig),

    {DriftAheadThreshold, DriftBehindThreshold} =
        case ns_bucket:drift_thresholds(BucketConfig) of
            undefined ->
                {undefined, undefined};
            {A, B} ->
                {misc:msecs_to_usecs(A), misc:msecs_to_usecs(B)}
        end,

    {ok, DBSubDir} = ns_storage_conf:this_node_bucket_dbdir(BucketName),

    [{"max_size", [{reload, flush}], MemQuota},
     {"dbname", [restart], DBSubDir},
     {"backend", [], couchdb},
     {"couch_bucket", [], BucketName},
     {"max_vbuckets", [], proplists:get_value(num_vbuckets, BucketConfig)},
     {"alog_path", [], filename:join(DBSubDir, "access.log")},
     {"data_traffic_enabled", [], false},
     {"max_num_workers", maybe_restart(),
      proplists:get_value(num_threads, BucketConfig, 3)},
     {"uuid", [], UUID},
     {"conflict_resolution_type", [],
      ns_bucket:conflict_resolution_type(BucketConfig)},
     {"bucket_type", [], storage_mode_to_bucket_type(StorageMode)},
     {"durability_min_level", [{reload, flush}],
      ns_bucket:durability_min_level(BucketConfig)},
     {"hlc_drift_ahead_threshold_us", [no_param, {reload, vbucket}],
      DriftAheadThreshold},
     {"hlc_drift_behind_threshold_us", [no_param, {reload, vbucket}],
      DriftBehindThreshold},
     {"item_eviction_policy", maybe_restart(),
      item_eviction_policy(StorageMode, BucketConfig)},
     {"ephemeral_full_policy", [{reload, flush}],
      ephemeral_full_policy(StorageMode, BucketConfig)},
     {"ephemeral_metadata_purge_age", [{reload, flush}],
      ephemeral_metadata_purge_age(StorageMode, BucketConfig)},
     {"max_ttl", [{reload, flush}], proplists:get_value(max_ttl, BucketConfig)},
     {"ht_locks", [], proplists:get_value(
                        ht_locks, BucketConfig,
                        misc:getenv_int("MEMBASE_HT_LOCKS", 47))},
     {"ht_size", [],
      proplists:get_value(ht_size, BucketConfig,
                          misc:getenv_int("MEMBASE_HT_SIZE", undefined))},
     {"compression_mode", [{reload, flush}],
      proplists:get_value(compression_mode, BucketConfig)}];
params(memcached, _BucketName, _BucketConfig, MemQuota, UUID) ->
    [{"cache_size", [], MemQuota},
     {"uuid", [], UUID}].

storage_mode_to_bucket_type(couchstore) ->
    persistent;
storage_mode_to_bucket_type(ephemeral) ->
    ephemeral.

maybe_restart() ->
    case ns_config:read_key_fast(dont_reload_bucket_on_cfg_change, false) of
        false ->
            [restart];
        true ->
            []
    end.

item_eviction_policy(couchstore, BucketConfig) ->
    proplists:get_value(eviction_policy, BucketConfig, value_only);
item_eviction_policy(ephemeral, _BucketConfig) ->
    undefined.

ephemeral_full_policy(ephemeral, BucketConfig) ->
    case proplists:get_value(eviction_policy, BucketConfig, no_eviction) of
        nru_eviction ->
            auto_delete;
        no_eviction ->
            fail_new_data
    end;
ephemeral_full_policy(couchstore, _BucketConfig) ->
    undefined.

ephemeral_metadata_purge_age(ephemeral, BucketConfig) ->
    %% Purge interval is accepted in # of days but the ep-engine
    %% needs it to be expressed in seconds.
    Val = proplists:get_value(purge_interval, BucketConfig,
                              ?DEFAULT_EPHEMERAL_PURGE_INTERVAL_DAYS),
    erlang:round(Val * 24 * 3600);
ephemeral_metadata_purge_age(couchstore, _BucketConfig) ->
    undefined.

get(Config, BucketName) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName, Config),
    BucketType = proplists:get_value(type, BucketConfig),

    MemQuota = proplists:get_value(ram_quota, BucketConfig),
    UUID = ns_bucket:bucket_uuid(BucketConfig),

    Params = params(BucketType, BucketName, BucketConfig, MemQuota, UUID),

    Engines = ns_config:search_node_prop(Config, memcached, engines),
    EngineConfig = proplists:get_value(BucketType, Engines),

    #cfg{type = BucketType, name = BucketName, config = BucketConfig,
         params = Params, engine_config = EngineConfig}.

query_stats(Sock) ->
    {ok, Stats} =
        mc_binary:quick_stats(
          Sock, <<>>,
          fun (<<"ep_", Name/binary>>, V, Dict) ->
                  dict:store(binary_to_list(Name), V, Dict);
              (_, _, Dict) ->
                  Dict
          end, dict:new()),
    Stats.

value_to_string(V) when is_binary(V) ->
    binary_to_list(V);
value_to_string(V) when is_integer(V) ->
    integer_to_list(V);
value_to_string(V) when is_atom(V) ->
    atom_to_list(V);
value_to_string(V) when is_list(V) ->
    V.

value_to_binary(undefined) ->
    undefined;
value_to_binary(V) when is_binary(V) ->
    V;
value_to_binary(V) ->
    list_to_binary(value_to_string(V)).

has_changed(_BucketName, _Name, undefined, _Dict) ->
    false;
has_changed(BucketName, Name, Value, Dict) ->
    case dict:find(Name, Dict) of
        {ok, Value} ->
            false;
        {ok, OldValue} ->
            ?log_info(
               "Detected change of parameter ~s for bucket ~s from ~s to ~s",
               [Name, BucketName, OldValue, Value]),
            true;
        error ->
            ?log_info(
               "Detected change of parameter ~s for bucket ~s to ~s",
               [Name, BucketName, Value]),
            true
    end.

maybe_update_param(Sock, Stats, BucketName, {Name, Props, Value}) ->
    case proplists:get_value(reload, Props) of
        undefined ->
            ok;
        ReloadType ->
            BinValue = value_to_binary(Value),
            case has_changed(BucketName, Name, BinValue, Stats) of
                true ->
                    ?log_info("Changing parameter ~s of bucket ~s to ~s",
                              [Name, BucketName, BinValue]),
                    ok = mc_client_binary:set_engine_param(
                           Sock, list_to_binary(Name), BinValue, ReloadType);
                false ->
                    ok
            end
    end.

ensure(Sock, #cfg{type = membase, name = BucketName, params = Params}) ->
    Stats = query_stats(Sock),
    Restart =
        lists:any(
          fun ({Name, Props, Value}) ->
                  lists:member(restart, Props) andalso
                      has_changed(BucketName, Name,
                                  value_to_binary(Value), Stats)
          end, Params),
    case Restart of
        true ->
            restart;
        false ->
            lists:foreach(
              maybe_update_param(Sock, Stats, BucketName, _), Params)
    end;
ensure(Sock, #cfg{type = memcached}) ->
    %% TODO: change max size of memcached bucket also
    %% Make sure it's a memcached bucket
    {ok, present} = mc_binary:quick_stats(
                      Sock, <<>>,
                      fun (<<"evictions">>, _, _) ->
                              present;
                          (_, _, CD) ->
                              CD
                      end, not_present),
    ok.

get_current_collections_uid(Sock) ->
    case mc_client_binary:get_collections_manifest(Sock) of
        {memcached_error, no_coll_manifest, _} ->
            undefined;
        {ok, Bin} ->
            {Json} = ejson:decode(Bin),
            proplists:get_value(<<"uid">>, Json)
    end.

need_update_collections_manifest(Sock, BucketConfig) ->
    case collections:uid(BucketConfig) of
        undefined ->
            false;
        Uid ->
            case get_current_collections_uid(Sock) of
                Uid ->
                    false;
                Other ->
                    {true, Other, Uid}
            end
    end.

ensure_collections(Sock, #cfg{name = BucketName, config = BucketConfig}) ->
    case need_update_collections_manifest(Sock, BucketConfig) of
        false ->
            ok;
        {true, Prev, Next} ->
            Manifest = collections:manifest_json(BucketConfig),
            ?log_debug(
               "Applying collection manifest to bucket ~p due to id change from"
               " ~p to ~p. Manifest = ~p",
               [BucketName, Prev, Next, Manifest]),
            ok = mc_client_binary:set_collections_manifest(
                   Sock, ejson:encode(Manifest)),
            gen_event:notify(buckets_events,
                             {set_collections_manifest,
                              ns_bucket:bucket_uuid(BucketConfig),
                              collections:convert_uid_from_memcached(Next)})
    end.

start_params(#cfg{config = BucketConfig,
                  params = Params,
                  engine_config = EngineConfig}) ->
    Engine = proplists:get_value(engine, EngineConfig),

    StaticConfigString =
        proplists:get_value(
          static_config_string, BucketConfig,
          proplists:get_value(static_config_string, EngineConfig)),
    ExtraConfigString =
        proplists:get_value(
          extra_config_string, BucketConfig,
          proplists:get_value(extra_config_string, EngineConfig, "")),

    DynamicParams =
        lists:filtermap(
          fun ({_Name, _Props, undefined}) ->
                  false;
              ({Name, Props, Value}) ->
                  case lists:member(no_param, Props) of
                      true ->
                          false;
                      false ->
                          {true, Name ++ "=" ++ value_to_string(Value)}
                  end
          end, Params),

    ExtraParams = [P || P <- [StaticConfigString, ExtraConfigString], P =/= ""],
    {Engine, string:join(DynamicParams ++ ExtraParams, ";")}.
