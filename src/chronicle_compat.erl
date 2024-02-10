%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc helpers for backward compatible calls to chronicle/ns_config
%%

-module(chronicle_compat).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("cut.hrl").

-export([get/2,
         get/3,
         set_multiple/1,
         transaction/2,
         txn/1,
         txn/2,
         ro_txn/2,
         txn_get/2,
         txn_get_many/2,
         get_snapshot/1,
         get_snapshot/2,
         get_snapshot_with_revision/2,
         pull/0,
         pull/1,
         push/1,
         push/2,
         node_keys/2,
         service_keys/1]).

%% RPC from another nodes
-export([do_pull/1]).

get(Key, Opts) ->
    get(direct, Key, Opts).

get(Source, Key, #{required := true}) ->
    {ok, Value} = get(Source, Key, #{}),
    Value;
get(Source, Key, #{default := Default}) ->
    case get(Source, Key, #{}) of
        {error, not_found} ->
            Default;
        {ok, Value} ->
            Value
    end;
get(Snapshot, Key, #{}) when is_map(Snapshot) ->
    case maps:find(Key, Snapshot) of
        {ok, {V, _R}} ->
            {ok, V};
        error ->
            {error, not_found}
    end;
get(direct, Key, #{}) ->
    case ns_node_disco:couchdb_node() =:= node() of
        true ->
            case ns_couchdb_chronicle_dup:lookup(Key) of
                [{Key, {Value, _Rev}}] ->
                    {ok, Value};
                [] ->
                    {error, not_found}
            end;
        false ->
            case chronicle_kv:get(kv, Key, #{}) of
                {ok, {V, _R}} ->
                    {ok, V};
                Error ->
                    Error
            end
    end.

set_multiple([]) ->
    ok;
set_multiple(List) ->
    Fun = fun (_) -> {commit, [{set, K, V} || {K, V} <- List]} end,
    case chronicle_kv:transaction(kv, [], Fun, #{}) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

transaction(Keys, Fun) ->
    txn(?cut(Fun(txn_get_many(Keys, _)))).

txn(Fun) ->
    txn(Fun, #{}).

txn(Fun, Opts) ->
    chronicle_kv:txn(kv, ?cut(Fun({chronicle, _})), Opts).

ro_txn(Body, Opts) ->
    Type =
        case ns_node_disco:couchdb_node() == node() of
            true ->
                couchdb;
            false ->
                chronicle
        end,
    ro_txn(Type, Body, Opts).

ro_txn(chronicle, Body, Opts) ->
    chronicle_kv:ro_txn(kv, ?cut(Body({chronicle, _})), Opts);
ro_txn(couchdb, Body, #{}) ->
    {ok, {ns_couchdb_chronicle_dup:ro_txn(?cut(Body({couchdb, _}))), no_rev}}.

txn_get(K, {chronicle, Txn}) ->
    chronicle_kv:txn_get(K, Txn);
txn_get(K, {couchdb, TxnGet}) ->
    TxnGet(K).

txn_get_many(Keys, {chronicle, Txn}) ->
    chronicle_kv:txn_get_many(Keys, Txn);
txn_get_many(Keys, {couchdb, _} = Txn) ->
    lists:foldl(
      fun (K, Acc) ->
              case txn_get(K, Txn) of
                  {ok, {V, R}} ->
                      maps:put(K, {V, R}, Acc);
                  {error, not_found} ->
                      Acc
              end
      end, #{}, Keys).

get_snapshot(Fetchers) ->
    get_snapshot(Fetchers, #{}).

get_snapshot(Fetchers, Opts) ->
    {Snapshot, _} = get_snapshot_with_revision(Fetchers, Opts),
    Snapshot.

get_snapshot_with_revision(Fetchers, Opts) ->
    {ok, SnapshotWithRev} =
        ro_txn(
          fun (Txn) ->
                  lists:foldl(
                    fun (Fetcher, Acc) ->
                            maps:merge(Fetcher(Txn), Acc)
                    end, #{}, Fetchers)
          end, Opts),
    SnapshotWithRev.

pull() ->
    pull(ns_config_rep:get_timeout(pull)).

pull(Timeout) ->
    do_pull(Timeout).

do_pull(Timeout) ->
    ok = chronicle_kv:sync(kv, Timeout).

push(Nodes) ->
    push(Nodes, ns_config_rep:get_timeout(push)).

push(Nodes, Timeout) ->
    {_, BadRPC, BadNodes} =
        misc:rpc_multicall_with_plist_result(Nodes, ?MODULE, do_pull, [Timeout],
                                             Timeout),
    case BadNodes =:= [] andalso BadRPC =:= [] of
        true ->
            ok;
        false ->
            Error = BadRPC ++ [{N, bad_node} || N <- BadNodes],
            ?log_warning("Failed to push chronicle config. Bad replies: ~p",
                         [Error]),
            {error, Error}
    end.

node_keys(Node, Buckets) ->
    [{node, Node, membership},
     {node, Node, services},
     {node, Node, recovery_type},
     {node, Node, failover_vbuckets},
     {node, Node, buckets_with_data} |
     [collections:last_seen_ids_key(Node, Bucket) || Bucket <- Buckets]].

service_keys(Service) ->
    [{service_map, Service},
     {service_failover_pending, Service}].
