%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks_bucket).

-behaviour(cb_deks).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").

-export([get_encryption_method/3,
         update_deks/2,
         get_required_usage/1,
         get_deks_lifetime/2,
         get_deks_rotation_interval/2,
         get_drop_deks_timestamp/2,
         get_force_encryption_timestamp/2,
         get_dek_ids_in_use/2,
         initiate_drop_deks/3,
         synchronize_deks/2,
         fetch_chronicle_keys_in_txn/2]).

-spec get_encryption_method(cb_deks:dek_kind(), cluster | node,
                            cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, cb_deks:encryption_method()} | {error, not_found}.
get_encryption_method({bucketDek, BucketUUID} = Kind, Scope, Snapshot)
                                            when Scope == cluster;
                                                 Scope == node ->
    maybe
        {ok, BucketName} ?= ns_bucket:uuid2bucket(BucketUUID, Snapshot),
        {ok, BucketConfig} ?= ns_bucket:get_bucket(BucketName, Snapshot),
        AnyBucketServices = cb_deks_cbauth:does_any_service_use_dek(Kind,
                                                                    Snapshot),
        %% Meaning of Scope:
        %% When Scope == cluster, we check if encryption for this bucket is
        %% enabled in general.
        %% When Scope == node, we check if this bucket is encrypted on this
        %% node (this node should have DEKs for this bucket)
        case (Scope == cluster) orelse
             bucket_exists_on_node(BucketUUID, Snapshot) orelse
             AnyBucketServices of
            true ->
                case proplists:get_value(encryption_secret_id, BucketConfig,
                                         ?SECRET_ID_NOT_SET) of
                    ?SECRET_ID_NOT_SET -> {ok, disabled};
                    Id -> {ok, {secret, Id}}
                end;
            false ->
                {error, not_found}
        end
    else
        not_present ->
            {error, not_found};
        {error, R} ->
            {error, R}
    end.

bucket_exists_on_node(BucketUUID, Snapshot) ->
    maybe
        {ok, BucketName} ?= ns_bucket:uuid2bucket(BucketUUID, Snapshot),
        {ok, BucketConfig} ?= ns_bucket:get_bucket(BucketName, Snapshot),
        IsNodeInServers = lists:member(node(), ns_bucket:get_servers(BucketConfig)),
        IsNodeInServers orelse
            begin %% In case of failed over node the data may still be on disk
                Services = ns_cluster_membership:node_services(Snapshot, node()),
                IsKVNode = lists:member(kv, Services),
                Dir = ns_storage_conf:this_node_bucket_dbdir(BucketUUID),
                ExistsOnDisk = filelib:is_dir(Dir),
                IsKVNode andalso ExistsOnDisk
            end
    else
        %% Bucket is not in config, so even if data is on disk it is not
        %% relevant and can be ignored.
        not_present ->
            false;
        {error, not_found} ->
            false
    end.

-spec update_deks(cb_deks:dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) -> ok | {error, _}.
update_deks({bucketDek, BucketUUID} = Kind, Snapshot) ->
    maybe
        ok ?= ns_memcached:set_active_dek_for_bucket_uuid(BucketUUID),
        ok ?= cb_deks_cbauth:update_deks(Kind, Snapshot),
        ok
    end.

-spec get_required_usage(cb_deks:dek_kind()) ->
          cb_cluster_secrets:secret_usage().
get_required_usage({bucketDek, BucketUUID}) ->
    {bucket_encryption, BucketUUID}.

-spec get_deks_lifetime(cb_deks:dek_kind(),
                        cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_lifetime({bucketDek, BucketUUID}, Snapshot) ->
    ns_bucket:get_dek_lifetime(BucketUUID, Snapshot).

-spec get_deks_rotation_interval(cb_deks:dek_kind(),
                                 cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_rotation_interval({bucketDek, BucketUUID}, Snapshot) ->
    ns_bucket:get_dek_rotation_interval(BucketUUID, Snapshot).

-spec get_drop_deks_timestamp(cb_deks:dek_kind(),
                              cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_drop_deks_timestamp({bucketDek, BucketUUID}, Snapshot) ->
    ns_bucket:get_drop_keys_timestamp(BucketUUID, Snapshot).

-spec get_force_encryption_timestamp(cb_deks:dek_kind(),
                                    cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_force_encryption_timestamp({bucketDek, BucketUUID}, Snapshot) ->
    ns_bucket:get_force_encryption_timestamp(BucketUUID, Snapshot).

-spec get_dek_ids_in_use(cb_deks:dek_kind(),
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, [cb_deks:dek_id()]} | {error, _}.
get_dek_ids_in_use({bucketDek, BucketUUID} = Kind, Snapshot) ->
    maybe
        {ok, Ids1} ?=
            case ns_memcached:get_dek_ids_in_use(BucketUUID) of
                {ok, Ids} -> {ok, Ids};
                {error, not_found} ->
                    %% We could not get it from memcached but it is
                    %% possible that we have data encrypted on disk.
                    %% If so, we should not assume there are no deks in use.
                    case bucket_exists_on_node(BucketUUID, Snapshot) of
                        true -> {error, not_found};
                        false -> {ok, []}
                    end
            end,
        {ok, Ids2} ?= cb_deks_cbauth:get_key_ids_in_use(Kind, Snapshot),
        {ok, lists:uniq(Ids1 ++ Ids2)}
    end.

-spec initiate_drop_deks(cb_deks:dek_kind(), [cb_deks:dek_id()],
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, done | started} | {error, not_found | retry | _}.
initiate_drop_deks({bucketDek, BucketUUID} = Kind, DekIds, Snapshot) ->
    Continuation = fun (_) ->
                       cb_cluster_secrets:dek_drop_complete(
                           {bucketDek, BucketUUID}, ok)
                   end,
    maybe
        {ok, started} ?=
            case bucket_exists_on_node(BucketUUID, Snapshot) of
                true ->
                    ns_memcached:drop_deks(BucketUUID, DekIds,
                                        cb_cluster_secrets,
                                        Continuation);
                false ->
                    {ok, started}
            end,
        {ok, started} ?= cb_deks_cbauth:initiate_drop_deks(Kind, DekIds,
                                                           Snapshot),
        {ok, started}
    end.

-spec synchronize_deks(cb_deks:dek_kind(),
                      cb_cluster_secrets:chronicle_snapshot()) ->
          ok | {error, _}.
synchronize_deks(Kind, Snapshot) ->
    maybe
        ok ?= cb_deks_cbauth:synchronize_deks(Kind, Snapshot),
        ok
    end.

-spec fetch_chronicle_keys_in_txn(cb_deks:dek_kind(), Txn :: term()) ->
          cb_cluster_secrets:chronicle_snapshot().
fetch_chronicle_keys_in_txn({bucketDek, BucketUUID} = Kind, Txn) ->
    BucketKeys = ns_bucket:all_keys_by_uuid([BucketUUID],
                                            [props, encr_at_rest, uuid], Txn),
    BucketSnapshot = chronicle_compat:txn_get_many(
                       [ns_bucket:root() | BucketKeys] ++
                       ns_cluster_membership:node_membership_keys(node()), Txn),
    CbauthSnapshot = cb_deks_cbauth:fetch_chronicle_keys_in_txn(Kind, Txn),
    maps:merge(BucketSnapshot, CbauthSnapshot).
