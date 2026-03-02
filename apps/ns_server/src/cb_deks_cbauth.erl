%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks_cbauth).

-behaviour(cb_deks).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
         fetch_chronicle_keys_in_txn/2,
         dek_consumers/2]).

-export([cbauth_key_type_to_dek_kind/2,
         get_kinds_for_label/1,
         call_update_keys_db/2]).

-define(CBAUTH_RPC_TIMEOUT, ?get_timeout(cbauth_rpc_timeout, 60000)).

%% cb_deks callbacks for {serviceBucketDek, _}

-spec get_encryption_method(cb_deks:dek_kind(), cluster | node,
                            cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, cb_deks:encryption_method()} | {error, not_found}.
get_encryption_method({serviceBucketDek, BucketUUID} = Kind,
                      Scope, Snapshot)
                          when Scope == cluster;
                               Scope == node ->
    maybe
        {ok, BucketName} ?= ns_bucket:uuid2bucket(BucketUUID, Snapshot),
        {ok, BucketConfig} ?= ns_bucket:get_bucket(BucketName, Snapshot),
        case (Scope == cluster) orelse
             does_any_service_use_dek(Kind, Snapshot) of
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

-spec update_deks(cb_deks:dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) -> ok | {error, _}.
update_deks(Kind, Snapshot) ->
    CbauthLabels = get_cbauth_labels(Kind, Snapshot),
    call_update_keys_db(Kind, CbauthLabels, true).

call_update_keys_db(Kind, CbauthLabels) ->
    call_update_keys_db(Kind, CbauthLabels, false).

call_update_keys_db(Kind, CbauthLabels, IgnoreMissing) ->
    {ok, DS} = cb_crypto:fetch_deks_snapshot(Kind),
    {ActiveDek, AllDeks} = cb_crypto:get_all_deks(DS),
    {KeyStoreJsonProps} = memcached_bucket_config:format_mcd_keys(ActiveDek,
                                                                  AllDeks),
    UnavailKeys = [Id || ?DEK_ERROR_PATTERN(Id, _) <- AllDeks],
    DekPath = case encryption_service:key_path(Kind) of
                  undefined -> <<>>;
                  Path -> iolist_to_binary(Path)
              end,
    Params = {[{dataType, dek_kind_to_json(Kind)},
               {unavailableKeys, UnavailKeys},
               {path, DekPath} |
               KeyStoreJsonProps]},
    maybe
        {ok, _} ?= cbauth_call("UpdateKeysDB", Params, Kind, CbauthLabels,
                               #{ignore_missing_connection => IgnoreMissing}),
        ok
    end.

-spec dek_consumers(cb_deks:dek_kind(),
                    cb_cluster_secrets:chronicle_snapshot()) -> [term()].
dek_consumers(Kind, Snapshot) ->
    CbauthLabels = get_cbauth_labels(Kind, Snapshot),
    [json_rpc_connection:get_pid(L) || L <- CbauthLabels].

-spec get_dek_ids_in_use(cb_deks:dek_kind(),
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, [cb_deks:dek_id()]} | {error, _}.
get_dek_ids_in_use(Kind, Snapshot) ->
    CbauthLabels = get_cbauth_labels(Kind, Snapshot),
    Params = dek_kind_to_json(Kind),
    maybe
        {ok, Res} ?= cbauth_call("GetInUseKeys", Params, Kind, CbauthLabels),
        Ids = lists:uniq(lists:append(Res)),
        TranslatedIds = lists:map(fun (<<>>) -> ?NULL_DEK;
                                      (Id) -> Id
                                  end, Ids),
        {ok, TranslatedIds}
    end.

-spec initiate_drop_deks(cb_deks:dek_kind(), [cb_deks:dek_id() | ?NULL_DEK],
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, started} | {error, _}.
initiate_drop_deks(Kind, DekIdsToDrop, Snapshot) ->
    CbauthLabels = get_cbauth_labels(Kind, Snapshot),
    PreparedDekIdsToDrop = lists:map(fun (?NULL_DEK) -> <<>>;
                                         (Id) -> Id
                                     end, DekIdsToDrop),
    Params = {[{dataType, dek_kind_to_json(Kind)},
               {keys, PreparedDekIdsToDrop}]},
    maybe
        {ok, _} ?= cbauth_call("DropKeys", Params, Kind, CbauthLabels),
        {ok, started}
    end.

-spec synchronize_deks(cb_deks:dek_kind(),
                      cb_cluster_secrets:chronicle_snapshot()) ->
          ok | {error, _}.
synchronize_deks(Kind, Snapshot) ->
    CbauthLabels = get_cbauth_labels(Kind, Snapshot),
    Params = dek_kind_to_json(Kind),
    maybe
        {ok, _} ?= cbauth_call("SynchronizeKeyFiles", Params, Kind, CbauthLabels),
        ok
    end.

-spec get_required_usage(cb_deks:dek_kind()) ->
          cb_cluster_secrets:secret_usage().
get_required_usage({serviceBucketDek, BucketUUID}) ->
    cb_deks_bucket:get_required_usage({bucketDek, BucketUUID}).

-spec get_deks_lifetime(cb_deks:dek_kind(),
                        cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_lifetime({serviceBucketDek, BucketUUID}, Snapshot) ->
    cb_deks_bucket:get_deks_lifetime({bucketDek, BucketUUID}, Snapshot).

-spec get_deks_rotation_interval(
        cb_deks:dek_kind(),
        cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_rotation_interval({serviceBucketDek, BucketUUID}, Snapshot) ->
    cb_deks_bucket:get_deks_rotation_interval({bucketDek, BucketUUID},
                                              Snapshot).

-spec get_drop_deks_timestamp(cb_deks:dek_kind(),
                              cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_drop_deks_timestamp({serviceBucketDek, BucketUUID}, Snapshot) ->
    cb_deks_bucket:get_drop_deks_timestamp({bucketDek, BucketUUID}, Snapshot).

-spec get_force_encryption_timestamp(cb_deks:dek_kind(),
                                     cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_force_encryption_timestamp({serviceBucketDek, BucketUUID}, Snapshot) ->
    cb_deks_bucket:get_force_encryption_timestamp({bucketDek, BucketUUID},
                                                  Snapshot).

-spec fetch_chronicle_keys_in_txn(cb_deks:dek_kind(),
                                  Txn :: term()) ->
          cb_cluster_secrets:chronicle_snapshot().
fetch_chronicle_keys_in_txn({serviceBucketDek, BucketUUID}, Txn) ->
    %% assuming that node_membership_keys will be added by cb_deks_bucket
    cb_deks_bucket:fetch_chronicle_keys_in_txn({bucketDek, BucketUUID}, Txn);
fetch_chronicle_keys_in_txn(_Kind, Txn) ->
    chronicle_compat:txn_get_many(
      ns_cluster_membership:node_membership_keys(node()),
      Txn).

-spec does_any_service_use_dek(cb_deks:dek_kind(),
                               cb_cluster_secrets:chronicle_snapshot()) ->
          boolean().
does_any_service_use_dek(DekKind, Snapshot) ->
    length(get_cbauth_labels(DekKind, Snapshot)) > 0.

-spec get_kinds_for_label(string()) -> [cb_deks:dek_kind()].
get_kinds_for_label(Label) ->
    Snapshot = chronicle_compat:get_snapshot(
                 [fun cb_cluster_secrets:fetch_snapshot_in_txn/1], #{}),
    Kinds = cb_deks:dek_kinds_list_existing_on_node(Snapshot),
    lists:filter(
        fun (Kind) ->
            try
                Labels = get_cbauth_labels(Kind, Snapshot),
                lists:member(Label, Labels)
            catch
                error:invalid_dek_kind -> false
            end
        end, Kinds).

%% This function defines which services use which encr-at-rest keys.
%% Currently returns [] for all supported kinds, but structure is in place
%% for future services that may use these DEKs.
get_cbauth_services(logDek) -> 
    [
    %% n1ql %% uncomment to pass log keys to n1ql
    %% fts %% uncomment to pass log keys to fts
    %% index %% uncomment to pass log keys to index
    %% projector %% uncomment to pass log keys to projector
    %% cbas %% uncomment to pass log keys to cbas
    %% eventing %% uncomment to pass log keys to eventing
    ];
get_cbauth_services({serviceBucketDek, _}) ->
    [
    %% n1ql %% uncomment to pass service bucket keys to n1ql
    %% fts %% uncomment to pass service bucket keys to fts
    %% index %% uncomment to pass service bucket keys to index
    %% cbas %% uncomment to pass service bucket keys to cbas
    %% eventing %% uncomment to pass service bucket keys to eventing
    ];
%% This is a fake kind just to trick dialyzer that this function can return
%% a non-empty list. To be removed once we have real services that use
%% encryption.
get_cbauth_services(unknown_kind) -> [unknown_service];
get_cbauth_services(_) -> error(invalid_dek_kind).

get_cbauth_labels(Kind, Snapshot) ->
    Node = node(),
    case ns_cluster_membership:is_newly_added_node(Node, Snapshot) of
        true -> [];
        false ->
            Services = get_cbauth_services(Kind),
            NodeServices = ns_cluster_membership:node_services(Snapshot, Node),
            AllNodeServices = case lists:member(kv, NodeServices) of
                                  true -> NodeServices ++ [projector];
                                  false -> NodeServices
                              end,
            TargetServices = [P || P <- Services,
                                   lists:member(P, AllNodeServices)],
            [menelaus_cbauth:service_to_label(P) || P <- TargetServices]
    end.

cbauth_call(Func, Params, Kind, CbauthLabels) ->
    cbauth_call(Func, Params, Kind, CbauthLabels, #{}).

cbauth_call(Func, Params, Kind, CbauthLabels, Opts) ->
    cbauth_call(Func, Params, Kind, CbauthLabels, Opts, []).

cbauth_call(_Func, _Params, _Kind, [], _Opts, Res) ->
    {ok, lists:reverse(Res)};
cbauth_call(Func, Params, Kind, [CbauthLabel | Tail], Opts, Acc) ->
    ?log_debug("Calling ~s for ~s for ~p", [Func, CbauthLabel, Kind]),
    RpcOpts = #{silent => false, timeout => ?CBAUTH_RPC_TIMEOUT},
    try json_rpc_connection:perform_call(CbauthLabel,
                                         "AuthCacheSvc." ++ Func,
                                         Params, RpcOpts) of
        {ok, Res} ->
            ?log_debug("~s at ~s returned ok", [Func, CbauthLabel]),
            cbauth_call(Func, Params, Kind, Tail, Opts, [Res | Acc]);
        {error, Reason} ->
            ?log_error("Failed to call ~s at ~s for ~p: ~p",
                       [Func, CbauthLabel, Kind, Reason]),
            {error, {cbauth_call_failed, Reason}}
    catch exit:{noproc, _} ->
            ?log_debug("Failed to call ~s at ~s for ~p: process is not running "
                       "or there is no cbauth connection yet",
                       [Func, CbauthLabel, Kind]),
            case maps:get(ignore_missing_connection, Opts, false) of
                true ->
                    Res = {ok, undefined},
                    cbauth_call(Func, Params, Kind, Tail, Opts, [Res | Acc]);
                false ->
                    {error, retry}
            end
    end.

dek_kind_to_json(Kind) ->
    {TypeName, BucketUUID} =
        case Kind of
            {bucketDek, UUID} -> {<<"bucket">>, UUID};
            {serviceBucketDek, UUID} -> {<<"service_bucket">>, UUID};
            logDek -> {<<"log">>, <<>>}
            %% Other kinds are not supported used by any service currently,
            %% so we don't need to support them here (dialyzer complains about
            %% them being unused)
        end,
    {[{typeName, TypeName},
      {bucketUUID, BucketUUID}]}.

-spec cbauth_key_type_to_dek_kind(string(), undefined | string()) ->
          {ok, cb_deks:dek_kind()} | {error, string()}.
cbauth_key_type_to_dek_kind("bucket", undefined) ->
    {error, "bucketUUID is required for bucketDek"};
cbauth_key_type_to_dek_kind("bucket", "") ->
    {error, "bucketUUID is required for bucketDek"};
cbauth_key_type_to_dek_kind("bucket", BucketUUID) ->
    {ok, {bucketDek, iolist_to_binary(BucketUUID)}};
cbauth_key_type_to_dek_kind("service_bucket", undefined) ->
    {error, "bucketUUID is required for serviceBucketDek"};
cbauth_key_type_to_dek_kind("service_bucket", "") ->
    {error, "bucketUUID is required for serviceBucketDek"};
cbauth_key_type_to_dek_kind("service_bucket", BucketUUID) ->
    {ok, {serviceBucketDek, iolist_to_binary(BucketUUID)}};
cbauth_key_type_to_dek_kind("log", _) ->
    {ok, logDek};
cbauth_key_type_to_dek_kind("config", _) ->
    {ok, configDek};
cbauth_key_type_to_dek_kind("audit", _) ->
    {ok, auditDek};
cbauth_key_type_to_dek_kind(TypeStr, _) ->
    Error = lists:flatten(io_lib:format("invalid type: ~p", [TypeStr])),
    {error, Error}.

-ifdef(TEST).

cbauth_key_type_to_dek_kind_test() ->
    %% Make sure the test fails if we add a new kind and forget to add
    %% a clause for it in cbauth_key_type_to_dek_kind/2
    HandledKinds =
        lists:map(
          fun ({Type, Expected}) ->
              {ok, Res} = cbauth_key_type_to_dek_kind(Type, undefined),
              ?assertEqual(Expected, Res),
              Res
          end, [{"config", configDek},
                {"log", logDek},
                {"audit", auditDek}]),

    ?assertEqual([], ?DEK_KIND_LIST_STATIC -- HandledKinds),

    ?assertEqual(
       {error, "bucketUUID is required for bucketDek"},
       cbauth_key_type_to_dek_kind("bucket", undefined)),
    ?assertEqual(
       {error, "bucketUUID is required for bucketDek"},
       cbauth_key_type_to_dek_kind("bucket", "")),
    ?assertEqual(
       {error, "bucketUUID is required for serviceBucketDek"},
       cbauth_key_type_to_dek_kind("service_bucket", undefined)),
    ?assertEqual(
       {error, "bucketUUID is required for serviceBucketDek"},
       cbauth_key_type_to_dek_kind("service_bucket", "")),
    ?assertEqual(
       {ok, {serviceBucketDek, <<"test-uuid">>}},
       cbauth_key_type_to_dek_kind("service_bucket", "test-uuid")),
    ?assertEqual(
       {error, "invalid type: \"unknown\""},
       cbauth_key_type_to_dek_kind("unknown", undefined)),
    ?assertEqual(
       {error, "invalid type: \"unknown\""},
       cbauth_key_type_to_dek_kind("unknown", "")),
    ?assertEqual(
       {error, "invalid type: \"unknown\""},
       cbauth_key_type_to_dek_kind("unknown", "123")).

-endif.
