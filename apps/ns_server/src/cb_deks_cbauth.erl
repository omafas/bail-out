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

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([update_deks/2,
         get_key_ids_in_use/2,
         initiate_drop_deks/3,
         synchronize_deks/2,
         fetch_chronicle_keys_in_txn/2,
         cbauth_key_type_to_dek_kind/2,
         does_any_service_use_dek/2]).

-define(CBAUTH_RPC_TIMEOUT, ?get_timeout(cbauth_rpc_timeout, 60000)).

-spec update_deks(cb_deks:dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) -> ok | {error, _}.
update_deks(Kind, Snapshot) ->
    CbauthLabels = get_cbauth_labels(Kind, Snapshot),
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
        {ok, _} ?= cbauth_call("UpdateKeysDB", Params, Kind, CbauthLabels),
        ok
    end.

-spec get_key_ids_in_use(cb_deks:dek_kind(),
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, [cb_deks:dek_id()]} | {error, _}.
get_key_ids_in_use(Kind, Snapshot) ->
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

-spec fetch_chronicle_keys_in_txn(cb_deks:dek_kind(), Txn :: term()) ->
          cb_cluster_secrets:chronicle_snapshot().
fetch_chronicle_keys_in_txn(_Kind, Txn) ->
    chronicle_compat:txn_get_many(
      ns_cluster_membership:node_membership_keys(node()),
      Txn).

-spec does_any_service_use_dek(cb_deks:dek_kind(),
                               cb_cluster_secrets:chronicle_snapshot()) ->
          boolean().
does_any_service_use_dek(DekKind, Snapshot) ->
    length(get_cbauth_labels(DekKind, Snapshot)) > 0.

%% This function defines which services use which encr-at-rest keys.
%% Currently returns [] for all supported kinds, but structure is in place
%% for future services that may use these DEKs.
get_cbauth_services(logDek) -> 
    [
    %% n1ql %% uncomment to pass log keys to n1ql
    %% fts %% uncomment to pass log keys to fts
    %% index %% uncomment to pass log keys to index
    %% cbas %% uncomment to pass log keys to cbas
    %% eventing %% uncomment to pass log keys to eventing
    ];
get_cbauth_services({bucketDek, _}) ->
    [
    %% n1ql %% uncomment to pass bucket keys to n1ql
    %% fts %% uncomment to pass bucket keys to fts
    %% index %% uncomment to pass bucket keys to index
    %% cbas %% uncomment to pass bucket keys to cbas
    %% eventing %% uncomment to pass bucket keys to eventing
    ];
%% This is a fake kind just to trick dialyzer that this function can return
%% a non-empty list. To be removed once we have real services that use
%% encryption.
get_cbauth_services(unknown_kind) -> [unknown_service];
get_cbauth_services(_) -> error(invalid_dek_kind).

get_cbauth_labels(Kind, Snapshot) ->
    Services = get_cbauth_services(Kind),
    NodeServices = ns_cluster_membership:node_services(Snapshot, node()),
    TargetServices = [P || P <- Services, lists:member(P, NodeServices)],
    [menelaus_cbauth:service_to_label(P) || P <- TargetServices].

cbauth_call(Func, Params, Kind, CbauthLabels) ->
    cbauth_call(Func, Params, Kind, CbauthLabels, []).

cbauth_call(_Func, _Params, _Kind, [], Res) ->
    {ok, lists:reverse(Res)};
cbauth_call(Func, Params, Kind, [CbauthLabel | Tail], Acc) ->
    ?log_debug("Calling ~s for ~s for ~p", [Func, CbauthLabel, Kind]),
    Opts = #{silent => false, timeout => ?CBAUTH_RPC_TIMEOUT},
    try json_rpc_connection:perform_call(CbauthLabel,
                                         "AuthCacheSvc." ++ Func,
                                         Params, Opts) of
        {ok, Res} ->
            ?log_debug("~s at ~s returned ok", [Func, CbauthLabel]),
            cbauth_call(Func, Params, Kind, Tail, [Res | Acc]);
        {error, Reason} ->
            ?log_error("Failed to call ~s at ~s for ~p: ~p",
                       [Func, CbauthLabel, Kind, Reason]),
            {error, {cbauth_call_failed, Reason}}
    catch exit:{noproc, _} ->
            ?log_error("Failed to call ~s at ~s for ~p: process is not running "
                       "or there is no cbauth connection yet",
                       [Func, CbauthLabel, Kind]),
            {error, not_running}
    end.

dek_kind_to_json(Kind) ->
    {TypeName, BucketUUID} =
        case Kind of
            {bucketDek, UUID} -> {<<"bucket">>, UUID};
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

    ?assertEqual({error, "bucketUUID is required for bucketDek"},
                 cbauth_key_type_to_dek_kind("bucket", undefined)),
    ?assertEqual({error, "bucketUUID is required for bucketDek"},
                 cbauth_key_type_to_dek_kind("bucket", "")),
    ?assertEqual({error, "invalid type: \"unknown\""},
                 cbauth_key_type_to_dek_kind("unknown", undefined)),
    ?assertEqual({error, "invalid type: \"unknown\""},
                 cbauth_key_type_to_dek_kind("unknown", "")),
    ?assertEqual({error, "invalid type: \"unknown\""},
                 cbauth_key_type_to_dek_kind("unknown", "123")).


-endif.
