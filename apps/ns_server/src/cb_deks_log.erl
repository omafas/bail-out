%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(cb_deks_log).

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
         fetch_chronicle_keys_in_txn/2,
         try_drop_dek_work/2]).

%% exported for rpc calls
-export([handle_ale_log_dek_update/1]).

-define(LOG_ENCR_RPC_TIMEOUT, ?get_timeout(log_encr_rpc_timeout, 60000)).
-define(LOG_ENCR_ALE_DROP_DEK_TIMEOUT,
        ?get_timeout(log_encr_ale_drop_dek_timeout, 60000)).
-define(DROP_DEK_ALE_WORK_SZ_THRESH,
        ?get_param(drop_dek_ale_work_sz_thresh, 62914560)).

-spec get_encryption_method(cb_deks:dek_kind(), cluster | node,
                            cb_cluster_secrets:chronicle_snapshot()) ->
              {ok, cb_deks:encryption_method()} | {error, not_found}.
get_encryption_method(_Kind, Scope, Snapshot) ->
    cb_crypto:get_encryption_method(log_encryption, Scope, Snapshot).

-spec update_deks(cb_deks:dek_kind(),
                  cb_cluster_secrets:chronicle_snapshot()) -> ok | {error, _}.
update_deks(logDek = Kind, Snapshot) ->
    maybe
        %% DS can't be shared across nodes since it has atomic references, so we
        %% pass in function to allow local nodes to create DS based on same keys
        {ok, CurrDS} ?= cb_crypto:fetch_deks_snapshot(Kind),
        ok ?= cb_crypto:active_key_ok(CurrDS),
        CreateNewDS =
            fun(PrevDS) ->
                {ActiveKeyId, AllKeys} = cb_crypto:get_all_deks(CurrDS),
                cb_crypto:create_deks_snapshot(ActiveKeyId, AllKeys, PrevDS)
            end,

        %% Push the dek update to the local memcached instance
        ok ?= ns_memcached:set_active_dek("@logs", CurrDS),

        %% Push the dek update locally to ns_server disk sinks
        ok ?= handle_ale_log_dek_update(CreateNewDS),

        %% Push the dek update to babysitter node disk sinks
        ok ?= rpc:call(ns_server:get_babysitter_node(), ?MODULE,
                       handle_ale_log_dek_update, [CreateNewDS],
                       ?LOG_ENCR_RPC_TIMEOUT),

        %% Push the dek update to couchdb node disk sinks
        ok ?= rpc:call(ns_node_disco:couchdb_node(), ?MODULE,
                       handle_ale_log_dek_update, [CreateNewDS],
                       ?LOG_ENCR_RPC_TIMEOUT),

        %% Reencrypt all rebalance reports local to this node based on CurrentDS
        ok ?= ns_rebalance_report_manager:reencrypt_local_reports(CurrDS),

        %% Reencrypt USER_LOG
        ok ?= ns_log:reencrypt_data_on_disk(),

        %% Reencrypt event logs
        ok ?= event_log_server:reencrypt_data_on_disk(),

        %% Push the DEKs to services
        ok ?= cb_deks_cbauth:update_deks(Kind, Snapshot)
    else
        {error, _} = Error ->
            Error;
        {badrpc, _} = Error ->
            {error, Error}
    end.

-spec get_required_usage(cb_deks:dek_kind()) -> cb_cluster_secrets:secret_usage().
get_required_usage(_Kind) ->
    log_encryption.

-spec get_deks_lifetime(cb_deks:dek_kind(),
                        cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_lifetime(_Kind, Snapshot) ->
    cb_crypto:get_dek_kind_lifetime(log_encryption, Snapshot).

-spec get_deks_rotation_interval(cb_deks:dek_kind(),
                                 cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_deks_rotation_interval(_Kind, Snapshot) ->
    cb_crypto:get_dek_rotation_interval(log_encryption, Snapshot).

-spec get_drop_deks_timestamp(cb_deks:dek_kind(),
                              cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_drop_deks_timestamp(_Kind, Snapshot) ->
    cb_crypto:get_drop_keys_timestamp(log_encryption, Snapshot).

-spec get_force_encryption_timestamp(cb_deks:dek_kind(),
                                    cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_force_encryption_timestamp(_Kind, Snapshot) ->
    cb_crypto:get_force_encryption_timestamp(log_encryption, Snapshot).

-spec get_dek_ids_in_use(cb_deks:dek_kind(),
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, [cb_deks:dek_id()]} | {error, _}.
get_dek_ids_in_use(Kind, Snapshot) ->
    maybe
        {ok, InUseMemcached} ?= ns_memcached:get_dek_ids_in_use("@logs"),

        {ok, InUseLocal} ?= ale:get_all_used_deks(),

        {ok, InuseBabySitter} ?= rpc:call(ns_server:get_babysitter_node(),
                                          ale, get_all_used_deks, [],
                                          ?LOG_ENCR_RPC_TIMEOUT),

        {ok, InuseCouchDb} ?= rpc:call(ns_node_disco:couchdb_node(),
                                       ale, get_all_used_deks, [],
                                       ?LOG_ENCR_RPC_TIMEOUT),

        {ok, InUseRebReports} ?= ns_rebalance_report_manager:get_in_use_deks(),

        {ok, InUseLogs} ?= ns_log:get_in_use_deks(),

        {ok, InUseEventLogs} ?= event_log_server:get_in_use_deks(),

        {ok, InUseCbauth} ?= cb_deks_cbauth:get_key_ids_in_use(Kind, Snapshot),

        AllInUse = lists:map(
                      fun(undefined) ->
                              ?NULL_DEK;
                         (Elem) ->
                              Elem
                      end, InUseMemcached ++ InUseLocal ++ InuseBabySitter ++
                           InuseCouchDb ++ InUseRebReports ++ InUseLogs ++
                           InUseEventLogs ++ InUseCbauth),
        {ok, lists:usort(AllInUse)}
    else
        {error, _} = Error ->
            Error;
        {badrpc, _} = Error ->
            {error, Error}
    end.

-spec initiate_drop_deks(cb_deks:dek_kind(), [cb_deks:dek_id()],
                         cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, done | started} | {error, not_found | retry | _}.
initiate_drop_deks(Kind, DekIdsToDrop, Snapshot) ->
    {ok, DS} = cb_crypto:fetch_deks_snapshot(Kind),

    %% Ale logger treats "undefined" as a NULL_DEK so we convert it here
    %% for ale appropriately
    DropIdsForAle =
        lists:map(
          fun (?NULL_DEK) -> undefined;
              (Id) -> Id
          end, DekIdsToDrop),

    RPC_TIMEOUT = ?LOG_ENCR_ALE_DROP_DEK_TIMEOUT + 5000,
    Work =
        fun() ->
                R1 = ale:drop_log_deks(DropIdsForAle,
                                       ?DROP_DEK_ALE_WORK_SZ_THRESH,
                                       ?LOG_ENCR_ALE_DROP_DEK_TIMEOUT),
                R2 = rpc:call(ns_server:get_babysitter_node(), ale,
                              drop_log_deks, [DropIdsForAle,
                                              ?DROP_DEK_ALE_WORK_SZ_THRESH,
                                              ?LOG_ENCR_ALE_DROP_DEK_TIMEOUT],
                              RPC_TIMEOUT),
                R3 = rpc:call(ns_node_disco:couchdb_node(), ale,
                              drop_log_deks, [DropIdsForAle,
                                              ?DROP_DEK_ALE_WORK_SZ_THRESH,
                                              ?LOG_ENCR_ALE_DROP_DEK_TIMEOUT],
                              RPC_TIMEOUT),

                R4 = ns_rebalance_report_manager:reencrypt_local_reports(DS),
                R5 = ns_memcached:prune_log_or_audit_encr_keys("@logs",
                                                               DekIdsToDrop),
                %% Reencrypt USER_LOG
                R6 = ns_log:reencrypt_data_on_disk(),

                %% Reencrypt event logs
                R7 = event_log_server:reencrypt_data_on_disk(),

                Errors = lists:filtermap(
                           fun(ok) ->
                                   false;
                              ({error, Error}) ->
                                   {true, Error};
                              ({badrpc, _} = Error) ->
                                   {true, Error}
                           end , [R1, R2, R3, R4, R5, R6, R7]),

                case Errors of
                    [] ->
                        ok;
                    _ ->
                        {error, lists:flatten(Errors)}
                end
        end,
    maybe
        {ok, started} ?= try_drop_dek_work(Work, Kind),
        {ok, started} ?= cb_deks_cbauth:initiate_drop_deks(Kind, DekIdsToDrop,
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
fetch_chronicle_keys_in_txn(Kind, Txn) ->
    LogsSnapshot = chronicle_compat:txn_get_many(
                    [?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY], Txn),
    CbauthSnapshot = cb_deks_cbauth:fetch_chronicle_keys_in_txn(Kind, Txn),
    maps:merge(LogsSnapshot, CbauthSnapshot).

handle_ale_log_dek_update(CreateNewDS) ->
    Old = ale:get_global_log_deks_snapshot(),
    New = CreateNewDS(Old),
    case (cb_crypto:get_dek_id(Old) /= cb_crypto:get_dek_id(New)) of
        true ->
            ale:set_log_deks_snapshot(New);
        false ->
            ok
    end.

-spec try_drop_dek_work(fun(), logDek | auditDek) ->
          {ok, started} | {error, retry}.
try_drop_dek_work(Work, Type) ->
    WorkProcessName = list_to_atom(?MODULE_STRING ++ "-drop-dek-" ++
                                       atom_to_list(Type)),
    F =
        fun () ->
                try
                    erlang:register(WorkProcessName, self())
                catch
                    _:_ ->
                        proc_lib:init_fail({error, already_running},
                                           {exit, normal})
                end,
                proc_lib:init_ack(ok),
                Res = try
                          Work()
                      catch
                          T:E:Stack ->
                              ?log_error("Drop DEKs work failed: ~p",
                                         {T, E, Stack}),
                              {error, {T, E}}
                      end,
                cb_cluster_secrets:dek_drop_complete(Type, Res)
        end,

    case proc_lib:start_link(erlang, apply, [F, []]) of
        ok ->
            {ok, started};
        {error, already_running} ->
            {error, retry}
    end.
