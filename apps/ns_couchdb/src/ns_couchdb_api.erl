%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc api that contains almost everything that ns_server node calls on
%%      ns_couchdb node
%%

-module(ns_couchdb_api).

-include("ns_common.hrl").
-include("couch_db.hrl").

-export([get_tasks/0,
         get_tasks/2,
         restart_capi_ssl_service/0,
         fetch_stats/0,
         fetch_raw_stats/1,
         delete_databases_and_files/1,
         wait_index_updated/2,
         initiate_indexing/1,
         set_vbucket_states/3,
         reset_master_vbucket/1,
         get_design_doc_signatures/1,
         foreach_doc/3,
         update_doc/2,
         get_doc/2,

         get_master_vbucket_size/1,
         start_master_vbucket_compact/1,
         cancel_master_vbucket_compact/1,
         start_view_compact/5,
         cancel_view_compact/4,
         try_to_cleanup_indexes/1,
         get_view_group_data_size/3,
         get_safe_purge_seqs/1,
         log_diagnostics/1,
         get_pid/0]).

-export([handle_rpc/1]).

-spec get_tasks() -> [{atom(), any()}].
get_tasks() ->
    maybe_rpc_couchdb_node(get_tasks).

-spec get_tasks(timeout(), [{atom(), any()}]) -> [{atom(), any()}].
get_tasks(RpcTimeout, Default) ->
    maybe_rpc_couchdb_node(get_tasks, RpcTimeout, Default).

restart_capi_ssl_service() ->
    maybe_rpc_couchdb_node(restart_capi_ssl_service).

fetch_stats() ->
    maybe_rpc_couchdb_node(fetch_stats).

fetch_raw_stats(BucketName) ->
    maybe_rpc_couchdb_node({fetch_raw_stats, BucketName}).

delete_databases_and_files(Bucket) ->
    case maybe_rpc_couchdb_node({delete_databases_and_files, Bucket}) of
        {delete_vbuckets_error, Error} ->
            ale:error(?USER_LOGGER, "Unable to delete some DBs for bucket ~s. Leaving bucket directory undeleted~n~p", [Bucket, Error]),
            Error;
        {rm_rf_error, Error} ->
            ale:error(?USER_LOGGER, "Unable to delete bucket database directory ~s~n~p", [Bucket, Error]),
            Error;
        Other ->
            Other
    end.

wait_index_updated(Bucket, VBucket) ->
    maybe_rpc_couchdb_node({wait_index_updated, Bucket, VBucket}).

initiate_indexing(Bucket) ->
    maybe_rpc_couchdb_node({initiate_indexing, Bucket}).

set_vbucket_states(Bucket, WantedVBuckets, RebalanceVBuckets) ->
    maybe_rpc_couchdb_node({set_vbucket_states, Bucket, WantedVBuckets, RebalanceVBuckets}).

reset_master_vbucket(Bucket) ->
    maybe_rpc_couchdb_node({reset_master_vbucket, Bucket}).

get_design_doc_signatures(Bucket) ->
    maybe_rpc_couchdb_node({get_design_doc_signatures, Bucket}).

-spec foreach_doc(ext_bucket_name() | xdcr,
                  fun ((tuple()) -> any()),
                  non_neg_integer() | infinity) -> [{binary(), any()}].
foreach_doc(Bucket, Fun, Timeout) ->
    maybe_rpc_couchdb_node({foreach_doc, Bucket, Fun, Timeout}).

update_doc(Bucket, Doc) ->
    maybe_rpc_couchdb_node({update_doc, Bucket, Doc}).

-spec get_doc(ext_bucket_name() | xdcr, binary()) -> {ok, #doc{}} | {not_found, atom()}.
get_doc(Bucket, Id) ->
    maybe_rpc_couchdb_node({get_doc, Bucket, Id}).

get_master_vbucket_size(Bucket) ->
    maybe_rpc_couchdb_node({get_master_vbucket_size, Bucket}).

start_master_vbucket_compact(Bucket) ->
    maybe_rpc_couchdb_node({start_master_vbucket_compact, Bucket}).

cancel_master_vbucket_compact(Db) ->
    maybe_rpc_couchdb_node({cancel_master_vbucket_compact, Db}).

start_view_compact(Bucket, DDocId, Kind, Type, InitialStatus) ->
    maybe_rpc_couchdb_node({start_view_compact, Bucket, DDocId, Kind, Type, InitialStatus}).

cancel_view_compact(Bucket, DDocId, Kind, Type) ->
    maybe_rpc_couchdb_node({cancel_view_compact, Bucket, DDocId, Kind, Type}).

try_to_cleanup_indexes(BucketName) ->
    maybe_rpc_couchdb_node({try_to_cleanup_indexes, BucketName}).

get_view_group_data_size(BucketName, DDocId, Kind) ->
    maybe_rpc_couchdb_node({get_view_group_data_size, BucketName, DDocId, Kind}).

get_safe_purge_seqs(BucketName) ->
    maybe_rpc_couchdb_node({get_safe_purge_seqs, BucketName}).

log_diagnostics(Err) ->
    maybe_rpc_couchdb_node({log_diagnostics, Err}).

get_pid() ->
    maybe_rpc_couchdb_node(get_pid).

maybe_rpc_couchdb_node(Request) ->
    maybe_rpc_couchdb_node(Request, infinity, undefined).

maybe_rpc_couchdb_node(Request, RpcTimeout, Default) ->
    ThisNode = node(),
    case ns_node_disco:couchdb_node() of
        ThisNode ->
            handle_rpc(Request);
        Node ->
            rpc_couchdb_node(Node, Request, RpcTimeout, Default)
    end.

rpc_couchdb_node(Node, Request, RpcTimeout, Default) ->
    RV = rpc:call(Node, ?MODULE, handle_rpc, [Request], RpcTimeout),
    case {RV, Default} of
        {{badrpc, _}, undefined} ->
            {current_stacktrace, Stack} = erlang:process_info(
                                            self(), current_stacktrace),
            ?log_debug("RPC to couchdb node failed for ~p with ~p~nStack: ~p",
                       [Request, RV, Stack]),
            exit({error, RV});
        {{badrpc, _}, Default} ->
            ?log_debug("RPC to couchdb node failed for ~p with ~p. Use default value ~p~n",
                       [Request, RV, Default]),
            Default;
        {_, _} ->
            RV
    end.

handle_rpc(get_tasks) ->
    couch_task_status:all();
handle_rpc(restart_capi_ssl_service) ->
    ns_couchdb_sup:restart_capi_ssl_service();
handle_rpc(fetch_stats) ->
    ns_couchdb_stats_collector:get_stats();
handle_rpc({fetch_raw_stats, BucketName}) ->
    couch_stats_reader:grab_raw_stats(BucketName);
handle_rpc({delete_databases_and_files, Bucket}) ->
    ns_couchdb_storage:delete_databases_and_files(Bucket);
handle_rpc({initiate_indexing, Bucket}) ->
    capi_set_view_manager:initiate_indexing(Bucket);
handle_rpc({wait_index_updated, Bucket, VBucket}) ->
    capi_set_view_manager:wait_index_updated(Bucket, VBucket);
handle_rpc({set_vbucket_states, BucketName, WantedVBuckets, RebalanceVBuckets}) ->
    capi_set_view_manager:set_vbucket_states(BucketName,
                                             WantedVBuckets,
                                             RebalanceVBuckets);
handle_rpc({reset_master_vbucket, BucketName}) ->
    capi_ddoc_manager:reset_master_vbucket(BucketName);

handle_rpc({get_design_doc_signatures, Bucket}) ->
    {capi_utils:get_design_doc_signatures(mapreduce_view, Bucket),
     capi_utils:get_design_doc_signatures(spatial_view, Bucket)};

handle_rpc({foreach_doc, Bucket, Fun, Timeout}) ->
    capi_ddoc_manager:foreach_doc(Bucket, Fun, Timeout);

handle_rpc({update_doc, Bucket, #doc{id = <<"_local/", _/binary>>} = Doc}) ->
    capi_frontend:with_master_vbucket(
      Bucket,
      fun (DB) ->
              ok = couch_db:update_doc(DB, Doc)
      end);
handle_rpc({update_doc, Bucket, Doc}) ->
    capi_ddoc_manager:update_doc(Bucket, Doc);

handle_rpc({get_doc, Bucket, <<"_local/", _/binary>> = Id}) ->
    capi_frontend:with_master_vbucket(
      Bucket,
      fun (DB) ->
              couch_db:open_doc_int(DB, Id, [ejson_body])
      end);

handle_rpc({get_master_vbucket_size, Bucket}) ->
    capi_frontend:with_master_vbucket(
      Bucket,
      fun (Db) ->
              {ok, DbInfo} = couch_db:get_db_info(Db),

              {proplists:get_value(data_size, DbInfo, 0),
               proplists:get_value(disk_size, DbInfo)}
      end);

handle_rpc({start_master_vbucket_compact, Bucket}) ->
    capi_frontend:with_master_vbucket(
      Bucket,
      fun (Db) ->
              {ok, Compactor} = couch_db:start_compact(Db, [dropdeletes]),
              %% return Db here assuming that Db#db.update_pid is alive and well
              %% after the Db is closed
              {ok, Compactor, Db}
      end);

handle_rpc({cancel_master_vbucket_compact, Db}) ->
    couch_db:cancel_compact(Db);

handle_rpc({start_view_compact, Bucket, DDocId, Kind, Type, InitialStatus}) ->
    couch_set_view_compactor:start_compact(Kind, Bucket,
                                           DDocId, Type, prod,
                                           InitialStatus);

handle_rpc({cancel_view_compact, Bucket, DDocId, Kind, Type}) ->
    couch_set_view_compactor:cancel_compact(Kind,
                                            Bucket, DDocId,
                                            Type, prod);

handle_rpc({try_to_cleanup_indexes, BucketName}) ->
    ?log_info("Cleaning up indexes for bucket `~s`", [BucketName]),

    try
        couch_set_view:cleanup_index_files(mapreduce_view, BucketName)
    catch SetViewT:SetViewE:SetViewStack ->
            ?log_error("Error while doing cleanup of old "
                       "index files for bucket `~s`: ~p~n~p",
                       [BucketName, {SetViewT, SetViewE}, SetViewStack])
    end,

    try
        couch_set_view:cleanup_index_files(spatial_view, BucketName)
    catch SpatialT:SpatialE:SpatialStack ->
            ?log_error("Error while doing cleanup of old "
                       "spatial index files for bucket `~s`: ~p~n~p",
                       [BucketName, {SpatialT, SpatialE}, SpatialStack])
    end;

handle_rpc({get_view_group_data_size, BucketName, DDocId, Kind}) ->
    couch_set_view:get_group_data_size(Kind, BucketName, DDocId);

handle_rpc({get_safe_purge_seqs, BucketName}) ->
    capi_set_view_manager:get_safe_purge_seqs(BucketName);

handle_rpc({log_diagnostics, Err}) ->
    timeout_diag_logger:do_log_diagnostics(Err);

handle_rpc(get_pid) ->
    list_to_integer(os:getpid()).
