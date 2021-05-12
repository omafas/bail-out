%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc module that contains storage related functions to be executed on ns_couchdb node
%%

-module(ns_couchdb_storage).

-include("ns_common.hrl").

-export([delete_databases_and_files/1]).

delete_databases_and_files(Bucket) ->
    AllDBs = bucket_databases(Bucket),
    MasterDB = iolist_to_binary([Bucket, <<"/master">>]),
    {MaybeMasterDb, RestDBs} = lists:partition(
                            fun (Name) ->
                                    Name =:= MasterDB
                            end, AllDBs),
    RV = case delete_databases_loop(MaybeMasterDb ++ RestDBs) of
             ok ->
                 {ok, DbDir} = ns_storage_conf:this_node_dbdir(),
                 Path = filename:join(DbDir, Bucket),
                 ?log_info("Couch dbs are deleted. Proceeding with bucket directory ~p", [Path]),
                 case misc:rm_rf(Path) of
                     ok -> ok;
                     Error ->
                         {rm_rf_error, Error}
                 end;
             Error ->
                 {delete_vbuckets_error, Error}
         end,
    do_delete_bucket_indexes(Bucket),
    ?log_info("Bucket ~p deletion has finished with ~p", [Bucket, RV]),
    RV.

do_delete_bucket_indexes(Bucket) ->
    {ok, BaseIxDir} = ns_storage_conf:this_node_ixdir(),
    ?log_info("Start deleting bucket ~p indexes at ~p", [Bucket, BaseIxDir]),
    couch_set_view:delete_index_dir(BaseIxDir, list_to_binary(Bucket)).

delete_databases_loop([]) ->
    ok;
delete_databases_loop([Db | Rest]) ->
    case delete_couch_database(Db) of
        ok ->
            delete_databases_loop(Rest);
        Error ->
            Error
    end.

bucket_databases(Bucket) when is_list(Bucket) ->
    bucket_databases(list_to_binary(Bucket));
bucket_databases(Bucket) when is_binary(Bucket) ->
    couch_server:all_known_databases_with_prefix(iolist_to_binary([Bucket, $/])).

delete_couch_database(DB) ->
    RV = couch_server:delete(DB, []),
    ?log_info("Deleting database ~p: ~p~n", [DB, RV]),
    RV.
