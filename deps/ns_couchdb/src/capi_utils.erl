%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

-module(capi_utils).

-compile(nowarn_export_all).
-compile(export_all).

-include("ns_common.hrl").
-include("couch_db.hrl").
-include("mc_entry.hrl").
-include("mc_constants.hrl").
-include("ns_config.hrl").

%% returns capi port for given node or undefined if node doesn't have CAPI
compute_capi_port({ssl, Node}) ->
    service_ports:get_port(ssl_capi_port, ns_config:latest(), Node);

compute_capi_port(Node) ->
    service_ports:get_port(capi_port, ns_config:latest(), Node).

%% returns http url to capi on given node with given path
-spec capi_url_bin(node() | {ssl, node()}, iolist() | binary(), iolist() | binary()) -> undefined | binary().
capi_url_bin(Node, Path, LocalAddr) ->
    case capi_url_cache:get_capi_base_url(Node, LocalAddr) of
        undefined -> undefined;
        X ->
            iolist_to_binary([X, Path])
    end.

capi_bucket_url_bin(Node, BucketName, BucketUUID, LocalAddr) ->
    capi_url_bin(Node, menelaus_util:concat_url_path([BucketName ++ "+" ++ ?b2l(BucketUUID)]), LocalAddr).

split_dbname(DbName) ->
    DbNameStr = binary_to_list(DbName),
    Tokens = string:tokens(DbNameStr, [$/]),
    build_info(Tokens, []).

build_info([VBucketStr], R) ->
    {lists:append(lists:reverse(R)), list_to_integer(VBucketStr)};
build_info([H|T], R)->
    build_info(T, [H|R]).

-spec split_dbname_with_uuid(DbName :: binary()) ->
                                    {binary(), binary() | undefined, binary() | undefined}.
split_dbname_with_uuid(DbName) ->
    [BeforeSlash | AfterSlash] = binary:split(DbName, <<"/">>),
    {BucketName, UUID} =
        case binary:split(BeforeSlash, <<"+">>) of
            [BN, U] ->
                {BN, U};
            [BN] ->
                {BN, undefined}
        end,

    {VBucketId, UUID1} = split_vbucket(AfterSlash, UUID),
    {BucketName, VBucketId, UUID1}.

%% if xdcr connects to pre 3.0 clusters we will receive UUID as part of VBucketID
%% use it if no UUID was found in BucketName
split_vbucket([], UUID) ->
    {undefined, UUID};
split_vbucket([AfterSlash], UUID) ->
    case {binary:split(AfterSlash, <<";">>), UUID} of
        {[VB, U], undefined} ->
            {VB, U};
        {[VB, _], UUID} ->
            {VB, UUID};
        {[VB], UUID} ->
            {VB, UUID}
    end.


-spec build_dbname(BucketName :: ext_bucket_name(), VBucket :: ext_vbucket_id()) -> binary().
build_dbname(BucketName, VBucket) ->
    SubName = case is_binary(VBucket) of
                  true -> VBucket;
                  _ -> integer_to_list(VBucket)
              end,
    iolist_to_binary([BucketName, $/, SubName]).


must_open_master_vbucket(BucketName) ->
    DBName = build_dbname(BucketName, <<"master">>),
    case couch_db:open_int(DBName, []) of
        {ok, RealDb} ->
            RealDb;
        Error ->
            exit({open_db_failed, Error})
    end.

couch_doc_to_json(Doc, parsed) ->
    couch_doc:to_json_obj(Doc);
couch_doc_to_json(Doc, unparsed) ->
    couch_doc:to_json_obj_with_bin_body(Doc).

extract_doc_id(Doc) ->
    Doc#doc.id.

sort_by_doc_id(Docs) ->
    lists:keysort(#doc.id, Docs).

capture_local_master_docs(Bucket, Timeout) ->
    misc:executing_on_new_process(
      fun () ->
              case Timeout of
                  infinity -> ok;
                  _ -> timer:kill_after(Timeout)
              end,
              DB = must_open_master_vbucket(Bucket),
              {ok, _, LocalDocs} = couch_btree:fold(DB#db.local_docs_btree,
                                                    fun (Doc, _, Acc) ->
                                                            {ok, [Doc | Acc]}
                                                    end, [], []),
              LocalDocs
      end).

-spec fetch_ddoc_ids(bucket_name() | binary()) -> [binary()].
fetch_ddoc_ids(Bucket) ->
    Pairs = foreach_live_ddoc_id(Bucket, fun (_) -> ok end),
    erlang:element(1, lists:unzip(Pairs)).

-spec foreach_live_ddoc_id(bucket_name() | binary(),
                           fun ((binary()) -> any())) -> [{binary(), any()}].
foreach_live_ddoc_id(Bucket, Fun) ->
    Ref = make_ref(),
    RVs = ns_couchdb_api:foreach_doc(
            Bucket,
            fun (Doc) ->
                    case Doc of
                        #doc{deleted = true} ->
                            Ref;
                        _ ->
                            Fun(Doc#doc.id)
                    end
            end, infinity),
    [Pair || {_Id, V} = Pair <- RVs,
             V =/= Ref].

full_live_ddocs(Bucket) ->
    full_live_ddocs(Bucket, infinity).

full_live_ddocs(Bucket, Timeout) ->
    full_live_ddocs(Bucket, Timeout, fun (Doc) -> Doc end).

full_live_ddocs(Bucket, Timeout, Fun) ->
    Ref = make_ref(),
    RVs = ns_couchdb_api:foreach_doc(
            Bucket,
            fun (Doc) ->
                    case Doc of
                        #doc{deleted = true} ->
                            Ref;
                        _ ->
                            Fun(Doc)
                    end
            end, Timeout),
    [V || {_Id, V} <- RVs,
          V =/= Ref].

fetch_ddoc_ids_for_mod(Mod, BucketId) ->
    Key = case Mod of
              mapreduce_view ->
                  <<"views">>;
              spatial_view ->
                  <<"spatial">>
          end,
    Pairs = full_live_ddocs(BucketId, infinity,
                            fun (DDoc) ->
                                    #doc{id = Id, body = {Fields}} = couch_doc:with_ejson_body(DDoc),
                                    {Id, couch_util:get_value(Key, Fields, {[]})}
                            end),
    [Id || {Id, Views} <- Pairs, Views =/= {[]}].

-spec get_design_doc_signatures(mapreduce_view | spatial_view, bucket_name() | binary()) -> dict:dict().
get_design_doc_signatures(Mod, BucketId) ->
    DesignDocIds = try
                       fetch_ddoc_ids_for_mod(Mod, BucketId)
                   catch
                       exit:{noproc, _} ->
                           []
                   end,

    %% fold over design docs and get the signature
    lists:foldl(
      fun (DDocId, BySig) ->
              {ok, Signature} = couch_set_view:get_group_signature(
                                  Mod, list_to_binary(BucketId),
                                  DDocId),
              dict:append(Signature, DDocId, BySig)
      end, dict:new(), DesignDocIds).
