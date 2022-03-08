%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(capi_crud).

-include("ns_common.hrl").
-include("couch_db.hrl").
-include("mc_entry.hrl").
-include("mc_constants.hrl").

-export([get/3, set/3, set/4, delete/2]).
-export([get/4, set/5, delete/3]).
-export([get/5, set/6, delete/4]).

-export([is_valid_json/1]).

construct_error_context(Context) ->
    mochijson2:encode({[{error, [{"context", Context}]}]}).

%% TODO: handle tmp failures here. E.g. during warmup
handle_mutation_rv(#mc_header{status = ?SUCCESS} = _Header, _Entry) ->
    ok;
handle_mutation_rv(#mc_header{status = ?KEY_ENOENT} = _Header, _Entry) ->
    {error, construct_error_context(<<"Key doesn't exist">>)};
handle_mutation_rv(#mc_header{status = ?EINVAL} = _Header, Entry) ->
    {error, Entry#mc_entry.data};
handle_mutation_rv(#mc_header{status = ?UNKNOWN_COLLECTION}, Entry) ->
    {error, Entry#mc_entry.data};
handle_mutation_rv(#mc_header{status = ?NOT_MY_VBUCKET} = _Header, _Entry) ->
    throw(not_my_vbucket).

%% Retaining the old API for backward compatibility.
set(BucketBin, DocId, Value) ->
    set(BucketBin, DocId, Value, 0).

%% For cluster pre ?VERSION_70
set(BucketBin, DocId, Value, Flags) ->
    set(BucketBin, DocId, undefined, Value, Flags).

%% For cluster pre ?VERSION_71
set(BucketBin, DocId, ColletionUid, Value, Flags) ->
    set(BucketBin, DocId, ColletionUid, Value, Flags, undefined).

set(BucketBin, DocId, CollectionUid, Value, Flags, Identity) ->
    Bucket = binary_to_list(BucketBin),
    {VBucket, _} = cb_util:vbucket_from_id(Bucket, DocId),
    {ok, Header, Entry, _} = ns_memcached:set(Bucket, DocId, CollectionUid,
                                              VBucket, Value, Flags,
                                              Identity),
    handle_mutation_rv(Header, Entry).

%% For cluster pre ?VERSION_70
delete(BucketBin, DocId) ->
    delete(BucketBin, DocId, undefined).

%% For cluster pre ?VERSION_71
delete(BucketBin, DocId, CollectionUid) ->
    delete(BucketBin, DocId, CollectionUid, undefined).

delete(BucketBin, DocId, CollectionUid, Identity) ->
    Bucket = binary_to_list(BucketBin),
    {VBucket, _} = cb_util:vbucket_from_id(Bucket, DocId),
    {ok, Header, Entry, _} = ns_memcached:delete(Bucket, DocId, CollectionUid,
                                                 VBucket, Identity),
    handle_mutation_rv(Header, Entry).

%% For cluster pre ?VERSION_70
get(BucketBin, DocId, Options) ->
    get(BucketBin, DocId, undefined, Options).

%% For cluster pre ?VERSION_71
get(BucketBin, DocId, CollectionUid, Options) ->
    get(BucketBin, DocId, CollectionUid, Options, undefined).

get(BucketBin, DocId, CollectionUid, Options, Identity) ->
    Bucket = binary_to_list(BucketBin),
    {VBucket, _} = cb_util:vbucket_from_id(Bucket, DocId),
    get_inner(Bucket, DocId, CollectionUid, VBucket, Options, Identity, 10).

get_inner(_Bucket, _DocId, _CollectionUid, _VBucket, _Options, _Identity, 0) ->
    erlang:error(cas_retries_exceeded);
get_inner(Bucket, DocId, CollectionUid, VBucket, Options, Identity,
          RetriesLeft) ->
    XAttrPermissions = proplists:get_value(xattrs_perm, Options, []),
    {ok, Header, Entry, _} = ns_memcached:get(Bucket, DocId, CollectionUid,
                                              VBucket, Identity),

    case Header#mc_header.status of
        ?SUCCESS ->
            CAS = Entry#mc_entry.cas,
            Value = Entry#mc_entry.data,
            ContentMeta = case is_valid_json(Value) of
                              true -> ?CONTENT_META_JSON;
                              false -> ?CONTENT_META_INVALID_JSON
                          end,
            try
                {ok, Rev, _MetaFlags} = get_meta(Bucket, DocId, CollectionUid,
                                                 VBucket, CAS, Identity),
                {ok, XAttrsJsonObj} = get_xattrs(Bucket, DocId,
                                                 CollectionUid,
                                                 VBucket, CAS,
                                                 XAttrPermissions,
                                                 Identity),
                {ok, #doc{id = DocId, body = Value, rev = Rev,
                          content_meta = ContentMeta},
                 XAttrsJsonObj}

            catch
                _:Reason ->
                    ?log_error("Error during retrieving doc for ~p/~p: ~p",
                               [Bucket, ns_config_log:tag_doc_id(DocId),
                                Reason]),
                    get_inner(Bucket, DocId, CollectionUid,
                              VBucket, Options, Identity, RetriesLeft-1)
            end;
        ?KEY_ENOENT ->
            {not_found, missing};
        ?NOT_MY_VBUCKET ->
            throw(not_my_vbucket);
        ?UNKNOWN_COLLECTION ->
            {error, Entry#mc_entry.data};
        ?EINVAL ->
            {error, Entry#mc_entry.data}
    end.

get_meta(Bucket, DocId, CollectionUid, VBucket, CAS, Identity) ->
    case ns_memcached:get_meta(Bucket, DocId, CollectionUid, VBucket,
                               Identity) of
        {ok, Rev, CAS, MetaFlags} -> {ok, Rev, MetaFlags};
        {ok, _, _, _} -> {error, bad_cas};
        _ -> {error, bad_resp}
    end.

get_xattrs(Bucket, DocId, CollectionUid, VBucket, CAS, Permissions, Identity) ->
    case ns_memcached:get_xattrs(Bucket, DocId, CollectionUid,
                                 VBucket, Permissions, Identity) of
        {ok, CAS, XAttrs} -> {ok, {[{<<"xattrs">>, {XAttrs}}]}};
        {ok, _, _} -> {error, bad_cas};
        Error -> Error
    end.

-spec is_valid_json(Data :: binary()) -> boolean().
is_valid_json(<<>>) ->
    false;
is_valid_json(Data) ->
    %% Docs should accept any JSON value, not just objs and arrays
    %% (this would be anything that is acceptable as a value in an array)
    case ejson:validate([<<"[">>, Data, <<"]">>]) of
        ok -> true;
        _ -> false
    end.
