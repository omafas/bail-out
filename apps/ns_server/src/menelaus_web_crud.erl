%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(menelaus_web_crud).

-include("ns_common.hrl").
-include("mc_entry.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_list/2, handle_list/4,
         handle_get/3, handle_get/5,
         handle_post/3, handle_post/5,
         handle_delete/3, handle_delete/5,
         assert_default_collection_uid/1,
         assert_collection_uid/3]).

%% RFC-20 Common flags value used by clients to indicate the
%% data format as JSON.
-define(COMMON_FLAGS_JSON, 16#02000006).

assert_collection_uid(Bucket, Scope, Collection) ->
    case collections:get_collection_uid(Bucket, Scope, Collection) of
        {ok, Uid} ->
            Uid;
        Err ->
            {Msg, Code} = menelaus_web_collections:get_formatted_err_msg(Err),
            menelaus_util:web_json_exception(Code,
                                             {[{error, iolist_to_binary(Msg)}]})
    end.

handle_list(Bucket, Scope, Collection, Req) ->
    menelaus_web_collections:assert_api_available(Bucket),
    handle_list(Bucket, assert_collection_uid(Bucket, Scope, Collection), Req).

get_xattrs_permissions(Bucket, Scope, Collection, Req) ->
    ServerPrivilege = {[{collection, [Bucket, Scope, Collection]},
                        data, sxattr], read},
    ServerPerm = menelaus_auth:has_permission(ServerPrivilege, Req),
    [server_read||ServerPerm].

handle_get(Bucket, Scope, Collection, DocId, Req) ->
    menelaus_web_collections:assert_api_available(Bucket),
    do_handle_get(Bucket, DocId,
                  assert_collection_uid(Bucket, Scope, Collection),
                  get_xattrs_permissions(Bucket, Scope, Collection, Req), Req).

handle_post(Bucket, Scope, Collection, DocId, Req) ->
    menelaus_web_collections:assert_api_available(Bucket),
    handle_post(Bucket, DocId,
                assert_collection_uid(Bucket, Scope, Collection), Req).

handle_delete(Bucket, Scope, Collection, DocId, Req) ->
    menelaus_web_collections:assert_api_available(Bucket),
    handle_delete(Bucket, DocId,
                  assert_collection_uid(Bucket, Scope, Collection), Req).

parse_bool(undefined, Default) -> Default;
parse_bool("true", _) -> true;
parse_bool("false", _) -> false;
parse_bool(_, _) -> throw(bad_request).

parse_int(Name, Params, Default) ->
    case proplists:get_value(Name, Params, Default) of
        Default ->
            Default;
        List ->
            case (catch list_to_integer(List)) of
                Int when is_integer(Int) -> Int;
                E -> {error, E}
            end
    end.

parse_int(Name, Params, Min, Max, Default) ->
    case parse_int(Name, Params, Default) of
        {error, _} = E -> E;
        Int when Int < Min -> {error, too_small};
        Int when Int > Max -> {error, too_large};
        Int -> Int
    end.

parse_key(undefined) -> undefined;
parse_key(Key) ->
    try ejson:decode(Key) of
        Binary when is_binary(Binary) ->
            Binary;
        _ ->
            throw(bad_request)
    catch
        throw:{invalid_json, _} ->
            throw(bad_request)
    end.

parse_params(Params) ->
    %% Get maximum if specified in /internalSettings.
    MaxLimit = ns_config:read_key_fast(max_docs_limit, ?DEFAULT_MAX_DOCS_LIMIT),
    MaxSkip = ns_config:read_key_fast(max_docs_skip, ?DEFAULT_MAX_DOCS_SKIP),

    %% Shouldn't have a default that's greater than the max allowed (e.g.
    %% maxDocsLimit is set to 66 using /internalSettings. The maximum allowed
    %% value for 'limit' should not be larger than 66).
    MaxLimitDefault = min(?DEFAULT_DOCS_LIMIT, MaxLimit),

    Limit = extract_int("limit", Params, ?LOWEST_ALLOWED_MAX_DOCS_LIMIT,
                        MaxLimit, MaxLimitDefault),
    Skip = extract_int("skip", Params, ?LOWEST_ALLOWED_MAX_DOCS_SKIP,
                       MaxSkip, ?DEFAULT_DOCS_SKIP),

    {Skip, Limit,
     [{include_docs, parse_bool(proplists:get_value("include_docs", Params), false)},
      {inclusive_end, parse_bool(proplists:get_value("inclusive_end", Params), true)},
      {limit, Skip + Limit},
      {start_key, parse_key(proplists:get_value("startkey", Params))},
      {end_key, parse_key(proplists:get_value("endkey", Params))}]}.

assert_default_collection_uid(Bucket) ->
    assert_collection_uid(Bucket, "_default", "_default").

handle_list(BucketId, Req) ->
    handle_list(BucketId, assert_default_collection_uid(BucketId), Req).

handle_list(BucketId, CollectionUid, Req) ->
    try parse_params(mochiweb_request:parse_qs(Req)) of
        {Skip, Limit, Params} ->
            do_handle_list(
              Req, BucketId,
              {Skip, Limit, [{collection_uid, CollectionUid} | Params]}, 20)
    catch
        throw:bad_request ->
            menelaus_util:reply_json(Req,
                                     {[{error, <<"bad_request">>},
                                       {reason, <<"bad request">>}]}, 400)
    end.

do_handle_list(Req, _Bucket, _Params, 0) ->
    menelaus_util:reply_json(
      Req,
      {[{error, <<"max_retry">>},
        {reason, <<"could not get consistent vbucket map">>}]}, 503);
do_handle_list(Req, Bucket, {Skip, Limit, Params}, N) ->
    NodeVBuckets = dict:to_list(vbucket_map_mirror:must_node_vbuckets_dict(Bucket)),
    Identity = get_identity(Req),

    case build_keys_heap(Bucket, NodeVBuckets, Params, Identity) of
        {ok, Heap} ->
            Heap1 = handle_skip(Heap, Skip),
            menelaus_util:reply_json(Req,
                                     {[{rows, handle_limit(Heap1, Limit)}]});
        {error, {memcached_error, not_my_vbucket}} ->
            timer:sleep(1000),
            do_handle_list(Req, Bucket, {Skip, Limit, Params}, N - 1);
        {error, {memcached_error, not_supported}} ->
            menelaus_util:reply_json(Req,
                                     {[{error, memcached_error},
                                       {reason, not_supported}]}, 501);
        {error, {memcached_error, Type}} ->
            menelaus_util:reply_json(Req,
                                     {[{error, memcached_error},
                                       {reason, Type}]}, 500);
        {error, Error} ->
            menelaus_util:reply_json(Req,
                                     {[{error, couch_util:to_binary(Error)},
                                       {reason, <<"unknown error">>}]}, 500)
    end.

build_keys_heap(Bucket, NodeVBuckets, Params, Identity) ->
    case ns_memcached:get_keys(Bucket, NodeVBuckets, Params, Identity) of
        {ok, Results} ->
            try lists:foldl(
                  fun ({_Node, R}, Acc) ->
                          case R of
                              {ok, Values} ->
                                  heap_insert(Acc, Values);
                              Error ->
                                  throw({error, Error})
                          end
                  end, couch_skew:new(), Results) of
                Heap ->
                    {ok, Heap}
            catch
                throw:{error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


heap_less([{A, _} | _], [{B, _} | _]) ->
    A < B.

heap_insert(Heap, Item) ->
    case Item of
        [] ->
            Heap;
        _ ->
            couch_skew:in(Item, fun heap_less/2, Heap)
    end.

handle_skip(Heap, 0) ->
    Heap;
handle_skip(Heap, Skip) ->
    case couch_skew:size(Heap) =:= 0 of
        true ->
            Heap;
        false ->
            {[_ | Rest], Heap1} = couch_skew:out(fun heap_less/2, Heap),
            handle_skip(heap_insert(Heap1, Rest), Skip - 1)
    end.

handle_limit(Heap, Limit) ->
    do_handle_limit(Heap, Limit, []).

do_handle_limit(_, 0, R) ->
    lists:reverse(R);
do_handle_limit(Heap, Limit, R) ->
    case couch_skew:size(Heap) =:= 0 of
        true ->
            lists:reverse(R);
        false ->
            {[Min | Rest], Heap1} = couch_skew:out(fun heap_less/2, Heap),
            do_handle_limit(heap_insert(Heap1, Rest), Limit - 1,
                            [encode_doc(Min) | R])
    end.

encode_doc({Key, undefined}) ->
    {[{id, Key}]};
encode_doc({Key, Value}) ->
    Doc = case Value of
              {binary, V} ->
                  {base64, base64:encode(V)};
              {json, V} ->
                  {json, V}
          end,
    {[{id, Key}, {doc, {[Doc]}}]}.

do_get(BucketId, DocId, CollectionUid, Options, Req) ->
    BinaryBucketId = list_to_binary(BucketId),
    BinaryDocId = list_to_binary(DocId),
    Args0 = [X || X <- [BinaryBucketId, BinaryDocId, CollectionUid,
                        [ejson_body | Options]],
                  X =/= undefined],

    Args = Args0 ++ [get_identity(Req)],

    attempt(BinaryBucketId, BinaryDocId, capi_crud, get, Args).

couch_errorjson_to_context(ErrData) ->
    ErrStruct = ejson:decode(ErrData),
    {JsonData} = ErrStruct,
    {Error} = proplists:get_value(<<"error">>, JsonData),
    Context = proplists:get_value(<<"context">>, Error),
    case Context of
        undefined -> throw(invalid_json);
        _ -> Context
    end.

construct_error_reply(Msg) ->
    Reason = try
                 couch_errorjson_to_context(Msg)
             catch
                 _:_ ->
                    ?log_debug("Unknown error format ~p", [Msg]),
                    <<"unknown error">>
             end,
    {[{error, <<"bad_request">>}, {reason, Reason}]}.

construct_etmpfail_error_reply() ->
    {[{error, <<"retry_needed">>},
      {reason, <<"etmpfail returned from memcached">>}]}.

construct_badrpc_error_reply(Reason) ->
    {[{error, <<"badrpc">>},
      {reason, iolist_to_binary(
                 io_lib:format("Remote procedure call failed: ~p",
                               [Reason]))}]}.

construct_form_encoded_required_reply() ->
    {[{error, <<"bad_request">>},
      {reason, <<"Must use 'application/x-www-form-urlencoded'">>}]}.

handle_get(BucketId, DocId, Req) ->
    CollectionUid = assert_default_collection_uid(BucketId),
    XAttrPerm = get_xattrs_permissions(BucketId, "_default", "_default", Req),
    do_handle_get(BucketId, DocId, CollectionUid, XAttrPerm, Req).

do_handle_get(BucketId, DocId, CollectionUid, XAttrPermissions, Req) ->
    case do_get(BucketId, DocId, CollectionUid,
                [{xattrs_perm, XAttrPermissions}], Req) of
        {not_found, missing} ->
            menelaus_util:reply(Req, 404);
        {error, Msg} ->
            menelaus_util:reply_json(Req, construct_error_reply(Msg), 400);
        {retry_needed, etmpfail} ->
            menelaus_util:reply_json(Req,construct_etmpfail_error_reply(), 503);
        {badrpc, Reason} ->
            menelaus_util:reply_json(Req, construct_badrpc_error_reply(Reason),
                                     503);
        {ok, EJSON, {XAttrs}} ->
            {Json} = capi_utils:couch_doc_to_json(EJSON, unparsed),
            ns_audit:read_doc(Req, BucketId, DocId),
            menelaus_util:reply_json(Req, {Json ++ XAttrs});
        %% backward compatibility code: response from node of version < 5.5
        {ok, EJSON} ->
            Res = capi_utils:couch_doc_to_json(EJSON, unparsed),
            menelaus_util:reply_json(Req, Res)
    end.

get_identity(Req) ->
    Identity = menelaus_auth:get_identity(Req),
    true = Identity =/= undefined,
    Identity.

mutate(Req, Oper, BucketId, DocId, CollectionUid, Body, Flags, Expiry,
       PreserveTTL) ->
    BinaryBucketId = list_to_binary(BucketId),
    BinaryDocId = list_to_binary(DocId),

    Args0 = [X || X <- [BinaryBucketId, BinaryDocId, CollectionUid, Body,
                        Flags, Expiry, PreserveTTL], X =/= undefined],

    Args = Args0 ++ [get_identity(Req)],

    case attempt(BinaryBucketId, BinaryDocId, capi_crud, Oper, Args) of
        ok ->
            ns_audit:mutate_doc(Req, Oper, BucketId, DocId),
            ok;
        Other ->
            Other
    end.

extract_flags(Params) ->
    case parse_int("flags", Params, ?COMMON_FLAGS_JSON) of
        {error, _E} ->
            menelaus_util:web_exception(
              400, "'flags' must be a valid positive integer");
        Val ->
            Val
    end.

extract_int(Name, Params, Min, Max, Default) ->
    case parse_int(Name, Params, Min, Max, Default) of
        {error, _E} ->
            menelaus_util:web_exception(
              400,
              io_lib:format("'~p' must be a valid integer between ~p and ~p",
                            [Name, Min, Max]));
        Val ->
            Val
    end.

extract_expiry(Params) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            extract_int("expiry", Params, 0, ?MAX_32BIT_SIGNED_INT,
                        ?NO_EXPIRY);
        false ->
            %% We don't bother giving an error if expiry is specified in a mixed
            %% mode cluster, because we wouldn't give an error on the old nodes,
            %% so we don't gain much by giving the error only on some nodes.
            %% We should use validator to give an error for any unexpected args
            %% when we rewrite this with validator (MB-59023)
            undefined
    end.

extract_preserve_ttl(Params) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            parse_bool(proplists:get_value("preserveTTL", Params), false);
        false ->
            %% We don't bother giving an error if preserveTTL is specified in a
            %% mixed mode cluster, for the same reason as expiry, given above.
            undefined
    end.

handle_post(BucketId, DocId, Req) ->
    handle_post(BucketId, DocId, assert_default_collection_uid(BucketId), Req).

handle_post(BucketId, DocId, CollectionUid, Req) ->
    case mochiweb_request:get_primary_header_value("content-type", Req) of
        "application/x-www-form-urlencoded" ->
            handle_post_inner(BucketId, DocId, CollectionUid, Req);
        undefined ->
            handle_post_inner(BucketId, DocId, CollectionUid, Req);
        _ ->
            menelaus_util:reply_json(Req,
                                     construct_form_encoded_required_reply(),
                                     400)
    end.

handle_post_inner(BucketId, DocId, CollectionUid, Req) ->
    Params = mochiweb_request:parse_post(Req),
    Value = list_to_binary(proplists:get_value("value", Params, [])),
    Flags = extract_flags(Params),
    Expiry = extract_expiry(Params),
    PreserveTTL = extract_preserve_ttl(Params),

    case mutate(Req, set, BucketId, DocId, CollectionUid, Value, Flags,
                Expiry, PreserveTTL) of
        ok ->
            menelaus_util:reply_json(Req, []);
        {retry_needed, etmpfail} ->
            menelaus_util:reply_json(Req, construct_etmpfail_error_reply(),
                                     503);
        {badrpc, Reason} ->
            menelaus_util:reply_json(Req, construct_badrpc_error_reply(Reason),
                                     503);
        {error, Msg} ->
            menelaus_util:reply_json(Req, construct_error_reply(Msg), 400)
    end.

handle_delete(BucketId, DocId, Req) ->
    handle_delete(BucketId, DocId, assert_default_collection_uid(BucketId),
                  Req).

handle_delete(BucketId, DocId, CollectionUid, Req) ->
    case mutate(Req, delete, BucketId, DocId, CollectionUid, undefined,
                undefined, undefined, undefined) of
        ok ->
            menelaus_util:reply_json(Req, []);
        {retry_needed, etmpfail} ->
            menelaus_util:reply_json(Req, construct_etmpfail_error_reply(),
                                     503);
        {badrpc, Reason} ->
            menelaus_util:reply_json(Req, construct_badrpc_error_reply(Reason),
                                     503);
        {error, Msg} ->
            menelaus_util:reply_json(Req, construct_error_reply(Msg), 400)
    end.


%% Attempt to forward the request to the correct server, first try normal
%% map, then vbucket map, then try all nodes
-spec attempt(binary(), binary(), atom(), atom(), list()) -> any().
attempt(DbName, DocId, Mod, Fun, Args) ->
    attempt(DbName, DocId, Mod, Fun, Args, plain_map).

-spec attempt(binary(), binary(), atom(),
              atom(), list(), list() | plain_map | fast_forward) -> any().
attempt(_DbName, _DocId, _Mod, _Fun, _Args, []) ->
    throw(max_vbucket_retry);

attempt(DbName, DocId, Mod, Fun, Args, [Node | Rest]) ->
    case rpc:call(Node, Mod, Fun, Args) of
        not_my_vbucket ->
            attempt(DbName, DocId, Mod, Fun, Args, Rest);
        Else ->
            Else
    end;

attempt(DbName, DocId, Mod, Fun, Args, plain_map) ->
    {_, Node} = cb_util:vbucket_from_id(binary_to_list(DbName), DocId),
    case rpc:call(Node, Mod, Fun, Args) of
        not_my_vbucket ->
            attempt(DbName, DocId, Mod, Fun, Args, fast_forward);
        Else ->
            Else
    end;

attempt(DbName, DocId, Mod, Fun, Args, fast_forward) ->
    R =
        case cb_util:vbucket_from_id_fastforward(binary_to_list(DbName), DocId) of
            ffmap_not_found ->
                next_attempt;
            {_, Node} ->
                case rpc:call(Node, Mod, Fun, Args) of
                    not_my_vbucket ->
                        next_attempt;
                    Else ->
                        {ok, Else}
                end
        end,

    case R of
        next_attempt ->
            Nodes = case ns_bucket:get_bucket(binary_to_list(DbName)) of
                        {ok, BC} ->
                            ns_bucket:get_servers(BC);
                        not_present ->
                            []
                    end,
            attempt(DbName, DocId, Mod, Fun, Args, Nodes);
        {ok, R1} ->
            R1
    end.

-ifdef(TEST).

-define(FAKE_REQUEST, {mochiweb_request,
                       ['Socket', 'Opts', 'Method', 'RawPath', 'Version',
                        mochiweb_headers:empty(),
                        #{authn_res =>
                          #authn_res{identity = {"Administrator", local}}}]}).

%% Setup fake RPC and mock the reply to be just JSON-encoded body.
setup() ->
    meck:new(rpc, [passthrough, unstick]),
    meck:new(cb_util, [passthrough]),
    meck:expect(cb_util, vbucket_from_id, fun (_, _) -> {0, 'Node'} end),
    meck:new(menelaus_util, []),
    meck:expect(menelaus_util, reply_json,
                fun (_, Body, _) ->
                        ejson:encode(Body)
                end),
    fake_chronicle_kv:new().

teardown(_) ->
    fake_chronicle_kv:unload(),
    meck:unload(menelaus_util),
    meck:unload(cb_util),
    meck:unload(rpc).

get_badrpc_test__() ->
    meck:expect(rpc, call,
                fun (_Node, _Module, _Function, _Args) ->
                        {badrpc, {'EXIT', {reason}}}
                end),
    ?assertEqual(<<"{\"error\":\"badrpc\",\"reason\":\"Remote "
                   "procedure call failed: {'EXIT',{reason}}\"}">>,
                 do_handle_get("bucket", "docid", 'CollectionUid',
                               'XAttrPermissions', ?FAKE_REQUEST)).

post_badrpc_test__() ->
    meck:expect(rpc, call,
                fun (_Node, _Module, _Function, _Args) ->
                        {badrpc, {'EXIT', {reason}}}
                end),
    ?assertEqual(<<"{\"error\":\"badrpc\",\"reason\":\"Remote "
                   "procedure call failed: {'EXIT',{reason}}\"}">>,
                 handle_post("bucket", "docid", 'CollectionUid',
                             ?FAKE_REQUEST)).

delete_badrpc_test__() ->
    meck:expect(rpc, call,
                fun (_Node, _Module, _Function, _Args) ->
                        {badrpc, {'EXIT', {reason}}}
                end),
    ?assertEqual(<<"{\"error\":\"badrpc\",\"reason\":\"Remote "
                   "procedure call failed: {'EXIT',{reason}}\"}">>,
                 handle_delete("bucket", "docid", 'CollectionUid',
                               ?FAKE_REQUEST)).

all_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [fun get_badrpc_test__/0,
      fun post_badrpc_test__/0,
      fun delete_badrpc_test__/0]}.

-endif.
