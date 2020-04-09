%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
-module(menelaus_web_crud).

-include("ns_common.hrl").

-export([handle_list/2,
         handle_get/3,
         handle_post/3,
         handle_delete/3]).

%% RFC-20 Common flags value used by clients to indicate the
%% data format as JSON.
-define(COMMON_FLAGS_JSON, 16#02000006).

parse_bool(undefined, Default) -> Default;
parse_bool("true", _) -> true;
parse_bool("false", _) -> false;
parse_bool(_, _) -> throw(bad_request).

parse_int(undefined, Default) -> Default;
parse_int(List, _) ->
    try list_to_integer(List)
    catch error:badarg ->
            throw(bad_request)
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
    Limit = parse_int(proplists:get_value("limit", Params), 1000),
    Skip = parse_int(proplists:get_value("skip", Params), 0),

    {Skip, Limit,
     [{include_docs, parse_bool(proplists:get_value("include_docs", Params), false)},
      {inclusive_end, parse_bool(proplists:get_value("inclusive_end", Params), true)},
      {limit, Skip + Limit},
      {start_key, parse_key(proplists:get_value("startkey", Params))},
      {end_key, parse_key(proplists:get_value("endkey", Params))}]}.

handle_list(BucketId, Req) ->
    try parse_params(mochiweb_request:parse_qs(Req)) of
        Params ->
            do_handle_list(Req, BucketId, Params, 20)
    catch
        throw:bad_request ->
            menelaus_util:reply_json(Req,
                                     {struct, [{error, <<"bad_request">>},
                                               {reason, <<"bad request">>}]}, 400)
    end.

do_handle_list(Req, _Bucket, _Params, 0) ->
    menelaus_util:reply_json(
      Req,
      {struct, [{error, <<"max_retry">>},
                {reason, <<"could not get consistent vbucket map">>}]}, 503);
do_handle_list(Req, Bucket, {Skip, Limit, Params}, N) ->
    NodeVBuckets = dict:to_list(vbucket_map_mirror:must_node_vbuckets_dict(Bucket)),

    case build_keys_heap(Bucket, NodeVBuckets, Params) of
        {ok, Heap} ->
            Heap1 = handle_skip(Heap, Skip),
            menelaus_util:reply_json(Req,
                                     {struct, [{rows, handle_limit(Heap1, Limit)}]});
        {error, {memcached_error, not_my_vbucket}} ->
            timer:sleep(1000),
            do_handle_list(Req, Bucket, {Skip, Limit, Params}, N - 1);
        {error, {memcached_error, not_supported}} ->
            menelaus_util:reply_json(Req,
                                     {struct, [{error, memcached_error},
                                               {reason, not_supported}]}, 501);
        {error, {memcached_error, Type}} ->
            menelaus_util:reply_json(Req,
                                     {struct, [{error, memcached_error},
                                               {reason, Type}]}, 500);
        {error, Error} ->
            menelaus_util:reply_json(Req,
                                     {struct, [{error, couch_util:to_binary(Error)},
                                               {reason, <<"unknown error">>}]}, 500)
    end.

build_keys_heap(Bucket, NodeVBuckets, Params) ->
    case ns_memcached:get_keys(Bucket, NodeVBuckets, Params) of
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
    {struct, [{id, Key}]};
encode_doc({Key, Value}) ->
    Doc = case Value of
              {binary, V} ->
                  {base64, base64:encode(V)};
              {json, V} ->
                  {json, V}
          end,
    {struct, [{id, Key}, {doc, {struct, [Doc]}}]}.

do_get(BucketId, DocId, Options) ->
    BinaryBucketId = list_to_binary(BucketId),
    BinaryDocId = list_to_binary(DocId),
    attempt(BinaryBucketId,
            BinaryDocId,
            capi_crud, get,
            [BinaryBucketId, BinaryDocId, [ejson_body|Options]]).

couch_errorjson_to_context(ErrData) ->
    ErrStruct = mochijson2:decode(ErrData),
    {struct, JsonData} = ErrStruct,
    {struct, Error} = proplists:get_value(<<"error">>, JsonData),
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
                    "unknown error"
             end,
    {struct, [{error, <<"bad_request">>}, {reason, Reason}]}.

handle_get(BucketId, DocId, Req) ->
    XAttrPermissions = get_xattrs_permissions(BucketId, Req),
    case do_get(BucketId, DocId, [{xattrs_perm, XAttrPermissions}]) of
        {not_found, missing} ->
            menelaus_util:reply(Req, 404);
        {error, Msg} ->
            menelaus_util:reply_json(Req, construct_error_reply(Msg), 400);
        {ok, EJSON, {XAttrs}} ->
            {Json} = capi_utils:couch_doc_to_json(EJSON, unparsed),
            ns_audit:read_doc(Req, BucketId, DocId),
            menelaus_util:reply_json(Req, {Json ++ XAttrs});
        %% backward compatibility code: response from node of version < 5.5
        {ok, EJSON} ->
            Res = capi_utils:couch_doc_to_json(EJSON, unparsed),
            menelaus_util:reply_json(Req, Res)
    end.

get_xattrs_permissions(BucketId, Req) ->
    ServerPrivilege = {[{bucket, BucketId}, data, sxattr], read},
    UserPrivilage = {[{bucket, BucketId}, data, xattr], read},
    ServerPerm = menelaus_auth:has_permission(ServerPrivilege, Req),
    UserPerm = menelaus_auth:has_permission(UserPrivilage, Req),
    [server_read||ServerPerm] ++ [user_read||UserPerm].

mutate(Req, Oper, BucketId, DocId, Body, Flags) ->
    BinaryBucketId = list_to_binary(BucketId),
    BinaryDocId = list_to_binary(DocId),

    Args =
        [X || X <- [BinaryBucketId, BinaryDocId, Body, Flags], X =/= undefined],
    case attempt(BinaryBucketId, BinaryDocId, capi_crud, Oper, Args) of
        ok ->
            ns_audit:mutate_doc(Req, Oper, BucketId, DocId),
            ok;
        Other ->
            Other
    end.

extract_flags(Params) ->
    case proplists:get_value("flags", Params) of
        undefined ->
            ?COMMON_FLAGS_JSON;
        Val ->
            case (catch list_to_integer(Val)) of
                Int when is_integer(Int) andalso Int > 0 ->
                    Int;
                _ ->
                    erlang:throw(
                      {web_exception, 400,
                       <<"'flags' must be a valid positive integer">>,
                       []})
            end
    end.

handle_post(BucketId, DocId, Req) ->
    Params = mochiweb_request:parse_post(Req),
    Value = list_to_binary(proplists:get_value("value", Params, [])),
    Flags = extract_flags(Params),

    case mutate(Req, set, BucketId, DocId, Value, Flags) of
        ok ->
            menelaus_util:reply_json(Req, []);
        {error, Msg} ->
            menelaus_util:reply_json(Req, construct_error_reply(Msg), 400)
    end.

handle_delete(BucketId, DocId, Req) ->
    case mutate(Req, delete, BucketId, DocId, undefined, undefined) of
        ok ->
            menelaus_util:reply_json(Req, []);
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
