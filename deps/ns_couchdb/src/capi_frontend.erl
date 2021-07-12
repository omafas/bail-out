%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(capi_frontend).

-compile(nowarn_export_all).
-compile(export_all).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
-include("ns_common.hrl").
-include("mc_entry.hrl").
-include("mc_constants.hrl").

not_implemented(Arg, Rest) ->
    {not_implemented, Arg, Rest}.

do_db_req(#httpd{path_parts=[DbName | _]} = Req, Fun) ->

    request_throttler:request(
      capi,
      fun () ->
              {BucketName, VBucket, UUID} = capi_utils:split_dbname_with_uuid(DbName),
              with_verify_bucket_auth(
                Req, BucketName, UUID,
                fun (Req1, BucketConfig) ->
                        continue_do_db_req(Req1, BucketConfig, BucketName, VBucket, Fun)
                end)
      end,
      fun (Error, Reason) ->
              Retry = integer_to_list(rand:uniform(10)),
              couch_httpd:send_json(Req, 503, [{"Retry-After", Retry}],
                                    {[{<<"error">>, couch_util:to_binary(Error)},
                                      {<<"reason">>, couch_util:to_binary(Reason)}]})
      end).

verify_bucket_uuid(_, _, undefined) ->
    ok;
verify_bucket_uuid(BucketName, Snapshot, MaybeUUID) ->
    case ns_bucket:uuid(BucketName, Snapshot) of
        MaybeUUID ->
            ok;
        _ ->
            erlang:throw({not_found, uuids_dont_match})
    end.

%% The following code splits all api calls into 3 types: xdcr, views and kv
%% xdcr -  needs the meta permission because the APIs reveal no data and overall goxdcr
%%         uses this permission to access memcached
%% views - access to design docs and view definitions and as such doesn't need data
%%         permissions
%% kv    - everything else. "rounded up" to data permissions because it's simple and
%%         people don't directly use APIs like _all_tasks and who cares about the "/"
%%         welcome API anyway?
%%
%% Note: _view_merge and _spatial_merge do not go through this codepath and have their
%% own auth handling in capi_view:handle_with_auth

xdcr_prefixes() ->
    [<<"_pre_replicate">>, <<"_mass_vbopaque_check">>, <<"_commit_for_checkpoint">>].

get_oper_type([<<"_", _B/binary>> = Prefix | RestOfPath]) ->
    case lists:member(Prefix, xdcr_prefixes()) of
        true ->
            xdcr;
        false ->
            get_oper_type_rest(RestOfPath)
    end;
get_oper_type([_ | RestOfPath]) ->
    get_oper_type_rest(RestOfPath).

get_oper_type_rest([<<"_design">> | _]) ->
    views;
get_oper_type_rest(_) ->
    kv.

get_oper('GET') ->
    read;
get_oper(_) ->
    write.

get_required_permission(BucketName, Method, xdcr) ->
    {[{bucket, BucketName}, data, meta], get_oper(Method)};
get_required_permission(BucketName, Method, views) ->
    {[{bucket, BucketName}, views], get_oper(Method)};
get_required_permission(BucketName, Method, kv) ->
    {[{bucket, BucketName}, data, couchdb], get_oper(Method)}.

continue_do_db_req(#httpd{user_ctx=UserCtx,
                          path_parts=[_DbName | RestPathParts]} = Req,
                   BucketConfig, BucketName, VBucket, Fun) ->
    case VBucket of
        undefined ->
            case lists:member(ns_node_disco:ns_server_node(),
                              ns_bucket:get_view_nodes(BucketConfig)) of
                true ->
                    %% undefined #db fields indicate bucket database
                    Db = #db{user_ctx = UserCtx, name = BucketName},
                    Fun(Req#httpd{path_parts = [BucketName | RestPathParts]}, Db);
                _ ->
                    send_no_active_vbuckets(Req, [BucketName])
            end;
        _ ->
            RealDbName = <<BucketName/binary, $/, VBucket/binary>>,
            PathParts = [RealDbName | RestPathParts],

            %% note that we don't fake mochi_req here; but it seems
            %% that couchdb doesn't use it in our code path
            Req1 = Req#httpd{path_parts=PathParts},

            %% note: we used to call couch_db_frontend:do_db_req here
            %% which opened vbucket even though none of our code is
            %% actually using that couch db.
            %%
            %% What our code is actually using is RealDbName to figure
            %% out which vbucket we're dealing with. So we just
            %% initialize fake #db instance that only has name filled
            %% in.
            %%
            %% We also set filepath to something distinct from
            %% undefined because a bunch of capi frontend code is
            %% using filepath = undefined as indication that we're
            %% referring to bucket
            Db = #db{name = RealDbName,
                     filepath = this_is_vbucket_marker},
            Fun(Req1, Db)
    end.

find_node_with_vbuckets(BucketBin) ->
    Bucket = erlang:binary_to_list(BucketBin),
    VBucketsDict = vbucket_map_mirror:must_node_vbuckets_dict(Bucket),
    Nodes = dict:fetch_keys(VBucketsDict),
    Len = erlang:length(Nodes),
    case Len of
        0 ->
            undefined;
        _ ->
            lists:nth(rand:uniform(Len), Nodes)
    end.

send_no_active_vbuckets(CouchReq, Bucket0) ->
    Req = CouchReq#httpd.mochi_req,
    Bucket = iolist_to_binary(Bucket0),
    LocalAddr = menelaus_util:local_addr(Req),
    RedirectNode = find_node_with_vbuckets(Bucket),
    Headers =
        case RedirectNode of
            undefined ->
                [];
            _ ->
                Path = erlang:iolist_to_binary(
                         mochiweb_request:get(raw_path, Req)),
                [{"Location",
                  capi_utils:capi_url_bin(RedirectNode, Path, LocalAddr)}]
        end,
    RespJson = {[{error, <<"no_active_vbuckets">>},
                 {reason, <<"Cannot execute view query since the "
                            "node has no active vbuckets">>}]},

    {ok, couch_httpd:send_json(CouchReq, 302, Headers, RespJson)}.

with_verify_bucket_auth(Req, BinBucketName, UUID, Fun) ->
    BucketName = ?b2l(BinBucketName),
    Snapshot = ns_bucket:get_snapshot(BucketName),
    case verify_bucket_auth(Req, BucketName, Snapshot) of
        {allowed, Req1, BucketConfig} ->
            verify_bucket_uuid(BucketName, Snapshot, UUID),
            Fun(Req1, BucketConfig);
        {not_found, _} = NotFound ->
            throw(NotFound);
        auth_failure ->
            throw({unauthorized, <<"password required">>});
        {forbidden, Permission} ->
            Resp = menelaus_web_rbac:forbidden_response([Permission]),
            couch_httpd:send_json(Req, 403, Resp)
    end.

verify_bucket_type_support(views = _OperType, BucketConfig) ->
    ns_bucket:can_have_views(BucketConfig);
verify_bucket_type_support(_OperType, BucketConfig) ->
    ns_bucket:bucket_type(BucketConfig) =:= membase.

verify_bucket_auth(#httpd{method = Method,
                          path_parts = Path,
                          mochi_req = MochiReq} = Req,
                   BucketName, Snapshot) ->
    Type = get_oper_type(Path),
    Permission = get_required_permission(BucketName, Method, Type),

    case ns_bucket:get_bucket(BucketName, Snapshot) of
        not_present ->
            {not_found, missing};
        {ok, BucketConfig} ->
            menelaus_auth:assert_no_meta_headers(MochiReq),
            case menelaus_auth:verify_rest_auth(MochiReq, Permission) of
                {allowed, Headers} ->
                    case verify_bucket_type_support(Type, BucketConfig) of
                        true ->
                            Req1 = Req#httpd{mochi_req=menelaus_auth:apply_headers(MochiReq, Headers)},
                            {allowed, Req1, BucketConfig};
                        _ ->
                            case Type =:= views of
                                true ->
                                    {not_found,
                                     <<"views are supported only on couchbase buckets">>};
                                false ->
                                    {not_found, no_couchbase_bucket_exists}
                            end
                    end;
                {forbidden, _} ->
                    {forbidden, Permission};
                {auth_failure, _} ->
                    auth_failure
            end
    end.

%% This is used by 2.x xdcr checkpointing. It's only supposed to work
%% against vbucket
get_db_info(#db{name = DbName}) ->
    Bucket = case string:tokens(binary_to_list(DbName), [$/]) of
                 [BucketV, _Vb] -> BucketV;
                 _ -> throw(not_found)
             end,
    Info = [{db_name, DbName},
            {instance_start_time, ns_memcached:get_ep_startup_time_for_xdcr(Bucket)}],
    {ok, Info}.

with_master_vbucket(#db{name = DbName}, Fun) ->
    with_master_vbucket(DbName, Fun);
with_master_vbucket(DbName, Fun) ->
    DB = capi_utils:must_open_master_vbucket(DbName),
    try
        Fun(DB)
    after
        couch_db:close(DB)
    end.

update_doc(#db{filepath = undefined, name=Name},
           #doc{id = <<"_design/",_/binary>>} = Doc, _Options) ->
    case capi_ddoc_manager:update_doc(Name, Doc) of
        ok ->
            ok;
        {not_found, _} = Error ->
            throw(Error);
        {invalid_design_doc, _Reason} = Error ->
            throw(Error)
    end;
update_doc(_Db, _Doc, _Options) ->
    throw(not_found).


update_docs(_Db,
            [#doc{id = <<?LOCAL_DOC_PREFIX, _Rest/binary>>}],
            _Options) ->
    %% NOTE: We assume it's remote checkpoint update request. We
    %% pretend that it works but avoid actual db update. See comment
    %% before ensure_full_commit below.
    ok;

update_docs(Db, Docs, Options) ->
    exit(not_implemented(update_docs, [Db, Docs, Options])).

update_docs(Db, Docs, Options, replicated_changes) ->
    Result =
        try
            capi_replication:update_replicated_docs(Db, Docs, Options)
        catch
            throw:unsupported ->
                exit(not_implemented(update_docs,
                                     [Db, Docs, Options, replicated_changes]))
        end,
    Result.


%% NOTE: I'd fail this. But it'll break pre-2.5.1 xdcr
%% checkpointing. So we instead pretend that it worked. And because we
%% don't really want such fake checkpoint to work, we'll intercept
%% checkpoint doc updates and drop them on the floor.
-spec ensure_full_commit(any(), integer()) -> {ok, binary()}.
ensure_full_commit(#db{name = DbName}, _RequiredSeq) ->
    [Bucket, _VBucket] = string:tokens(binary_to_list(DbName), [$/]),
    {ok, ns_memcached:get_ep_startup_time_for_xdcr(Bucket)}.

check_is_admin(_Db) ->
    ok.

handle_changes(ChangesArgs, Req, Db) ->
    exit(not_implemented(handle_changes, [ChangesArgs, Req, Db])).

start_view_compact(DbName, GroupId) ->
    exit(not_implemented(start_view_compact, [DbName, GroupId])).

start_db_compact(Db) ->
    exit(not_implemented(start_db_compact, [Db])).

cleanup_view_index_files(Db) ->
    exit(not_implemented(cleanup_view_index_files, [Db])).

%% TODO: check if it's useful
get_group_info(#db{filepath = undefined} = Db, DesignId) ->
    with_master_vbucket(Db,
                        fun (RealDb) ->
                                couch_view:get_group_info(RealDb, DesignId)
                        end);
get_group_info(Db, DesignId) ->
    couch_view:get_group_info(Db, DesignId).

create_db(DbName, UserCtx) ->
    exit(not_implemented(create_db, [DbName, UserCtx])).

delete_db(DbName, UserCtx) ->
    exit(not_implemented(delete_db, [DbName, UserCtx])).

purge_docs(Db, IdsRevs) ->
    exit(not_implemented(purge_docs, [Db, IdsRevs])).

get_missing_revs(Db, JsonDocIdRevs) ->
    Result =
        try
            capi_replication:get_missing_revs(Db, JsonDocIdRevs)
        catch
            throw:unsupported ->
                exit(not_implemented(get_missing_revs, [Db, JsonDocIdRevs]))
        end,

    Result.

open_doc(#db{filepath = undefined} = Db, <<"_design/",_/binary>> = DocId, Options) ->
    with_master_vbucket(Db,
                        fun (RealDb) ->
                                couch_db:open_doc(RealDb, DocId, Options)
                        end);
%% 2.x xdcr checkpointing seemingly uses for it's checkpoints in
%% _local/ docs.
%%
%% Because we don't allow direct vbucket access we just drop this and
%% pretend that doc doesn't exist
open_doc(_Db, _DocId, _Options) ->
    {not_found, missing}.

task_status_all() ->
    couch_db_frontend:task_status_all().

restart_core_server() ->
    exit(not_implemented(restart_core_server, [])).

increment_update_seq(Db) ->
    exit(not_implemented(increment_update_seq, [Db])).

stats_aggregator_all(Range) ->
    exit(not_implemented(stats_aggregator_all, [Range])).

stats_aggregator_get_json(Key, Range) ->
    exit(not_implemented(stats_aggregator_get_json, [Key, Range])).

stats_aggregator_collect_sample() ->
    exit(not_implemented(stats_aggregator_collect_sample, [])).

%% this is used by couch_httpd_db:db_doc_req. But open_doc actually
%% only allows opening design docs and only for bucket and nothing
%% else.
couch_doc_open(Db, DocId, Options) ->
    case open_doc(Db, DocId, Options) of
        {ok, Doc} ->
            Doc;
        Error ->
            throw(Error)
    end.

%% Grab the first vbucket we can find on this server
-spec first_vbucket(binary()) -> non_neg_integer().
first_vbucket(Bucket) ->
    {ok, Config} = ns_bucket:get_bucket(?b2l(Bucket)),
    Map = proplists:get_value(map, Config, []),
    {ok, Index} = first_vbucket(ns_node_disco:ns_server_node(), Map, 0),
    Index.


-spec first_vbucket(atom(), list(), integer()) ->
                           {ok, integer()} | {error, no_vbucket_found}.
first_vbucket(_Node, [], _Acc) ->
    {error, no_vbucket_found};
first_vbucket(Node, [[Node|_] | _Rest], I) ->
    {ok, I};
first_vbucket(Node, [_First|Rest], I) ->
    first_vbucket(Node, Rest, I + 1).

has_active_vbuckets(Bucket) ->
    {ok, Config} = ns_bucket:get_bucket(?b2l(Bucket)),
    Map = proplists:get_value(map, Config, []),
    first_vbucket(ns_node_disco:ns_server_node(), Map, 0) =/= {error, no_vbucket_found}.

-spec get_version() -> string().
get_version() ->
    Apps = application:loaded_applications(),
    case lists:keysearch(ns_server, 1, Apps) of
        {value, {_, _, Vsn}} -> Vsn;
        false -> "0.0.0"
    end.

-spec welcome_message(binary()) -> [{atom(), binary()}].
welcome_message(WelcomeMessage) ->
    [
     {couchdb, WelcomeMessage},
     {version, list_to_binary(couch_server:get_version())},
     {couchbase, list_to_binary(get_version())}
    ].
