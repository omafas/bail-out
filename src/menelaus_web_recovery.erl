%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(menelaus_web_recovery).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3]).

-export([handle_start_recovery/3,
         handle_recovery_status/3,
         handle_stop_recovery/3,
         handle_commit_vbucket/3]).

handle_start_recovery(_PooldId, Bucket, Req) ->
    case ns_orchestrator:start_recovery(Bucket) of
        {ok, UUID, RecoveryMap} ->
            reply_json(Req, build_start_recovery_reply(UUID, RecoveryMap));
        Error ->
            reply_common(Req, Error)
    end.

handle_recovery_status(_PoolId, Bucket, Req) ->
    UUID = proplists:get_value("recovery_uuid", mochiweb_request:parse_qs(Req)),

    case UUID of
        undefined ->
            reply_common(Req, uuid_missing);
        _ ->
            UUIDBinary = list_to_binary(UUID),
            case ns_orchestrator:recovery_map(Bucket, UUIDBinary) of
                {ok, Map} ->
                    reply_json(Req, build_start_recovery_reply(UUIDBinary, Map));
                Error ->
                    reply_common(Req, Error)
            end
    end.

handle_stop_recovery(_PoolId, Bucket, Req) ->
    UUID = proplists:get_value("recovery_uuid", mochiweb_request:parse_qs(Req)),

    Reply =
        case UUID of
            undefined ->
                uuid_missing;
            _ ->
                UUIDBinary = list_to_binary(UUID),
                ns_orchestrator:stop_recovery(Bucket, UUIDBinary)
        end,

    reply_common(Req, Reply).

handle_commit_vbucket(_PoolId, Bucket, Req) ->
    UUID = proplists:get_value("recovery_uuid", mochiweb_request:parse_qs(Req)),
    VBucket = proplists:get_value("vbucket", mochiweb_request:parse_post(Req)),

    Reply =
        case UUID of
            undefined ->
                uuid_missing;
            _ ->
                UUIDBinary = list_to_binary(UUID),
                try list_to_integer(VBucket) of
                    V when is_integer(V) ->
                        ns_orchestrator:commit_vbucket(Bucket, UUIDBinary, V)
                catch
                    error:badarg ->
                        bad_or_missing_vbucket
                end
        end,

    reply_common(Req, Reply).

%% internal
build_start_recovery_reply(UUID, RecoveryMap) ->
    {struct, [{uuid, UUID},
              {code, ok},
              {recoveryMap, build_recovery_map_json(RecoveryMap)}]}.

build_recovery_map_json(RecoveryMap) ->
    dict:fold(
      fun (Node, VBuckets, Acc) ->
              JSON = {struct, [{node, Node},
                               {vbuckets, VBuckets}]},
              [JSON | Acc]
      end, [], RecoveryMap).

reply_common(Req, Reply) ->
    Status = reply_status_code(Reply),
    Code = reply_code(Reply),
    Extra = build_common_reply_extra(Reply),

    JSON = {struct, [{code, Code} | Extra]},
    reply_json(Req, JSON, Status).

build_common_reply_extra(Reply) when is_atom(Reply) ->
    [];
build_common_reply_extra({error, Error}) ->
    build_error_reply_extra(Error).

build_error_reply_extra({failed_nodes, Nodes}) ->
    [{failedNodes, Nodes}];
build_error_reply_extra(OtherError) ->
    [{internalError, iolist_to_binary(io_lib:format("~p", [OtherError]))}].

reply_code(Reply) when is_atom(Reply) ->
    Reply;
reply_code({error, Error}) ->
    error_reply_code(Error).

error_reply_code({failed_nodes, _}) ->
    failed_nodes;
error_reply_code(_) ->
    internal_error.

reply_status_code(ok) ->
    200;
reply_status_code(recovery_completed) ->
    200;
reply_status_code(not_present) ->
    404;
reply_status_code(bad_recovery) ->
    404;
reply_status_code(vbucket_not_found) ->
    404;
reply_status_code(rebalance_running) ->
    503;
reply_status_code(Error) when is_atom(Error) ->
    400;
reply_status_code({error, _}) ->
    500.
