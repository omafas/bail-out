%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(ns_server_testrunner_api).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-compile(nowarn_export_all).
-compile(export_all).

restart_memcached(Timeout) ->
    {ok, _} = ns_ports_manager:restart_port_by_name(ns_server:get_babysitter_node(), memcached, Timeout).

kill_memcached(Timeout) ->
    try
        {ok, Pid} = ns_ports_manager:send_command(ns_server:get_babysitter_node(), memcached, <<"die!\n">>),
        ok = misc:wait_for_process(Pid, Timeout)
    catch E:T:ST ->
            ?log_error("Got exception in kill_memcached: ~p~n~p", [{E,T}, ST]),
            erlang:raise(E, T, ST)
    end.

eval_string(String) ->
    {value, Value, _} = misc:eval(String, erl_eval:new_bindings()),
    Value.

%% without this API we're forced to rpc call into erlang:apply and
%% pass erl_eval-wrapped function literals which doesn't work across
%% different erlang versions
eval_string_multi(String, Nodes, Timeout) ->
    rpc:call(Nodes, ns_server_testrunner_api, eval_string, String, Timeout).

get_active_vbuckets(Bucket) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    VBucketMap = couch_util:get_value(map, BucketConfig, []),
    Node = node(),
    {json, [Ordinal-1 ||
               {Ordinal, VBuckets} <- misc:enumerate(VBucketMap),
               hd(VBuckets) =:= Node]}.

process_memcached_error_response({ok, #mc_header{status=Status}, #mc_entry{data=Msg},
                                  _NCB}) ->
    {struct, [{result, error},
              {status, mc_client_binary:map_status(Status)},
              {message, Msg}]};
process_memcached_error_response({Err, _, _, _}) ->
    {struct, [{result, error},
              {status, Err},
              {message, "Unknown error"}]}.

get_default_collection_uid() ->
    case collections:enabled() of
        true -> 0;
        false -> undefined
    end.

add_document(Bucket, VBucket, Key, Value) ->
    add_document(Bucket, VBucket, Key, get_default_collection_uid(), Value).

add_document(Bucket, VBucket, Key, CollectionUid, Value) ->
    {json, case ns_memcached:add(Bucket, Key, CollectionUid, VBucket, Value) of
               {ok, #mc_header{status=?SUCCESS}, _, _} ->
                   {struct, [{result, ok}]};
               Error ->
                   process_memcached_error_response(Error)
           end}.

get_document_replica(Bucket, VBucket, Key) ->
    get_document_replica(Bucket, VBucket, Key, get_default_collection_uid()).

get_document_replica(Bucket, VBucket, Key, CollectionUid) ->
    {json, case ns_memcached:get_from_replica(Bucket, Key, CollectionUid,
                                              VBucket) of
               {ok, #mc_header{status=?SUCCESS}, #mc_entry{data = Data}, _} ->
                   {struct, [{result, ok},
                             {value, Data}]};
               Error ->
                   process_memcached_error_response(Error)
           end}.

grab_all_xdcr_checkpoints(BucketName, Timeout) ->
    Fn = fun () ->
                 {json, {struct, capi_utils:capture_local_master_docs(BucketName, Timeout)}}
         end,
    rpc:call(ns_node_disco:couchdb_node(), erlang, apply, [Fn, []]).

grab_all_goxdcr_checkpoints() ->
    {json, {struct, metakv:iterate_matching(?XDCR_CHECKPOINT_PATTERN)}}.

shutdown_nicely() ->
    ns_babysitter_bootstrap:remote_stop(ns_server:get_babysitter_node()).

master_node() ->
    mb_master:master_node().
