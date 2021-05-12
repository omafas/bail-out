%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc entry point for document replicators from other nodes. resides
%%      on ns_server nodes, accepts pushed document changes from document
%%      replicators from other nodes and forwards them to the document
%%      manager that runs on ns_couchdb node
%%

-module(doc_replication_srv).
-include("ns_common.hrl").


-export([start_link/1,
         proxy_server_name/1]).

start_link(Bucket) ->
    ns_bucket_sup:ignore_if_not_couchbase_bucket(
      Bucket,
      fun (_) ->
              proc_lib:start_link(erlang, apply, [fun start_proxy_loop/1, [Bucket]])
      end).

start_proxy_loop(Bucket) ->
    erlang:register(proxy_server_name(Bucket), self()),
    proc_lib:init_ack({ok, self()}),
    DocMgr = replicated_storage:wait_for_startup(),
    proxy_loop(DocMgr).

proxy_loop(DocMgr) ->
    receive
        {'$gen_call', From, SyncMsg} ->
            RV = gen_server:call(DocMgr, SyncMsg, infinity),
            gen_server:reply(From, RV),
            proxy_loop(DocMgr);
        Msg ->
            DocMgr ! Msg,
            proxy_loop(DocMgr)
    end.

proxy_server_name(Bucket) ->
    list_to_atom("capi_ddoc_replication_srv-" ++ Bucket).
