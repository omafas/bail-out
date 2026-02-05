%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc process responsible for pushing document changes to other nodes
%%

-module(doc_replicator).

-include("ns_common.hrl").
-include("pipes.hrl").

-export([start_link/3]).

start_link(Name, GetNodes, StorageFrontend) ->
    proc_lib:start_link(
      erlang, apply, [fun start_loop/3, [Name, GetNodes, StorageFrontend]]).

start_loop(Name, GetNodes, StorageFrontend) ->
    erlang:register(Name, self()),
    proc_lib:init_ack({ok, self()}),
    LocalStoragePid = replicated_storage:wait_for_startup(),

    %% anytime we disconnect or reconnect, force a replicate event.
    erlang:spawn_link(
      fun () ->
              ok = net_kernel:monitor_nodes(true),
              nodeup_monitoring_loop(LocalStoragePid)
      end),

    %% Explicitly ask all available nodes to send their documents to us
    [{StorageFrontend, N} ! replicate_newnodes_docs ||
        N <- GetNodes()],

    loop(GetNodes, StorageFrontend, []).

loop(GetNodes, StorageFrontend, OldRemoteNodes) ->
    NodesFun =
        fun () ->
            ActualNodes = GetNodes(),
            RemoteNodes =
                case OldRemoteNodes -- ActualNodes of
                    [] ->
                        OldRemoteNodes;
                    EjectedNodes ->
                        ?log_debug("Stopping replication to following nodes: "
                                   "~p", [EjectedNodes]),
                        OldRemoteNodes -- EjectedNodes
                end,
            {ActualNodes, RemoteNodes}
        end,

    NewRemoteNodes =
        receive
            {replicate_change, Id, Doc} ->
                {_ActualNodes, RemoteNodes} = NodesFun(),
                lists:foreach(
                  fun (Node) ->
                          replicate_change_to_node(
                            StorageFrontend, Node, Id, Doc)
                  end, RemoteNodes),
                RemoteNodes;
            {replicate_changes, Docs} ->
                {_ActualNodes, RemoteNodes} = NodesFun(),
                replicate_changes_to_nodes(StorageFrontend, RemoteNodes,
                                           {batch, Docs}),
                RemoteNodes;
            {replicate_newnodes_docs, Producer} ->
                {ActualNodes, RemoteNodes} = NodesFun(),
                case ActualNodes -- RemoteNodes of
                    [] ->
                        ok;
                    NewNodes ->
                        ?log_debug("Replicating all docs to new nodes: ~p",
                                   [NewNodes]),
                        lists:foreach(
                          fun (Node) ->
                                  monitor(process, {StorageFrontend, Node})
                          end, NewNodes),
                        pipes:foreach(
                          Producer,
                          fun (Docs) ->
                              replicate_changes_to_nodes(StorageFrontend,
                                                         NewNodes, Docs)
                          end)
                end,
                ActualNodes;
            {sync_token, From} ->
                {_ActualNodes, RemoteNodes} = NodesFun(),
                ?log_debug("Received sync_token from ~p", [From]),
                gen_server:reply(From, ok),
                RemoteNodes;
            {'$gen_call', From, {sync_to_me, NodesWanted, Timeout}} ->
                {_ActualNodes, RemoteNodes} = NodesFun(),
                ?log_debug("Received sync_to_me with timeout = ~p, nodes = ~p",
                           [Timeout, NodesWanted]),
                proc_lib:spawn_link(
                  fun () ->
                          handle_sync_to_me(From, StorageFrontend, NodesWanted,
                                            Timeout)
                  end),
                RemoteNodes;
            {'DOWN', _Ref, _Type, {Server, RemoteNode}, Error} ->
                {_ActualNodes, RemoteNodes} = NodesFun(),
                ?log_warning("Remote server node ~p process down: ~p",
                             [{Server, RemoteNode}, Error]),
                RemoteNodes -- [RemoteNode];
            Msg ->
                ?log_error("Got unexpected message: ~p", [Msg]),
                exit({unexpected_message, Msg})
        end,

    loop(GetNodes, StorageFrontend, NewRemoteNodes).

replicate_changes_to_nodes(StorageFrontend, Nodes, {batch, Docs})
  when is_list(Docs) andalso Docs =/= [] ->
    CompressedBatch = misc:compress(Docs),
    ?log_debug("Sending batch of size ~p to ~p", [size(CompressedBatch), Nodes]),
    lists:foreach(
      fun (Node) ->
          gen_server:cast({StorageFrontend, Node},
                          {replicated_batch, CompressedBatch})
      end, Nodes);
replicate_changes_to_nodes(StorageFrontend, Nodes, Docs) when is_list(Docs) ->
    lists:foreach(
      fun (Node) ->
          lists:foreach(
            fun ({Id, Doc}) ->
                replicate_change_to_node(StorageFrontend, Node, Id, Doc)
            end, Docs)
      end, Nodes).

replicate_change_to_node(StorageFrontend, Node, Id, Doc) ->
    ?log_debug("Sending ~p to ~p", [ns_config_log:tag_user_data(Id), Node]),
    gen_server:cast({StorageFrontend, Node}, {replicated_update, Doc}).

nodeup_monitoring_loop(LocalStoragePid) ->
    receive
        {nodeup, _} ->
            ?log_debug("got nodeup event. Considering ddocs replication"),
            LocalStoragePid ! replicate_newnodes_docs;
        _ ->
            ok
    end,
    nodeup_monitoring_loop(LocalStoragePid).

handle_sync_to_me(From, StorageFrontend, Nodes, Timeout) ->
    Results = async:map(
                fun (Node) ->
                        gen_server:call({StorageFrontend, Node}, sync_token, Timeout)
                end, Nodes),
    case lists:filter(
           fun ({_Node, Result}) ->
                   Result =/= ok
           end, lists:zip(Nodes, Results)) of
        [] ->
            gen_server:reply(From, ok);
        Failed ->
            gen_server:reply(From, {error, Failed})
    end.
