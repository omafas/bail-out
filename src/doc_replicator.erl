%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2018 Couchbase, Inc.
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

loop(GetNodes, StorageFrontend, RemoteNodes) ->
    NewRemoteNodes =
        receive
            {replicate_change, Id, Doc} ->
                lists:foreach(
                  fun (Node) ->
                          replicate_change_to_node(
                            StorageFrontend, Node, Id, Doc)
                  end, RemoteNodes),
                RemoteNodes;
            {replicate_newnodes_docs, Producer} ->
                AllNodes = GetNodes(),
                ?log_debug("doing replicate_newnodes_docs"),

                NewNodes = AllNodes -- RemoteNodes,
                case NewNodes of
                    [] ->
                        ok;
                    _ ->
                        lists:foreach(
                          fun (Node) ->
                                  monitor(process, {StorageFrontend, Node})
                          end, NewNodes),
                        pipes:foreach(
                          Producer,
                          fun (Docs) ->
                                  lists:foreach(
                                    fun (Node) ->
                                            replicate_changes_to_node(
                                              StorageFrontend, Node, Docs)
                                    end, NewNodes)
                          end)
                end,
                AllNodes;
            {sync_token, From} ->
                ?log_debug("Received sync_token from ~p", [From]),
                gen_server:reply(From, ok),
                RemoteNodes;
            {'$gen_call', From, {sync_to_me, NodesWanted, Timeout}} ->
                ?log_debug("Received sync_to_me with timeout = ~p, nodes = ~p",
                           [Timeout, NodesWanted]),
                proc_lib:spawn_link(
                  fun () ->
                          handle_sync_to_me(From, StorageFrontend, NodesWanted,
                                            Timeout)
                  end),
                RemoteNodes;
            {'DOWN', _Ref, _Type, {Server, RemoteNode}, Error} ->
                ?log_warning("Remote server node ~p process down: ~p",
                             [{Server, RemoteNode}, Error]),
                RemoteNodes -- [RemoteNode];
            Msg ->
                ?log_error("Got unexpected message: ~p", [Msg]),
                exit({unexpected_message, Msg})
        end,

    loop(GetNodes, StorageFrontend, NewRemoteNodes).

replicate_changes_to_node(StorageFrontend, Node, {batch, Docs})
  when is_list(Docs) andalso Docs =/= [] ->
    CompressedBatch = misc:compress(Docs),
    ?log_debug("Sending batch of size ~p to ~p", [size(CompressedBatch), Node]),
    gen_server:cast({StorageFrontend, Node}, {replicated_batch, CompressedBatch});
replicate_changes_to_node(StorageFrontend, Node, Docs) when is_list(Docs) ->
    lists:foreach(
      fun ({Id, Doc}) ->
              replicate_change_to_node(StorageFrontend, Node, Id, Doc)
      end, Docs).

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
