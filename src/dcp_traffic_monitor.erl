%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% dcp_traffic_monitor keeps track of liveliness of dcp_proxy traffic.
%%
%% dcp_proxy periodically calls the dcp_traffic_monitor:node_alive() API
%% as long as the dcp_proxy traffic is alive.
%% There can be mulitiple dcp_proxy processes running on a node -
%% dcp_producer_conn or dcp_consumer_conn for various buckets.
%% The traffic monitor tracks status on {Node, Bucket} basis.
%%
%% mref2node ETS table is used to keep track of all the dcp_proxy processes
%% that are actively updating the traffic monitor.
%% First time a dcp_proxy process calls node_alive(), the traffic monitor
%% starts monitoring that dcp_proxy process. On a process DOWN event,
%% the corresponding entry is removed from the Status information.
%%

-module(dcp_traffic_monitor).

-include("ns_common.hrl").

-export([start_link/0]).
-export([get_nodes/0,
         node_alive/2]).
-export([init/0, handle_call/4, handle_cast/3, handle_info/3]).

start_link() ->
    health_monitor:start_link(?MODULE).

init() ->
    ets:new(mref2node, [private, named_table]),
    health_monitor:common_init(?MODULE).

handle_call(get_nodes, _From, Statuses, _Nodes) ->
    RV = dict:map(
           fun(_Node, Buckets) ->
                   lists:map(
                     fun({Bucket, LastHeard, _Pids}) ->
                             {Bucket, LastHeard}
                     end, Buckets)
           end, Statuses),
    {reply, RV};

handle_call(Call, From, Statuses, _Nodes) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, Statuses]),
    {reply, nack}.

handle_cast({node_alive, Node, BucketInfo}, Statuses, Nodes) ->
    case lists:member(Node, Nodes) of
        true ->
            NewStatuses = misc:dict_update(
                            Node,
                            fun (Buckets) ->
                                    update_bucket(Node, Buckets, BucketInfo)
                            end, [], Statuses),
            {noreply, NewStatuses};
        false ->
            ?log_debug("Ignoring unknown node ~p", [Node]),
            noreply
    end;

handle_cast(Cast, Statuses, _Nodes) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, Statuses]),
    noreply.

handle_info({'DOWN', MRef, process, Pid, _Reason}, Statuses, _Nodes) ->
    [{MRef, {Node, Bucket}}] = ets:lookup(mref2node, MRef),
    ?log_debug("Deleting Node:~p Bucket:~p Pid:~p", [Node, Bucket, Pid]),
    NewStatuses = case dict:find(Node, Statuses) of
                      {ok, Buckets} ->
                          case delete_pid(Buckets, Bucket, Pid) of
                              [] ->
                                  dict:erase(Node, Statuses);
                              NewBuckets ->
                                  dict:store(Node, NewBuckets, Statuses)
                          end;
                      _ ->
                          Statuses
                  end,
    ets:delete(mref2node, MRef),
    {noreply, NewStatuses};
handle_info(Info, Statuses, _Nodes) ->
    ?log_warning("Unexpected message ~p when in state:~n~p", [Info, Statuses]),
    noreply.

%% APIs
get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

node_alive(Node, BucketInfo) ->
    gen_server:cast(?MODULE, {node_alive, Node, BucketInfo}).

%% Internal functions
delete_pid(Buckets, Bucket, Pid) ->
    case lists:keyfind(Bucket, 1, Buckets) of
        false ->
            Buckets;
        {Bucket, LastHeard, Pids} ->
            case lists:delete(Pid, Pids) of
                [] ->
                    lists:keydelete(Bucket, 1, Buckets);
                NewPids ->
                    lists:keyreplace(Bucket, 1, Buckets,
                                     {Bucket, LastHeard, NewPids})
            end
    end.

monitor_process(Node, Bucket, Pid) ->
    MRef = erlang:monitor(process, Pid),
    ets:insert(mref2node, {MRef, {Node, Bucket}}).

update_bucket(Node, Buckets, {Bucket, LastHeard, Pid}) ->
    NewPids =
        case lists:keyfind(Bucket, 1, Buckets) of
            false ->
                ?log_debug("Saw that bucket ~p became alive on node ~p",
                           [Bucket, Node]),
                monitor_process(Node, Bucket, Pid),
                [Pid];
            {Bucket, _, Pids} ->
                case lists:member(Pid, Pids) of
                    false ->
                        monitor_process(Node, Bucket, Pid),
                        [Pid | Pids];
                    true ->
                        Pids
                end
        end,
    lists:keystore(Bucket, 1, Buckets, {Bucket, LastHeard, NewPids}).
