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
%% There can be multiple dcp_proxy processes running on a node -
%% dcp_producer_conn or dcp_consumer_conn for various buckets.
%% The traffic monitor tracks status on {Node, Bucket} basis.
%% This monitor's statuses are used to populate kv_monitor - which
%% tracks not only the node's own buckets but other nodes' bucket
%% activity as well (if tracked by a DCP traffic monitor).
%%
%% mref2node ETS table is used to keep track of all the dcp_proxy processes
%% that are actively updating the traffic monitor.
%% First time a dcp_proxy process calls node_alive(), the traffic monitor
%% starts monitoring that dcp_proxy process. On a process DOWN event,
%% the corresponding entry is removed from the Status information.
%%

-module(dcp_traffic_monitor).

-behaviour(health_monitor).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([get_statuses/0,
         can_refresh/1,
         node_alive/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-ifdef(TEST).
-export([common_test_setup/0,
         common_test_teardown/0,
         health_monitor_test_setup/0,
         health_monitor_t/0,
         health_monitor_test_teardown/0]).
-endif.

start_link() ->
    health_monitor:start_link(?MODULE).

init(BaseMonitorState) ->
    ets:new(mref2node, [private, named_table]),
    BaseMonitorState.

handle_call(get_statuses, _From, MonitorState) ->
    #{nodes := Statuses} = MonitorState,
    RV = dict:map(
           fun(_Node, Buckets) ->
                   lists:map(
                     fun({Bucket, LastHeard, _Pids}) ->
                             {Bucket, LastHeard}
                     end, Buckets)
           end, Statuses),
    {reply, RV};

handle_call(Call, From, MonitorState) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, MonitorState]),
    {reply, nack}.

handle_cast({node_alive, Node, BucketInfo}, MonitorState) ->
    #{nodes := Statuses, nodes_wanted := Nodes} = MonitorState,
    case lists:member(Node, Nodes) of
        true ->
            NewStatuses = misc:dict_update(
                            Node,
                            fun (Buckets) ->
                                    update_bucket(Node, Buckets, BucketInfo)
                            end, [], Statuses),
            {noreply, MonitorState#{nodes => NewStatuses}};
        false ->
            ?log_debug("Ignoring unknown node ~p", [Node]),
            noreply
    end;

handle_cast(Cast, MonitorState) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, MonitorState]),
    noreply.

handle_info({'DOWN', MRef, process, Pid, _Reason}, MonitorState) ->
    #{nodes := Statuses} = MonitorState,
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
    {noreply, MonitorState#{nodes => NewStatuses}};

handle_info(Info, MonitorState) ->
    ?log_warning("Unexpected message ~p when in state:~n~p",
                 [Info, MonitorState]),
    noreply.

%% APIs
get_statuses() ->
    gen_server:call(?MODULE, get_statuses).

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

can_refresh(_State) ->
    false.

-ifdef(TEST).
%% See health_monitor.erl for tests common to all monitors that use these
%% functions
common_test_setup() ->
    ok.

health_monitor_test_setup() ->
    common_test_setup().

health_monitor_t() ->
    ?assert(dict:is_empty(get_statuses())),

    %% Test a 'DOWN' message, we must first setup some state in an ets table
    %% via node_alive cast (can't mock ets/BIFs).
    PidToMonitor =
        erlang:spawn(
          fun() ->
                  %% Block in receive to ensure that this process remains
                  %% alive til we've called get_statuses() at least once
                  receive _ ->
                          ok
                  end
          end),

    BucketInfo = {"bucket", erlang:monotonic_time(), PidToMonitor},
    node_alive(node(), BucketInfo),

    %% We should now be tracking something
    ?assertNot(dict:is_empty(get_statuses())),

    %% Kill PidToMonitor to trigger a 'DOWN' message
    misc:terminate_and_wait(PidToMonitor, "reason"),

    %% And we should no longer be tracking anything. We poll rather than
    %% assert this once to avoid relying on the ordering of the DOWN messages
    %% sent when we terminate PidToMonitor.
    ?assert(misc:poll_for_condition(
              fun() ->
                      dict:is_empty(get_statuses())
              end,
              30000, 100)).

common_test_teardown() ->
    ok.

health_monitor_test_teardown() ->
    common_test_teardown().

-endif.
