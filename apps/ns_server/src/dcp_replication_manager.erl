%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc serializes starting and killing replicators in dcp_sup
%%      it is guaranteed that this guy won't get stuck even if one of
%%      the replicators is infinitely waiting for the correct response
%%      from ep-engine
%%

-module(dcp_replication_manager).

-behavior(gen_server).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/1,
         get_actual_replications/1,
         set_desired_replications/2,
         get_replicator_pid/2,
         get_connection_count/1,
         set_connection_count/2,
         get_connection_for_partition/2]).

-record(state, {bucket = undefined :: bucket_name(),
                connection_count = undefined :: undefined | pos_integer()}).

start_link(Bucket) ->
    gen_server:start_link({local, server_name(Bucket)}, ?MODULE,
                          Bucket, []).

server_name(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

init(Bucket) ->
    {ok, #state{bucket = Bucket}}.

get_actual_replications(Bucket) ->
    try gen_server:call(server_name(Bucket), get_actual_replications, infinity) of
        RV ->
            RV
    catch exit:{noproc, _} ->
            not_running
    end.

-spec get_replicator_pid(bucket_name(), vbucket_id()) -> pid().
get_replicator_pid(Bucket, Partition) ->
    gen_server:call(server_name(Bucket), {get_replicator_pid, Partition}, infinity).

set_desired_replications(Bucket, DesiredReps) ->
    NeededNodes = [Node || {Node, [_|_]} <- DesiredReps],
    gen_server:call(server_name(Bucket),
                    {manage_replicators, NeededNodes}, infinity),

    ConnectionCount = get_connection_count(Bucket),

    Rs = [{Node, dcp_replicator:setup_replication(Node, Bucket, Partitions,
                                                  ConnectionCount)}
          || {Node, [_|_] = Partitions} <- DesiredReps],
    Bad = [Pair || {_, R} = Pair <- Rs, R =/= ok],

    case Bad of
        [] ->
            ok;
        _ ->
            ?log_error("Failed to setup some replications:~n~p", [Bad]),
            {error, {setup_replications_failed, Bad}}
    end.

-spec get_connection_count(bucket_name()) -> pos_integer().
get_connection_count(Bucket) ->
    gen_server:call(server_name(Bucket), get_connection_count).

-spec set_connection_count(bucket_name(), pos_integer()) -> ok.
set_connection_count(Bucket, ConnectionCount) ->
    gen_server:call(server_name(Bucket),
        {set_connection_count, ConnectionCount}).

-spec get_connection_for_partition(pos_integer() | bucket_name(),
    vbucket_id()) -> pos_integer().
get_connection_for_partition(ConnectionCount, Partition)
  when is_integer(ConnectionCount) ->
    Partition rem ConnectionCount;
get_connection_for_partition(Bucket, Partition) when is_list(Bucket) ->
    get_connection_for_partition(get_connection_count(Bucket), Partition).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p" , [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(Msg, State) ->
    ?rebalance_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call({manage_replicators, NeededNodes}, _From,
            #state{bucket = Bucket,
                   connection_count = NeededConnections} = State) ->
    dcp_sup:manage_replicators(Bucket, NeededNodes, NeededConnections),
    {reply, ok, State};
handle_call(get_actual_replications, _From, #state{bucket = Bucket} = State) ->
    Replicators =
        [{Node, dcp_replicator:get_partitions(Pid)} ||
            {Node, Pid, _, _} <- dcp_sup:get_children(Bucket)],
    Folded = lists:foldl(
               fun({_Node, _Partitions} = This, Acc) ->
                       misc:merge_proplists(
                         fun(_Key, Left, Right) ->
                                 %% These are ordered, they are partition lists
                                 %% from dcp_replicator/dcp_consumer_conn which
                                 %% stores this as an ordset. This means that we
                                 %% can use lists:merge rather than append and
                                 %% sort.
                                 lists:merge(Left, Right)
                         end, [This], Acc)
               end, [], Replicators),
    %% We need to sort our resulting list. We'll use this in
    %% replication_manager:handle_call({change_vbucket_replication,...) which
    %% uses it as an input to misc:ukeymerge. That requires a sorted list...
    {reply, lists:keysort(1, Folded), State};
handle_call({get_replicator_pid, Partition}, _From,
            #state{bucket = Bucket} = State) ->
    ChildrenTail =
        lists:dropwhile(fun ({_, Pid, _, _}) ->
                                not lists:member(Partition,
                                                 dcp_replicator:get_partitions(Pid))
                        end, dcp_sup:get_children(Bucket)),
    RV = case ChildrenTail of
             [{_, Pid, _, _} | _] ->
                 Pid;
             _ ->
                 undefined
         end,
    {reply, RV, State};
handle_call(get_connection_count, _From,
            #state{connection_count = Count} = State) ->
    {reply, Count, State};
handle_call({set_connection_count, NewCount}, _From, State)
  when is_integer(NewCount) ->
    {reply, ok, State#state{connection_count = NewCount}}.
