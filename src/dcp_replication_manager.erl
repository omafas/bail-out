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

-export([start_link/1, get_actual_replications/1, set_desired_replications/2,
         get_replicator_pid/2]).

start_link(Bucket) ->
    gen_server:start_link({local, server_name(Bucket)}, ?MODULE,
                          Bucket, []).

server_name(Bucket) ->
    list_to_atom(?MODULE_STRING "-" ++ Bucket).

init(Bucket) ->
    {ok, Bucket}.

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

    Rs = [{Node, dcp_replicator:setup_replication(Node, Bucket, Partitions)}
          || {Node, [_|_] = Partitions} <- DesiredReps],
    Bad = [Pair || {_, R} = Pair <- Rs, R =/= ok],

    case Bad of
        [] ->
            ok;
        _ ->
            ?log_error("Failed to setup some replications:~n~p", [Bad]),
            {error, {setup_replications_failed, Bad}}
    end.

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

handle_call({manage_replicators, NeededNodes}, _From, Bucket) ->
    dcp_sup:manage_replicators(Bucket, NeededNodes),
    {reply, ok, Bucket};
handle_call(get_actual_replications, _From, Bucket) ->
    Reps = lists:sort([{Node, dcp_replicator:get_partitions(Pid)} ||
                          {Node, Pid, _, _} <- dcp_sup:get_children(Bucket)]),
    {reply, Reps, Bucket};
handle_call({get_replicator_pid, Partition}, _From, Bucket) ->
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
    {reply, RV, Bucket}.
