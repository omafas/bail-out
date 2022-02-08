%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc this service is used to wait for sample_archived event on the
%% particular node and then gather stats on this node and maybe on other nodes
%%
-module(menelaus_stats_gatherer).

-behaviour(gen_server).

-export([start_link/0,
         gather_stats/4, gather_stats/5,
         invoke_archiver/3, invoke_archiver/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ns_stats.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

gather_stats(_Bucket, [], _ClientTStamp, _Window) ->
    {none, [], []};
gather_stats(Bucket, Nodes, ClientTStamp, Window) ->
    FirstNode = get_first_node(Nodes),
    gen_server:call({?MODULE, FirstNode},
                    {gather_stats, Bucket, Nodes, ClientTStamp, Window},
                    infinity).

gather_stats(_Bucket, [], _ClientTStamp, _Window, _StatList) ->
    {none, [], []};
gather_stats(Bucket, Nodes, ClientTStamp, Window, StatList) ->
    FirstNode = get_first_node(Nodes),
    gen_server:call({?MODULE, FirstNode},
                    {gather_stats, Bucket, Nodes, ClientTStamp, Window,
                     StatList}, infinity).

gather_op_stats(Bucket, Nodes, ClientTStamp, Window) ->
    gather_op_stats(Bucket, Nodes, ClientTStamp, Window, all).

gather_op_stats(Bucket, Nodes, ClientTStamp, Window, StatList) ->
    %% Immitating old stats system behavior here.
    %% We don't have log archiver anymore, so we can't subscribe to stats events
    case Window of
        {_, minute, _} when ClientTStamp =/= undefined ->
            Now = os:system_time(millisecond),
            %% this is an approximation, not an exact next sample timestamp
            NextSampleTimestamp = ClientTStamp + 1000,
            case NextSampleTimestamp > Now of
                true ->
                    SleepTime = min(NextSampleTimestamp - Now, 2000),
                    timer:sleep(SleepTime);
                false -> ok
            end;
        _ -> ok
    end,
    RV = invoke_archiver(Bucket, Nodes, Window, StatList),
    MaxCommonTS = lists:foldl(
                    fun ({_, []}, Acc) -> Acc;
                        ({_, SL}, Acc) ->
                            min((lists:last(SL))#stat_entry.timestamp, Acc)
                    end, undefined, RV),
    case MaxCommonTS of
        undefined -> {node(), [], []};
        _ ->
            lists:foldl(
              fun ({N, []}, {MainNode, MainSamples, OtherSamples}) ->
                      {MainNode, MainSamples, [{N, []} | OtherSamples]};
                  ({N, SL}, {undefined, _MainSamples, OtherSamples}) ->
                      SL2 = case ClientTStamp of
                                undefined -> SL;
                                _ ->
                                    lists:dropwhile(
                                      fun (E) ->
                                          E#stat_entry.timestamp < ClientTStamp
                                      end, SL)
                            end,
                      SL2Reversed = lists:reverse(SL2),
                      SL3 = lists:dropwhile(
                              fun (E) ->
                                  E#stat_entry.timestamp > MaxCommonTS
                              end, SL2Reversed),
                      {N, SL3, OtherSamples};
                  ({N, SL}, {MainNode, MainSamples, OtherSamples}) ->
                      {MainNode, MainSamples, [{N, SL} | OtherSamples]}
              end, {undefined, undefined, []}, RV)
    end.

invoke_archiver(Bucket, NodeS, Window) ->
    invoke_archiver(Bucket, NodeS, Window, all).
invoke_archiver(Bucket, NodeS, {Step, Period, Count}, StatList) ->
    RV = (catch stats_reader:latest_specific_stats(Period, NodeS, Bucket, Step,
                                                   Count, StatList)),
    case is_list(NodeS) of
        true -> [{K, V} || {K, {ok, V}} <- RV];
        _ ->
            case RV of
                {ok, List} -> List;
                _ -> []
            end
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, {}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call({gather_stats, Bucket, Nodes, ClientTStamp, Window}, From, State) ->
    proc_lib:spawn_link(
      fun () ->
              RV = gather_op_stats(Bucket, Nodes, ClientTStamp, Window),
              gen_server:reply(From, RV)
      end),
    {noreply, State};
handle_call({gather_stats, Bucket, Nodes, ClientTStamp, Window, StatList}, From, State) ->
    proc_lib:spawn_link(
      fun () ->
              RV = gather_op_stats(Bucket, Nodes, ClientTStamp, Window, StatList),
              gen_server:reply(From, RV)
      end),
    {noreply, State};
handle_call(_, _From, State) ->
    {reply, not_supported, State}.

handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

get_first_node(Nodes) ->
    case Nodes of
        [X] ->
            X;
        [FN | _] ->
            case lists:member(node(), Nodes) of
                true ->
                    node();
                _ ->
                    FN
            end
    end.
