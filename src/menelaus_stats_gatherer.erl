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

gather_op_stats(FirstNode, Bucket, Nodes, ClientTStamp, Window) ->
    gather_op_stats(FirstNode, Bucket, Nodes, ClientTStamp, Window, all).

gather_op_stats(FirstNode, Bucket, Nodes, ClientTStamp, {_, Period, _} = Window, StatList) ->
    Self = self(),
    Ref = make_ref(),
    Subscription = ns_pubsub:subscribe_link(
                     ns_stats_event,
                     fun (_, done) -> done;
                         ({sample_archived, Name, _}, _)
                           when Name =:= Bucket ->
                             Self ! Ref,
                             done;
                         (_, X) -> X
                     end, []),
    %% don't wait next sample for anything other than real-time stats
    RefToPass = case Period of
                    minute -> Ref;
                    _ -> []
                end,
    try gather_op_stats_body(FirstNode, Bucket, Nodes, ClientTStamp, RefToPass, Window, StatList) of
        Something -> Something
    after
        ns_pubsub:unsubscribe(Subscription),

        misc:flush(Ref)
    end.

gather_op_stats_body(FirstNode, Bucket, Nodes, ClientTStamp,
                     Ref, Window, StatList) ->
    case invoke_archiver(Bucket, FirstNode, Window, StatList) of
        [] -> {FirstNode, [], []};
        [_] -> {FirstNode, [], []};
        RV ->
            OtherNodes = lists:delete(FirstNode, Nodes),

            %% only if we aggregate more than one node
            %% we throw out last sample 'cause it might be missing on other nodes yet
            %% previous samples should be ok on all live nodes
            Samples = case OtherNodes of
                          [] ->
                              lists:reverse(RV);
                          _ ->
                              tl(lists:reverse(RV))
                      end,
            LastTStamp = (hd(Samples))#stat_entry.timestamp,
            case LastTStamp of
                %% wait if we don't yet have fresh sample
                ClientTStamp when Ref =/= [] ->
                    receive
                        Ref ->
                            gather_op_stats_body(FirstNode, Bucket, Nodes, ClientTStamp, [], Window, StatList)
                    after 2000 ->
                            {FirstNode, [], []}
                    end;
                _ ->
                    %% cut samples up-to and including ClientTStamp
                    CutSamples = lists:dropwhile(fun (Sample) ->
                                                         Sample#stat_entry.timestamp =/= ClientTStamp
                                                 end, lists:reverse(Samples)),
                    MainSamples = case CutSamples of
                                      [] -> Samples;
                                      _ -> lists:reverse(CutSamples)
                                  end,

                    Replies = case OtherNodes of
                                  [] ->
                                      [];
                                  _ ->
                                      invoke_archiver(Bucket, OtherNodes, Window, StatList)
                              end,

                    {FirstNode, MainSamples, Replies}
            end
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
              RV = gather_op_stats(node(), Bucket, Nodes, ClientTStamp, Window),
              gen_server:reply(From, RV)
      end),
    {noreply, State};
handle_call({gather_stats, Bucket, Nodes, ClientTStamp, Window, StatList}, From, State) ->
    proc_lib:spawn_link(
      fun () ->
              RV = gather_op_stats(node(), Bucket, Nodes, ClientTStamp, Window, StatList),
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
