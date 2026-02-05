%% @copyright 2007-2008 Basho Technologies
%% @copyright 2010-2014 Couchbase, Inc.

%% @reference Leslie Lamport (1978). "Time, clocks, and the ordering of events in a distributed system". Communications of the ACM 21 (7): 558-565.

%% @reference Friedemann Mattern (1988). "Virtual Time and Global States of Distributed Systems". Workshop on Parallel and Distributed Algorithms: pp. 215-226

%% @author Justin Sheehy <justin@basho.com>
%% @author Andy Gross <andy@basho.com>

%% @doc A simple Erlang implementation of vector clocks as inspired by Lamport logical clocks.

%% Copyright 2007-2008 Basho Technologies
%%
%% Use of this software is governed by the Apache License, Version 2.0,
%% included in the file licenses/APL2.txt.

-module(vclock).

-author('Justin Sheehy <justin@basho.com>').
-author('Andy Gross <andy@basho.com>').

-export([fresh/0, descends/2, merge/1,
         increment/2, increment/3, likely_newer/2,
         get_latest_timestamp/1, count_changes/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% @type vclock() = [vc_entry].
% @type   vc_entry() = {node(), {counter(), timestamp()}}.
% The timestamp is present but not used, in case a client wishes to inspect it.
% @type   node() = term().
% Nodes can have any term() as a name, but they must differ from each other.
% @type   counter() = integer().
% @type   timestamp() = integer().

% @doc Create a brand new vclock.
% @spec fresh() -> vclock()
fresh() ->
    [].

% @doc Reflect an update performed by Node.
%      See increment/2 for usage.
% @spec extend(VC_Entry :: VC_Entry, VClock :: vclock()) -> vclock()
extend({Node,{Ctr,TS}}, VClock) ->
    simplify([{Node,{Ctr,TS}}|VClock]).

% @doc Remove trivial ancestors.
% @spec simplify(vclock()) -> vclock()
simplify(VClock) ->
    simplify(VClock, []).
simplify([], NClock) ->
    NClock;
simplify([{Node,{Ctr1,TS1}}|VClock], NClock) ->
    {Ctr2,TS2} = proplists:get_value(Node, NClock, {Ctr1,TS1}),
    {Ctr,TS} = if
                   Ctr1 > Ctr2 ->
                       {Ctr1,TS1};
                   true ->
                       {Ctr2,TS2}
               end,
    simplify(VClock, [{Node,{Ctr,TS}}|proplists:delete(Node, NClock)]).

% @doc Return true if Va is a direct descendant of Vb, else false -- remember, a vclock is its own descendant!
% @spec descends(Va :: vclock(), Vb :: vclock()) -> bool()
descends(_, []) ->
    % all vclocks descend from the empty vclock
    true;
descends(Va, Vb) ->
    [{NodeB, {CtrB, _T}}|RestB] = Vb,
    CtrA =
        case proplists:get_value(NodeB, Va) of
            undefined ->
                false;
            {CA, _TSA} -> CA
        end,
    case CtrA of
        false -> false;
        _ ->
            if
                CtrA < CtrB ->
                    false;
                true ->
                    descends(Va,RestB)
            end
    end.

%% @doc Applies to case where descends(Va, Vb) is true and
%% descends(Vb, Va) is true. Returns true iff Va has later timestamp
%% then Vb for one of the nodes.
likely_newer(Va, Vb) ->
    lists:any(fun ({Node, {_Counter, TStampA}}) ->
                      case lists:keyfind(Node, 1, Vb) of
                          false -> false;
                          {_, {_CounterB, TStampB}} ->
                              TStampA > TStampB
                      end
              end, Va).

% @doc Combine all VClocks in the input list into their least possible
%      common descendant.
% @spec merge(VClocks :: [vclock()]) -> vclock()
merge(VClocks) ->
    merge(VClocks, []).
merge([], NClock) ->
    NClock;
merge([AClock|VClocks],NClock) ->
    merge(VClocks, lists:foldl(fun(X,L) -> extend(X,L) end, NClock, AClock)).

get_latest_timestamp([]) ->
    0;
get_latest_timestamp([{_Node, {_Ctr, TS}} | Rest]) ->
    RestTS = get_latest_timestamp(Rest),
    case TS < RestTS of
        true ->
            RestTS;
        _ ->
            TS
    end.

increment(Node, VClock) ->
    increment(Node, 0, VClock).

% @doc Increment VClock at Node.
% @spec increment(Node :: node(), VClock :: vclock()) -> vclock()
increment(Node, TS, VClock) ->
    {Ctr, FinalTS} =
        case proplists:get_value(Node, VClock) of
            undefined ->
                {1, TS};
            {C, OldTS} ->
                {C + 1, max(OldTS, TS)}
        end,
    extend({Node, {Ctr, FinalTS}}, VClock).

count_changes_rec([], Acc) -> Acc;
count_changes_rec([{_Node, {Counter, _}} | RestVClock], Acc) ->
    count_changes_rec(RestVClock, Acc + Counter).

count_changes(VClock) ->
    count_changes_rec(VClock, 0).


-ifdef(TEST).
% @doc Serves as both a trivial test and some example code.
example_test() ->
    A = vclock:fresh(),
    B = vclock:fresh(),
    A1 = vclock:increment(a, A),
    B1 = vclock:increment(b, B),
    true = vclock:descends(A1,A),
    true = vclock:descends(B1,B),
    false = vclock:descends(A1,B1),
    A2 = vclock:increment(a, A1),
    C = vclock:merge([A2, B1]),
    C1 = vclock:increment(c, C),
    true = vclock:descends(C1, A2),
    true = vclock:descends(C1, B1),
    false = vclock:descends(B1, C1),
    false = vclock:descends(B1, A1),
    BadClock1 = [{node1, {1, 10}}, {node2, {1, 20}}],
    BadClock2 = [{node1, {1, 12}}, {node2, {1, 20}}],
    true = vclock:descends(BadClock1, BadClock2),
    true = vclock:descends(BadClock2, BadClock1),
    false = vclock:likely_newer(BadClock1, BadClock2),
    true = vclock:likely_newer(BadClock2, BadClock1),
    BadClock3 = [{node1, {1, 12}}, {node2, {1, 21}}],
    true = vclock:descends(BadClock1, BadClock3),
    true = vclock:descends(BadClock3, BadClock1),
    false = vclock:likely_newer(BadClock1, BadClock3),
    true = vclock:likely_newer(BadClock3, BadClock1),
    ok.
-endif.
