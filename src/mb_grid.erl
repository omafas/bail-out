%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc mb_grid makes calls to the cluster distributing the computation.
%% this is a minimal stub that does the work on the calling node.

-module(mb_grid).

-export([aggregate_call/4,
         aggregate_call/5,
         aggregate_call/6,
         abcast/3]).

%% @doc Will broadcast request to named process on nodes
abcast(Nodes, Name, Request) ->
    gen_server:abcast(Nodes, Name, Request).

%% @doc Will make synchronous call to nodes and aggregate.
aggregate_call(Nodes, Module, Request, AFun, Timeout, Accumulator) ->
    {Replies, BadNodes} = gen_server:multi_call(Nodes, Module, Request, Timeout),
    {lists:foldl(AFun, Accumulator, [Reply || {_Node, Reply} <- Replies]), BadNodes}.

%% @doc Will make synchronous call to nodes and aggregate.
aggregate_call(Nodes, Module, Request, AFun, Timeout) ->
    {Replies, BadNodes} = gen_server:multi_call(Nodes, Module, Request, Timeout),
    if  length(Replies) < 2 -> {Replies, BadNodes};
        true -> [First | Rest] = [Reply || {_Node, Reply} <- Replies],
             {lists:foldl(AFun, First, Rest), BadNodes} end.

%% @doc Same as {@link aggregate_call/5} with timeout of infinity.
aggregate_call(Nodes, Module, Request, AFun) ->
    aggregate_call(Nodes, Module, Request, AFun, infinity).
