%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Monitor and maintain the vbucket layout of each bucket.
%% There is one of these per bucket.
%%
%% @doc code for calculating fusion uploaders during the rebalance
%%

-module(fusion_uploaders).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-export([build_fast_forward_info/4,
         build_initial/1,
         get_moves/1,
         get_current/1,
         fail_nodes/2]).

%% incremented starting from 1 with each uploader change
%% The purpose of Term is to help
%% s3 to recognize rogue uploaders and ignore them.
-type inc_term() :: pos_integer().
-type uploader() :: {node() | undefined, inc_term()}.
%% fusion uploaders map, one item per vbucket
-type uploaders() :: [uploader()].
-type move() :: same | uploader().
-type moves() :: [move()].
-type fast_forward_info() :: {moves(), uploaders()}.

-export_type([fast_forward_info/0, uploaders/0]).

-spec build_fast_forward_info(bucket_name(), proplists:proplist(),
                              vbucket_map(), vbucket_map()) ->
          undefined | fast_forward_info().
build_fast_forward_info(Bucket, BucketConfig, Map, FastForwardMap) ->
    case ns_bucket:is_fusion(BucketConfig) of
        false ->
            undefined;
        true ->
            Current = ns_bucket:get_fusion_uploaders(Bucket),
            {Moves, Usage} = calculate_moves(Map, FastForwardMap, Current),
            ?rebalance_info(
               "Calculated fusion uploader moves. Usage ~p~nMoves:~n~p",
               [Usage, Moves]),
            {Moves, Current}
end.

-spec build_initial(vbucket_map()) -> uploaders().
build_initial(VBucketMap) ->
    [{N, 1} || [N | _] <- VBucketMap].

-spec get_moves(fast_forward_info()) -> moves().
get_moves({Moves, _}) ->
    Moves.

-spec get_current(fast_forward_info()) -> uploaders().
get_current({_, Current}) ->
    Current.

%% uploader starts uploading from scratch if it is moved to a
%% node that was not filled from s3, so basically to any node that
%% is not a current uploader and is present in old chain
%%
%% we calculate uploader moves doing the best effort to minimize
%% the number of uploaders started from scratch and distribute
%% uploaders evenly between nodes
%%
%% we do calculation in 2 passes, the first one deals with the
%% situations where there's no choice how to place the uploader,
%% the second one tries to place the uploaders on the nodes with
%% lesser usage. This helps to distribute uploders among nodes
%% more evenly.
calculate_moves(Map, FastForwardMap, CurrentUploaders) ->
    FutureUploaders = lists:duplicate(length(Map), undefined),
    calculate_moves(
      lists:zip(lists:zip3(Map, FastForwardMap, CurrentUploaders),
                FutureUploaders)).

calculate_moves(AllZipped) ->
    {AllZipped1, UsageMap1} = process_moves_without_choice(AllZipped),
    {AllZipped2, UsageMap2} = process_moves_with_choice(AllZipped1, UsageMap1),
    {[Move || {_Current, Move} <- AllZipped2], UsageMap2}.

set_uploader(Current = {_, _, {_, Counter}}, Node, Usage, FromScratch) ->
    Usage1 = maps:update_with(Node, _ + 1, 1, Usage),
    Usage2 = case FromScratch of
                 true ->
                     %% this is needed so the rebalance_info above
                     %% will contain the information on how many
                     %% uploaders were started from scratch
                     maps:update_with(from_scratch, _ + 1, 1, Usage1);
                 false ->
                     Usage1
             end,
    {{Current, {Node, Counter + 1}}, Usage2}.

keep_uploader(Current = {_, _, {Node, _}}, Usage) ->
    {{Current, same}, maps:update_with(Node, _ + 1, 1, Usage)}.

select_uploader(Current = {_, _, {Node, _}}, Nodes, Usage, FromScratch) ->
    {_, Winner} = lists:min([{maps:get(N, Usage, 0), N} || N <- Nodes]),
    case Winner of
        Node ->
            keep_uploader(Current, Usage);
        _ ->
            set_uploader(Current, Winner, Usage, FromScratch)
    end.

%% this pass processes all situations when usage doesn't matter
process_moves_without_choice(AllZipped) ->
    lists:mapfoldl(
      fun ({{OldChain, NewChain, {UploaderNode, _Counter}} = Current,
            undefined},
           Usage) ->
              case NewChain -- lists:delete(UploaderNode, OldChain) of
                  [UploaderNode] ->
                      keep_uploader(Current, Usage);
                  [NewS3Replica] ->
                      set_uploader(Current, NewS3Replica, Usage, false);
                  [] ->
                      case NewChain of
                          [Active] ->
                              set_uploader(Current, Active, Usage, true);
                          _ ->
                              {{Current, undefined}, Usage}
                      end;
                  _ ->
                      {{Current, undefined}, Usage}
              end
      end, #{}, AllZipped).

%% this pass processes all situations when usage should be taken into account
process_moves_with_choice(AllZipped, UsageSoFar) ->
    lists:mapfoldl(
      fun ({{OldChain, NewChain, {UploaderNode, _Counter}} = Current,
            undefined},
           Usage) ->
              case NewChain -- lists:delete(UploaderNode, OldChain) of
                  [] ->
                      %% no candidates for uploader found, so we'll
                      %% have to upload from scratch.
                      select_uploader(Current, NewChain, Usage, true);
                  Candidates ->
                      select_uploader(Current, Candidates, Usage, false)
              end;
          (CurrentAndMove, Usage) ->
              %% already selected by pass 1
              {CurrentAndMove, Usage}
      end, UsageSoFar, AllZipped).

-spec fail_nodes(uploaders(), [node()]) -> uploaders().
fail_nodes(Uploaders, FailedNodes) ->
    [case lists:member(N, FailedNodes) of
         true ->
             {undefined, C};
         false ->
             {N, C}
     end || {N, C} <- Uploaders].
