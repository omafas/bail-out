%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(recovery).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_recovery/1,
         get_recovery_map/1, commit_vbucket/2, note_commit_vbucket_done/2,
         is_recovery_complete/1]).

-record(state, {bucket_config :: list(),
                recovery_map :: dict:dict(),
                post_recovery_chains :: dict:dict(),
                apply_map :: array:array(),
                effective_map :: array:array()}).

-spec start_recovery(BucketConfig) ->
                            {ok, RecoveryMap, {Servers, BucketConfig}, #state{}}
                                | not_needed
  when BucketConfig :: list(),
       RecoveryMap :: dict:dict(),
       Servers :: [node()].
start_recovery(BucketConfig) ->
    NumVBuckets = proplists:get_value(num_vbuckets, BucketConfig),
    true = is_integer(NumVBuckets),

    NumReplicas = ns_bucket:num_replicas(BucketConfig),

    OldMap =
        case proplists:get_value(map, BucketConfig) of
            undefined ->
                lists:duplicate(NumVBuckets, lists:duplicate(NumReplicas + 1, undefined));
            V ->
                V
        end,
    Servers = ns_bucket:get_servers(BucketConfig),
    true = (Servers =/= undefined),

    MissingVBuckets = compute_missing_vbuckets(OldMap),

    case MissingVBuckets of
        [] ->
            not_needed;
        _ ->
            RecoveryMap0 = compute_recovery_map(OldMap, Servers, NumVBuckets, MissingVBuckets),
            RecoveryMap = dict:from_list(RecoveryMap0),

            PostRecoveryChains =
                lists:foldl(
                  fun ({Node, VBuckets}, Acc) ->
                          lists:foldl(
                            fun (VBucket, Acc1) ->
                                    Chain = [Node |
                                             lists:duplicate(NumReplicas, undefined)],
                                    dict:store(VBucket, Chain, Acc1)
                            end, Acc, VBuckets)
                  end, dict:new(), RecoveryMap0),

            ApplyMap =
                lists:foldr(
                  fun ({V, [undefined | _]}, Acc) ->
                          [Owner | _] = dict:fetch(V, PostRecoveryChains),
                          %% this is fake chain just to create replica
                          Chain = [undefined, Owner],
                          [Chain | Acc];
                      ({_V, Chain}, Acc) ->
                          [Chain | Acc]
                  end, [], misc:enumerate(OldMap, 0)),

            NewBucketConfig = misc:update_proplist(BucketConfig,
                                                   [{map, ApplyMap}]),

            {ok, RecoveryMap, {Servers, NewBucketConfig},
             #state{bucket_config=BucketConfig,
                    recovery_map=RecoveryMap,
                    post_recovery_chains=PostRecoveryChains,
                    apply_map=array:from_list(ApplyMap),
                    effective_map=array:from_list(OldMap)}}
    end.

-spec get_recovery_map(#state{}) -> dict:dict().
get_recovery_map(#state{recovery_map=RecoveryMap}) ->
    RecoveryMap.

-spec commit_vbucket(vbucket_id(), #state{}) ->
                            {ok, {Servers, BucketConfig}, #state{}}
                                | vbucket_not_found
  when Servers :: [node()],
       BucketConfig :: list().
commit_vbucket(VBucket,
               #state{bucket_config=BucketConfig,
                      post_recovery_chains=Chains,
                      apply_map=ApplyMap} = State) ->
    case dict:find(VBucket, Chains) of
        {ok, Chain} ->
            ApplyMap1 = array:to_list(array:set(VBucket, Chain, ApplyMap)),
            NewBucketConfig = misc:update_proplist(BucketConfig,
                                                   [{map, ApplyMap1}]),
            Servers = [S || S <- Chain, S =/= undefined],
            {ok, {Servers, NewBucketConfig}, State};
        error ->
            vbucket_not_found
    end.

-spec note_commit_vbucket_done(vbucket_id(), #state{}) ->
                                      {ok, VBucketMap, #state{}}
  when VBucketMap :: list().
note_commit_vbucket_done(VBucket,
                         #state{post_recovery_chains=Chains,
                                recovery_map=RecoveryMap,
                                apply_map=ApplyMap,
                                effective_map=EffectiveMap} = State) ->
    [Node | _] = Chain = dict:fetch(VBucket, Chains),
    VBuckets = dict:fetch(Node, RecoveryMap),
    VBuckets1 = lists:delete(VBucket, VBuckets),

    RecoveryMap1 =
        case VBuckets1 of
            [] ->
                dict:erase(Node, RecoveryMap);
            _ ->
                dict:store(Node, VBuckets1, RecoveryMap)
        end,
    Chains1 = dict:erase(VBucket, Chains),
    ApplyMap1 = array:set(VBucket, Chain, ApplyMap),
    EffectiveMap1 = array:set(VBucket, Chain, EffectiveMap),

    {ok, array:to_list(EffectiveMap1),
     State#state{recovery_map=RecoveryMap1,
                 post_recovery_chains=Chains1,
                 apply_map=ApplyMap1,
                 effective_map=EffectiveMap1}}.

-spec is_recovery_complete(#state{}) -> boolean().
is_recovery_complete(#state{post_recovery_chains=Chains}) ->
    dict:size(Chains) =:= 0.


%% internal
compute_recovery_map(OldMap, Servers, NumVBuckets, MissingVBuckets) ->
    {PresentVBucketsCount, NodeToVBucketCountsDict} =
        lists:foldl(
          fun ([undefined | _], Acc) ->
                  Acc;
              ([Node | _], {AccTotal, AccDict}) ->
                  {AccTotal + 1, dict:update(Node, fun (C) -> C + 1 end, AccDict)}
          end,
          {0, dict:from_list([{N, 0} || N <- Servers])}, OldMap),
    NodeToVBucketCounts = lists:keysort(2, dict:to_list(NodeToVBucketCountsDict)),

    true = (NumVBuckets == (length(MissingVBuckets) + PresentVBucketsCount)),

    NodesCount = length(Servers),

    Q = NumVBuckets div NodesCount,
    R = NumVBuckets rem NodesCount,

    do_compute_recovery_map(NodeToVBucketCounts, MissingVBuckets, Q, R).

do_compute_recovery_map([], _, _, _) ->
    [];
do_compute_recovery_map(_, [], _, _) ->
    [];
do_compute_recovery_map([{Node, Count} | Rest], MissingVBuckets, Q, R) ->
    {TargetCount, R1} = case R of
                            0 ->
                                {Q, 0};
                            _ ->
                                {Q + 1, R - 1}
                        end,

    true = (TargetCount > Count),
    {NodeVBuckets, MissingVBuckets1} =
        misc:safe_split(TargetCount - Count, MissingVBuckets),
    [{Node, NodeVBuckets} | do_compute_recovery_map(Rest, MissingVBuckets1, Q, R1)].

compute_missing_vbuckets(Map) ->
    lists:foldr(
      fun ({V, Chain}, Acc) ->
              case Chain of
                  [undefined | _] ->
                      [V | Acc];
                  _ ->
                      Acc
              end
      end, [], misc:enumerate(Map, 0)).


-ifdef(TEST).

-define(NUM_TEST_ATTEMPTS, 200).
-define(MAX_NUM_SERVERS, 50).

compute_recovery_map_test_() ->
    {timeout, 200,
     {inparallel,
      [begin
           NumServers = rand:uniform(?MAX_NUM_SERVERS - 1) + 1,
           NumCopies = rand:uniform(4),

           Title = lists:flatten(
                     io_lib:format("NumServers=~p, NumCopies=~p",
                                   [NumServers, NumCopies])),

           Fun = fun () ->
                         compute_recovery_map_test__(NumServers, NumCopies)
                 end,
           {timeout, 200, {Title, Fun}}
       end || _ <- lists:seq(1, ?NUM_TEST_ATTEMPTS)]}}.

compute_recovery_map_test__(NumServers, NumCopies) ->
    Servers = lists:seq(1, NumServers),
    EmptyMap = lists:duplicate(1024,
                               lists:duplicate(NumCopies, undefined)),
    Map = mb_map:generate_map(EmptyMap, NumCopies - 1, Servers, []),

    FailoverServers = misc:shuffle(tl(Servers)),
    lists:foldl(
      fun (Server, AccMap) ->
              AccMap1 = mb_map:promote_replicas(AccMap, [Server]),
              MissingVBuckets = compute_missing_vbuckets(AccMap1),

              RecoveryMap = compute_recovery_map(AccMap1, Servers,
                                                 1024, MissingVBuckets),
              RecoveryMapVBuckets =
                  lists:flatten([Vs || {_, Vs} <- RecoveryMap]),

              ?assertEqual(lists:sort(MissingVBuckets),
                           lists:sort(RecoveryMapVBuckets)),

              AccMap1
      end, Map, FailoverServers).
-endif.
