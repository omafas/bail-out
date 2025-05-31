#!/bin/sh
#
# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
#
# Use as follows:
#   ./rebuild_vbuckets_on_nodes.sh <username> <password> <host:port> <bucket> <vbucket_node_list>
#   vbucket_node_list is a comma separated list of vbucket:otpName pairs ("0:ns_1@hostname1, 1:ns_1@hostname2")
#
#   Example:
#     ./rebuild_vbuckets_on_nodes.sh Administrator asdasd 127.0.0.1:8091 travel-sample "0:ns_1@172.17.0.2, 1:ns_1@172.17.0.3"
#
#   Two vbucket moves are performed:
#     - First move drops vbucket 0 on node ns_1@172.17.0.2 and vbucket 1 on node ns_1:172.17.0.3
#     - Second move adds back the vbucket 0 on node ns_1@172.17.0.2 and vbucket 1 on node ns_1:172.17.0.3
#       (if it was previously a replica it's added back as a replica, if it was previously a active it's be added back as an active)
#
#   Inaddition to the above, the script:
#     - detects if a vbucket is requested to be rebuilt on the node it doesn't
#       exist on & error out.
#     - detects if a vbucket is requested to be rebuilt on all nodes, it
#       errors out since that would lead to a data loss.

set -e

user=$1
password=$2
host=$3
bucket=$4
vbucket_node_list=$5

curl --fail -X POST -u $user:$password http://$host/diag/eval --data-binary @- <<EOF
Bucket = "${bucket}",
VBucketNodeString = "${vbucket_node_list}",

ParseInput =
  fun(CorruptVbucketNodeString) ->
    L = string:split(CorruptVbucketNodeString, ",", all),
    Trimmed = [string:trim(Item) || Item <- L],
    ParsePair = fun(S) ->
      [VBStr, NodeStr] = string:split(S, ":", all),
      {list_to_integer(VBStr), list_to_atom(NodeStr)}
    end,
    Pairs = [ParsePair(S) || S <- Trimmed],
    GroupPairs =
      fun(Ps) ->
        lists:foldl(
          fun({VB, Node}, Acc) ->
            maps:update_with(
              VB,
              fun(Ns) -> [Node | Ns] end,
              [Node],
              Acc)
          end,
          #{},
          Ps)
      end,
    maps:to_list(GroupPairs(Pairs))
  end,

ValidateVBPresentOnNodes =
  fun(VbucketToNodesList) ->
    {ok, Config} = ns_bucket:get_bucket(Bucket),
    {map, Map} = lists:keyfind(map, 1, Config),
    lists:foldl(
      fun({VB, Nodes}, Acc) ->
        OldChain = lists:nth(VB + 1, Map),
        InvalidNodes = Nodes -- OldChain,
        case InvalidNodes of
          [] -> Acc;
          _ -> [{VB, InvalidNodes} | Acc]
        end
      end, [], VbucketToNodesList)
  end,

ValidateNoDataLoss =
  fun(VbucketToNodesList) ->
    {ok, Config} = ns_bucket:get_bucket(Bucket),
    {map, Map} = lists:keyfind(map, 1, Config),
    lists:foldl(
      fun({VB, Nodes}, Acc) ->
        OldChain = lists:nth(VB + 1, Map),
        NewNodes = OldChain -- Nodes,
        AllUndefined = lists:all(fun(Node) -> Node == 'undefined' end, NewNodes),
        case NewNodes =:= [] orelse AllUndefined of
          true -> [VB | Acc];
          false -> Acc
        end
      end, [], VbucketToNodesList)
  end,

WaitForRebalance =
  fun (Rec) ->
    case rebalance:status() of
      none ->
        ok;
      running ->
        Rec(Rec);
      {none, Error} ->
        {error, Error}
    end
  end,

MoveVBuckets =
  fun (Chains) ->
    ale:info(ns_server, "Ensuring janitor run"),
    true = misc:poll_for_condition(
             fun() ->
                     ok =:= ns_orchestrator:ensure_janitor_run({bucket, Bucket})
             end, 2000, 250),
    ale:info(ns_server, "Moving vbuckets: ~p", [Chains]),
    ok = gen_statem:call({via, leader_registry, ns_orchestrator},
                         {move_vbuckets, Bucket, Chains}),
    ale:info(ns_server, "Vbuckets moved: ~p", [Chains]),
    WaitForRebalance(WaitForRebalance)
  end,

GetVbucketMoves =
  fun (VbucketToNodesList) ->
    {ok, Config} = ns_bucket:get_bucket(Bucket),
    {map, Map} = lists:keyfind(map, 1, Config),
    lists:foldl(
      fun({VB, Nodes}, {OldChainsAcc, NewChainsAcc}) ->
        OldChain = lists:nth(VB + 1, Map),
        OldSize = length(OldChain),
        NewChain = OldChain -- Nodes,
        NewSize = length(NewChain),
        % Keep the number of nodes in the chain the same; add undefined nodes
        % if necessary at the end of the chain
        NewChain1 = NewChain ++ ['undefined' || _ <- lists:seq(1, OldSize - NewSize)],
        {[{VB, OldChain} | OldChainsAcc], [{VB, NewChain1} | NewChainsAcc]}
      end, {[], []}, VbucketToNodesList)
  end,

VBucketToNodesList = ParseInput(VBucketNodeString),
InvalidVbuckets = ValidateVBPresentOnNodes(VBucketToNodesList),
DataLossVbuckets = ValidateNoDataLoss(VBucketToNodesList),

case {InvalidVbuckets, DataLossVbuckets} of
  {[], []} ->
    {OldChains, NewChains} = GetVbucketMoves(VBucketToNodesList),
    %% Run the first rebalance to delete the vbuckets
    case MoveVBuckets(NewChains) of
      ok ->
        %% Run the second rebalance to add back the vbuckets only if the first
        %% rebalance succeeded
        case MoveVBuckets(OldChains) of
          ok ->
            lists:flatten("Vbuckets deleted & rebuilt!");
          {error, Error} ->
            lists:flatten(io_lib:format("Successfully deleted corrupted vbuckets. Failed to rebuild vbuckets. Error: ~p.", [Error]))
        end;
      {error, Error} ->
        lists:flatten(io_lib:format("Failed to delete corrupt vbuckets. Error: ~p.", [Error]))
    end;
  {InvalidVbuckets, []} ->
    lists:flatten(io_lib:format("Found ~p invalid vbuckets: ~w.",
                                [length(InvalidVbuckets), InvalidVbuckets]));
  {[], DataLossVbuckets} ->
    lists:flatten(io_lib:format("Found ~p vbuckets with data loss: ~w.",
                                [length(DataLossVbuckets), DataLossVbuckets]));
  {InvalidVbuckets, DataLossVbuckets} ->
    lists:flatten(io_lib:format("Found ~p invalid vbuckets: ~w. Found ~p vbuckets with data loss: ~w.",
                  [length(InvalidVbuckets), InvalidVbuckets, length(DataLossVbuckets), DataLossVbuckets]))
end.
EOF
