#!/bin/sh
#
# @author Couchbase <info@couchbase.com>
# @copyright 2016-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
#
# A script that drops all replicas for a particular vbucket and then recreates
# them from active copy.
#
# Use as follows:
#
#   ./rebuild_replicas.sh <username> <password> <host:rest port> <bucket> <vbucket>
#
set -e

user=$1
password=$2
host=$3
bucket=$4
vbucket=$5
sleep=${6:-10000}

curl --fail -X POST -u $user:$password http://$host/diag/eval --data-binary @- <<EOF
Bucket = "${bucket}",
VBucket = ${vbucket},
Sleep = ${sleep},

GetChainsFromBucket =
  fun () ->
    {ok, Conf} = ns_bucket:get_bucket(Bucket),
    {map, Map} = lists:keyfind(map, 1, Conf),
    OldChain = lists:nth(VBucket+1, Map),
    %% Do not change length of chain according to num_replicas.
    NewChain = [hd(OldChain)] ++ [undefined || _ <- tl(OldChain)],
    {OldChain, NewChain}
  end,

WaitForRebalance =
  fun (Rec) ->
    case ns_orchestrator:rebalance_progress() of
      not_running ->
        ok;
      _ ->
        Rec(Rec)
    end
  end,

SyncConfig =
  fun () ->
    Nodes = ns_node_disco:nodes_wanted(),

    ns_config_rep:pull_and_push(Nodes),
    ns_config_rep:ensure_config_seen_by_nodes(Nodes)
  end,

Rebalance =
  fun (C) ->
    logger:notice("Starting fixup rebalance ~p", [{VBucket, C}]),

    SyncConfig(),
    ok = ns_orchestrator:ensure_janitor_run({bucket, Bucket}),
    ok = gen_fsm:sync_send_event({via, leader_registry, ns_orchestrator},
                                 {move_vbuckets, Bucket, [{VBucket, C}]}),
    logger:notice("Waiting for a fixup rebalance ~p to complete", [{VBucket, C}]),
    WaitForRebalance(WaitForRebalance),
    logger:notice("Fixup rebalance ~p is complete", [{VBucket, C}])
  end,

{OldChain, NoReplicasChain} =
  case ns_config:search({fixup_rebalance, Bucket, VBucket}) of
    {value, Chains} ->
      logger:notice("Found unfinished fixup rebalance for ~p. Chains:~n~p", [VBucket, Chains]),
      Chains;
    false ->
      Chains = GetChainsFromBucket(),
      ns_config:set({fixup_rebalance, Bucket, VBucket}, Chains),
      Chains
  end,

Rebalance(NoReplicasChain),
Rebalance(OldChain),
ns_config:delete({fixup_rebalance, Bucket, VBucket}),

timer:sleep(Sleep).
EOF
