 How rebalance works
============

_Note: there is quite a bit of discussion here about views, which was deprecated
in 7.0, but is still included here as the logic is still in the code, so we
should retain that reasoning here until it is completely removed._ 

Orchestrator node (circa 7.6.0)
------------

Among cluster nodes one is elected as orchestrator. See mb_master module for
details. Election is currently quite naive and easily seen as not 100%
bulletproof. Particularly it does not guarantee that _at most 1_ orchestrator
is active at a time. In fact "one at a time" is in many ways a weakly defined
concept in distributed system.

Orchestrator node starts the few services that must be run once, which we call
"singleton services". There is also a "second line of defense" from the
"one at a time" issue, which is that those services are registered in a global
naming facility (see the leader_registry module) which also ensures that there's
only one process registered under a given name in a "cluster" (connected set of
nodes, strictly speaking; i.e. under network partition, every "half" runs its
own singleton services).

For a full list of the singleton services, check the child_specs of
mb_master_sup, ns_orchestrator_sup, and ns_orchestrator_child_sup.

To give context for rebalance, the following are some of the singleton services:

* ns_tick (started by mb_master_sup) - related to stats gathering and is
  irrelevant for the purpose of this discussion.

* auto_failover (started by ns_orchestrator_sup) - keeps track of live-ness of
  all other nodes and decides if and when autofailover should happen.
  Its discussion is also outside of scope of this text.

* ns_orchestrator (started by ns_orchestrator_child_sup) - central coordination
  service. It does:
  * bucket creation
  * bucket deletion
  * "janitoring" (activating vbuckets and spawning/restoring replications to
    match vbucket map, normally after node (re)start of failed rebalance)
  * rebalance (main topic of this text)
  * failover

[//]: # (TODO: Documentation on leases and leader_lease_acquirer - MB-59020)


By performing those activities through ns_orchestrator we also serialize them.

So rebalance starts as a call to ns_orchestrator. Note that because
ns_orchestrator is a globally registered name, any node may call it. This means
that the UI or REST layer of any node may initiate rebalance, failover, or
any other task run the by orchestrator.

As ns_orchestrator is a gen_statem, it can be in one of several states, which
includes idle and rebalancing. This allows the orchestrator to reject requests
when in certain states, for example bucket creation requests during rebalance,
while accepting requests when it is in idle. It can be in many other states,
but those are not covered here.

When a rebalance happens, the orchestration is done in a child process of
ns_orchestrator. So when a rebalance call arrives in idle state, it spawns the
ns_rebalancer process, stores its pid, and switches to the rebalancing state.
The actual work happens in the ns_rebalancer process, and the entry point for
this is ns_rebalancer:init_rebalance/1.


High level outline of rebalance (circa 7.6.0)
---------------------------------

ns_rebalancer:init_rebalance/1 determines a list of nodes to keep, and calls
rebalance/7 with the following arguments:
* nodes to keep
* nodes to eject (rebalance out)
* nodes that are already failed over (which will also be rebalanced out)
* nodes to be delta recovered
* buckets to be delta recovered
* services to rebalance
* desired servers for each bucket

### Minor preparation steps

* Prepare rebalance on all live nodes (nodes to keep and nodes to eject)
  * This is handled by rebalance_agent, which is dedicated to rebalance related
    tasks that are not per bucket
* Drop old secondary indexes on added nodes that aren't being recovered
* Wait for all bucket shutdowns to be done on nodes that we are keeping or
  adding. This is important for to-be-added nodes. In case they're about to
  delete buckets that we're about to recreate, we want them to complete those
  deletions so that if a node is to-be-added, then it starts with empty buckets.
* Prepare delta recovery buckets
* Clear recovery_type and failover_vbuckets config keys for kept nodes
* Service janitor cleanup - creates service maps
* Activate kept nodes with chronicle_master:activity_nodes and
  leader_activities:active_quorum_nodes
* Synchronize config (pull only) from live nodes
* Failed over and rebalanced out nodes are ejected immediately. They don't have
  any vbuckets (because they are failed over) and we don't need to rebalance
  them back in

### KV rebalance

* Bucket rebalance is handled in rebalance_kv/4, which does the following:
 * Updates the desired_servers prop for each bucket, according to the list of
   desired servers passed to rebalance/7.
 * Instructs alive kv nodes to clean up dead files (deleted buckets). We try to
   preserve data files on failover in case a node is delta-recovered. When we
   rebalance a node back in to the cluster, including in full-recovery, these
   files must be cleaned up to ensure that the node is re-built correctly.
 * Iterates over buckets and does per-bucket rebalance in rebalance_bucket/6
   (described below). Only one bucket is rebalanced at a time. It should be
   noted that memcached buckets are actually not rebalanced (because they do not
   support dcp). Instead, for them, we just update list of servers.
   Actual rebalance _only applies for couchbase buckets_.
* If bucket placer is enabled, we instruct alive kv nodes to remove any no
  longer used bucket files

### Rebalance other services

* Rebalances all remaining services in the list of services to
  rebalance

### Minor post-rebalance steps

* Unprepare rebalance on all live nodes (the same set as were prepared at the
  start of the rebalance). If the rebalance failed or was otherwise interrupted,
  this will not be reached

* Eject rebalanced out nodes (excluding the orchestrator, which ejects itself if
  needed in ns_orchestrator)


Couchbase bucket rebalance (circa 7.6.0)
------------------

* Bucket rebalance starts with adding to-be-added kv nodes to the bucket's list
  of servers, using the bucket's desired_servers (which was set to the same
  desired servers list passed to rebalance/7). Because each node has code that
  monitors these lists, to-be-added nodes will spot this change and will create
  local bucket instances with memcached.

* We then wait until all nodes report that the bucket is ready. This happens via
  janitor_agent:check_bucket_ready/3. Notably, we wait on a separate process,
  in janitor_agent:wait_for_memcached/3, which is done in order to anticipate
  any potential stop messages from ns_orchestrator while janitor_agent:
  wait_for_memcached waits.

* We then run ns_janitor:cleanup/2 which ensures that all vbucket states and
  replications match the current vbucket map.

* Further single bucket rebalance happens in ns_rebalancer:
  do_rebalance_membase_bucket/5, which starts with generating the target vbucket
  map and options in generate_vbucket_map/4 (which is its own interesting
  topic), unless we are performing a delta recovery. The target vbucket map is
  stored as the last balanced vbucket map.

* It then spawns an ns_vbucket_mover instance (from run_mover/6) and passes it
  the bucket name, servers list, current map, target map (which it sets as the
  bucket's fastForwardMap), and a progress function to use for tracking vbucket
  move progress. We wait for it to finish with wait_for_mover/1 (anticipating
  stop rebalance message from parent too).
  See ns_vbucket_mover module which is a gen_server.

* Finally, we set the hash of the vbucket map options for the bucket, set the
  bucket's deltaRecoveryMap to undefined, and then verify that vbucket
  replicators were set up correctly with verify_replication/3


ns_vbucket_mover (circa 7.6.0)
------------------

The job of ns_vbucket_mover is to orchestrate the movement of vbuckets. Any
vbucket map row that differs between the current map and the target map is
considered a move. I.e. if the active node for a vBucket is the same, but
replicas differ, we still consider it a move.
It maintains a list of moves that it still needs to do, and tracks currently
running moves.

Actual moves are done in children of this process in the ns_single_vbucket_mover
module. So ns_vbucket_mover only orchestrates overall process while making sure
our limits are respected. One such limit is 4 dcp backfills (dcp backfill is
replication of majority of data) in or out of any node. We also have a max
inflight moves per node (by default 64), but this is enforced by
vbucket_move_scheduler.

ns_vbucket_mover orchestrates the process with the following:

* When initiated, it first reports whether we are performing a swap rebalance,
  and then calls spawn_initial which starts the first set of actions with
  spawn_workers/1:
  * The logic of which move(s) to start out of all possible moves is implemented
    in vbucket_move_scheduler:choose_action/1.
  * It spawns a worker for each action with spawn_worker/2 and stores the action
    assigned to the worker with store_worker/2.
  * If no actions were chosen, it checks if the rebalance of the bucket is done
    and calls janitor_agent:finish_rebalance/3 if it is.

* Once a compaction or backfill is done, or a move completes, it attempts to start
  more actions with spawn_workers/1. If it was a move that completed, it also
  updates the rebalance progress with report_progress/1

* It generally spends most of its life waiting for individual vbucket movers.

* When all vbucket moves are done ns_vbucket_mover exits.

* It also anticipates shutdown request from parent, and it performs it by first
  killing all its children (synchronously) and then exiting.



Moves scheduling rationale (circa 7.6.0)
-----------------------------

All vbuckets are totally independent of each other. This independence also
includes moves, so it's theoretically possible to do all moves concurrently.

However, this is highly inadvisable, because:

* During vbucket movement, data is written through the cache of the destination
  node which will be retained until the bucket high watermark is reached and the
  data has been persisted. This can put additional memory pressure on the
  destination node, particularly if disks are slow.

* Moving vBuckets one by one means that we are more likely to have some moves
  completed should a rebalance be aborted.

* It's more efficient to move vbuckets more or less one by one.
  * First, after vbucket move is done its resources are immediately released on
    the old active, freeing precious memory for subsequent vbucket moves
    (incoming or outgoing).
  * Second, vbuckets are stored separately on disk, so it's more efficient to do
    more or less linear streaming of data vbucket by vbucket rather than trying
    to read and send all vbuckets at once.

In an earlier version we had simpler logic that would limit _outgoing_ moves to
single vbucket per node. I.e. at any time several concurrent vbucket moves are
possible, but they must originate from different nodes. Note that this behavior
heavily penalized one important use-case.
Namely, adding 1 node to a cluster. It can be seen that in that case _all_
cluster members would do one vbucket move into new node, potentially overloading
it considerably.

It should also be noted that not only are the old and new active nodes worth
considering when we think about resource usage, but also the replica nodes.
Future replica nodes are potential targets of incoming DCP traffic. But early
code was not taking that into account.

Once the bulk of data is transferred, a vBucket move potentially spends a large
amount of time persisting data if the disks are slower than our ability to
replicate it. We've found that this phase benefits _a lot_ from actually
allowing multiple vbuckets at a time per node, especially for smaller scale
rebalance under even very small load. This is because data is stored in
per-vbucket files and kv-engine commits vbuckets in round-robin fashion.


### If views are enabled (deprecated in 7.0)
View indexes also benefit from giving them work to do all of the time and "wide"
load (i.e. across several vbuckets) if possible.

The currently implemented logic is to split vbucket move into two phases. One
phase is called "backfill phase", and for that phase _only_ we apply our
"4 at a time move per node" limitation. Once we detect backfill is complete, we
do not count this node against the "4 at a time" limitation anymore.
After a certain number of moves affecting a node it must do a forced view index
compaction.

Picture (drawn by Aaron Miller. Many thanks) helps illustrate the vBucket move
scheduling when views are enabled:

```
          VBucket Move Scheduling
Time
  |   /------------\
  |   | Backfill 0 |                       Backfills cannot happen
  |   \------------/                       concurrently.
  |         |             /------------\
  |   +------------+      | Backfill 1 |
  |   | Index File |      \------------/
  |   |     0      |            |
  |   |            |      +------------+   However, indexing _can_ happen
  |   |            |      | Index File |   concurrently with backfills and
  |   |            |      |     1      |   other indexing.
  |   |            |      |            |
  |   +------------+      |            |
  |         |             |            |
  |         |             +------------+
  |         |                   |
  |         \---------+---------/
  |                   |
  |   /--------------------------------\   Compaction for a set of vbucket moves
  |   |  Compact both source and dest. |   cannot happen concurrently with other
  v   \--------------------------------/   vbucket moves.
```

All that logic is also verbosely explained in the header comment of
vbucket_move_scheduler.erl. Picture is also taken from there. So do read it as
well.


vbucket_move_scheduler (circa 7.6.0)
------------------------

When it's time to start next move this module decides which of many potential
and remaining moves to do first.

It honors our limits:

* only 4 backfills at a time

* and forced view compaction on a node each 64 (configurable via
  rebalanceMovesBeforeCompaction internal setting) moves in-to/out-of it

And within those limits we still frequently have plenty of potential moves to
pick from. So there's simple heuristics that try to do better than just
picking random move out of possible moves.

I'm quoting from vbucket_move_scheduler.erl:

> vbucket moves are picked w.r.t. this 2 constrains and we also have
> heuristics to decide which moves to proceed based on the following
> understanding of goodness:
>
> a) we want to start moving active vbuckets sooner. I.e. prioritize
> moves that change master node and not just replicas. So that
> balance w.r.t. node's load on GETs and SETs is more quickly
> equalized.
>
> b) given that indexer is our bottleneck we want as much as possible
> nodes to do some indexing work all or most of the time

Note that indexer/indexing here refers to view indexes.

We sort the moves according to the following weights, in order (adapted from
comments in vbucket_move_scheduler:choose_action_not_compaction/1):
> 1. Prefer moves which involve node with most moves left.
> We also want to keep the bottleneck nodes involved so that
> we are not stuck with moves to/from the same node(s) at the
> end.
> 2. Prefer moves which will give us the most parallelism.
> Parallelism is achieved by scheduling moves that use
> different connections(i.e., {Src, Dst}) for backfills.
> We penalize moves that will result in multiple backfills on
> same connection. We call this the SerialScore (see below)
> 3. Prefer active moves over replica moves, and prefer moves
> that will help spread the view index building across the
> cluster
> 4. Prefer nodes with least current moves.
> We want to spread the load across the cluster, hence
> penalise moves that are involved in the current moves
> 5. Prefer active moves over replica moves, and prefer
> active moves closer to compaction
> 6. Last resort tie breaker - vBucket id

The SerialScore is defined as follows:

> KV/Data service is limited in term of processing number of
> backfill streams per connection (at the time of writing this
> comment, KV can handle only one backfill stream at a time
> per connection, as they have one thread per connection for
> processing data).
> Therefore, in order to achieve the max amount of parallelism
> we need to schedule vbucket moves in such a fashion that we
> involve separate connection at any given point in time. For
> example, in a 4->4 swap rebalance case, when node3 is
> replaced by node4, we can achieve max data transfer when we
> have concurrent backfilling as below,
> 1. node0 -> node4 (replica move)
> 2. node1 -> node4 (replica move)
> 3. node2 -> node4 (replica move)
> 4. node3 -> node4 (active move)
>
> SerialScore is calculated on the new backfill connections for
> this move, this is where we have maximum data flow.
> If existing backfills use this connection we effectively
> serialize the moves involved.
> We are not only trying to determine the speed with which this
> move will complete but also how this move affects the
> existing moves.

ns_single_vbucket_mover (circa 7.6.0)
-------------------------

This module is responsible for doing single particular vbucket move.

It's entry point is spawn_mover/5 which spawns an erlang process that
orchestrates some particular move.

The process entry point is mover/6. This function calls mover_inner/6, after
which final move chores are performed in on_move_done/6, process cleanup is
performed with misc:sync_shutdown_many_i_am_trapping_exits(get_cleanup_list()),
and the parent process (main vbucket mover process responsible for all moves
orchestration as described above) is notified with
ns_vbucket_mover:note_move_done/2.

## mover_inner/6

If the old active node for the vbucket is undefined, then we just set the
vbucket state to active for the new active node. Otherwise, the move is more
complicated.

If views are enabled then we must disable view compaction on both the old and
new active nodes for the vbucket. Note that calls for this are done via an
intermediate process (spawn_and_wait) to anticipate EXIT message from parent.

Then the new replica nodes are computed, the nodes that need backfill are
determined (currently just the new active node, and only if it wasn't already
the active), replicas are possibly reset (see rebalance_quirks) and replication
streams are set up in set_initial_vbucket_state using
janitor_agent:bulk_set_vbucket_state.

If views are enabled then we wait initiate it on the future active node.

Then we wait until we have indication that backfill is complete. I.e. that all
replica building dcp streams are mostly done sending data to their destinations,
where mostly done means that the estimate of docs left to replicate is less than
the dcp_move_done_limit, which is by default 1000.

Our next step is waiting until data is persisted. We first fetch the max seqno
of the old active vBucket. We then wait until the future active and replicas
mark the max seqno as persisted with the seqno_persistence memcached command.

At this point we signal to the parent process that the backfill phase is done.
This allows ns_vbucket_mover to start rebalancing the next vbucket.

If the old active node is the same as the new active node then our replication
streams are already established, so we finalise the move by setting the dual
topology (discussed further down) and returning from mover_inner/6.

Otherwise, if view indexes are enabled, then we possibly pause view compaction
on the active node. The thinking is that we want to make sure that future active
view indexes are at least not behind view indexes on the old active by the time
we complete the vbucket move.

Then we set the dual topology. This means setting the vbucket state to have
both the old set of nodes and the new. This is for SyncWrites/Durability, and
it ensures that all sync writes made to the old nodes will be persisted to the
new set of nodes during takeover.

Next we stop replication into the future active, and finalize the vbucket move
by running dcp_takeover/7. The DCP takeover is then handled in
replication_manager:dcp_takeover/3 by a process spawned in
janitor_agent:do_handle_call({dcp_takeover, ...), which is discussed in more
detail in the next section. Note that during takeover, KV does not have a valid
topology for a "new" active, so it uses an empty topology, which means that
SyncWrite is temporarily not possible.

Once takeover is complete, we reset the vBucket's topology to a single chain (no
longer an empty topology, so SyncWrite is possible again), set the new active
vbucket's state to active, and cleanup all old streams into the new set of
replicas. Further details are documented above the call to
cleanup_old_streams/4, and above the function itself.

Some of the steps above are disable-able via config flags exposed to internal
settings.

DCP takeover (circa 7.6.0)
---------------------------------

DCP takeover is handled by the replication_manager module which primarily
maintains a list of desired_replications and controls the starting and killing
of these replications.

When the replication_manager receives a dcp_takeover call, it updates its
list of desired_replications and calls dcp_replicator:takeover/3. This tells
dcp_consumer_conn to close the vBucket's existing stream and creates a new
takeover stream. The stream is proxied through ns_server by dcp_proxy, allowing
us to detect when the vbucket is set to active.

Once takeover is complete, kv_engine will send a DCP_SET_VBUCKET_STATE request
to set the vbucket to active. This will be passed to dcp_consumer_conn by
dcp_proxy, and this will reply to dcp_replicator's call to add the takeover
stream. This then allows ns_single_vbucket_mover:mover_inner/6 to continue, as
the takeover is complete.

See [/kv_engine/docs/dcp/documentation/rebalance.md](https://src.couchbase.org/source/xref/trunk/kv_engine/docs/dcp/documentation/rebalance.md?r=7a704b75)
for more detail on KV's perspective of DCP takeover.


Cluster orchestration guts: janitor_agent (circa 7.6.0)
-------------------------------------------

Since 2.0.0 we've separated janitor-and-rebalance to node interaction into its
own dedicated module. This is done in order to more tightly control what is
de-facto API so that code changes are not breaking backwards compat.
But another reason is to properly serialize certain actions. Like janitor
setting vbucket states and replication and rebalance changing them further.

janitor_agent is implemented as a gen-server that is remotely called by
orchestrator node to change replications of that node or vbucket states or
anything else cluster-orchestration and vbucket states related.
