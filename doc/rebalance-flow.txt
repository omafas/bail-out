How rebalance works circa 2.2.0 release
----------------------------------------


Master node
------------

Among cluster nodes one is elected as master. See mb_master module for
details. Election is currently quite naive and easily seen as not 100%
bulletproof. Particularly it does not guarantee that _at most 1_
master is active at a time. In fact "one at a time" is in many ways
weakly defined concept in distributed system.

Master node starts few services that has to be run once. We call them
"singleton services". And there's "second line of defense" from "one
at a time" issue mentioned above that those services are registered in
erlang global naming facility which also ensures that there's only one
process registered under given name in "cluster" (connected set of
nodes, strictly speaking; i.e. under network partition, every "half"
runs it's own singleton services).

Here's list of singleton services (be sure to check
ns-server-hierarchy.txt as well):

* ns_orchestrator

* ns_tick

* auto_failover

ns_tick is related to stats gathering and is irrelevant for the
purpose of this discussion.

auto_failover is the guy who keeps track of live-ness of all other
nodes and decides if and when autofailover should happen. It's
discussion is also outside of scope of this text.

ns_orchestrator is a cental coordination service. It does:

* bucket creation

* bucket deletion

* "janitoring" (activating vbuckets and spawning/restoring
  replications to match vbucket map, normally after node (re)start of
  failed rebalance)

* rebalance (main topic of this text)

* failover

By performing those activities through ns_orchestrator we also
serialize them.

So rebalance starts as call to ns_orchestrator. Note that because
ns_orchestrator is globally registered name, any node may call it. So
UI or REST layer of any node may initiate rebalance or failover or
anything.

ns_orchestrator is actually gen_fsm. What this means is that it can be
in one of several states. One of this states is idle and other is
rebalancing. The idea is we want it to bounce e.g. bucket creation
requests during rebalance, rather than queuing all of them while
rebalance happens.

And when rebalance happens actual rebalance orchestration is done in
child of ns_orchestrator.

So when rebalance call arrives in idle state, it spawns rebalancer
process and remembers it's pid and switches to state
rebalancing. Actual work happens in rebalancer.

See ns_rebalancer:rebalance/3 for code that runs rebalance.




High level outline of rebalance
---------------------------------

ns_rebalancer:rebalance/3 receives list of nodes to keep (or balance
in), list of nodes to rebalance out, and list of nodes that are
already failed over and which will also be rebalanced out. And does
the following in order:

* synchronize config with remote nodes and waiting until all bucket
  deletions on those remote nodes are done. This is important for
  to-be-added nodes. In case they're about to delete buckets that
  we're about to create back, we want them to complete those deletions
  so that if node is to-be-added it starts with empty bucket.

* it starts cleanup of dead files (past bucket) on all nodes. This is
  due to quite tricky and subtle thing. I.e. we're trying to preserve
  data files on failover and rebalance out just in case, but if
  rebalance-back-in happens we actually have to get rid of those files
  somewhere. So this is the place.

* failed over and rebalanced out nodes are ejected immediately. They
  don't have any vbuckets (because the are failed over) and we don't
  need to rebalance-back-in them too.

* it iterates over buckets and does per-bucket rebalance (described
  below). So we only rebalance one bucket at a time. It should be
  noted that memcached buckets are actually not rebalanced (because
  they do not support tap). Instead for them we just update list of
  servers. Actual rebalance _only applies for couchbase buckets_.

* after we're done rebalancing all buckets we carefully eject
  rebalanced out nodes


Bucket rebalance
------------------

Bucket rebalance starts with adding to-be-added nodes to list of
bucket servers. Because each node has code that monitors this lists,
to-be-added nodes will spot this change and will create bucket
instances on their memcacheds.

We then wait until all nodes have bucket ready. This happens via
janitor_agent:query_states. And notably on separate process, which is
done in order to anticipate stop message from ns_orchestrator while
janitor_agent:query_states is doing it's waiting.

We then run ns_janitor:cleanup/2 (which is often called just janitor
because it's main and only entry point). Which ensures that all
vbucket states and replications match current vbucket map.

Further single bucket rebalance happens in ns_rebalancer:rebalance/5
(note different arity compared to rebalance/3). Which starts with
generating target vbucket map (which is it's own interesting
topic). And target vbucket map is immediately set as fast-forward map.

It then spawns ns_vbucket_mover instance and passed it bucket name,
current and target maps. And merely waits for it (anticipating stop
rebalance message from parent too).

See ns_vbucket_mover module which is a gen_server.


ns_vbucket_mover
------------------

Job of ns_vbucket_mover is to orchestrate move of vbuckets that need
to be moved. Any vbucket map row that differs between current map and
target map is considered a move. I.e. if master node for vbucket is
same, but replicas differ, we still consider it a move.

Actual moves are done in childs of this guy in module
ns_single_vbucket_mover. So ns_vbucket_mover only orchestrates overall
process while making sure our limits are respected. One such limit is
1 tap backfill (tap backfill is replication of majority of data) in or
out of any node.

ns_vbucket_mover maintains list of moves that it still need to do and
it also tracks currently running moves. And at any given time it may
want to start some or one of them. The logic of what move(s) to start
out of all possible moves is implemented in vbucket_move_scheduler
(this is new code that appeared in 2.0.1).

Once current vbucket move is complete ns_vbucket_mover asks
vbucket_move_scheduler what is next move to start. And it generally
spends most of its life waiting for individual vbucket movers.

When all vbucket moves are done ns_vbucket_mover exits.

It also anticipates shutdown request from parent and it performs it by
first killing all it's childs (synchronously) and then exiting.


"B-superstar" index and it's "rebalance"
-------------------------------------------

Since 2.0 and 2.0.1 we're not just moving data, but we also
orchestrate index state transitions during vbucket moves. And we also
control index compactions during rebalance. So it's logical to start
describing per-vbucket move actions with higher-level description what
is needed and why.

I believe some description of what happens with indexes is important
to have before we can dig into our index management actions.

At the time of this writing (just before 2.2.0 and afaik this is not
going to change at least until after 3.0.0) our indexing is organized
as follows. Index is sharded in exactly same way as data. I.e. for
every data vbucket there's corresponding index "vbucket" that has all
k-v pairs emitted from it's map functions for documents of it's data
vbucket _and only for documents of it's data vbucket_.

At query time we have to merge index replies from all index
shards. This is done as part of view engine so does not concern us
much.

However early on, when we actually had index file per vbucket we found
that this index merging of many indexes is not very fast. So folks
decided to combine all index shards on any given node into one
combined/merged index file. Which is much faster at query time because
it avoids excessively wide index merges (<result rows count> * O(log
<index partitions count>)).

However during rebalance vbuckets move and regardless of those moves,
index querying must still work. And that requires us to always merge
all index partitions and in a way where each vbucket occurs _exactly
once_ (no more, no less).

In order to fix this issue folks devised quite sophisticated ways to
incrementally add and remove vbuckets into combined "superstar" index.

Basically for all index rows it keeps track of it's vbucket and it
also maintains list of vbuckets that are active, passive and
cleanup. When request arrives only active vbuckets are queried. It
actually filters out index rows that do not belong to active vbuckets
(note that it "breaks" fast couchdb-style reductions). Passive
vbuckets is vbuckets for which index is being built but is not yet
complete. And cleanup vbuckets is vbuckets which are being removed
from index. And basic idea is we can keep quering some set of vbuckets
while another set of vbuckets is being added-to/removed-from index.

Now assume we're moving vbucket 10 from node A to node B. Before move,
10 is part of active vbuckets on A and does not exist in indexes of
B. And as soon as some data of vbucket 10 is available on B (as
replica) it is added to B's index as passive. So while indexing and
data replication of 10 on B continues we continue querying 10 on A and
queries hitting B ignore 10 (because it's passive yet). Once
replication and index of vbucket 10 on B is complete we can "switch"
owner of vbucket 10 in index. Details will be given below but
basically we active-ate 10 on B and we remove 10 on A (which puts it into
cleanup state).

Another complicating aspect is replica indexes. But I'll leave this
subject for another text. See code of capi_set_view_manager and
janitor_agent for some details.

== Effect of vbucket moves on index compaction

Every vbucket move involves indexing all documents from that vbucket
on future master and cleanup of that vbucket on old master.

Both this actions update most btree nodes at least once (well truth is
a bit more complex given that there are many different btrees and on
btree updates are very localized). Which means index files become very
fragmented and require compaction.

Compaction for index and data file is automatic and is performed by
it's own dedicated service. And up to 2.0.1 release that was causing
some massive inefficiency. Imagine index being heavily updated as part
of vbucket move. And then compaction kicks in, hampering update speed.
Also because mutations continue during compaction, final "compacted
file" will be fragmented too.

So we take this process under strict control to both speed it up (by
avoiding massive inefficiency) and to control disk space usage
(otherwise we've seen "race" between index updater and index compactor
to eat tons of disk space).

Basic idea of taking index compaction under control is to forgid
automatic index compaction during vbucket moves (both incoming and
outgoing); allow configurable number of moves to proceed without index
compaction; and then do index compaction pausing moves for it's
duration.

That behavior prevents concurrent massive index updates and
compactions. Lowers frequency of expensive index compactions. And thus
speeds up rebalance.

Note however that because currently we don't control replica index
updates as part of vbucket moves, effect described above might still
happen for replica indexes.


Moves scheduling rationale
-----------------------------

All vbuckets are totally independent from each other. This
independence also includes moves and it's theoretically possible to do
all moves concurrently.

However such naive implementation is highly inadvisable because:

* during vbucket move both source and destination keep it in the
  memory (at least somewhat for value and at this time fully for
  metadata). All vbucket moves done at once could double resource
  usage

* it's safer to move vbuckets more or less one by one. So that if
  rebalance is aborted in the middle we have at least some moves fully
  completed. In "all at once" case there would be a high chance that
  none of vbucket moves are done at the middle of rebalance.

* it's more efficient to move vbuckets more or less one by one. First,
  after vbucket move is done it's resources are immediately released
  on it's old master, freeing precious memory for subsequent vbucket
  moves (incoming or outgoing). Second, vbuckets are stored separately
  on disk, so it's more efficient to do more or less linear streaming
  of data vbucket by vbucket rather than trying to read and send all
  vbuckets at once.

So since early versions we had simple logic that would limit
_outgoing_ moves to single vbucket per node. I.e. at any time several
concurrent vbucket moves are possible, but they must originate from
different nodes.

Note that this behavior heavily penalized one important
use-case. Namely, adding 1 node to a cluster. It can be seen that in
that case _all_ cluster members would do vbucket move into new node,
potentially overloading it considerably.

It should also be noted that not just old master and future master are
nodes that spend resources on vbucket move. Future replica nodes are
potential targets of incoming data traffic. But current code is not
taking that into account.

So our current code counts node as "used by move" if it's either old
master or new master.

Once bulk of data is transferred vbucket move is waiting on persisting
that data and index it. We've found that this phase benefits _a lot_
from actually allowing multiple vbuckets at a time per node.

Especially for smaller scale rebalance under even very small
load. This is because data is stored in per-vbucket files and
ep-engine commits vbuckets in round-robin fashion. So if there's a
minimal updates rate ep-engine will need full or near-full round of
commits for all vbuckets on it's node and each commit is few hundred
milliseconds. So waiting until vbucket under move is commited easily
takes couple minutes and that slows down rebalance in this case a lot.

Indexes also benefit from giving them indexing work to do all the time
and "wide" load (i.e. across several vbuckets) if possible.

So currently implemented logic is to split vbucket move into two
phases. One phase is called "backfill phase", and for that phase
_only_ we apply our "1 at a time move per node" limitation. And once
we detect backfill is complete, we do not count node as being "used"
for the purpose of limiting concurrent moves anymore.

Also as pointed out above, after certain number of moves affecting
node it must do a forced index compaction. And while doing index
compaction on that node(s) we do not allow any moves that touch them.

Picture (drawn by Aaron Miller. Many thanks) helps illustrate that:

%%           VBucket Move Scheduling
%% Time
%%
%%   |   /------------\
%%   |   | Backfill 0 |                       Backfills cannot happen
%%   |   \------------/                       concurrently.
%%   |         |             /------------\
%%   |   +------------+      | Backfill 1 |
%%   |   | Index File |      \------------/
%%   |   |     0      |            |
%%   |   |            |      +------------+   However, indexing _can_ happen
%%   |   |            |      | Index File |   concurrently with backfills and
%%   |   |            |      |     1      |   other indexing.
%%   |   |            |      |            |
%%   |   +------------+      |            |
%%   |         |             |            |
%%   |         |             +------------+
%%   |         |                   |
%%   |         \---------+---------/
%%   |                   |
%%   |   /--------------------------------\   Compaction for a set of vbucket moves
%%   |   |  Compact both source and dest. |   cannot happen concurrently with other
%%   v   \--------------------------------/   vbucket moves.
%%
%%

All that logic is also verbosely explained in header comment of
vbucket_move_scheduler.erl. Picture is also taken from there. So do
read it as well.

Out of many moves possible for any given situation we can pick random
move. Which was done in some past releases. But we can do
better. Particularly, it makes sense to prefer moves that "equalize"
active vbuckets balance faster. Again see vbucket_move_scheduler.erl
for details.

vbucket_move_scheduler
------------------------

When its time to start next move this module decides which of many
potential and remaining moves to do first.

It honors our limits:

* only 1 backfill at a time

* and forced view compaction on a node each 64 (configurable via
  rebalanceMovesBeforeCompaction internal setting) moves in-to/out-of
  it

And within that limits we still frequently have plenty of potential
moves to pick from. So there's simple heuristics that tries to do
better than just picking random move out of possible moves.

I'm quoting from vbucket_move_scheduler.erl:

%% vbucket moves are picked w.r.t. this 2 constrains and we also have
%% heuristics to decide which moves to proceed based on the following
%% understanding of goodness:
%%
%% a) we want to start moving active vbuckets sooner. I.e. prioritize
%% moves that change master node and not just replicas. So that
%% balance w.r.t. node's load on GETs and SETs is more quickly
%% equalized.
%%
%% b) given that indexer is our bottleneck we want as much as possible
%% nodes to do some indexing work all or most of the time

ns_single_vbucket_mover
-------------------------

This module is responsible for doing single particular vbucket move.

It's entry point is spawn_mover/5 which spawns erlang process that
orchestrates some particular move.

Process entry point is mover/6. This function calls mover_inner/7 (or
mover_inner_old_style for 1.8.x backwards compat case), after which
cleanup is performed and parent process is notified (main vbucket
mover process responsible for all moves orchestration as described
above).

mover_inner starts with disabling index compaction on affected
node. Note that calls for that are done via intermediate process
(spawn_and_wait) to anticipate EXIT message from parent.

Then extended vbucket states are computed and set via
janitor_agent:bulk_set_vbucket_state. Extended states are normal
vbucket states plus "index" states. Particularly, future master is
asked to set it's vbucket to passive state in index. So that it starts
indexing vbucket data as soon as possible.

Next step is spawning of replica builder. Replica builder creates
replication stream from old master to future master and all future
replicas. This is where bulk of data is transferred.

We wait until indexing can be initiated and initiate it on future
master. Note that replica index is not controller at all by current
code. It will auto-kick itself but because we do not at this time
control it's progress during rebalance we don't care.

"can be initiated" is a very subtle thing in current code. In code
that's call to wait_backfill_determination. We do that because with
current tap checkpoints implementation there is a possibility that
establishing replication stream will first reset it's destination
messing up our tracking of index update process (it's impossible to
wait for indexing completion for vbucket that doesn't exist which is
intermediate state of that "reset").

Our next action is asking tap checkpoint id on old master. It will be
used later to ensure that we have more or less all data on future
master and replicas.

Then we wait until we have indication that backfill is
complete. I.e. that all replica building tap streams are mostly done
sending data to their destinations.

At this point we signal to parent that backfill phase is done. Note
that the code also has to handle a case where not all nodes are
2.0.1+. In which case there's no way to detect backfill completion and
our code will simply signal that backfill is completed only after next
step.

Note at this time it's very likely that most of that data is not yet
on disk. And thus not yet "visible" for indexer.

So our next step is waiting until data is persisted. We use another
implementation detail of current tap checkpoints-based ep-engine to
wait until future master and replicas mark checkpoint id grabbed
earlier as persisted. There's special undocumented command for that.

In next step we pause indexing on old master. The thinking is that
we want to make sure that future master indexes are at least not
behind indexes on old master by the time we complete vbucket move.

In order to do that after pausing indexing on old master we capture
it's last closed checkpoint id and wait until it's persisted on future
master and then wait until indexer on future master is up to
date. That sequence ensure that there's no data that's in old master's
indexes but is not yet in future master's indexes.

Some of the steps above are disable-able via config flags exposed to
internal settings.

After that we know indexer is ready. So we stop replication into
future master and finalize vbucket move by running "takeover"
ebucketmigrator. Takeover is a special flag of tap that makes
ep-engine initiate vbucket takeover at the end of replication.


1.8.x backwards compatibility
-------------------------------

1.8.x does not have indexes and it does not have "wait for checkpoint
persistence" command. Also 1.8.x runs replication ebucketmigrators on
source nodes rather than on destinations.

In order to handle that huge difference we have ns_config variable
named cluster_compat_mode. And only when we see that all nodes are
2.0+ we set it to [2, 0, 0] (3.0 sets it to [3, 0, 0]). This variable
allows us to very quickly detect if we need to run old-style code
paths or new-style code paths. And for many actions that have to
handle 1.8.x different we simply have difference pieces of the code.

Also note that 2.0.0 compact mode forbids adding 1.8.x nodes. So this
variable never jumps backwards (unless carefully done via specifically
left "backdoor" functions that are supposed to be called only via
/diag/eval). Same applies for 3.0 compat mode and 2.x nodes.


cluster orchestration guts: janitor_agent
-------------------------------------------

Since 2.0.0 we've separated janitor-and-rebalance to node interaction
into it's own dedicated module. This is done in order to more tightly
control what is de-factor API so that code changes are not breaking
backwards compat. But another reason is to properly serialize certain
actions. Like janitor setting vbucket states and replication and
rebalance changing them further.

janitor_agent is implemented as gen-server that is remotely called by
master node to change replications of that node or vbucket states or
something else cluster-orchestration and vbucket states related.


cluster orchestration guts: capi_set_view_manager
---------------------------------------------------

capi_set_view_manager is a per-bucket service (see
ns-server-hierarchy.txt) that keeps track of design documents and
serializes our access to indexes. A bunch of actions like changing
vbucket states, creating indexes, deleting indexes, waiting for index
completion, kicking indexes etc are done through that guy. And for
reasons outlined in section above janitor_agent those actions are
never remotely called. Few actions (like waiting for indexing
completion) that master node needs to do are all called through
janitor_agent.
