# Config Remap Script

This script is an internal Couchbase tool and is not intended for use by any
un-authorized persons. Improper use of this script may render a Couchbase Server
cluster permanently inoperable.

A script to facilitate remapping of a Couchbase Server node from one hostname to
another. The script rewrites varies Server config structures to accomplish this.

## Structure:

- Base erlang code (config_remap.erl)
- Convenience wrapper - escript

## Things that are processed:

1) ip (files)
2) nodefile
3) initargs
4) ns_config 
   1) config.dat (the main config file)
   2) cookie (part of the config file)
   3) (cluster) uuid (part of the config file)
5) chronicle

## How it's processed:
1) Simple string files (1 and 2) are just parsed and rewritten
2) Term based files (3 and 4) are read into a term, misc:rewrite_value is used 
to rewrite the term
3) The ns_config cookie (4ii) is encrypted. We must provide a new cookie but we
should provide it encrypted for security reasons. We use the secrets management
code that the server runs for that.
4) The cluster uuid (4iii) is re-generated as this is logically a new cluster.
5) Chronicle (5) is split into log files and snapshot. Snapshots are fairly
simple to rewrite, they behave like term files (3) but are compressed and have a
crc hash that needs regenerating. Log files are iterated term by term and a new
log is built as we process these terms.

## How to run:

NOTE: Overwriting the same directory causes issue when rewriting the chronicle
log. For some reason the new file is being picked up at some point during the
write, so output-path must be specified. Will investigate later.

Use/testing with a typical deployment:
1) Set up EE cluster with all services and some bucket(s).
2) Take snapshot of nodes.
   1) If using cloud providers than cloud disk snapshots can be used.
   2) If using VMs then the server can be shut down and the contents of
   `/opt/couchbase/var/lib/couchbase` can be copied.
3) Load snapshot into new machines.
4) Run node remap script on all nodes.
   1) The remap script can be run as such:
   `/opt/couchbase/bin/escript /opt/couchbase/bin/escript-wrapper --initargs-path /opt/couchbase/var/lib/couchbase/initargs -- /opt/couchbase/bin/config_remap output-path output remap ns_1@<nodeA1Hostname> ns_1@<nodeA2Hostname> remap ns_1@<nodeB1Hostname> ns_1@<nodeB2Hostname> --regenerate-cookie --regenerate-cluster-uuid`
   2) Copy output over the existing information: `yes | cp -Rf output/* /opt/couchbase/var/lib/couchbase/`
5) Start up Couchbase Server on all nodes.

Testing with cluster_run:
1) Set up 2 node EE cluster `./cluster_run -n2 --dont-rename`
2) Connect cluster and bucket (just data service) `./cluster_connect -n2`
3) Shut down cluster
4) Copy baseline data (data files, uninteresting config etc.) to "new node" `cp -R data/n_0 data/n_10 && cp -R data/n_1 data/n_11 && cp couch/n_0_conf.ini couch/n_10_conf.ini && cp couch/n_1_conf.ini couch/n_11_conf.ini`
5) Run node rename script as follows. This renames nodes 0 and 1 to nodes 10 and
11 in both the node 10 and node 11 directories.
`
escript ../install/bin/escript-wrapper --initargs-path data/n_0/initargs -- deps/node_rename/node_rename output-path "data/n_10" cookie "foo" remap n_0@127.0.0.1 n_10@127.0.0.1 remap n_1@127.0.0.1 n_11@127.0.0.1 && escript ../install/bin/escript-wrapper --initargs-path data/n_0/initargs -- deps/config_remap/config_remap output-path "data/n_11" cookie "foo" remap n_0@127.0.0.1 n_10@127.0.0.1 remap n_1@127.0.0.1 n_11@127.0.0.1 --regenerate-cookie --regenerate-cluster-uuid
`
6) Start up the cluster_run `./cluster_run -n2 --dont-rename --start-index=10`.
   The cluster will continue to use the original ports.

There is an optional argument to log at debug level to the console:
`log-level debug`.

Single node clusters can be remapped with the script, but the hostname of single
node clusters is `cb.local` rather than the hostname of the machine. To remap a
single node cluster the hostname provided to the `remap` argument must be
`cb.local`. The identifier prefix, `n_1`/`ns_1` remains the same.

## Limitations:

Upon Couchbase Server startup after a node rename each node will regenerate a
new node cert from the OOTB root CA cert. If node to node encryption is enabled
and the OOTB root CA cert is untrusted then node to node communication cannot be
reestablished. The OOTB root CA cert must remain trusted.

This script only works for EE deployments as it expects gosecrets to be present.
If, in the future, CE remapping is required then we can enhance the script for
that, but at the moment there are no known CE usage requirements.

## Todo:
- Master branch, this could perhaps be a rebar3 application/release executable
instead of executed via escript-wrapper. That will break the API though.
- Investigate chronicle log overwrite noted below
