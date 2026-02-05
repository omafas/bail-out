# The Couchbase Server

This application represents the top of the hierarchy of all memcached
smart services.  It is an application in the Erlang OTP sense.

## Building

Build dependencies include:

* erlang 25

Building:

You should use top level make file and repo manifest as explained
here: https://github.com/couchbase/manifest/blob/master/README.markdown

## Runtime dependencies

Before you start the server, you may need to do the following
  * Make sure the needed ports are not being used (these include
    8091, 11211, 11212, etc).


## Running

After building everything via top level makefile you'll have
`couchbase-server` script in your `$REPO/install/bin` (or other prefix if
you specified so). You can run this script for normal single node
startup.

During development it's convenient to have several 'nodes' on your
machine. There's `./build/cluster_run` script in root directory for achieving
that. Feel free to ask `--help`. You normally need something like -n2
where 2 is number of nodes you want.

It'll start REST API on ports 9000...9000+n. memcached on ports
12000+2*i and 11999-i and moxi ports on 12001+2*i ports. CAPI (which
was originally expected to be couch-compatible http API
implementation) is 9500...9500+n

Note that blank nodes are not configured and need to be setup. I
suggest trying web UI first to get the feeling of what's
possible. Just visit REST API port(s) via browser. For development
mode clusters it's port 9000 and higher. For production mode it's port
8091.

Other alternative is setting up and clustering nodes via REST
API. couchbase-cli allows that. And you can easily write your own
script(s).

There's `./build/cluster_connect` script that eases cluster configuration for
development clusters. Again, ask `--help`.

Sometimes during debugging/development you want smaller number of
vbuckets. You can change vbuckets number by setting
`COUCHBASE_NUM_VBUCKETS` environment variable to desired number of vbuckets
before creating new couchbase bucket.

### Other tools

Couchbase ships with a bunch of nice tools. Feel free to check
`$REPO/install/bin` (or `$PREFIX/bin`). One of notable tools is
`mbstats`. It allows you to query buckets for all kinds of internal
stats.

Another notable tool is `couchbase-cli`.

* * * * *
Copyright (c) 2016, Couchbase, Inc.
