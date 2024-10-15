%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Bucket related macros
%%

-include("ns_common.hrl").

-define(MAGMA_FRAG_PERCENTAGE, 50).
-define(MIN_MAGMA_FRAG_PERCENTAGE, 10).
-define(MAX_MAGMA_FRAG_PERCENTAGE, 100).
-define(DEFAULT_MAGMA_SHARDS, 0).
-define(MIN_MAGMA_SHARDS, 1).
-define(MAX_MAGMA_SHARDS, 128).

-define(MAGMA_STORAGE_QUOTA_PERCENTAGE, 50).
-define(MIN_MAGMA_STORAGE_QUOTA_PERCENTAGE, 1).
-define(MAX_MAGMA_STORAGE_QUOTA_PERCENTAGE, 85).

-define(MAGMA_KEY_TREE_DATA_BLOCKSIZE, 4096).
-define(MIN_MAGMA_KEY_TREE_DATA_BLOCKSIZE, 4096).
-define(MAX_MAGMA_KEY_TREE_DATA_BLOCKSIZE, 131072).

-define(MAGMA_SEQ_TREE_DATA_BLOCKSIZE, 4096).
-define(MIN_MAGMA_SEQ_TREE_DATA_BLOCKSIZE, 4096).
-define(MAX_MAGMA_SEQ_TREE_DATA_BLOCKSIZE, 131072).

-define(NUM_WORKER_THREADS, 3).
-define(MIN_NUM_WORKER_THREADS, 2).
-define(MAX_NUM_WORKER_THREADS, 8).

-define(MEMBASE_HT_LOCKS, 47).
-define(DEFAULT_MEMBASE_NUM_REPLICAS, 1).
-define(DEFAULT_MEMCACHED_NUM_REPLICAS, 0).
-define(MAX_NUM_REPLICAS, 3).
-define(MIN_DRIFT_BEHIND_THRESHOLD, 100).

-define(MIN_NUM_VBUCKETS, 16).
-define(MAX_NUM_VBUCKETS, 1024).

%% Storage limits in GiBs
-define(DEFAULT_KV_STORAGE_LIMIT, 500).
-define(MIN_KV_STORAGE_LIMIT, -1).
-define(MAX_KV_STORAGE_LIMIT, 100000).

-define(DEFAULT_INDEX_STORAGE_LIMIT, 500).
-define(MIN_INDEX_STORAGE_LIMIT, -1).
-define(MAX_INDEX_STORAGE_LIMIT, 100000).

-define(DEFAULT_FTS_STORAGE_LIMIT, 500).
-define(MIN_FTS_STORAGE_LIMIT, -1).
-define(MAX_FTS_STORAGE_LIMIT, 100000).

%% Throttle limits in units of Read Units Ops and Write Units Ops
-define(DEFAULT_KV_THROTTLE_LIMIT, 5000).
-define(DEFAULT_INDEX_THROTTLE_LIMIT, 5000).
-define(DEFAULT_FTS_THROTTLE_LIMIT, 5000).
-define(DEFAULT_N1QL_THROTTLE_LIMIT, 5000).
-define(MIN_THROTTLE_LIMIT, -1).
-define(MAX_THROTTLE_LIMIT, ?MAX_32BIT_UNSIGNED_INT).

-define(HISTORY_RETENTION_SECONDS_DEFAULT, 0).
-define(HISTORY_RETENTION_BYTES_DEFAULT, 0).
%% 2GiB in bytes
-define(HISTORY_RETENTION_BYTES_MIN, 2 * 1024 * 1024 * 1024).
-define(HISTORY_RETENTION_COLLECTION_DEFAULT_DEFAULT, true).
