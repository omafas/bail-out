%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

-define(ERROR_NOT_FOUND, <<"not_found">>).
-define(ERROR_CONFLICT, <<"conflict">>).
-define(ERROR_NOT_SUPPORTED, <<"operation_not_supported">>).
-define(ERROR_RECOVERY_IMPOSSIBLE, <<"recovery_impossible">>).

-define(TOPOLOGY_CHANGE_REBALANCE, <<"topology-change-rebalance">>).
-define(TOPOLOGY_CHANGE_FAILOVER, <<"topology-change-failover">>).

-define(TASK_TYPE_REBALANCE, <<"task-rebalance">>).
-define(TASK_TYPE_PREPARED, <<"task-prepared">>).

-define(TASK_STATUS_RUNNING, <<"task-running">>).
-define(TASK_STATUS_FAILED, <<"task-failed">>).

-define(RECOVERY_FULL, <<"recovery-full">>).
-define(RECOVERY_DELTA, <<"recovery-delta">>).
