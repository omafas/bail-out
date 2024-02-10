%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-define(ERROR_NOT_FOUND, <<"not_found">>).
-define(ERROR_CONFLICT, <<"conflict">>).
-define(ERROR_NOT_SUPPORTED, <<"operation_not_supported">>).
-define(ERROR_RECOVERY_IMPOSSIBLE, <<"recovery_impossible">>).

-define(TOPOLOGY_CHANGE_REBALANCE, <<"topology-change-rebalance">>).
-define(TOPOLOGY_CHANGE_FAILOVER, <<"topology-change-failover">>).

-define(TASK_TYPE_REBALANCE, <<"task-rebalance">>).
-define(TASK_TYPE_PREPARED, <<"task-prepared">>).

-define(TASK_TYPE_PAUSE_BUCKET, <<"task-pause-bucket">>).
-define(TASK_TYPE_RESUME_BUCKET, <<"task-resume-bucket">>).

-define(TASK_STATUS_RUNNING, <<"task-running">>).
-define(TASK_STATUS_FAILED, <<"task-failed">>).

%% TASK_STATUS_CANNOT_RESUME is a special status used only during the "dry-run"
%% phase for resuming a bucket and means a bucket can not be resumed.  If the
%% execution of the "dry-run" resume bucket task itself fails
%% TASK_STATUS_FAILED is returned.

-define(TASK_STATUS_CANNOT_RESUME, <<"task-status-cannot-resume">>).

-define(RECOVERY_FULL, <<"recovery-full">>).
-define(RECOVERY_DELTA, <<"recovery-delta">>).
