%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

%% When adding a new task type, include it in both task_type() and ?TYPES, to
%% ensure that appropriate tests are ran
-type(task_type() :: loadingSampleBucket).
-define(TYPES, [loadingSampleBucket]).

%% When adding a new task status, include it in both status() and ?STATUSES, to
%% ensure that appropriate tests are ran
-type(status() :: queued | running | completed | failed).
-define(STATUSES, [queued, running, completed, failed]).

-type(task() :: [{type, task_type()} |
                 {timestamp, integer()} |
                 {status, status()} |
                 {extras, extras()}]).
-type(extras() :: [{atom(), any()}]).

-record(global_task, {task_id :: binary(),
                      type :: task_type(),
                      status :: status(),
                      extras = [] :: extras()}).

-export_type([task_type/0, status/0, task/0, extras/0]).
