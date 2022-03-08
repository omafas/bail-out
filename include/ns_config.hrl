%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-record(config, {init,         % Initialization parameters.
                 static = [],  % List of TupleList's; TupleList is {K, V}.
                 dynamic = [], % List of TupleList's; TupleList is {K, V}.
                 policy_mod,
                 saver_mfa,
                 saver_pid,
                 pending_more_save = false,
                 uuid,
                 upgrade_config_fun
                }).
-define(METADATA_VCLOCK, '_vclock').
-define(DELETED_MARKER, '_deleted').

-type uuid() :: binary().
-type vclock_counter() :: integer().
-type vclock_timestamp() :: integer().
-type vclock_entry() :: {uuid(), {vclock_counter(), vclock_timestamp()}}.
-type vclock() :: [vclock_entry()].

-type key() :: term().
-type raw_value() :: term().
-type value() :: [{?METADATA_VCLOCK, vclock()} | raw_value()] | raw_value().
-type kvpair() :: {key(), value()}.
-type kvlist() :: [kvpair()].

-type ns_config() :: #config{} | [kvlist()] | 'latest-config-marker'.

-type run_txn_return() :: {commit, [kvlist()]}
                        | {commit, [kvlist()], any()}
                        | {abort, any()}
                        | retry_needed.
