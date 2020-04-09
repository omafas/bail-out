%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2016 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
