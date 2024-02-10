%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(testconditions).

-include("ns_common.hrl").

-define(TESTCONDITION_STORE, testconditions).

%% APIs
-export([get/1,
         set/2,
         delete/1]).

get(Key) ->
    simple_store:get(?TESTCONDITION_STORE, Key).

%% TODO: No need to persist the testconditions.
%% Add function for non-persistent set() in simple_store.
set(Key, Value) ->
    simple_store:set(?TESTCONDITION_STORE, Key, Value).

delete(Key) ->
    simple_store:delete(?TESTCONDITION_STORE, Key).
