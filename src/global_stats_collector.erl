%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-2017 Couchbase, Inc.
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
%%
-module(global_stats_collector).

-include("ns_common.hrl").

-include("ns_stats.hrl").

%% API
-export([start_link/0]).

%% callbacks
-export([init/1, grab_stats/1, process_stats/5]).

start_link() ->
    base_stats_collector:start_link(?MODULE, []).

init([]) ->
    {ok, []}.

grab_stats([]) ->
    {ok, Stats} = ns_audit:stats(),
    Stats.

process_stats(_TS, Stats, _PrevCounters, _PrevTS, []) ->
    AuditDroppedEvents = proplists:get_value(<<"dropped_events">>, Stats, <<"0">>),
    AuditQueueLen = proplists:get_value(queue_length, Stats, 0),
    AuditRetries = proplists:get_value(unsuccessful_retries, Stats, 0),

    AuditStats = [{audit_dropped_events, list_to_integer(binary_to_list(AuditDroppedEvents))},
                  {audit_queue_length, AuditQueueLen},
                  {audit_unsuccessful_retries, AuditRetries}],
    {[{"@global", AuditStats}], undefined, []}.
