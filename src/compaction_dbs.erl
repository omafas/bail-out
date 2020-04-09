%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
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
%% @doc manager for kv compaction
%%      - serializes distribution of vbuckets among workers
%%      - serializes task progress
%%
-module(compaction_dbs).

-behaviour(gen_server).

-include("ns_common.hrl").

-record(state, {dbs_to_compact :: [vbucket_id()],
                progress_total :: non_neg_integer(),
                progress_current :: non_neg_integer()}).

-export([start_link/4,
         pick_db_to_compact/1,
         update_progress/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

start_link(BucketName, VBucketDbs, Force, OriginalTarget) ->
    gen_server:start_link(?MODULE, [BucketName, VBucketDbs, Force, OriginalTarget], []).

pick_db_to_compact(Pid) ->
    gen_server:call(Pid, pick_db_to_compact, infinity).

update_progress(Pid) ->
    gen_server:cast(Pid, update_progress).

init([BucketName, VBucketDbs, Force, OriginalTarget]) ->
    TriggerType = case Force of
                      true ->
                          manual;
                      false ->
                          scheduled
                  end,

    Total = length(VBucketDbs),

    ok = local_tasks:add_task(
           [{type, bucket_compaction},
            {original_target, OriginalTarget},
            {trigger_type, TriggerType},
            {bucket, BucketName},
            {vbuckets_done, 0},
            {total_vbuckets, Total},
            {progress, 0}]),

    {ok, #state{dbs_to_compact = VBucketDbs,
                progress_total = Total,
                progress_current = 0}}.

handle_call(pick_db_to_compact, _From,
            #state{dbs_to_compact = []} = State) ->
    {reply, undefined, State};
handle_call(pick_db_to_compact, _From,
            #state{dbs_to_compact = [Dbs | Rest]} = State) ->
    {reply, Dbs, State#state{dbs_to_compact = Rest}}.

handle_cast(update_progress,
            #state{progress_total = Total,
                   progress_current = Current} = State) ->
    NewCurrent = Current + 1,
    Progress = (NewCurrent * 100) div Total,
    ok = local_tasks:update(
           [{vbuckets_done, NewCurrent},
            {progress, Progress}]),
    {noreply, State#state{progress_current = NewCurrent}}.

handle_info(_Message, State) ->
    {stop, not_supported, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
