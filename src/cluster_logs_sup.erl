%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(cluster_logs_sup).

-include("ns_common.hrl").

-behaviour(supervisor).

-export([start_link/0, start_collect_logs/3,
         cancel_logs_collection/0]).

%% rpc:call-ed by cancel_logs_collection since 3.0
-export([cancel_local_logs_collection/0]).

%% rcp:call-ed by start_collect_logs since 3.0
-export([check_local_collect/0]).

-export([init/1]).

-define(TASK_CHECK_TIMEOUT, 5000).

init([]) ->
    {ok, {{one_for_all, 10, 10},
          [{ets_holder, {cluster_logs_collection_task, start_link_ets_holder, []},
            permanent, 1000, worker, []}]}}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_collect_logs(Nodes, BaseURL, Options) ->
    {Results, _} = MCResult = rpc:multicall(?MODULE, check_local_collect, [], ?TASK_CHECK_TIMEOUT),
    ?log_debug("check_local_collect returned: ~p", [MCResult]),
    case lists:any(fun (Res) -> Res =:= true end, Results) of
        false ->
            Spec = {collect_task,
                    {cluster_logs_collection_task, start_link, [Nodes, BaseURL,
                                                                Options]},
                    temporary,
                    brutal_kill,
                    worker, []},
            case supervisor:start_child(?MODULE, Spec) of
                {ok, _P} ->
                    ok;
                {error, {already_started, _}} ->
                    already_started
            end;
        true ->
            ?log_debug("Got already_started via check_local_collect check: ~p", [MCResult]),
            already_started
    end.

check_local_collect() ->
    RV = [T || T <- supervisor:which_children(?MODULE),
               case T of
                   {Id, _, _, _} ->
                       Id =:= collect_task
               end],
    RV =/= [].

cancel_logs_collection() ->
    _ = rpc:multicall(?MODULE, cancel_local_logs_collection, []).

cancel_local_logs_collection() ->
    _ = supervisor:terminate_child(?MODULE, collect_task),
    ok.
