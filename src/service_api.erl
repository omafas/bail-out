%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
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
-module(service_api).

-include("ns_common.hrl").
-include("service_api.hrl").

-export([shutdown/1, get_node_info/1,
         get_task_list/2, cancel_task/3,
         get_current_topology/2,
         prepare_topology_change/6, start_topology_change/6]).

-define(RPC_TIMEOUT,       ?get_timeout(rpc, 60000)).
-define(LONG_POLL_TIMEOUT, ?get_timeout(long_poll, 30000)).

shutdown(Pid) ->
    perform_call(Pid, "Shutdown", empty_req()).

get_node_info(Pid) ->
    perform_call(Pid, "GetNodeInfo", empty_req()).

get_task_list(Pid, Rev) ->
    perform_call(Pid, "GetTaskList", get_req(Rev, ?LONG_POLL_TIMEOUT)).

cancel_task(Pid, Id, Rev) ->
    perform_call(Pid, "CancelTask", cancel_task_req(Id, Rev)).

get_current_topology(Pid, Rev) ->
    perform_call(Pid, "GetCurrentTopology", get_req(Rev, ?LONG_POLL_TIMEOUT)).

prepare_topology_change(Pid, Id, Rev, Type, KeepNodes, EjectNodes) ->
    perform_call(Pid, "PrepareTopologyChange",
                 topology_change_req(Id, Rev, Type, KeepNodes, EjectNodes)).

start_topology_change(Pid, Id, Rev, Type, KeepNodes, EjectNodes) ->
    perform_call(Pid, "StartTopologyChange",
                 topology_change_req(Id, Rev, Type, KeepNodes, EjectNodes)).

%% internal
perform_call(Pid, Name, Arg) ->
    FullName = "ServiceAPI." ++ Name,
    handle_result(json_rpc_connection:perform_call(Pid, FullName,
                                                   Arg, ?RPC_TIMEOUT)).

handle_result({ok, null}) ->
    ok;
handle_result({ok, _} = Result) ->
    Result;
handle_result({error, Error}) when is_binary(Error) ->
    {error, map_error(Error)};
handle_result({error, _} = Error) ->
    Error.

empty_req() ->
    {[]}.

get_req(Rev, Timeout) ->
    {[{rev, encode_rev(Rev)},
      {timeout, encode_timeout(Timeout)}]}.

cancel_task_req(Id, Rev) when is_binary(Id) ->
    {[{id, Id},
      {rev, encode_rev(Rev)}]}.

topology_change_req(Id, Rev, Type, KeepNodes, EjectNodes) ->
    true = is_binary(Id),

    {[{id, Id},
      {currentTopologyRev, encode_rev(Rev)},
      {type, encode_topology_change_type(Type)},
      {keepNodes, encode_keep_nodes(KeepNodes)},
      {ejectNodes, encode_eject_nodes(EjectNodes)}]}.

encode_rev(undefined) ->
    null;
encode_rev(Rev) when is_binary(Rev) ->
    Rev.

encode_timeout(infinity) ->
    0;
encode_timeout(Timeout) when is_integer(Timeout) ->
    Timeout.

encode_recovery_type(full) ->
    ?RECOVERY_FULL;
encode_recovery_type(delta) ->
    ?RECOVERY_DELTA.

encode_topology_change_type(rebalance) ->
    ?TOPOLOGY_CHANGE_REBALANCE;
encode_topology_change_type(failover) ->
    ?TOPOLOGY_CHANGE_FAILOVER.

encode_node_info(Props) ->
    {_, Id} = lists:keyfind(node_id, 1, Props),
    {_, Priority} = lists:keyfind(priority, 1, Props),
    {_, Opaque} = lists:keyfind(opaque, 1, Props),

    {[{nodeId, Id},
      {priority, Priority},
      {opaque, Opaque}]}.

encode_keep_nodes(KeepNodes) ->
    lists:map(
      fun ({NodeInfo, RecoveryType}) ->
              {[{nodeInfo, encode_node_info(NodeInfo)},
                {recoveryType, encode_recovery_type(RecoveryType)}]}
      end, KeepNodes).

encode_eject_nodes(Nodes) ->
    [encode_node_info(N) || N <- Nodes].

map_error(?ERROR_NOT_FOUND) ->
    not_found;
map_error(?ERROR_CONFLICT) ->
    conflict;
map_error(?ERROR_NOT_SUPPORTED) ->
    operation_not_supported;
map_error(?ERROR_RECOVERY_IMPOSSIBLE) ->
    recovery_impossible;
map_error(Error) -> {unknown_error, Error}.
