%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc supervisor for dcp_replicator's
%%
-module(dcp_sup).

-behavior(supervisor).

-include("ns_common.hrl").

-export([start_link/1, init/1]).

-export([get_children/1, manage_replicators/3, nuke/1,
         foreach_connection/2, connection_iterator_list/1]).
-export([get_replication_features/0]).

start_link(Bucket) ->
    supervisor:start_link({local, server_name(Bucket)}, ?MODULE, []).

-spec server_name(bucket_name()) -> atom().
server_name(Bucket) ->
    list_to_atom(?MODULE_STRING "-" ++ Bucket).

init([]) ->
    {ok, {{one_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          []}}.

get_children(Bucket) ->
    [{Node, C, T, M} ||
        {{Node, _RepFeatures, _ConnNum}, C, T, M}
            <- supervisor:which_children(server_name(Bucket)),
        is_pid(C)].

build_child_spec(Bucket, {ProducerNode, RepFeatures, ConnNum} = ChildId) ->
    #{id => ChildId,
      start => {dcp_replicator, start_link,
                [ProducerNode, Bucket, RepFeatures, ConnNum]},
      restart => temporary,
      shutdown => 60000,
      type => worker,
      modules => [dcp_replicator]}.

start_replicator(Bucket, {ProducerNode, RepFeatures}, ConnectionCount) ->
    ?log_debug("Starting ~p DCP replications from ~p for bucket ~p "
               "(Features = ~p)",
               [ConnectionCount, ProducerNode, Bucket, RepFeatures]),

    foreach_connection(
      ConnectionCount,
      fun(ConnNum) ->
              ChildID = {ProducerNode, RepFeatures, ConnNum},
              case supervisor:start_child(server_name(Bucket),
                                          build_child_spec(Bucket, ChildID)) of
                  {ok, _} -> ok;
                  {ok, _, _} -> ok
              end
      end).

kill_replicator(Bucket, {ProducerNode, RepFeatures} = _ChildId,
                ConnectionCount) ->
    ?log_debug("Going to stop ~p DCP replications from ~p for bucket ~p "
               "(Features = ~p)",
               [ConnectionCount, ProducerNode, Bucket, RepFeatures]),

    foreach_connection(
      ConnectionCount,
      fun(ConnNum) ->
              ChildID = {ProducerNode, RepFeatures, ConnNum},
              _ = supervisor:terminate_child(server_name(Bucket), ChildID)
      end),
    ok.

%% Replicators need to negotiate features while opening DCP connections
%% in order to enable certain features and these features can be enabled
%% only when the entire cluster is at a particular compat_version.
%%
%% We try to determine what features to enable and send this information as a
%% canonicalized list which is encoded into the replicator names (child ID).
%% Whenever features become eligible to be turned on or disabled, this list
%% would differ in its content thereby signalling the supervisor to drop
%% existing connections and recreate them with appropriate features enabled.
%% This could mean that the ongoing rebalance can fail and we are ok with that
%% as it can be restarted.
get_replication_features() ->
    CertAuth = ns_ssl_services_setup:client_cert_auth_state() =:= "mandatory",
    FeatureSet = [{xattr, true},
                  {snappy, memcached_config_mgr:is_snappy_enabled()},
                  %% this function is called for membase buckets only
                  %% so we can assume that if collections are enabled globally
                  %% they cannot be disabled for particular bucket
                  {collections, true},
                  {del_times, true},
                  {ssl, misc:should_cluster_data_be_encrypted()},
                  %% Not used directly but we need it to make sure we restart
                  %% replications when client_cert_auth_state change
                  {cert_auth, CertAuth},
                  {set_consumer_name, true},
                  {json, true},
                  {del_user_xattr, true}],
    misc:canonical_proplist(FeatureSet).

manage_replicators(Bucket, NeededNodes, NeededConnections) ->
    CurrNodes =
        [{Node, RepFeatures} ||
            {{Node, RepFeatures, _}, _C, _T, _M} <-
                supervisor:which_children(server_name(Bucket))],

    RepFeatures = get_replication_features(),
    ExpectedNodes = [{Node, RepFeatures} || Node <- NeededNodes],


    ToKill = lists:filter(
               fun({Node, _Features}) ->
                       case lists:keyfind(Node, 1, ExpectedNodes) of
                           %% not found - keep - kill
                           false -> true;
                           %% match - don't keep - don't kill
                           _ -> false
                       end
               end, CurrNodes),

    [kill_replicator(Bucket, CurrId, NeededConnections) || CurrId <- ToKill],
    [start_replicator(Bucket, NewId, NeededConnections) ||
        NewId <- ExpectedNodes -- CurrNodes].

nuke(Bucket) ->
    Children = try get_children(Bucket) of
                   RawKids ->
                       [{ProducerNode, Pid} || {ProducerNode, Pid, _, _} <- RawKids]
               catch exit:{noproc, _} ->
                       []
               end,

    ?log_debug("Nuking DCP replicators for bucket ~p:~n~p",
               [Bucket, Children]),
    misc:terminate_and_wait([Pid || {_, Pid} <- Children], {shutdown, nuke}),

    Connections = dcp_replicator:get_connections(Bucket),
    misc:parallel_map(
      fun (ConnName) ->
              dcp_proxy:nuke_connection(consumer, ConnName, node(), Bucket)
      end,
      Connections,
      infinity),

    ok.

-spec foreach_connection(pos_integer(), fun((integer()) -> any())) -> any().
%% @doc Iterate over the connections that this dcp_replication_manager knows
%% about. This is executed in the context of the caller, although a call may
%% be made to dcp_connection_manager to fetch the connection count.
foreach_connection(ConnectionCount, Fun) when is_integer(ConnectionCount) ->
    lists:foreach(
        fun(ConnNum) ->
            Fun(ConnNum)
        end, connection_iterator_list(ConnectionCount)).

-spec connection_iterator_list(pos_integer()) -> list().
%% @doc A list of the connection numbers between nodes for use when iterating
%% over connections.
connection_iterator_list(ConnectionCount) when is_integer(ConnectionCount) ->
    lists:seq(0, ConnectionCount - 1).
