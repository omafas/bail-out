%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc suprevisor for dcp_replicator's
%%
-module(dcp_sup).

-behavior(supervisor).

-include("ns_common.hrl").

-export([start_link/1, init/1]).

-export([get_children/1, manage_replicators/2, nuke/1]).
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
        {{Node, _RepFeatures}, C, T, M}
            <- supervisor:which_children(server_name(Bucket)),
        is_pid(C)].

build_child_spec(Bucket, {ProducerNode, RepFeatures} = ChildId) ->
    {ChildId,
     {dcp_replicator, start_link, [ProducerNode, Bucket, RepFeatures]},
     temporary, 60000, worker, [dcp_replicator]}.

start_replicator(Bucket, {ProducerNode, RepFeatures} = ChildId) ->
    ?log_debug("Starting DCP replication from ~p for bucket ~p (Features = ~p)",
               [ProducerNode, Bucket, RepFeatures]),

    case supervisor:start_child(server_name(Bucket),
                                build_child_spec(Bucket, ChildId)) of
        {ok, _} -> ok;
        {ok, _, _} -> ok
    end.

kill_replicator(Bucket, {ProducerNode, RepFeatures} = ChildId) ->
    ?log_debug("Going to stop DCP replication from ~p for bucket ~p "
               "(Features = ~p)", [ProducerNode, Bucket, RepFeatures]),
    _ = supervisor:terminate_child(server_name(Bucket), ChildId),
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
    FeatureSet = [%% Unconditionally setting 'xattr' to true as xattr feature
                  %% must be negotiated by default in post-5.0 clusters.
                  {xattr, true},
                  {snappy, memcached_config_mgr:is_snappy_enabled()},
                  %% this function is called for membase buckets only
                  %% so we can assume that if collections are enabled globally
                  %% they cannot be disabled for particular bucket
                  {collections, collections:enabled()},
                  %% Unconditionally setting 'del_times' to true as feature
                  %% must be negotiated in post-5.5 clusters, and earlier
                  %% versions are no longer supported.
                  {del_times, true},
                  {ssl, misc:should_cluster_data_be_encrypted()},
                  %% Unconditionally setting 'set_consumer_name' and
                  %% 'json' to true as features are negotiated starting
                  %% with the 6.5 release
                  {set_consumer_name, true},
                  {json, true},
                  {del_user_xattr, cluster_compat_mode:is_cluster_66()}],
    misc:canonical_proplist(FeatureSet).

manage_replicators(Bucket, NeededNodes) ->
    CurrNodes = [ChildId || {ChildId, _C, _T, _M} <-
                                supervisor:which_children(server_name(Bucket))],

    RepFeatures = get_replication_features(),
    ExpectedNodes = [{Node, RepFeatures} || Node <- NeededNodes],

    [kill_replicator(Bucket, CurrId) || CurrId <- CurrNodes -- ExpectedNodes],
    [start_replicator(Bucket, NewId) || NewId <- ExpectedNodes -- CurrNodes].

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
