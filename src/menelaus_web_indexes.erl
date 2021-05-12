%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(menelaus_web_indexes).

-include("ns_common.hrl").
-include("cut.hrl").
-export([handle_settings_get/1, handle_settings_post/1, handle_index_status/1]).

handle_settings_get(Req) ->
    Settings = get_settings(),
    true = (Settings =/= undefined),
    menelaus_util:reply_json(Req, {Settings}).

get_settings() ->
    index_settings_manager:get(generalSettings) ++
        [{storageMode, index_settings_manager:get(storageMode)}].

settings_post_validators() ->
    [validator:has_params(_),
     validator:integer(indexerThreads, 0, 1024, _),
     validator:integer(memorySnapshotInterval, 1, infinity, _),
     validator:integer(stableSnapshotInterval, 1, infinity, _),
     validator:integer(maxRollbackPoints, 1, infinity, _)] ++
        case cluster_compat_mode:is_cluster_70() of
            true ->
                [validator:boolean(redistributeIndexes, _),
                 validator:integer(numReplica, 0, 16, _)];
            _ ->
                []
        end ++
        [validate_param(logLevel, _),
         validate_storage_mode(_),
         validator:unsupported(_)].

validate_storage_mode(State) ->
    %% Note, at the beginning the storage mode will be empty. Once set,
    %% validate_param will prevent user from changing it back to empty
    %% since it is not one of the acceptable values.
    State1 = validate_param(storageMode, State),

    %% Do not allow:
    %% - setting index storage mode to mem optimized or plasma in community edition
    %% - changing index storage mode in community edition
    %% - changing index storage mode when there are nodes running index
    %%   service in the cluster in enterprise edition.
    %% - changing index storage mode back to forestdb after having it set to either
    %%   memory_optimized or plasma in enterprise edition.
    %% - setting the storage mode to forestdb on a newly configured 5.0 enterprise cluster.
    IndexErr = "Changing the optimization mode of global indexes is not supported when index service nodes are present in the cluster. Please remove all index service nodes to change this option.",

    OldValue = index_settings_manager:get(storageMode),
    validator:validate(
      fun (Value) when Value =:= OldValue ->
              ok;
          (Value) ->
              case OldValue =:= <<"">> of
                  true ->
                      is_storage_mode_acceptable(Value);
                  false ->
                      %% Note it is not sufficient to check service_active_nodes(index) because the
                      %% index nodes could be down or failed over. However, we should allow the
                      %% storage mode to be changed if there is an index node in "inactiveAdded"
                      %% state (the state set when a node has been added but the rebalance has not
                      %% been run yet).
                      NodesWanted = ns_node_disco:nodes_wanted(),
                      AllIndexNodes = ns_cluster_membership:service_nodes(NodesWanted, index),
                      InactiveAddedNodes = ns_cluster_membership:inactive_added_nodes(),
                      IndexNodes = AllIndexNodes -- InactiveAddedNodes,

                      case IndexNodes of
                          [] ->
                              is_storage_mode_acceptable(Value);
                          _ ->
                              ?log_debug("Index nodes ~p present. Cannot change index storage mode.~n",
                                         [IndexNodes]),
                              {error, IndexErr}
                      end
              end
      end, storageMode, State1).

is_storage_mode_acceptable(Value) ->
    ReportError = fun(Msg) ->
                          ?log_debug(Msg),
                          {error, Msg}
                  end,

    case Value of
        ?INDEX_STORAGE_MODE_FORESTDB ->
            case cluster_compat_mode:is_enterprise() of
                true ->
                    ReportError("Storage mode cannot be set to 'forestdb' in 5.0 enterprise edition.");
                false ->
                    ok
            end;
        ?INDEX_STORAGE_MODE_MEMORY_OPTIMIZED ->
            case cluster_compat_mode:is_enterprise() of
                true ->
                    ok;
                false ->
                    ReportError("Memory optimized indexes are restricted to enterprise edition and "
                                "are not available in the community edition.")
            end;
        ?INDEX_STORAGE_MODE_PLASMA ->
            case cluster_compat_mode:is_enterprise() of
                true ->
                    ok;
                false ->
                    ReportError("Storage mode can be set to 'plasma' only if the cluster is "
                                "5.0 enterprise edition.")
            end;
        _ ->
            ReportError(io_lib:format("Invalid value '~s'", [binary_to_list(Value)]))
    end.

acceptable_values(logLevel) ->
    ["silent", "fatal", "error", "warn", "info", "verbose", "timing", "debug",
     "trace"];
acceptable_values(storageMode) ->
    Modes = case cluster_compat_mode:is_enterprise() of
                true ->
                    [?INDEX_STORAGE_MODE_PLASMA,
                     ?INDEX_STORAGE_MODE_MEMORY_OPTIMIZED];
                false ->
                    [?INDEX_STORAGE_MODE_FORESTDB]
            end,
    [binary_to_list(X) || X <- Modes].

validate_param(Name, State) ->
    functools:chain(
      State,
      [validator:one_of(Name, acceptable_values(Name), _),
       validator:convert(Name, fun list_to_binary/1, _)]).

update_storage_mode(Req, Values) ->
    case proplists:get_value(storageMode, Values) of
        undefined ->
            Values;
        StorageMode ->
            ok = update_settings(storageMode, StorageMode),
            ns_audit:modify_index_storage_mode(Req, StorageMode),
            proplists:delete(storageMode, Values)
    end.
update_settings(Key, Value) ->
    case index_settings_manager:update(Key, Value) of
        {ok, _} ->
            ok;
        retry_needed ->
            erlang:error(exceeded_retries)
    end.

handle_settings_post(Req) ->
    validator:handle(
      fun (Values) ->
              Values1 = update_storage_mode(Req, Values),
              case Values1 of
                  [] ->
                      ok;
                  _ ->
                      ok = update_settings(generalSettings, Values1),
                      ns_audit:modify_index_settings(Req, Values1)
              end,
              menelaus_util:reply_json(Req, {get_settings()})
      end, Req, form, settings_post_validators()).

filter_indexes(Roles, Indexes) ->
    CollectionEnabled = collections:enabled(),
    lists:filter(
      fun (Index) ->
              B = binary_to_list(proplists:get_value(bucket, Index)),
              {S, C} = case CollectionEnabled of
                           false ->
                               {all, all};
                           true ->
                               {binary_to_list(proplists:get_value(scope,
                                                                   Index)),
                                binary_to_list(proplists:get_value(collection,
                                                                   Index))}
                       end,
              menelaus_roles:is_allowed(
                {[{collection, [B, S, C]}, n1ql, index], read}, Roles)
      end, Indexes).

handle_index_status(Req) ->
    Identity = menelaus_auth:get_identity(Req),
    Roles = menelaus_roles:get_compiled_roles(Identity),
    {ok, Indexes, Stale, Version} = service_index:get_indexes(),
    Filtered = filter_indexes(Roles, Indexes),

    Warnings =
        case Stale of
            true ->
                Msg = <<"Cannot communicate with indexer process. "
                        "Information on indexes may be stale. Will retry.">>,
                [Msg];
            false ->
                []
        end,

    menelaus_util:reply_json(Req, {[{indexes, [{I} || I <- Filtered]},
                                    {version, Version},
                                    {warnings, Warnings}]}).
