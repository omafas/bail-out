%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(rebalance_quirks).

-define(TABLE, ?MODULE).
-define(WAIT_STATUSES_TIMEOUT,
        ns_config:get_timeout({?MODULE, wait_statuses}, 15000)).

-export([get_quirks/2, is_enabled/2, get_node_quirks/2,
         default_config/0, upgrade_config_project_intact_patched/0]).
-export_type([quirk/0]).

-type quirk() :: takeover_via_orchestrator |
                 disable_old_master |
                 reset_replicas |
                 trivial_moves |
                 dont_truncate_long_names.

%% APIs
get_quirks(Nodes, Type) ->
    Config = ns_config:latest(),
    OverrideQuirks =
        lists:filtermap(
          fun (Node) ->
                  case get_override_quirks(Node, Config) of
                      false ->
                          false;
                      {value, Quirks} ->
                          {true, {Node, Quirks}}
                  end
          end, Nodes),

    OtherNodes     = Nodes -- proplists:get_keys(OverrideQuirks),
    ComputedQuirks = compute_quirks(OtherNodes, Config, Type),

    OverrideQuirks ++
        lists:map(
          fun ({Node, NodeQuirks}) ->
                  Extra    = get_extra_quirks(Node, Config),
                  Disabled = get_disabled_quirks(Node, Config),

                  {Node, lists:usort(NodeQuirks ++ Extra) -- Disabled}
          end, ComputedQuirks).

is_enabled(Quirk, Quirks) ->
    true = lists:member(Quirk, all_quirks()),
    proplists:get_bool(Quirk, Quirks).

get_node_quirks(Node, Quirks) ->
    proplists:get_value(Node, Quirks, []).

default_config() ->
    [{project_intact_vulnerable_key(), false}].

upgrade_config_project_intact_patched() ->
    [{set, project_intact_vulnerable_key(), false}].

%% internal
get_override_quirks(Node, Config) ->
    ns_config:search_node(Node, Config, override_rebalance_quirks).

get_extra_quirks(Node, Config) ->
    ns_config:search_node_with_default(Node, Config,
                                       extra_rebalance_quirks, []).

get_disabled_quirks(Node, Config) ->
    ns_config:search_node_with_default(Node, Config,
                                       disable_rebalance_quirks, []).

compute_quirks(Nodes, Config, project_intact) ->
    Unpatched = [N || N <- Nodes,
                      not has_project_intact_patches(N, Config)],

    case Unpatched of
        [] ->
            [{N, []} || N <- Nodes];
        _ ->
            compute_quirks(Nodes)
    end;
compute_quirks(Nodes, _Config, long_names) ->
    compute_quirks(Nodes).

compute_quirks(Nodes) ->
    lists:map(fun ({Node, Status}) ->
                      case get_version(Status) of
                          {ok, Version} ->
                              {Node, quirks_for_version(Version)};
                          no_version ->
                              exit({no_version_for_node, Node})
                      end
              end, get_statuses(Nodes)).

get_statuses(Nodes) ->
    case ns_doctor:wait_statuses(Nodes, ?WAIT_STATUSES_TIMEOUT) of
        {ok, Statuses} ->
            dict:to_list(Statuses);
        {error, {timeout, MissingNodes}} ->
            exit({missing_node_statuses, MissingNodes})
    end.

get_version(Status) ->
    AppVersions = proplists:get_value(version, Status, []),
    case proplists:get_value(ns_server, AppVersions) of
        undefined ->
            no_version;
        Version ->
            try misc:parse_version(Version) of
                {[_, _, _] = VersionTriple, _, _} ->
                    {ok, VersionTriple};
                _Other ->
                    no_version
            catch
                _:_ ->
                    no_version
            end
    end.

-spec all_quirks() -> [quirk()].
all_quirks() ->
    [takeover_via_orchestrator,
     disable_old_master,
     reset_replicas,
     trivial_moves,
     dont_truncate_long_names].

quirks_for_version(Version) ->
    [Quirk || Quirk <- all_quirks(),
              is_quirk_required(Quirk, Version)].

is_quirk_required(dont_truncate_long_names, [7, 0, X]) when X < 2 ->
    true;
is_quirk_required(dont_truncate_long_names, Version) ->
    Version < [6, 6, 5];
is_quirk_required(takeover_via_orchestrator, Version) ->
    Version < [5, 1, 0];
is_quirk_required(_, _) ->
    false.

has_project_intact_patches(Node, Config) ->
    not ns_config:search(Config, project_intact_vulnerable_key(Node), true).

project_intact_vulnerable_key() ->
    project_intact_vulnerable_key(node()).

project_intact_vulnerable_key(Node) ->
    {node, Node, {project_intact, is_vulnerable}}.
