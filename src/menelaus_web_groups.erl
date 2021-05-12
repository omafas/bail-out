%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc REST API for managing server groups (racks)
-module(menelaus_web_groups).

-include("ns_common.hrl").
-include("cut.hrl").

-export([handle_server_groups/1,
         handle_server_groups_put/1,
         handle_server_groups_post/1,
         handle_server_group_update/2,
         handle_server_group_delete/2]).

-import(menelaus_util,
        [bin_concat_path/1,
         reply_json/2,
         reply_json/3]).

build_group_uri(UUID) when is_binary(UUID) ->
    bin_concat_path(["pools", "default", "serverGroups", UUID]);
build_group_uri(GroupPList) ->
    build_group_uri(proplists:get_value(uuid, GroupPList)).

handle_server_groups(Req) ->
    Groups = ns_cluster_membership:server_groups(),
    IncludeOtpCookie = menelaus_auth:has_permission({[admin, internal], all},
                                                    Req),
    Ctx = menelaus_web_node:get_context(Req, IncludeOtpCookie, unstable),
    Fun = menelaus_web_node:build_nodes_info_fun(Ctx),
    J = [begin
             UUIDBin = proplists:get_value(uuid, G),
             L = [{name, proplists:get_value(name, G)},
                  {uri, build_group_uri(UUIDBin)},
                  {addNodeURI, bin_concat_path(["pools", "default",
                                                "serverGroups", UUIDBin, "addNode"])},
                  {nodes, [Fun(N, undefined) || N <- proplists:get_value(nodes, G, [])]}],
             {struct, L}
         end || G <- Groups],
    V = list_to_binary(integer_to_list(erlang:phash2(Groups))),
    reply_json(Req, {struct, [{groups, J},
                              {uri, <<"/pools/default/serverGroups?rev=",V/binary>>}]}).

handle_server_groups_put(Req) ->
    menelaus_util:assert_is_enterprise(),
    Rev = proplists:get_value("rev", mochiweb_request:parse_qs(Req)),
    JSON = menelaus_util:parse_json(Req),

    RV = chronicle_compat:transaction(
           [rebalancer_pid, server_groups, nodes_wanted],
           server_groups_put_txn(_, JSON, Rev)),
    case RV of
        {ok, _, Groups} ->
            [ns_audit:update_group(Req, Group) || Group <- Groups],
            menelaus_util:reply_json(Req, [], 200);
        {parse_error, Error} ->
            reply_json(Req, Error, 400);
        rebalance_running ->
            menelaus_util:reply_json(
              Req, "Cannot update server group while rebalance is running",
              503);
        wrong_revision ->
            menelaus_util:reply_json(Req, [], 409)
    end.

build_replacement_groups(Groups, ParsedGroups) ->
    ParsedGroupsDict = dict:from_list(ParsedGroups),
    ReplacementGroups0 =
        [case dict:find(build_group_uri(PList),
                        ParsedGroupsDict) of
             {ok, NewNodes} ->
                 lists:keystore(nodes, 1, PList, {nodes, lists:sort(NewNodes)})
         end ||  PList <- Groups],
    lists:sort(fun (A, B) ->
                       KA = lists:keyfind(uuid, 1, A),
                       KB = lists:keyfind(uuid, 1, B),
                       KA =< KB
               end, ReplacementGroups0).

server_groups_put_txn(Cfg, JSON, Rev) ->
    Groups = ns_cluster_membership:server_groups(Cfg),
    Nodes = ns_node_disco:nodes_wanted(Cfg),
    ParsedGroups =
        try
            parse_validate_groups_payload(JSON, Groups, Nodes)
        catch throw:group_parse_error ->
                {abort, {parse_error, <<"Bad input">>}};
              throw:{group_parse_error, Parsed} ->
                {abort, {parse_error, [<<"Bad input">>,
                                       [{struct, PL} || PL <- Parsed]], 400}}
        end,
    case ParsedGroups of
        {abort, _} = Abort ->
            Abort;
        _ ->
            case integer_to_list(erlang:phash2(Groups)) of
                Rev ->
                    case rebalance:running(Cfg) of
                        true ->
                            {abort, rebalance_running};
                        false ->
                            NewGroups = build_replacement_groups(
                                          Groups, ParsedGroups),
                            {commit, [{set, server_groups, NewGroups}],
                             NewGroups}
                    end;
                _ ->
                    {abort, wrong_revision}
            end
    end.

assert_assignments(Dict, Assignments) ->
    PostDict =
        lists:foldl(
          fun ({Key, Value}, Acc) ->
                  dict:update(Key,
                              fun (MaybeNone) ->
                                      case MaybeNone of
                                          none -> Value;
                                          _ -> erlang:throw(group_parse_error)
                                      end
                              end, unknown, Acc)
          end, Dict, Assignments),
    _ = dict:fold(fun (_, Value, _) ->
                          case Value =:= none orelse Value =:= unknown of
                              true ->
                                  erlang:throw(group_parse_error);
                              _ ->
                                  ok
                          end
                  end, [], PostDict).

parse_validate_groups_payload(JSON, Groups, Nodes) ->
    NodesSet = sets:from_list([list_to_binary(atom_to_list(N)) || N <- Nodes]),
    ParsedGroups = parse_server_groups(NodesSet, JSON),
    try
        parse_validate_groups_payload_inner(ParsedGroups, Groups, Nodes)
    catch throw:group_parse_error ->
            PLists = [[{uri, URI},
                       {nodeNames, Ns}]
                      ++ case Name of
                             undefined -> [];
                             _ ->
                                 [{name, Name}]
                         end
                      || {URI, Ns, Name} <- ParsedGroups],
            erlang:throw({group_parse_error, PLists})
    end.


parse_validate_groups_payload_inner(ParsedGroups, Groups, Nodes) ->
    ExpectedNamesList = [{build_group_uri(G), proplists:get_value(name, G)}
                         || G <- Groups],
    ExpectedNames = sets:from_list(ExpectedNamesList),


    %% If we do have at least one uri/name pair that does not match
    %% uri/name pair in current groups then we either have unexpected
    %% new group (which will be checked later anyways) or we have
    %% renaming. In any case we raise error, but intention here is to
    %% catch renaming
    [case sets:is_element({URI, Name}, ExpectedNames) of
         false ->
             erlang:throw(group_parse_error);
         _ ->
             ok
     end || {URI, _, Name} <- ParsedGroups,
            Name =/= undefined],

    GroupsDict = dict:from_list([{URI, none} || {URI, _Name} <- ExpectedNamesList]),
    StrippedParsedGroups = [{URI, Ns} || {URI, Ns, _} <- ParsedGroups],
    assert_assignments(GroupsDict, StrippedParsedGroups),

    NodesDict = dict:from_list([{N, none} || N <- Nodes]),
    NodeAssignments = [{N, G} || {G, Ns} <- StrippedParsedGroups,
                                 N <- Ns],
    assert_assignments(NodesDict, NodeAssignments),
    StrippedParsedGroups.

parse_server_groups(NodesSet, JSON) ->
    case JSON of
        {struct, JPlist} ->
            L = proplists:get_value(<<"groups">>, JPlist),
            case is_list(L) of
                true -> ok;
                _ -> erlang:throw(group_parse_error)
            end,
            [parse_single_group(NodesSet, G) || G <- L];
        _ ->
            erlang:throw(group_parse_error)
    end.

parse_single_group(NodesSet, {struct, G}) ->
    Nodes = proplists:get_value(<<"nodes">>, G),
    case is_list(Nodes) of
        false ->
            erlang:throw(group_parse_error);
        _ ->
            Ns =
                [case NodesEl of
                     {struct, PList} ->
                         case proplists:get_value(<<"otpNode">>, PList) of
                             undefined ->
                                 erlang:throw(group_parse_error);
                             N ->
                                 case sets:is_element(N, NodesSet) of
                                     true ->
                                         list_to_existing_atom(binary_to_list(N));
                                     _ ->
                                         erlang:throw(group_parse_error)
                                 end
                         end;
                     _ ->
                         erlang:throw(group_parse_error)
                 end || NodesEl <- Nodes],
            URI = proplists:get_value(<<"uri">>, G),
            Name = proplists:get_value(<<"name">>, G),
            case URI =:= undefined of
                true ->
                    erlang:throw(group_parse_error);
                _ ->
                    {URI, Ns, Name}
            end
    end;
parse_single_group(_NodesSet, _NonStructG) ->
    erlang:throw(group_parse_error).

handle_server_groups_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    case parse_groups_post(mochiweb_request:parse_post(Req)) of
        {ok, Name} ->
            case do_handle_server_groups_post(Name, Req) of
                ok ->
                    reply_json(Req, []);
                already_exists ->
                    reply_json(Req, {struct, [{name, <<"already exists">>}]}, 400)
            end;
        {errors, Errors} ->
            reply_json(Req, {struct, Errors}, 400)
    end.

find_group_by_prop(Prop, Value, Groups) ->
    lists:search(?cut(proplists:get_value(Prop, _) =:= Value), Groups).

do_handle_server_groups_post(Name, Req) ->
    RV = chronicle_compat:transaction(
           [server_groups],
           fun (Snapshot) ->
                   Groups = ns_cluster_membership:server_groups(Snapshot),
                   case find_group_by_prop(name, Name, Groups) of
                       false ->
                           UUID = couch_uuids:random(),
                           true = is_binary(UUID),
                           AGroup = [{uuid, UUID},
                                     {name, Name},
                                     {nodes, []}],
                           NewGroups = lists:sort([AGroup | Groups]),
                           {commit, [{set, server_groups, NewGroups}], AGroup};
                       {value, _} ->
                           {abort, already_exists}
                   end
           end),
    case RV of
        {ok, _, AGroup} ->
            ns_audit:add_group(Req, AGroup),
            ok;
        Error ->
            Error
    end.

parse_groups_post(Params) ->
    MaybeName = case proplists:get_value("name", Params) of
                    undefined ->
                        {error, <<"is missing">>};
                    Name ->
                        case string:trim(Name) of
                            "" ->
                                {error, <<"cannot be empty">>};
                            Trimmed ->
                                case length(Name) > 64 of
                                    true ->
                                        {error, <<"cannot be longer than 64 bytes">>};
                                    _ ->
                                        {ok, list_to_binary(Trimmed)}
                                end
                        end
                 end,
    ExtraParams = [K || {K, _} <- Params,
                        K =/= "name"],
    MaybeExtra = case ExtraParams of
                     [] ->
                         [];
                     _ ->
                         [{'_', iolist_to_binary([<<"unknown parameters: ">>, string:join(ExtraParams, ", ")])}]
                 end,
    Errors = MaybeExtra ++ case MaybeName of
                                {error, Msg} ->
                                    [{name, Msg}];
                                _ ->
                                    []
                            end,
    case Errors of
        [] ->
            {ok, _} = MaybeName,
            MaybeName;
        _ ->
            {errors, Errors}
    end.

handle_server_group_update(GroupUUID, Req) ->
    menelaus_util:assert_is_enterprise(),
    case parse_groups_post(mochiweb_request:parse_post(Req)) of
        {ok, Name} ->
            case do_group_update(list_to_binary(GroupUUID), Name, Req) of
                ok ->
                    reply_json(Req, []);
                not_found ->
                    reply_json(Req, [], 404);
                already_exists ->
                    reply_json(Req, {struct, [{name, <<"already exists">>}]}, 400)
            end;
        {errors, Errors} ->
            reply_json(Req, {struct, Errors}, 400)
    end.

do_group_update(GroupUUID, Name, Req) ->
    RV = chronicle_compat:transaction(
           [server_groups],
           fun (Snapshot) ->
                   Groups = ns_cluster_membership:server_groups(Snapshot),
                   case {find_group_by_prop(uuid, GroupUUID, Groups),
                         find_group_by_prop(name, Name, Groups)} of
                       {false, _} ->
                           {abort, not_found};
                       {_, {value, _}} ->
                           {abort, already_exists};
                       {{value, G}, false} ->
                           UpdatedGroup = lists:keyreplace(name, 1, G,
                                                           {name, Name}),
                           NewGroups =
                               lists:sort([UpdatedGroup | Groups -- [G]]),
                           {commit, [{set, server_groups, NewGroups}],
                            UpdatedGroup}
                   end
           end),
    case RV of
        {ok, _, UpdatedGroup} ->
            ns_audit:update_group(Req, UpdatedGroup),
            ok;
        Error ->
            Error
    end.

handle_server_group_delete(GroupUUID, Req) ->
    menelaus_util:assert_is_enterprise(),
    case do_group_delete(list_to_binary(GroupUUID), Req) of
        ok ->
            reply_json(Req, []);
        not_found ->
            reply_json(Req, [], 404);
        not_empty ->
            reply_json(Req, {struct, [{'_', <<"group is not empty">>}]}, 400)
    end.

do_group_delete(GroupUUID, Req) ->
    RV = chronicle_compat:transaction(
           [server_groups],
           fun (Snapshot) ->
                   Groups = ns_cluster_membership:server_groups(Snapshot),
                   case find_group_by_prop(uuid, GroupUUID, Groups) of
                       false ->
                           {abort, not_found};
                       {value, Victim} ->
                           case proplists:get_value(nodes, Victim) of
                               [_|_] ->
                                   {abort, not_empty};
                               [] ->
                                   {commit,
                                    [{set, server_groups, Groups -- [Victim]}],
                                    Victim}
                           end
                   end
           end),
    case RV of
        {ok, _, Victim} ->
            ns_audit:delete_group(Req, Victim),
            ok;
        Error ->
            Error
    end.
