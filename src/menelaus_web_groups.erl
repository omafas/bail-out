%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2018 Couchbase, Inc.
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
%% @doc REST API for managing server groups (racks)
-module(menelaus_web_groups).

-include("ns_common.hrl").

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
    Config = ns_config:get(),
    {value, Groups} = ns_config:search(Config, server_groups),
    LocalAddr = menelaus_util:local_addr(Req),
    CanIncludeOtpCookie = menelaus_auth:has_permission({[admin, internal], all}, Req),
    Fun = menelaus_web_node:build_nodes_info_fun(
            CanIncludeOtpCookie, normal, unstable, LocalAddr, Config),
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
    Config = ns_config:get(),
    Groups = ns_config:search(Config, server_groups, []),
    {value, Nodes} = ns_config:search(Config, nodes_wanted),
    V = integer_to_list(erlang:phash2(Groups)),
    case V =/= Rev of
        true ->
            reply_json(Req, [], 409);
        _ ->
            try parse_validate_groups_payload(JSON, Groups, Nodes) of
                ParsedGroups ->
                    ReplacementGroups = build_replacement_groups(Groups, ParsedGroups),
                    finish_handle_server_groups_put(Req, ReplacementGroups, Groups)
            catch throw:group_parse_error ->
                    reply_json(Req, <<"Bad input">>, 400);
                  throw:{group_parse_error, Parsed} ->
                    reply_json(Req, [<<"Bad input">>,
                                     [{struct, PL} || PL <- Parsed]], 400)
            end
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

finish_handle_server_groups_put(Req, ReplacementGroups, RawGroups) ->
    TXNRV = ns_config:run_txn(
              fun (Cfg, SetFn) ->
                      RebalancerPid = ns_config:search(Cfg, rebalancer_pid, undefined),
                      case RebalancerPid of
                          undefined ->
                              {value, NowRawGroups} = ns_config:search(Cfg, server_groups),
                              case NowRawGroups =:= RawGroups of
                                  true ->
                                      {commit, SetFn(server_groups, ReplacementGroups, Cfg)};
                                  _ ->
                                      {abort, retry}
                              end;
                          _ ->
                              {abort, {error, rebalance_running}}
                      end
              end),
    case TXNRV of
        {commit, _} ->
            [ns_audit:update_group(Req, Group) || Group <- ReplacementGroups],
            reply_json(Req, [], 200);
        {abort, {error, rebalance_running}} ->
            reply_json(Req, <<"Cannot update server group while rebalance is running">>, 503);
        retry_needed ->
            erlang:error(exceeded_retries);
        _ ->
            reply_json(Req, [], 409)
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
                {error, already_exists} ->
                    reply_json(Req, {struct, [{name, <<"already exists">>}]}, 400)
            end;
        {errors, Errors} ->
            reply_json(Req, {struct, Errors}, 400)
    end.

do_handle_server_groups_post(Name, Req) ->
    TXNRV = ns_config:run_txn(
              fun (Cfg, SetFn) ->
                      {value, ExistingGroups} = ns_config:search(Cfg, server_groups),
                      case [G || G <- ExistingGroups,
                                 proplists:get_value(name, G) =:= Name] of
                          [] ->
                              UUID = couch_uuids:random(),
                              true = is_binary(UUID),
                              AGroup = [{uuid, UUID},
                                        {name, Name},
                                        {nodes, []}],
                              NewGroups = lists:sort([AGroup | ExistingGroups]),
                              {commit, SetFn(server_groups, NewGroups, Cfg), AGroup};
                          _ ->
                              {abort, {error, already_exists}}
                      end
              end),
    case TXNRV of
        {commit, _, AGroup} ->
            ns_audit:add_group(Req, AGroup),
            ok;
        {abort, {error, already_exists} = Error} ->
            Error;
        retry_needed ->
            erlang:error(exceeded_retries)
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
                {error, not_found} ->
                    reply_json(Req, [], 404);
                {error, already_exists} ->
                    reply_json(Req, {struct, [{name, <<"already exists">>}]}, 400)
            end;
        {errors, Errors} ->
            reply_json(Req, {struct, Errors}, 400)
    end.

do_group_update(GroupUUID, Name, Req) ->
    TXNRV = ns_config:run_txn(
              fun (Cfg, SetFn) ->
                      {value, Groups} = ns_config:search(Cfg, server_groups),
                      MaybeCandidateGroup = [G || G <- Groups,
                                                  proplists:get_value(uuid, G) =:= GroupUUID],
                      OtherGroups = Groups -- MaybeCandidateGroup,
                      MaybeDuplicateName = [G || G <- Groups,
                                                 proplists:get_value(name, G) =:= Name],
                      case {MaybeCandidateGroup, MaybeDuplicateName} of
                          {[], _} ->
                              {abort, {error, not_found}};
                          {_, [_]} ->
                              {abort, {error, already_exists}};
                          {[_], []} ->
                              UpdatedGroup = lists:keyreplace(name, 1, hd(MaybeCandidateGroup), {name, Name}),
                              NewGroups = lists:sort([UpdatedGroup | OtherGroups]),
                              {commit, SetFn(server_groups, NewGroups, Cfg), UpdatedGroup}
                      end
              end),
    case TXNRV of
        {commit, _, UpdatedGroup} ->
            ns_audit:update_group(Req, UpdatedGroup),
            ok;
        {abort, Error} ->
            Error;
        retry_needed ->
            erlang:error(exceeded_retries)
    end.

handle_server_group_delete(GroupUUID, Req) ->
    menelaus_util:assert_is_enterprise(),
    case do_group_delete(list_to_binary(GroupUUID), Req) of
        ok ->
            reply_json(Req, []);
        {error, not_found} ->
            reply_json(Req, [], 404);
        {error, not_empty} ->
            reply_json(Req, {struct, [{'_', <<"group is not empty">>}]}, 400)
    end.

do_group_delete(GroupUUID, Req) ->
    TXNRV = ns_config:run_txn(
              fun (Cfg, SetFn) ->
                      {value, Groups} = ns_config:search(Cfg, server_groups),
                      MaybeG = [G || G <- Groups,
                                     proplists:get_value(uuid, G) =:= GroupUUID],
                      case MaybeG of
                          [] ->
                              {abort, {error, not_found}};
                          [Victim] ->
                              case proplists:get_value(nodes, Victim) of
                                  [_|_] ->
                                      {abort, {error, not_empty}};
                                  [] ->
                                      NewGroups = Groups -- MaybeG,
                                      {commit, SetFn(server_groups, NewGroups, Cfg), Victim}
                              end
                      end
              end),
    case TXNRV of
        {commit, _, Victim} ->
            ns_audit:delete_group(Req, Victim),
            ok;
        {abort, Error} ->
            Error;
        retry_needed ->
            erlang:error(exceeded_retries)
    end.
