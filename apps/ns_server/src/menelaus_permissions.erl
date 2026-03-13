%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc handling of rbac permissions

-module(menelaus_permissions).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("ns_test.hrl").
-endif.

-include("rbac.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-export([flatten_priv_object/1]).
-endif.

-export([privileges/1, children/1, bucket_permissions/4,
         check_collection_permissions/5]).

-record(priv_object, {privileges, children}).

privileges(#priv_object{privileges = Privileges}) -> Privileges.

children(#priv_object{children = Children}) -> Children.

priv_set(Privilege, Object, PrivTree) ->
    priv_set(Privilege, Object, PrivTree, sets:new()).

%% Note that privileges can be set at the bucket, scope or collection level.
%% If the same privilege is applied to a parent level, priv_set retains the
%% privilege in its children (as they *do not* inherit the parent's privileges).
%% For example, specifying:
%% {[{collection, [bucket1, S, C]}, data, docs], [sread]} and
%% {[{bucket, bucket1}, data, docs], [sread]}
%% sets SystemCollectionLookup at both the bucket and collection levels.
priv_set(Privilege, [Object], PrivTree, ParentPrivileges) ->
    maps:update_with(
      Object,
      fun (#priv_object{privileges = Privileges} = PrivObj) ->
              PrivObj#priv_object{privileges = sets:add_element(Privilege,
                                                                Privileges)}
      end,
      #priv_object{privileges = sets:add_element(Privilege, ParentPrivileges),
                   children = #{}},
      PrivTree);
priv_set(Privilege, [Object | Rest], PrivTree, ParentPrivileges) ->
    PrivTree#{Object =>
                  case maps:find(Object, PrivTree) of
                      {ok, #priv_object{privileges = NextParentPrivileges,
                                        children = Children} = Old} ->
                          Old#priv_object{
                            children = priv_set(Privilege, Rest, Children,
                                                NextParentPrivileges)};
                      error ->
                          #priv_object{privileges = ParentPrivileges,
                                       children = priv_set(Privilege, Rest, #{},
                                                           ParentPrivileges)}
                  end}.

priv_set_all(Privileges, PrivTree) ->
    sets:fold(
        fun (Key, Acc) ->
                maps:fold(fun (Object, _, Acc1) ->
                                  priv_set(Key, [Object], Acc1)
                          end, Acc, Acc)
        end, PrivTree, Privileges).

priv_unset(Privilege, Object, PrivTree) ->
    priv_unset(Privilege, Object, PrivTree, sets:new()).

priv_unset(Privilege, [Object], PrivTree, ParentPrivileges) ->
    maps:update_with(
      Object,
      fun (#priv_object{privileges = Privileges} = PrivObj) ->
              PrivObj#priv_object{privileges = sets:del_element(Privilege,
                                                                Privileges)}
      end,
      #priv_object{privileges = sets:del_element(Privilege, ParentPrivileges),
                   children = #{}},
      PrivTree);
priv_unset(Privilege, [Object | Rest], PrivTree, ParentPrivileges) ->
    PrivTree#{Object =>
                  case maps:find(Object, PrivTree) of
                      {ok, #priv_object{privileges = NextParentPrivileges,
                                        children = Children} = Old} ->
                          Old#priv_object{
                            children = priv_unset(Privilege, Rest, Children,
                                                  NextParentPrivileges)};
                      error ->
                          #priv_object{
                              privileges = ParentPrivileges,
                              children = priv_unset(Privilege, Rest, #{},
                                                    ParentPrivileges)}
                  end}.

bucket_permissions(BucketName, CompiledRoles, Permissions, Acc) ->
    lists:foldl(
      fun ({Permission, MappedPrivilege}, Acc1) ->
              case menelaus_roles:is_allowed(Permission, CompiledRoles) of
                  true ->
                      priv_set(MappedPrivilege, [BucketName], Acc1);
                  false ->
                      Acc1
              end
      end, Acc, Permissions).

%% Params is a collection param [bucket_name, scope_name, collection_name]
%% where bucket_name cannot be any.
collection_permissions({[{collection, Params} | ObjRest], Ops}, PrivTree,
                       CollectionPermissionsFun, PrivObjFun) ->
    ToCheck = menelaus_roles:strip_ids(?RBAC_COLLECTION_PARAMS, Params),
    ObjStripped = [{collection, ToCheck} | ObjRest],
    ToStore = PrivObjFun(Params),
    lists:foldl(
      fun ({{ObjToCheck, OpToCheck}, MappedPrivilege}, Acc1) ->
              ObjMatches = lists:sublist(ObjToCheck, length(ObjStripped)) =:=
                  ObjStripped,
              OpMatches = Ops =:= all orelse
                                        (Ops =/= none andalso
                                            lists:member(OpToCheck, Ops)),
              case {ObjMatches, OpMatches} of
                  {true, true} ->
                      priv_set(MappedPrivilege, ToStore, Acc1);
                  {true, false} ->
                      priv_unset(MappedPrivilege, ToStore, Acc1);
                  _ ->
                      Acc1
              end
      end, PrivTree, CollectionPermissionsFun(ToCheck)).

remove_trailing_any([any, any, any]) ->
    [];
remove_trailing_any([B, any, any]) ->
    [B];
remove_trailing_any([B, S, any]) ->
    [B, S];
remove_trailing_any([B, S, C]) ->
    [B, S, C].

get_params([] = Rest) ->
    {[any, any, any], Rest};
get_params([{bucket, B} | Rest]) ->
    {[B, any, any], Rest};
get_params([{scope, [B, S]} | Rest]) ->
    {[B, S, any], Rest};
get_params([{collection, [B, S, C]} | Rest]) ->
    {[B, S, C], Rest};
get_params([O | _] = Rest) when is_atom(O) ->
    %% We don't want to silently fail when there are invalid parameterisations,
    %% so we need the first vertex to be an atom if it's not a known parameter
    {[any, any, any], Rest}.

expand_collection_params({PermissionObject, Operations}, Snapshot) ->
    {CollectionParams, ObjRest} = get_params(PermissionObject),
    %% memcached permissions are specified for a [bucket_name, scope uid,
    %% collection uid]. If either scope or collection uid are not specified, the
    %% permissions apply to all otherwise unspecified scopes and collections in
    %% the bucket (or all collections in the scope). These do not override the
    %% set of permissions specified at a deeper level.
    %%
    %% CollectionParams is of the form [bucket_name, scope_name,
    %% collection_name] where (bucket|scope|collection)_name may be "any".
    %%
    %% A wildcard "any" that is not trailing must be expanded to enumerate all
    %% buckets (scopes) to which it applies.
    %% e.g. [any, "_system", "_mobile"] must be expanded to:
    %% [["b1", "_system", "_mobile"], ["b2", "_system", "_mobile"]...]
    %% for every bucket in the system.
    %%
    %% Why?
    %% (1) The uids of a scope (like "_system") or a collection (like "_mobile")
    %%     are not constant across all buckets or collections.
    %% (2) MB-61241: memcached doesn't allow specifying a permission with a
    %%     wildcard * bucket_name and a specific scope or collection uid.
    %%
    %% Why do we need this now?
    %% For all parameterized roles (except mobile_sync_gateway), setting a * for
    %% bucket automatically sets * for scopes and collections and setting a
    %% wildcard * for scope sets * for collection. They exclusively use trailing
    %% wildcards, so we omit the scope and/or collection uids when set to *.
    %%
    %% mobile_sync_gateway introduces a bucket-parameterized permission:
    %% [{bucket, bucket_name}, "_system", "_mobile"] where a wildcard * can be
    %% specified by the user for bucket_name. [any, "_system", _mobile"] is the
    %% only case that has a non-trailing wildcard which needs to be expanded.
    Params = remove_trailing_any(CollectionParams),
    case lists:member(any, Params) of
        true ->
            Expanded = split_and_expand_collection_params(Params, Snapshot),
            lists:map(fun (NewParams) ->
                              {{misc:align_list(NewParams,
                                                length(?RBAC_COLLECTION_PARAMS),
                                                any),
                                ObjRest}, Operations}
                      end, Expanded);
        false -> [{{CollectionParams, ObjRest}, Operations}]
    end.

split_and_expand_collection_params(CollectionParams, Snapshot) ->
    split_and_expand_collection_params(bucket, CollectionParams, Snapshot).
split_and_expand_collection_params(bucket, CollectionParams, Snapshot) ->
    Buckets =
        case hd(CollectionParams) of
            any -> ns_bucket:get_bucket_names(Snapshot);
            Bucket -> [Bucket]
        end,
    Rest = tl(CollectionParams),
    [[B | R] || B <- Buckets,
                (Manifest = collections:get_manifest(B, Snapshot))
                    =/= undefined,
                R <- split_and_expand_collection_params(scope, Rest, Manifest)];

split_and_expand_collection_params(scope, RemainingParams, Manifest) ->
    Scopes =
        case hd(RemainingParams) of
            any -> [Scope || {Scope, _} <- collections:get_scopes(Manifest)];
            Scope -> [Scope]
        end,
    Rest = tl(RemainingParams),
    [[Scope | Rest] || Scope <- Scopes].

check_collection_permissions(Snapshot, CompiledRoles, BucketPermissionsFun,
                             CollectionPermissionsFun, PrivObjFun) ->
    lists:foldl(
      fun (CompiledRole, Acc) ->
              privileges_merge(
                Acc,
                check_permissions_for_role(Snapshot, CompiledRole,
                                           BucketPermissionsFun,
                                           CollectionPermissionsFun,
                                           PrivObjFun))
      end,
      #{}, CompiledRoles).

check_permissions_for_role(Snapshot, CompiledRole, BucketPermissionsFun,
                           CollectionPermissionsFun, PrivObjFun) ->
    ExpandedPerms = lists:reverse(
                      lists:flatmap(
                        expand_collection_params(_, Snapshot), CompiledRole)),
    FinalPartialRoles =
        %% TODO: memcached allows specifying a wildcard * for bucket_name but we
        %% don't use it. collection_permissions() doesn't handle bucket "any".
        lists:flatmap(
          fun({{[any, any, any], ObjRest}, Ops}) ->
                  [{{[B, any, any], ObjRest}, Ops} ||
                      B <- ns_bucket:get_bucket_names(Snapshot)];
             (Other) -> [Other]
          end, ExpandedPerms),

    %% Use compile_params to get uids of scopes/collections (required by
    %% memcached).
    Collections =
        lists:filtermap(
          fun({{Params, ObjRest}, Ops}) ->
                  %% TODO: The collection need not be found (and isn't checked
                  %% for during compilation of roles nor is it accounted for in
                  %% fixup_collection_params). So compile_params can return
                  %% false. Should probably check (and cache) these while
                  %% compiling roles.
                  case menelaus_roles:compile_params(?RBAC_COLLECTION_PARAMS,
                                                     Params, Snapshot) of
                      false ->
                          false;
                      NewParams ->
                          %% Convert back to parameterised object, now with
                          %% uids for comprehension by memcached
                          {true, {[{collection, NewParams} | ObjRest], Ops}}
                  end
          end, FinalPartialRoles),

    BucketPrivileges =
        lists:foldl(fun (Name, Acc) ->
                            bucket_permissions(Name, [CompiledRole],
                                               BucketPermissionsFun(Name), Acc)
                    end, #{}, ns_bucket:get_bucket_names(Snapshot)),
    CollectionPrivileges = lists:foldl(
                             collection_permissions(
                               _, _, CollectionPermissionsFun, PrivObjFun),
                             #{}, Collections),
    AllPrivileges =
        maps:merge_with(
          fun (_, #priv_object{privileges = Privileges1},
               #priv_object{privileges = Privileges2,
                            children = Children}) ->
                  %% We merge bucket privileges in without modifying the
                  %% scope/collection privileges since the BucketPrivileges
                  %% are those that only apply at the bucket level
                  #priv_object{privileges = sets:union(Privileges1,
                                                       Privileges2),
                               children = Children}
          end, BucketPrivileges, CollectionPrivileges),
    maps:filter(
      fun (_, #priv_object{privileges = Privileges, children = Children}) ->
              sets:size(Privileges) > 0 orelse map_size(Children) > 0
      end, AllPrivileges).

remove_redundant_privileges(Map, ParentPrivs) ->
    maps:filtermap(
      fun (_, #priv_object{privileges = Privileges, children = Children}) ->
              case remove_redundant_privileges(Children, Privileges) of
                  NewChildren when map_size(NewChildren) =:= 0 ->
                      case sets:is_equal(Privileges, ParentPrivs) of
                          true -> false;
                          false -> {true, #priv_object{privileges = Privileges,
                                                       children = NewChildren}}
                      end;
                  NewChildren ->
                      {true, #priv_object{privileges = Privileges,
                                          children = NewChildren}}
              end
      end, Map).

privileges_merge(Priv1, Priv2) ->
    privileges_merge(Priv1, Priv2, sets:new()).

privileges_merge(Priv1, Priv2, ParentPrivs) ->
    Merged =
        maps:merge_with(
          fun (_, #priv_object{privileges = Privileges1, children = Children1},
               #priv_object{privileges = Privileges2, children = Children2}) ->
                  Privs = sets:union(Privileges1, Privileges2),
                  case {map_size(Children1) =:= 0, map_size(Children2) =:= 0} of
                      {true, false} ->
                          Children3 = priv_set_all(Privileges1, Children2),
                          Children4 = remove_redundant_privileges(Children3,
                                                                  Privs),
                          #priv_object{privileges = Privs,
                                       children = Children4};
                      {false, true} ->
                          Children3 = priv_set_all(Privileges2, Children1),
                          Children4 = remove_redundant_privileges(Children3,
                                                                  Privs),
                          #priv_object{privileges = Privs,
                                       children = Children4};
                      _ ->
                          Children = privileges_merge(Children1, Children2,
                                                      Privs),
                          #priv_object{privileges = Privs,
                                       children = Children}
                  end
          end, Priv1, Priv2),
    maps:filter(
      fun (_, #priv_object{privileges = Privileges, children = Children})
            when map_size(Children) =:= 0 ->
              case sets:is_equal(Privileges, ParentPrivs) of
                  true -> false;
                  false -> true
              end;
          (_, _) ->
              true
      end, Merged).


-ifdef(TEST).
flatten_priv_object(Map) ->
    flatten_priv_object([], [], Map).

flatten_priv_object(Prefix, Acc, Map) ->
    maps:fold(
      fun (Key, #priv_object{privileges = Privileges,
                             children = Children}, Acc1) ->
              case sets:size(Privileges) =:= 0 andalso
                  map_size(Children) =:= 0 of
                  true ->
                      [{lists:reverse([Key | Prefix]), empty} | Acc1];
                  false ->
                      NewPrefix = [Key | Prefix],
                      NewPrefixReversed = lists:reverse(NewPrefix),
                      flatten_priv_object(
                        NewPrefix,
                        Acc1 ++
                            [{NewPrefixReversed, K} ||
                                K <- sets:to_list(Privileges)],
                        Children)
              end
      end, Acc, Map).


priv_is_granted(Privilege, Key, PrivTree) ->
    %% Check for whether Privilege is granted for Key in PrivTree, assuming it
    %% was not already granted at a higher level
    priv_is_granted(Privilege, Key, PrivTree, false).

%% Privilege is granted if the deepest matching object has that privilege
%% granted. If the object has undefined privilege, then it inherits the parent
%% privileges.
priv_is_granted(Privilege, [Object | Rest], PrivTree, GrantedByParent) ->
    case maps:find(Object, PrivTree) of
        {ok, #priv_object{privileges = Privileges, children = Children}} ->
            GrantedAtThisLevel = sets:is_element(Privilege, Privileges),
            case Rest of
                [] ->
                    %% If privileges are specified at this level, and the
                    %% object we are looking for is exactly at this level, then
                    %% the privilege is granted if it is granted at this level
                    GrantedAtThisLevel;
                _ ->
                    %% If privileges are specified at this level, but the object
                    %% we are looking for is more specific, then we check the
                    %% next level in case the object matches at a lower level
                    priv_is_granted(Privilege, Rest, Children,
                                    GrantedAtThisLevel)
            end;
        error ->
            %% If privileges aren't specified at the specific level, then the
            %% lowest level determines whether it is granted
            GrantedByParent
    end.

priv_object_test() ->
    PrivObject =
        functools:chain(
          #{},
          [priv_set(p3, [b1], _),
           priv_set(p1, [b1, s1, c1], _),
           priv_set(p2, [b1, s1, c1], _),
           priv_set(p3, [b1, s1, c1], _),
           priv_set(p2, [b1, s1, c2], _),
           priv_unset(p3, [b1, s1, c2], _),
           priv_set(p2, [b1, s2], _),
           priv_unset(p3, [b1, s2], _),
           priv_set(p2, [b1, s2, c3], _),
           priv_unset(p3, [b1, s2, c3], _),
           priv_set(p1, [b2], _)]),
    ?assert(priv_is_granted(p1, [b1, s1, c1], PrivObject)),
    ?assertNot(priv_is_granted(p1, [b1, s1], PrivObject)),
    ?assertNot(priv_is_granted(p1, [b1], PrivObject)),
    ?assert(priv_is_granted(p1, [b2, any, any], PrivObject)),
    ?assert(priv_is_granted(p1, [b2, any], PrivObject)),
    ?assert(priv_is_granted(p1, [b2], PrivObject)),
    ?assertNot(priv_is_granted(wrong, [b2], PrivObject)),
    ?assertNot(priv_is_granted(p1, [wrong], PrivObject)),
    ?assert(priv_is_granted(p3, [b1, s1], PrivObject)),
    ?assertListsEqual([{[b1, s1, c1], p1},
                       {[b2], p1},
                       {[b1, s1, c1], p2},
                       {[b1, s1, c2], p2},
                       {[b1, s2, c3], p2},
                       {[b1, s2], p2},
                       {[b1, s1, c1], p3},
                       {[b1], p3},
                       {[b1, s1], p3}], flatten_priv_object(PrivObject)).

fixup_collection_params_test() ->
    Manifest1 =
        [{uid,3},
         {scopes,[{"_default",
                   [{uid,0},{collections,[{"_default",[{uid,0}]}]}]},
                  {"def",
                   [{uid,8},{collections,[{"xyz",[{uid,10}]}]}]},
                  {"_system",
                   [{uid,9},
                    {collections,
                     [{"_query",[{uid,11}]},
                      {"_mobile",[{uid,12}]}]}]}]}],
    Manifest2 =
        [{uid,3},
         {scopes,[{"_default",
                   [{uid,0},{collections,[{"_default",[{uid,0}]}]}]},
                  {"ghi",
                   [{uid,7},{collections,[{"xyz",[{uid,10}]}]}]},
                  {"_system",
                   [{uid,8},
                    {collections,
                     [{"_query",[{uid,8}]},
                      {"_mobile",[{uid,9}]}]}]}]}],
    Snapshot =
        ns_bucket:toy_buckets(
          [{"test", [{uuid, <<"test_id">>}]},
           {"default", [{uuid, <<"default_id">>}, {collections, Manifest1}]},
           {"abc", [{uuid, <<"abc_id">>}, {collections, Manifest2}]}]),

    ?assertEqual([{{[any, any, any],
                    []}, op}],
                 expand_collection_params({[{bucket, any}],
                                           op}, Snapshot)),
    ?assertEqual([{{["default", any, any],
                    []}, op}],
                 expand_collection_params({[{bucket, "default"}],
                                           op}, Snapshot)),
    ?assertEqual([{{["default", "def", any],
                    []}, op}],
                 expand_collection_params({[{scope, ["default", "def"]}],
                                           op}, Snapshot)),
    ?assertEqual([{{["default", "_system", "_mobile"],
                    []}, op}],
                 expand_collection_params(
                   {[{collection, ["default", "_system", "_mobile"]}], op},
                   Snapshot)),

    Result1 = [{{["default", "_system", "_mobile"],
                 []}, op},
               {{["abc", "_system", "_mobile"],
                 []}, op}],
    ?assertEqual(Result1,
                 expand_collection_params({[{collection,
                                             [any, "_system", "_mobile"]}],
                                           op}, Snapshot)),

    Result2 = [{{["default", "_system", any],
                 []}, op},
               {{["abc", "_system", any],
                 []}, op}],
    ?assertEqual(Result2,
                 expand_collection_params(
                   {[{collection, [any, "_system", any]}], op}, Snapshot)),

    Result3 = [{{["default", "_default", "_mobile"],
                 []}, op},
               {{["default", "def", "_mobile"],
                 []}, op},
               {{["default", "_system", "_mobile"],
                 []}, op}],
    ?assertEqual(Result3,
                 expand_collection_params(
                   {[{collection, ["default", any, "_mobile"]}], op},
                   Snapshot)),

    %% Note the expansion populates all combinations, although _mobile doesn't
    %% exist in any scope other than "_system". Non-existent combinations are
    %% weeded out when compile_params returns false.
    Result4 = [{{["default", "_default", "_mobile"], []}, op},
               {{["default", "def", "_mobile"], []}, op},
               {{["default", "_system", "_mobile"], []}, op},
               {{["abc", "_default", "_mobile"], []}, op},
               {{["abc", "ghi", "_mobile"], []}, op},
               {{["abc", "_system", "_mobile"], []}, op}],
    ?assertEqual(expand_collection_params(
                   {[{collection, [any, any, "_mobile"]}], op},
                   Snapshot),
                 Result4).

privileges_merge_test() ->
    Set = fun (L) -> sets:from_list(L) end,
    Priv0 = #{},
    Priv1 = #{b => #priv_object{privileges = Set([x]),
                                children = #{}}},
    ?assertEqual(Priv1,
                 privileges_merge(Priv0, Priv1)),
    ?assertEqual(Priv1,
                 privileges_merge(Priv1, Priv0)),
    Priv2 =
        #{b => #priv_object{
                  privileges = Set([]),
                  children =
                      #{s => #priv_object{
                                privileges = Set([]),
                                children =
                                    #{c => #priv_object{
                                              privileges = Set([x]),
                                              children = #{}}}}}}},
    ?assertEqual(Priv1,
                 privileges_merge(Priv1, Priv2)),
    ?assertEqual(Priv1,
                 privileges_merge(Priv2, Priv1)),
    Priv3 = #{b => #priv_object{
                      privileges = Set([]),
                      children = #{
                                   s => #priv_object{
                                           privileges = Set([y]),
                                           children = #{}}}}},
    Priv4 =
        #{b => #priv_object{
                  privileges = Set([]),
                  children =
                      #{s => #priv_object{
                                privileges = Set([y]),
                                children =
                                    #{c => #priv_object{
                                              privileges = Set([x, y]),
                                              children = #{}}}}}}},
    ?assertEqual(Priv4,
                 privileges_merge(Priv3, Priv2)),
    ?assertEqual(Priv4,
                 privileges_merge(Priv2, Priv3)),
    Priv5 = #{b => #priv_object{
                      privileges = Set([x]),
                      children = #{s => #priv_object{
                                           privileges = Set([x, y]),
                                           children = #{}}}}},
    ?assertEqual(Priv5,
                 privileges_merge(Priv1, Priv3)).
-endif.
