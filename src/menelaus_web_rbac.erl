%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%% @doc rest api's for rbac support

-module(menelaus_web_rbac).

-include("cut.hrl").
-include("ns_common.hrl").
-include("pipes.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_saslauthd_auth_settings/1,
         handle_saslauthd_auth_settings_post/1,
         handle_get_roles/1,
         handle_get_users/2,
         handle_get_users/3,
         handle_get_user/3,
         handle_whoami/1,
         handle_put_user/3,
         handle_delete_user/3,
         handle_change_password/1,
         handle_reset_admin_password/1,
         handle_check_permissions_post/1,
         check_permissions_url_version/1,
         handle_check_permission_for_cbauth/1,
         forbidden_response/1,
         role_to_string/1,
         validate_cred/2,
         handle_get_password_policy/1,
         handle_post_password_policy/1,
         assert_no_users_upgrade/0,
         domain_to_atom/1,
         handle_put_group/2,
         handle_delete_group/2,
         handle_get_groups/2,
         handle_get_group/2,
         assert_groups_and_ldap_enabled/0,
         handle_get_profiles/1,
         handle_get_profile/2,
         handle_delete_profile/2,
         handle_put_profile/2,
         handle_lookup_ldap_user/2,
         gen_password/1,
         handle_get_uiroles/1
]).

-define(MIN_USERS_PAGE_SIZE, 2).
-define(MAX_USERS_PAGE_SIZE, 100).

-define(SECURITY_READ, {[admin, security, admin], read}).
-define(SECURITY_WRITE, {[admin, security, admin], write}).

-define(EXTERNAL_READ, {[admin, security, external], read}).
-define(EXTERNAL_WRITE, {[admin, security, external], write}).

-define(LOCAL_READ, {[admin, security, local], read}).
-define(LOCAL_WRITE, {[admin, security, local], write}).

assert_is_saslauthd_enabled() ->
    case cluster_compat_mode:is_saslauthd_enabled() of
        true ->
            ok;
        false ->
            menelaus_util:web_exception(
              400, "This http API endpoint is only supported in enterprise "
              "edition running on GNU/Linux")
    end.

handle_saslauthd_auth_settings(Req) ->
    assert_is_saslauthd_enabled(),

    menelaus_util:reply_json(Req, {saslauthd_auth:build_settings()}).

extract_user_list(undefined) ->
    asterisk;
extract_user_list(String) ->
    StringNoCR = [C || C <- String, C =/= $\r],
    Strings = string:tokens(StringNoCR, "\n"),
    [B || B <- [list_to_binary(string:trim(S)) || S <- Strings],
          B =/= <<>>].

parse_validate_saslauthd_settings(Params) ->
    EnabledR = case menelaus_util:parse_validate_boolean_field(
                      "enabled", enabled, Params) of
                   [] ->
                       [{error, enabled, <<"is missing">>}];
                   EnabledX -> EnabledX
               end,
    [AdminsParam, RoAdminsParam] =
        case EnabledR of
            [{ok, enabled, false}] ->
                ["", ""];
            _ ->
                [proplists:get_value(K, Params) || K <- ["admins", "roAdmins"]]
        end,
    Admins = extract_user_list(AdminsParam),
    RoAdmins = extract_user_list(RoAdminsParam),
    MaybeExtraFields =
        case proplists:get_keys(Params) -- ["enabled", "roAdmins", "admins"] of
            [] ->
                [];
            UnknownKeys ->
                Msg =
                    io_lib:format("failed to recognize the following fields ~s",
                                  [string:join(UnknownKeys, ", ")]),
                [{error, '_', iolist_to_binary(Msg)}]
        end,
    MaybeTwoAsterisks =
        case Admins =:= asterisk andalso RoAdmins =:= asterisk of
            true ->
                [{error, 'admins',
                  <<"at least one of admins or roAdmins needs to be given">>}];
            false ->
                []
        end,
    Everything = EnabledR ++ MaybeExtraFields ++ MaybeTwoAsterisks,
    case [{Field, Msg} || {error, Field, Msg} <- Everything] of
        [] ->
            [{ok, enabled, Enabled}] = EnabledR,
            {ok, [{enabled, Enabled},
                  {admins, Admins},
                  {roAdmins, RoAdmins}]};
        Errors ->
            {errors, Errors}
    end.

handle_saslauthd_auth_settings_post(Req) ->
    assert_is_saslauthd_enabled(),

    case parse_validate_saslauthd_settings(mochiweb_request:parse_post(Req)) of
        {ok, Props} ->
            saslauthd_auth:set_settings(Props),
            ns_audit:setup_saslauthd(Req, Props),
            handle_saslauthd_auth_settings(Req);
        {errors, Errors} ->
            menelaus_util:reply_json(Req, {Errors}, 400)
    end.

jsonify_param(any) ->
    <<"*">>;
jsonify_param(Value) ->
    list_to_binary(strip_id(Value)).

strip_id({P, _Id}) ->
    P;
strip_id(P) ->
    P.

strip_ids(Params) ->
    lists:map(fun strip_id/1, Params).

role_to_json(Name) when is_atom(Name) ->
    [{role, Name}];
role_to_json({Name, Params}) ->
    Definitions = menelaus_roles:get_definitions(public),
    [{role, Name} |
     [{Param, jsonify_param(Value)} ||
         {Param, Value} <-
             lists:zip(menelaus_roles:get_param_defs(Name, Definitions),
                       Params)]].

role_to_json(Role, Origins) ->
    role_to_json(Role) ++
    [{origins, [role_origin_to_json(O) || O <- Origins]}
        || Origins =/= []].

role_origin_to_json(user) ->
    {[{type, user}]};
role_origin_to_json(O) ->
    {[{type, group}, {name, list_to_binary(O)}]}.

get_roles_by_permission(Permission, Snapshot) ->
    pipes:run(
      menelaus_roles:produce_roles_by_permission(Permission, Snapshot),
      pipes:collect()).

maybe_remove_security_roles(Req, Snapshot, Roles) ->
    Roles --
        case menelaus_auth:has_permission(?SECURITY_READ, Req) of
            true ->
                [];
            false ->
                menelaus_roles:get_security_roles(Snapshot)
        end.

handle_get_roles(Req) ->
    Snapshot = ns_bucket:get_snapshot(),
    validator:handle(
      fun (Values) ->
              Permission = proplists:get_value(permission, Values),
              Roles = maybe_remove_security_roles(
                        Req, Snapshot,
                        get_roles_by_permission(Permission, Snapshot)),
              Json =
                  [{role_to_json(Role) ++ jsonify_props(Props)} ||
                      {Role, Props} <- Roles],
              menelaus_util:reply_json(Req, Json)
      end, Req, qs, get_users_or_roles_validators()).

jsonify_props(Props) ->
    [{name, proplists:get_value(name, Props)},
     {desc, proplists:get_value(desc, Props)}].

user_to_json({Id, Domain}, Props) ->
    Is65 = cluster_compat_mode:is_cluster_65(),
    RolesJson = user_roles_to_json(Props, Is65),
    Name = proplists:get_value(name, Props),
    Groups = proplists:get_value(groups, Props),
    ExtGroups = proplists:get_value(external_groups, Props),
    Passwordless = proplists:get_value(passwordless, Props),
    PassChangeTime = format_password_change_time(
                       proplists:get_value(password_change_timestamp, Props)),

    {[{id, list_to_binary(Id)},
      {domain, Domain},
      {roles, RolesJson}] ++
     [{groups, [list_to_binary(G) || G <- Groups]} || Groups =/= undefined,
                                                      Is65] ++
     [{external_groups, [list_to_binary(G) || G <- ExtGroups]}
          || ExtGroups =/= undefined, Is65] ++
     [{name, list_to_binary(Name)} || Name =/= undefined] ++
     [{passwordless, Passwordless} || Passwordless == true] ++
     [{password_change_date, PassChangeTime} || PassChangeTime =/= undefined]}.

user_roles_to_json(Props, true) ->
    UserRoles = proplists:get_value(user_roles, Props, []),
    GroupRoles = proplists:get_value(group_roles, Props, []),
    AddOrigin =
        fun (Origin, List, AccMap) ->
                lists:foldl(
                  fun (R, Acc) ->
                          maps:put(R, [Origin|maps:get(R, Acc, [])], Acc)
                  end, AccMap, List)
        end,
    Map = lists:foldl(
             fun ({G, R}, Acc) ->
                AddOrigin(G, R, Acc)
             end, #{}, [{user, UserRoles} | GroupRoles]),
    maps:fold(
       fun (Role, Origins, Acc) ->
           [{role_to_json(Role, Origins)}|Acc]
       end, [], Map);
user_roles_to_json(Props, false) ->
    UserRoles = proplists:get_value(user_roles, Props, []),
    [{role_to_json(Role)} || Role <- UserRoles].

format_password_change_time(undefined) -> undefined;
format_password_change_time(TS) ->
    Local = calendar:now_to_local_time(misc:time_to_timestamp(TS, millisecond)),
    menelaus_util:format_server_time(Local).

handle_get_users(Path, Req) ->
    handle_get_users_with_domain(Req, '_', Path).

get_users_or_roles_validators() ->
    [validate_permission(permission, _)].

get_users_page_validators(DomainAtom, HasStartFrom) ->
    [validator:integer(pageSize, ?MIN_USERS_PAGE_SIZE, ?MAX_USERS_PAGE_SIZE, _),
     validator:touch(startFrom, _),
     validator:one_of(sortBy, ["id", "name", "domain",
                               "password_change_timestamp"], _),
     validator:convert(sortBy, fun list_to_atom/1, _),
     validator:one_of(order, ["asc", "desc"], _),
     validator:touch(substr, _),
     validator:convert(order, fun list_to_atom/1, _)] ++
        case HasStartFrom of
            false ->
                [];
            true ->
                case DomainAtom of
                    '_' ->
                        [validator:required(startFromDomain, _),
                         validator:one_of(startFromDomain, known_domains(), _),
                         validator:convert(startFromDomain, fun list_to_atom/1,
                                           _)];
                    _ ->
                        [validator:prohibited(startFromDomain, _),
                         validator:return_value(startFromDomain, DomainAtom, _)]
                end
        end ++ get_users_or_roles_validators().

validate_permission(Name, State) ->
    validator:validate(
      fun (RawPermission) ->
              case parse_permission(RawPermission) of
                  error ->
                      {error, "Malformed permission"};
                  Permission ->
                      {value, Permission}
              end
      end, Name, State).

handle_get_users(Path, Domain, Req) ->
    case domain_to_atom(Domain) of
        unknown ->
            menelaus_util:reply_json(Req, <<"Unknown user domain.">>, 404);
        DomainAtom ->
            handle_get_users_with_domain(Req, DomainAtom, Path)
    end.

get_roles_for_users_filtering(undefined) ->
    all;
get_roles_for_users_filtering(Permission) ->
    get_roles_by_permission(Permission, ns_bucket:get_snapshot()).

handle_get_users_with_domain(Req, DomainAtom, Path) ->
    Query = mochiweb_request:parse_qs(Req),
    case lists:keyfind("pageSize", 1, Query) of
        false ->
            validator:handle(
              handle_get_all_users(Req, {'_', DomainAtom}, _), Req, Query,
              get_users_or_roles_validators());
        _ ->
            HasStartFrom = lists:keyfind("startFrom", 1, Query) =/= false,
            validator:handle(
              handle_get_users_page(Req, DomainAtom, Path, _),
              Req, Query, get_users_page_validators(DomainAtom, HasStartFrom))
    end.

ldap_ref_filter(Req) ->
    case menelaus_auth:has_permission(?EXTERNAL_READ, Req) of
        true ->
            pipes:filter(fun (_) -> true end);
        false ->
            pipes:filter(
              fun ({{group, _Id}, Props}) ->
                      menelaus_users:is_empty_ldap_group_ref(
                        proplists:get_value(ldap_group_ref, Props))
              end)
    end.

security_filter(Req) ->
    case menelaus_auth:has_permission(?SECURITY_READ, Req) of
        true ->
            pipes:filter(fun (_) -> true end);
        false ->
            SecurityRoles = get_security_roles(),
            pipes:filterfold(
              fun (User, Cache) ->
                  {Res, NewCache} = has_role(User, SecurityRoles, Cache),
                  {not Res, NewCache}
              end, #{})
    end.

get_domain_access_permission(read, Domain) ->
    case Domain of
        admin -> ?SECURITY_READ;
        local -> ?LOCAL_READ;
        external -> ?EXTERNAL_READ
    end;
get_domain_access_permission(write, Domain) ->
    case Domain of
        admin -> ?SECURITY_WRITE;
        local -> ?LOCAL_WRITE;
        external -> ?EXTERNAL_WRITE
    end.

domain_filter(Domain, Req) ->
    Permission = get_domain_access_permission(read, Domain),
    case menelaus_auth:has_permission(Permission, Req) of
        true ->
            pipes:filter(fun (_) -> true end);
        false ->
            pipes:filter(
              fun ({{_, {_, D}}, _}) when D =:= Domain ->
                      false;
                  (_) ->
                      true
              end)
    end.

substr_filter(undefined, _) -> pipes:filter(fun (_) -> true end);
substr_filter(Substr, PropsToCheck) ->
    LowerSubstr = string:to_lower(Substr),
    CheckProps =
        fun (Props) ->
            lists:any(
                fun (P) ->
                    case proplists:get_value(P, Props) of
                        undefined -> false;
                        V when is_list(V) ->
                            string:str(string:to_lower(V), LowerSubstr) > 0
                    end
                end, PropsToCheck)
        end,
    pipes:filter(
      fun ({{user, {Id, _}}, Props}) ->
              (string:str(string:to_lower(Id), LowerSubstr) > 0) orelse
                  CheckProps(Props);
          ({{group, Id}, Props}) ->
              (string:str(string:to_lower(Id), LowerSubstr) > 0) orelse
                  CheckProps(Props)
      end).

handle_get_all_users(Req, Pattern, Params) ->
    Roles = get_roles_for_users_filtering(
              proplists:get_value(permission, Params)),
    pipes:run(menelaus_users:select_users(Pattern),
              [filter_by_roles(Roles),
               security_filter(Req),
               domain_filter(local, Req),
               domain_filter(external, Req),
               jsonify_users(),
               sjson:encode_extended_json([{compact, true},
                                           {strict, false}]),
               pipes:simple_buffer(2048)],
              menelaus_util:send_chunked(
                Req, 200, [{"Content-Type", "application/json"}])).

handle_lookup_ldap_user(Name, Req) ->
    case ldap_util:get_setting(authentication_enabled) of
        true ->
            case ldap_auth_cache:lookup_user(Name) of
                {ok, _} ->
                    Identity = {Name, external},
                    Exists = menelaus_users:user_exists(Identity),
                    {JSONProps} = get_user_json(Identity),
                    Res = {[{recordExists, Exists} | JSONProps]},
                    menelaus_util:reply_json(Req, Res);
                {error, Reason} ->
                    Msg = iolist_to_binary(ldap_auth:format_error(Reason)),
                    menelaus_util:reply_json(Req, Msg, 404)
            end;
        false ->
            menelaus_util:reply_json(Req, <<"LDAP is disabled.">>, 404)
    end.

handle_get_user(Domain, UserId, Req) ->
    case domain_to_atom(Domain) of
        unknown ->
            menelaus_util:reply_json(Req, <<"Unknown user domain.">>, 404);
        DomainAtom ->
            Identity = {UserId, DomainAtom},
            case menelaus_users:user_exists(Identity) of
                false ->
                    menelaus_util:reply_json(Req, <<"Unknown user.">>, 404);
                true ->
                    verify_security_roles_access(
                      Req, ?SECURITY_READ, menelaus_users:get_roles(Identity)),
                    menelaus_util:reply_json(Req, get_user_json(Identity))
            end
    end.

filter_by_roles(all) ->
    pipes:filter(fun (_) -> true end);
filter_by_roles(Roles) ->
    RoleNames = [Name || {Name, _} <- Roles],
    pipes:filterfold(?cut(has_role(_1, RoleNames, _2)), #{}).

has_role({_, Props}, Roles, Cache) ->
    UserRoles = proplists:get_value(user_roles, Props, []) ++
                    proplists:get_value(roles, Props, []),
    case overlap(UserRoles, Roles) of
        true -> {true, Cache};
        false ->
            {LocalGroups, ExtGroups} =
                proplists:get_value(dirty_groups, Props, {[], []}),
            has_role_in_groups(LocalGroups ++ ExtGroups, Roles, Cache)
    end.

has_role_in_groups([], _, Cache) -> {false, Cache};
has_role_in_groups([G|T], Roles, Cache) ->
    case maps:find(G, Cache) of
        {ok, true} -> {true, Cache};
        {ok, false} -> has_role_in_groups(T, Roles, Cache);
        error ->
            GroupRoles = menelaus_users:get_group_roles(G),
            case overlap(GroupRoles, Roles) of
                true ->
                    {true, Cache#{G => true}};
                false ->
                    has_role_in_groups(T, Roles, Cache#{G => false})
            end
    end.

jsonify_users() ->
    ?make_transducer(
       begin
           ?yield(array_start),
           pipes:foreach(
             ?producer(),
             fun ({{user, Identity}, Props}) ->
                     ?yield({json, user_to_json(Identity, Props)})
             end),
           ?yield(array_end)
       end).

-record(skew, {skew, size, less_fun, filter, skipped = 0}).

skew_compare(El1, El2, #skew{less_fun = LessFun}) -> LessFun(El1, El2).

add_to_skew(_El, undefined) ->
    undefined;
add_to_skew(El, #skew{skew = CouchSkew,
                      size = Size,
                      filter = Filter,
                      less_fun = LessFun,
                      skipped = Skipped} = Skew) ->
    case Filter(El, LessFun) of
        false ->
            Skew#skew{skipped = Skipped + 1};
        true ->
            CouchSkew1 = couch_skew:in(El, LessFun, CouchSkew),
            case couch_skew:size(CouchSkew1) > Size of
                true ->
                    {_, CouchSkew2} = couch_skew:out(LessFun, CouchSkew1),
                    Skew#skew{skew = CouchSkew2};
                false ->
                    Skew#skew{skew = CouchSkew1}
            end
    end.

skew_to_list(#skew{skew = CouchSkew,
                   less_fun = LessFun}) ->
    skew_to_list(CouchSkew, LessFun, []).

skew_to_list(CouchSkew, LessFun, Acc) ->
    case couch_skew:size(CouchSkew) of
        0 ->
            Acc;
        _ ->
            {El, NewSkew} = couch_skew:out(LessFun, CouchSkew),
            skew_to_list(NewSkew, LessFun, [El | Acc])
    end.

skew_size(#skew{skew = CouchSkew}) ->
    couch_skew:size(CouchSkew).

skew_out(#skew{skew = CouchSkew, less_fun = LessFun} = Skew) ->
    {El, NewCouchSkew} = couch_skew:out(LessFun, CouchSkew),
    {El, Skew#skew{skew = NewCouchSkew}}.

skew_min(undefined) ->
    undefined;
skew_min(#skew{skew = CouchSkew}) ->
    case couch_skew:size(CouchSkew) of
        0 ->
            undefined;
        _ ->
            couch_skew:min(CouchSkew)
    end.

skew_skipped(#skew{skipped = Skipped}) ->
    Skipped.

create_skews(Start, PageSize, SortProp, Order) ->

    LessFun =
        case {SortProp, Order} of
            {id, asc} -> fun ({A, _}, {B, _}) -> A >= B end;
            {id, desc} -> fun ({A, _}, {B, _}) -> B >= A end;
            {domain, asc} ->
                fun ({{N1, D1}, _}, {{N2, D2}, _}) -> {D1, N1} >= {D2, N2} end;
            {domain, desc} ->
                fun ({{N1, D1}, _}, {{N2, D2}, _}) -> {D2, N2} >= {D1, N1} end;
            {P, O} ->
                fun ({AId, AProps}, {BId, BProps}) ->
                        %% compare props first
                        %% if they are equal compare ids
                        A = {proplists:get_value(P, AProps), AId},
                        B = {proplists:get_value(P, BProps), BId},
                        case O of
                            asc -> A >= B;
                            desc -> B >= A
                        end
                end
        end,

    SkewThis =
        #skew{
           skew = couch_skew:new(),
           size = PageSize + 1,
           less_fun = LessFun,
           filter = fun (El, Less) ->
                            Start =:= undefined orelse Less(El, Start)
                    end},
    SkewPrev =
        case Start of
            undefined ->
                undefined;
            _ ->
                #skew{
                   skew = couch_skew:new(),
                   size = PageSize,
                   less_fun = fun (A, B) -> not LessFun(A, B) end,
                   filter = fun (El, Less) ->
                                    Less(El, Start)
                            end}
        end,
    SkewLast =
        #skew{
           skew = couch_skew:new(),
           size = PageSize,
           less_fun = fun (A, B) -> not LessFun(A, B) end,
           filter = fun (_El, _LessFun) ->
                            true
                    end},
    [SkewPrev, SkewThis, SkewLast].

add_to_skews(El, Skews) ->
    [add_to_skew(El, Skew) || Skew <- Skews].

build_group_links(Links, Path, Params) ->
    {[{LinkName, build_pager_link(Path, StartFrom, Params)}
         || {LinkName, StartFrom} <- Links]}.

build_user_links(Links, NeedDomain, Path, Params) ->
    Json = lists:map(
             fun ({LinkName, noparams = UName}) ->
                     {LinkName, build_pager_link(Path, UName, Params)};
                 ({LinkName, {UName, Domain}}) ->
                     DomainParams = [{startFromDomain, Domain} || NeedDomain],
                     {LinkName, build_pager_link(Path, UName,
                                                 Params ++ DomainParams)}
             end, Links),
    {Json}.

build_pager_link(Path, StartObj, ExtraParams) ->
    PaginatorParams = format_paginator_params(StartObj),
    Params = mochiweb_util:urlencode(ExtraParams ++ PaginatorParams),
    iolist_to_binary(io_lib:format("/~s?~s", [Path, Params])).

format_paginator_params(noparams) -> [];
format_paginator_params(ObjName) -> [{startFrom, ObjName}].

seed_links(Pairs) ->
    [{Name, Object} || {Name, Object} <- Pairs, Object =/= undefined].

page_data_from_skews([SkewPrev, SkewThis, SkewLast], PageSize) ->
    {Objects, NextObj} =
        case skew_size(SkewThis) of
            Size when Size =:= PageSize + 1 ->
                {N, NewSkew} = skew_out(SkewThis),
                {skew_to_list(NewSkew), N};
            _ ->
                {skew_to_list(SkewThis), undefined}
        end,
    {First, Prev} = case skew_min(SkewPrev) of
                        undefined ->
                            {undefined, undefined};
                        {P, _} ->
                            {noparams, P}
                    end,
    {Last, Next} =
        case NextObj of
            undefined ->
                {undefined, undefined};
            _ ->
                {L, _} = LastObj = skew_min(SkewLast),
                case skew_compare(LastObj, NextObj, SkewLast) of
                    false -> {L, element(1, NextObj)};
                    true -> {L, L}
                end
        end,
    {Objects,
     skew_skipped(SkewThis),
     seed_links([{first, First}, {prev, Prev},
                 {next, Next}, {last, Last}])}.

handle_get_users_page(Req, DomainAtom, Path, Values) ->
    SortAndFilteringProps = [user_roles, dirty_groups, name,
                             password_change_timestamp],
    Start =
        case proplists:get_value(startFrom, Values) of
            undefined ->
                undefined;
            U ->
                Id = {U, proplists:get_value(startFromDomain, Values)},
                {Id, menelaus_users:get_user_props(Id, SortAndFilteringProps)}
        end,
    PageSize = proplists:get_value(pageSize, Values),
    Permission = proplists:get_value(permission, Values),
    Roles = get_roles_for_users_filtering(Permission),
    Order = proplists:get_value(order, Values, asc),
    Sort = proplists:get_value(sortBy, Values, id),
    Substr = proplists:get_value(substr, Values, undefined),

    {PageSkews, Total} =
        pipes:run(menelaus_users:select_users({'_', DomainAtom},
                                              SortAndFilteringProps),
                  [filter_by_roles(Roles),
                   security_filter(Req),
                   domain_filter(local, Req),
                   domain_filter(external, Req),
                   substr_filter(Substr, [name])],
                  ?make_consumer(
                     pipes:fold(
                       ?producer(),
                       fun ({{user, Identity}, Props}, {Skews, T}) ->
                               {add_to_skews({Identity, Props}, Skews), T + 1}
                       end, {create_skews(Start, PageSize, Sort, Order), 0}))),

    UserJson = fun ({Identity, _}) ->
                       Props = menelaus_users:get_user_props(Identity),
                       user_to_json(Identity, Props)
               end,

    {Users, Skipped, Links} = page_data_from_skews(PageSkews, PageSize),
    UsersJson = [UserJson(O) || O <- Users],
    LinksParams = [{permission, format_permission(Permission)}
                        || Permission =/= undefined] ++
                  [{substr, Substr} || Substr =/= undefined] ++
                  [{pageSize, PageSize},
                   {sortBy, Sort},
                   {order, Order}],
    LinksJson = build_user_links(Links, DomainAtom == '_', Path, LinksParams),
    Json = {[{total, Total},
             {links, LinksJson},
             {skipped, Skipped},
             {users, UsersJson}]},
    menelaus_util:reply_json(Req, Json).

handle_whoami(Req) ->
    Identity = menelaus_auth:get_identity(Req),
    Props = menelaus_users:get_user_props(Identity,
                                          [name, passwordless,
                                           password_change_timestamp]),
    {JSON} = user_to_json(Identity, Props),
    Roles = menelaus_roles:get_roles(Identity),
    RolesJSON = [{roles, [{role_to_json(R)} || R <- Roles]}],
    menelaus_util:reply_json(Req, {misc:update_proplist(JSON, RolesJSON)}).

get_user_json(Identity) ->
    user_to_json(Identity, menelaus_users:get_user_props(Identity)).

parse_until(Str, Delimeters) ->
    lists:splitwith(fun (Char) ->
                            not lists:member(Char, Delimeters)
                    end, Str).

role_to_atom(Role) ->
    list_to_existing_atom(string:to_lower(Role)).

get_num_params(Role, Definitions) ->
    case menelaus_roles:get_param_defs(Role, Definitions) of
        not_found ->
            not_found;
        Defs ->
            length(Defs)
    end.

adjust_role(Role, Params, Definitions) ->
    RoleAtom = role_to_atom(Role),
    AdjustedParams =
        case get_num_params(RoleAtom, Definitions) of
            I when is_integer(I),
                   I >= length(Params) ->
                misc:align_list(Params, I, any);
            _ ->
                %% this will be handled later
                %% in validate_roles
                Params
        end,
    {RoleAtom, AdjustedParams}.

parse_role(RoleRaw, Definitions) ->
    try
        case parse_until(RoleRaw, "[") of
            {Role, []} ->
                role_to_atom(Role);
            {Role, "[*]"} ->
                adjust_role(Role, [any], Definitions);
            {Role, [$[ | ParamAndBracket]} ->
                case parse_until(ParamAndBracket, "]") of
                    {Param, "]"} when Param =/= [] ->
                        adjust_role(Role, string:split(Param, ":", all),
                                    Definitions);
                    _ ->
                        {error, RoleRaw}
                end
        end
    catch error:badarg ->
            {error, RoleRaw}
    end.

parse_roles(undefined) ->
    [];
parse_roles(RolesStr) ->
    Definitions = menelaus_roles:get_definitions(public),
    RolesRaw = string:tokens(RolesStr, ","),
    [parse_role(string:trim(RoleRaw), Definitions) || RoleRaw <- RolesRaw].

params_to_string([any]) ->
    "*";
params_to_string([any | Rest]) ->
    params_to_string(Rest);
params_to_string(Params) ->
    lists:flatten(lists:join(":", strip_ids(lists:reverse(Params)))).

role_to_string(Role) when is_atom(Role) ->
    atom_to_list(Role);
role_to_string({Role, Params}) ->
    lists:flatten(io_lib:format(
                    "~p[~s]",
                    [Role, params_to_string(lists:reverse(Params))])).

known_domains() ->
    ["local", "external"].

domain_to_atom(Domain) ->
    case lists:member(Domain, known_domains()) of
        true ->
            list_to_atom(Domain);
        false ->
            unknown
    end.

verify_length([P, Len]) ->
    length(P) >= Len.

verify_control_chars(P) ->
    lists:all(
      fun (C) ->
              C > 31 andalso C =/= 127
      end, P).

verify_utf8(P) ->
    couch_util:validate_utf8(P).

verify_lowercase(P) ->
    string:to_upper(P) =/= P.

verify_uppercase(P) ->
    string:to_lower(P) =/= P.

verify_digits(P) ->
    lists:any(
      fun (C) ->
              C > 47 andalso C < 58
      end, P).

password_special_characters() ->
    "@%+\\/'\"!#$^?:,(){}[]~`-_".

verify_special(P) ->
    lists:any(
      fun (C) ->
              lists:member(C, password_special_characters())
      end, P).

get_verifier(uppercase, P) ->
    {fun verify_uppercase/1, P,
     <<"The password must contain at least one uppercase letter">>};
get_verifier(lowercase, P) ->
    {fun verify_lowercase/1, P,
     <<"The password must contain at least one lowercase letter">>};
get_verifier(digits, P) ->
    {fun verify_digits/1, P,
     <<"The password must contain at least one digit">>};
get_verifier(special, P) ->
    {fun verify_special/1, P,
     list_to_binary(
       "The password must contain at least one of the following characters: " ++
           password_special_characters())}.

execute_verifiers([]) ->
    true;
execute_verifiers([{Fun, Arg, Error} | Rest]) ->
    case Fun(Arg) of
        true ->
            execute_verifiers(Rest);
        false ->
            Error
    end.

get_password_policy() ->
    {value, Policy} = ns_config:search(password_policy),
    MinLength = proplists:get_value(min_length, Policy),
    true = (MinLength =/= undefined),
    MustPresent = proplists:get_value(must_present, Policy),
    true = (MustPresent =/= undefined),
    {MinLength, MustPresent}.

validate_cred(undefined, _) -> <<"Field must be given">>;
validate_cred(P, password) ->
    is_valid_password(P, get_password_policy());
validate_cred(Username, username) ->
    validate_id(Username, <<"Username">>).

validate_id([], Fieldname) ->
    <<Fieldname/binary, " must not be empty">>;
validate_id(Id, Fieldname) when length(Id) > 128 ->
    <<Fieldname/binary, " may not exceed 128 characters">>;
validate_id("@" ++ _, Fieldname) ->
    <<Fieldname/binary, " cannot start with '@'">>;
validate_id(Id, Fieldname) ->
    V = lists:all(
          fun (C) ->
                  C > 32 andalso C =/= 127 andalso
                      not lists:member(C, "()<>,;:\\\"/[]?={}")
          end, Id)
        andalso couch_util:validate_utf8(Id),

    V orelse
        <<Fieldname/binary, " must not contain spaces, control or any of "
          "()<>,;:\\\"/[]?={} characters and must be valid utf8">>.

is_valid_password(P, {MinLength, MustPresent}) ->
    LengthError = io_lib:format(
                    "The password must be at least ~p characters long.",
                    [MinLength]),

    Verifiers =
        [{fun verify_length/1, [P, MinLength], list_to_binary(LengthError)},
         {fun verify_utf8/1, P, <<"The password must be valid utf8">>},
         {fun verify_control_chars/1, P,
          <<"The password must not contain control characters">>}] ++
        [get_verifier(V, P) || V <- MustPresent],

    execute_verifiers(Verifiers).

handle_put_user(Domain, UserId, Req) ->
    assert_no_users_upgrade(),

    case validate_cred(UserId, username) of
        true ->
            case domain_to_atom(Domain) of
                unknown ->
                    menelaus_util:reply_json(Req, <<"Unknown user domain.">>,
                                             404);
                external = T ->
                    menelaus_util:assert_is_enterprise(),
                    handle_put_user_with_identity({UserId, T}, Req);
                local = T ->
                    handle_put_user_with_identity({UserId, T}, Req)
            end;
        Error ->
            menelaus_util:reply_global_error(Req, Error)
    end.

validate_password(State) ->
    validator:validate(
      fun (P) ->
              case validate_cred(P, password) of
                  true ->
                      ok;
                  Error ->
                      {error, Error}
              end
      end, password, State).

put_user_validators(Domain) ->
    [validator:touch(name, _),
     validate_user_groups(groups, _),
     validate_roles(roles, _)] ++
        case Domain of
            local ->
                [validate_password(_)];
            external ->
                []
        end ++
        [validator:unsupported(_)].

bad_roles_error(BadRoles) ->
    Str = string:join(BadRoles, ","),
    io_lib:format(
      "Cannot assign roles to user because the following roles are unknown,"
      " malformed or role parameters are undefined: [~s]", [Str]).

validate_user_groups(Name, State) ->
    Is65 = cluster_compat_mode:is_cluster_65(),
    IsEnterprise = cluster_compat_mode:is_enterprise(),
    validator:validate(
      fun (GroupsRaw) when (GroupsRaw =/= "") andalso not IsEnterprise ->
              {error, "User groups require enterprise edition"};
          (GroupsRaw) when (GroupsRaw =/= "") andalso not Is65 ->
              {error, "User groups are not supported in "
                      "mixed version clusters"};
          (GroupsRaw) ->
              Groups = parse_groups(GroupsRaw),
              case lists:filter(?cut(not menelaus_users:group_exists(_)),
                                Groups) of
                  [] -> {value, Groups};
                  BadGroups ->
                      BadGroupsStr = string:join(BadGroups, ","),
                      ErrorStr = io_lib:format("Groups do not exist: ~s",
                                               [BadGroupsStr]),
                      {error, ErrorStr}
              end
      end, Name, State).

parse_groups(GroupsStr) ->
    GroupsTokens = string:tokens(GroupsStr, ","),
    [string:trim(G) || G <- GroupsTokens].

validate_roles(Name, State) ->
    Snapshot = ns_bucket:get_snapshot(),
    validator:validate(
      fun (RawRoles) ->
              Roles = parse_roles(RawRoles),

              BadRoles = [BadRole || BadRole = {error, _} <- Roles],
              case BadRoles of
                  [] ->
                      {value, Roles};
                  _ ->
                      GoodRoles = Roles -- BadRoles,
                      {_, MoreBadRoles} =
                          menelaus_roles:validate_roles(GoodRoles, Snapshot),
                      {error, bad_roles_error(
                                [Raw || {error, Raw} <- BadRoles] ++
                                    [role_to_string(R) || R <- MoreBadRoles])}
              end
      end, Name, State).

handle_put_user_with_identity({_UserId, Domain} = Identity, Req) ->
    validator:handle(
      fun (Values) ->
              handle_put_user_validated(Identity,
                                        proplists:get_value(name, Values),
                                        proplists:get_value(password, Values),
                                        proplists:get_value(roles, Values, []),
                                        proplists:get_value(groups, Values),
                                        Req)
      end, Req, form, put_user_validators(Domain)).

verify_security_roles_access(Req, Permission, Roles) ->
    case overlap(Roles, get_security_roles()) of
        true ->
            menelaus_util:require_permission(Req, Permission);
        false ->
            ok
    end.

overlap(List1, List2) ->
    lists:any(fun (V) -> lists:member(V, List1) end, List2).

get_security_roles() ->
    [R || {R, _} <- menelaus_roles:get_security_roles(
                      ns_bucket:get_snapshot())].

verify_domain_access(Req, {_UserId, Domain})
  when Domain =:= local orelse Domain =:= external ->
    Permission = get_domain_access_permission(write, Domain),
    do_verify_domain_access(Req, Permission).

do_verify_domain_access(Req, Permission) ->
    case menelaus_auth:has_permission(Permission, Req) of
        true ->
            ok;
        false ->
            menelaus_util:require_permission(Req, Permission)
    end.

handle_put_user_validated(Identity, Name, Password, Roles, Groups, Req) ->
    GroupList = case Groups of
                    undefined -> [];
                    _ -> Groups
                end,
    GroupRoles = lists:concat([menelaus_users:get_group_roles(G)
                                   || G <- GroupList]),
    LdapMapGroups = lists:any(fun menelaus_users:has_group_ldap_ref/1,
                              GroupList),
    UniqueRoles = lists:usort(Roles),
    OldRoles = menelaus_users:get_roles(Identity),

    verify_security_roles_access(
      Req, ?SECURITY_WRITE, lists:usort(GroupRoles ++ Roles ++ OldRoles)),

    verify_domain_access(Req, Identity),

    verify_ldap_access(Req, ?EXTERNAL_WRITE, LdapMapGroups),

    Reason = case menelaus_users:user_exists(Identity) of
                 true -> updated;
                 false -> added
             end,
    case menelaus_users:store_user(Identity, Name, Password,
                                   UniqueRoles, Groups) of
        {commit, _} ->
            ns_audit:set_user(Req, Identity, UniqueRoles, Name, Groups,
                              Reason),
            reply_put_delete_users(Req);
        {abort, {error, roles_validation, UnknownRoles}} ->
            menelaus_util:reply_error(
              Req, "roles",
              bad_roles_error([role_to_string(UR) || UR <- UnknownRoles]));
        {abort, password_required} ->
            menelaus_util:reply_error(Req, "password",
                                      "Password is required for new user.");
        {abort, too_many} ->
            menelaus_util:reply_error(
              Req, "_",
              "You cannot create any more users on Community Edition.")
    end.

handle_delete_user(Domain, UserId, Req) ->
    assert_no_users_upgrade(),

    case domain_to_atom(Domain) of
        unknown ->
            menelaus_util:reply_json(Req, <<"Unknown user domain.">>, 404);
        T ->
            Identity = {UserId, T},
            verify_security_roles_access(
              Req, ?SECURITY_WRITE, menelaus_users:get_roles(Identity)),

            verify_domain_access(Req, Identity),

            case menelaus_users:delete_user(Identity) of
                {commit, _} ->
                    ns_audit:delete_user(Req, Identity),
                    reply_put_delete_users(Req);
                {abort, {error, not_found}} ->
                    menelaus_util:reply_json(Req, <<"User was not found.">>,
                                             404)
            end
    end.

reply_put_delete_users(Req) ->
    menelaus_util:reply_json(Req, <<>>, 200).

change_password_validators() ->
    [validator:required(password, _),
     validator:validate(
       fun (P) ->
               case validate_cred(P, password) of
                   true ->
                       ok;
                   Error ->
                       {error, Error}
               end
       end, password, _),
     validator:unsupported(_)].

handle_change_password(Req) ->
    menelaus_util:assert_is_enterprise(),

    case menelaus_auth:get_token(Req) of
        undefined ->
            case menelaus_auth:get_identity(Req) of
                {_, local} = Identity ->
                    handle_change_password_with_identity(Req, Identity);
                {_, admin} = Identity ->
                    handle_change_password_with_identity(Req, Identity);
                _ ->
                    menelaus_util:reply_json(
                      Req,
                      <<"Changing of password is not allowed for this user.">>,
                      404)
            end;
        _ ->
            menelaus_util:require_auth(Req)
    end.

handle_change_password_with_identity(Req, Identity) ->
    validator:handle(
      fun (Values) ->
              case do_change_password(Identity,
                                      proplists:get_value(password, Values)) of
                  ok ->
                      ns_audit:password_change(Req, Identity),
                      menelaus_util:reply(Req, 200);
                  user_not_found ->
                      menelaus_util:reply_json(Req, <<"User was not found.">>,
                                               404);
                  unchanged ->
                      menelaus_util:reply(Req, 200)
              end
      end, Req, form, change_password_validators()).

do_change_password({_, local} = Identity, Password) ->
    menelaus_users:change_password(Identity, Password);
do_change_password({User, admin}, Password) ->
    ns_config_auth:set_admin_credentials(User, Password).

gen_password(Policy) ->
    gen_password(Policy, 100).

gen_password(Policy, 0) ->
    erlang:error({pass_gen_retries_exceeded, Policy});
gen_password({MinLength, _} = Policy, Retries) ->
    Length = max(MinLength, misc:rand_uniform(8, 16)),
    Letters =
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*?",
    Pass = crypto_random_string(Length, Letters),
    case is_valid_password(Pass, Policy) of
        true -> Pass;
        _ -> gen_password(Policy, Retries - 1)
    end.

crypto_random_string(Length, AllowedChars) ->
    S = rand:seed_s(exrop, misc:generate_crypto_seed()),
    AllowedLen = length(AllowedChars),
    {Password, _} = lists:mapfoldl(
        fun(_, Acc) ->
                {Rand, NewAcc} = rand:uniform_s(AllowedLen, Acc),
                {lists:nth(Rand, AllowedChars), NewAcc}
        end, S, lists:seq(1, Length)),
    Password.

reset_admin_password(Password) ->
    {User, Error} =
        case ns_config_auth:get_user(admin) of
            undefined ->
                {undefined, "Failed to reset administrative password. Node is "
                 "not initialized."};
            U ->
                {U, case validate_cred(Password, password) of
                        true ->
                            undefined;
                        ErrStr ->
                            ErrStr
                    end}
        end,

    case Error of
        undefined ->
            ok = ns_config_auth:set_admin_credentials(User, Password),
            ns_audit:password_change(undefined, {User, admin}),
            {ok, Password};
        _ ->
            {error, Error}
    end.

handle_reset_admin_password(Req) ->
    assert_no_users_upgrade(),

    menelaus_util:ensure_local(Req),
    Password =
        case proplists:get_value("generate", mochiweb_request:parse_qs(Req)) of
            "1" ->
                gen_password(get_password_policy());
            _ ->
                PostArgs = mochiweb_request:parse_post(Req),
                proplists:get_value("password", PostArgs)
        end,
    case Password of
        undefined ->
            menelaus_util:reply_error(Req, "password",
                                      "Password should be supplied");
        _ ->
            case reset_admin_password(Password) of
                {ok, Password} ->
                    menelaus_util:reply_json(
                      Req, {struct, [{password, list_to_binary(Password)}]});
                {error, Error} ->
                    menelaus_util:reply_global_error(Req, Error)
            end
    end.

list_to_rbac_atom(List) ->
    try
        list_to_existing_atom(List)
    catch error:badarg ->
            '_unknown_'
    end.

parse_permission(RawPermission) ->
    case string:split(RawPermission, "!", all) of
        [Object, Operation] ->
            case parse_object(Object) of
                error ->
                    error;
                Parsed ->
                    {Parsed, list_to_rbac_atom(Operation)}
            end;
        _ ->
            error
    end.

parse_object("cluster" ++ RawObject) ->
    parse_vertices(RawObject, []);
parse_object(_) ->
    error.

parse_vertices([], Acc) ->
    lists:reverse(Acc);
parse_vertices([$. | Rest], Acc) ->
    case parse_until(Rest, ".[") of
        {Name, [$. | Rest1]} ->
            parse_vertices([$. | Rest1], [list_to_rbac_atom(Name) | Acc]);
        {Name, []} ->
            parse_vertices([], [list_to_rbac_atom(Name) | Acc]);
        {Name, [$[ | Rest1]} ->
            case parse_until(Rest1, "]") of
                {ParamsStr, [$] | Rest2]} ->
                    case parse_parameterized_vertex(Name, ParamsStr) of
                        error ->
                            error;
                        Parsed ->
                            parse_vertices(Rest2, [Parsed | Acc])
                    end;
                _ ->
                    error
            end
    end;
parse_vertices(_, _) ->
    error.

parse_parameterized_vertex(Name, Params) ->
    NameAtom = list_to_rbac_atom(Name),
    case parse_vertex_params(NameAtom, Params) of
        error ->
            error;
        Res ->
            {NameAtom, Res}
    end.

parse_vertex_param(".") ->
    any;
parse_vertex_param(Param) ->
    Param.

params_length(scope) ->
    2;
params_length(collection) ->
    3;
params_length(_) ->
    undefined.

parse_vertex_params(bucket, Name) ->
    parse_vertex_param(Name);
parse_vertex_params(VertexName, ParamsStr) ->
    Params = string:split(ParamsStr, ":", all),
    Length = params_length(VertexName),
    case length(Params) of
        Length ->
            lists:map(fun parse_vertex_param/1, Params);
        _ ->
            error
    end.

parse_permissions(Body) ->
    RawPermissions = string:split(Body, ",", all),
    lists:map(fun (RawPermission) ->
                      Trimmed = string:trim(RawPermission),
                      {Trimmed, parse_permission(Trimmed)}
              end, RawPermissions).

handle_check_permissions_post(Req) ->
    Body = mochiweb_request:recv_body(Req),
    case Body of
        undefined ->
            menelaus_util:reply_json(
              Req, <<"Request body should not be empty.">>, 400);
        _ ->
            Permissions = parse_permissions(binary_to_list(Body)),
            Malformed = [Bad || {Bad, error} <- Permissions],
            case Malformed of
                [] ->
                    Tested =
                        [{list_to_binary(RawPermission),
                          menelaus_auth:has_permission(Permission, Req)} ||
                            {RawPermission, Permission} <- Permissions],
                    menelaus_util:reply_json(Req, {Tested});
                _ ->
                    Message = io_lib:format("Malformed permissions: [~s].",
                                            [string:join(Malformed, ",")]),
                    menelaus_util:reply_json(Req, iolist_to_binary(Message),
                                             400)
            end
    end.

check_permissions_url_version(Snapshot) ->
    B = term_to_binary(
          [cluster_compat_mode:get_compat_version(),
           menelaus_users:get_users_version(),
           menelaus_users:get_groups_version(),
           menelaus_roles:params_version(Snapshot),
           ns_config_auth:get_no_auth_buckets(Snapshot)]),
    base64:encode(crypto:hash(sha, B)).

handle_check_permission_for_cbauth(Req) ->
    Params = mochiweb_request:parse_qs(Req),
    Identity = {proplists:get_value("user", Params),
                list_to_existing_atom(proplists:get_value("domain", Params))},
    RawPermission = proplists:get_value("permission", Params),
    Permission = parse_permission(string:trim(RawPermission)),

    case menelaus_roles:is_allowed(Permission, Identity) of
        true ->
            menelaus_util:reply_text(Req, "", 200);
        false ->
            menelaus_util:reply_text(Req, "", 401)
    end.

vertex_param_to_list(all) ->
    "*";
vertex_param_to_list(any) ->
    ".";
vertex_param_to_list(Param) ->
    Param.

vertex_to_iolist(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
vertex_to_iolist({bucket, Param}) ->
    vertex_to_iolist(bucket, [Param]);
vertex_to_iolist({Atom, Params}) when Atom =:= collection;
                                      Atom =:= scope ->
    vertex_to_iolist(Atom, Params).

vertex_to_iolist(Atom, Params) ->
    ConvertedParams = lists:map(fun vertex_param_to_list/1, Params),
    [atom_to_list(Atom), "[", lists:join(":", ConvertedParams), "]"].

format_permission({Object, Operation}) ->
    FormattedVertices = ["cluster" | [vertex_to_iolist(Vertex) ||
                                         Vertex <- Object]],
    iolist_to_binary(
      [string:join(FormattedVertices, "."), "!", atom_to_list(Operation)]).

forbidden_response(Permissions) ->
    FormattedList = [format_permission(P) || P <- lists:usort(Permissions)],
    {[{message, <<"Forbidden. User needs the following permissions">>},
      {permissions, FormattedList}]}.

handle_get_password_policy(Req) ->
    {MinLength, MustPresent} = get_password_policy(),
    menelaus_util:reply_json(
      Req,
      {[{minLength, MinLength},
        {enforceUppercase, lists:member(uppercase, MustPresent)},
        {enforceLowercase, lists:member(lowercase, MustPresent)},
        {enforceDigits, lists:member(digits, MustPresent)},
        {enforceSpecialChars, lists:member(special, MustPresent)}]}).

post_password_policy_validators() ->
    [validator:required(minLength, _),
     validator:integer(minLength, 0, 100, _),
     validator:boolean(enforceUppercase, _),
     validator:boolean(enforceLowercase, _),
     validator:boolean(enforceDigits, _),
     validator:boolean(enforceSpecialChars, _),
     validator:unsupported(_)].

must_present_value(JsonField, MustPresentAtom, Args) ->
    case proplists:get_value(JsonField, Args) of
        true ->
            [MustPresentAtom];
        _ ->
            []
    end.

handle_post_password_policy(Req) ->
    validator:handle(
      fun (Values) ->
              Policy =
                  [{min_length, proplists:get_value(minLength, Values)},
                   {must_present,
                    must_present_value(enforceUppercase, uppercase, Values) ++
                        must_present_value(enforceLowercase, lowercase,
                                           Values) ++
                        must_present_value(enforceDigits, digits, Values) ++
                        must_present_value(enforceSpecialChars, special,
                                           Values)}],
              ns_config:set(password_policy, Policy),
              ns_audit:password_policy(Req, Policy),
              menelaus_util:reply(Req, 200)
      end, Req, form, post_password_policy_validators()).

assert_no_users_upgrade() ->
    case menelaus_users:upgrade_in_progress() of
        false ->
            ok;
        true ->
            menelaus_util:web_exception(
              503, "Not allowed during cluster upgrade.")
    end.

verify_ldap_access(Req, Permission, ExistingMapping) ->
    verify_ldap_access(Req, Permission, ExistingMapping, false).

verify_ldap_access(_Req, _Permission, false, false) ->
    ok;
verify_ldap_access(Req, Permission, _ExistingMapping, _NewMapping) ->
    menelaus_util:require_permission(Req, Permission).

handle_put_group(GroupId, Req) ->
    assert_groups_and_ldap_enabled(),

    case validate_id(GroupId, <<"Group name">>) of
        true ->
            validator:handle(
              fun (Values) ->
                      Description = proplists:get_value(description, Values),
                      Roles = proplists:get_value(roles, Values),
                      UniqueRoles = lists:usort(Roles),
                      LDAPGroup = proplists:get_value(ldap_group_ref, Values),

                      verify_security_roles_access(
                        Req, ?SECURITY_WRITE,
                        lists:usort(
                          Roles ++ menelaus_users:get_group_roles(GroupId))),

                      verify_ldap_access(
                        Req, ?EXTERNAL_WRITE,
                        menelaus_users:has_group_ldap_ref(GroupId),
                        not menelaus_users:is_empty_ldap_group_ref(LDAPGroup)),

                      do_store_group(GroupId, Description, UniqueRoles,
                                     LDAPGroup, Req)
              end, Req, form, put_group_validators());
        Error ->
            menelaus_util:reply_global_error(Req, Error)
    end.

put_group_validators() ->
    [validator:touch(description, _),
     validator:required(roles, _),
     validate_roles(roles, _),
     validate_ldap_ref(ldap_group_ref, _),
     validator:unsupported(_)].

validate_ldap_ref(Name, State) ->
    validator:validate(
      fun (undefined) -> undefined;
          (DN) ->
              case eldap:parse_dn(DN) of
                  {ok, _} ->
                      {value, DN};
                  {parse_error, Reason, _} ->
                      {error, io_lib:format("Should be valid LDAP distinguished"
                                            " name: ~p", [Reason])}
              end
      end, Name, State).

do_store_group(GroupId, Description, UniqueRoles, LDAPGroup, Req) ->
    Reason = case menelaus_users:group_exists(GroupId) of
                 true -> updated;
                 false -> added
             end,
    case menelaus_users:store_group(GroupId, Description, UniqueRoles,
                                    LDAPGroup) of
        ok ->
            ns_audit:set_user_group(Req, GroupId, UniqueRoles, Description,
                                    LDAPGroup, Reason),
            menelaus_util:reply_json(Req, <<>>, 200);
        {error, {roles_validation, UnknownRoles}} ->
            menelaus_util:reply_error(
              Req, "roles",
              bad_roles_error([role_to_string(UR) || UR <- UnknownRoles]))
    end.

handle_delete_group(GroupId, Req) ->
    assert_groups_and_ldap_enabled(),
    verify_security_roles_access(
      Req, ?SECURITY_WRITE, menelaus_users:get_group_roles(GroupId)),
    verify_ldap_access(Req, ?EXTERNAL_WRITE,
                       menelaus_users:has_group_ldap_ref(GroupId)),

    case menelaus_users:delete_group(GroupId) of
        ok ->
            ns_audit:delete_user_group(Req, GroupId),
            menelaus_util:reply_json(Req, <<>>, 200);
        {error, not_found} ->
            menelaus_util:reply_json(Req, <<"Group was not found.">>, 404)
    end.

handle_get_groups(Path, Req) ->
    assert_groups_and_ldap_enabled(),
    Query = mochiweb_request:parse_qs(Req),
    case lists:keyfind("pageSize", 1, Query) of
        false ->
            handle_get_all_groups(Req);
        _ ->
            validator:handle(
              handle_get_groups_page(Req, Path, _),
              Req, Query, get_groups_page_validators())
    end.

get_groups_page_validators() ->
    [validator:integer(pageSize, ?MIN_USERS_PAGE_SIZE, ?MAX_USERS_PAGE_SIZE, _),
     validator:touch(startFrom, _),
     validator:one_of(sortBy,
                      ["id", "description", "roles", "ldap_group_ref"], _),
     validator:convert(sortBy, fun list_to_atom/1, _),
     validator:one_of(order, ["asc", "desc"], _),
     validator:touch(substr, _),
     validator:convert(order, fun list_to_atom/1, _),
     validator:unsupported(_)].

handle_get_groups_page(Req, Path, Values) ->
    StartId = proplists:get_value(startFrom, Values),
    Start = case StartId of
                undefined -> undefined;
                _ -> {StartId, menelaus_users:get_group_props(StartId)}
            end,
    PageSize = proplists:get_value(pageSize, Values),
    Order = proplists:get_value(order, Values, asc),
    Sort = proplists:get_value(sortBy, Values, id),
    Substr = proplists:get_value(substr, Values, undefined),

    {PageSkews, Total} =
        pipes:run(menelaus_users:select_groups('_'),
                  [security_filter(Req),
                   ldap_ref_filter(Req),
                   substr_filter(Substr, [description])],
                  ?make_consumer(
                     pipes:fold(
                       ?producer(),
                       fun ({{group, Identity}, Props}, {Skews, T}) ->
                               {add_to_skews({Identity, Props}, Skews), T + 1}
                       end, {create_skews(Start, PageSize, Sort, Order), 0}))),

    {Groups, Skipped, Links} = page_data_from_skews(PageSkews, PageSize),
    GroupsJson = [group_to_json(Id, Props) || {Id, Props} <- Groups],
    LinksJson = build_group_links(Links, Path,
                                  [{pageSize, PageSize},
                                   {sortBy, Sort},
                                   {order, Order}] ++
                                  [{substr, Substr} || Substr =/= undefined]),
    Json = {[{total, Total},
             {links, LinksJson},
             {skipped, Skipped},
             {groups, GroupsJson}]},

    menelaus_util:reply_json(Req, Json).

handle_get_all_groups(Req) ->
    pipes:run(menelaus_users:select_groups('_'),
              [security_filter(Req),
               ldap_ref_filter(Req),
               jsonify_groups(),
               sjson:encode_extended_json([{compact, true},
                                           {strict, false}]),
               pipes:simple_buffer(2048)],
              menelaus_util:send_chunked(
                Req, 200, [{"Content-Type", "application/json"}])).

jsonify_groups() ->
    ?make_transducer(
       begin
           ?yield(array_start),
           pipes:foreach(
             ?producer(),
             fun ({{group, GroupId}, Props}) ->
                     ?yield({json, group_to_json(GroupId, Props)})
             end),
           ?yield(array_end)
       end).

handle_get_group(GroupId, Req) ->
    assert_groups_and_ldap_enabled(),
    case menelaus_users:group_exists(GroupId) of
        false ->
            menelaus_util:reply_json(Req, <<"Unknown group.">>, 404);
        true ->
            verify_security_roles_access(
              Req, ?SECURITY_READ, menelaus_users:get_group_roles(GroupId)),

            menelaus_util:reply_json(Req, get_group_json(GroupId))
    end.

get_group_json(GroupId) ->
    group_to_json(GroupId, menelaus_users:get_group_props(GroupId)).

group_to_json(GroupId, Props) ->
    Description = proplists:get_value(description, Props),
    LDAPGroup = proplists:get_value(ldap_group_ref, Props),
    {[{id, list_to_binary(GroupId)},
      {roles, [{role_to_json(R)} || R <- proplists:get_value(roles, Props)]}] ++
         [{ldap_group_ref, list_to_binary(LDAPGroup)}
          || LDAPGroup =/= undefined] ++
         [{description, list_to_binary(Description)}
          || Description =/= undefined]}.

assert_groups_and_ldap_enabled() ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_65().

jsonify_profiles() ->
    ?make_transducer(
       begin
           ?yield(array_start),
           pipes:foreach(
             ?producer(),
             fun ({{_, {Id, Domain}}, Json}) ->
                     ?yield({json, {[{id, list_to_binary(Id)},
                                     {domain, Domain},
                                     {profile, Json}]}})
             end),
           ?yield(array_end)
       end).

handle_get_profiles(Req) ->
    pipes:run(menelaus_users:select_profiles(),
              [domain_filter(admin, Req),
               domain_filter(local, Req),
               domain_filter(external, Req),
               jsonify_profiles(),
               sjson:encode_extended_json([{compact, true},
                                           {strict, false}]),
               pipes:simple_buffer(2048)],
              menelaus_util:send_chunked(
                Req, 200, [{"Content-Type", "application/json"}])).

get_identity_for_profiles(self, Req) ->
    menelaus_auth:get_identity(Req);
get_identity_for_profiles({Name, "admin"}, _) ->
    {Name, admin};
get_identity_for_profiles({Name, DomainStr}, _) ->
    {Name, case domain_to_atom(DomainStr) of
               unknown ->
                   menelaus_util:web_exception(404, "Unknown domain");
               Atom ->
                   Atom
           end}.

validate_identity_for_profiles({Name, Domain}) ->
    case validate_cred(Name, username) of
        true ->
            case Domain of
                admin ->
                    case ns_config_auth:get_user(admin) of
                        Name ->
                            ok;
                        _ ->
                            menelaus_util:web_exception(404,
                                                        "Unknown identity")
                    end;
                local ->
                    case menelaus_users:user_exists({Name, local}) of
                        false ->
                            menelaus_util:web_exception(
                              404, "User does not exist");
                        true ->
                            ok
                    end;
                _ ->
                    ok
            end;
        Error ->
            menelaus_util:web_exception(400, Error)
    end.

verify_domain_access_for_profiles(_Req, _Op, _Identity, self) ->
    ok;
verify_domain_access_for_profiles(Req, Op, {_, Domain}, _RawIdentity) ->
    Permission = get_domain_access_permission(Op, Domain),
    do_verify_domain_access(Req, Permission).

handle_get_profile(RawIdentity, Req) ->
    Identity = get_identity_for_profiles(RawIdentity, Req),
    verify_domain_access_for_profiles(Req, read, Identity, RawIdentity),
    case menelaus_users:get_profile(Identity) of
        undefined ->
            menelaus_util:reply_json(Req, <<"UI profile was not found.">>, 404);
        Json ->
            menelaus_util:reply_json(Req, Json)
    end.

handle_delete_profile(RawIdentity, Req) ->
    Identity = get_identity_for_profiles(RawIdentity, Req),
    verify_domain_access_for_profiles(Req, write, Identity, RawIdentity),
    case menelaus_users:delete_profile(Identity) of
        ok ->
            ns_audit:delete_user_profile(Req, Identity),
            menelaus_util:reply_json(Req, <<>>, 200);
        {error, not_found} ->
            menelaus_util:reply_json(Req, <<"UI profile was not found.">>, 404)
    end.

handle_put_profile(RawIdentity, Req) ->
    Identity = get_identity_for_profiles(RawIdentity, Req),
    verify_domain_access_for_profiles(Req, write, Identity, RawIdentity),
    validate_identity_for_profiles(Identity),

    Body = mochiweb_request:recv_body(Req),
    try ejson:decode(Body) of
        Json ->
            menelaus_users:store_profile(Identity, Json),
            ns_audit:set_user_profile(Req, Identity, Json),
            menelaus_util:reply_json(Req, <<>>, 200)
    catch _:_ ->
            menelaus_util:reply_json(Req, <<"Invalid Json">>, 400)
    end.

handle_get_uiroles(Req) ->
    menelaus_util:require_permission(Req, {[admin, security], read}),

    Snapshot = ns_bucket:get_snapshot(),
    Roles =
        maybe_remove_security_roles(
          Req, Snapshot, menelaus_roles:get_visible_role_definitions()),
    Folders =
        lists:filtermap(build_ui_folder(_, Roles), menelaus_roles:ui_folders()),

    BucketNames = menelaus_auth:filter_accessible_buckets(
                    ?cut({[{bucket, _}, settings], read}),
                    ns_bucket:get_bucket_names(Snapshot), Req),

    Parameters = {[build_ui_parameters(bucket_name, {BucketNames, Snapshot})]},

    menelaus_util:reply_json(Req, {[{folders, Folders},
                                    {parameters, Parameters}]}).

build_ui_folder({Key, Name}, Roles) ->
    case lists:filter(fun ({_, _, Props, _}) ->
                              proplists:get_value(folder, Props) =:= Key
                      end, Roles) of
        [] ->
            false;
        FolderRoles ->
            {true, {[{name, list_to_binary(Name)},
                     {roles, [build_ui_role(Role) || Role <- FolderRoles]}]}}
    end.

build_ui_role({Role, Params, Props, _}) ->
    {[{role, Role}, {params, Params} | jsonify_props(Props)]}.

build_ui_value(Value, Children) ->
    case lists:filter(fun ({_, L}) -> L =/= [] end, Children) of
        [] ->
            {[{value, list_to_binary(Value)}]};
        NonEmpty ->
            {[{value, list_to_binary(Value)},
              {children, {NonEmpty}}]}
    end.

build_ui_parameters(Name, Data) ->
    {Name, build_ui_values(Name, Data)}.

build_ui_values(bucket_name, {Buckets, Snapshot}) ->
    lists:map(
      fun (Name) ->
              Manifest = collections:get_manifest(Name, Snapshot),
              Scopes =
                  case cluster_compat_mode:is_enterprise() andalso
                      Manifest =/= undefined of
                      true ->
                          collections:get_scopes(Manifest);
                      false ->
                          []
                  end,
              build_ui_value(Name, [build_ui_parameters(scope_name, Scopes)])
      end, Buckets);
build_ui_values(scope_name, Scopes) ->
    [build_ui_value(
       Name, [build_ui_parameters(collection_name,
                                  collections:get_collections(Scope))]) ||
        {Name, Scope} <- Scopes];
build_ui_values(collection_name, Collections) ->
    [build_ui_value(Name, []) || {Name, _} <- Collections].

-ifdef(TEST).
role_to_string_test() ->
    ?assertEqual("role", role_to_string(role)),
    ?assertEqual("role[b]", role_to_string({role, [{"b", 0}]})),
    ?assertEqual("role[*]", role_to_string({role, [any]})),
    ?assertEqual("role[b:s:c]",
                 role_to_string({role, [{"b", 0}, {"s", 1}, {"c", 2}]})),
    ?assertEqual("role[b:s]",
                 role_to_string({role, [{"b", 0}, {"s", 1}, any]})),
    ?assertEqual("role[b]", role_to_string({role, [{"b", 0}, any, any]})).

t_wrap(Tests) ->
    {foreach,
     fun() ->
             meck:new(cluster_compat_mode, [passthrough]),
             meck:expect(cluster_compat_mode, is_enterprise,
                         fun () -> true end),
             meck:expect(cluster_compat_mode, get_compat_version,
                         fun (_) -> ?VERSION_70 end)
     end,
     fun (_) ->
             meck:unload(cluster_compat_mode)
     end,
     Tests}.

role_to_json_test_() ->
    Test = fun (Expected, Role) ->
                   ?assertEqual(lists:sort(Expected),
                                lists:sort(role_to_json(Role)))
           end,
    t_wrap(
      [{"role to json",
        fun () ->
                Test([{role, admin}], admin),
                Test([{role, bucket_admin}, {bucket_name, <<"*">>}],
                     {bucket_admin, [any]}),
                Test([{role, bucket_admin}, {bucket_name, <<"test">>}],
                     {bucket_admin, ["test"]}),
                Test([{role, data_reader}, {bucket_name, <<"*">>},
                      {scope_name, <<"*">>}, {collection_name, <<"*">>}],
                     {data_reader, [any, any, any]}),
                Test([{role, data_reader}, {bucket_name, <<"test">>},
                      {scope_name, <<"*">>}, {collection_name, <<"*">>}],
                     {data_reader, ["test", any, any]}),
                Test([{role, data_reader}, {bucket_name, <<"test">>},
                      {scope_name, <<"s">>}, {collection_name, <<"*">>}],
                     {data_reader, ["test", "s", any]}),
                Test([{role, data_reader}, {bucket_name, <<"test">>},
                      {scope_name, <<"s">>}, {collection_name, <<"c">>}],
                     {data_reader, ["test", "s", "c"]})
        end}]).

parse_roles_test_() ->
    t_wrap(
      [{"without collections",
        fun () ->
                ?assertEqual(
                   [admin,
                    {bucket_admin, ["test.test"]},
                    {bucket_admin, [any]},
                    {error, "bucket_admin[]"},
                    {error, "no_such_atom"},
                    {error, "bucket_admin[default"}],
                   parse_roles("admin, bucket_admin[test.test], "
                               "bucket_admin[*], bucket_admin[],"
                               "no_such_atom, bucket_admin[default"))
        end},
       {"with collections",
        fun () ->
                ?assertEqual(
                   [{data_reader, [any, any, any]},
                    {data_reader, ["test", any, any]},
                    {data_reader, ["test", "s", any]},
                    {data_reader, ["test", "s", "c"]},
                    {data_reader, ["test", "s", "c", "c", "c"]},
                    {bucket_admin, ["test", "s"]},
                    {data_reader, ["", "", ""]}],
                   parse_roles("data_reader[*], data_reader[test], "
                               "data_reader[test:s], data_reader[test:s:c], "
                               "data_reader[test:s:c:c:c], "
                               "bucket_admin[test:s], data_reader[::]"))
        end}]).

parse_permissions_test() ->
    TestOne =
        fun (String, Expected) ->
                ?assertEqual([{String, Expected}], parse_permissions(String))
        end,

    ?assertEqual([{"", error}], parse_permissions("")),
    ?assertEqual([{"", error}, {"", error}], parse_permissions(",")),

    ?assertEqual(
       [{"", error},
        {"cluster.admin!write", {[admin], write}},
        {"cluster.admin", error},
        {"admin!write", error}],
       parse_permissions(",cluster.admin!write, cluster.admin, admin!write")),
    ?assertEqual(
       [{"cluster.bucket[test.test]!read", {[{bucket, "test.test"}], read}},
        {"cluster.bucket[test.test].stats!read",
         {[{bucket, "test.test"}, stats], read}}],
       parse_permissions(" cluster.bucket[test.test]!read, "
                         "cluster.bucket[test.test].stats!read ")),

    TestOne("cluster.bucket[].stats!read", {[{bucket, ""}, stats], read}),
    TestOne("cluster.bucket[].stats!!read", error),
    TestOne("cluster.bucket[.].stats!read", {[{bucket, any}, stats], read}),

    TestOne("cluster.no_such_atom!no_such_atom", {['_unknown_'], '_unknown_'}),

    TestOne("cluster.collection[test:s:c].n1ql.update!execute",
            {[{collection, ["test", "s", "c"]}, n1ql, update], execute}),
    TestOne("cluster.collection[:s:c].n1ql.update!execute",
            {[{collection, ["", "s", "c"]}, n1ql, update], execute}),
    TestOne("cluster.collection[::].n1ql.update!execute",
            {[{collection, ["", "", ""]}, n1ql, update], execute}),
    TestOne("cluster.collection[test:s:c].n1ql.update!execute",
            {[{collection, ["test", "s", "c"]}, n1ql, update], execute}),
    TestOne("cluster.collection[test:s:.].n1ql.update!execute",
            {[{collection, ["test", "s", any]}, n1ql, update], execute}),
    TestOne("cluster.collection[test:.:.].n1ql.update!execute",
            {[{collection, ["test", any, any]}, n1ql, update], execute}),
    TestOne("cluster.collection[.:.:.].n1ql.update!execute",
            {[{collection, [any, any, any]}, n1ql, update], execute}),

    TestOne("cluster.scope[test:s].n1ql.update!execute",
            {[{scope, ["test", "s"]}, n1ql, update], execute}),
    TestOne("cluster.scope[test:].n1ql.update!execute",
            {[{scope, ["test", ""]}, n1ql, update], execute}),
    TestOne("cluster.scope[test:.].n1ql.update!execute",
            {[{scope, ["test", any]}, n1ql, update], execute}),
    TestOne("cluster.scope[.:.].n1ql.update!execute",
            {[{scope, [any, any]}, n1ql, update], execute}),

    TestOne("cluster.scope[].n1ql.update!execute", error),
    TestOne("cluster.scope[test].n1ql.update!execute", error),
    TestOne("cluster.scope[test:s:c].n1ql.update!execute", error),
    TestOne("cluster.scope[test::s].n1ql.update!execute", error),
    TestOne("cluster.collection[].n1ql.update!execute", error),
    TestOne("cluster.collection[test].n1ql.update!execute", error),
    TestOne("cluster.collection[test:s].n1ql.update!execute", error),
    TestOne("cluster.collection[test:s::c].n1ql.update!execute", error),

    TestOne("cluster.wrong[].n1ql.update!execute", error),
    TestOne("cluster.wrong[test:s::c].n1ql.update!execute", error).

format_permission_test() ->
    ?assertEqual(<<"cluster.bucket[.].views!write">>,
                 format_permission({[{bucket, any}, views], write})),
    ?assertEqual(<<"cluster.bucket[default]!all">>,
                 format_permission({[{bucket, "default"}], all})),
    ?assertEqual(<<"cluster!all">>,
                 format_permission({[], all})),
    ?assertEqual(<<"cluster.admin.diag!read">>,
                 format_permission({[admin, diag], read})),
    Test =
        fun (Expected, Vertex) ->
                ?assertEqual(list_to_binary("cluster." ++ Expected ++
                                                ".n1ql.update!execute"),
                             format_permission(
                               {[Vertex, n1ql, update], execute}))
        end,
    Test("collection[test:s:c]", {collection, ["test", "s", "c"]}),
    Test("collection[test:s:.]", {collection, ["test", "s", any]}),
    Test("collection[test:.:.]", {collection, ["test", any, any]}),
    Test("collection[.:.:.]", {collection, [any, any, any]}),

    Test("scope[test:s]", {scope, ["test", "s"]}),
    Test("scope[test:.]", {scope, ["test", any]}),
    Test("scope[.:.]", {scope, [any, any]}).

toy_users(First, Last) ->
    [toy_user(U) || U <- lists:seq(First, Last)].

toy_user(U) ->
    {{lists:flatten(io_lib:format("a~b", [U])), local},
     [{p, lists:flatten(io_lib:format("p~b", [1000 - U]))}]}.

process_toy_users(Users, Start, PageSize) ->
    {PageUsers, Skipped, Links} =
        page_data_from_skews(
          lists:foldl(
            fun (U, Skews) ->
                    add_to_skews(U, Skews)
            end, create_skews(Start, PageSize, id, asc), Users),
          PageSize),
    ?assertEqual({PageUsers, Skipped, Links},
                 page_data_from_skews(
                   lists:foldl(
                     fun (U, Skews) ->
                             add_to_skews(U, Skews)
                     end, create_skews(Start, PageSize, p, desc), Users),
                   PageSize)),
    {[{skipped, Skipped}, {users, PageUsers}], lists:sort(Links)}.

toy_result(Params, Links) ->
    {lists:sort(Params), lists:sort(seed_links(Links))}.

no_users_no_params_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 0},
          {users, []}],
         []),
       process_toy_users([], undefined, 3)).

no_users_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 0},
          {users, []}],
         []),
       process_toy_users([], toy_user(14), 3)).

one_user_no_params_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 0},
          {users, toy_users(10, 10)}],
         []),
       process_toy_users(toy_users(10, 10), undefined, 3)).

first_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 0},
          {users, toy_users(10, 12)}],
         [{last, {"a28", local}},
          {next, {"a13", local}}]),
       process_toy_users(toy_users(10, 30), undefined, 3)).

first_page_with_params_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 0},
          {users, toy_users(10, 12)}],
         [{last, {"a28", local}},
          {next, {"a13", local}}]),
       process_toy_users(toy_users(10, 30), toy_user(10), 3)).

middle_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 4},
          {users, toy_users(14, 16)}],
         [{first, noparams},
          {prev, {"a11", local}},
          {last, {"a28", local}},
          {next, {"a17", local}}]),
       process_toy_users(toy_users(10, 30), toy_user(14), 3)).

middle_page_non_existent_user_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 5},
          {users, toy_users(15, 17)}],
         [{first, noparams},
          {prev, {"a12", local}},
          {last, {"a28", local}},
          {next, {"a18", local}}]),
       process_toy_users(toy_users(10, 30),
                         {{"a14b", local}, [{p, "p985b"}]}, 3)).

near_the_end_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 17},
          {users, toy_users(27, 29)}],
         [{first, noparams},
          {prev, {"a24", local}},
          {last, {"a28", local}},
          {next, {"a28", local}}]),
       process_toy_users(toy_users(10, 30), toy_user(27), 3)).

at_the_end_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 19},
          {users, toy_users(29, 30)}],
         [{first, noparams},
          {prev, {"a26", local}}]),
       process_toy_users(toy_users(10, 30), toy_user(29), 3)).

after_the_end_page_test() ->
    ?assertEqual(
       toy_result(
         [{skipped, 21},
          {users, []}],
         [{first, noparams},
          {prev, {"a28", local}}]),
       process_toy_users(toy_users(10, 30),
                         {{"b29", local}, [{p, "o971"}]}, 3)).

validate_cred_username_test() ->
    LongButValid = "Username_that_is_127_characters_XXXXXXXXXXXXXXXXXXXXXXXXXX"
        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    ?assertEqual(127, length(LongButValid)),
    ?assertEqual(true, validate_cred("valid", username)),
    ?assertEqual(true, validate_cred(LongButValid, username)),
    ?assertNotEqual(true, validate_cred([], username)),
    ?assertNotEqual(true, validate_cred("", username)),
    ?assertNotEqual(true, validate_cred(LongButValid ++
                                            "more_than_128_characters",
                                        username)),
    ?assertNotEqual(true, validate_cred([7], username)),
    ?assertNotEqual(true, validate_cred([127], username)),
    ?assertNotEqual(true, validate_cred("=", username)),

    %% The following block does not work after compilation with erralng 16
    %% due to non-native utf8 enoding of strings in .beam compiled files.
    %% TODO: re-enable this after upgrading to eralng 19+.
    %% Utf8 = "ξ",
    %% ?assertEqual(1,length(Utf8)),
    %% ?assertEqual(true, validate_cred(Utf8, username)),                  % "ξ" is codepoint 958
    %% ?assertEqual(true, validate_cred(LongButValid ++ Utf8, username)),  % 128 code points
    ok.

gen_password_test() ->
    Pass1 = gen_password({20, [uppercase]}),
    Pass2 = gen_password({0,  [digits]}),
    Pass3 = gen_password({5,  [uppercase, lowercase, digits, special]}),
    %% Using assertEqual instead of assert because assert is causing
    %% false dialyzer errors
    ?assertEqual(true, length(Pass1) >= 20),
    ?assertEqual(true, verify_uppercase(Pass1)),
    ?assertEqual(true, length(Pass2) >= 8),
    ?assertEqual(true, verify_digits(Pass2)),
    ?assertEqual(true, verify_lowercase(Pass3)),
    ?assertEqual(true, verify_uppercase(Pass3)),
    ?assertEqual(true, verify_special(Pass3)),
    ?assertEqual(true, verify_digits(Pass3)),
    ok.

gen_password_monkey_test_() ->
    GetRandomPolicy =
        fun () ->
            MustPresent = [uppercase || rand:uniform(2) == 1] ++
                          [lowercase || rand:uniform(2) == 1] ++
                          [digits    || rand:uniform(2) == 1] ++
                          [special   || rand:uniform(2) == 1],
            {rand:uniform(30), MustPresent}
        end,
    Test = fun () ->
                   [gen_password(GetRandomPolicy()) || _ <- lists:seq(1,100000)]
           end,
    {timeout, 100, Test}.
-endif.
