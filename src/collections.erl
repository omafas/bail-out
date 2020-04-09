%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
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

%% @doc methods for handling collections

-module(collections).

-include("cut.hrl").
-include("ns_common.hrl").

-export([start_link/0,
         enabled/0,
         enabled/1,
         uid/1,
         manifest_json/1,
         create_scope/2,
         create_collection/4,
         drop_scope/2,
         drop_collection/3,
         wait_for_manifest_uid/5,
         convert_uid_from_memcached/1]).

%% rpc from other nodes
-export([wait_for_manifest_uid/4]).

-define(SERVER, {via, leader_registry, ?MODULE}).

start_link() ->
    misc:start_singleton(work_queue, start_link, [?SERVER]).

enabled() ->
    cluster_compat_mode:is_enabled(?VERSION_65) andalso
        cluster_compat_mode:is_developer_preview().

enabled(BucketConfig) ->
    enabled() andalso ns_bucket:bucket_type(BucketConfig) =:= membase.

default_manifest() ->
    [{uid, 0},
     {next_uid, 0},
     {next_scope_uid, 7},
     {next_coll_uid, 7},
     {scopes,
      [{"_default",
        [{uid, 0},
         {collections,
          [{"_default",
            [{uid, 0}]}]}]}]}].

uid(BucketCfg) ->
    case enabled(BucketCfg) of
        true ->
            get_uid_in_memcached_format(get_manifest(BucketCfg));
        false ->
            undefined
    end.

get_uid(Props) ->
    proplists:get_value(uid, Props).

convert_uid_to_memcached(V) ->
    list_to_binary(string:to_lower(integer_to_list(V, 16))).

convert_uid_from_memcached(V) when is_list(V) ->
    list_to_integer(V, 16);
convert_uid_from_memcached(V) when is_binary(V) ->
    convert_uid_from_memcached(binary_to_list(V)).

get_uid_in_memcached_format(Props) ->
    convert_uid_to_memcached(get_uid(Props)).

collection_prop_to_memcached(uid, V) ->
    convert_uid_to_memcached(V);
collection_prop_to_memcached(_, V) ->
    V.

collection_to_memcached(Name, Props) ->
    {[{name, list_to_binary(Name)} |
      [{K, collection_prop_to_memcached(K, V)} || {K, V} <- Props]]}.

manifest_json(BucketCfg) ->
    Manifest = get_manifest(BucketCfg),

    ScopesJson =
        lists:map(
          fun ({ScopeName, Scope}) ->
                  {[{name, list_to_binary(ScopeName)},
                    {uid, get_uid_in_memcached_format(Scope)},
                    {collections,
                     [collection_to_memcached(CollName, Coll) ||
                         {CollName, Coll} <- get_collections(Scope)]}]}
          end, get_scopes(Manifest)),

    {[{uid, get_uid_in_memcached_format(Manifest)},
      {scopes, ScopesJson}]}.

create_scope(Bucket, Name) ->
    update(Bucket, {create_scope, Name}).

create_collection(Bucket, Scope, Name, Props) ->
    update(Bucket, {create_collection, Scope, Name, Props}).

drop_scope(Bucket, Name) ->
    update(Bucket, {drop_scope, Name}).

drop_collection(Bucket, Scope, Name) ->
    update(Bucket, {drop_collection, Scope, Name}).

update(Bucket, Operation) ->
    work_queue:submit_sync_work(
      ?SERVER, ?cut(do_update(Bucket, Operation))).

do_update(Bucket, Operation) ->
    ?log_debug("Performing operation ~p on bucket ~p", [Operation, Bucket]),
    RV =
        case leader_activities:run_activity(
               {?MODULE, Bucket}, {?MODULE, Bucket}, majority,
               fun () ->
                       do_update_as_leader(Bucket, Operation)
               end, []) of
            {leader_activities_error, _, Err} ->
                {unsafe, Err};
            Res ->
                Res
        end,
    case RV of
        {ok, _} ->
            RV;
        {user_error, Error} ->
            ?log_debug("Operation ~p for bucket ~p failed with ~p",
                       [Operation, Bucket, RV]),
            Error;
        {Error, Details} ->
            ?log_error("Operation ~p for bucket ~p failed with ~p (~p)",
                       [Operation, Bucket, Error, Details]),
            Error
    end.

do_update_as_leader(Bucket, Operation) ->
    OtherNodes = ns_node_disco:nodes_actual_other(),
    case pull_config(OtherNodes) of
        ok ->
            {ok, BucketCfg} = ns_bucket:get_bucket(Bucket),
            Manifest = get_manifest(BucketCfg),
            case verify_oper(Operation, Manifest) of
                ok ->
                    NewManifest = bump_ids(Manifest, Operation),
                    ok = update_manifest(Bucket, NewManifest),
                    case ns_config_rep:ensure_config_seen_by_nodes(
                           OtherNodes) of
                        ok ->
                            do_update_with_manifest(Bucket, NewManifest,
                                                    Operation);
                        Error ->
                            {push_config, Error}
                    end;
                Error ->
                    {user_error, Error}
            end;
        Error ->
            {pull_config, Error}
    end.

do_update_with_manifest(Bucket, Manifest, Operation) ->
    ?log_debug("Perform operation ~p on manifest ~p of bucket ~p",
               [Operation, Manifest, Bucket]),
    NewManifest = handle_oper(Operation, Manifest),
    {Uid, NewManifestWithId} = update_manifest_uid(NewManifest),
    ?log_debug("Resulting manifest ~p", [NewManifestWithId]),
    ok = update_manifest(Bucket, NewManifestWithId),
    {ok, convert_uid_to_memcached(Uid)}.

update_manifest(Bucket, Manifest) ->
    ns_bucket:set_property(Bucket, collections_manifest, Manifest).

bump_ids(Manifest, Oper) ->
    do_bump_ids(Manifest, [next_uid | needed_ids(Oper)]).

do_bump_ids(Manifest, IDs) ->
    lists:foldl(
      fun (ID, ManifestAcc) ->
              misc:key_update(ID, ManifestAcc, _ + 1)
      end, Manifest, IDs).

update_manifest_uid(Manifest) ->
    Uid = proplists:get_value(next_uid, Manifest),
    {Uid, lists:keystore(uid, 1, Manifest, {uid, Uid})}.

needed_ids({create_scope, _}) ->
    [next_scope_uid];
needed_ids({create_collection, _, _, _}) ->
    [next_coll_uid];
needed_ids(_) ->
    [].

verify_oper({create_scope, Name}, Manifest) ->
    Scopes = get_scopes(Manifest),
    case find_scope(Name, Scopes) of
        undefined ->
            ok;
        _ ->
            scope_already_exists
    end;
verify_oper({drop_scope, Name}, Manifest) ->
    Scopes = get_scopes(Manifest),
    case Name of
        "_default" ->
            default_scope;
        _ ->
            case find_scope(Name, Scopes) of
                undefined ->
                    scope_not_found;
                _ ->
                    ok
            end
    end;
verify_oper({create_collection, ScopeName, Name, _}, Manifest) ->
    Scopes = get_scopes(Manifest),
    case find_scope(ScopeName, Scopes) of
        undefined ->
            scope_not_found;
        Scope ->
            Collections = get_collections(Scope),
            case find_collection(Name, Collections) of
                undefined ->
                    ok;
                _ ->
                    collection_already_exists
            end
    end;
verify_oper({drop_collection, ScopeName, Name}, Manifest) ->
    Scopes = get_scopes(Manifest),
    case find_scope(ScopeName, Scopes) of
        undefined ->
            scope_not_found;
        Scope ->
            Collections = get_collections(Scope),
            case find_collection(Name, Collections) of
                undefined ->
                    collection_not_found;
                _ ->
                    ok
            end
    end.

handle_oper({create_scope, Name}, Manifest) ->
    on_scopes(add_scope(Name, _, Manifest), Manifest);
handle_oper({drop_scope, Name}, Manifest) ->
    on_scopes(delete_scope(Name, _), Manifest);
handle_oper({create_collection, Scope, Name, Props}, Manifest) ->
    on_collections(add_collection(Name, Props, _, Manifest), Scope, Manifest);
handle_oper({drop_collection, Scope, Name}, Manifest) ->
    on_collections(delete_collection(Name, _), Scope, Manifest).

get_manifest(BucketCfg) ->
    proplists:get_value(collections_manifest, BucketCfg, default_manifest()).

get_scopes(Manifest) ->
    proplists:get_value(scopes, Manifest).

find_scope(Name, Scopes) ->
    proplists:get_value(Name, Scopes).

add_scope(Name, Scopes, Manifest) ->
    [{Name, [{uid, proplists:get_value(next_scope_uid, Manifest)},
             {collections, []}]} | Scopes].

delete_scope(Name, Scopes) ->
    lists:keydelete(Name, 1, Scopes).

update_scopes(Scopes, Manifest) ->
    lists:keystore(scopes, 1, Manifest, {scopes, Scopes}).

on_scopes(Fun, Manifest) ->
    Scopes = get_scopes(Manifest),
    NewScopes = Fun(Scopes),
    update_scopes(NewScopes, Manifest).

get_collections(Scope) ->
    proplists:get_value(collections, Scope).

find_collection(Name, Collections) ->
    proplists:get_value(Name, Collections).

add_collection(Name, Props, Collections, Manifest) ->
    [{Name, [{uid, proplists:get_value(next_coll_uid, Manifest)} | Props]} |
     Collections].

delete_collection(Name, Collections) ->
    lists:keydelete(Name, 1, Collections).

update_collections(Collections, Scope) ->
    lists:keystore(collections, 1, Scope, {collections, Collections}).

on_collections(Fun, ScopeName, Manifest) ->
    on_scopes(
      fun (Scopes) ->
              Scope = find_scope(ScopeName, Scopes),
              Collections = get_collections(Scope),
              NewCollections = Fun(Collections),
              NewScope = update_collections(NewCollections, Scope),
              lists:keystore(ScopeName, 1, Scopes, {ScopeName, NewScope})
      end, Manifest).

pull_config(Nodes) ->
    ?log_debug("Attempting to pull config from nodes:~n~p", [Nodes]),

    Timeout = ?get_timeout(pull_config, 5000),
    case ns_config_rep:pull_remotes(Nodes, Timeout) of
        ok ->
            ?log_debug("Pulled config successfully."),
            ok;
        Error ->
            ?log_error("Failed to pull config from some nodes: ~p.",
                       [Error]),
            Error
    end.

wait_for_manifest_uid(Bucket, BucketUuid, Uid, Timeout) ->
    case async:run_with_timeout(
           ?cut(wait_for_manifest_uid(Bucket, BucketUuid, Uid)), Timeout) of
        {ok, R} ->
            R;
        {error, timeout} ->
            timeout
    end.

wait_for_manifest_uid(Bucket, BucketUuid, Uid) ->
    Ref = erlang:make_ref(),
    Parent = self(),
    Subscription =
        ns_pubsub:subscribe_link(
          buckets_events,
          fun ({set_collections_manifest, B, U}) when U >= Uid,
                                                      B =:= BucketUuid ->
                  Parent ! {Ref, ok};
              ({stopped, B}) when B =:= Bucket ->
                  Parent ! {Ref, stopped};
              (_) ->
                  ok
          end),
    try
        case ns_memcached:get_collections_uid(Bucket) of
            U when U >= Uid ->
                ok;
            _ ->
                receive
                    {Ref, Ret} ->
                        Ret
                end
        end
    after
        (catch ns_pubsub:unsubscribe(Subscription))
    end.

wait_for_manifest_uid(Nodes, Bucket, BucketUuid, Uid, Timeout) ->
    {Ret, [], BadNodes} =
        misc:rpc_multicall_with_plist_result(
          Nodes, ?MODULE, wait_for_manifest_uid,
          [Bucket, BucketUuid, Uid, Timeout], Timeout),
    case BadNodes of
        [] ->
            case lists:all(fun ({_, ok}) -> true; (_) -> false end, Ret) of
                true ->
                    ok;
                false ->
                    check_for_stopped_bucket(Ret)
            end;
        _ ->
            check_for_stopped_bucket(Ret)
    end.

check_for_stopped_bucket(Ret) ->
    case lists:any(fun ({_, stopped}) -> true; (_) -> false end, Ret) of
        true ->
            stopped;
        false ->
            timeout
    end.
