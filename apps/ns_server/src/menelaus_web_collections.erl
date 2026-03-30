%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc rest api's for collections

-module(menelaus_web_collections).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_get/2,
         handle_post_scope/2,
         handle_post_collection/3,
         handle_delete_scope/3,
         handle_delete_collection/4,
         handle_patch_collection/4,
         handle_set_manifest/2,
         assert_api_available/1,
         get_formatted_err_msg/1,
         handle_ensure_manifest/3,
         name_validator/1]).

handle_get(Bucket, Req) ->
    External = is_req_external(Req),
    menelaus_util:reply_json(
      Req, collections:manifest_json_for_rest_response(
             menelaus_auth:get_authn_res(Req), Bucket, direct, External)).

handle_post_scope(Bucket, Req) ->
    assert_api_available(Bucket),

    validator:handle(
      fun (Values) ->
              Name = proplists:get_value(name, Values),
              RV = collections:create_scope(Bucket, Name),
              maybe_audit(RV, Req, ns_audit:create_scope(_, Bucket, Name, _)),
              maybe_add_event_log(RV, Bucket, []),
              handle_rv(RV, scope_create, Req)
      end, Req, form, scope_validators(default_not_allowed)).

maybe_audit({ok, {Uid, _}}, Req, SuccessfulAuditFun) ->
    SuccessfulAuditFun(Req, Uid);
maybe_audit({ok, Uid}, Req, SuccessfulAuditFun) ->
    SuccessfulAuditFun(Req, Uid);
maybe_audit(forbidden, Req, _SuccessfulAuditFun) ->
    ns_audit:access_forbidden(Req);
maybe_audit(_, _Req, _SuccessfulAuditFun) ->
    ok.

%% Add event logs for a specific set of operations.
get_event_and_attributes({create_scope, Name, _}) ->
    {scope_created, [{scope, Name}]};
get_event_and_attributes({drop_scope, Name}) ->
    {scope_deleted, [{scope, Name}]};
get_event_and_attributes({create_collection, Scope, Collection, _}) ->
    {collection_created, [{scope, Scope},
                          {collection, Collection}]};
get_event_and_attributes({drop_collection, Scope, Collection}) ->
    {collection_deleted, [{scope, Scope},
                          {collection, Collection}]};
get_event_and_attributes(_) ->
    ok.

maybe_add_event_log({ok, {Uid, OperationsDone}}, Bucket, Extra) ->
    JsonifyFun = fun (K, V) when is_list(V) ->
                         {K, list_to_binary(V)};
                     (K, V) ->
                         {K, V}
                 end,
    lists:foreach(fun (Operation) ->
                          case get_event_and_attributes(Operation) of
                              {Event, Attributes} ->
                                  AttrsJson =
                                      [JsonifyFun(K, V) ||
                                       {K, V} <- Attributes],
                                  event_log:add_log(
                                    Event,
                                    [{bucket, list_to_binary(Bucket)}] ++
                                    AttrsJson ++ Extra ++
                                    [{new_manifest_id, Uid}]);
                              _ ->
                                  ok
                          end
                  end, OperationsDone);
maybe_add_event_log(_, _, _) ->
    ok.

scope_validators(default_not_allowed) ->
    scope_validators([]);
scope_validators(special_allowed) ->
    scope_validators(["_default", ?SYSTEM_SCOPE_NAME] ++
                         collections:system_collections());
scope_validators(Exceptions) ->
    scope_validators_without_unsupported(Exceptions) ++
        [validator:unsupported(_)].

scope_validators_without_unsupported(Exceptions) ->
    [validator:required(name, _),
     validator:string(name, _),
     validator:length(name, 1, 251, _),
     name_validator(_),
     name_first_char_validator(_, Exceptions)].


%% "history" can only be true for magma buckets.
history_validator(BucketConfig, State) ->
    State1 = validator:touch(history, State),
    case validator:get_value(history, State1) of
        true ->
            case ns_bucket:is_magma(BucketConfig) of
                true ->
                    State1;
                false ->
                    menelaus_util:web_exception(
                      400, "Not allowed on this type of bucket")
            end;
        _ ->
            State1
    end.

collection_modifiable_validators(BucketConfig) ->
    HistoryAllowedValues =
        case cluster_compat_mode:is_enterprise() of
            true -> ["true", "false"];
            false -> ["false"]
        end,
    [validator:one_of(history, HistoryAllowedValues, _),
     validator:boolean(history, _),
     history_validator(BucketConfig, _),
     validator:integer(maxTTL, collections:get_maxTTL_min_value(),
                       ?MAX_32BIT_SIGNED_INT, _),
     validator:valid_in_enterprise_only(maxTTL, _),
     validator:no_duplicate_keys(_)
    ].

collection_validators(DefaultAllowed, BucketConfig) ->
    collection_modifiable_validators(BucketConfig) ++
        scope_validators(DefaultAllowed).

external_collection_validators() ->
    [validator:no_duplicate_keys(_),
     validator:prohibited(rev, _)]
        ++ scope_validators_without_unsupported([]).

handle_post_collection(Bucket, Scope, Req) ->
    assert_api_available(Bucket),

    maybe
        {ok, BucketConf} ?= ns_bucket:get_bucket(Bucket),
        ExternalCollection = is_req_external(Req),

        case ExternalCollection of
            false ->
                handle_post_couchbase_collection(Bucket, Scope, Req,
                                                 BucketConf);
            true ->
                handle_post_external_collection(Bucket, Scope, Req)
        end
    else
        not_present ->
            handle_rv({bucket_not_found, Bucket}, collection_create, Req)
    end.

handle_post_couchbase_collection(Bucket, Scope, Req, BucketConf) ->
    validator:handle(
      fun (Values) ->
              Name = proplists:get_value(name, Values),
              RV = collections:create_collection(
                     Bucket, Scope, Name, proplists:delete(name, Values)),
              maybe_audit(RV, Req,
                          ns_audit:create_collection(_, Bucket, Scope, Name,
                                                     _)),
              maybe_add_event_log(RV, Bucket, []),
              handle_rv(RV, collection_create, Req)
      end, Req, form,
      collection_validators(default_not_allowed, BucketConf)).

handle_post_external_collection(Bucket, Scope, Req) ->
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Values) ->
              CollectionParams = collection_params(Values),
              case validate_external_collection_with_service(
                     Bucket, Scope, CollectionParams) of
                  {ok, ServiceOKs} ->
                      Name = proplists:get_value(name, Values),
                      Props = build_collection_props(
                                proplists:delete(name, Values), ServiceOKs),
                      RV = collections:create_external_collection(
                             Bucket, Scope, Name, Props),
                      maybe_audit(RV, Req,
                                  ns_audit:create_collection(
                                    _, Bucket, Scope, Name, _)),
                      maybe_add_event_log(RV, Bucket, []),
                      handle_rv(RV, collection_create, Req);
                  no_query_node ->
                      reply_global_error(
                        Req,
                        "Must have a query node to configure an external "
                        "collection",
                        400);
                  {errors, Errors} ->
                      reply_validation_errors(Req, Errors)
              end
      end, Req, form,
      external_collection_validators()).

handle_patch_collection(Bucket, Scope, Name, Req) ->
    assert_api_available(Bucket),

    maybe
        {ok, BucketConf} ?= ns_bucket:get_bucket(Bucket),
        ExternalCollection = is_req_external(Req),
        case ExternalCollection of
            false ->
                handle_patch_couchbase_collection(Bucket, Scope, Name, Req,
                                                  BucketConf);
            true ->
                handle_patch_external_collection(Bucket, Scope, Name, Req)
        end
    else
        not_present ->
            handle_rv({bucket_not_found, Bucket}, collection_patch, Req)
    end.

handle_patch_couchbase_collection(Bucket, Scope, Name, Req, BucketConf) ->
    validator:handle(
      fun (Values) ->
              RV = collections:modify_collection(
                     Bucket, Scope, Name, proplists:delete(name, Values)),
              maybe_audit(RV, Req,
                          ns_audit:modify_collection(_, Bucket, Scope, Name,
                                                     _)),
              maybe_add_event_log(RV, Bucket, []),
              handle_rv(RV, collection_patch, Req)
      end, Req, form,
      collection_modifiable_validators(BucketConf) ++
          %% Don't allow any other params
          [validator:unsupported(_)]).

handle_patch_external_collection(Bucket, Scope, Name, Req) ->
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Values) ->
              CollectionParams =
                  collection_params(
                    [{name, Name} | Values]),
              case validate_external_collection_with_service(
                     Bucket, Scope, CollectionParams) of
                  {ok, ServiceOKs} ->
                      Props = build_collection_props(
                                Values,
                                ServiceOKs),
                      RV = collections:modify_external_collection(
                             Bucket, Scope, Name, Props),
                      maybe_audit(
                        RV, Req,
                        ns_audit:modify_collection(
                          _, Bucket, Scope, Name, _)),
                      maybe_add_event_log(RV, Bucket, []),
                      handle_rv(RV, collection_patch, Req);
                  {errors, Errors} ->
                      reply_validation_errors(Req, Errors)
              end
      end, Req, form,
      [validator:prohibited(name, _),
       validator:prohibited(rev, _),
       validator:no_duplicate_keys(_)]).

handle_delete_scope(Bucket, Name, Req) ->
    assert_api_available(Bucket),
    RV = collections:drop_scope(Bucket, Name),
    maybe_audit(RV, Req, ns_audit:drop_scope(_, Bucket, Name, _)),
    maybe_add_event_log(RV, Bucket, []),
    handle_rv(RV, scope_delete, Req).

handle_delete_collection(Bucket, Scope, Name, Req) ->
    assert_api_available(Bucket),
    ExternalCollection = is_req_external(Req),
    RV =
        case ExternalCollection of
            false -> collections:drop_collection(Bucket, Scope, Name);
            true ->
                menelaus_util:assert_is_totoro(),
                collections:drop_external_collection(Bucket, Scope, Name)
        end,
    maybe_audit(RV, Req, ns_audit:drop_collection(_, Bucket, Scope, Name, _)),
    maybe_add_event_log(RV, Bucket, []),
    handle_rv(RV, collection_delete, Req).

handle_set_manifest(Bucket, Req) ->
    assert_api_available(Bucket),

    ValidOnUid = proplists:get_value("validOnUid",
                                     mochiweb_request:parse_qs(Req)),
    maybe
        {ok, BucketConf} ?= ns_bucket:get_bucket(Bucket),
        validator:handle(
          fun (KVList) ->
                  Scopes = proplists:get_value(scopes, KVList),
                  AuthnRes = menelaus_auth:get_authn_res(Req),
                  RV = collections:set_manifest(Bucket, AuthnRes, Scopes,
                                                ValidOnUid),
                  InputManifest = mochiweb_request:recv_body(Req),
                  maybe_audit(RV, Req,
                              ns_audit:set_manifest(_, Bucket, InputManifest,
                                                    ValidOnUid, _)),
                  %% Add event logs for each of the specific operation performed
                  maybe_add_event_log(RV, Bucket, []),
                  handle_rv(RV, manifest_set, Req)
          end, Req, json,
          [validator:required(scopes, _),
           validate_scopes(scopes, BucketConf, _),
           check_duplicates(scopes, _),
           validator:unsupported(_)])
    else
        not_present ->
            handle_rv(
              {error,
               io_lib:format("Could not get config for Bucket ~p", [Bucket])},
              manifest_set, Req)
    end.

check_duplicates(Name, State) ->
    validator:validate(
      fun (JsonArray) ->
              Names = [proplists:get_value(name, Props) ||
                          {Props} <- JsonArray],
              case length(Names) =:= length(lists:usort(Names)) of
                  true ->
                      ok;
                  false ->
                      {error, "Contains duplicate name"}
              end
      end, Name, State).

validate_scopes(Name, BucketConfig, State) ->
    validator:json_array(
      Name, [validate_collections(collections, BucketConfig, _),
             check_duplicates(collections, _) |
             scope_validators(special_allowed)],
      State).

validate_collections(Name, BucketConfig, State) ->
    validator:json_array(Name,
                         collection_validators(special_allowed, BucketConfig),
                         State).

handle_ensure_manifest(Bucket, Uid, Req) ->
    assert_api_available(Bucket),
    UidInt = convert_uid(Uid),
    Snapshot = ns_bucket:get_snapshot(Bucket, [uuid, props]),
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket, Snapshot),

    BucketNodes = ns_bucket:live_bucket_nodes_from_config(BucketConfig),
    BucketUuid = ns_bucket:uuid(Bucket, Snapshot),

    validator:handle(
      fun (Values) ->
              Nodes = proplists:get_value(nodes, Values, BucketNodes),
              Timeout = proplists:get_value(timeout, Values, 30000),
              case collections:wait_for_manifest_uid(
                     Nodes, Bucket, BucketUuid, UidInt, Timeout) of
                  ok ->
                      menelaus_util:reply(Req, 200);
                  timeout ->
                      menelaus_util:reply_text(Req, "timeout", 504);
                  stopped ->
                      menelaus_util:reply_text(Req, "bucket no longer exists",
                                               404)
              end
      end, Req, form,
      [validator:integer(timeout, 0, 300000, _),
       nodes_validator(BucketNodes, Req, _),
       validator:unsupported(_)]).

assert_api_available(Bucket) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    case collections:enabled(BucketConfig) of
        true ->
            ok;
        false ->
            menelaus_util:web_exception(
              400, "Not allowed on this type of bucket")
    end.

name_first_char_validator(State, Exceptions) ->
    validator:validate(
      fun ([Char | _] = Elem) ->
              case lists:member(Char, "_%") of
                  true ->
                      case lists:member(Elem, Exceptions) of
                          false ->
                              {error, "First character must not be _ or %"};
                          true ->
                              ok
                      end;
                  false ->
                      ok
              end
      end, name, State).

name_validator(State) ->
    validator:string(
      name, "^[0-9A-Za-z_%\-]+$", [dollar_endonly],
      "Can only contain characters A-Z, a-z, 0-9 and the following symbols "
      "_ - %", State).

convert_uid(Uid) ->
    try collections:convert_uid_from_memcached(Uid) of
        UidInt when UidInt < 0 ->
            menelaus_util:web_exception(400, "Invalid UID");
        UidInt ->
            UidInt
    catch error:badarg ->
            menelaus_util:web_exception(400, "Invalid UID")
    end.

nodes_validator(BucketNodes, Req, State) ->
    validator:validate(
      fun (NodesStr) ->
              Nodes = string:split(NodesStr, ",", all),
              {Good, Bad} =
                  lists:foldl(
                    fun (HostPort, {G, B}) ->
                            case menelaus_web_node:find_node_hostname(
                                   HostPort, Req) of
                                {error, _} ->
                                    {G, [HostPort | B]};
                                {ok, Node} ->
                                    case lists:member(Node, BucketNodes) of
                                        true ->
                                            {[Node | G], B};
                                        false ->
                                            {G, [HostPort | B]}
                                    end
                            end
                    end, {[], []}, Nodes),
              case Bad of
                  [] ->
                      {value, Good};
                  _ ->
                      {error, "Invalid nodes : " ++ string:join(Bad, ",")}
              end
      end, nodes, State).

get_err_code_msg(forbidden) ->
    {"Operation is not allowed due to insufficient permissions", 403};
get_err_code_msg(invalid_uid) ->
    {"Invalid validOnUid", 400};
get_err_code_msg(uid_mismatch) ->
    {"validOnUid doesn't match the current manifest Uid", 400};
get_err_code_msg({cannot_create_default_collection, ScopeName}) ->
    {"Cannot create _default collection in scope ~p. Creation of _default "
     "collection is not allowed.", [ScopeName], 400};
get_err_code_msg({cannot_create_collection_in_system_scope}) ->
    {"cannot create collection in '_system' scope", 400};
get_err_code_msg({cannot_drop_system_collection, Scope, Collection}) ->
    {"Cannot drop system collection ~p for scope ~p",
     [Collection, Scope], 400};
get_err_code_msg({cannot_modify_collection, Scope, Collection}) ->
    {"Cannot modify collection properties for Scope ~p Collection ~p",
     [Scope, Collection], 400};
get_err_code_msg({scope_already_exists, ScopeName}) ->
    {"Scope with name ~p already exists", [ScopeName], 400};
get_err_code_msg({collection_already_exists, ScopeName, CollectionName}) ->
    {"Collection with name ~p in scope ~p already exists",
     [CollectionName, ScopeName], 400};
get_err_code_msg({collection_has_history, Name,
                  storage_mode_migration_in_progress}) ->
    {"Cannot create collection (~p) with history enabled while storage mode "
     "migration is in progress", [Name], 400};
get_err_code_msg({cannot_modify_history, Name,
                  storage_mode_migration_in_progress}) ->
    {"Cannot enable history for collection (~p), while storage mode migration "
     " is in progress", [Name], 400};
get_err_code_msg({collection_not_found, ScopeName, CollectionName}) ->
    {"Collection with name ~p in scope ~p is not found",
     [CollectionName, ScopeName], 404};
get_err_code_msg({scope_not_found, ScopeName}) ->
    {"Scope with name ~p is not found", [ScopeName], 404};
get_err_code_msg(cannot_drop_default_scope) ->
    {"Deleting _default scope is not allowed", 400};
get_err_code_msg(cannot_drop_system_scope) ->
    {"Deleting _system scope is not allowed", 400};
get_err_code_msg({bucket_limit, max_number_exceeded, num_collections, Num}) ->
    {"Maximum number of collections (~p) for this bucket has been reached",
     [Num], 429};
get_err_code_msg({bucket_limit, max_number_exceeded, num_scopes, Num}) ->
    {"Maximum number of scopes (~p) for this bucket has been reached",
     [Num], 429};
get_err_code_msg({max_number_exceeded, num_scopes}) ->
    {"Maximum number of scopes has been reached", 400};
get_err_code_msg({max_number_exceeded, num_collections}) ->
    {"Maximum number of collections has been reached", 400};
get_err_code_msg(exceeded_retries) ->
    {"Exceeded retries due to conflicting operations on bucket properties.",
     503};
get_err_code_msg({nodes_are_behind, Nodes}) ->
    {"Operation is not possible due to following nodes being too far behind: "
     "~p.", [Nodes], 503};
get_err_code_msg(unfinished_failover) ->
    {"Operation is not possible during unfinished failover.", 503};
get_err_code_msg({bucket_not_found, Bucket}) ->
    {"Bucket with name ~p not found", [Bucket], 404};
get_err_code_msg(Error) ->
    {"Unknown error ~p", [Error], 400}.

get_formatted_err_msg(Error) ->
    case get_err_code_msg(Error) of
        {Msg, Code} -> {Msg, Code};
        {Msg, Params, Code} -> {io_lib:format(Msg, Params), Code}
    end.

handle_rv({ok, {Uid, _}}, Type, Req) ->
    handle_rv({ok, Uid}, Type, Req);
handle_rv({ok, Uid}, _Type, Req) ->
    menelaus_util:reply_json(Req, {[{uid, Uid}]}, 200);
handle_rv({errors, List}, Type, Req) when is_list(List) ->
    Errors = lists:map(fun (Elem) ->
                               {Msg, _} = get_formatted_err_msg(Elem),
                               Msg
                       end, lists:usort(List)),
    ns_server_stats:notify_counter({<<"rest_request_failure">>,
                                    [{type, Type}, {code, 400}]}),
    menelaus_util:reply_json(Req, {[{errors, Errors}]}, 400);
handle_rv(Error, Type, Req) ->
    {Msg, Code} = get_formatted_err_msg(Error),
    ns_server_stats:notify_counter({<<"rest_request_failure">>,
                                    [{type, Type}, {code, Code}]}),
    reply_global_error(Req, Msg, Code).

reply_global_error(Req, Msg, Code) ->
    menelaus_util:reply_json(
      Req, {[{errors, {[{<<"_">>, iolist_to_binary(Msg)}]}}]}, Code).

is_req_external(Req) ->
    proplists:get_value("external", mochiweb_request:parse_qs(Req)) =:= "1".

%% External collection validation

validate_external_collection_with_service(Bucket, Scope, Params) ->
    CollectionConfig = {menelaus_web_external_catalogs:binary_params(Params) ++
                            [{bucket, list_to_binary(Bucket)},
                             {scope, list_to_binary(Scope)}]},
    Options =
        #{clusterCompatVersion =>
              cluster_compat_mode:effective_cluster_compat_version()},
    Nodes = get_query_nodes(),
    case Nodes of
        [] ->
            no_query_node;
        _ ->
            validate_collection_against_query_nodes(
              Nodes, CollectionConfig, Options)
    end.

get_query_nodes() ->
    AllNodes = ns_cluster_membership:service_active_nodes(n1ql),
    ns_node_disco:only_live_nodes(AllNodes).

validate_collection_against_query_nodes([FirstNode | _], CollectionConfig,
                                        Options) ->
    maybe
        %% Whilst we could validate on all nodes, there is currently no need.
        %% The code should handle multiple already though, just in case.
        {ok, Results} ?=
            service_agent:validate_external_collection_config(
              n1ql, [FirstNode], CollectionConfig, Options),

        ValidationOptions = #{filter_internal => false,
                              filter_unsupported => false},
        {ok, {OKs, Errors}} =
            delegated_config:process_service_api_validation_results(
              Results, ValidationOptions),

        %% Cluster_tests need a way to test as much of the code as possible,
        %% without relying on other components (i.e. knowing the config that
        %% query supports). We can inject our own values for the sake of
        %% testing.
        ForcedValidationResults =
            ns_config:read_key_fast(
              forced_external_collection_validation_results, #{}),

        case maps:size(ForcedValidationResults) of
            0 ->
                case maps:size(Errors) of
                    0 -> {ok, OKs};
                    _ -> {errors, maps:to_list(Errors)}
                end;
            _ ->
                {ok, ForcedValidationResults}
        end
    else
        {error, Error} ->
            ?log_error(
               "Error validating external collection "
               "config with query service: ~p",
               [Error]),
            {errors, [{<<"_">>,
                       <<"Service validation failed">>}]}
    end.

collection_params(Values) ->
    lists:filter(
      fun ({name, _}) -> false;
          ({external_collection, _}) -> false;
          (_) -> true
      end, Values).

build_collection_props(Values, ServiceOKs) ->

    %% Filter down the OKsFromServices to only Params we wanted
    %% to set, since the returned list is all parameters, not just
    %% the ones we wanted to set.
    lists:filtermap(
      fun ({Key, Value}) ->
              BKey = list_to_binary(Key),
              case maps:is_key(BKey, ServiceOKs) of
                  false -> false;
                  true -> {true, {BKey, list_to_binary(Value)}}
              end
      end, Values).

reply_validation_errors(Req, Errors) ->
    ErrorJson =
        {[{Key, Msg} || {Key, Msg} <- Errors]},
    menelaus_util:reply_json(
      Req, {[{errors, ErrorJson}]}, 400).

-ifdef(TEST).
bucket_config_not_found_when_posting_collections_test() ->
    meck:new(collections, [passthrough]),
    meck:expect(collections, enabled, fun(_) -> true end),

    % We need to pass the first get_bucket call and fail the second, we can use
    % meck:seq to specify a sequence of return values
    meck:new(ns_bucket),
    meck:expect(ns_bucket, get_bucket, 1, meck:seq([{ok, []}, not_present])),

    meck:new(menelaus_auth),
    meck:expect(menelaus_auth, get_identity, fun (_) -> ok end),

    % Matching the function clause in the expect here is the test
    meck:new(menelaus_util),
    meck:expect(menelaus_util,
                reply_json,
                fun (_,
                     {[{errors, {[{<<"_">>,
                                   <<"Bucket with name [] not found">>}]}}]},
                     404) -> ok
                end),

    % Values passed in here don't matter, we're mocking everything we need
    handle_post_collection([],[],[]),

    meck:unload(menelaus_util),
    meck:unload(menelaus_auth),
    meck:unload(ns_bucket),
    meck:unload(collections).
-endif.
