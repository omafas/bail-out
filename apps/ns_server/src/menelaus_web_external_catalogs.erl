%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in
%% that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%% @doc REST API for managing external catalogs

-module(menelaus_web_external_catalogs).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-export([handle_get_catalogs/1,
         handle_get_catalog/2,
         handle_post_catalog/1,
         handle_put_catalog/2,
         handle_patch_catalog/2,
         handle_delete_catalog/2]).

-define(CHRONICLE_KEY, external_catalogs).
-define(MAX_NAME_LENGTH, 256).

handle_get_catalogs(Req) ->
    menelaus_util:assert_is_totoro(),
    Catalogs = get_catalogs(),

    menelaus_util:reply_json(Req, format_catalogs(Catalogs)).

handle_get_catalog(Name, Req) ->
    menelaus_util:assert_is_totoro(),
    BinName = list_to_binary(Name),
    case find_catalog(BinName, get_catalogs()) of
        {ok, Catalog} ->
            menelaus_util:reply_json(
              Req, format_catalog(Catalog));
        not_found ->
            menelaus_util:reply_not_found(Req)
    end.

handle_post_catalog(Req) ->
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Params) ->
              %% It's easier to treat everything as binaries.
              BinaryParams = binary_params(Params),
              Name = proplists:get_value(name, BinaryParams),
              case validate_with_service(BinaryParams) of
                  {ok, ServiceOKs} ->
                      Catalog = build_catalog(
                                  ServiceOKs,
                                  BinaryParams),
                      case add_catalog(Name, Catalog) of
                          {ok, _} ->
                              menelaus_util:reply_json(
                                Req,
                                format_catalog(Catalog),
                                200);
                          already_exists ->
                              reply_conflict(Req, Name)
                      end;
                  {errors, Errors} ->
                      reply_validation_errors(
                        Req, Errors)
              end
      end, Req, form, name_validators()).

handle_put_catalog(Name, Req) ->
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Params) ->
              %% Name is prohibited in params, so we can append it safely.
              BinaryParams = binary_params([{name, Name} | Params]),
              BinName = proplists:get_value(name, BinaryParams),
              case validate_with_service(BinaryParams) of
                  {ok, ServiceOKs} ->
                      Updated = build_catalog(
                                  ServiceOKs,
                                  BinaryParams),
                      case replace_catalog(
                             BinName, Updated) of
                          {ok, _} ->
                              menelaus_util:reply_json(
                                Req,
                                format_catalog(Updated),
                                200);
                          not_found ->
                              menelaus_util:reply_not_found(
                                Req)
                      end;
                  {errors, Errors} ->
                      reply_validation_errors(
                        Req, Errors)
              end
      end, Req, form, [validator:prohibited(name, _)]).

handle_patch_catalog(Name, Req) ->
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Params) ->
              %% Name is prohibited in params, so we can append it safely.
              BinaryParams = binary_params([{name, Name} | Params]),
              BinName = proplists:get_value(name, BinaryParams),

              maybe
                  Catalogs = get_catalogs(),
                  {ok, ExistingCatalog} ?= find_catalog(BinName, Catalogs),

                  ExistingExtra =
                      proplists:get_value(
                        extra_params, ExistingCatalog, []),
                  AllProps = misc:update_proplist(ExistingExtra, BinaryParams),

                  {ok, ServiceOKs} ?= validate_with_service(AllProps),

                  Updated = build_catalog(ServiceOKs, AllProps),

                  %% TODO: This is racy, someone else could update the catalog
                  %% in the mean time. We are adding a rev in a future change
                  %% though, and we can use that to either retry or fail.
                  {ok, _} ?= replace_catalog(BinName, Updated),

                  menelaus_util:reply_json(Req,
                                           format_catalog(Updated),
                                           200)
              else
                  not_found ->
                      menelaus_util:reply_not_found(Req);
                  {errors, Errors} ->
                      reply_validation_errors(Req, Errors)
              end
      end,
      Req, form,
      [validator:prohibited(name, _)]).

handle_delete_catalog(Name, Req) ->
    menelaus_util:assert_is_totoro(),
    case delete_catalog(list_to_binary(Name)) of
        {ok, _} ->
            menelaus_util:reply_json(Req, [], 200);
        not_found ->
            menelaus_util:reply_not_found(Req)
    end.

%% Service validation

validate_with_service(ExtraParams) ->
    WithCompat =
        [{compat_version,
          cluster_compat_mode:effective_cluster_compat_version()} |
         ExtraParams],
    Nodes = get_query_nodes(),
    case Nodes of
        [] ->
            {ok, #{}};
        _ ->
            validate_against_query_nodes(
              Nodes, {WithCompat}, #{})
    end.

get_query_nodes() ->
    AllNodes =
        ns_cluster_membership:service_active_nodes(
          n1ql),
    ns_node_disco:only_live_nodes(AllNodes).

validate_against_query_nodes([FirstNode | _], CatalogConfig,
                             ServiceOptions) ->
    maybe
        %% Whilst we could validate on all nodes, there is currently no need.
        %% The code should handle multiple already though, just in case.
        {ok, Results} = service_agent:validate_external_catalog_config(
                          n1ql, [FirstNode], CatalogConfig, ServiceOptions),

        ValidationOptions = #{filter_internal => false,
                              filter_unsupported => false},
        {ok, {OKs, Errors}} =
            delegated_config:process_service_api_validation_results(
              Results,
              ValidationOptions),

        %% Cluster_tests need a way to test as much of the code as possible,
        %% without relying on other components (i.e. knowing the config that
        %% query supports). We can inject our own values for the sake of
        %% testing.
        ForcedValidationResults =
            ns_config:read_key_fast(forced_external_catalog_validation_results,
                                    #{}),

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
               "Error validating external catalog config with query service: "
               "~p",
                [Error]),
            {errors, [{<<"_">>, <<"Service validation failed">>}]}
    end.

%% Chronicle operations

get_catalogs() ->
    get_catalogs(direct).

get_catalogs(Snapshot) ->
    chronicle_compat:get(Snapshot, ?CHRONICLE_KEY,
                         #{default => #{}}).

add_catalog(Name, Catalog) ->
    chronicle_kv:transaction(
      kv, [?CHRONICLE_KEY],
      fun (Snapshot) ->
              Catalogs = get_catalogs(Snapshot),
              case find_catalog(Name, Catalogs) of
                  {ok, _} ->
                      {abort, already_exists};
                  not_found ->
                      {commit,
                       [{set, ?CHRONICLE_KEY,
                         Catalogs#{Name => Catalog}}]}
              end
      end).

replace_catalog(Name, NewCatalog) ->
    chronicle_kv:transaction(
        kv, [?CHRONICLE_KEY],
        fun(Snapshot) ->
            Catalogs = get_catalogs(Snapshot),
            case find_catalog(Name, Catalogs) of
                not_found ->
                    {abort, not_found};
                {ok, _} ->
                    NewCatalogs = Catalogs#{Name => NewCatalog},
                    {commit,
                        [{set, ?CHRONICLE_KEY, NewCatalogs}]}
            end
        end).

delete_catalog(Name) ->
    chronicle_kv:transaction(
      kv, [?CHRONICLE_KEY],
      fun (Snapshot) ->
              Catalogs = get_catalogs(Snapshot),
              case find_catalog(Name, Catalogs) of
                  not_found ->
                      {abort, not_found};
                  {ok, _} ->
                      NewCatalogs = maps:remove(Name, Catalogs),
                      {commit,
                       [{set, ?CHRONICLE_KEY, NewCatalogs}]}
              end
      end).

%% Internal helpers

find_catalog(Name, Catalogs) ->
    case maps:find(Name, Catalogs) of
        error -> not_found;
        R -> R
    end.

format_catalog(Catalog) ->
    ExtraParams = proplists:get_value(extra_params, Catalog, []),
    {ExtraParams}.

format_catalogs(Catalogs) when is_map(Catalogs) ->
    {maps:to_list(maps:map(fun (_K, V) -> format_catalog(V) end, Catalogs))}.

build_catalog(ServiceOKs, Params) ->
    %% Filter down the OKsFromServices to only Params we wanted
    %% to set, since the returned list is all parameters, not just
    %% the ones we wanted to set.
    ExtraParams = lists:filtermap(
                    fun ({Key, Value}) ->
                            case maps:is_key(Key, ServiceOKs) of
                                false -> false;
                                true -> {true, {Key, Value}}
                            end
                    end, Params),

    [{extra_params, ExtraParams}].

binary_params(Params) ->
    lists:foldl(
        fun({name, V}, Acc) ->
            [{name, list_to_binary(V)} | Acc];
            ({K, V}, Acc) ->
                [{list_to_binary(K), list_to_binary(V)} | Acc]
            end, [], Params).

reply_conflict(Req, Name) ->
    menelaus_util:reply_json(
      Req,
      iolist_to_binary(
        io_lib:format(
          "External catalog '~s' already exists",
          [Name])),
      409).

reply_validation_errors(Req, Errors) ->
    ErrorJson =
        {[{Key, Msg} || {Key, Msg} <- Errors]},
    menelaus_util:reply_json(
      Req, {[{errors, ErrorJson}]}, 400).

name_validators() ->
    [validator:required(name, _),
     menelaus_web_collections:name_validator(_),
     validator:length(name, 1, ?MAX_NAME_LENGTH, _)].
