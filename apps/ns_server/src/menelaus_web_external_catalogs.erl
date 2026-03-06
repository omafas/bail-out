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
              Name = list_to_binary(proplists:get_value(name, Params)),
              %% For now we just support the name (map key), more will be added
              %% soon.
              Catalog = [],
              case add_catalog(Name, Catalog) of
                  {ok, _} ->
                      menelaus_util:reply_json(
                        Req, format_catalog(Catalog), 200);
                  already_exists ->
                      menelaus_util:reply_json(
                        Req,
                        iolist_to_binary(
                          io_lib:format(
                            "External catalog '~s' "
                            "already exists", [Name])),
                        409)
              end
      end, Req, form, name_validators()).

handle_put_catalog(Name, Req) ->
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (_Params) ->
              %% This one doesn't make much sense yet, not til we add support
              %% for service params
              BinName = list_to_binary(Name),
              Catalog = [],
              case replace_catalog(BinName, Catalog) of
                  {ok, _} ->
                      menelaus_util:reply_json(
                        Req,
                        format_catalog(Catalog),
                        200);
                  not_found ->
                      menelaus_util:reply_not_found(Req)
              end
      end, Req, form, [validator:prohibited(name, _)]).

handle_delete_catalog(Name, Req) ->
    menelaus_util:assert_is_totoro(),
    case delete_catalog(list_to_binary(Name)) of
        {ok, _} ->
            menelaus_util:reply_json(Req, [], 200);
        not_found ->
            menelaus_util:reply_not_found(Req)
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
    {Catalog}.

format_catalog(Name, Catalog) ->
    {[{Name, format_catalog(Catalog)}]}.

format_catalogs(Catalogs) when is_map(Catalogs) ->
    {maps:to_list(maps:map(fun format_catalog/2, Catalogs))}.


name_validators() ->
    [validator:required(name, _),
     menelaus_web_collections:name_validator(_),
     validator:length(name, 1, ?MAX_NAME_LENGTH, _)].
