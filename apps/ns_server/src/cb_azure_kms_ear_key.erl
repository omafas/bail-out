%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_azure_kms_ear_key).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").

-export([persist/3,
         test_props/3]).

-export_type([secret_props/0]).

-type secret_props() :: #{key_url := string(),
                          encryption_algorithm := string(),
                          credentials_chain := string(),
                          req_timeout_ms := integer(),
                          stored_ids := [cb_kms_ear_key:key_props()],
                          last_rotation_time := calendar:datetime()}.

-spec persist(secret_props(), binary(), list()) -> ok | {error, _}.
persist(Props, _ExtraAD, _) ->
    ensure_azure_kek_on_disk(Props, false).

-spec test_props(secret_props(), binary(), list()) -> ok | {error, _}.
test_props(#{stored_ids := [StoredId | _]} = Props, _ExtraAD, _) ->
    %% We don't want to test all stored ids
    PropsWithoutHistKeys = Props#{stored_ids => [StoredId]},
    case ensure_azure_kek_on_disk(PropsWithoutHistKeys, true) of
        ok -> ok;
        {error, [{_, Reason}]} -> {error, Reason};
        {error, _} = E -> E
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

ensure_azure_kek_on_disk(#{stored_ids := StoredIds} = Props, TestOnly) ->
    Params = maps:with([key_url, encryption_algorithm, credentials_chain,
                        req_timeout_ms], Props),
    Res = lists:map(
            fun (#{id := Id, creation_time := CreationTime}) ->
                {Id, encryption_service:store_azure_key(Id, Params,
                                                        CreationTime, TestOnly)}
            end, StoredIds),
    misc:many_to_one_result(Res).