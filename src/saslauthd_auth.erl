%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(saslauthd_auth).

-include("ns_common.hrl").

-export([build_settings/0,
         set_settings/1,
         authenticate/2
        ]).

verify_creds(Username, Password) ->
    case json_rpc_connection:perform_call("saslauthd-saslauthd-port", "SASLDAuth.Check",
                                          {[{user, list_to_binary(Username)},
                                            {password, list_to_binary(Password)}]}) of
        {ok, Resp} ->
            Resp =:= true;
        {error, ErrorMsg} ->
            ?log_error("Revrpc to saslauthd returned error: ~p", [ErrorMsg]),
            false
    end.

build_settings() ->
    case ns_config:search(saslauthd_auth_settings) of
        {value, Settings} ->
            Settings;
        false ->
            [{enabled, false},
             {admins, []},
             {roAdmins, []}]
    end.

set_settings(Settings) ->
    ns_config:set(saslauthd_auth_settings, Settings).

authenticate(Username, Password) ->
    case os:getenv("BYPASS_SASLAUTHD") of
        "1" ->
            true;
        _ ->
            do_authenticate(Username, Password)
    end.

do_authenticate(User, Password) ->
    Enabled = ns_config:search_prop(ns_config:latest(), saslauthd_auth_settings, enabled, false),
    case Enabled of
        false ->
            false;
        true ->
            verify_creds(User, Password)
    end.
