%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(roles_cache).

-behaviour(active_cache).

%% API
-export([start_link/0, build_compiled_roles/1, renew/0]).

%% callbacks
-export([init/1, translate_options/1]).

-include("cut.hrl").
-include("ns_common.hrl").
-include("ns_config.hrl").

-define(DEFAULT_MAX_PARALLEL_PROCESSES, 100).
-define(DEFAULT_MAX_CACHE_SIZE, 10000).
-define(DEFAULT_VALUE_LIFETIME, 24*60*60*1000).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    Get = ?cut({_1, ns_config:read_key_fast(_1, ?DELETED_MARKER)}),
    Settings = [Get(external_user_roles_cache_size),
                Get(external_user_roles_parrallel_procs),
                Get(external_user_roles_cache_expiration_timeout),
                {external_auth_polling_interval,
                 menelaus_roles:external_auth_polling_interval()}],
    Opts = [opt(P) || P <- Settings],
    active_cache:start_link(?MODULE, ?MODULE, [], Opts).

build_compiled_roles(Identity) ->
    Key = {?FUNCTION_NAME, Identity},
    Fun = fun () -> menelaus_roles:build_compiled_roles(Identity) end,
    active_cache:get_value_and_touch(?MODULE, Key, Fun).

renew() ->
    active_cache:renew_cache(?MODULE).

%%%===================================================================
%%% callbacks
%%%===================================================================

init([]) ->
    Self = self(),
    EventHandler =
        fun ({P, _} = Event)
            when P =:= external_user_roles_cache_size;
                 P =:= external_user_roles_parrallel_procs;
                 P =:= external_user_roles_cache_expiration_timeout;
                 P =:= external_auth_polling_interval ->
                active_cache:reload_opts(Self, [Event]);
            ({ldap_settings, _}) ->
                %% Ldap cache might receive this notification later
                %% but we need to make sure it is flushed before
                %% we renew the upper level cache
                ldap_auth_cache:remote_flush(ns_node_disco:ns_server_node()),
                active_cache:renew_cache(Self);
            ({group_version, _}) ->
                active_cache:renew_cache(Self);
            ({user_version, _}) ->
                active_cache:renew_cache(Self);
            (_) -> ok
        end,
    ns_pubsub:subscribe_link(ns_config_events, EventHandler),
    UserEvents =
        case ns_node_disco:couchdb_node() == node() of
            true ->
                dist_manager:wait_for_node(fun ns_node_disco:ns_server_node/0),
                {user_storage_events, ns_node_disco:ns_server_node()};
            false ->
                user_storage_events
        end,
    ns_pubsub:subscribe_link(UserEvents, EventHandler),

    %% Flush the roles cache when a bucket, collection or scope changes.
    chronicle_compat_events:subscribe(
      fun (cluster_compat_version) ->
              true;
          (Key) ->
              ns_bucket:buckets_change(Key)
                  orelse (collections:key_match(Key) =/= false)
      end,
      fun(_) ->
              active_cache:flush(Self)
      end),
    ok.

translate_options([Opt]) -> [opt(Opt)].

%%%===================================================================
%%% Internal functions
%%%===================================================================

opt({external_user_roles_cache_size, ?DELETED_MARKER}) ->
    {max_size, ?DEFAULT_MAX_CACHE_SIZE};
opt({external_user_roles_cache_size, V}) ->
    {max_size, V};
opt({external_user_roles_parrallel_procs, ?DELETED_MARKER}) ->
    {max_parallel_procs, ?DEFAULT_MAX_PARALLEL_PROCESSES};
opt({external_user_roles_parrallel_procs, V}) ->
    {max_parallel_procs, V};
opt({external_user_roles_cache_expiration_timeout, ?DELETED_MARKER}) ->
    {value_lifetime, ?DEFAULT_VALUE_LIFETIME};
opt({external_user_roles_cache_expiration_timeout, V}) ->
    {value_lifetime, V};
opt({external_auth_polling_interval, _}) ->
    Val = menelaus_roles:external_auth_polling_interval(),
    {renew_interval, round(0.75*Val)}.
