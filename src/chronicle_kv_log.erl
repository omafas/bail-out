%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(chronicle_kv_log).

-behaviour(gen_server2).

-export([sanitize/2]).
-export([sanitize_snapshot/2, sanitize_log/2]).

%% gen_server callbacks:
-export([start_link/0, init/1, handle_info/2]).

-include("ns_common.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(
      chronicle_compat_events:kv_event_manager(),
      fun ({{key, K}, R, {updated, V}}) ->
              Self ! {{key, K}, R, {updated, fun() -> V end}};
          ({_, _, _} = Evt) ->
              Self ! Evt;
          (_) ->
              ok
      end),
    {ok, #{}}.

handle_info({{key, K}, R, {updated, VFun}}, State) ->
    NewState = log(K, VFun, R, State),
    {noreply, NewState, hibernate};
handle_info({{key, K}, R, deleted}, State) ->
    ?log_debug("delete (key: ~p, rev: ~p)", [K, R]),
    {noreply, maps:remove(K, State), hibernate};
handle_info(Info, State) ->
    ?log_warning("Unexpected message(~p, ~p)", [Info, State]),
    {noreply, State, hibernate}.

calculate_diff(K, V, Diff, State) ->
    {case maps:find(K, State) of
         {ok, Old} ->
             Diff(V, Old);
         error ->
             V
     end, maps:put(K, V, State)}.

log(K, VFun, R, State) ->
    {NewV, NewState} = prepare_value(K, VFun, State),
    VB = list_to_binary(io_lib:print(NewV, 0, 80, 100)),
    ?log_debug("update (key: ~p, rev: ~p)~n~s", [K, R, VB]),
    NewState.


sanitize(root_cert_and_pkey, V) ->
    {sanitized, base64:encode(crypto:hash(sha256, term_to_binary(V)))};
sanitize(_, V) ->
    V.

sanitize_snapshot(Mod, ModState) ->
    case Mod of
        chronicle_kv ->
            chronicle_kv:sanitize_state(fun sanitize/2, ModState);
        _ ->
            ModState
    end.

sanitize_log(Name, Command) ->
    case Name of
        kv ->
            chronicle_kv:sanitize_command(fun sanitize/2, Command);
        _ ->
            Command
    end.

prepare_value(K, VFun, State) ->
    V = VFun(),
    case ns_bucket:sub_key_match(K) of
        {true, _Bucket, props} ->
            calculate_diff(K, V, fun ns_config_log:compute_bucket_diff/2,
                           State);
        {true, _Bucket, collections} ->
            calculate_diff(K, V, fun collections:diff_manifests/2, State);
        _ ->
            {sanitize(K, V), State}
    end.
