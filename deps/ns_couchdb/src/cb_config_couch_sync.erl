%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc This server syncs part of (replicated) ns_config to local
%% couch config.

-module(cb_config_couch_sync).

-behaviour(gen_server).

-export([start_link/0, set_db_and_ix_paths/2, get_db_and_ix_paths/0]).

%% gen_event callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/1),
    announce_notable_keys(),
    {ok, #state{}}.

-spec get_db_and_ix_paths() -> [{db_path | index_path, string()}].
get_db_and_ix_paths() ->
    DbPath = couch_config:get("couchdb", "database_dir"),
    IxPath = couch_config:get("couchdb", "view_index_dir", DbPath),
    [{db_path, filename:join([DbPath])},
     {index_path, filename:join([IxPath])}].

-spec set_db_and_ix_paths(DbPath :: string(), IxPath :: string()) -> ok.
set_db_and_ix_paths(DbPath0, IxPath0) ->
    DbPath = filename:join([DbPath0]),
    IxPath = filename:join([IxPath0]),

    couch_config:set("couchdb", "database_dir", DbPath),
    couch_config:set("couchdb", "view_index_dir", IxPath).

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _) ->
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({notable_change, cluster_encryption_level = Key}, State) ->
    flush_for_key(Key),
    apply_to_couch_config(httpd, ip4_bind_address,
                          menelaus_web:get_addr(inet, false), true),
    apply_to_couch_config(httpd, ip6_bind_address,
                          menelaus_web:get_addr(inet6, false), true),
    {noreply, State};
handle_info({notable_change, secure_headers = Key}, State) ->
    flush_for_key(Key),

    ValueRaw = menelaus_util:compute_sec_headers(),
    Value = lists:flatten(io_lib:format("~p", [ValueRaw])),

    apply_to_couch_config(httpd, extra_headers, Value, false),
    {noreply, State};
handle_info({notable_change, audit = Key}, State) ->
    flush_for_key(Key),

    {value, ValueRaw} = ns_config:search(Key),
    Value1 = [{audit_enabled, couch_util:get_value(auditd_enabled, ValueRaw, false)},
              {disabled_users, couch_util:get_value(disabled_users, ValueRaw, [])},
              {enabled_events, couch_util:get_value(enabled, ValueRaw, [])}
             ],

    apply_to_couch_config(security, audit, lists:flatten(io_lib:format("~p", [Value1])), false),
    {noreply, State};
handle_info({notable_change, Key}, State) ->
    flush_for_key(Key),

    {couchdb, SK} = ActualKey = to_global_key(Key),

    {value, ValueRaw} = ns_config:search_node(ActualKey),
    Value = couch_util:to_list(ValueRaw),
    {CouchSectionAtom, CouchKeyAtom} =
        if
            is_tuple(SK) ->
                SK;
            is_atom(SK) ->
                %% if section is not specified, default to "couchdb"
                {couchdb, SK}
        end,

    apply_to_couch_config(CouchSectionAtom, CouchKeyAtom, Value, true),
    {noreply, State};
handle_info(_Event, State) ->
    {noreply, State}.

%% Auxiliary functions.

apply_to_couch_config(CouchSectionAtom, CouchKeyAtom, Value, Persist) ->
    CouchSection = atom_to_list(CouchSectionAtom),
    CouchKey     = atom_to_list(CouchKeyAtom),

    Current = couch_config:get(CouchSection, CouchKey),
    case Current =:= Value of
        true ->
            ok;
        false ->
            ok = couch_config:set(CouchSection, CouchKey, Value, Persist)
    end.

is_notable_key({couchdb, _}) -> true;
is_notable_key({{node, Node, {couchdb, _}}}) when Node =:= node() -> true;
is_notable_key(secure_headers) -> true;
is_notable_key(audit) -> true;
is_notable_key(cluster_encryption_level) -> true;
is_notable_key(_) -> false.

handle_config_event({Key, _Value}) ->
    case is_notable_key(Key) of
        true ->
            ?MODULE ! {notable_change, Key};
        _ ->
            ok
    end;
handle_config_event(_) ->
    ok.

to_global_key({couchdb, _} = Key) ->
    Key;
to_global_key(cluster_encryption_level = Key) ->
    Key;
to_global_key(secure_headers = Key) ->
    Key;
to_global_key(audit = Key) ->
    Key;
to_global_key({node, Node, {couchdb, _} = Key}) when Node =:= node() ->
    Key.

flush_for_key(Key) ->
    GlobalKey = to_global_key(Key),
    LocalKey  = {node, node(), GlobalKey},

    do_flush_for_key(GlobalKey),
    do_flush_for_key(LocalKey).

do_flush_for_key(Key) ->
    receive
        {notable_change, Key, _} ->
            do_flush_for_key(Key)
    after
        0 ->
            ok
    end.

announce_notable_keys() ->
    KVList = ns_config:get_kv_list(),

    lists:foreach(
      fun ({Key, _Value}) ->
              case is_notable_key(Key) of
                  true ->
                      ?MODULE ! {notable_change, Key};
                  false ->
                      ok
              end
      end, KVList).
