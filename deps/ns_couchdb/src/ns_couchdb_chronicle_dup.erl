%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc process responsible for maintaining the copy of chronicle_kv
%% on ns_couchdb node
%%

-module(ns_couchdb_chronicle_dup).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include("ns_config.hrl").

-export([start_link/0,
         init/1,
         handle_info/2,
         handle_call/3,
         lookup/1,
         ro_txn/1]).

-define(RO_TXN_TIMEOUT, 10000).

-record(state, {child, ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

lookup(Key) ->
    ets:lookup(?MODULE, Key).

ro_txn(Body) ->
    gen_server:call(?MODULE, {ro_txn, Body}, ?RO_TXN_TIMEOUT).

init([]) ->
    ets:new(?MODULE, [public, set, named_table]),
    process_flag(trap_exit, true),
    State = subscribe_to_events(),
    pull(),
    {ok, State}.

subscribe_to_events() ->
    Self = self(),
    Ref = make_ref(),
    NsServer = ns_node_disco:ns_server_node(),
    ?log_debug("Subscribing to events from ~p with ref = ~p", [NsServer, Ref]),
    Child = ns_pubsub:subscribe_link(
              {chronicle_kv:event_manager(kv), NsServer},
              fun ({{key, Key}, Rev, {updated, Value}}) ->
                      Self ! {insert, Ref, Key, fun () -> Value end, Rev};
                  ({{key, Key}, Rev, deleted}) ->
                      Self ! {delete, Ref, Key, Rev};
                  (_) ->
                      ok
              end),
    #state{child = Child, ref = Ref}.

handle_call({ro_txn, Body}, _From, State) ->
    TxnGet = fun (K) ->
                     case ets:lookup(?MODULE, K) of
                         [{K, VR}] ->
                             {ok, VR};
                         [] ->
                             {error, not_found}
                     end
             end,
    {reply, Body(TxnGet), State}.

handle_info({'EXIT', Child, Reason}, #state{child = Child}) ->
    ?log_debug("Received exit ~p from event subscriber", [Reason]),
    resubscribe(),
    {noreply, #state{child = undefined, ref = undefined}};
handle_info({'EXIT', From, Reason}, State) ->
    ?log_debug("Received exit ~p from ~p", [Reason, From]),
    {stop, Reason, State};
handle_info({insert, Ref, K, ValFun, Rev}, State = #state{ref = Ref}) ->
    insert(K, ValFun(), Rev),
    {noreply, State};
handle_info({delete, Ref, K, Rev}, State = #state{ref = Ref}) ->
    delete(K, Rev),
    {noreply, State};
handle_info(resubscribe, #state{child = undefined} = State) ->
    {noreply, try
                  NewState = subscribe_to_events(),
                  self() ! pull,
                  NewState
              catch error:Error ->
                      ?log_debug("Subscription failed with ~p", [Error]),
                      resubscribe(),
                      State
              end};
handle_info(pull, State) ->
    misc:flush(pull),
    pull(),
    {noreply, State};
handle_info(Message, State) ->
    ?log_debug("Unexpected message ~p at state ~p", [Message, State]),
    {noreply, State}.

resubscribe() ->
    erlang:send_after(200, self(), resubscribe).

notify(Evt) ->
    gen_event:notify(chronicle_kv:event_manager(kv), Evt).

insert(K, V, R) ->
    ?log_debug("Set ~p, rev = ~p", [K, R]),
    ets:insert(?MODULE, {K, {V, R}}),
    notify({{key, K}, R, {updated, V}}).

delete(K, R) ->
    ?log_debug("Delete ~p, rev = ~p", [K, R]),
    ets:delete(?MODULE, K),
    notify({{key, K}, R, deleted}).

pull() ->
    NsServer = ns_node_disco:ns_server_node(),
    ?log_debug("Pulling everything from ~p", [NsServer]),
    Snapshot =
        try
            chronicle_local:get_snapshot(NsServer)
        catch
            Type:What ->
                ?log_debug("Config pull from ~p:~p failed due to ~p",
                           [NsServer, Type, What]),
                erlang:send_after(200, self(), pull),
                undefined
        end,
    apply_snapshot(Snapshot).

apply_snapshot(undefined) ->
    ok;
apply_snapshot(Snapshot) ->
    lists:foreach(
      fun ({K, {_V, R}}) ->
              case maps:is_key(K, Snapshot) of
                  true ->
                      ok;
                  false ->
                      delete(K, R)
              end
      end, ets:tab2list(?MODULE)),
    lists:foreach(
      fun ({K, {V, R}}) ->
              case ets:lookup(?MODULE, K) of
                  [{K, {V, R}}] ->
                      ok;
                  _ ->
                      insert(K, V, R)
              end
      end, maps:to_list(Snapshot)).
