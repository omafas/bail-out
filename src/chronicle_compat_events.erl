%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(chronicle_compat_events).

-behaviour(gen_server2).

-export([start_link/0,
         hush_chronicle/0,
         resume_chronicle/0,
         event_manager/0,
         kv_event_manager/0,
         subscribe/1,
         subscribe/2,
         notify_if_key_changes/2,
         start_refresh_worker/2,
         sync/0]).

-export([init/1, handle_call/3]).

-include("ns_common.hrl").
-include("cut.hrl").

-record(state, {chronicle_events_pid, event_manager, kv_event_manager}).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

resume_chronicle() ->
    gen_server2:call(?MODULE, resume_chronicle).

hush_chronicle() ->
    gen_server2:call(?MODULE, hush_chronicle).

sync() ->
    gen_server2:call(?MODULE, sync,
                     ?get_timeout(chronicle_compat_events_sync, 30000)).

init([]) ->
    ok = misc:wait_for_process(event_manager(), 10000),
    ok = misc:wait_for_process(kv_event_manager(), 10000),
    {ok, EventManager} = gen_event:start_link({local, event_manager()}),
    {ok, KVEventManager} = gen_event:start_link({local, kv_event_manager()}),
    subscribe_to_ns_config_events(EventManager),
    Pid = subscribe_to_chronicle_events(EventManager, KVEventManager),
    {ok, #state{chronicle_events_pid = Pid,
                event_manager = EventManager,
                kv_event_manager = KVEventManager}}.

event_manager() ->
    chronicle_compat_event_manager.

kv_event_manager() ->
    chronicle_compat_kv_event_manager.

subscribe_to_chronicle_events(EventManager, KVEventManager) ->
    ns_pubsub:subscribe_link(
      chronicle_kv:event_manager(kv),
      fun ({{key, Key}, _Rev, _} = Event) ->
              gen_event:notify(KVEventManager, Event),
              gen_event:notify(EventManager, Key);
          (_) ->
              ok
      end).

subscribe_to_ns_config_events(EventManager) ->
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({Key, _}) ->
              gen_event:notify(EventManager, Key);
          (_) ->
              ok
      end).

handle_call(hush_chronicle, _From,
            #state{chronicle_events_pid = undefined} = State) ->
    {reply, ok, State};
handle_call(hush_chronicle, _From,
            #state{chronicle_events_pid = Pid} = State) ->
    ns_pubsub:unsubscribe(Pid),
    {reply, ok, State#state{chronicle_events_pid = undefined}};
handle_call(resume_chronicle, _From,
            #state{chronicle_events_pid = undefined,
                   event_manager = EventManager,
                   kv_event_manager = KVEventManager} = State) ->
    Pid = subscribe_to_chronicle_events(EventManager, KVEventManager),
    {reply, ok, State#state{chronicle_events_pid = Pid}};
handle_call(sync, _From, #state{chronicle_events_pid = Pid} = State) ->
    ns_config:sync_announcements(),
    Pid == undefined orelse
        gen_event:which_handlers(chronicle_kv:event_manager(kv)),
    gen_event:which_handlers(event_manager()),
    {reply, ok, State}.

subscribe(Handler) ->
    ns_pubsub:subscribe_link(event_manager(), Handler).

subscribe(Keys, Worker) when is_list(Keys) ->
    subscribe(lists:member(_, Keys), Worker);
subscribe(Filter, Worker) ->
    subscribe(fun (Key) ->
                      case Filter(Key) of
                          false ->
                              ok;
                          true ->
                              Worker(Key)
                      end
              end).

filter_with_compat_ver(Keys) when is_list(Keys) ->
    [cluster_compat_version | Keys];
filter_with_compat_ver(Filter) ->
    fun (cluster_compat_version) ->
            true;
        (Key) ->
            Filter(Key)
    end.

notify_if_key_changes(Filter, Message) ->
    Self = self(),
    subscribe(filter_with_compat_ver(Filter), fun (_) -> Self ! Message end).

start_refresh_worker(Filter, Refresh) ->
    RV = {ok, Pid} =
        work_queue:start_link(
          fun () ->
                  Self = self(),
                  subscribe(
                    filter_with_compat_ver(Filter),
                    fun (_) ->
                            work_queue:submit_work(Self, Refresh)
                    end)
          end),
    work_queue:submit_sync_work(Pid, Refresh),
    RV.
