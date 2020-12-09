%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
-module(menelaus_event).

-behaviour(gen_event).

% Allows menelaus erlang processes (especially long-running HTTP /
% REST streaming processes) to register for messages when there
% are configuration changes.

-export([start_link/0]).

-export([register_watcher/1,
         unregister_watcher/1,
         flush_watcher_notifications/1,
         sync/1]).

%% gen_event callbacks

-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {webconfig,
                disable_non_ssl_ports,
                watchers = []}).

-include("ns_common.hrl").

% Noop process to get initialized in the supervision tree.

start_link() ->
    misc:start_event_link(fun () ->
                                  ok = gen_event:add_sup_handler(ns_config_events,
                                                                 {?MODULE, ns_config_events},
                                                                 ns_config_events),
                                  ok = gen_event:add_sup_handler(ns_node_disco_events,
                                                                 {?MODULE, ns_node_disco_events},
                                                                 simple_events_handler),
                                  ok = gen_event:add_sup_handler(buckets_events,
                                                                 {?MODULE, buckets_events},
                                                                 simple_events_handler),
                                  ok = gen_event:add_sup_handler(index_events,
                                                                 {?MODULE, index_events},
                                                                 simple_events_handler),
                                  ok = gen_event:add_sup_handler(audit_events,
                                                                 {?MODULE, audit_events},
                                                                 simple_events_handler)
                          end).

register_watcher(Pid) ->
    ok = gen_event:call(ns_config_events,
                        {?MODULE, ns_config_events},
                        {register_watcher, Pid}),
    ok = gen_event:call(ns_node_disco_events,
                        {?MODULE, ns_node_disco_events},
                        {register_watcher, Pid}),
    ok = gen_event:call(buckets_events,
                        {?MODULE, buckets_events},
                        {register_watcher, Pid}),
    ok = gen_event:call(index_events,
                        {?MODULE, index_events},
                        {register_watcher, Pid}),
    ok = gen_event:call(audit_events,
                        {?MODULE, audit_events},
                        {register_watcher, Pid}).

unregister_watcher(Pid) ->
    ok = gen_event:call(ns_config_events,
                        {?MODULE, ns_config_events},
                        {unregister_watcher, Pid}),
    ok = gen_event:call(ns_node_disco_events,
                        {?MODULE, ns_node_disco_events},
                        {unregister_watcher, Pid}),
    ok = gen_event:call(buckets_events,
                        {?MODULE, buckets_events},
                        {unregister_watcher, Pid}),
    ok = gen_event:call(index_events,
                        {?MODULE, index_events},
                        {unregister_watcher, Pid}),
    ok = gen_event:call(audit_events,
                        {?MODULE, audit_events},
                        {unregister_watcher, Pid}).

sync(Module) ->
    gen_event:call(Module, {?MODULE, Module}, sync).

%% Implementation

init(ns_config_events) ->
    {ok, #state{watchers = [],
                disable_non_ssl_ports = misc:disable_non_ssl_ports(),
                webconfig = menelaus_web:webconfig()}};

init(_) ->
    {ok, #state{watchers = []}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_event(Event, State) ->
    NewState = maybe_restart(Event, State),
    maybe_notify_watchers(Event, State),
    {ok, NewState}.

handle_call({register_watcher, Pid},
            #state{watchers = Watchers} = State) ->
    Watchers2 = case lists:keysearch(Pid, 1, Watchers) of
                    false -> MonitorRef = erlang:monitor(process, Pid),
                             [{Pid, MonitorRef} | Watchers];
                    _     -> Watchers
                end,
    {ok, ok, State#state{watchers = Watchers2}};

handle_call({unregister_watcher, Pid},
            #state{watchers = Watchers} = State) ->
    Watchers2 = case lists:keytake(Pid, 1, Watchers) of
                    false -> Watchers;
                    {value, {Pid, MonitorRef}, WatchersRest} ->
                        erlang:demonitor(MonitorRef, [flush]),
                        WatchersRest
                end,
    {ok, ok, State#state{watchers = Watchers2}};

handle_call(sync, State) ->
    {ok, ok, State};

handle_call(Request, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p)", [Request, State]),
    {ok, ok, State}.

handle_info({'DOWN', MonitorRef, _, _, _},
            #state{watchers = Watchers} = State) ->
    Watchers2 = case lists:keytake(MonitorRef, 2, Watchers) of
                    false -> Watchers;
                    {value, {_Pid, MonitorRef}, WatchersRest} ->
                        erlang:demonitor(MonitorRef, [flush]),
                        WatchersRest
                end,
    {ok, State#state{watchers = Watchers2}};

handle_info(_Info, State) ->
    {ok, State}.

% ------------------------------------------------------------
is_interesting_to_watchers({significant_buckets_change, _}) -> true;
is_interesting_to_watchers({memcached, _}) -> true;
is_interesting_to_watchers({{node, _, memcached}, _}) -> true;
is_interesting_to_watchers({{node, _, membership}, _}) -> true;
is_interesting_to_watchers({rebalance_status, _}) -> true;
is_interesting_to_watchers({recovery_status, _}) -> true;
is_interesting_to_watchers({buckets, _}) -> true;
is_interesting_to_watchers({nodes_wanted, _}) -> true;
is_interesting_to_watchers({server_groups, _}) -> true;
is_interesting_to_watchers({ns_node_disco_events, _NodesBefore, _NodesAfter}) -> true;
is_interesting_to_watchers({autocompaction, _}) -> true;
is_interesting_to_watchers({cluster_compat_version, _}) -> true;
is_interesting_to_watchers({developer_preview_enabled, _}) -> true;
is_interesting_to_watchers({cluster_name, _}) -> true;
is_interesting_to_watchers({memory_quota, _}) -> true;
is_interesting_to_watchers({index_settings_change, memoryQuota, _}) -> true;
is_interesting_to_watchers({indexes_change, index, _}) -> true;
is_interesting_to_watchers({goxdcr_enabled, _}) -> true;
is_interesting_to_watchers({{node, _, stop_xdcr}, _}) -> true;
is_interesting_to_watchers({{node, _, services}, _}) -> true;
is_interesting_to_watchers({{service_map, _}, _}) -> true;
is_interesting_to_watchers({user_roles, _}) -> true;
is_interesting_to_watchers({client_cert_auth, _}) -> true;
is_interesting_to_watchers({audit_uid_change, _}) -> true;
is_interesting_to_watchers(_) -> false.

maybe_notify_watchers(Event, State) ->
    case is_interesting_to_watchers(Event) of
        true -> notify_watchers(State);
        false -> ok
    end.

notify_watchers(#state{watchers = Watchers}) ->
    UpdateID = erlang:unique_integer(),
    lists:foreach(fun({Pid, _}) ->
                          Pid ! {notify_watcher, UpdateID}
                  end, Watchers).

restart_event({{node, N, rest}, _}) when N =:= node() -> true;
restart_event({rest, _}) -> true;
restart_event({cluster_encryption_level, _}) -> true;
restart_event(_) -> false.

maybe_restart(Event, State) ->
    case restart_event(Event) of
        true -> maybe_restart(State);
        false -> State
    end.

maybe_restart(#state{webconfig = WebConfigOld,
                     disable_non_ssl_ports = DisableOld} = State) ->
    WebConfigNew = menelaus_web:webconfig(),
    DisableNew = misc:disable_non_ssl_ports(),
    case WebConfigNew =:= WebConfigOld andalso DisableOld =:= DisableNew of
        true -> State;
        false -> {ok, _} = menelaus_web_sup:restart_web_servers(),
                 State#state{webconfig = WebConfigNew,
                             disable_non_ssl_ports = DisableNew}
    end.

flush_watcher_notifications(PrevID) ->
    receive
        {notify_watcher, ID} -> flush_watcher_notifications(ID)
    after 0 ->
        PrevID
    end.
