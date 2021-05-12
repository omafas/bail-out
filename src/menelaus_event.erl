%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
                module,
                disable_non_ssl_ports,
                afamily_requirement,
                watchers = []}).

-include("ns_common.hrl").

% Noop process to get initialized in the supervision tree.

modules() ->
    [chronicle_compat_events:event_manager(),
     ns_node_disco_events,
     buckets_events,
     index_events,
     audit_events,
     bucket_info_cache_invalidations,
     user_storage_events].

start_link() ->
    misc:start_event_link(
      fun () ->
              [ok = gen_event:add_sup_handler(M, {?MODULE, M}, M) ||
                  M <- modules()]
      end).

register_watcher(Pid) ->
    [ok = gen_event:call(M, {?MODULE, M}, {register_watcher, Pid}) ||
        M <- modules()].

unregister_watcher(Pid) ->
    [ok = gen_event:call(M, {?MODULE, M}, {unregister_watcher, Pid}) ||
        M <- modules()].

sync(Module) ->
    gen_event:call(Module, {?MODULE, Module}, sync).

%% Implementation

init(Module) ->
    {ok,
     case chronicle_compat_events:event_manager() of
         Module ->
             #state{webconfig = menelaus_web:webconfig(),
                    disable_non_ssl_ports = misc:disable_non_ssl_ports(),
                    afamily_requirement = misc:address_family_requirement(),
                    module = Module};
         _ ->
             #state{module = Module}
     end}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_event(Event, State) ->
    NewState = maybe_restart(Event, State),
    maybe_notify_watchers(convert_event(Event, NewState), NewState),
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

convert_event(_, #state{module = bucket_info_cache_invalidations}) ->
    bucket_info_cache_invalidation;
convert_event(Key, #state{module = Module}) ->
    case chronicle_compat_events:event_manager() of
        Module ->
            {Key, something};
        _ ->
            Key
    end.

is_interesting_to_watchers({significant_buckets_change, _}) -> true;
is_interesting_to_watchers({memcached, _}) -> true;
is_interesting_to_watchers({{node, _, memcached}, _}) -> true;
is_interesting_to_watchers({{node, _, membership}, _}) -> true;
is_interesting_to_watchers({rebalance_status, _}) -> true;
is_interesting_to_watchers({recovery_status, _}) -> true;
is_interesting_to_watchers({nodes_wanted, _}) -> true;
is_interesting_to_watchers({server_groups, _}) -> true;
is_interesting_to_watchers({ns_node_disco_events, _, _}) -> true;
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
is_interesting_to_watchers({client_cert_auth, _}) -> true;
is_interesting_to_watchers({audit_uid_change, _}) -> true;
is_interesting_to_watchers({user_version, _}) -> true;
is_interesting_to_watchers({group_version, _}) -> true;
is_interesting_to_watchers(bucket_info_cache_invalidation) -> true;
is_interesting_to_watchers({Key, _}) ->
    collections:key_match(Key) =/= false orelse ns_bucket:buckets_change(Key);
is_interesting_to_watchers(_) -> false.

maybe_notify_watchers(Event, State) ->
    case is_interesting_to_watchers(Event) of
        true -> notify_watchers(State);
        false -> ok
    end.

notify_watchers(#state{watchers = Watchers}) ->
    UpdateID = erlang:unique_integer([monotonic]),
    lists:foreach(fun({Pid, _}) ->
                          Pid ! {notify_watcher, UpdateID}
                  end, Watchers).

restart_event({node, N, rest}) when N =:= node() -> true;
restart_event({node, N, address_family_only}) when N =:= node() -> true;
restart_event({node, N, address_family}) when N =:= node() -> true;
restart_event(rest) -> true;
restart_event(cluster_encryption_level) -> true;
restart_event(_) -> false.

maybe_restart(Event, State = #state{module = Module}) ->
    case Module =:= chronicle_compat_events:event_manager()
        andalso restart_event(Event) of
        true -> maybe_restart(State);
        false -> State
    end.

maybe_restart(#state{webconfig = WebConfigOld,
                     disable_non_ssl_ports = DisableOld,
                     afamily_requirement = AFROld} = State) ->
    WebConfigNew = menelaus_web:webconfig(),
    DisableNew = misc:disable_non_ssl_ports(),
    AFRNew = misc:address_family_requirement(),
    case WebConfigNew =:= WebConfigOld andalso
         DisableOld =:= DisableNew andalso
         AFROld =:= AFRNew of
        true -> State;
        false -> {ok, _} = menelaus_web_sup:restart_web_servers(),
                 State#state{webconfig = WebConfigNew,
                             disable_non_ssl_ports = DisableNew,
                             afamily_requirement = AFRNew}
    end.

flush_watcher_notifications(PrevID) ->
    receive
        {notify_watcher, ID} -> flush_watcher_notifications(ID)
    after 0 ->
        PrevID
    end.
