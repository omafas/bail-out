%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc server for maintaining audit configuration file
%%
-module(ns_audit_cfg).

-behaviour(gen_server).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, get_global/0, set_global/1, sync_set_global/1,
         default_audit_json_path/0, get_log_path/0, get_uid/0]).

-export([upgrade_descriptors/0, get_descriptors/1,
         jsonifier/1, get_non_filterable_descriptors/0, read_config/1]).

-record(state, {global,
                merged}).

jsonifier(log_path) ->
    fun list_to_binary/1;
jsonifier(descriptors_path) ->
    fun list_to_binary/1;
jsonifier(uuid) ->
    fun list_to_binary/1;
jsonifier(disabled_userids) ->
    fun (UList) ->
            [{[{user, list_to_binary(N)}, {source, D}]} || {N, D} <- UList]
    end;
jsonifier(_) ->
    fun functools:id/1.

version() ->
    2.

fields() ->
    [version,
     auditd_enabled,
     log_path,
     rotate_interval,
     rotate_size,
     descriptors_path,
     sync,
     disabled_userids,
     uuid,
     filtering_enabled,
     event_states].

is_notable_config_key(audit) ->
    true;
is_notable_config_key({node, N, audit}) ->
    N =:= node();
is_notable_config_key(_) ->
    false.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_global() ->
    gen_server:call(?MODULE, get_global).

get_uid() ->
    gen_server:call(?MODULE, get_uid).

set_global(KVList) ->
    ns_config:set_sub(audit, KVList).

sync_set_global(KVList) ->
    ns_config:set_sub(audit, KVList),
    ns_config:sync_announcements(),
    sync().

sync() ->
    gen_server:call(?MODULE, sync, infinity).

init([]) ->
    {Global, Local} = read_config(),
    Merged = prepare_params(Global, Local),

    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun ({Key, _}) ->
                                     case is_notable_config_key(Key) of
                                         true ->
                                             Self ! update_audit_json;
                                         _ ->
                                             []
                                     end;
                                 (_Other) ->
                                     []
                             end),

    write_audit_json(Merged),
    self() ! notify_memcached,
    {ok, #state{global = Global, merged = Merged}}.

handle_call(get_uid, _From, #state{merged = Merged} = State) ->
    {reply, proplists:get_value(uuid, Merged), State};
handle_call(get_global, _From, #state{global = Global,
                                      merged = Merged} = State) ->
    Return =
        case proplists:get_value(uuid, Merged) of
            undefined ->
                Global;
            UID ->
                [{uid, UID} | Global]
        end,
    {reply, Return, State};
handle_call(sync, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(notify_memcached, State) ->
    notify_memcached(State),
    {noreply, State};

handle_info(update_audit_json, #state{merged = OldMerged}) ->
    misc:flush(update_audit_json),
    {Global, Local} = read_config(),
    Merged = prepare_params(Global, Local),
    NewState = #state{global = Global, merged = Merged},
    case Merged of
        OldMerged ->
            ok;
        _ ->
            write_audit_json(Merged),
            notify_memcached(NewState)
    end,
    {noreply, NewState}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

notify_memcached(#state{merged = Merged}) ->
    ?log_debug("Instruct memcached to reload audit config"),
    ok = ns_memcached_sockets_pool:executing_on_socket(
           fun (Sock) ->
                   mc_client_binary:audit_config_reload(Sock)
           end),

    case proplists:get_value(uuid, Merged) of
        undefined ->
            ok;
        UID ->
            gen_event:notify(audit_events, {audit_uid_change, UID})
    end.

default_audit_json_path() ->
    filename:join(path_config:component_path(data, "config"), "audit.json").

audit_json_path() ->
    ns_config:search_node_prop(ns_config:latest(), memcached, audit_file).

is_enabled() ->
    ns_config:search_node_prop(
      ns_config:latest(), audit, auditd_enabled, false).

get_log_path() ->
    case is_enabled() of
        false ->
            undefined;
        true ->
            case ns_config:search_node_prop(
                   ns_config:latest(), audit, log_path) of
                undefined ->
                    undefined;
                Path ->
                    {ok, misc:absname(Path)}
            end
    end.

prepare_params(Global, Local) ->
    massage_params(lists:ukeymerge(1, Local, Global)).

calculate_event_states(Params) ->
    %% leave only those events that change the default
    Descriptors = orddict:from_list((get_descriptors(ns_config:latest()))),
    Enabled = proplists:get_value(enabled, Params, []),
    Disabled = proplists:get_value(disabled, Params, []),

    Filter =
        fun (Id, IsEnabled) ->
                case orddict:find(Id, Descriptors) of
                    {ok, Props} ->
                        proplists:get_value(enabled, Props)
                            =/= IsEnabled;
                    error ->
                        false
                end
        end,
    {[{integer_to_binary(Id), enabled} || Id <- Enabled, Filter(Id, true)] ++
         [{integer_to_binary(Id), disabled} ||
             Id <- Disabled, Filter(Id, false)]}.

massage_params(Params) ->
    EventStates = calculate_event_states(Params),
    DisabledUsers = proplists:get_value(disabled_users, Params, []),

    FilteringEnabled = EventStates =/= [] orelse DisabledUsers =/= [],

    NewParams =
        misc:update_proplist(Params, [{event_states, EventStates},
                                      {filtering_enabled, FilteringEnabled},
                                      {disabled_userids, DisabledUsers}]),

    UID = integer_to_list(erlang:phash2(NewParams)),

    [{uuid, UID} | NewParams].

write_audit_json(Params) ->
    Version = version(),
    CompleteParams = [{descriptors_path, path_config:component_path(sec)},
                      {version, Version}] ++ Params,

    Path = audit_json_path(),

    Fields = fields(),
    JsonParams = [{K, V} || {K, V} <- CompleteParams,
                            lists:member(K, Fields)],
    Json = [{K, (jsonifier(K))(V)} || {K, V} <- JsonParams],
    ?log_debug("Writing new content to ~p, Params ~p",
               [Path, ns_config_log:sanitize(JsonParams)]),

    Bytes = misc:ejson_encode_pretty({Json}),
    ok = misc:atomic_write_file(Path, Bytes).

read_config() ->
    read_config(ns_config:latest()).

read_config(Config) ->
    {case ns_config:search(Config, audit) of
         {value, V} ->
             lists:keysort(1, V);
         false ->
             []
     end,
     case ns_config:search(Config, {node, node(), audit}) of
         {value, V} ->
             lists:keysort(1, V);
         false ->
             []
     end}.

get_descriptors(Config) ->
    ns_config:search(Config, audit_decriptors, []).

get_audit_descs_from_file(EventPredicate) ->
    Path = filename:join(path_config:component_path(sec), "audit_events.json"),
    {ok, Bin} = file:read_file(Path),
    {Json} = ejson:decode(Bin),
    true = lists:member(proplists:get_value(<<"version">>, Json), [1, 2]),
    Modules = proplists:get_value(<<"modules">>, Json),
    lists:flatmap(
      fun ({Module}) ->
              ModuleIdBin = proplists:get_value(<<"module">>, Module),
              ModuleId = list_to_atom(binary_to_list(ModuleIdBin)),
              Events = proplists:get_value(<<"events">>, Module),
              lists:filtermap(
                fun ({Event}) ->
                        case EventPredicate(Event) of
                            false ->
                                false;
                            true ->
                                {true,
                                 {proplists:get_value(<<"id">>, Event),
                                  [{name,
                                    proplists:get_value(<<"name">>, Event)},
                                   {description,
                                    proplists:get_value(<<"description">>,
                                                        Event)},
                                   {enabled,
                                    proplists:get_value(<<"enabled">>, Event)},
                                   {module, ModuleId}]}}
                        end
                end, Events)
      end, Modules).

get_non_filterable_descriptors() ->
    get_audit_descs_from_file(
      fun(E) ->
              proplists:get_value(<<"filtering_permitted">>, E) =:= false
      end).

read_descriptors() ->
    get_audit_descs_from_file(
      fun(E) ->
              proplists:get_value(<<"filtering_permitted">>, E) =:= true
      end).

upgrade_descriptors() ->
    [{set, audit_decriptors, lists:ukeysort(1, read_descriptors())}].
