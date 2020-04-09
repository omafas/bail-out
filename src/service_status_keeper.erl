%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-2018 Couchbase, Inc.
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
-module(service_status_keeper).

-include("ns_common.hrl").

-behavior(gen_server).

%% API
-export([start_link/1, update/2, get_status/2,
         get_items/1, get_version/1, process_indexer_status/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(WORKER, service_status_keeper_worker).

get_refresh_interval(Service) ->
    ?get_timeout({Service:get_type(), service_status_keeper_refresh}, 5000).

get_stale_threshold(Service) ->
    ?get_param({Service:get_type(), service_status_keeper_stale_threshold}, 2).

dont_restart_service(Service) ->
    ?get_param({Service:get_type(), dont_restart_service}, false).

server_name(Service) ->
    list_to_atom(?MODULE_STRING "-" ++ atom_to_list(Service:get_type())).

start_link(Service) ->
    gen_server:start_link({local, server_name(Service)}, ?MODULE, Service, []).

update(Service, Status) ->
    gen_server:cast(server_name(Service), {update, Status}).

get_status(Service, Timeout) ->
    gen_server:call(server_name(Service), get_status, Timeout).

get_items(Service) ->
    gen_server:call(server_name(Service), get_items).

get_version(Service) ->
    gen_server:call(server_name(Service), get_version).

-record(state, {service :: atom(),
                num_connections,

                items,
                stale :: undefined | true | {false, non_neg_integer()},
                version,

                restart_pending,
                source :: local | {remote, [node()], non_neg_integer()}}).

init(Service) ->
    Self = self(),

    Self ! refresh,

    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/2, Self),
    ns_pubsub:subscribe_link(ns_node_disco_events, fun handle_node_disco_event/2, Self),

    State = #state{service = Service,
                   num_connections = 0,
                   restart_pending = false,
                   source = get_source(Service)},

    {ok, set_items([], State)}.

handle_call(get_items, _From, #state{items = Items,
                                     stale = StaleInfo,
                                     version = Version} = State) ->
    {reply, {ok, Items, is_stale(StaleInfo), Version}, State};
handle_call(get_version, _From, #state{version = Version} = State) ->
    {reply, {ok, Version}, State};
handle_call(get_status, _From,
            #state{num_connections = NumConnections} = State) ->
    Status = [{num_connections, NumConnections}],
    {reply, {ok, Status}, State}.

handle_cast({update, Status}, #state{source = local} = State) ->
    NumConnections = proplists:get_value(index_num_connections, Status, 0),
    NeedsRestart = proplists:get_value(index_needs_restart, Status, false),

    NewState0 = State#state{num_connections = NumConnections},
    NewState = case NeedsRestart of
                   true ->
                       maybe_restart_service(NewState0);
                   false ->
                       NewState0
               end,

    {noreply, NewState};
handle_cast({update, _}, State) ->
    ?log_warning("Got unexpected status update when source is not local. Ignoring."),
    {noreply, State};
handle_cast({refresh_done, Result}, #state{service = Service} = State) ->
    NewState =
        case Result of
            {ok, Items} ->
                set_items(Items, State);
            {stale, Items} ->
                set_stale(Items, State);
            {error, _} ->
                ?log_error("Service ~p returned incorrect status", [Service]),
                increment_stale(State)
        end,

    erlang:send_after(get_refresh_interval(Service), self(), refresh),
    {noreply, NewState};
handle_cast(restart_done, #state{restart_pending = true} = State) ->
    {noreply, State#state{restart_pending = false}}.

handle_info(notable_event, #state{service = Service} = State) ->
    misc:flush(notable_event),
    {noreply, State#state{source = get_source(Service)}};
handle_info(refresh, State) ->
    refresh_status(State),
    {noreply, State};
handle_info(Msg, State) ->
    ?log_debug("Ignoring unknown msg: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
maybe_restart_service(#state{restart_pending = false,
                             service = Service} = State) ->
    case dont_restart_service(Service) of
        true ->
            State;
        false ->
            restart_service(State)
    end;
maybe_restart_service(State) ->
    State.

restart_service(#state{service = Service,
                       restart_pending = false,
                       source = local} = State) ->
    Self = self(),
    work_queue:submit_work(
      ?WORKER,
      fun () ->
              ?log_info("Restarting the ~p", [Service]),

              case Service:restart() of
                  {ok, _} ->
                      ?log_info("Restarted the ~p successfully", [Service]);
                  Error ->
                      ?log_error("Failed to restart the ~p: ~p",
                                 [Service, Error])
              end,

              gen_server:cast(Self, restart_done)
      end),

    State#state{restart_pending = true}.

refresh_status(State) ->
    Self = self(),
    work_queue:submit_work(
      ?WORKER,
      fun () ->
              gen_server:cast(Self, {refresh_done, grab_status(State)})
      end).

grab_status(#state{service = Service,
                   source = local}) ->
    case Service:get_local_status() of
        {ok, Status} ->
            Service:process_status(Status);
        Error ->
            Error
    end;
grab_status(#state{service = Service,
                   source = {remote, Nodes, NodesCount}}) ->
    case Nodes of
        [] ->
            {ok, []};
        _ ->
            Node = lists:nth(rand:uniform(NodesCount), Nodes),

            try Service:get_remote_items(Node) of
                {ok, Items, Stale, _Version} ->
                    %% note that we're going to recompute the version instead
                    %% of using the one from the remote node; that's because
                    %% the version should be completely opaque; if we were to
                    %% use it, that would imply that we assume the same
                    %% algorithm for generation of the versions on all the
                    %% nodes
                    case Stale of
                        true ->
                            {stale, Items};
                        false ->
                            {ok, Items}
                    end;
                Error ->
                    ?log_error("Couldn't get items from node ~p: ~p",
                               [Node, Error]),
                    {error, failed}
            catch
                T:E ->
                    ?log_error(
                       "Got exception while getting items from node ~p: ~p",
                       [Node, {T, E}]),
                    {error, failed}
            end
    end.

process_indexer_status(Mod, {[_|_] = Status}, Mapping) ->
    case lists:keyfind(<<"code">>, 1, Status) of
        {_, <<"success">>} ->
            RawIndexes =
                case lists:keyfind(<<"status">>, 1, Status) of
                    false ->
                        [];
                    {_, V} ->
                        V
                end,

            {ok, process_indexes(RawIndexes, Mapping)};
        _ ->
            ?log_error("Indexer ~p returned unsuccessful status:~n~p",
                       [Mod, Status]),
            {error, bad_status}
    end;
process_indexer_status(Mod, Other, _Mapping) ->
    ?log_error("~p got invalid status: ~p", [Mod, Other]),
    {error, bad_status}.


process_indexes(Indexes, Mapping) ->
    lists:map(
      fun ({Index}) ->
              lists:foldl(
                fun ({Key, BinKey}, Acc) when is_atom(Key) ->
                        {_, Val} = lists:keyfind(BinKey, 1, Index),
                        [{Key, Val} | Acc];
                    ({ListOfKeys, BinKey}, Acc) when is_list(ListOfKeys) ->
                        {_, Val} = lists:keyfind(BinKey, 1, Index),
                        lists:foldl(fun (Key, Acc1) ->
                                            [{Key, Val} | Acc1]
                                    end, Acc, ListOfKeys)
                end, [], Mapping)
      end, Indexes).

get_source(Service) ->
    Config = ns_config:get(),
    case ns_cluster_membership:should_run_service(Config, Service:get_type(),
                                                  ns_node_disco:ns_server_node()) of
        true ->
            local;
        false ->
            ServiceNodes =
                ns_cluster_membership:service_actual_nodes(Config,
                                                           Service:get_type()),
            {remote, ServiceNodes, length(ServiceNodes)}
    end.

is_notable({{node, _, membership}, _}) ->
    true;
is_notable({nodes_wanted, _}) ->
    true;
is_notable({rest_creds, _}) ->
    true;
is_notable({{service_map, _}, _}) ->
    true;
is_notable(_) ->
    false.

notify_notable(Pid) ->
    Pid ! notable_event.

handle_config_event(Event, Pid) ->
    case is_notable(Event) of
        true ->
            notify_notable(Pid);
        false ->
            ok
    end,
    Pid.

handle_node_disco_event(Event, Pid) ->
    case Event of
        {ns_node_disco_events, _NodesOld, _NodesNew} ->
            notify_notable(Pid);
        false ->
            ok
    end,
    Pid.

set_items(Items, #state{version = OldVersion,
                        items = OldItems,
                        service = Service} = State) ->
    Version = Service:compute_version(Items, false),

    case Version =:= OldVersion of
        true ->
            case Items =:= OldItems of
                true ->
                    State;
                false ->
                    %% Some items are not used in the computation of the
                    %% version.  Update the state so getters get the most
                    %% recent info.
                    State#state{items = Items, stale = {false, 0}}
            end;
        false ->
            notify_change(State#state{items = Items,
                                      stale = {false, 0},
                                      version = Version})
    end.

set_stale(#state{items = Items, service = Service} = State) ->
    Version = Service:compute_version(Items, true),
    notify_change(State#state{stale = true,
                              version = Version}).

set_stale(Items, State) ->
    set_stale(State#state{items = Items}).

increment_stale(#state{stale = StaleInfo,
                       service = Service} = State) ->
    case StaleInfo of
        true ->
            %% we're already stale; no need to do anything
            State;
        {false, StaleCount} ->
            NewStaleCount = StaleCount + 1,

            case NewStaleCount >= get_stale_threshold(Service) of
                true ->
                    set_stale(State);
                false ->
                    State#state{stale = {false, NewStaleCount}}
            end
    end.

notify_change(#state{service = Service,
                     version = Version} = State) ->
    gen_event:notify(index_events, {indexes_change, Service:get_type(),
                                    Version}),
    State.

is_stale({false, _}) ->
    false;
is_stale(true) ->
    true.
