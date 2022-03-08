%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(service_status_keeper).

-include("ns_common.hrl").

-behavior(gen_server).

%% API
-export([start_link/1, get_items/1, get_version/1, process_indexer_status/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(WORKER, service_status_keeper_worker).

get_refresh_interval(Service) ->
    ?get_timeout({Service:get_type(), service_status_keeper_refresh}, 5000).

get_stale_threshold(Service) ->
    ?get_param({Service:get_type(), service_status_keeper_stale_threshold}, 2).

server_name(Service) ->
    list_to_atom(?MODULE_STRING "-" ++ atom_to_list(Service:get_type())).

start_link(Service) ->
    gen_server:start_link({local, server_name(Service)}, ?MODULE, Service, []).

get_items(Service) ->
    gen_server:call(server_name(Service), get_items).

get_version(Service) ->
    gen_server:call(server_name(Service), get_version).

-record(state, {service :: atom(),

                etag = undefined :: undefined | string(),
                items,
                stale :: undefined | true | {false, non_neg_integer()},
                version,

                restart_pending,
                source :: local | {remote, [node()], non_neg_integer()}}).

init(Service) ->
    Self = self(),

    Self ! refresh,

    chronicle_compat_events:notify_if_key_changes(
      fun ({node, _, membership}) ->
              true;
          (nodes_wanted) ->
              true;
          (rest_creds) ->
              true;
          ({service_map, _}) ->
              true;
          (_) ->
              false
      end, notable_event),

    ns_pubsub:subscribe_link(ns_node_disco_events,
                             fun handle_node_disco_event/2, Self),

    State = #state{service = Service,
                   restart_pending = false,
                   source = get_source(Service)},

    {ok, set_items([], State)}.

handle_call(get_items, _From, #state{items = Items,
                                     stale = StaleInfo,
                                     version = Version} = State) ->
    {reply, {ok, Items, is_stale(StaleInfo), Version}, State};
handle_call(get_version, _From, #state{version = Version} = State) ->
    {reply, {ok, Version}, State}.

%% Backward compat:
handle_cast({update, _}, State) ->
    ?log_warning("Ignoring update request."),
    {noreply, State};
handle_cast({refresh_done, Result}, #state{service = Service,
                                           etag = Etag} = State) ->
    NewState =
        case Result of
            {ok, Etag, _} when Etag =/= undefined ->
                %% Same Etag returned as previous. No change in State.
                State;
            {ok, NewEtag, Items} ->
                set_items(Items, State#state{etag = NewEtag});
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

%%% internal
refresh_status(State) ->
    Self = self(),
    work_queue:submit_work(
      ?WORKER,
      fun () ->
              gen_server:cast(Self, {refresh_done, grab_status(State)})
      end).

-spec grab_status(#state{}) -> {ok, undefined | string(), any()} |
                               {error, any()} |
                               {stale, any()}.
%% Grab status from local service.
grab_status(#state{service = Service,
                   source = local,
                   etag = Etag,
                   items = Items}) ->
    %% - Request for service status with previous known Etag in the
    %%   if-none-match header field
    %% - Service is expected to return status with
    %%   200 -> if Etag has changed and returns new status.
    %%   304 -> Etag is the same, and status is empty payload.
    Headers = case Etag of
                  undefined ->
                      [];
                  _ ->
                      [menelaus_rest:if_none_match_header(Etag)]
              end,
    case Service:get_local_status(Headers) of
        {ok, ResHeaders, Status} ->
            case proplists:get_value("Etag", ResHeaders) of
                Etag when Etag =/= undefined ->
                    %% Same Etag returned as previous. No change in Items.
                    {ok, Etag, Items};
                NewEtag ->
                    case Service:process_status(Status) of
                        {ok, RV} -> {ok, NewEtag, RV};
                        Err -> Err
                    end
            end;
        Error ->
            Error
    end;
%% Grab status from remote node.
grab_status(#state{service = Service,
                   source = {remote, Nodes, NodesCount}}) ->
    case Nodes of
        [] ->
            {ok, undefined, []};
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
                            {ok, undefined, Items}
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
    Snapshot = ns_cluster_membership:get_snapshot(),
    case ns_cluster_membership:should_run_service(
           Snapshot, Service:get_type(), ns_node_disco:ns_server_node()) of
        true ->
            local;
        false ->
            ServiceNodes =
                ns_cluster_membership:service_actual_nodes(
                  Snapshot, Service:get_type()),
            {remote, ServiceNodes, length(ServiceNodes)}
    end.

handle_node_disco_event(Event, Pid) ->
    case Event of
        {ns_node_disco_events, _NodesOld, _NodesNew} ->
            Pid ! notable_event;
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
