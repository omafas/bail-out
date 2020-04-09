%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2012-2017 Couchbase, Inc.
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
-module(set_view_update_daemon).
-behaviour(gen_server).

%% public API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("ns_common.hrl").
-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-record(state, {interval,
                num_changes,
                replica_num_changes,
                timer_ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({set_view_update_daemon, _}) ->
              Self ! config_changed;
          (_) ->
              ok
      end),

    State = read_config(#state{}),

    ?log_info("Set view update daemon, starting with the following settings:~n"
              "  update interval:           ~pms~n"
              "  minimum number of changes: ~p~n",
              [State#state.interval, State#state.num_changes]),
    {ok, schedule_timer(State)}.


handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.


handle_cast(trigger_updates,
            #state{num_changes=MinNumChanges,
                   replica_num_changes=ReplicaMinNumChanges} = State) ->
    Buckets = ns_bucket:node_bucket_names_of_type(ns_node_disco:ns_server_node(),
                                                  membase, couchstore),
    lists:foreach(
      fun (Bucket) ->
              ok = trigger_updates_for_bucket(Bucket,
                                              MinNumChanges, ReplicaMinNumChanges)
      end, Buckets),
    {noreply, schedule_timer(State)}.

handle_info(config_changed, State) ->
    misc:flush(config_changed),
    {noreply, schedule_timer(read_config(State))};
handle_info(Info, State) ->
    ?log_warning("Got unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

schedule_timer(#state{interval=0} = State) ->
    cancel_timer(State);
schedule_timer(#state{interval=Interval} = State) ->
    _ = cancel_timer(State),
    {ok, NewTimerRef} =
        timer2:apply_after(Interval, gen_server, cast, [?MODULE, trigger_updates]),
    State#state{timer_ref=NewTimerRef}.

cancel_timer(#state{timer_ref=undefined} = State) ->
    State;
cancel_timer(#state{timer_ref=Ref} = State) ->
    {ok, cancel} = timer2:cancel(Ref),
    State#state{timer_ref=undefined}.

read_config(State) ->
    Config = ns_config:get(),

    Opts = ns_config:search(Config, set_view_update_daemon, []),
    UpdateInterval = proplists:get_value(update_interval, Opts, 5000),
    UpdateMinChanges = proplists:get_value(update_min_changes, Opts, 5000),
    ReplicaUpdateMinChanges =
        proplists:get_value(replica_update_min_changes, Opts, 5000),

    State#state{interval=UpdateInterval,
                num_changes=UpdateMinChanges,
                replica_num_changes=ReplicaUpdateMinChanges}.

trigger_updates_for_bucket(Bucket, MinNumChanges, ReplicaMinNumChanges) ->
    SetName = list_to_binary(Bucket),

    lists:foreach(
      fun (#doc{id=Id} = DDoc) ->
              case Id of
                  <<"_design/dev_", _/binary>> ->
                      ok;
                  _Other ->
                      do_trigger_update_for_ddoc(SetName, DDoc,
                                                 MinNumChanges, ReplicaMinNumChanges)
              end
      end, capi_utils:full_live_ddocs(Bucket)).

do_trigger_update_for_ddoc(SetName, DDoc0, MinNumChanges, ReplicaMinNumChanges) ->
    #doc{id=Id} = DDoc = couch_doc:with_ejson_body(DDoc0),
    DDocMinNumChanges =
        ddoc_update_min_changes(<<"updateMinChanges">>, DDoc, MinNumChanges),
    DDocReplicaMinNumChanges =
        ddoc_update_min_changes(<<"replicaUpdateMinChanges">>,
                                DDoc, ReplicaMinNumChanges),

    case DDocMinNumChanges of
        0 ->
            ok;
        _ ->
            lists:foreach(
              fun (Type) ->
                      ok = couch_set_view:trigger_update(
                             Type, SetName, Id, DDocMinNumChanges)
              end, [mapreduce_view, spatial_view])
    end,

    case DDocReplicaMinNumChanges of
        0 ->
            ok;
        _ ->
            lists:foreach(
              fun (Type) ->
                      ok = couch_set_view:trigger_replica_update(
                             Type, SetName, Id, DDocReplicaMinNumChanges)
              end, [mapreduce_view, spatial_view])
    end.

ddoc_update_min_changes(Key, #doc{body={Body}}, Default) ->
    {Options} = proplists:get_value(<<"options">>, Body, {[]}),
    case proplists:get_value(Key, Options) of
        V when is_integer(V) ->
            V;
        _ ->
            Default
    end.
