%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
-module(ns_tick_agent).

-behavior(gen_server2).

-include("cut.hrl").
-include("ns_common.hrl").

-export([start_link/0, send_tick/2]).

%% gen_server2 callbacks
-export([init/1, handle_info/2, handle_cast/2]).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

send_tick(Nodes, TS) ->
    case cluster_compat_mode:is_cluster_65() of
        true ->
            gen_server2:abcast(Nodes, ?MODULE, {tick, node(), TS});
        false ->
            lists:foreach(notify(_, TS), Nodes)
    end.

%% callbacks
init([]) ->
    Master = mb_master:master_node(),

    Self = self(),
    ns_pubsub:subscribe_link(leader_events,
                             case _ of
                                 {new_leader, _} = Event ->
                                     Self ! Event;
                                 _ ->
                                     ok
                             end),
    {ok, Master}.

handle_info({new_leader, NewMaster}, _OldMaster) ->
    {noreply, NewMaster};
handle_info(Info, Master) ->
    ?log_warning("Received an unexpected message ~p", [Info]),
    {noreply, Master}.

handle_cast({tick, FromNode, TS}, Master) ->
    {noreply, handle_tick(Master, FromNode, TS)};
handle_cast(Cast, Master) ->
    ?log_warning("Received an unexpected cast ~p", [Cast]),
    {noreply, Master}.

%% internal
handle_tick(FromNode, Master, TS)
  when FromNode =:= Master ->
    notify(TS),
    Master;
handle_tick(FromNode, Master, _TS) ->
    ?log_warning("Ignoring tick from a non-master node ~p. Master: ~p",
                 [FromNode, Master]),
    Master.

notify(TS) ->
    notify(node(), TS).

notify(Node, TS) ->
    gen_event:notify({ns_tick_event, Node}, {tick, TS}).
