%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2018 Couchbase, Inc.
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
%% @doc versioned cache, emptied when version changes

-module(versioned_cache).

-behaviour(gen_server).

-export([start_link/5, get/2]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_call/3,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(state, {name, get, get_version, version}).

get(Name, Id) ->
    case mru_cache:lookup(Name, Id) of
        {ok, Val} ->
            Val;
        false ->
            gen_server:call(Name, {get_and_cache, Id})
    end.

start_link(Name, CacheSize, Get, GetEvents, GetVersion) ->
    proc_lib:start_link(?MODULE, init,
                        [[Name, CacheSize, Get, GetEvents, GetVersion]]).


init([Name, CacheSize, Get, GetEvents, GetVersion]) ->
    register(Name, self()),
    ?log_debug("Starting versioned cache ~p", [Name]),
    mru_cache:new(Name, CacheSize),
    Pid = self(),
    proc_lib:init_ack({ok, Pid}),

    lists:foreach(fun ({Event, Filter}) ->
                          Handler =
                              fun (Evt) ->
                                      case Filter(Evt) of
                                          true ->
                                              Pid ! maybe_refresh;
                                          false ->
                                              ok
                                      end
                              end,
                          ns_pubsub:subscribe_link(Event, Handler)
                  end, GetEvents()),
    gen_server:enter_loop(?MODULE, [], #state{get = Get,
                                              get_version = GetVersion,
                                              name = Name}).

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_call({get_and_cache, Id}, _From, #state{name = Name, get = Get} = State) ->
    case mru_cache:lookup(Name, Id) of
        {ok, Val} ->
            {reply, Val, State};
        false ->
            Res = Get(Id),
            mru_cache:add(Name, Id, Res),
            {reply, Res, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(maybe_refresh, #state{get_version = GetVersion,
                                  version = Version,
                                  name = Name} = State) ->
    misc:flush(maybe_refresh),
    case GetVersion() of
        Version ->
            {noreply, State};
        NewVersion ->
            ?log_debug("Flushing cache ~p due to version change from ~p to ~p",
                       [Name, Version, NewVersion]),
            mru_cache:flush(Name),
            {noreply, State#state{version = NewVersion}}
    end.
