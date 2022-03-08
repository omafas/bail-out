%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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

    lists:foreach(fun ({config_events, Filter}) ->
                          chronicle_compat_events:notify_if_key_changes(
                            Filter, maybe_refresh);
                      ({Event, Filter}) ->
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
