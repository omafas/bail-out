%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ns_log_sink).

-behaviour(gen_server).

%% API
-export([start_link/1, get_loglevel/2, meta/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("ale/include/ale.hrl").
-include("ns_common.hrl").
-include("ns_log.hrl").

-record(state, {}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

meta() ->
    [{type, raw}].

init([]) ->
    {ok, #state{}, hibernate}.

%% we don't handle preformatted logging calls
handle_call({log, _Msg}, _From, State) ->
    {reply, ok, State, hibernate};

handle_call({raw_log, Info, Msg}, _From, State) ->
    RV = do_log(Info, Msg),
    {reply, RV, State, hibernate};

%% not implemented for now
handle_call(sync, _From, State) ->
    {reply, ok, State, hibernate};

handle_call(_Request, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Msg, State) ->
    {noreply, State, hibernate}.

handle_info(_Info, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

pre_format_msg(Msg) ->
    %% our code demands msg to be a list, but it's more efficient to
    %% store and pass around binaries rather than mostly strings (but
    %% in fact iolists)
    [iolist_to_binary(Msg)].

do_log(#log_info{loglevel=LogLevel, time=Time, module=Module,
                 node=Node, user_data=undefined} = _Info, Msg) ->
    Category = loglevel_to_category(LogLevel),
    ns_log:log(Module, Node, Time, Category, pre_format_msg(Msg), []);
do_log(#log_info{loglevel=LogLevel, time=Time,
                 node=Node, user_data={Module, Code}} = _Info, Msg) ->
    Category = loglevel_to_category(LogLevel),
    ns_log:log(Module, Node, Time, Code, Category, pre_format_msg(Msg), []).

-spec loglevel_to_category(loglevel()) -> log_classification().
loglevel_to_category(debug) ->
    info;
loglevel_to_category(info) ->
    info;
loglevel_to_category(warn) ->
    warn;
loglevel_to_category(error) ->
    crit;
loglevel_to_category(critical) ->
    crit.

-spec get_loglevel(atom(), integer()) -> info | warn | critical.
get_loglevel(Module, Code) ->
    case catch(Module:ns_log_cat(Code)) of
        info -> info;
        warn -> warn;
        crit -> critical;
        _ -> info % Anything unknown is info (this includes {'EXIT', Reason})
    end.
