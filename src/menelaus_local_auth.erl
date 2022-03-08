%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_local_auth).

-include("ns_common.hrl").

-define(REGENERATE_AFTER, 60000).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([check_token/1]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec check_token(string()) -> true | false.
check_token(Token) ->
    gen_server:call(?MODULE, {check_token, Token}, infinity).

init([]) ->
    self() ! generate_token,
    {ok, {undefined, undefined}}.

handle_call({check_token, Token}, _From, {_, Token} = State) ->
    {reply, true, State};
handle_call({check_token, Token}, _From, {Token, _} = State) ->
    {reply, true, State};
handle_call({check_token, _}, _From, State) ->
    {reply, false, State}.

handle_cast(Msg, _State) ->
    erlang:error({unknown_cast, Msg}).

handle_info(generate_token, {_OldToken, CurrentToken}) ->
    misc:flush(generate_token),
    Token = binary_to_list(couch_uuids:random()),

    Path = path_config:component_path(data, "localtoken"),

    ok = misc:atomic_write_file(Path, Token ++ "\n"),
    erlang:send_after(?REGENERATE_AFTER, self(), generate_token),

    {noreply, {CurrentToken, Token}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
