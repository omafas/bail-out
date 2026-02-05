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

-define(DEFAULT_TIMEOUT, 120000).

-behaviour(gen_server).

-export([start_link/0, resave/0, get_key_ids_in_use/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([check_token/1]).

-record(state, {cur_token, prev_token, keys_in_use}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec check_token(string()) -> true | false.
check_token(Token) ->
    gen_server:call(?MODULE, {check_token, Token}, infinity).

resave() ->
    gen_server:call(?MODULE, resave, ?DEFAULT_TIMEOUT).

get_key_ids_in_use() ->
    gen_server:call(?MODULE, get_key_ids_in_use, ?DEFAULT_TIMEOUT).

init([]) ->
    self() ! generate_token,
    catch file:delete(localtoken_path()),
    {ok, #state{cur_token = undefined,
                prev_token = undefined,
                keys_in_use = []}}.

handle_call(resave, _From, #state{cur_token = undefined} = State) ->
    {reply, ok, State};
handle_call(resave, _From, State) ->
    {reply, ok, write_token(State)};
handle_call(get_key_ids_in_use, _From, #state{keys_in_use = Ids} = State) ->
    {reply, {ok, Ids}, State};
handle_call({check_token, undefined}, _From, State) ->
    {reply, false, State};
handle_call({check_token, Token}, _From, #state{cur_token = Token} = State) ->
    {reply, true, State};
handle_call({check_token, Token}, _From, #state{prev_token = Token} = State) ->
    {reply, true, State};
handle_call({check_token, _}, _From, State) ->
    {reply, false, State}.

handle_cast(Msg, _State) ->
    erlang:error({unknown_cast, Msg}).

handle_info(generate_token, #state{cur_token = CurrentToken} = State) ->
    misc:flush(generate_token),
    Token = binary_to_list(couch_uuids:random()),

    NewState = write_token(State#state{cur_token = Token,
                                       prev_token = CurrentToken}),

    erlang:send_after(?REGENERATE_AFTER, self(), generate_token),

    {noreply, NewState}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

write_token(#state{cur_token = Token} = State) ->
    Path = localtoken_path(),
    {ok, DS} = cb_crypto:fetch_deks_snapshot(configDek),
    ok = cb_crypto:atomic_write_file(Path, Token ++ "\n", DS),
    State#state{keys_in_use = [cb_crypto:get_dek_id(DS)]}.

localtoken_path() ->
    path_config:component_path(data, "localtoken").
