%% @author Couchbase <info@couchbase.com>
%% @copyright 2016 Couchbase, Inc.
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
