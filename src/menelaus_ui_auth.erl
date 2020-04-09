%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2019 Couchbase, Inc.
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
-module(menelaus_ui_auth).

-include("ns_common.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/0]).

-export([generate_token/1, maybe_refresh/1,
         check/1, reset/0, logout/1, set_token_node/2]).

start_link() ->
    token_server:start_link(?MODULE, 1024, ?UI_AUTH_EXPIRATION_SECONDS,
                            fun ns_audit:session_expired/2).

-spec generate_token(term()) -> auth_token().
generate_token(Memo) ->
    token_server:generate(?MODULE, Memo).

-spec maybe_refresh(auth_token()) -> nothing | {new_token, auth_token()}.
maybe_refresh(Token) ->
    token_server:maybe_refresh(?MODULE, Token).

-spec set_token_node(auth_token(), atom()) -> auth_token().
set_token_node(Token, Node) ->
    base64:encode(erlang:term_to_binary({Node, Token})).

-spec get_token_node(auth_token() | undefined) ->
        {Node :: atom(), auth_token() | undefined}.
get_token_node(undefined) ->
    {undefined, undefined};
get_token_node(Token) ->
    try
        erlang:binary_to_term(base64:decode(Token), [safe])
    catch
        _:_ -> {undefined, Token}
    end.

-spec check(auth_token() | undefined) -> false | {ok, term()}.
check(Token) ->
    {Node, CleanToken} = get_token_node(Token),
    case Node of
        undefined -> token_server:check(?MODULE, CleanToken);
        _ -> token_server:check(?MODULE, CleanToken, Node)
    end.

-spec reset() -> ok.
reset() ->
    token_server:reset_all(?MODULE).

-spec logout(auth_token()) -> ok.
logout(Token) ->
    token_server:remove(?MODULE, Token).

init() ->
    ns_pubsub:subscribe_link(ns_config_events,
                             fun ns_config_event_handler/1).

%% TODO: implement it correctly for all users or get rid of it
ns_config_event_handler({rest_creds, _}) ->
    token_server:purge(?MODULE, {'_', admin});
ns_config_event_handler(_Evt) ->
    ok.


-ifdef(TEST).
set_and_get_token_node_test() ->
    ?assertEqual({undefined, undefined}, get_token_node(undefined)),
    ?assertEqual({undefined, <<"token">>}, get_token_node(<<"token">>)),
    ?assertEqual({undefined, "token"}, get_token_node("token")),
    [?assertEqual({Node, Token}, get_token_node(set_token_node(Token, Node)))
        || _    <- lists:seq(1,1000),
           Node <- ['n_0@192.168.0.1',
                    'n_0@::1',
                    'n_0@2001:db8:0:0:0:ff00:42:8329',
                    'n_0@crazy*host%name;'],
           Token <- [couch_uuids:random(),
                     binary_to_list(couch_uuids:random())]].
-endif.
