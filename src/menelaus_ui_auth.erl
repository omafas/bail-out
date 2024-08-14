%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_ui_auth).

-include("ns_common.hrl").
-include("rbac.hrl").

-export([start_link/0]).
-export([init/0]).

-export([start_ui_session/3, maybe_refresh/1,
         check/1, reset/0, logout/1, session_type_by_id/1,
         logout_by_session_name/2, logout_by_session_type/1,
         get_authn_res_from_ui_session/1]).

start_link() ->
    token_server:start_link(?MODULE, 1024, ?UI_AUTH_EXPIRATION_SECONDS,
                            fun (#uisession{authn_res = AuthnRes}, _Token) ->
                                #authn_res{identity = Id,
                                           session_id = SessionId} = AuthnRes,
                                ns_audit:session_expired(Id, SessionId)
                            end).

-spec start_ui_session(simple | {sso, SSOName :: string()},
                       binary(),
                       #authn_res{}) -> auth_token().
start_ui_session(UISessionType, SessionName,
                 #authn_res{type = ui} = AuthnRes) ->
    ExpirationDatetimeUTC = AuthnRes#authn_res.expiration_datetime_utc,
    SessionInfo = #uisession{type = UISessionType,
                             session_name = SessionName,
                             authn_res = AuthnRes},
    token_server:generate(?MODULE, SessionInfo, ExpirationDatetimeUTC).

-spec maybe_refresh(auth_token()) -> nothing | {new_token, auth_token()}.
maybe_refresh(Token) ->
    token_server:maybe_refresh(?MODULE, Token).

-spec get_token_node(auth_token() | undefined) ->
        {Node :: atom(), auth_token() | undefined}.
get_token_node(undefined) ->
    {local, undefined};
get_token_node(Token) ->
    try
        erlang:binary_to_term(base64:decode(Token), [safe])
    catch
        _:_ -> {local, Token}
    end.

-spec check(auth_token() | undefined) -> false | {ok, term()}.
check(Token) ->
    {Node, CleanToken} = get_token_node(Token),
    case token_server:check(?MODULE, CleanToken, Node) of
        false -> false;
        {ok, #uisession{authn_res = #authn_res{} = AuthnRes}} ->
            {ok, AuthnRes};
        {ok, Id} -> %% Pre-7.6 nodes will return Id
            {ok, (menelaus_auth:init_auth(Id))#authn_res{type = ui}}
    end.

-spec reset() -> ok.
reset() ->
    token_server:reset_all(?MODULE).

-spec logout(SessionId :: binary()) -> #uisession{} | undefined.
logout(SessionId) ->
    AuthPattern = #authn_res{type = ui, session_id = SessionId, _ = '_'},
    MemoParrern = #uisession{authn_res = AuthPattern, _ = '_'},
    case token_server:take_memos(?MODULE, MemoParrern) of
        [] -> undefined;
        %% In general take_memos can return multiple memos, but in this case
        %% (since we filter by session id) there should be only one _unique_
        %% session.
        [#uisession{} = SessionInfo] -> SessionInfo
    end.

session_type_by_id(SessionId) ->
    AuthPattern = #authn_res{type = ui, session_id = SessionId, _ = '_'},
    MemoParrern = #uisession{authn_res = AuthPattern, _ = '_'},
    case token_server:find_memos(?MODULE, MemoParrern) of
        [] -> undefined;
        [#uisession{type = Type}] -> Type
    end.

init() ->
    ns_pubsub:subscribe_link(ns_config_events,
                             fun ns_config_event_handler/1).

%% TODO: implement it correctly for all users or get rid of it
ns_config_event_handler({rest_creds, _}) ->
    AuthnPattern = #authn_res{type = ui,
                              identity = {'_', admin},
                              _ = '_'},
    token_server:purge(?MODULE, #uisession{authn_res = AuthnPattern, _ = '_'});
ns_config_event_handler(_Evt) ->
    ok.

logout_by_session_name(Type, SessionName) ->
    Pattern = #uisession{type = Type, session_name = SessionName, _ = '_'},
    token_server:purge(?MODULE, Pattern).

logout_by_session_type(Type) ->
    Pattern = #uisession{type = Type, _ = '_'},
    token_server:purge(?MODULE, Pattern).

-spec get_authn_res_from_ui_session(Id :: string()) -> #authn_res{} | undefined.
get_authn_res_from_ui_session(Id) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            SessionBin = list_to_binary(Id),
            AuthPattern = #authn_res{type = ui, session_id = SessionBin,
                                     _ = '_'},
            MemoPattern = #uisession{authn_res = AuthPattern, _ = '_'},
            case token_server:find_memos(?MODULE, MemoPattern) of
                [] -> undefined;
                [#uisession{authn_res = UiAuthnRes}] -> UiAuthnRes
            end;
        true ->
            rpc:call(ns_node_disco:ns_server_node(),
                     ?MODULE, get_authn_res_from_ui_session, [Id])
    end.
