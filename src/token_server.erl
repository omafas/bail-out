%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(token_server).

-include("ns_common.hrl").
-include("rbac.hrl").

-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([generate/2, generate/3, maybe_refresh/2,
         check/3, reset_all/1,
         purge/2, take/2, take_memos/2, find_memos/2]).

-define(EXPIRATION_CHECKING_INTERVAL, 15000).

-type token() :: binary().

-record(token_record,
        {token :: token() | '$1' | '_',
         %% session expiration shows when the whole session (token chain)
         %% expires
         session_expiration_timestamp :: pos_integer() | undefined | '_',
         %% expiration_timestamp shows when a particular token expires
         expiration_timestamp :: pos_integer() | '_',
         %% refresh_timestamp shows when token can be refreshed/rotated,
         %% undefined means it can't be refreshed/rotated
         refresh_timestamp = no_refresh :: pos_integer() | no_refresh | '_',
         prev_token :: token() | undefined | '_',
         next_token :: token() | undefined | '_',
         memo :: term()}).

start_link(Module, MaxTokens, TokenExpirationSeconds, ExpirationCallback) ->
    gen_server:start_link({local, Module}, ?MODULE,
                          [Module, MaxTokens, TokenExpirationSeconds,
                           ExpirationCallback], []).

tok2bin(Token) when is_list(Token) ->
    list_to_binary(Token);
tok2bin(Token) ->
    Token.

generate(Module, Memo) ->
    generate(Module, Memo, undefined).

generate(Module, Memo, SessionExpDatetimeUTC) ->
    SessionExpirationTimestampS =
        case SessionExpDatetimeUTC of
            undefined ->
                undefined;
            _ ->
                CurrentDT = calendar:universal_time(),
                get_now() +
                calendar:datetime_to_gregorian_seconds(SessionExpDatetimeUTC) -
                calendar:datetime_to_gregorian_seconds(CurrentDT)
        end,
    gen_server:call(Module,
                    {generate, Memo, SessionExpirationTimestampS}, infinity).

maybe_refresh(Module, Token) ->
    gen_server:call(Module, {maybe_refresh, tok2bin(Token)}, infinity).


check(_, undefined, _) ->
    false;
check(Module, Token, local) ->
    %% we do not check if node is part of the cluster for the
    %% local node to avoid 401 during the node rename
    gen_server:call(Module, {check, tok2bin(Token)}, infinity);
check(Module, Token, Node) ->
    case lists:member(Node, ns_node_disco:nodes_actual()) of
        true ->
            gen_server:call({Module, Node}, {check, tok2bin(Token)}, infinity);
        false ->
            false
    end.

reset_all(Module) ->
    gen_server:call(Module, reset_all, infinity).

take(Module, Token) ->
    gen_server:call(Module, {take, tok2bin(Token)}, infinity).

take_memos(Module, MemoPattern) ->
    gen_server:call(Module, {take_memos, MemoPattern}, infinity).

find_memos(Module, MemoPattern) ->
    gen_server:call(Module, {find_memos, MemoPattern}, infinity).

purge(Module, MemoPattern) ->
    gen_server:cast(Module, {purge, MemoPattern}).

-record(state, {table_by_token,
                table_by_exp,
                max_tokens,
                token_exp_seconds,
                exp_callback}).

init([Module, MaxTokens, TokenExpirationSeconds, ExpirationCallback]) ->
    ExpTable = list_to_atom(atom_to_list(Module) ++ "_by_expiration"),
    _ = ets:new(Module, [protected, named_table, set,
                         {keypos, #token_record.token}]),
    _ = ets:new(ExpTable, [protected, named_table, ordered_set]),
    Module:init(),
    case ExpirationCallback of
        undefined ->
            ok;
        _ ->
            erlang:send_after(?EXPIRATION_CHECKING_INTERVAL, self(),
                              check_for_expirations)
    end,
    {ok, #state{table_by_token = Module,
                table_by_exp = ExpTable,
                max_tokens = MaxTokens,
                token_exp_seconds = TokenExpirationSeconds,
                exp_callback = ExpirationCallback}}.

maybe_expire(#state{table_by_token = Table,
                    max_tokens = MaxTokens} = State) ->
    Size = ets:info(Table, size),
    case Size < MaxTokens of
        true ->
            ok;
        _ ->
            expire_oldest(State)
    end.

expire_oldest(#state{table_by_token = Table,
                     table_by_exp = ExpTable,
                     exp_callback = ExpCallback} = State) ->
    {_ExpirationTS, Token} = ets:first(ExpTable),
    {Memo, NextToken} =
        case ExpCallback of
            undefined ->
                {unused, unused};
            _ ->
                [#token_record{memo = Memo0, next_token = Next}] =
                    ets:lookup(Table, Token),
                {Memo0, Next}
           end,
    remove_token_chain_left(Token, true, State),
    maybe_do_callback(ExpCallback, Memo, Token, NextToken),
    ok.

remove_token(Token, #state{table_by_token = Table,
                           table_by_exp = ExpTable}) ->
    case ets:lookup(Table, Token) of
        [#token_record{token = Token,
                       expiration_timestamp = ExpirationTS,
                       prev_token = PrevToken,
                       next_token = NextToken}] ->
            ets:delete(ExpTable, {ExpirationTS, Token}),
            ets:delete(Table, Token),
            {PrevToken, NextToken};
        [] ->
            {undefined, undefined}
    end.

remove_token_chain(undefined, _State) -> ok;
remove_token_chain(Token, State) ->
    {PrevToken, NextToken} = remove_token(Token, State),
    remove_token_chain_left(PrevToken, false, State),
    remove_token_chain_right(NextToken, State),
    ok.

%% Remove all the tokens that are older than Token in its session.
%% It sets next element's prev_token to undefined
%% only if ShouldUpdateNext is true.
remove_token_chain_left(undefined, _, _State) -> ok;
remove_token_chain_left(Token, ShouldUpdateNext,
                        #state{table_by_token = Table} = State) ->
    {PrevToken, NextToken} = remove_token(Token, State),
    case ShouldUpdateNext of
        true ->
            ets:update_element(Table,
                               NextToken,
                               {#token_record.prev_token, undefined});
        false ->
            ok
    end,
    remove_token_chain_left(PrevToken, false, State).

%% Remove all the tokens that are newer than Token in its session.
%% Note: it never updates prev element's next_token.
remove_token_chain_right(undefined, _State) -> ok;
remove_token_chain_right(Token, State) ->
    {_PrevToken, NextToken} = remove_token(Token, State),
    remove_token_chain_right(NextToken, State).

get_now() ->
    erlang:monotonic_time(second).

do_generate_token(ReplacedToken, Memo, SesExpTimestampS,
                  #state{table_by_token = Table,
                         table_by_exp = ExpTable,
                         token_exp_seconds = TokenExpirationSeconds}) ->
    %% NOTE: couch_uuids:random is using crypto-strong random
    %% generator
    Token = couch_uuids:random(),
    Now = get_now(),
    {ExpirationTS, RefreshTS} =
        case is_number(SesExpTimestampS) andalso
             (Now + TokenExpirationSeconds >= SesExpTimestampS) of
            true ->
                {SesExpTimestampS, no_refresh};
            false ->
                {Now + TokenExpirationSeconds,
                 Now + TokenExpirationSeconds div 2}
        end,
    ets:insert(Table,
               #token_record{token = Token,
                             session_expiration_timestamp = SesExpTimestampS,
                             expiration_timestamp = ExpirationTS,
                             refresh_timestamp = RefreshTS,
                             prev_token = ReplacedToken,
                             next_token = undefined,
                             memo = Memo}),
    ets:insert(ExpTable, {{ExpirationTS, Token}}),
    case ReplacedToken =/= undefined of
        true ->
            true = ets:update_element(Table,
                                      ReplacedToken,
                                      {#token_record.next_token, Token});
        false ->
            ok
    end,
    Token.

validate_token_maybe_expire(Now, Token,
                            #state{table_by_token = Table,
                                   exp_callback = ExpCallback} = State) ->
    case ets:lookup(Table, Token) of
        [#token_record{token = Token,
                       expiration_timestamp = ExpirationTS,
                       next_token = NextToken,
                       memo = Memo} = Record] ->
            case ExpirationTS < Now of
                true ->
                    %% This token has expired, remove it and all the tokens
                    %% in this sessions that are older
                    remove_token_chain_left(Token, true, State),
                    maybe_do_callback(ExpCallback, Memo, Token, NextToken),
                    false;
                _ ->
                    Record
            end;
        [] ->
            false
    end.

maybe_do_callback(undefined, _Memo, _Token, _NextToken) ->
    ok;
%% Calling the callback only if that is the last token for this session
%% (when there is no next token)
maybe_do_callback(Callback, Memo, Token, undefined) ->
    Callback(Memo, Token);
maybe_do_callback(_Callback, _Memo, _Token, _NextToken) ->
    ok.

handle_call(reset_all, _From, #state{table_by_token = Table,
                                     table_by_exp = ExpTable} = State) ->
    ets:delete_all_objects(Table),
    ets:delete_all_objects(ExpTable),
    {reply, ok, State};
handle_call({generate, Memo, SesExpirationTimestampS}, _From, State) ->
    maybe_expire(State),
    Token = do_generate_token(undefined, Memo, SesExpirationTimestampS, State),
    {reply, Token, State};
handle_call({maybe_refresh, Token}, _From, #state{} = State) ->
    Now = get_now(),
    case validate_token_maybe_expire(Now, Token, State) of
        false ->
            {reply, nothing, State};
        #token_record{refresh_timestamp = RefreshTS,
                      memo = Memo,
                      next_token = NextToken,
                      session_expiration_timestamp = SesExpirationTimestampS} ->
            case is_number(RefreshTS) andalso (RefreshTS =< Now) of
                true when NextToken == undefined ->
                    %% NOTE: we take note of current and still valid
                    %% token for correctness of logout
                    %% NOTE: It is important that we are not modifying Memo here
                    %% because we rely on the fact that all tokens in a session
                    %% have the same memo when removing tokens (purge and take)
                    NewToken = do_generate_token(Token,
                                                 Memo,
                                                 SesExpirationTimestampS,
                                                 State),
                    {reply, {new_token, NewToken}, State};
                true ->
                    %% Newer token already exists for this token
                    {reply, {new_token, NextToken}, State};
                false ->
                    {reply, nothing, State}
            end
    end;
%% Find session by token, remove it (all tokens), return Memo for this session
handle_call({take, Token}, _From, State) ->
    case validate_token_maybe_expire(get_now(), Token, State) of
        false ->
            {reply, false, State};
        #token_record{memo = Memo} ->
            remove_token_chain(Token, State),
            {reply, {ok, Memo}, State}
    end;
handle_call({find_memos, MemoPattern}, _From,
            #state{table_by_token = Table} = State) ->
    Records = ets:select(Table, [{#token_record{memo = MemoPattern, _ = '_'},
                                  [], ['$_']}]),
    Memos = lists:usort([Memo || #token_record{memo = Memo} <- Records]),
    {reply, Memos, State};
%% Remove all the sessions (all tokens) that have Memo that matches the pattern
%% Return Memos of all removed sessions
handle_call({take_memos, MemoPattern}, _From,
            #state{table_by_token = Table} = State) ->
    Records = ets:select(Table, [{#token_record{memo = MemoPattern, _ = '_'},
                                  [], ['$_']}]),
    [remove_token(Token, State) || #token_record{token = Token} <- Records],
    Memos = lists:usort([Memo || #token_record{memo = Memo} <- Records]),
    {reply, Memos, State};
handle_call({check, Token}, _From, State) ->
    case validate_token_maybe_expire(get_now(), Token, State) of
        false ->
            {reply, false, State};
        #token_record{memo = Memo} ->
            Res =
                case cluster_compat_mode:is_cluster_76() of
                    true -> Memo;
                    false ->
                        %% token server should not know anything about
                        %% memo guts, but we make exception here to
                        %% stay backward compatible
                        case Memo of
                            #uisession{authn_res = #authn_res{identity = Id}} ->
                                Id;
                            _ ->
                                Memo
                        end
                end,
            {reply, {ok, Res}, State}
    end;
handle_call(Msg, From, _State) ->
    erlang:error({unknown_call, Msg, From}).

%% Remove all the sessions that have Memo that matches the pattern
handle_cast({purge, MemoPattern}, #state{table_by_token = Table} = State) ->
    Tokens = ets:match(Table, #token_record{token = '$1',
                                            memo = MemoPattern,
                                            _ = '_'}),
    ?log_debug("Purge tokens ~p", [Tokens]),
    [remove_token(Token, State) || [Token] <- Tokens],
    {noreply, State};

handle_cast(Msg, _State) ->
    erlang:error({unknown_cast, Msg}).

handle_info(check_for_expirations, State) ->
    do_check_for_expirations(State),
    erlang:send_after(?EXPIRATION_CHECKING_INTERVAL, self(),
                      check_for_expirations),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

do_check_for_expirations(#state{table_by_exp = ExpTable} = State) ->
    case ets:first(ExpTable) of
        '$end_of_table' ->
            ok;
        {ExpirationTS, _Token} ->
            Now = get_now(),
            case ExpirationTS < Now of
                true ->
                    expire_oldest(State),
                    do_check_for_expirations(State);
                false ->
                    ok
            end
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
