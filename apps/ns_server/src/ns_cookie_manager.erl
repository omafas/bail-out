%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Process managing cookies. Split from ns_node_disco to avoid race
%% conditions while saving cookies to disk.
-module(ns_cookie_manager).

-behavior(gen_server).
-behavior(ns_log_categorizing).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% API
-export([start_link/0,
         cookie_init/0, cookie_sync/0]).

-export([ns_log_cat/1, ns_log_code_string/1, sanitize_cookie/1]).


-define(SERVER, ?MODULE).
-record(state, {}).

-define(COOKIE_INHERITED, 1).
-define(COOKIE_SYNCHRONIZED, 2).
-define(COOKIE_GEN, 3).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

cookie_init() ->
    gen_server:call(?SERVER, cookie_init).

cookie_sync() ->
    gen_server:call(?SERVER, cookie_sync).

init([]) ->
    {ok, #state{}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _) ->
    {ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

handle_call(cookie_init, _From, State) ->
    {reply, do_cookie_init(), State};
handle_call(cookie_sync, _From, State) ->
    {reply, do_cookie_sync(), State}.

sanitize_cookie(nocookie) ->
    nocookie;
sanitize_cookie(Cookie) when is_atom(Cookie) ->
    sanitize_cookie(list_to_binary(atom_to_list(Cookie)));
sanitize_cookie(Cookie) when is_binary(Cookie) ->
    ns_config_log:sanitize_value(Cookie).

%% Auxiliary functions

do_cookie_init() ->
    NewCookie = do_cookie_gen(),
    ok = do_cookie_set(NewCookie),
    ?user_log(?COOKIE_GEN, "Initial otp cookie generated: ~p",
              [sanitize_cookie(NewCookie)]),
    {ok, NewCookie}.

do_cookie_gen() ->
    case misc:get_env_default(dont_reset_cookie, false) of
        false ->
            misc:generate_cookie();
        true ->
            erlang:get_cookie()
    end.

do_cookie_get() ->
    ns_config:search_prop(ns_config:latest(), otp, cookie).

do_cookie_set(Cookie) ->
    OldCookie = erlang:get_cookie(),

    erlang:set_cookie(node(), Cookie),
    maybe_disconnect_stale_nodes(OldCookie, Cookie),
    ns_config:set(otp, [{cookie, Cookie}]).

do_cookie_sync() ->
    ?log_debug("ns_cookie_manager do_cookie_sync"),
    case do_cookie_get() of
        undefined ->
            case erlang:get_cookie() of
                nocookie ->
                    % TODO: We should have length(nodes_wanted) == 0 or 1,
                    %       so, we should check that assumption.
                    do_cookie_init();
                CurrCookie ->
                    ok = do_cookie_set(CurrCookie),
                    ?user_log(?COOKIE_INHERITED,
                              "Node ~p inherited otp cookie ~p from cluster",
                              [node(), sanitize_cookie(CurrCookie)]),
                    {ok, CurrCookie}
            end;
        WantedCookie ->
            case erlang:get_cookie() of
                WantedCookie -> {ok, WantedCookie};
                _ ->
                    erlang:set_cookie(node(), WantedCookie),
                    disconnect_stale_nodes(),
                    ?user_log(?COOKIE_SYNCHRONIZED,
                              "Node ~p synchronized otp cookie ~p from cluster",
                              [node(), sanitize_cookie(WantedCookie)]),
                    {ok, WantedCookie}
            end
    end.

maybe_disconnect_stale_nodes(OldCookie, NewCookie) ->
    case OldCookie =:= NewCookie of
        true ->
            ok;
        false ->
            disconnect_stale_nodes()
    end.

disconnect_stale_nodes() ->
    lists:foreach(fun erlang:disconnect_node/1, nodes()).

ns_log_cat(_X) ->
    info.

ns_log_code_string(?COOKIE_INHERITED) ->
    "cookie update";
ns_log_code_string(?COOKIE_SYNCHRONIZED) ->
    "cookie update";
ns_log_code_string(?COOKIE_GEN) ->
    "cookie update".
