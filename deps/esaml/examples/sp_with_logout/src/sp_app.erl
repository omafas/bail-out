%% esaml - SAML for erlang
%%
%% Copyright (c) 2013, Alex Wilson and the University of Queensland
%% All rights reserved.
%%
%% Distributed subject to the terms of the 2-clause BSD license, see
%% the LICENSE file in the root of the distribution.

-module(sp_app).

-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    HostMatch = '_',
    PathMatch = "/saml/:operation",
    InitialState = #{},
    Dispatch = cowboy_router:compile([
        {HostMatch, [{PathMatch, sp_handler, InitialState}]}
    ]),
    {ok, _} = cowboy:start_clear(sp_with_logout_http_listener, [{port, 8080}],
        #{env => #{dispatch => Dispatch}}),
    sp_sup:start_link().

stop(_State) ->
    ok.
