%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc handlers for miscellaneous REST API's

-module(menelaus_web_misc).

-define(MAX_EVENT_SIZE, 3*1024).

-export([handle_uilogin/1,
         handle_uilogout/1,
         handle_can_use_cert_for_auth/1,
         handle_versions/1,
         handle_tasks/2,
         handle_event_log_post/1,
         handle_log_post/1]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3,
         reply_ok/3,
         reply_ok/4,
         parse_validate_number/3]).

-include("ns_common.hrl").

handle_uilogin(Req) ->
    Params = mochiweb_request:parse_post(Req),
    menelaus_auth:uilogin(Req, Params).

handle_uilogout(Req) ->
    Token = menelaus_auth:get_token(Req),
    false = (Token =:= undefined),
    menelaus_ui_auth:logout(Token),
    menelaus_auth:complete_uilogout(Req).

handle_can_use_cert_for_auth(Req) ->
    RV = menelaus_auth:can_use_cert_for_auth(Req),
    menelaus_util:reply_json(Req, {[{cert_for_auth, RV}]}).

handle_versions(Req) ->
    reply_json(Req, {struct, menelaus_web_cache:get_static_value(versions)}).

handle_tasks(PoolId, Req) ->
    DefaultTimeout = integer_to_list(?REBALANCE_OBSERVER_TASK_DEFAULT_TIMEOUT),
    RebTimeoutS = proplists:get_value("rebalanceStatusTimeout",
                                      mochiweb_request:parse_qs(Req),
                                      DefaultTimeout),
    case parse_validate_number(RebTimeoutS, 1000, 120000) of
        {ok, RebTimeout} ->
            do_handle_tasks(PoolId, Req, RebTimeout);
        _ ->
            reply_json(Req, {struct, [{rebalanceStatusTimeout, <<"invalid">>}]}, 400)
    end.

do_handle_tasks(PoolId, Req, RebTimeout) ->
    JSON = ns_doctor:build_tasks_list(PoolId, RebTimeout),
    reply_json(Req, JSON, 200).

handle_log_post(Req) ->
    Params = mochiweb_request:parse_post(Req),
    Msg = proplists:get_value("message", Params),
    LogLevel = proplists:get_value("logLevel", Params),
    Component = proplists:get_value("component", Params),

    Errors =
        lists:flatten([case Msg of
                           undefined ->
                               {<<"message">>, <<"missing value">>};
                           _ ->
                               []
                       end,
                       case LogLevel of
                           "info" ->
                               [];
                           "warn" ->
                               [];
                           "error" ->
                               [];
                           _ ->
                               {<<"logLevel">>, <<"invalid or missing value">>}
                       end,
                       case Component of
                           undefined ->
                               {<<"component">>, <<"missing value">>};
                           _ ->
                               []
                       end]),

    case Errors of
        [] ->
            Fun = list_to_existing_atom([$x | LogLevel]),
            ale:Fun(?USER_LOGGER,
                    {list_to_atom(Component), unknown, -1}, undefined, Msg, []),
            reply_json(Req, []);
        _ ->
            reply_json(Req, {struct, Errors}, 400)
    end.

handle_event_log_post(Req) ->
    %% If cluster_compat < 7.1.0, drop the log and return 200 ok.
    case cluster_compat_mode:is_cluster_71() of
        true ->
            do_handle_event_log_post(Req);
        false ->
            reply_json(Req, [], 200)
    end.

do_handle_event_log_post(Req) ->
    Log = mochiweb_request:recv_body(Req),

    %% Validate Event JSON size.
    case erlang:byte_size(Log) of
        Size when Size > ?MAX_EVENT_SIZE ->
            Msg = io_lib:format("Event JSON larger than ~p bytes",
                                [?MAX_EVENT_SIZE]),
            menelaus_util:web_exception(413, Msg);
        _ ->
            ok
    end,

    validator:handle(fun (_Values) ->
                       event_log:log(Log),
                       reply_json(Req, [], 200)
                     end, Req, json, event_log:validators()).
