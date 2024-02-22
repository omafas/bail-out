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
         handle_get_ui_auth_methods/1,
         handle_versions/1,
         handle_tasks/2,
         handle_event_log_post/1,
         handle_log_post/1,
         handle_rotate_internal_creds/1]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3,
         reply_ok/3,
         reply_ok/4,
         parse_validate_number/3]).

-include("ns_common.hrl").
-include("cut.hrl").
-include("rbac.hrl").

handle_uilogin(Req) ->
    Params = mochiweb_request:parse_post(Req),
    menelaus_auth:uilogin(Req, Params).

handle_uilogout(Req) ->
    case menelaus_auth:get_authn_res(Req) of
        #authn_res{type = ui, session_id = SessionId} ->
            %% Note that in case of pre-elixir mixed cluster,
            %% SessionType will be undefined, because SessionId is undefined
            SessionType = menelaus_ui_auth:session_type_by_id(SessionId),
            DefaultLogout =
                fun () ->
                    {_Session, Headers} = menelaus_auth:complete_uilogout(Req),
                    menelaus_util:reply(Req, 200, Headers)
                end,
            case SessionType of
                saml ->
                    try
                        menelaus_web_saml:handle_uilogout_post(Req)
                    catch
                        error:disabled -> DefaultLogout()
                    end;
                _ ->
                    DefaultLogout()
            end;
        _ ->
            menelaus_util:reply(Req, 200)
    end.

handle_can_use_cert_for_auth(Req) ->
    RV = menelaus_auth:can_use_cert_for_auth(Req),
    menelaus_util:reply_json(Req, {[{cert_for_auth, RV}]}).

handle_get_ui_auth_methods(Req) ->
    CertAuth = menelaus_auth:can_use_cert_for_auth(Req),
    SamlAuth = menelaus_web_saml:is_enabled(),
    menelaus_util:reply_json(Req, {[{clientCertificates, CertAuth},
                                    {saml, SamlAuth}]}).

handle_versions(Req) ->
    reply_json(Req, {menelaus_web_cache:get_static_value(versions)}).

tasks_validators() ->
    [validator:integer(rebalanceStatusTimeout, 1000, 120000, _),
     validator:default(rebalanceStatusTimeout,
                       ?REBALANCE_OBSERVER_TASK_DEFAULT_TIMEOUT, _),
     validator:trimmed_string_multi_value(taskId, _)].

handle_tasks(PoolId, Req) ->
    validator:handle(do_handle_tasks(PoolId, Req, _), Req, qs,
                     tasks_validators()).

do_handle_tasks(PoolId, Req, Params) ->
    RebTimeout = proplists:get_value(rebalanceStatusTimeout, Params),
    case proplists:get_all_values(taskId, Params) of
        [] ->
            JSON = ns_doctor:build_tasks_list(PoolId, RebTimeout),
            reply_json(Req, JSON, 200);
        TaskIds ->
            BinaryTaskIds = lists:map(list_to_binary(_), TaskIds),
            case global_tasks:get_tasks(BinaryTaskIds) of
                [] ->
                    menelaus_util:reply_not_found(Req);
                Tasks ->
                    JSON = [{Task} || Task <- Tasks],
                    reply_json(Req, JSON, 200)
            end
    end.

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
            reply_json(Req, {Errors}, 400)
    end.

handle_event_log_post(Req) ->
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

handle_rotate_internal_creds(Req) ->
    menelaus_util:assert_is_76(),
    case cb_creds_rotation:rotate_password() of
        {error, tmp_error} ->
            ErrStr = <<"System is being reconfigured. Please try later.">>,
            reply_json(Req, [ErrStr], 503);
        _ ->
            ns_audit:password_rotated(Req),
            reply_json(Req, [], 200)
    end.
