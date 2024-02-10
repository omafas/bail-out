%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_mail).

-export([send_async/3, send/3, send/4, send_alert_async/4]).

-include("ns_common.hrl").

-define(SEND_TIMEOUT, 15000).

%% API

send_async(Subject, Body, Config) ->
    do_send_async(Subject, Body, Config, fun (_) -> ok end),
    ok.

send(Subject, Body, Config) ->
    send(Subject, Body, Config, ?SEND_TIMEOUT).

send(Subject, Body, Config, Timeout) ->
    Caller = self(),
    Ref = make_ref(),

    Pid = do_send_async(Subject, Body, Config,
                        fun (Reply) ->
                                Caller ! {Ref, Reply}
                        end),

    await_response(Ref, Pid, Timeout).

send_alert_async(AlertKey, Subject0, Message, Config) when is_atom(AlertKey) ->
    EnabledAlerts = proplists:get_value(alerts, Config, []),
    case lists:member(AlertKey, EnabledAlerts) of
        true ->
            Subject =
                lists:flatten(
                  io_lib:format("Couchbase Server alert: ~s", [Subject0])),
            send_async(Subject, Message, Config);
        false ->
            ok
    end.

%% Internal functions

do_send_async(Subject, Body, Config, Callback) ->
    Sender = proplists:get_value(sender, Config),
    Recipients = proplists:get_value(recipients, Config),
    ServerConfig = proplists:get_value(email_server, Config),
    Options = config_to_options(ServerConfig),
    Message0 = mimemail:encode({<<"text">>, <<"plain">>,
                                make_headers(Sender, Recipients, Subject), [],
                                couch_util:to_binary(Body)}),
    Message = binary_to_list(Message0),

    {ok, Pid} =
        gen_smtp_client:send(
          {Sender, Recipients, Message}, Options,
          fun (Reply0) ->
                  Reply = case Reply0 of
                              {ok, _} ->
                                  ok;
                              {error, _, Reason} ->
                                  {error, Reason};
                              {exit, Reason} ->
                                  {error, Reason}
                          end,

                  case Reply of
                      {error, _} ->
                          ale:warn(?USER_LOGGER,
                                   "Could not send email: ~p. "
                                   "Make sure that your email settings are "
                                   "correct.", [Reply0]);
                      _ ->
                          ?log_debug("An email with the following subject has "
                                     "been sent to the configured "
                                     "recipients:~n~s~n", [Subject]),
                          ns_audit:alert_email_sent(Sender, Recipients,
                                                    Subject, Body),
                          ok
                  end,

                  Callback(Reply)
          end),
    Pid.

await_response(Ref, Pid, Timeout) ->
    receive
        {Ref, Reply} ->
            Reply
    after Timeout ->
            %% gen_smtp_client:send/3 does not link spawned process to anyone;
            %% hence there's no need receive {'EXIT', Pid, _} messages here
            exit(Pid, kill),
            receive
                {Ref, Reply} ->
                    Reply
            after 0 ->
                    ale:warn(?USER_LOGGER,
                             "Could not send email: timeout exceeded. "
                             "Make sure that your email settings are correct."),
                    {error, timeout}
            end
    end.

format_addr(Rcpts) ->
    string:join(["<" ++ Addr ++ ">" || Addr <- Rcpts], ", ").

make_headers(Sender, Rcpts, Subject) ->
    [{<<"From">>, couch_util:to_binary(format_addr([Sender]))},
     {<<"To">>, couch_util:to_binary(format_addr(Rcpts))},
     {<<"Subject">>, couch_util:to_binary(Subject)}].

config_to_options(ServerConfig) ->
    Username = proplists:get_value(user, ServerConfig),
    Password = proplists:get_value(pass, ServerConfig),
    Relay = proplists:get_value(host, ServerConfig),
    Port = proplists:get_value(port, ServerConfig),
    Encrypt = proplists:get_bool(encrypt, ServerConfig),
    Options = [{relay, Relay}, {port, Port}],
    Options2 = case Username of
        "" ->
            Options;
        _ ->
            [{username, Username}, {password, Password}] ++ Options
    end,
    case Encrypt of
        true -> [{tls, always} | Options2];
        false -> Options2
    end.
