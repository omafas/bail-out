%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_email_alert).

%% API
-export([start_link/0,
         alert/3]).

-include("ns_common.hrl").

start_link() ->
    work_queue:start_link(?MODULE).

alert(AlertKey, Fmt, Args) ->
    work_queue:submit_work(?MODULE,
                           fun() ->
                                   handle_alert({alert, AlertKey, Fmt, Args})
                           end).

handle_alert({alert, AlertKey, Fmt, Args}) ->
    Config = menelaus_alert:get_config(),
    case proplists:get_bool(enabled, Config) of
        true ->
            Message = lists:flatten(io_lib:format(Fmt, Args)),
            ns_mail:send_alert_async(AlertKey, AlertKey, Message, Config);
        false ->
            ok
    end.
