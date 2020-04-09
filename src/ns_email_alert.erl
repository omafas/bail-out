%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
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
    {value, Config} = ns_config:search(email_alerts),
    case proplists:get_bool(enabled, Config) of
        true ->
            Message = lists:flatten(io_lib:format(Fmt, Args)),
            ns_mail:send_alert_async(AlertKey, AlertKey, Message, Config);
        false ->
            ok
    end.
