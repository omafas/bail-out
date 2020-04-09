%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2018 Couchbase, Inc.
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

%% This module provides a wrapper around global and
%% leader_registry_server. Normally, leader_registry_server is called. But if
%% the end user chooses to disable new orchestration, the module will revert
%% to using global.
-module(leader_registry).

-include("ns_common.hrl").

%% name service API
-export([register_name/2, unregister_name/1, whereis_name/1, send/2]).

register_name(Name, Pid) ->
    wrap_registry_api(register_name, [Name, Pid]).

unregister_name(Name) ->
    wrap_registry_api(unregister_name, [Name]).

whereis_name(Name) ->
    wrap_registry_api(whereis_name, [Name]).

send(Name, Msg) ->
    wrap_registry_api(send, [Name, Msg]).

%% internal
backend() ->
    case leader_utils:is_new_orchestration_disabled() of
        true ->
            global;
        false ->
            leader_registry_server
    end.

wrap_registry_api(Name, Args) ->
    erlang:apply(backend(), Name, Args).
