%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2015 Couchbase, Inc.
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
-include("ns_common.hrl").

%% The range used within this file is arbitrary and undefined, so I'm
%% defining an arbitrary value here just to be rebellious.
-define(BUCKET_DELETED, 11).
-define(BUCKET_CREATED, 12).
-define(START_FAIL, 100).
-define(NODE_EJECTED, 101).
-define(UI_SIDE_ERROR_REPORT, 102).

-define(MENELAUS_WEB_LOG(Code, Msg, Args),
        ale:xlog(?MENELAUS_LOGGER,
                 ns_log_sink:get_loglevel(menelaus_web, Code),
                 {menelaus_web, Code}, Msg, Args)).

-define(MENELAUS_WEB_LOG(Code, Msg), ?MENELAUS_WEB_LOG(Code, Msg, [])).
