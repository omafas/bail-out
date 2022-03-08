%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
