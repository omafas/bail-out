%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Log information about the OS
%%
-module (log_os_info).

-include("ns_common.hrl").

-export([start_link/0]).

start_link() ->
    ?log_info("OS type: ~p Version: ~p~nRuntime info: ~p",
              [os:type(), os:version(), ns_info:runtime()]),
    ?log_info("Manifest:~n~p~n", [diag_handler:manifest()]),
    ignore.
