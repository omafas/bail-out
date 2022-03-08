%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-type log_classification() :: 'warn' | 'info' | 'crit'.

-record(log_entry, {
          tstamp      :: erlang:timestamp(),
          node        :: atom(),
          module      :: atom(),
          code        :: undefined | integer(),
          msg         :: iolist(),
          args        :: list(),
          cat         :: log_classification(),
          server_time :: undefined | calendar:datetime1970()
         }).
