%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-type loglevel() :: debug | info | warn | error | critical.
-type sink_type() :: raw | preformatted.

-define(LOGLEVELS, [debug, info, warn, error, critical]).

-define(DEFAULT_LOGLEVEL, warn).
-define(DEFAULT_FORMATTER, ale_default_formatter).
-define(DEFAULT_SINK_TYPE, preformatted).

-define(ALE_LOGGER, ale_logger).
-define(ERROR_LOGGER, error_logger).

-type time() :: {integer(), integer(), integer()}.

-record(log_info,
        { logger          :: atom(),
          loglevel        :: loglevel(),
          module          :: atom(),
          function        :: atom(),
          line            :: integer(),
          time            :: time(),
          pid             :: pid(),
          registered_name :: atom(),
          node            :: node(),
          user_data       :: any() }).
