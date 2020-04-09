%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2012 Couchbase, Inc.
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
