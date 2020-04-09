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
%%
-module(backoff).

-export_type([backoff/0]).

-export([new/1]).
-export([get_timeout/1, reset/1, next/1]).

-include("cut.hrl").

-record(backoff, { initial    :: pos_integer(),
                   threshold  :: non_neg_integer(),
                   multiplier :: float(),

                   current_timeout :: pos_integer() }).

-type backoff() :: #backoff{}.

new(Props) ->
    {initial, Initial}     = lists:keyfind(initial, 1, Props),
    {threshold, Threshold} = lists:keyfind(threshold, 1, Props),
    Multiplier             = proplists:get_value(multiplier, Props, 2),

    true = (Initial > 0),
    true = (Threshold > Initial),
    true = (Multiplier > 1),

    #backoff{initial    = Initial,
             threshold  = Threshold,
             multiplier = Multiplier,

             current_timeout = Initial}.

reset(#backoff{initial = Initial} = Backoff) ->
    Backoff#backoff{current_timeout = Initial}.

next(#backoff{multiplier      = Multiplier,
              threshold       = Threshold,
              current_timeout = Timeout} = Backoff) ->

    NewTimeout0 = trunc(Timeout * Multiplier),
    NewTimeout  = min(Threshold, NewTimeout0),

    Backoff#backoff{current_timeout = NewTimeout}.

get_timeout(#backoff{current_timeout = Timeout}) ->
    Timeout.
