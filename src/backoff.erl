%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
