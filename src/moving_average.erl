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
%% Exponential moving average over a time window. Details here:
%% http://en.wikipedia.org/wiki/Moving_average#Application_to_measuring_computer_performance
%%
-module(moving_average).

-export([new/1, feed/3, is_empty/1, get_estimate/1]).
-export_type([moving_average/0]).

-type ts() :: non_neg_integer().
-type ts_delta() :: integer().

-record(moving_average, { estimate  :: undefined | float(),
                          timestamp :: undefined | ts(),
                          window    :: ts_delta() }).

-type moving_average() :: #moving_average{}.

new(Window) ->
    true = (Window > 0),
    #moving_average{window = Window}.

feed(Measurement, Ts, #moving_average{timestamp = OldTs} = Avg)
  when OldTs =:= undefined;
       OldTs > Ts ->

    %% Either first measurement or we traveled back in time. So just use
    %% current measurement.
    Avg#moving_average{timestamp = Ts,
                       estimate  = Measurement};
feed(Measurement, Ts, Avg) ->
    misc:update_field(#moving_average.estimate, Avg,
                      fun (Estimate) ->
                              Estimate + gain(Ts, Avg) * (Measurement - Estimate)
                      end).

is_empty(#moving_average{estimate = Estimate}) ->
    Estimate =:= undefined.

get_estimate(#moving_average{estimate = undefined}) ->
    empty;
get_estimate(#moving_average{estimate = Estimate}) ->
    Estimate.

%% internal
gain(Ts, #moving_average{window = Window, timestamp = OldTs}) ->
    1 - math:exp((OldTs - Ts) / Window).
