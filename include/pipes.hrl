%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2017 Couchbase, Inc.
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
-define(make_producer(Body),
        fun (__Yield) ->
                Body
        end).
-define(make_transducer(Body),
        fun (__Producer) ->
                fun (__Yield) ->
                        Body
                end
        end).
-define(make_consumer(Body),
        fun (__Producer) ->
                Body
        end).

-define(producer(), __Producer).
-define(yield(), __Yield).
-define(yield(R), __Yield(R)).
