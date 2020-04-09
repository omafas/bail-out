%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
-module(service_eventing).

-include("ns_common.hrl").

-export([get_functions/0,
         start_keeper/0,
         get_type/0,
         get_local_status/0,
         get_remote_items/1,
         process_status/1,
         grab_stats/0,
         get_gauges/0,
         get_counters/0,
         get_computed/0,
         get_service_gauges/0,
         get_service_counters/0,
         compute_gauges/1,
         compute_service_gauges/1,
         compute_version/2,
         split_stat_name/1,
         is_started/0]).

get_functions() ->
    {ok, Functions, _, _} = service_status_keeper:get_items(?MODULE),
    Functions.

start_keeper() ->
    service_status_keeper:start_link(?MODULE).

get_type() ->
    eventing.

get_port() ->
    service_ports:get_port(eventing_http_port).

get_local_status() ->
    Timeout = ?get_timeout(status, 30000),
    rest_utils:get_json_local(eventing, "api/v1/functions",
                              get_port(), Timeout).

get_remote_items(Node) ->
    remote_api:get_service_remote_items(Node, ?MODULE).

process_status(Status) ->
    {ok, lists:filtermap(
           fun ({Function}) ->
                   {_, Name} = lists:keyfind(<<"appname">>, 1, Function),
                   {_, {Settings}} = lists:keyfind(<<"settings">>, 1, Function),
                   case lists:keyfind(<<"processing_status">>, 1, Settings) of
                       {_, true} ->
                           {true, Name};
                       {_, false} ->
                           false
                   end
           end, Status)}.

interesting_sections() ->
    [<<"events_remaining">>,
     <<"execution_stats">>,
     <<"failure_stats">>].

flatten_stats(Json) ->
    lists:flatmap(
      fun ({Function}) ->
              {_, Name} = lists:keyfind(<<"function_name">>, 1, Function),
              lists:flatmap(
                fun (Key) ->
                        {SectionStats} =
                            proplists:get_value(Key, Function, {[]}),
                        [{[Name, Stat], Val} || {Stat, Val} <- SectionStats]
                end, interesting_sections())
      end, Json).

is_started() ->
    misc:is_local_port_open(get_port(), 1000).

grab_stats() ->
    Timeout = ?get_timeout(stats, 30000),
    case rest_utils:get_json_local(eventing, "api/v1/stats",
                                   get_port(), Timeout) of
        {ok, Json} ->
            {ok, {flatten_stats(Json)}};
        Error ->
            Error
    end.

get_gauges() ->
    [].

get_counters() ->
    [].

get_computed() ->
    [processed_count, failed_count].

successes() ->
    [on_update_success,
     on_delete_success].

failures() ->
    [bucket_op_exception_count,
     checkpoint_failure_count,
     n1ql_op_exception_count,
     timeout_count,
     doc_timer_create_failure,
     non_doc_timer_create_failure,
     on_update_failure,
     on_delete_failure].

get_service_gauges() ->
    [dcp_backlog | successes() ++ failures()].

get_service_counters() ->
    [].

compute_service_gauges(Stats) ->
    Successes = [list_to_binary(atom_to_list(S)) || S <- successes()],
    Failures = [list_to_binary(atom_to_list(F)) || F <- failures()],

    dict:to_list(
      lists:foldl(
        fun ({{Function, Metric}, Value}, D) ->
                case lists:member(Metric, Successes) of
                    true ->
                        dict:update({Function, <<"processed_count">>},
                                    fun (V) ->
                                            V + Value
                                    end, Value, D);
                    false ->
                        case lists:member(Metric, Failures) of
                            true ->
                                dict:update({Function, <<"failed_count">>},
                                            fun (V) ->
                                                    V + Value
                                            end, Value, D);
                            false ->
                                D
                        end
                end
        end, dict:new(), Stats)).

compute_gauges(_Gauges) ->
    [].

compute_version(Items, IsStale) ->
    erlang:phash2({Items, IsStale}).

split_stat_name(X) ->
    X.
