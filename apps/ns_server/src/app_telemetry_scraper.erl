%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(app_telemetry_scraper).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-behaviour(gen_server).

%% API
-export([start_link/0, handle_connect/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(CONFIG_KEY, app_telemetry).

-define(FETCH_TIMEOUT, ?get_param(fetch_timeout, 1000)).

%% Statuses
-define(SUCCESS, 0).

%% Commands
-define(GET_TELEMETRY, 0).

-define(METRIC_LABELS, [<<"le">>, <<"alt_node">>]).

-record(state, {timer_ref :: undefined | reference(),
                fetch_pid :: undefined | pid(),
                enabled :: boolean(),
                scrape_interval_seconds :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

handle_connect(Req) ->
    case menelaus_web_app_telemetry:is_accepting_connections() of
        true ->
            app_telemetry_pool:handle_connect(Req);
        false ->
            menelaus_util:reply_not_found(Req)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/1),

    Config = menelaus_web_app_telemetry:get_config(),
    Enabled = menelaus_web_app_telemetry:is_enabled(Config),
    MaxClients = menelaus_web_app_telemetry:get_max_clients_per_node(Config),
    ScrapeInterval = menelaus_web_app_telemetry:get_scrape_interval(Config),
    app_telemetry_pool:start_link(#{max => MaxClients}),
    State = #state{enabled = Enabled,
                   scrape_interval_seconds = ScrapeInterval},
    {ok, restart_timer(State)}.

handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(fetch, #state{enabled = Enabled, fetch_pid = FetchPid} = State0) ->
    %% Start the timer again so that we'll flush the metrics in 60s
    State1 = restart_timer(State0),
    case Enabled of
        true ->
            %% Only start a new fetch if one isn't already in progress
            NewFetchPid =
                case FetchPid of
                    undefined -> start_fetch();
                    _ -> FetchPid
                end,
            {noreply, State1#state{fetch_pid = NewFetchPid}};
        false ->
            {noreply, State1}
    end;
handle_info(fetch_done, State) ->
    {noreply, State#state{fetch_pid = undefined}};
handle_info(config_change, State) ->
    {noreply, update_config(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_config_event({app_telemetry, _}) ->
    ?SERVER ! config_change;
handle_config_event(_) ->
    ok.

update_config(State0) ->
    Config = menelaus_web_app_telemetry:get_config(),

    Enabled = menelaus_web_app_telemetry:is_enabled(Config),
    case Enabled of
        false ->
            %% Disconnect existing clients by setting the max to 0
            app_telemetry_pool:update_max(0);
        true ->
            %% Update the max clients
            Max = menelaus_web_app_telemetry:get_max_clients_per_node(Config),
            app_telemetry_pool:update_max(Max)
    end,

    %% Update the scraper state
    ScrapeInterval = menelaus_web_app_telemetry:get_scrape_interval(Config),
    State1 = State0#state{enabled = Enabled,
                          scrape_interval_seconds = ScrapeInterval},

    %% Ensure that any change in the scrape interval immediately takes effect
    restart_timer(State1).


%% We need to make sure there is only one timer at any given moment, otherwise
%% the system would be fragile to future changes or diag/evals
restart_timer(#state{timer_ref = Ref} = State) when is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    restart_timer(State#state{timer_ref = undefined});
restart_timer(#state{timer_ref = undefined,
                     scrape_interval_seconds = ScrapeInterval} = State) ->
    State#state{timer_ref = erlang:send_after(ScrapeInterval * 1000, self(),
                                              fetch)}.

start_fetch() ->
    Parent = self(),
    spawn_link(
      fun () ->
              Pids = app_telemetry_pool:get_pids(),
              lists:foreach(fun get_telemetry/1, Pids),
              %% Flush all the metrics. We do this at a regular interval anyway,
              %% but we might as well do it now since we have scraped from all
              %% connected clients
              app_telemetry_aggregator:flush_remote_metrics(),
              Parent ! fetch_done
      end).

get_telemetry(Pid) ->
    Cmd = <<?GET_TELEMETRY:8>>,
    maybe
        {ok, Data} ?= app_telemetry_pool:call(Pid, Cmd, ?FETCH_TIMEOUT),
        ok ?= handle_data(Data)
    else
        {error, dropped} ->
            ?log_debug("Couldn't fetch telemetry from ~p (dropped)", [Pid]);
        {error, Error} ->
            ?log_warning("Couldn't fetch telemetry from ~p (~p)", [Pid, Error]),
            app_telemetry_pool:drop(Pid)
    end.

handle_data({binary, <<?SUCCESS:8, Data/binary>>}) ->
    Updates = parse_metric_updates(Data),
    lists:foreach(
      fun ({Node, Metric, Value}) when Node =:= node() ->
              ns_server_stats:notify_counter_raw(Metric, Value);
          ({Node, Metric, Value}) ->
              app_telemetry_aggregator:update_remote_cache(Node, Metric, Value);
          ({error, E}) ->
              ?log_warning("Failed to parse metric line. Error: ~p", [E]),
              ns_server_stats:notify_counter_raw(<<"sdk_invalid_metric_total">>)
      end, Updates);
handle_data({binary, <<Status:8, _Rest/binary>>}) ->
    {error, Status};
handle_data(_Other) ->
    {error, not_binary}.

parse_metric_updates(Data) ->
    Lines = string:split(Data, "\n", all),
    NodeUUIDMap = get_node_uuid_map(ns_node_disco:nodes_wanted(),
                                    ns_config:latest()),
    {ok, NameRe} = re:compile("^[a-zA-Z_][a-zA-Z_0-9]*$"),
    lists:filtermap(
      fun (Line) ->
              case string:trim(Line) of
                  <<>> ->
                      false;
                  <<"#", _Line/binary>> ->
                      false;
                  LineTrimmed ->
                      maybe
                          {Name, Labels, Value} ?= parse_metric_line(
                                                     LineTrimmed, NameRe),
                          {ok, Node} ?= get_node(Labels, NodeUUIDMap),
                          FilteredLabels = filter_labels(Labels),
                          {true, {Node, {Name, FilteredLabels}, Value}}
                      else
                          {error, _} = E ->
                              {true, E}
                      end
              end
      end, Lines).

parse_metric_line(Line, NameRe) ->
    maybe
        {ok, {RawName, RawLabels, RawValue}} ?= decompose_line(Line),
        {ok, Name} ?= validate(RawName, NameRe),
        {ok, Labels} ?= parse_labels(RawLabels, NameRe),
        {ok, Value} ?= parse_value(RawValue),
        {Name, Labels, Value}
    else
        {error, _} = E -> E
    end.

decompose_line(RawLine) ->
    case string:split(RawLine, "{") of
        [M, R0] ->
            case string:split(R0, "}") of
                [L, R1] ->
                    case string:lexemes(R1, " ") of
                        [R2] -> {ok, {M, L, R2}};
                        [R2, _T] -> {ok, {M, L, R2}};
                        [_, _T, Extra] -> {error, {unexpected, Extra}}
                    end;
                [R1] ->
                    {error, {missing_right_brace, R1}}
            end;
        [RawLine] ->
            case string:lexemes(RawLine, " ") of
                [M, R1] -> {ok, {M, <<"">>, R1}};
                [M, R1, _T] -> {ok, {M, <<"">>, R1}};
                [_M, _, _T, Extra] -> {error, {unexpected, Extra}};
                [_] -> {error, missing_value}
            end
    end.

parse_labels(RawLabels, NameRe) ->
    Pairs = lists:foldl(
              fun (<<>>, Acc) ->
                      Acc;
                  (Label, Acc) when is_list(Acc) ->
                      maybe
                          [RawName, RawValue] ?= split_by_substr(Label, "="),
                          {ok, Name} ?= validate(RawName, NameRe),
                          {ok, Value} ?= validate_label_value(RawValue),
                          [{Name, Value} | Acc]
                      else
                          {error, _} = E -> E
                      end;
                  (_, Acc) ->
                      Acc
              end, [], string:split(RawLabels, ",", all)),
    case Pairs of
        {error, _} = E ->
            E;
        _ ->
            {ok, Pairs}
    end.

split_by_substr(Str, Substr) ->
    split_by_substr(Str, Substr, leading).

split_by_substr(Str, Substr, Direction) ->
    case string:split(Str, Substr, Direction) of
        [_, _] = Substrs -> Substrs;
        _ -> {error, {split_by_substr, [{substr, Substr}, {str, Str}]}}
    end.

validate_label_value(RawValue) ->
    case string:trim(RawValue) of
        <<"\"", Value/binary>> ->
            case string:split(Value, "\"", trailing) of
                [Trimmed, <<>>] ->
                    Unescaped = re:replace(Trimmed, <<"\\\\\"">>, <<"\"">>,
                                           [global, {return, binary}]),
                    {ok, Unescaped};
                [_] -> {error, {missing_right_quote, RawValue}};
                [_, Rest] -> {error, {unexpected_after_quote, Rest}}
            end;
        Value ->
            {error, {missing_left_quote, Value}}
    end.

validate(RawText, Re) ->
    Text = string:trim(RawText),
    case re:run(Text, Re) of
        nomatch ->
            {error, {bad_format, Text}};
        _ ->
            {ok, Text}
    end.

parse_value(RawValue) ->
    try promQL:parse_value(RawValue) of
        I when is_integer(I) -> {ok, I};
        F when is_float(F) -> {ok, round(F)};
        _ -> {error, {bad_value, RawValue}}
    catch _:_ -> {error, {bad_value, RawValue}}
    end.

get_node_uuid_map(Nodes, Config) ->
    #{ns_config:search_node_with_default(Node, Config, uuid, undefined) => Node
        || Node <- Nodes}.

get_node(Labels, NodeUUIDMap) ->
    case proplists:get_value(<<"node_uuid">>, Labels) of
        undefined ->
            {error, missing_node_uuid};
        NodeUUID ->
            case maps:find(NodeUUID, NodeUUIDMap) of
                {ok, _} = Res -> Res;
                error -> {error, node_not_found}
            end
    end.

filter_labels(Labels) ->
    lists:filter(
      fun ({Label, _Value}) ->
              lists:member(Label, ?METRIC_LABELS)
      end, Labels).

-ifdef(TEST).

setup(Node, UUID) ->
    meck:expect(ns_node_disco, nodes_wanted,
                fun () -> [Node] end),
    meck:expect(ns_config, search_node_with_default,
                fun (N, _, uuid, _) when N =:= Node -> UUID;
                    (_, _, uuid, Default) -> Default
                end).

teardown(_) ->
    meck:unload().

parse_metric_updates_test__(Node, UUID) ->
    ?assertEqual([], parse_metric_updates(<<>>)),
    ?assertEqual([], parse_metric_updates(<<" ">>)),
    ?assertEqual([], parse_metric_updates(<<"#">>)),
    ?assertEqual([], parse_metric_updates(<<"#test">>)),
    ?assertEqual([], parse_metric_updates(<<" #">>)),
    ?assertEqual([{error, missing_node_uuid}],
                 parse_metric_updates(<<"sdk_test 1">>)),
    ?assertEqual([{error, missing_node_uuid}],
                 parse_metric_updates(<<"sdk_test{} 1">>)),
    ?assertEqual([{error, {bad_format, <<"sdk_tes:t">>}}],
                 parse_metric_updates(<<"sdk_tes:t 1">>)),
    ?assertEqual([{error, {bad_format, <<"5dk_test">>}}],
                 parse_metric_updates(<<"5dk_test 1">>)),
    ?assertEqual([{error, {bad_value, <<"x">>}}],
                 parse_metric_updates(<<"sdk_test x">>)),
    ?assertEqual([{error, {unexpected, <<"1">>}}],
                 parse_metric_updates(<<"sdk_test 1 1 1">>)),
    ?assertEqual([{error, {unexpected, <<"1">>}}],
                 parse_metric_updates(<<"sdk_test{} 1 1 1">>)),
    ?assertEqual([{error, {missing_right_brace, <<"a 1">>}}],
                 parse_metric_updates(<<"sdk_test{a 1">>)),
    ?assertEqual([{error, {missing_left_quote, <<"y">>}}],
                 parse_metric_updates(<<"sdk_test{label=y} 1">>)),
    ?assertEqual([{error, {missing_right_quote, <<"\"z">>}}],
                 parse_metric_updates(<<"sdk_test{label=\"z} 1">>)),
    ?assertEqual([{error, node_not_found}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"wrong uuid\"} "
                     "1">>)),
    ?assertEqual([{Node, {<<"sdk_test">>, []}, 1}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"", UUID/binary, "\"} "
                     "1">>)),
    %% Whitespace is ignored
    ?assertEqual([{Node, {<<"sdk_test">>, []}, 1}],
                 parse_metric_updates(
                   <<" sdk_test {"
                     " node_uuid = \"", UUID/binary, "\" } "
                     "1">>)),
    ?assertEqual([{Node, {<<"sdk_test">>, []}, 100}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"", UUID/binary, "\"} "
                     "1e2">>)),
    %% Floats are currently rounded
    ?assertEqual([{Node, {<<"sdk_test">>, []}, 1}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"", UUID/binary, "\"} "
                     "1.2">>)),
    ?assertEqual([{Node, {<<"sdk_test">>, []}, 1}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"", UUID/binary, "\"} "
                     "1 100000">>)),
    ?assertEqual([{Node, {<<"Sdk_t3st">>, []}, 1}],
                 parse_metric_updates(
                   <<"Sdk_t3st{"
                     "node_uuid=\"", UUID/binary, "\"} "
                     "1">>)),
    ?assertEqual([{Node, {<<"sdk_test">>, [{<<"le">>, <<"0.1">>}]}, 1}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"", UUID/binary, "\","
                     "le=\"0.1\","
                     "label=\"value\"} 1">>)),
    ?assertEqual([{Node, {<<"sdk_test">>,
                          [{<<"le">>, <<"escaped \"quotes\"">>}]}, 1}],
                 parse_metric_updates(
                   <<"sdk_test{"
                     "node_uuid=\"", UUID/binary, "\","
                     "le=\"escaped \\\"quotes\\\"\"} 1">>)).

parse_test_() ->
    Node = node(),
    UUID = <<"b5d0491cf67c56dbe7a4f15d71a149b5">>,
    {setup,
     ?cut(setup(Node, UUID)),
     fun teardown/1,
     ?cut(parse_metric_updates_test__(Node, UUID))}.

-endif.
