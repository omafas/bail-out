%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2019 Couchbase, Inc.
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

%% @doc the main purpose of this process is to send 'on-demand pricing' reports
%% to configured server with configured period

-module(license_reporting).

-behaviour(gen_server).

%% API
-export([start_link/0, build_settings/0, validate_settings/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(s, {
    enabled = false :: boolean(),
    reporting_interval :: pos_integer(),
    report_timer_ref :: undefined | reference()
}).

-define(SERVER, {via, leader_registry, ?MODULE}).
-define(PROTECTIVE_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    misc:start_singleton(gen_server, start_link, [?SERVER, ?MODULE, [], []]).

build_settings() ->
    Settings = ns_config:read_key_fast(license_settings, []),
    misc:update_proplist(defaults(), Settings).

validate_settings(Settings) ->
    Timeout = get_setting(reporting_timeout, Settings) + ?PROTECTIVE_TIMEOUT,
    try
        gen_server:call(?SERVER, {validate_settings, Settings}, Timeout)
    catch
        exit:{timeout, _} -> {error, <<"timeout">>}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ?log_debug("Starting license_reporting server"),
    %% We can't check for enterprise version and return ignore here
    %% because leader_registry_server does not support unregistering
    Self = self(),
    EventHandler =
        fun ({license_settings, _} = E) -> Self ! E;
            (_) -> ok
        end,
    ns_pubsub:subscribe_link(ns_config_events, EventHandler),
    Settings = build_settings(),
    State = #s{reporting_interval = get_setting(reporting_interval,
                                                Settings),
               enabled = get_setting(reporting_enabled, Settings)},
    {ok, restart_timer(State), hibernate}.

handle_call({validate_settings, Settings}, _From, State) ->
    {reply, send_report(Settings, [{validation, true}]), State};

handle_call(Request, _From, State) ->
    ?log_error("Unhandled call: ~p", [Request]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?log_error("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info(report, State) ->
    case send_report(build_settings(), []) of
        {ok, _} -> ok;
        {error, Error} ->
            ?log_error("On-demand pricing report send failed "
                       "with reason: ~s", [Error]),
            system_stats_collector:increment_counter(odp_report_failed)
    end,
    {noreply, restart_timer(State), hibernate};

handle_info({license_settings, _},
            #s{reporting_interval = Interval, enabled = Enabled} = State) ->
    Settings = build_settings(),
    NewInterval = proplists:get_value(reporting_interval, Settings, Interval),
    NewEnabled = proplists:get_value(reporting_enabled, Settings, Enabled),

    NewState = State#s{reporting_interval = NewInterval, enabled = NewEnabled},

    NewState2 =
        case NewState == State of
            true -> NewState;
            false -> restart_timer(NewState)
        end,

    {noreply, NewState2, hibernate};

handle_info(Info, State) ->
    ?log_error("Unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

send_report(Settings, Extra) ->
    Report = create_report(Settings, Extra),
    ?log_info("Generated on-demand pricing report:~n~p", [Report]),
    User = get_setting(contract_id, Settings),
    {password, Pass} = get_setting(customer_token, Settings),
    URL = get_setting(reporting_endpoint, Settings),
    BasicAuth = base64:encode_to_string(User ++ ":" ++ Pass),
    Timeout = get_setting(reporting_timeout, Settings),
    Headers = [{"Content-Type", "application/json"},
               {"Authorization", "Basic " ++ BasicAuth}],
    Body = ejson:encode(Report),
    case proplists:get_bool(generation_only, Settings) of
        false ->
            case post(URL, Headers, Body, Timeout, 10) of
                ok -> {ok, Report};
                {error, Reason} -> {error, Reason}
            end;
        true ->
            {ok, Report}
    end.

post(_URL, _Headers, _Body, _Timeout, 0) ->
    ?log_error("Sending on-demand pricing report failed. Too many redirects"),
    {error, <<"too many redirects">>};
post(URL, Headers, Body, Timeout, RedirectsLeft) ->
    try lhttpc:request(URL, "POST", Headers, Body, Timeout,
                       [{connect_timeout, Timeout}]) of
        {ok, {{Status, _}, _RespHeaders, _RespBody}} when Status == 200;
                                                          Status == 201 ->
            ?log_debug("On-demand pricing report sent successfuly"),
            ok;
        {ok, {{Status, _}, RespHeaders, _}} when Status == 301; Status == 302;
                                                 Status == 307; Status == 308->
            Location = proplists:get_value("Location", RespHeaders),
            ?log_debug("On-demand pricing report redirected to ~p", [Location]),
            post(Location, Headers, Body, Timeout, RedirectsLeft - 1);
        {ok, {{Status, Reason}, _RespHeaders, RespBody}} ->
            ?log_error("Sending on-demand pricing report failed. "
                       "Remote server returned ~p ~p:~n~p",
                       [Status, Reason, RespBody]),
            RespBodyFormatted =
                case RespBody of
                    undefined -> "";
                    Bin when is_binary(Bin) andalso size(Bin) > 100 ->
                        format_bin(" ~100s...", [Bin]);
                    Bin when is_binary(Bin) ->
                        format_bin(" ~s", [Bin])
                end,
            {error, format_bin("server returned ~p ~p~s",
                               [Status, Reason, RespBodyFormatted])};
        {error, Reason} ->
            ?log_error("Sending on-demand pricing report failed. Error: ~p",
                       [Reason]),
            Reason2 =
                case Reason of
                    %% In some cases it returns {Reason, Stack} here, but we
                    %% don't realy need the stack
                    {R, [_|_]} -> R;
                    R -> R
                end,
            {error, format_bin("~p", [Reason2])}
    catch
        _:Error ->
            ?log_error("Sending on-demand pricing report crashed with error: ~p"
                       "~nStacktrace: ~p",
                       [Error, erlang:get_stacktrace()]),
            {error, format_bin("http client crashed with reason ~p", [Error])}
    end.

format_bin(F, A) ->
    iolist_to_binary(io_lib:format(F, A)).

get_setting(Prop, Settings) ->
    proplists:get_value(Prop, Settings).

restart_timer(#s{report_timer_ref = Ref,
                 enabled = Enabled,
                 reporting_interval = Timeout} = State) ->
    Ref =/= undefined andalso erlang:cancel_timer(Ref),
    NewRef =
        case Enabled of
            true -> erlang:send_after(max(Timeout, 10000), self(), report);
            false -> undefined
        end,
    State#s{report_timer_ref = NewRef}.

defaults() ->
    [{reporting_enabled, false},
     {reporting_interval, 3600000}, % 1 hour
     {contract_id, ""},
     {customer_token, {password, ""}},
     {reporting_endpoint, "https://ph.couchbase.net/odp"},
     {reporting_timeout, 5000}].

create_report(Settings, Extra) ->
    Nodes = ns_node_disco:nodes_actual(),
    NodesData =
        lists:map(
          fun (Node) ->
              Props = ns_doctor:get_node(Node),
              SystemStats = proplists:get_value(system_stats, Props, []),
              MemLimit = proplists:get_value(mem_limit, SystemStats),
              Cores = proplists:get_value(cpu_cores_available, SystemStats),
              Hostname = misc:extract_node_address(Node),
              {[{node, Node},
                {hostname, iolist_to_binary(Hostname)},
                {cpu_cores_available, Cores},
                {mem_limit, MemLimit}]}
          end, Nodes),

    {[{timestamp, iolist_to_binary(misc:timestamp_utc_iso8601())},
      {cluster_uuid, ns_config:read_key_fast(uuid, 1)},
      {contract_id, iolist_to_binary(get_setting(contract_id, Settings))},
      {cluster_size, length(Nodes)},
      {nodes, NodesData} | Extra]}.
