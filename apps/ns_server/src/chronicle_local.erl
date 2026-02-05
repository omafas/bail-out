%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(chronicle_local).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include_lib("ns_common/include/cut.hrl").
-include_lib("ale/include/ale.hrl").
-include("cb_cluster_secrets.hrl").
-include("cb_crypto.hrl").

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_info/2,
         prepare_join/1,
         join_cluster/1,
         leave_cluster/0,
         rename/1,
         get_snapshot/1,
         sync/0,
         maybe_apply_new_keys/0,
         get_encryption_dek_ids/0]).

%% exported callbacks used by chronicle
-export([log/4, report_stats/1, encrypt_data/1, decrypt_data/1,
         external_decrypt/1]).

%% used by config_remap
-export([set_chronicle_deks_snapshot/1]).

%% exported for log formatting
-export([format_msg/2, format_time/1]).

-define(CALL_TIMEOUT, 180000).

% External term format always starts with 131, so
% it is important to not use 131 here, otherwise any
% number should work
-define(ENCRYPTION_MAGIC, 45).

-record(state, {last_applied_keys_hash = undefined}).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    application:set_env(chronicle, data_dir,
                        path_config:component_path(data, "config")),
    application:set_env(chronicle, logger_function, {?MODULE, log}),

    case misc:get_env_default(enable_chronicle_stats, true) of
        true ->
            application:set_env(chronicle,
                                stats_function, {?MODULE, report_stats});
        false ->
            ok
    end,

    ok = read_and_set_data_keys(),

    application:set_env(chronicle, encrypt_function, {?MODULE, encrypt_data}),
    application:set_env(chronicle, decrypt_function, {?MODULE, decrypt_data}),

    ?log_debug("Ensure chronicle is started"),
    ok = application:ensure_started(chronicle, permanent),

    ChronicleState = chronicle:get_system_state(),
    ?log_debug("Chronicle state is: ~p", [ChronicleState]),

    case ChronicleState of
        not_provisioned ->
            provision();
        _ ->
            ok
    end,

    case dist_manager:need_fixup() of
        {true, OldNode} ->
            ?log_info("Aborted rename from ~p was detected", [OldNode]),
            handle_rename(OldNode);
        false ->
            ok
    end,

    {ok, #state{last_applied_keys_hash = undefined}}.

handle_call({prepare_join, Info}, _From, State) ->
    ?log_debug("Wiping chronicle before prepare join."),
    ok = chronicle:wipe(),
    case Info of
        undefined ->
            ?log_debug("Joining not chronicle enabled cluster"),
            provision();
        _ ->
            ?log_debug("Prepare join. Info: ~p", [Info]),
            ok = chronicle:prepare_join(Info)
    end,
    {reply, ok, State};
handle_call({join_cluster, Info}, _From, State) ->
    ?log_debug("Joining cluster. Info: ~p", [Info]),
    ok = chronicle:join_cluster(Info),
    {reply, ok, State};
handle_call(leave_cluster, _From, State) ->
    handle_leave(),
    {reply, ok, State};
handle_call({rename, OldNode}, _From, State) ->
    handle_rename(OldNode),
    {reply, ok, State};
handle_call(get_snapshot, _From, Pid) ->
    RV =
        try chronicle_kv:get_full_snapshot(kv) of
            {ok, {Snapshot, _}} ->
                {ok, Snapshot}
        catch T:E:S ->
                ?log_debug("Unable to obtain chronicle snapshot:~n~p",
                           [{T, E, S}]),
                {error, cannot_get_snapshot}
        end,
    {reply, RV, Pid};
handle_call(maybe_apply_new_keys, _From, State) ->
    {Res, NewState} = maybe_apply_new_keys(State),
    {reply, Res, NewState};
handle_call(get_encryption_dek_ids, _From,
            #state{last_applied_keys_hash = KeysHash} = State) ->
    AllIds = cb_crypto:get_all_dek_ids(get_chronicle_deks_snapshot()),
    %% If we haven't rewritten chronicle data with the most recent key yet,
    %% we can still have some data unencrypted, so add 'undefined' just in case
    %% in this case.
    Res = case KeysHash of
              undefined -> lists:uniq([undefined | AllIds]);
              _ -> AllIds
          end,
    {reply, {ok, Res}, State};
handle_call(sync, _From, State) ->
    {reply, ok, State}.

handle_info(Message, State) ->
    ?log_debug("Ignoring unexpected message ~p", [Message]),
    {noreply, State}.

leave_cluster() ->
    gen_server2:call(?MODULE, leave_cluster, ?CALL_TIMEOUT).

prepare_join(Info) ->
    gen_server2:call(?MODULE, {prepare_join, Info}, ?CALL_TIMEOUT).

join_cluster(undefined) ->
    ok;
join_cluster(Info) ->
    gen_server2:call(?MODULE, {join_cluster, Info}, ?CALL_TIMEOUT).

rename(OldNode) ->
    gen_server2:call(?MODULE, {rename, OldNode}).

get_snapshot(Node) ->
    {ok, Snapshot} = gen_server2:call({?MODULE, Node}, get_snapshot,
                                      ?CALL_TIMEOUT),
    Snapshot.

sync() ->
    gen_server2:call(?MODULE, sync, ?CALL_TIMEOUT).

maybe_apply_new_keys() ->
    gen_server2:call(?MODULE, maybe_apply_new_keys, ?CALL_TIMEOUT).

get_encryption_dek_ids() ->
    gen_server2:call(?MODULE, get_encryption_dek_ids, ?CALL_TIMEOUT).

provision() ->
    ?log_debug("Provision chronicle on this node"),
    try
        ok = chronicle:provision([{kv, chronicle_kv, []},
                                  {metakv, chronicle_kv, []}]),
        chronicle_upgrade:maybe_initialize()
    catch
        E:T:S ->
            ?log_error("Provision chronicle failed on this node.~n"
                       "Error - ~p, Type - ~p, Stacktrace - ~p",
                       [E, T, S]),
            %% As a part of chronicle:provision/1, we do the following steps:
            %%
            %% 1. Add an entry to the append log with state = provisioned
            %% 2. Spawn all chronicle related processes such as
            %%    chronicle_leader, chronicle_server and
            %%    chronicle_single_rsm_sup'es (for chronicle_config_rsm and
            %%    chronicle_kv) etc via chronicle_secondary_sup.
            %% 3. Wait for the all the processes to be spawned in the step
            %%    above for 20 secs.
            %%
            %% And eventually seed the default keys in chronicle_kv.
            %%
            %% There is tight coupling between the internal state in
            %% chronicle_config and chronicle_secondary_sup and it's very hard
            %% to atomically perform all the above steps.
            %%
            %% If any of them fail, return to a clean slate and let retry do
            %% it's magic.
            ok = chronicle:wipe(),
            erlang:raise(E, T, S)
    end.

handle_leave() ->
    ?log_debug("Leaving cluster"),
    ok = chronicle:wipe(),
    provision().

handle_rename(OldNode) ->
    NewNode = node(),
    ?log_debug("Handle renaming from ~p to ~p", [OldNode, NewNode]),
    ok = chronicle:reprovision(),

    {ok, _} =
        chronicle_kv:rewrite(
          kv,
          fun (K, V) ->
                  case {misc:rewrite_value(OldNode, NewNode, K),
                        misc:rewrite_value(OldNode, NewNode, V)} of
                      {K, V} ->
                          keep;
                      {NewK, NewV} ->
                          {update, NewK, NewV}
                  end
          end).

log(Level, Fmt, Args, Info) ->
    AleLevel = case Level of
                   warning -> warn;
                   _ -> Level
               end,
    ale:xlog(?CHRONICLE_ALE_LOGGER, AleLevel, Info, Fmt, Args).

format_time(Time) ->
    ale_default_formatter:format_time(Time).

format_msg(#log_info{user_data = #{module := M, function := F, line := L}}
           = Info, UserMsg) ->
    ale_default_formatter:format_msg(
      Info#log_info{module = M, function = F, line = L}, UserMsg).

report_stats({histo, Metric, Max, Unit, Value}) ->
    ns_server_stats:notify_histogram(Metric, Max, Unit, Value);
report_stats({counter, Metric, By}) ->
    ns_server_stats:notify_counter(Metric, By);
report_stats({gauge, Metric, Value}) ->
    ns_server_stats:notify_gauge(Metric, Value);
report_stats({max, Metric, Window, Bucket, Value}) ->
    ns_server_stats:notify_max({Metric, Window, Bucket}, Value).

set_chronicle_deks_snapshot(DeksSnapshot) ->
    %% Note: The context and label are used to derive the key from the current
    %% key. Change of these values may result in backward compatibility issues.
    KDFContext =  #kdf_context{context = "ns_server/chronicle",
                               label = "encryption-at-rest"},
    DerivedSnapshot = cb_crypto:derive_deks_snapshot(DeksSnapshot, KDFContext),
    %% Pre-totoro nodes use DEKs directly, while totoro nodes use derived DEKs.
    ok = persistent_term:put(chronicle_deks_snapshot,
                             #{legacy => DeksSnapshot,
                               current => DerivedSnapshot}).

get_chronicle_deks_snapshot() ->
    get_chronicle_deks_snapshot(current).

get_legacy_chronicle_deks_snapshot() ->
    get_chronicle_deks_snapshot(legacy).

get_chronicle_deks_snapshot(Key) when Key =:= current orelse Key =:= legacy ->
    M = persistent_term:get(chronicle_deks_snapshot, #{}),
    maps:get(Key, M, undefined).

%% We assume that data is in erlang external term format, this is important
%% because that's how we determine if it is encrypted or not
encrypt_data(<<131, _/binary>> = Data) ->
    DeksSnapshot = get_chronicle_deks_snapshot(),
    case DeksSnapshot of
        undefined -> erlang:error(no_keys);
        _ -> ok
    end,
    case cb_crypto:encrypt(Data, <<>>, DeksSnapshot) of
        {ok, EncryptedData} ->
            Version = 1,
            <<?ENCRYPTION_MAGIC, Version, EncryptedData/binary>>;
        {error, no_active_key} ->
            Data %% Encryption is disabled
    end.

decrypt_data(<<131, _/binary>> = D) -> {ok, D};
%% Backward compatibility with pre-totoro data
decrypt_data(<<?ENCRYPTION_MAGIC, 0, Data/binary>>) ->
    case get_legacy_chronicle_deks_snapshot() of
        undefined -> {error, no_keys};
        DeksSnapshot -> cb_crypto:decrypt(Data, <<>>, DeksSnapshot)
    end;
decrypt_data(<<?ENCRYPTION_MAGIC, 1, Data/binary>>) ->
    case get_chronicle_deks_snapshot() of
        undefined -> {error, no_keys};
        DeksSnapshot -> cb_crypto:decrypt(Data, <<>>, DeksSnapshot)
    end.

%% This functions is supposed to be called from chronicle_dump only
external_decrypt(Data) ->
    case decrypt_data(Data) of
        {ok, Decrypted} ->
            {ok, Decrypted};
        {error, no_keys} ->
            maybe
                ok ?= external_setup_keys(),
                decrypt_data(Data)
            end;
        {error, E} ->
            {error, E}
    end.

%% This functions is supposed to be called from chronicle_dump only
external_setup_keys() ->
    %% In order to make path_config work
    application:load(ns_server),
    Opts = case os:getenv("CB_CONFIG_PATH") of
               false ->
                   #{};
               Path ->
                   #{config_path_override => Path}
           end,
    case cb_deks_raw_utils:bootstrap_get_deks(configDek, Opts) of
        {ok, DS} ->
            set_chronicle_deks_snapshot(DS);
        Error ->
            Error
    end.

read_and_set_data_keys() ->
    maybe
        {ok, DeksSnapshot} ?= cb_crypto:fetch_deks_snapshot(configDek),
        ok ?= cb_crypto:all_keys_ok(DeksSnapshot),
        set_chronicle_deks_snapshot(DeksSnapshot)
    else
        {error, Reason} ->
            ?log_error("Failed to get encryption keys for chronicle: ~p",
                       [Reason]),
            {error, Reason}
    end.

rewrite_chronicle_data() ->
    try
        maybe
            %% The purpose of this function is to force chronicle to rewrite
            %% all files that contain sensitive data on disk.
            %% By doing so we can guarantee that all the chronicle data on disk
            %% is encrypted by the actual encryption key.
            %% The idea is to force snapshot creation two times. Since chronicle
            %% currently keeps last two logs on disk, creation of two snapshots
            %% should rewrite both of them.
            %% Modification of chronicle_key_snapshot_enforcer is needed just to
            %% make sure snapshot has changed since the last snapshot. Otherwise
            %% chronicle:force_snapshot() will do nothing.
            Timeout = get_snapshot_enforcer_timeout(),
            {ok, _} ?= chronicle_kv:set(kv, chronicle_key_snapshot_enforcer,
                                        crypto:strong_rand_bytes(8),
                                        any,
                                        #{timeout => Timeout}),
            {ok, _} ?= chronicle:force_snapshot(),
            {ok, _} ?= chronicle_kv:set(kv, chronicle_key_snapshot_enforcer,
                                        crypto:strong_rand_bytes(8),
                                        any,
                                        #{timeout => Timeout}),
            {ok, _} ?= chronicle:force_snapshot(),
            ok
        else
            {error, Reason} ->
                ?log_error("Failed to rewrite chronicle data: ~p", [Reason]),
                {error, Reason}
        end
    catch
        exit:timeout ->
            ?log_error("Failed to rewrite chronicle data: timeout"),
            {error, timeout}
    end.

get_snapshot_enforcer_timeout() ->
    Default = 1000,
    try
        ?get_timeout(snapshot_enforcer_timeout, Default)
    catch
        _:_ ->
            %% Don't want to introduce a strong dependency on ns_config here
            %% (ns_config starts after chronicle_local, so there is a chance
            %% that ns_config is not started yet). At the same time, if
            %% ns_config is not available, it is not a big deal, we can simply
            %% use a default value here. Key enforcement will retry in case
            %% of timeout anyway, and if ns_config gets available later, the
            %% new timeout value will be used next time.
            Default
    end.

maybe_apply_new_keys(State = #state{last_applied_keys_hash = Hash}) ->
    maybe
        {ok, New} ?= cb_crypto:fetch_deks_snapshot(configDek),
        ok ?= cb_crypto:all_keys_ok(New),
        NewWithoutHistDeks = cb_crypto:without_historical_deks(New),
        NewHash = cb_crypto:get_deks_snapshot_hash(NewWithoutHistDeks),
        % If what we want to apply is different from what we have applied before,
        % we need to rewrite the chronicle data and apply new keys.
        case (Hash /= NewHash) of
            true ->
                set_chronicle_deks_snapshot(New),
                case rewrite_chronicle_data() of
                    ok ->
                        set_chronicle_deks_snapshot(NewWithoutHistDeks),
                        {ok, State#state{last_applied_keys_hash = NewHash}};
                    {error, Reason} ->
                        {{error, Reason}, State}
                end;
            false ->
                {ok, State}
        end
    end.
