-module(encryption_service).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0,
         decrypt/1,
         encrypt/1,
         change_password/1,
         get_keys_ref/0,
         rotate_data_key/0,
         maybe_clear_backup_key/1,
         get_state/0,
         os_pid/0,
         reconfigure/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(RUNNER, {cb_gosecrets_runner, ns_server:get_babysitter_node()}).
-define(RESTART_WAIT_TIMEOUT, 120000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

encrypt(Data) ->
    cb_gosecrets_runner:encrypt(?RUNNER, Data).

decrypt(Data) ->
    cb_gosecrets_runner:decrypt(?RUNNER, Data).

change_password(NewPassword) ->
    cb_gosecrets_runner:change_password(?RUNNER, NewPassword).

get_keys_ref() ->
    cb_gosecrets_runner:get_keys_ref(?RUNNER).

get_state() ->
    cb_gosecrets_runner:get_state(?RUNNER).

rotate_data_key() ->
    cb_gosecrets_runner:rotate_data_key(?RUNNER).

maybe_clear_backup_key(DataKey) ->
    cb_gosecrets_runner:maybe_clear_backup_key(?RUNNER, DataKey).

os_pid() ->
    cb_gosecrets_runner:os_pid(?RUNNER).

reconfigure(NewCfg) ->
    safe_call({change_config, NewCfg}, infinity).

%%%===================================================================
%%% callbacks
%%%===================================================================

init([]) ->
    case recover() of
        ok -> {ok, #{}};
        {error, _} = Error -> {stop, {recover_failed, Error}}
    end.

handle_call({change_config, Cfg}, _From, State) ->
    case change_config(Cfg) of
        ok -> {reply, ok, State};
        {error, _} = Error ->
            {stop, {change_cfg_failed, Error}, Error, State}
    end;
handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(Msg, _From, State) ->
    ?log_error("unhandled call: ~p", [Msg]),
    {reply, unhandled, State}.

handle_cast(Msg, State) ->
    ?log_error("unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?log_error("unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

change_config(NewCfg) ->
    OldCfg = ns_config:read_key_fast(ns_config_sm_key(), []),
    ?log_debug("Change config started.~nOld cfg: ~p~nNew cfg: ~p",
               [OldCfg, NewCfg]),
    MarkerPath = change_cfg_marker(),
    MarkerBody = io_lib:format("~p.", [{config_change, {OldCfg, NewCfg}}]),
    misc:create_marker(MarkerPath, iolist_to_binary(MarkerBody)),
    case change_config(NewCfg, OldCfg, MarkerPath, _CopySecrets = true,
                       _ResetPassword = true) of
        ok ->
            misc:remove_marker(MarkerPath),
            ok;
        {error, _} = Error -> Error
    end.

%% Note: We don't need to copy secrets if we are recovering after failure
%% because if we have removed old cfg secrets, the config change is actually
%% already finished.
change_config(NewCfg, OldCfg, MarkerPath, _CopySecrets = true,
              ResetPassword = true) ->
    case cb_gosecrets_runner:copy_secrets(?RUNNER, NewCfg) of
        {ok, <<"same">>} ->
            ok;
        {ok, <<"copied">>} ->
            MarkerBody = io_lib:format("~p.",
                                       [{config_change_copy_done,
                                         {OldCfg, NewCfg}}]),
            misc:create_marker(MarkerPath, iolist_to_binary(MarkerBody)),
            change_config(NewCfg, OldCfg, MarkerPath, false, ResetPassword);
        {error, _} = Error ->
            Error
    end;
%% copy_secrets doesn't support using custom passwords, because we always
%% reset it. Looks like we don't really need to support that case anyway.
%% The only scenario when we don't want to reset password is when we are
%% recovering after unsuccessful change_config attempt.
change_config(_NewCfg, _OldCfg, _MarkerPath, _CopySecrets = true,
              _ResetPassword = false) ->
    error(not_supported);
change_config(NewCfg, OldCfg, MarkerPath, false, ResetPassword) ->
    ns_config:set(ns_config_sm_key(), NewCfg),
    case cb_gosecrets_runner:set_config(?RUNNER, NewCfg, ResetPassword) of
        ok ->
            %% If a hard error happens during removal of old secrets,
            %% it might be very hard to recover to previous config
            %% (because old secrets can be already removed and we can't
            %% copy secrets back because of that hard error).
            %% So it seems like it is safer to not return error here.
            MarkerBody = io_lib:format("~p.",
                                       [{cleanup_secrets, OldCfg}]),
            misc:create_marker(MarkerPath, iolist_to_binary(MarkerBody)),
            cb_gosecrets_runner:cleanup_secrets(?RUNNER, OldCfg),
            ok;
        {error, _} = Error ->
            Error
    end.

recover() ->
    MarkerPath = change_cfg_marker(),
    case misc:consult_marker(MarkerPath) of
        %% Actual change has not started yet, so we can just remove new secrets
        %% that we created during the config change attempt and be done
        {ok, [{config_change, {_OldCfg, NewCfg}}]} ->
            ?log_warning("Found config_change marker. Starting cleanup for new cfg: ~p",
                         [NewCfg]),
            case cb_gosecrets_runner:cleanup_secrets(?RUNNER, NewCfg) of
                ok ->
                    ?log_debug("Cleanup finished successfully");
                {error, _} = Error ->
                    %% Ignoring because there is not match we can do
                    ?log_error("Cleanup failed with error: ~p. Ignoring...",
                               [Error])
            end,
            misc:remove_marker(MarkerPath),
            ok;
        %% Copy of configs has finished, and actual config change has probably
        %% started, but old secrets are not removed yet.
        {ok, [{config_change_copy_done, {OldCfg, NewCfg}}]} ->
            ?log_warning("Found change config marker. Starting gosecrets "
                         "recover to old config~nOld cfg: ~p~nNew cfg: ~p",
                         [OldCfg, NewCfg]),
            case change_config(OldCfg, NewCfg, MarkerPath,
                               _CopySecrets = false, _ResetPassword = false) of
                ok ->
                    ?log_debug("Recover finished successfully"),
                    misc:remove_marker(MarkerPath),
                    ok;
                {error, _} = Error ->
                    ?log_error("Recover failed with reason: ~p", [Error]),
                    Error
            end;
        %% We have already finished new config reload. The only thing that is
        %% not done yet, is the removal of old secrets.
        {ok, [{cleanup_secrets, OldCfg}]} ->
            ?log_warning("Found cleanup marker. Starting cleanup for cfg: ~p",
                         [OldCfg]),
            case cb_gosecrets_runner:cleanup_secrets(?RUNNER, OldCfg) of
                ok ->
                    ?log_debug("Cleanup finished successfully");
                {error, _} = Error ->
                    %% Ignoring because there is not match we can do
                    ?log_error("Cleanup failed with error: ~p. Ignoring...",
                               [Error])
            end,
            misc:remove_marker(MarkerPath),
            ok;
        false ->
            ?log_debug("Config marker ~s doesn't exist", [MarkerPath]),
            ok
    end.

change_cfg_marker() ->
    filename:join(path_config:component_path(data, "config"),
                  "sm_load_config_marker").

ns_config_sm_key() -> {node, node(), secret_mngmt_cfg}.

safe_call(Req, Timeout) ->
    safe_call(Req, Timeout, 100).

safe_call(Req, _Timeout, AttemptsLeft) when AttemptsLeft =< 0 ->
    ?log_error("Call ~p retries exceeded", [Req]),
    {error, call_retries_exceeded};
safe_call(Req, Timeout, AttemptsLeft) ->
    wait_for_server_start(),
    try
        gen_server:call(?MODULE, Req, Timeout)
    catch
        exit:{{change_cfg_failed, _}, _} ->
            %% The process is being restarted because it failed to change
            %% config (previous call has failed, not this one), so we should
            %% wait until it restarts, and retry
            true = wait_for_server_start(),
            safe_call(Req, Timeout, AttemptsLeft - 1);
        exit:{{recover_failed, _}, _} ->
            %% The process is being restarted because it failed to recover
            %% after unsuccessful config change.
            %% config (previous call has failed, not this one), so we should
            %% wait until it restarts, and retry
            true = wait_for_server_start(),
            safe_call(Req, Timeout, AttemptsLeft - 1)
    end.

wait_for_server_start() ->
    misc:poll_for_condition(
      fun () ->
          try
              is_process_alive(whereis(?MODULE)) andalso
              (ok == gen_server:call(?MODULE, sync))
          catch
              _:_ -> false
          end
      end, ?RESTART_WAIT_TIMEOUT, 100).
