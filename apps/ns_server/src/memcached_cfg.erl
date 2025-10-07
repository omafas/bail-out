%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc behavior for maintaining memcached configuration files

-module(memcached_cfg).

-behaviour(gen_server).

-include_lib("ns_common/include/cut.hrl").

-export([start_link/2, sync/1, sync_reload/1, get_key_ids_in_use/1]).

%% gen_event callbacks
-export([init/1, handle_cast/2, handle_call/3,
         handle_info/2, terminate/2, code_change/3]).

-export([format_status/1]).

-define(MAX_RENAME_RETRIES, 6).

-callback init() -> term().
-callback filter_event(term()) -> boolean().
-callback handle_event(term(), term()) -> {changed, term()} | unchanged.
-callback producer(term()) -> pipes:producer(iolist()).
-callback refresh() -> term().

-include("ns_common.hrl").

-record(state, {stuff,
                module,
                retry_timer,
                path,
                tmp_path,
                sync_froms = [],
                keys_in_use}).

format_status(#{state := #state{module = Mod, stuff = Stuff} = State}) ->
    case erlang:function_exported(Mod, format_status, 1) of
        true ->
            #{state => State#state{stuff = Mod:format_status(Stuff)}};
        false ->
            #{state => State}
    end.

sync(Module) ->
    ns_config:sync_announcements(),
    gen_server:call(Module, sync, infinity),
    memcached_refresh:sync().

sync_reload(Module) ->
    gen_server:cast(Module, reload_file),
    sync(Module).

get_key_ids_in_use(Module) ->
    gen_server:call(Module, get_key_ids_in_use).

start_link(Module, Path) ->
    gen_server:start_link({local, Module}, ?MODULE, [Module, Path], []).

init([Module, Path]) ->
    ?log_debug("Init config writer for ~p, ~p", [Module, Path]),
    Pid = self(),
    EventHandler =
        fun (Key) ->
                case Module:filter_event(Key) of
                    true ->
                        Pid ! {event, Key};
                    false ->
                        ok
                end
        end,
    chronicle_compat_events:subscribe(
      fun (cluster_compat_version) ->
              gen_server:cast(Pid, full_reset);
          (Key) ->
              EventHandler(Key)
      end),
    ns_pubsub:subscribe_link(user_storage_events,
                             fun ({Key, _}) -> EventHandler(Key) end),

    Stuff = Module:init(),
    State = #state{path = Path,
                   tmp_path = Path ++ ".tmp",
                   stuff = Stuff,
                   module = Module,
                   retry_timer = undefined},

    {ok, initiate_write(State)}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_cast(reload_file, State) ->
    {noreply, initiate_write(State)};
handle_cast(full_reset, State = #state{module = Module}) ->
    {noreply, initiate_write(State#state{stuff = Module:init()})}.

handle_call(get_key_ids_in_use, _from, #state{keys_in_use = InUse} = State) ->
    Res = case InUse of
              #{cfg := {ok, K1}, tmp := {ok, K2}} -> {ok, K1 ++ K2};
              #{cfg := {error, E}, tmp := _} -> {error, {read_file_error, E}};
              #{cfg := _, tmp := {error, E}} -> {error, {read_file_error, E}}
          end,
    {reply, Res, State};
handle_call(sync, _From, #state{retry_timer = undefined} = State) ->
    {reply, ok, State};
handle_call(sync, From, #state{sync_froms = Froms} = State) ->
    {noreply, State#state{sync_froms = [From | Froms]}}.

handle_info({event, Key} = Event, State = #state{module = Module,
                                                 stuff = Stuff}) ->
    misc:flush(Event),
    case Module:handle_event(Key, Stuff) of
        {changed, NewStuff} ->
            {noreply, initiate_write(State#state{stuff = NewStuff})};
        unchanged ->
            {noreply, State}
    end;
handle_info({retry_rename_and_refresh, Tries, SleepTime}, State) ->
    NewState = case rename_and_refresh(State, Tries, SleepTime) of
                   ok ->
                       %% Rename was successful
                       reply_to_syncs(State#state{retry_timer = undefined});
                   TRef ->
                       %% Error still happened so new timer started.
                       State#state{retry_timer = TRef}
               end,
    {noreply, update_keys_in_use(NewState)};

handle_info(_Info, State) ->
    {noreply, State}.

reply_to_syncs(#state{sync_froms = Froms} = State) ->
    [gen_server:reply(F, ok) || F <- lists:reverse(Froms)],
    State#state{sync_froms = []}.

cancel_retry_timer(undefined) ->
    ok;
cancel_retry_timer(TRef) ->
    erlang:cancel_timer(TRef),
    %% Just in case message came in right before the cancel attempt
    ?flush({retry_rename_and_refresh, _, _}),
    ok.

initiate_write(#state{retry_timer = TRef} = State) ->
    %% If we're retrying a rename we'll just punt on that and start over
    %% with new config information.
    cancel_retry_timer(TRef),
    NewState = case write_cfg(State) of
                    ok ->
                        reply_to_syncs(State#state{retry_timer = undefined});
                    NewTRef when is_reference(NewTRef) ->
                        %% Rename failed and needs to be retried
                        State#state{retry_timer = NewTRef}
                end,
    update_keys_in_use(NewState).

write_cfg(#state{path = Path,
                 tmp_path = TmpPath,
                 stuff = Stuff,
                 module = Module} = State) ->
    case Module:producer(Stuff) of
        undefined ->
            ok;
        Producer ->
            ok = filelib:ensure_dir(TmpPath),
            ?log_debug("Writing config file for: ~p", [Path]),
            WriteConsumer = pipes:write_encrypted_file(_,
                                                       extract_deks_snapshot()),
            WriteFun = ?cut(pipes:run(Producer, WriteConsumer(_))),
            case misc:write_file(TmpPath, WriteFun) of
                ok ->
                    ok;
                Error ->
                    ?log_error("Failed to write configuration to ~p: ~p",
                               [TmpPath, Error]),
                    %% Failed to write the configuration. Restart and hope
                    %% things are better next time.
                    erlang:exit({failed_to_write_configuration, TmpPath})
            end,
            rename_and_refresh(State, ?MAX_RENAME_RETRIES, 101)
    end.

extract_deks_snapshot() ->
    case memcached_config_mgr:get_global_memcached_deks() of
        undefined ->
            {ok, DS} = cb_crypto:fetch_deks_snapshot(configDek),
            case memcached_config_mgr:push_config_encryption_key(false) of
                ok ->
                    memcached_config_mgr:get_global_memcached_deks();
                {error, retry} ->
                    %% This means the process is not started yet
                    %% (which is normal because it starts after this
                    %% process). If so, we are ok to use prefetched
                    %% encryption keys. The biggest risk is if we use the
                    %% key that is not known by memcached. Since here we
                    %% fetch keys before we call memcached_config_mgr,
                    %% it is guaranteed that the prefetched keys will not be
                    %% newer than those that memcached_config_mgr
                    %% will push to memcached.
                    DS
            end;
        DS -> DS
    end.

rename_and_refresh(#state{path = Path,
                          tmp_path = TmpPath,
                          module = Module}, Tries, SleepTime) ->
    case memcached_refresh:apply_to_file(TmpPath, Path) of
        ok ->
            ok = Module:refresh(),
            ?log_debug("Successfully renamed ~p to ~p", [TmpPath, Path]),
            ok;
        {error, Reason} ->
            %% It's likely the rename failed as the destination file is
            %% open by memcached. Retrying will allow it to finish up
            %% and close the file.
            ?log_warning("Error renaming ~p to ~p: ~p", [TmpPath, Path, Reason]),
            case Tries of
                0 ->
                    ?log_error("Exhausted ~p retries to rename ~p to ~p",
                               [?MAX_RENAME_RETRIES, TmpPath, Path]),
                    %% We failed to rename the file and effectively have lost
                    %% the writes. Restart and hope things are better next
                    %% time.
                    file:delete(TmpPath),
                    erlang:exit({failed_to_rename_configuration,
                                 TmpPath, Path});
                _ ->
                    ?log_info("Trying again after ~p ms (~p tries remaining)",
                              [SleepTime, Tries]),
                    erlang:send_after(SleepTime, self(),
                                      {retry_rename_and_refresh, Tries - 1,
                                       SleepTime * 2})
            end
    end.

update_keys_in_use(#state{tmp_path = TmpPath, path = Path} = State) ->
    State#state{keys_in_use = #{cfg => cb_crypto:get_file_dek_ids(Path),
                                tmp => cb_crypto:get_file_dek_ids(TmpPath)}}.
