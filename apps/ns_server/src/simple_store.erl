%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%%
%% Simple KV storage using ETS table as front end and file as the back end
%% for persistence.
%% Initialize using simple_store:start_link([your_store_name]).
%% Current consumer is XDCR checkpoints.
%%
-module(simple_store).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

%% APIs

-export([start_link/2,
         get/2, get/3,
         set/3,
         delete/2,
         delete_matching/2,
         clear/1,
         list/1,
         iterate_matching/2,
         resave/1,
         get_key_ids_in_use/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

%% Macros

%% Persist the ETS table to file after 10 secs.
%% All updates to the table during that window will automatically get batched
%% and flushed to the file together.
-define(FLUSH_AFTER, 10 * 1000).

%% Max number of unsucessful flush attempts before giving up.
-define(FLUSH_RETRIES, 10).

-record(state, {flush_pending :: undefined | erlang:reference(),
                store_name :: atom(),
                keys_in_use = [] :: [undefined | cb_deks:dek_id()],
                should_encrypt = true :: boolean()}).

%% Exported APIs

start_link(StoreName, ShouldEncrypt) ->
    ProcName = get_proc_name(StoreName),
    gen_server:start_link({local, ProcName}, ?MODULE,
                          [StoreName, ShouldEncrypt], []).

get(StoreName, Key) ->
    get(StoreName, Key, false).

get(StoreName, Key, Default) ->
    case ets:lookup(StoreName, Key) of
        [{Key, Value}] ->
            Value;
        [] ->
            Default
    end.

set(StoreName, Key, Value) ->
    do_work(StoreName, fun update_store/2, [Key, Value]).

delete(StoreName, Key) ->
    do_work(StoreName, fun delete_from_store/2, [Key]).

clear(StoreName) ->
    do_work(StoreName, fun clear_store/2, []).

list(StoreName) ->
    do_work(StoreName, fun list_store/2, []).

%% Delete keys with matching prefix
delete_matching(StoreName, KeyPattern) ->
    do_work(StoreName, fun del_matching/2, [KeyPattern]).

%% Return keys with matching prefix
iterate_matching(StoreName, KeyPattern) ->
    ets:foldl(
      fun ({Key, Value}, Acc) ->
              case misc:is_prefix(KeyPattern, Key) of
                  true ->
                      ?metakv_debug("Returning Key ~p.", [Key]),
                      [{Key, Value} | Acc];
                  false ->
                      Acc
              end
      end, [], StoreName).

resave(StoreName) ->
    do_work(StoreName, fun resave/2, []).

get_key_ids_in_use(StoreName) ->
    do_work(StoreName, fun get_key_ids/2, []).

%% Internal
init([StoreName, ShouldEncrypt]) ->
    %% Populate the table from the file if the file exists otherwise create
    %% an empty table.
    FilePath = path_config:component_path(data, get_file_name(StoreName)),
    {Created, KeysInUse} =
        case cb_crypto:get_file_dek_ids(FilePath) of
            {ok, []} ->
                {false, []};
            {ok, [undefined]} ->
                ?metakv_debug("Reading ~p content from unencrypted ~s",
                              [StoreName, FilePath]),
                {init_unencrypted(StoreName, FilePath), [undefined]};
            {ok, [KeyId]} when is_binary(KeyId) ->
                ?metakv_debug("Reading ~p content from encrypted ~s",
                              [StoreName, FilePath]),
                {init_encrypted(StoreName, FilePath), [KeyId]};
            {error, Error} ->
                ?metakv_debug("Failed to read ~p content from ~s: ~p",
                              [StoreName, FilePath, Error]),
                {false, []}
        end,

    case Created of
        true ->
            ok;
        false ->
            ?metakv_debug("Creating Table: ~p", [StoreName]),
            init_new(StoreName)
    end,

    State = #state{flush_pending = undefined, store_name = StoreName,
                   should_encrypt = ShouldEncrypt},
    {ok, State#state{keys_in_use = KeysInUse}}.

init_new(StoreName) ->
    ets:new(StoreName, [named_table, set, protected]).

init_encrypted(StoreName, FilePath) ->
    init_new(StoreName),
    {ok, DS} = cb_crypto:fetch_deks_snapshot(configDek),
    case file2tab_encrypted(StoreName, FilePath, DS) of
        ok ->
            true;
        {error, Error} ->
            ?metakv_debug("Failed to read ~p content from ~s: ~p",
                          [StoreName, FilePath, Error]),
            %% Removing the table so fresh init can recreate it
            ets:delete(StoreName),
            false
    end.

init_unencrypted(StoreName, FilePath) ->
    case ets:file2tab(FilePath, [{verify, true}]) of
        {ok, StoreName} ->
            true;
        {error, Error} ->
            ?metakv_debug("Failed to read ~p content from ~s: ~p",
                          [StoreName, FilePath, Error]),
            false
    end.

handle_call({work, Fun}, _From, State) ->
    {Res, NewState} = Fun(State),
    {reply, Res, NewState, hibernate};

handle_call(Unhandled, _From, State) ->
    ?log_error("Unhandled call: ~p", [Unhandled]),
    {noreply, State}.

handle_cast(Unhandled, State) ->
    ?log_error("Unhandled cast: ~p", [Unhandled]),
    {noreply, State}.

handle_info({timer, flush, NumRetries}, State) ->
    ?flush({timer, flush, _}),
    {noreply, flush_table(NumRetries, State)};

handle_info(Unhandled, State) ->
    ?log_error("Unhandled info: ~p", [Unhandled]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

do_work(StoreName, Fun, Args) ->
    gen_server:call(
      get_proc_name(StoreName),
      {work, fun (S) -> erlang:apply(Fun, [Args, S]) end},
       infinity).

%% Update the ETS table and schedule a flush to the file.
update_store([Key, Value], #state{store_name = StoreName} = State) ->
    ?metakv_debug("Updating data ~p in table ~p.", [[{Key, Value}], StoreName]),
    ets:insert(StoreName, [{Key, Value}]),
    {ok, schedule_flush(?FLUSH_RETRIES, State)}.

%% Delete from the ETS table and schedule a flush to the file.
delete_from_store([Key], #state{store_name = StoreName} = State) ->
    ?metakv_debug("Deleting key ~p in table ~p.", [Key, StoreName]),
    ets:delete(StoreName, Key),
    {ok, schedule_flush(?FLUSH_RETRIES, State)}.

clear_store([], #state{store_name = StoreName} = State) ->
    ?metakv_debug("Deleting all keys in table ~p.", [StoreName]),
    ets:delete_all_objects(StoreName),
    {ok, schedule_flush(?FLUSH_RETRIES, State)}.

list_store([], #state{store_name = StoreName} = State) ->
    ?metakv_debug("Listing all entries in table ~p.", [StoreName]),
    List = ets:tab2list(StoreName),
    {{ok, List}, State}.

del_matching([KeyPattern], #state{store_name = StoreName} = State) ->
    ets:foldl(
      fun ({Key, _}, _) ->
              case misc:is_prefix(KeyPattern, Key) of
                  true ->
                      ?metakv_debug("Deleting Key ~p.", [Key]),
                      ets:delete(StoreName, Key);
                  false ->
                      ok
              end
      end, undefined, StoreName),
    {ok, schedule_flush(?FLUSH_RETRIES, State)}.

resave([], #state{keys_in_use = InUse} = State) ->
    {ok, DS} = cb_crypto:fetch_deks_snapshot(configDek),
    ActiveKeyId = cb_crypto:get_dek_id(DS),
    case [ActiveKeyId] == InUse of
        true -> {ok, State};
        false -> write_table_to_disk(DS, State)
    end.

get_key_ids([], #state{keys_in_use = KeyIds} = State) ->
    {{ok, KeyIds}, State}.

%% Nothing can be done if we failed to flush repeatedly.
schedule_flush(0, #state{store_name = StoreName} = _State) ->
    ?metakv_debug("Tried to flush table ~p ~p times but failed. Giving up.",
                  [StoreName, ?FLUSH_RETRIES]),
    exit(flush_failed);

%% If flush is pending then nothing else to do otherwise schedule a
%% flush to the file for later.
schedule_flush(_NumRetries,
               #state{flush_pending = Ref} = State) when is_reference(Ref) ->
    ?metakv_debug("Flush is already pending."),
    State;
schedule_flush(NumRetries, #state{flush_pending = undefined} = State) ->
    Ref = erlang:send_after(?FLUSH_AFTER, self(), {timer, flush, NumRetries}),
    ?metakv_debug("Successfully scheduled a flush to the file."),
    State#state{flush_pending = Ref}.

%% Flush the table to the file.
flush_table(NumRetries, State) ->
    {ok, DS} = cb_crypto:fetch_deks_snapshot(configDek),
    case write_table_to_disk(DS, stop_flush_timer(State)) of
        {ok, NewState} ->
            NewState;
        {{error, _Error}, NewState} ->
            %% Reschedule another flush.
            schedule_flush(NumRetries - 1, NewState)
    end.

write_table_to_disk(DS, #state{store_name = StoreName,
                               should_encrypt = ShouldEncrypt} = State) ->
    FilePath = path_config:component_path(data, get_file_name(StoreName)),
    ?metakv_debug("Persisting Table ~p to file ~p.", [StoreName, FilePath]),
    ActiveKeyId = cb_crypto:get_dek_id(DS),
    EncryptionEnabled = (ActiveKeyId /= undefined),
    Res = case ShouldEncrypt andalso EncryptionEnabled of
              true -> tab2file_encrypted(StoreName, FilePath, DS);
              false -> ets:tab2file(StoreName, FilePath,
                                    [{extended_info, [object_count]}])
          end,
    case Res of
        ok ->
            {ok, State#state{keys_in_use = [ActiveKeyId]}};
        {error, Error} ->
            ?metakv_debug("Failed to persist table ~p to file ~p with error "
                          "~p.", [StoreName, FilePath, Error]),
            {{error, Error}, State}
    end.

stop_flush_timer(#state{flush_pending = undefined} = State) -> State;
stop_flush_timer(#state{flush_pending = Ref} = State) when is_reference(Ref) ->
    catch erlang:cancel_timer(Ref),
    State#state{flush_pending = undefined}.

get_proc_name(StoreName) ->
    list_to_atom(get_file_name(StoreName)).

get_file_name(StoreName) ->
    atom_to_list(?MODULE) ++ "_" ++ atom_to_list(StoreName).

file2tab_encrypted(StoreName, FilePath, DS) ->
    F = fun (Chunk, Acc) ->
            Records = binary_to_term(iolist_to_binary(Chunk)),
            ets:insert(StoreName, Records),
            {ok, Acc}
        end,
    case cb_crypto:read_file_chunks(FilePath, F, ok, DS, #{}) of
        {ok, _} -> ok;
        {error, R, _} -> {error, R}
    end.

tab2file_encrypted(StoreName, FilePath, DS) ->
    WriteFun = write_terms(_, init, {StoreName, DS}),
    try
        ets:safe_fixtable(StoreName, true),
        misc:atomic_write_file(FilePath, WriteFun)
    after
        ets:safe_fixtable(StoreName, false)
    end.

write_terms(FileHandle, init, {StoreName, DS}) ->
    maybe
        {ok, {Header, EncrState}} ?= cb_crypto:file_encrypt_init(DS),
        ok ?= file:write(FileHandle, Header),
        write_terms(FileHandle,
                    ets:select(StoreName, [{'_', [], ['$_']}], 100),
                    EncrState)
    end;
write_terms(FileHandle, {Records, Continuation}, State) ->
    Data = term_to_binary(Records),
    {EncryptedData, NewState} = cb_crypto:file_encrypt_chunk(Data, State),
    case file:write(FileHandle, EncryptedData) of
        ok -> write_terms(FileHandle, ets:select(Continuation), NewState);
        {error, _} = E ->
            _ = cb_crypto:file_encrypt_finish(NewState),
            E
    end;
write_terms(FileHandle, '$end_of_table', State) ->
    FinalData = cb_crypto:file_encrypt_finish(State),
    case file:write(FileHandle, FinalData) of
        ok -> ok;
        {error, _} = E -> E
    end.
