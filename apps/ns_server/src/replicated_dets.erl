%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc replicated storage based on dets

-module(replicated_dets).

-include("ns_common.hrl").
-include("pipes.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("cb_cluster_secrets.hrl").

-behaviour(replicated_storage).

-define(ENCRYPTION_MAGIC, 45).
-define(RESAVE_TIMEOUT_MS, 120000).

-export([start_link/5, set/3, set/4,
         change_multiple/2, delete/2, delete/3, get/2, get/3,
         get_last_modified/3, select/3,
         select_with_update/4]).

-export([init/1, init_after_ack/1, handle_call/3, handle_info/2,
         get_id/1, get_value/1, find_doc/2, all_docs/2,
         get_revision/1, set_revision/2, is_deleted/1, save_docs/2,
         handle_mass_update/3, on_replicate_in/1, on_replicate_out/1,
         is_higher_priority/2, apply_keys_and_resave/1, get_key_ids_in_use/1]).

%% unit test helpers
-export([toy_init/1, toy_set/3, toy_select_with_update/4]).

-record(state, {child_module :: atom(),
                child_state :: term(),
                path :: string(),
                name :: atom()}).

-record(docv2, {id    :: term(),
                value :: term(),
                props :: [{atom(), term()}] | '_'}).

start_link(ChildModule, InitParams, Name, Path, Replicator) ->
    replicated_storage:start_link(Name, ?MODULE,
                                  [Name, ChildModule, InitParams, Path, Replicator],
                                  Replicator).

set(Name, Id, Value) ->
    set(Name, Id, Value, []).

set(Name, Id, Value, ExtraProps) when is_list(ExtraProps) ->
   gen_server:call(
     Name, {interactive_update,
            update_doc(Id, Value, ExtraProps)}, infinity).

change_multiple(Name, Docs) when is_list(Docs) ->
    gen_server:call(
      Name,
      {interactive_update_multi,
       lists:map(fun
                     ({set, Id, Val}) ->
                        update_doc(Id, Val);
                     ({set, Id, Val, DocProps}) when is_list(DocProps) ->
                        update_doc(Id, Val, DocProps);
                     ({delete, Id}) ->
                        delete_doc(Id);
                     ({delete, Id, DocProps}) when is_list(DocProps) ->
                        delete_doc(Id, DocProps)
                end, Docs)},
      infinity).

delete(Name, Id) ->
    delete(Name, Id, []).

delete(Name, Id, ExtraProps) when is_list(ExtraProps) ->
    gen_server:call(
      Name, {interactive_update, delete_doc(Id, ExtraProps)}, infinity).

delete_doc(Id) ->
    delete_doc(Id, []).

delete_doc(Id, ExtraProps) ->
    #docv2{id = Id, value = [],
           props = [{deleted, true}, {rev, 0}] ++ ExtraProps}.

update_doc(Id, Value) ->
    update_doc(Id, Value, []).

update_doc(Id, Value, ExtraProps) ->
    #docv2{id = Id, value = Value,
           props =
               [{deleted, false}, {rev, 0},
                {last_modified, os:system_time(millisecond)}] ++ ExtraProps}.

with_live_doc(TableName, Id, Default, Fun) ->
    case ets:lookup(TableName, Id) of
        [Doc] ->
            Fun(Doc);
        [] ->
            Default
    end.

get(TableName, Id) ->
    with_live_doc(TableName, Id, false,
                  fun (#docv2{value = Value}) ->
                          {Id, Value}
                  end).

get(Name, Id, Default) ->
    case get(Name, Id) of
        false ->
            Default;
        {Id, Value} ->
            Value
    end.

get_last_modified(Name, Id, Default) ->
    with_live_doc(Name, Id, Default,
                  fun(#docv2{props = Props}) ->
                          proplists:get_value(last_modified, Props, Default)
                  end).

select(Name, KeySpec, N) ->
    DocSpec = #docv2{id = KeySpec, _ = '_'},
    MatchSpec = [{DocSpec, [], ['$_']}],
    ?make_producer(
       select_from_table(Name, MatchSpec, N,
                         fun (Selection) ->
                                 lists:foreach(
                                   fun (#docv2{id = Id, value = Value}) ->
                                           ?yield({Id, Value})
                                   end, Selection)
                         end, ets)).

select_with_update(Name, KeySpec, N, UpdateFun) ->
    gen_server:call(Name, {mass_update, {Name, KeySpec, N, UpdateFun}}, infinity).

handle_mass_update({Name, KeySpec, N, UpdateFun}, Updater, _State) ->
    Transducer =
        ?make_transducer(
           pipes:foreach(
             ?producer(),
             fun ({Id, Value}) ->
                     case UpdateFun(Id, Value) of
                         skip ->
                             ok;
                         delete ->
                             ?yield(delete_doc(Id));
                         {update, NewValue} ->
                             ?yield(update_doc(Id, NewValue))
                     end
             end)),
    {RawErrors, ParentState} = pipes:run(select(Name, KeySpec, N), Transducer,
                                         Updater),
    Errors = [{Id, Error} || {#docv2{id = Id}, Error} <- RawErrors],
    {Errors, ParentState}.

apply_keys_and_resave(Name) ->
    gen_server:call(Name, apply_encryption_keys, ?RESAVE_TIMEOUT_MS).

get_key_ids_in_use(Name) ->
    gen_server:call(Name, get_key_ids_in_use, ?RESAVE_TIMEOUT_MS).

init([Name, ChildModule, InitParams, Path, Replicator]) ->
    ChildState = ChildModule:init(InitParams),
    replicated_storage:announce_startup(Replicator),
    init_ets(Name, protected),
    #state{name = Name,
           path = Path,
           child_module = ChildModule,
           child_state = ChildState}.

init_ets(Name, Access) ->
    ets:new(Name, [named_table, set, Access, {keypos, #docv2.id}]).

init_after_ack(State = #state{name = TableName}) ->
    Start = os:timestamp(),

    {ok, DekSnapshot} = cb_crypto:fetch_deks_snapshot(configDek),
    set_deks_snapshot(DekSnapshot),
    cb_dets_utils:setup_callbacks(fun encrypt_term/1,
                                  fun decrypt_term/1),
    ok = open(State),
    ?log_debug("Loading ~p items, ~p words took ~pms",
               [ets:info(TableName, size),
                ets:info(TableName, memory),
                timer:now_diff(os:timestamp(), Start) div 1000]),
    State.

encrypt_term(Term) ->
    Bin = term_to_binary(Term),
    case cb_crypto:encrypt(Bin, <<>>, get_deks_snapshot()) of
        {ok, EncryptedData} ->
            Version = 0,
            Size = byte_size(EncryptedData),
            <<?ENCRYPTION_MAGIC, Version, Size:32/unsigned-integer,
              EncryptedData/binary>>;
        {error, no_active_key} ->
            Bin
    end.

decrypt_term(<<131, _/binary>> = D) ->
    binary_to_term(D);
decrypt_term(<<?ENCRYPTION_MAGIC, 0, Size:32/unsigned-integer, Data:Size/binary,
             _Rest/binary>>) ->
    {ok, Decrypted} = cb_crypto:decrypt(Data, <<>>, get_deks_snapshot()),
    binary_to_term(Decrypted).

set_deks_snapshot(DeksSnapshot) ->
    ok = cb_crypto:all_keys_ok(DeksSnapshot),
    ok = persistent_term:put(dets_deks_snapshot, DeksSnapshot).

get_deks_snapshot() ->
    persistent_term:get(dets_deks_snapshot, undefined).

open(#state{path = Path, name = TableName}) ->
    ?log_debug("Opening file ~p", [Path]),
    case do_open(Path, TableName, 3) of
        ok ->
            ok;
        error ->
            Time = os:system_time(microsecond),
            Backup = lists:flatten(
                       io_lib:format("~s.~b.bak", [Path, Time])),
            ?log_error("Renaming possibly corrupted dets file ~p to ~p", [Path, Backup]),
            ok = file:rename(Path, Backup),
            ok = do_open(Path, TableName, 1)
    end.

dets_opts(Path) ->
    ASave = ns_config:read_key_fast(replicated_dets_auto_save, 60000),
    [{type, set},
     {auto_save, ASave},
     {keypos, #docv2.id},
     {file, Path}].

do_open(_Path, _TableName, 0) ->
    error;
do_open(Path, TableName, Tries) ->
    Opts = dets_opts(Path),
    case cb_dets:open_file(TableName, Opts) of
        {ok, TableName} ->
            %% Resaving to make sure latest encryption key is used
            ok = resave_dets(TableName, Opts),
            DocSpec = #docv2{id = '_', value = '_', props = '_'},
            MatchSpec = [{DocSpec, [], ['$_']}],
            pipes:foreach(
              ?make_producer(
                 select_from_table(TableName, MatchSpec, 100,
                                   fun(Selection) ->
                                           lists:foreach(
                                             fun(Entry) ->
                                                     case is_deleted(Entry) of
                                                         false ->
                                                             ?yield(Entry);
                                                         _ ->
                                                             ok
                                                     end
                                             end, Selection)
                                   end, cb_dets)),
              fun(Doc) ->
                      true = ets:insert(TableName, Doc)
              end),
            ok;
        Error ->
            ?log_error("Unable to open ~p, Error: ~p", [Path, Error]),
            timer:sleep(1000),
            do_open(Path, TableName, Tries - 1)
    end.

apply_encryption_keys(#state{name = TableName, path = Path}) ->
    Old = get_deks_snapshot(),
    {ok, New} = cb_crypto:fetch_deks_snapshot(configDek),
    NewWithoutHistDeks = cb_crypto:without_historical_deks(New),
    case (cb_crypto:get_dek_id(Old) /= cb_crypto:get_dek_id(New)) orelse
         (NewWithoutHistDeks /= New) of
        true ->
            set_deks_snapshot(New),
            ok = resave_dets(TableName, dets_opts(Path)),
            set_deks_snapshot(NewWithoutHistDeks);
        false ->
            ok
    end.

resave_dets(TableName, Opts) ->
    maybe
        TmpName = list_to_atom(atom_to_list(TableName) ++ "_tmp_table"),
        CurFilename = proplists:get_value(file, Opts),
        TmpFilename = CurFilename ++ ".resave.tmp",
        TmpOptsMap = maps:with([type, keypos], proplists:to_map(Opts)),
        TmpOpts = proplists:from_map(TmpOptsMap#{file => TmpFilename,
                                                 ram_file => true}),
        ok ?= case file:delete(TmpFilename) of
                  ok -> ok;
                  {error, enoent} -> ok;
                  {error, _} = E -> E
              end,
        {ok, TmpName} ?= cb_dets:open_file(TmpName, TmpOpts),
        cb_dets:traverse(
          TableName,
          fun (Obj) ->
              cb_dets:insert(TmpName, Obj),
              continue
          end),
        ok ?= cb_dets:close(TmpName),
        ok ?= cb_dets:close(TableName),
        ok ?= misc:atomic_rename(TmpFilename, CurFilename),
        {ok, TableName} ?= cb_dets:open_file(TableName, Opts),
        ok
    end.

get_id(#docv2{id = Id}) ->
    Id.

get_value(#docv2{value = Value}) ->
    Value.

find_doc(Id, #state{name = TableName}) ->
    case ets:lookup(TableName, Id) of
        [Doc] ->
            Doc;
        [] ->
            case cb_dets:lookup(TableName, Id) of
                [DDoc] -> DDoc;
                [] -> false
            end
    end.

all_docs(_Pid, #state{name = TableName}) ->
    ?make_producer(select_from_table(TableName, [{'_', [], ['$_']}], 500,
                                     fun (Batch) ->
                                             ?yield({batch, Batch})
                                     end, cb_dets)).

get_revision(#docv2{props = Props}) ->
    proplists:get_value(rev, Props).

%% If there is no priority set, set it to the default priority
%% (?REPLICATED_DETS_NORMAL_PRIORITY).
-spec get_priority(term()) ->
    ?REPLICATED_DETS_NORMAL_PRIORITY | ?REPLICATED_DETS_HIGH_PRIORITY.
get_priority(Props) ->
    proplists:get_value(priority, Props, ?REPLICATED_DETS_NORMAL_PRIORITY).

is_higher_priority(#docv2{props = OldProps} = _OldDoc,
                   #docv2{props = NewProps} = _NewDoc) ->
    get_priority(NewProps) > get_priority(OldProps).

set_revision(Doc, NewRev) ->
    misc:update_field(#docv2.props, Doc,
        fun (Props) ->
                misc:update_proplist(Props, [{rev, NewRev}])
        end).

is_deleted(#docv2{props = Props}) ->
    proplists:get_bool(deleted, Props).

save_docs(Docs, #state{name = TableName,
                       child_module = ChildModule,
                       child_state = ChildState} = State) ->
    ?log_debug("Saving ~b docs", [length(Docs)]),
    Live =
        lists:foldl(
          fun(Doc, Acc) ->
              case is_deleted(Doc) of
                  true ->
                      %% The doc is deleted so we need to remove it
                      %% from the ETS table.
                      ets:delete(TableName, get_id(Doc)),
                      Acc;
                  false -> [Doc | Acc]
              end
          end, [], Docs),
    ok = cb_dets:insert(TableName, Docs),
    %% Only insert live, non-deleted documents
    true = ets:insert(TableName, Live),
    NewChildState = ChildModule:on_save(Docs, ChildState),
    ?log_debug("save complete"),
    {ok, State#state{child_state = NewChildState}}.

on_replicate_in(Doc) ->
    Doc.

on_replicate_out(Doc) ->
    Doc.

handle_call(apply_encryption_keys, _From, #state{} = State) ->
    ok = apply_encryption_keys(State),
    {reply, ok, State};

handle_call(get_key_ids_in_use, _From, State) ->
    DekSnapshot = get_deks_snapshot(),
    %% This process crashes apply_encryption_keys fails.
    %% This means that we can't actually use two keys at this place:
    %% either we use one key (active) or this process is actually down.
    DekId = case cb_crypto:get_dek_id(DekSnapshot) of
                undefined -> ?NULL_DEK;
                Id when is_binary(Id) -> Id
            end,
    {reply, {ok, [DekId]}, State};

handle_call(Msg, From, #state{name = TableName,
                              child_module = ChildModule,
                              child_state = ChildState} = State) ->
    {reply, RV, NewChildState} = ChildModule:handle_call(Msg, From, TableName, ChildState),
    {reply, RV, State#state{child_state = NewChildState}}.

handle_info(Msg, #state{child_module = ChildModule,
                        child_state = ChildState} = State) ->
    {noreply, NewChildState} = ChildModule:handle_info(Msg, ChildState),
    {noreply, State#state{child_state = NewChildState}}.

select_from_table(TableName, MatchSpec, N, Yield, Module) ->
    ?log_debug("[~p] Starting select with ~p",
               [Module, {TableName, MatchSpec, N}]),
    Module:safe_fixtable(TableName, true),
    do_select_from_table(TableName, MatchSpec, N, Yield, Module),
    Module:safe_fixtable(TableName, false),
    ok.

do_select_from_table(TableName, MatchSpec, N, Yield, Module) ->
    case Module:select(TableName, MatchSpec, N) of
        {Selection, Continuation} when is_list(Selection) ->
            do_select_from_table_continue(Selection, Continuation,
                                          Yield, Module);
        '$end_of_table' ->
            ok
    end.

do_select_from_table_continue(Selection, Continuation, Yield, Module) ->
    Yield(Selection),
    case Module:select(Continuation) of
        {Selection2, Continuation2} when is_list(Selection2) ->
            do_select_from_table_continue(Selection2, Continuation2,
                                          Yield, Module);
        '$end_of_table' ->
            ok
    end.

toy_init(Name) ->
    init_ets(Name, public).

toy_set(Name, Id, Value) ->
    true = ets:insert(Name, [update_doc(Id, Value)]),
    ok.

toy_select_with_update(Name, KeySpec, N, UpdateFun) ->
    Updater = replicated_storage:make_mass_updater(
                fun (Doc, undefined) ->
                        true = ets:insert(Name, [Doc]),
                        {ok, undefined}
                end, undefined),
    {RV, _} =
        handle_mass_update({Name, KeySpec, N, UpdateFun}, Updater, undefined),
    RV.
