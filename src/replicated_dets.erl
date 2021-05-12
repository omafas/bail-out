%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc replicated storage based on dets

-module(replicated_dets).

-include("ns_common.hrl").
-include("pipes.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(replicated_storage).

-export([start_link/5, set/3, delete/2, get/2, get/3,
         get_last_modified/3, select/3, select/4, empty/1,
         select_with_update/4]).

-export([init/1, init_after_ack/1, handle_call/3, handle_info/2,
         get_id/1, get_value/1, find_doc/2, all_docs/1,
         get_revision/1, set_revision/2, is_deleted/1, save_docs/2,
         handle_mass_update/3, on_replicate_in/1, on_replicate_out/1]).

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
    gen_server:call(Name, {interactive_update, update_doc(Id, Value)}, infinity).

delete(Name, Id) ->
    gen_server:call(Name, {interactive_update, delete_doc(Id)}, infinity).

delete_doc(Id) ->
    #docv2{id = Id, value = [], props = [{deleted, true}, {rev, 0}]}.

update_doc(Id, Value) ->
    LastModified = [{last_modified, os:system_time(millisecond)}],
    #docv2{id = Id, value = Value,
           props = [{deleted, false}, {rev, 0}] ++ LastModified}.

empty(Name) ->
    gen_server:call(Name, empty, infinity).

with_live_doc(TableName, Id, Default, Fun) ->
    case ets:lookup(TableName, Id) of
        [Doc] ->
            case is_deleted(Doc) of
                false ->
                    Fun(Doc);
                true ->
                    Default
            end;
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
    select(Name, KeySpec, N, false).

select(Name, KeySpec, N, Locked) ->
    DocSpec = #docv2{id = KeySpec, _ = '_'},
    MatchSpec = [{DocSpec, [], ['$_']}],

    Select =
        case Locked of
            true ->
                fun select_from_dets_locked/4;
            false ->
                fun select_from_dets/4
        end,

    ?make_producer(Select(Name, MatchSpec, N,
                          fun (Selection) ->
                                  lists:foreach(
                                    fun (#docv2{id = Id, value = Value} = D) ->
                                            case is_deleted(D) of
                                                false -> ?yield({Id, Value});
                                                true -> ok
                                            end
                                    end, Selection)
                          end)).

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
    {RawErrors, ParentState} = pipes:run(select(Name, KeySpec, N, true), Transducer, Updater),
    Errors = [{Id, Error} || {#docv2{id = Id}, Error} <- RawErrors],
    {Errors, ParentState}.

init([Name, ChildModule, InitParams, Path, Replicator]) ->
    replicated_storage:anounce_startup(Replicator),
    ChildState = ChildModule:init(InitParams),
    init_ets(Name, protected),
    #state{name = Name,
           path = Path,
           child_module = ChildModule,
           child_state = ChildState}.

init_ets(Name, Access) ->
    ets:new(Name, [named_table, set, Access, {keypos, #docv2.id}]).

init_after_ack(State = #state{name = TableName}) ->
    Start = os:timestamp(),
    ok = open(State),
    ?log_debug("Loading ~p items, ~p words took ~pms",
               [ets:info(TableName, size),
                ets:info(TableName, memory),
                timer:now_diff(os:timestamp(), Start) div 1000]),
    State.

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

do_open(_Path, _TableName, 0) ->
    error;
do_open(Path, TableName, Tries) ->
    case dets:open_file(TableName,
                        [{type, set},
                         {auto_save, ns_config:read_key_fast(replicated_dets_auto_save, 60000)},
                         {keypos, #docv2.id},
                         {file, Path}]) of
        {ok, TableName} ->
            TableName = dets:to_ets(TableName, TableName),
            ok;
        Error ->
            ?log_error("Unable to open ~p, Error: ~p", [Path, Error]),
            timer:sleep(1000),
            do_open(Path, TableName, Tries - 1)
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
            false
    end.

all_docs(Pid) ->
    ?make_producer(select_from_dets(Pid, [{'_', [], ['$_']}], 500,
                                    fun (Batch) -> ?yield({batch, Batch}) end)).

get_revision(#docv2{props = Props}) ->
    proplists:get_value(rev, Props).

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
    ok = dets:insert(TableName, Docs),
    true = ets:insert(TableName, Docs),
    NewChildState = ChildModule:on_save(Docs, ChildState),
    {ok, State#state{child_state = NewChildState}}.

on_replicate_in(Doc) ->
    Doc.

on_replicate_out(Doc) ->
    Doc.

handle_call(suspend, {Pid, _} = From, #state{name = TableName} = State) ->
    MRef = erlang:monitor(process, Pid),
    ?log_debug("Suspended by process ~p", [Pid]),
    gen_server:reply(From, {ok, TableName}),
    receive
        {'DOWN', MRef, _, _, _} ->
            ?log_info("Suspending process ~p died", [Pid]),
            {noreply, State};
        release ->
            ?log_debug("Released by process ~p", [Pid]),
            erlang:demonitor(MRef, [flush]),
            {noreply, State}
    end;
handle_call(empty, _From, #state{name = TableName,
                                 child_module = ChildModule,
                                 child_state = ChildState} = State) ->
    ok = dets:delete_all_objects(TableName),
    true = ets:delete_all_objects(TableName),
    NewChildState = ChildModule:on_empty(ChildState),
    {reply, ok, State#state{child_state = NewChildState}};
handle_call(Msg, From, #state{name = TableName,
                              child_module = ChildModule,
                              child_state = ChildState} = State) ->
    {reply, RV, NewChildState} = ChildModule:handle_call(Msg, From, TableName, ChildState),
    {reply, RV, State#state{child_state = NewChildState}}.

handle_info(Msg, #state{child_module = ChildModule,
                        child_state = ChildState} = State) ->
    {noreply, NewChildState} = ChildModule:handle_info(Msg, ChildState),
    {noreply, State#state{child_state = NewChildState}}.

select_from_dets(Name, MatchSpec, N, Yield) ->
    Key = {suspended, ?MODULE, Name},
    {Locked, TableName} =
        case erlang:get(Key) of
            undefined ->
                {ok, Table} = gen_server:call(Name, suspend, infinity),
                erlang:put(Key, Table),
                {true, Table};
            Table ->
                {false, Table}
        end,
    try
        select_from_dets_locked(TableName, MatchSpec, N, Yield)
    after
        Locked andalso erlang:erase(Key),
        Locked andalso (Name ! release)
    end.

select_from_dets_locked(TableName, MatchSpec, N, Yield) ->
    ?log_debug("Starting select with ~p", [{TableName, MatchSpec, N}]),
    ets:safe_fixtable(TableName, true),
    do_select_from_dets(TableName, MatchSpec, N, Yield),
    ets:safe_fixtable(TableName, false),
    ok.

do_select_from_dets(TableName, MatchSpec, N, Yield) ->
    case ets:select(TableName, MatchSpec, N) of
        {Selection, Continuation} when is_list(Selection) ->
            do_select_from_dets_continue(Selection, Continuation, Yield);
        '$end_of_table' ->
            ok
    end.

do_select_from_dets_continue(Selection, Continuation, Yield) ->
    Yield(Selection),
    case ets:select(Continuation) of
        {Selection2, Continuation2} when is_list(Selection2) ->
            do_select_from_dets_continue(Selection2, Continuation2, Yield);
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
