%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(capi_ddoc_manager).

-behaviour(replicated_storage).

-export([start_link/3,
         start_link_event_manager/1,
         start_replicator/1,
         replicator_name/1,
         subscribe_link/2,
         update_doc/2,
         foreach_doc/3,
         reset_master_vbucket/1]).

-export([init/1, init_after_ack/1, handle_call/3, handle_cast/2,
         handle_info/2, get_id/1, find_doc/2, all_docs/2,
         get_revision/1, set_revision/2, is_deleted/1, save_docs/2,
         on_replicate_in/1, on_replicate_out/1]).

-include("ns_common.hrl").
-include("couch_db.hrl").
-include("pipes.hrl").

-record(state, {bucket :: bucket_name(),
                event_manager :: pid(),
                local_docs :: undefined | [#doc{}]
               }).

start_link(Bucket, Replicator, ReplicationSrv) ->
    replicated_storage:start_link(
      server(Bucket), ?MODULE, [Bucket, Replicator, ReplicationSrv], Replicator).

start_link_event_manager(Bucket) ->
    gen_event:start_link({local, event_manager(Bucket)}).


replicator_name(Bucket) ->
    list_to_atom("capi_doc_replicator-" ++ Bucket).

start_replicator(Bucket) ->
    ns_bucket_sup:ignore_if_not_couchbase_bucket(
      Bucket,
      %% ignoring BucketConfig passed here, since it might become
      %% stale in the future
      fun (_) ->
              GetRemoteNodes =
                  fun () ->
                          ViewNodes =
                              case ns_bucket:get_bucket(Bucket) of
                                  {ok, BucketConfig} ->
                                      ns_bucket:get_view_nodes(BucketConfig);
                                  not_present ->
                                      []
                              end,
                          case ViewNodes of
                              [] ->
                                  [];
                              _ ->
                                  LiveOtherNodes =
                                      ns_node_disco:nodes_actual_other(),
                                  ordsets:intersection(LiveOtherNodes,
                                                       ViewNodes)
                          end
                  end,
              doc_replicator:start_link(
                replicator_name(Bucket), GetRemoteNodes,
                doc_replication_srv:proxy_server_name(Bucket))
      end).

subscribe_link(Bucket, Body) ->
    Self = self(),
    Ref = make_ref(),

    %% we only expect to be called by capi_set_view_manager that doesn't trap
    %% exits
    {trap_exit, false} = erlang:process_info(self(), trap_exit),

    Pid = ns_pubsub:subscribe_link(
            event_manager(Bucket),
            fun (Event, false) ->
                    case Event of
                        {snapshot, Docs} ->
                            Self ! {Ref, Docs},
                            true;
                        _ ->
                            %% we haven't seen snapshot yet; so we ignore
                            %% spurious notifications
                            false
                    end;
                (Event, true) ->
                    case Event of
                        {snapshot, _} ->
                            error(unexpected_snapshot);
                        _ ->
                            Body(Event)
                    end,
                    true
            end, false),
    gen_server:cast(server(Bucket), request_snapshot),

    receive
        {Ref, Docs} ->
            {Pid, Docs}
    end.

-spec foreach_doc(ext_bucket_name(),
                  fun ((#doc{}) -> any()),
                  non_neg_integer() | infinity) -> [{binary(), any()}].
foreach_doc(Bucket, Fun, Timeout) ->
    gen_server:call(server(Bucket), {foreach_doc, Fun}, Timeout).

update_doc(Bucket, Doc) ->
    gen_server:call(server(Bucket), {interactive_update, Doc}, infinity).

reset_master_vbucket(Bucket) ->
    gen_server:call(server(Bucket), reset_master_vbucket, infinity).

%% replicated_storage callbacks

init([Bucket, Replicator, ReplicationSrv]) ->
    replicated_storage:announce_startup(Replicator),
    replicated_storage:announce_startup(ReplicationSrv),

    EventManager = whereis(event_manager(Bucket)),
    true = is_pid(EventManager) andalso is_process_alive(EventManager),

    chronicle_compat_events:notify_if_key_changes(
      fun ({node, _, membership}) ->
              true;
          ({node, _, services}) ->
              true;
          (Key) ->
              ns_bucket:buckets_change(Key)
      end, replicate_newnodes_docs),

    #state{bucket = Bucket,
           event_manager = EventManager}.

init_after_ack(#state{bucket = Bucket} = State) ->
    ok = misc:wait_for_local_name(couch_server, 10000),

    Docs = load_local_docs(Bucket),
    State#state{local_docs = Docs}.

get_id(#doc{id = Id}) ->
    Id.

find_doc(Id, #state{local_docs = Docs}) ->
    lists:keyfind(Id, #doc.id, Docs).

all_docs(Pid, _) ->
    ?make_producer(?yield(gen_server:call(Pid, get_all_docs, infinity))).

get_revision(#doc{rev = Rev}) ->
    Rev.

set_revision(Doc, NewRev) ->
    Doc#doc{rev = NewRev}.

is_deleted(#doc{deleted = Deleted}) ->
    Deleted.

save_docs([NewDoc], State) ->
    try
        {ok, do_save_doc(NewDoc, State)}
    catch throw:{invalid_design_doc, _} = Error ->
            ?log_debug("Document validation failed: ~p", [Error]),
            {error, Error}
    end.

on_replicate_in(Docs) -> Docs.
on_replicate_out(Docs) -> Docs.

handle_call({foreach_doc, Fun}, _From, #state{local_docs = Docs} = State) ->
    Res = [{Id, Fun(Doc)} || #doc{id = Id} = Doc <- Docs],
    {reply, Res, State};
handle_call(reset_master_vbucket, _From, #state{bucket = Bucket,
                                                local_docs = LocalDocs} = State) ->
    MasterVBucket = master_vbucket(Bucket),
    ok = couch_server:delete(MasterVBucket, []),

    %% recreate the master db (for the case when there're no design documents)
    {ok, MasterDB} = open_local_db(Bucket),
    ok = couch_db:close(MasterDB),

    [do_save_doc(Doc, State) || Doc <- LocalDocs],
    {reply, ok, State};
handle_call(get_all_docs, _From, #state{local_docs = Docs} = State) ->
    {reply, [{Doc#doc.id, Doc} || Doc <- Docs], State}.

handle_cast(request_snapshot,
            #state{event_manager = EventManager,
                   local_docs = Docs} = State) ->
    gen_event:notify(EventManager, {snapshot, Docs}),
    {noreply, State}.

handle_info(Info, State) ->
    ?log_info("Ignoring unexpected message: ~p", [Info]),
    {noreply, State}.

%% internal
server(Bucket) when is_binary(Bucket) ->
    server(binary_to_list(Bucket));
server(Bucket) when is_list(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

event_manager(Bucket) ->
    list_to_atom("capi_ddoc_manager_events-" ++ Bucket).

master_vbucket(Bucket) ->
    iolist_to_binary([Bucket, <<"/master">>]).

open_local_db(Bucket) ->
    MasterVBucket = master_vbucket(Bucket),
    case couch_db:open(MasterVBucket, []) of
        {ok, Db} ->
            {ok, Db};
        {not_found, _} ->
            couch_db:create(MasterVBucket, [])
    end.

load_local_docs(Bucket) ->
    {ok, Db} = open_local_db(Bucket),
    try
        {ok, Docs} = couch_db:get_design_docs(Db, deleted_also),
        Docs
    after
        ok = couch_db:close(Db)
    end.

do_save_doc(#doc{id = Id} = Doc,
            #state{bucket = Bucket,
                   event_manager = EventManager,
                   local_docs = Docs} = State) ->

    Ref = make_ref(),
    gen_event:sync_notify(EventManager, {suspend, Ref}),

    try
        do_save_doc_with_bucket(Doc, Bucket),
        gen_event:sync_notify(EventManager, {resume, Ref, {ok, Doc}})
    catch
        T:E:S ->
            ?log_debug("Saving of document ~p for bucket ~p failed with ~p:~p~nStack trace: ~p",
                       [Id, Bucket, T, E, S]),
            gen_event:sync_notify(EventManager, {resume, Ref, {error, Doc, E}}),
            throw(E)
    end,
    State#state{local_docs = lists:keystore(Id, #doc.id, Docs, Doc)}.

do_save_doc_with_bucket(Doc, Bucket) ->
    {ok, Db} = open_local_db(Bucket),
    try
        ok = couch_db:update_doc(Db, Doc)
    after
        ok = couch_db:close(Db)
    end.
