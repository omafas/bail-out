%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2018 Couchbase, Inc.
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

-module(replicated_storage).

-behaviour(gen_server).

-export([start_link/4, start_link_remote/5, wait_for_startup/0,
         anounce_startup/1, sync_to_me/3]).

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-callback init(term()) -> term().
-callback init_after_ack(term()) -> term().
-callback get_id(term()) -> term().
-callback find_doc(term(), term()) -> term() | false.
-callback all_docs(pid()) -> term().
-callback get_revision(term()) -> term().
-callback set_revision(term(), term()) -> term().
-callback is_deleted(term()) -> boolean().
-callback save_docs([term()], term()) -> {ok, term()} | {error, term()}.

-include("ns_common.hrl").
-include("pipes.hrl").

-record(state, {child_module :: atom(),
                child_state :: term(),
                replicator :: pid()
               }).

start_link(Name, Module, InitParams, Replicator) ->
    proc_lib:start_link(?MODULE, init,
                        [[Name, Module, InitParams, Replicator]]).

start_link_remote(Node, Name, Module, InitParams, Replicator) ->
    misc:start_link(Node, misc, turn_into_gen_server,
                    [{local, Name}, ?MODULE,
                     [Module, InitParams, Replicator], []]).

wait_for_startup() ->
    ?log_debug("Start waiting for startup"),
    receive
        {replicated_storege_pid, Pid} ->
            ?log_debug("Received replicated storage registration from ~p", [Pid]),
            Pid;
        {'EXIT', ExitPid, Reason} ->
            ?log_debug("Received exit from ~p with reason ~p", [ExitPid, Reason]),
            exit(Reason)
    after 10000 ->
            ?log_error("Waited 10000 ms for replicated storage pid to no avail. Crash."),
            exit(replicated_storage_not_available)
    end.

anounce_startup(Pid) ->
    ?log_debug("Announce my startup to ~p", [Pid]),
    Pid ! {replicated_storege_pid, self()}.

sync_to_me(Name, Nodes, Timeout) ->
    gen_server:call(Name, {sync_to_me, Nodes, Timeout}, infinity).

init([Name, Module, InitParams, Replicator]) ->
    register(Name, self()),
    Self = self(),
    ChildState1 = Module:init(InitParams),
    Self ! replicate_newnodes_docs,

    proc_lib:init_ack({ok, Self}),

    ChildState2 = Module:init_after_ack(ChildState1),
    gen_server:enter_loop(?MODULE, [],
                          #state{child_module = Module,
                                 child_state = ChildState2,
                                 replicator = Replicator}).

handle_call({interactive_update, Doc}, _From,
            #state{child_module = Module,
                   child_state = ChildState,
                   replicator = Replicator} = State) ->
    Rand = misc:rand_uniform(0, 16#100000000),
    RandBin = <<Rand:32/integer>>,
    {NewRev, FoundType} =
        case Module:find_doc(Module:get_id(Doc), ChildState) of
            false ->
                {{1, RandBin}, missing};
            ExistingDoc ->
                {Pos, _DiskRev} = Module:get_revision(ExistingDoc),
                Deleted = Module:is_deleted(ExistingDoc),
                FoundType0 = case Deleted of
                                 true ->
                                     deleted;
                                 false ->
                                     existent
                             end,
                {{Pos + 1, RandBin}, FoundType0}
        end,

    case Module:is_deleted(Doc) andalso FoundType =/= existent of
        true ->
            {reply, {not_found, FoundType}, State};
        false ->
            NewDoc = Module:set_revision(Doc, NewRev),
            ?log_debug("Writing interactively saved doc ~p",
                       [ns_config_log:sanitize(NewDoc, true)]),
            case Module:save_docs([NewDoc], ChildState) of
                {ok, NewChildState} ->
                    Replicator ! {replicate_change, Module:get_id(NewDoc),
                                  Module:on_replicate_out(NewDoc)},
                    {reply, ok, State#state{child_state = NewChildState}};
                {error, Error} ->
                    {reply, Error, State}
            end
    end;
handle_call({mass_update, Context}, From, #state{child_module = Module,
                                                 child_state = ChildState} = State) ->
    Updater =
        ?make_consumer(
           pipes:fold(
             ?producer(),
             fun (Doc, {Errors, St}) ->
                     {reply, RV, NewSt} =
                         handle_call({interactive_update, Doc}, From, St),
                     {case RV of
                          ok ->
                              Errors;
                          Error ->
                              [{Doc, Error} | Errors]
                      end, NewSt}
             end, {[], State})),
    {RV1, NewState} =
        Module:handle_mass_update(Context, Updater, ChildState),
    {reply, RV1, NewState};
handle_call(sync_token, From, #state{replicator = Replicator} = State) ->
    ?log_debug("Received sync_token from ~p", [From]),
    Replicator ! {sync_token, From},
    {noreply, State};
handle_call({sync_to_me, Nodes, Timeout}, From,
            #state{replicator = Replicator} = State) ->
    ?log_debug("Received sync_to_me with timeout = ~p, nodes = ~p",
               [Timeout, Nodes]),
    proc_lib:spawn_link(
      fun () ->
              Res = gen_server:call(Replicator, {sync_to_me, Nodes, Timeout},
                                    infinity),
              ?log_debug("sync_to_me reply: ~p", [Res]),
              gen_server:reply(From, Res)
      end),
    {noreply, State};
handle_call(Msg, From, #state{child_module = Module, child_state = ChildState} = State) ->
    case Module:handle_call(Msg, From, ChildState) of
        {reply, Res, NewChildState} ->
            {reply, Res, State#state{child_state = NewChildState}};
        {noreply, NewChildState} ->
            {noreply, State#state{child_state = NewChildState}}
    end.

handle_cast({replicated_batch, CompressedBatch}, State) ->
    ?log_debug("Applying replicated batch. Size: ~p", [size(CompressedBatch)]),
    Batch = misc:decompress(CompressedBatch),
    true = is_list(Batch) andalso Batch =/= [],
    {noreply, handle_replication_update(Batch, false, State)};
handle_cast({replicated_update, Doc}, State) ->
    {noreply, handle_replication_update([Doc], true, State)};

handle_cast(Msg, #state{child_module = Module, child_state = ChildState} = State) ->
    {noreply, NewChildState} = Module:handle_cast(Msg, ChildState),
    {noreply, State#state{child_state = NewChildState}}.

handle_info(replicate_newnodes_docs, #state{child_module = Module,
                                            replicator = Replicator} = State) ->
    Producer =
        pipes:compose(
          Module:all_docs(self()),
          pipes:map(
            fun ({batch, Docs}) ->
                    ToReplicate = [Module:on_replicate_out(Doc) || Doc <- Docs],
                    {batch, ToReplicate};
                (DocsWithIds) ->
                    [{Id, Module:on_replicate_out(Doc)} ||
                        {Id, Doc} <- DocsWithIds]
            end)),
    Replicator ! {replicate_newnodes_docs, Producer},
    {noreply, State};
handle_info(Msg, #state{child_module = Module, child_state = ChildState} = State) ->
    {noreply, NewChildState} = Module:handle_info(Msg, ChildState),
    {noreply, State#state{child_state = NewChildState}}.

terminate(_Reason, _State) ->
    ok.

handle_replication_update(Docs, NeedLog,
                          #state{child_module = Module,
                                 child_state = ChildState} = State) ->
    DocsToWrite =
        lists:filtermap(
          fun (Doc) ->
                  Converted = Module:on_replicate_in(Doc),
                  case should_be_written(Converted, Module, ChildState) of
                      true ->
                          {true, Converted};
                      false ->
                          false
                  end
          end, Docs),
    [?log_debug("Writing replicated doc ~p", [ns_config_log:tag_user_data(D)])
        || NeedLog, D <- DocsToWrite],

    {ok, NewChildState} =
        case DocsToWrite of
            [] -> {ok, ChildState};
            _ -> Module:save_docs(DocsToWrite, ChildState)
        end,
    State#state{child_state = NewChildState}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

should_be_written(NewDoc, Module, ChildState) ->
    %% this is replicated from another node in the cluster. We only accept it
    %% if it doesn't exist or the rev is higher than what we have.
    NewRev = Module:get_revision(NewDoc),
    case Module:find_doc(Module:get_id(NewDoc), ChildState) of
        false -> true;
        OldDoc -> NewRev > Module:get_revision(OldDoc)
    end.
