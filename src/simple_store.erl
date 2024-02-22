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

%% APIs

-export([start_link/1,
         get/2, get/3,
         set/3,
         delete/2,
         delete_matching/2,
         iterate_matching/2]).

%% Macros

%% Persist the ETS table to file after 10 secs.
%% All updates to the table during that window will automatically get batched
%% and flushed to the file together.
-define(FLUSH_AFTER, 10 * 1000).

%% Max number of unsucessful flush attempts before giving up.
-define(FLUSH_RETRIES, 10).

%% Exported APIs

start_link(StoreName) ->
    ProcName = get_proc_name(StoreName),
    work_queue:start_link(ProcName, fun () -> init(StoreName) end).

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

%% Internal
init(StoreName) ->
    %% Initialize flush_pending to false.
    erlang:put(flush_pending, false),

    %% Populate the table from the file if the file exists otherwise create
    %% an empty table.
    FilePath = path_config:component_path(data, get_file_name(StoreName)),
    Read =
        case filelib:is_regular(FilePath) of
            true ->
                ?metakv_debug("Reading ~p content from ~s", [StoreName, FilePath]),
                case ets:file2tab(FilePath, [{verify, true}]) of
                    {ok, StoreName} ->
                        true;
                    {error, Error} ->
                        ?metakv_debug("Failed to read ~p content from ~s: ~p",
                                      [StoreName, FilePath, Error]),
                        false
                end;
            false ->
                false
        end,

    case Read of
        true ->
            ok;
        false ->
            ?metakv_debug("Creating Table: ~p", [StoreName]),
            ets:new(StoreName, [named_table, set, protected]),
            ok
    end.

do_work(StoreName, Fun, Args) ->
    work_queue:submit_sync_work(
      get_proc_name(StoreName),
      fun () ->
              Fun(StoreName, Args)
      end).

%% Update the ETS table and schedule a flush to the file.
update_store(StoreName, [Key, Value]) ->
    ?metakv_debug("Updating data ~p in table ~p.", [[{Key, Value}], StoreName]),
    ets:insert(StoreName, [{Key, Value}]),
    schedule_flush(StoreName, ?FLUSH_RETRIES).

%% Delete from the ETS table and schedule a flush to the file.
delete_from_store(StoreName, [Key]) ->
    ?metakv_debug("Deleting key ~p in table ~p.", [Key, StoreName]),
    ets:delete(StoreName, Key),
    schedule_flush(StoreName, ?FLUSH_RETRIES).

del_matching(StoreName, [KeyPattern]) ->
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
    schedule_flush(StoreName, ?FLUSH_RETRIES).

%% Nothing can be done if we failed to flush repeatedly.
schedule_flush(StoreName, 0) ->
    ?metakv_debug("Tried to flush table ~p ~p times but failed. Giving up.",
                  [StoreName, ?FLUSH_RETRIES]),
    exit(flush_failed);

%% If flush is pending then nothing else to do otherwise schedule a
%% flush to the file for later.
schedule_flush(StoreName, NumRetries) ->
    case erlang:get(flush_pending) of
        true ->
            ?metakv_debug("Flush is already pending."),
            ok;
        false ->
            erlang:put(flush_pending, true),
            {ok, _} = timer:apply_after(?FLUSH_AFTER, work_queue, submit_work,
                                        [self(),
                                         fun () ->
                                                 flush_table(StoreName, NumRetries)
                                         end]),
            ?metakv_debug("Successfully scheduled a flush to the file."),
            ok
    end.

%% Flush the table to the file.
flush_table(StoreName, NumRetries) ->
    %% Reset flush pending.
    erlang:put(flush_pending, false),
    FilePath = path_config:component_path(data, get_file_name(StoreName)),
    ?metakv_debug("Persisting Table ~p to file ~p.", [StoreName, FilePath]),
    case ets:tab2file(StoreName, FilePath, [{extended_info, [object_count]}]) of
        ok ->
            ok;
        {error, Error} ->
            ?metakv_debug("Failed to persist table ~p to file ~p with error ~p.",
                          [StoreName, FilePath, Error]),
            %% Reschedule another flush.
            schedule_flush(StoreName, NumRetries - 1)
    end.

get_proc_name(StoreName) ->
    list_to_atom(get_file_name(StoreName)).

get_file_name(StoreName) ->
    atom_to_list(?MODULE) ++ "_" ++ atom_to_list(StoreName).
