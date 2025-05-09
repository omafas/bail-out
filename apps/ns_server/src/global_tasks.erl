%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(global_tasks).

-behaviour(gen_server2).

-include("global_tasks.hrl").
-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/0, handle_info/2]).
-export([get_status_uri/1, get_tasks/1, get_default_tasks/0, update_task/1,
         update_task/2,  update_tasks/1, update_tasks/2]).
-export([task_id/1, type/1, status/1, source_node/1, timestamp/1, extras/1]).

-define(SERVER, ?MODULE).

%% 60 seconds between cleanups
-define(CLEANUP_INTERVAL_MS, ?get_timeout(cleanup_interval_ms, 60000)).
%% 10 minutes before cleaning up a task without a defined expiry period
-define(DEFAULT_EXPIRY_PERIOD_S, ?get_timeout(default_expiry_period_s, 600)).

%% Task fields which cannot be updated
-define(IMMUTABLE_KEYS, [task_id, type, source_node, timestamp]).

%% The json formatted tasks should be safe to encode with ejson, without risk
%% of errors or mis-encoding a string as a list
-type(task_json() :: [{atom(), binary() | atom()}]).


-record(state, {}).

-spec get_status_uri(binary()) -> binary().
get_status_uri(TaskId) ->
    list_to_binary("/pools/default/tasks?taskId=" ++ binary_to_list(TaskId)).

%% The default tasks list which will be retrieved when no filter is provided
%% As we add tasks from ns_heart, we will need to update this filter to keep
%% the /pools/default/tasks backwards compatible
-spec get_default_tasks() -> [task_json()].
get_default_tasks() ->
    get_tasks(is_default_task(_)).

%% Get the list of tasks, either filtering by task id or by an arbitrary filter
%% If chronicle can't find the tasks list, returns []
-spec get_tasks([binary()] | fun ((task()) -> boolean())) ->
          [task_json()] | {error, any()}.
get_tasks(TaskIds) when is_list(TaskIds) ->
    get_tasks(fun (Task) ->
                      lists:member(task_id(Task), TaskIds)
              end);
get_tasks(Filter) when is_function(Filter, 1) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            case chronicle_compat:get(tasks, #{}) of
                {ok, Tasks} ->
                    lists:map(format_task(_), lists:filter(Filter, Tasks));
                {error, not_found} ->
                    ?log_warning("Missing tasks list. Assuming no tasks exist"),
                    [];
                {error, Reason} = Error ->
                    ?log_error("Failed to get tasks list. Error: ~p",
                               [Reason]),
                    Error
            end;
        false ->
            []
    end.


%% update_task/1 creates (or replaces) a task status for a unique task id,
%% taking a #global_task{} tuple which has the following fields:
%%
%% - task_id is a unique binary string to identify the task
%% - type is the category that the task falls in. The type should not change for
%%   statuses of a specific task_id
%% - status denotes the current state of the task
%% - extras should be a list of {atom(), binary()} (in order to be consistently
%%   converted to json when returned by /pools/default/tasks?taskId={task_id})
%%   extras should only be used when absolutely necessary, and when we know it
%%   will have a bounded size, as we wish to avoid large chronicle values
-spec update_task(#global_task{}) -> ok.
update_task(Task) ->
    update_tasks([Task]).

%% update_task/2 takes a task id and a function to update the task with that id.
%% If no task exists with that id, an error will be logged and no change will be
%% made to any tasks
-spec update_task(binary(), fun ((task()) -> task())) -> ok.
update_task(TaskId, UpdateTask) ->
    update_tasks([TaskId], UpdateTask).

%% update_tasks/1 takes a list of #global_task{} tuples and creates (or updates)
%% the task for each tuple, as for update_task/1
-spec update_tasks([#global_task{}]) -> ok.
update_tasks(Tasks) ->
    UpdateTasks = functools:chain(_, [replace_task(_, build_task(Task))
                                      || Task <- Tasks]),
    do_update_tasks(UpdateTasks).

%% update_tasks/2 takes a list of task ids and an update function, which is
%% called on each task corresponding to one of the task ids provided. If no task
%% exists with any of the ids, an error will be logged but other tasks may still
%% be updated. If the function updates any immutable fields (?IMMUTABLE_KEYS)
%% of the task, these changes will be overriden by the original values
-spec update_tasks([binary()], fun ((extras()) -> extras())) -> ok.
update_tasks(TaskIds, UpdateTask) ->
    UpdateTasks = functools:chain(_, [do_update_task(TaskId, UpdateTask, _)
                                      || TaskId <- TaskIds]),
    do_update_tasks(UpdateTasks).

%% Call a function on the tasks list, in a chronicle transaction
-spec do_update_tasks(fun (([task()]) -> [task()])) -> ok.
do_update_tasks(UpdateTasks) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            Result =
                chronicle_compat:transaction(
                  [tasks],
                  fun (Snapshot) ->
                          OldTasks = chronicle_compat:get(Snapshot, tasks,
                                                          #{required => true}),
                          FinalTasks = UpdateTasks(OldTasks),
                          {commit, [{set, tasks, FinalTasks}]}
                  end),
            case Result of
                {ok, _} ->
                    ok;
                Error ->
                    ?log_error("Failed to update tasks. Error: ~p",
                               [Error]),
                    erlang:throw(Error)
            end;
        false ->
            ok
    end.

%% Call an update function on a task specified by its task id, from the list of
%% tasks. Gives an error if the task id cannot be found
-spec do_update_task(binary(), fun ((task()) -> task()), [task()]) -> [task()].
do_update_task(TaskId, UpdateTask, Tasks) ->
    ExistingTasks = lists:filter(
                      fun (Task) ->
                              task_id(Task) =:= TaskId
                      end, Tasks),
    case ExistingTasks of
        [OldTask] ->
            replace_task(Tasks, modify_task(UpdateTask, OldTask));
        [] ->
            ?log_error("Failed to update tasks. No existing task with task id: "
                       "~p", [TaskId]),
            Tasks
    end.


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    proc_lib:start_link(?MODULE, init, []).

init() ->
    register(?SERVER, self()),
    proc_lib:init_ack({ok, self()}),
    initialise_tasks(),
    erlang:send_after(?CLEANUP_INTERVAL_MS, self(), cleanup),
    gen_server:enter_loop(?MODULE, [], #state{}).


handle_info(cleanup, State) ->
    cleanup_tasks(),
    erlang:send_after(?CLEANUP_INTERVAL_MS, self(), cleanup),
    {noreply, State};

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

initialise_tasks() ->
    Result = chronicle_compat:transaction(
               [tasks],
               fun (Snapshot) ->
                       case chronicle_compat:get(Snapshot, tasks, #{}) of
                           {ok, _} ->
                               %% Tasks list already exists
                               {commit, []};
                           {error, not_found} ->
                               %% Create an empty tasks list
                               {commit, [{set, tasks, []}]};
                           {error, _} = Error ->
                               {abort, Error}
                       end
               end),
    case Result of
        {ok, _} ->
            ok;
        Error ->
            ?log_error("Failed to initialise global tasks. Error: ~p", [Error]),
            exit(Error)
    end.

task_id(Task) ->
    proplists:get_value(task_id, Task).

type(Task) ->
    proplists:get_value(type, Task).

status(Task) ->
    proplists:get_value(status, Task).

source_node(Task) ->
    proplists:get_value(source_node, Task).

timestamp(Task) ->
    proplists:get_value(timestamp, Task).

extras(Task) ->
    proplists:get_value(extras, Task).

-spec format_task(task()) -> task_json().
format_task(Task) ->
    Type = type(Task),
    [{task_id, task_id(Task)},
     {status, status(Task)},
     {type, Type}] ++
        format_task_extras(Type, extras(Task)).

%% Format an task type specific fields appropriately. The result of this gets
%% appended to the default task fields
-spec format_task_extras(task_type(), extras()) -> task_json().
format_task_extras(loadingSampleBucket, Extras) ->
    case proplists:get_value(bucket, Extras) of
        undefined ->
            [];
        Bucket ->
            [{bucket, list_to_atom(Bucket)},
             {bucket_uuid, proplists:get_value(bucket_uuid, Extras)}]
    end.

-spec is_default_task(task()) -> boolean().
is_default_task(Task) ->
    case {type(Task), status(Task)} of
        {loadingSampleBucket, running} -> true;
        _ -> false
    end.

%% Construct the internal representation of a new task
-spec build_task(#global_task{}) -> task().
build_task(#global_task{task_id = TaskId,
                        type = Type,
                        status = Status,
                        extras = Extras}) ->
    [{task_id, TaskId},
     {type, Type},
     {status, Status},
     {source_node, node()},
     {timestamp, now_secs()},
     {extras, Extras}].

%% Update the internal representation of an existing task, without allowing
%% modification of the immutable keys
-spec modify_task(fun ((extras()) -> extras()), task()) -> task().
modify_task(UpdateTask, OldTask) ->
    %% Get the properties that cannot be updated
    Immutables = lists:map(fun (Key) ->
                                   {Key, proplists:get_value(Key, OldTask)}
                           end, ?IMMUTABLE_KEYS),
    NewTask = UpdateTask(OldTask),
    lists:foldl(fun ({Key, _Value} = Tuple, Task) ->
                        lists:keyreplace(Key, 1, Task, Tuple)
                end, NewTask, Immutables).

-spec replace_task([task()], extras()) -> [task()].
replace_task(Tasks, NewTask) ->
    [NewTask | lists:filter(
                 fun (Task) ->
                         task_id(Task) =/= task_id(NewTask)
                 end, Tasks)].

%% Expiry period of a task status in seconds
-spec get_expiry_period(task()) -> integer().
get_expiry_period(Task) ->
    ConfigKey = {type(Task), status(Task)},
    ExpiryTime =
        case ConfigKey of
            {loadingSampleBucket, queued} ->
                %% 1 hour
                3600;
            {loadingSampleBucket, running} ->
                %% 1 hour
                3600;
            {loadingSampleBucket, completed} ->
                %% 10 minutes
                600;
            {loadingSampleBucket, failed} ->
                %% 10 minutes
                600;
            _ ->
                ?DEFAULT_EXPIRY_PERIOD_S
        end,
    %% All expiry periods should be configurable at run time, including those
    %% using the default expiry period
    ?get_timeout(ConfigKey, ExpiryTime).

cleanup_tasks() ->
    BucketUUIDKeys = lists:map(ns_bucket:sub_key(_, uuid),
                               ns_bucket:get_bucket_names()),
    %% We check and update in a transaction to avoid a task being updated
    %% after being added to a list of tasks to delete, but before the
    %% deletion has occurred, causing the deletion of the updated task
    Result = chronicle_compat:transaction([tasks | BucketUUIDKeys],
                                          do_cleanup(_)),
    case Result of
        {ok, _} ->
            ok;
        Error ->
            ?log_error("Failed to cleanup global tasks. Error: ~p", [Error])
    end.

-spec do_cleanup(map()) ->
          {commit, [{set, tasks, [task()]}]} | {abort, {error, any()}}.
do_cleanup(Snapshot) ->
    case chronicle_compat:get(Snapshot, tasks, #{}) of
        {ok, Tasks} ->
            NewTasks = lists:filter(should_keep_task(_), Tasks),
            case NewTasks =:= Tasks of
                true ->
                    {commit, []};
                false ->
                    {commit, [{set, tasks, NewTasks}]}
            end;
        {error, _} = Error ->
            {abort, Error}
    end.

-spec should_keep_task(task()) -> boolean().
should_keep_task(Task) ->
    case functools:alternative(Task, cleanup_checks()) of
        {ok, Reason} ->
            ?log_debug("Cleaning up task (~s):~n~p", [Reason, Task]),
            false;
        false ->
            true
    end.

%% Checks for cleaning up tasks that are no longer relevant
cleanup_checks() ->
    [task_expired(_),
     node_missing(_)].

-spec task_expired(task()) -> false | {ok, binary()}.
task_expired(Task) ->
    ExpiryTime = timestamp(Task) + get_expiry_period(Task),
    case now_secs() > ExpiryTime of
        true ->
            {ok, <<"expired">>};
        false ->
            false
    end.

-spec node_missing(task()) -> false | {ok, binary()}.
node_missing(Task) ->
    Node = source_node(Task),
    case lists:member(Node, ns_cluster_membership:nodes_wanted()) of
        true ->
            false;
        false ->
            {ok, <<"node missing">>}
    end.

-spec now_secs() -> integer().
now_secs() ->
    erlang:system_time(second).

-ifdef(TEST).
modules() ->
    [chronicle_compat, ns_cluster_membership, cluster_compat_mode, ns_config].

setup() ->
    meck:new(modules(), [passthrough]),
    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> true end).

teardown(_) ->
    meck:unload(modules()).

-define(EXTRAS, [[], [{test, <<"test">>}]]).
-define(UUID, <<"test">>).
-define(SNAPSHOT, #{{bucket, "default", uuid} => {?UUID, 0}}).

%% Generate tasks of all possible type and status, and for each of those
%% configurations, also generate one with/without a bucket, and with/without a
%% key in the extras field.
%% Doesn't generate specific values for task_id, timestamp, or source_node,
%% which are determined as normal
-spec generate_task_creates() -> [#global_task{}].
generate_task_creates() ->
    lists:append(
      [lists:append(
         [[#global_task{task_id = misc:uuid_v4(),
                        type = Type,
                        status = Status,
                        extras = Extras}
           || Type <- ?TYPES]
          || Status <- ?STATUSES])
       || Extras <- ?EXTRAS]).

-spec generate_tasks() -> [task()].
generate_tasks() ->
    lists:map(build_task(_), generate_task_creates()).

%% Generate a cleanup candidate for each task, for each possible cleanup reason
-spec generate_all_expired_tasks([task()]) -> [task()].
generate_all_expired_tasks(Tasks) ->
    lists:append(lists:map(generate_expired_tasks(_), Tasks)).

%% Generate cleanup candidates for the task, for each possible cleanup reason
-spec generate_expired_tasks(task()) -> [task()].
generate_expired_tasks(Task) ->
    [%% time based expiry. Assumes that time won't jump 1 second back between
     %% now and the cleanup
     lists:keyreplace(timestamp, 1, Task,
                      {timestamp, now_secs() - get_expiry_period(Task) - 1000}),

     %% node missing
     lists:keyreplace(source_node, 1, Task, {source_node, other_node})
    ].

%% Since lists would get truncated in the eunit output if we called ?assertEqual
%% opn the lists themselves, we instead have to assert that each pair of terms
%% in the lists are equal
-spec assert_lists_equal(list(), list()) -> ok.
assert_lists_equal(ExpectedList, ActualList) ->
    lists:foreach(fun ({Expected, Actual}) ->
                          ?assertEqual(Expected, Actual)
                  end,
                  lists:zip(lists:sort(ExpectedList), lists:sort(ActualList))).

cleanup_test__() ->
    meck:expect(chronicle_compat, get_snapshot,
                fun (_, _) ->
                        #{}
                end),
    meck:expect(ns_cluster_membership, nodes_wanted,
                fun () ->
                        [node()]
                end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) ->
                        Default
                end),
    TasksToKeep = generate_tasks(),
    TasksToRemove = generate_all_expired_tasks(TasksToKeep),
    Snapshot = ?SNAPSHOT#{tasks => {TasksToRemove ++ TasksToKeep, 0}},
    {commit, [{set, tasks, NewTasks}]} = do_cleanup(Snapshot),
    assert_lists_equal(TasksToKeep, NewTasks).

get_tasks_test__() ->
    Tasks = generate_tasks(),

    meck:expect(chronicle_compat, get,
                fun (_, _) ->
                        {ok, Tasks}
                end),

    %% Fetch all tasks, by task_id
    TaskIds = lists:map(task_id(_), Tasks),
    FetchedTasks = get_tasks(TaskIds),

    %% The fetched tasks should be formatted, for correct json encoding by ejson
    ExpectedTasks = lists:map(fun (Task) -> format_task(Task) end, Tasks),
    assert_lists_equal(ExpectedTasks, FetchedTasks),

    %% The list of tasks will be converted as follows in menelaus_web_misc, in
    %% order for ejson to encode the whole list together
    JSONTasks = [{Task} || Task <- FetchedTasks],

    %% Confirm that the final tasks list can be converted to json
    ejson:encode(JSONTasks),

    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> false end),

    %% Confirm that get_tasks/1 returns [] for mixed version clusters
    ?assertEqual([], get_tasks(TaskIds)).

get_default_tasks_test__() ->
    Tasks = generate_tasks(),
    %% For now we only care about running tasks. As we move more tasks out from
    %% the heartbeat, we will need to update this test to match the previous
    %% behaviour
    DefaultTasks = lists:filter(
                     fun (Task) ->
                             status(Task) =:= running
                     end, Tasks),

    meck:expect(chronicle_compat, get,
                fun (_, _) ->
                        {ok, Tasks}
                end),

    %% Fetch the default list of tasks
    FetchedTasks = get_default_tasks(),

    %% The fetched tasks should be formatted, for correct json encoding by ejson
    ExpectedTasks = lists:map(fun (Task) -> format_task(Task) end, DefaultTasks),
    assert_lists_equal(ExpectedTasks, FetchedTasks),

    %% The list of tasks will be converted as follows in menelaus_web_misc, in
    %% order for ejson to encode the whole list together
    JSONTasks = [{Task} || Task <- FetchedTasks],

    %% Confirm that the final tasks list can be converted to json
    ejson:encode(JSONTasks),

    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> false end),

    %% Confirm that get_default_tasks/0 returns [] for mixed version clusters
    ?assertEqual([], get_default_tasks()).

%% Generate a new task which is the same as Task, except for the status
-spec generate_task_update(task()) -> task().
generate_task_update(Task) ->
    OldStatus = status(Task),
    %% Generate a new, different status
    Status = hd(?STATUSES -- [OldStatus]),
    lists:keyreplace(status, 1, Task, {status, Status}).

%% Instead of somehow starting up chronicle in the unit test, we replace the
%% transaction call with one which checks that each new Task gets added to the
%% tasks list.
-spec assert_create_tasks_individually([#global_task{}], _) -> ok.
assert_create_tasks_individually(Tasks, Transaction) ->
    meck:expect(chronicle_compat, transaction, Transaction),
    Return =
        lists:foreach(
          fun (Task) ->
                  update_task(Task)
          end, Tasks),
    ?assertEqual(ok, Return).

-spec assert_create_tasks_together([#global_task{}], _) -> ok.
assert_create_tasks_together(Tasks, Transaction) ->
    meck:expect(chronicle_compat, transaction, Transaction),
    Return = update_tasks(Tasks),
    ?assertEqual(ok, Return).

-spec assert_update_tasks_individually([binary()], _, _) -> ok.
assert_update_tasks_individually(TaskIds, Update, Transaction) ->
    meck:expect(chronicle_compat, transaction, Transaction),
    Return =
        lists:foreach(
          fun (TaskId) ->
                  update_task(TaskId, Update)
          end, TaskIds),
    ?assertEqual(ok, Return).

-spec assert_update_tasks_together([binary()], _, _) -> ok.
assert_update_tasks_together(TaskIds, Update, Transaction) ->
    meck:expect(chronicle_compat, transaction, Transaction),
    Return = update_tasks(TaskIds, Update),
    ?assertEqual(ok, Return).

-spec assert_task(#global_task{} | task(), task()) -> ok.
assert_task(#global_task{task_id = TaskId,
                         type = Type,
                         extras = Extras},
            Task) ->
    ?assertEqual(TaskId, task_id(Task)),
    ?assertEqual(Type, type(Task)),
    ?assertEqual(Extras, extras(Task));
assert_task(TaskExp, TaskActual) ->
    ?assertEqual(task_id(TaskExp), task_id(TaskActual)),
    ?assertEqual(type(TaskExp), type(TaskActual)),
    ?assertEqual(extras(TaskExp), extras(TaskActual)).


%% Pretend to be a chronicle transaction, and check that the modified task is
%% modified as expected
-spec fake_transaction(map(), [#global_task{}],
                       _, fun((map()) -> {commit, list()})) ->
          {ok, any()}.
fake_transaction(Snapshot0, ExpectedTasks, Keys, Fun) ->
    Snapshot1 = maps:filter(fun (Key, _) ->
                                    lists:member(Key, Keys)
                            end, Snapshot0),
    case Fun(Snapshot1) of
        {commit, [{set, tasks, [NewTask | _]}]} ->
            TaskId = task_id(NewTask),
            [ExpectedTask] =
                lists:filter(
                  fun (#global_task{task_id = ExpTaskId}) ->
                          TaskId =:= ExpTaskId;
                      (Task) ->
                          TaskId =:= task_id(Task)
                  end, ExpectedTasks),
            assert_task(ExpectedTask, NewTask),
            {ok, 0};
        Action ->
            ?assert(false, {unexpected_action, Action})
    end.

update_task_test__() ->
    Snapshot0 = ?SNAPSHOT#{tasks => {[], 0}},
    Tasks = generate_task_creates(),
    TaskIds = [Task#global_task.task_id || Task <- Tasks],

    %% Test that new tasks can be added individually
    Transaction0 = fake_transaction(Snapshot0, Tasks, _, _),
    assert_create_tasks_individually(Tasks, Transaction0),
    assert_create_tasks_together(Tasks, Transaction0),

    BuiltTasks = lists:map(build_task(_), Tasks),

    %% Generate an updated task with a new status, for each existing task
    Snapshot1 = Snapshot0#{tasks => {BuiltTasks, 0}},

    %% Generate an updated task with a new status, for each existing task
    TaskUpdates = [generate_task_update(Task) || Task <- BuiltTasks],

    %% Test that current tasks can be updated individually
    Transaction1 = fake_transaction(Snapshot1, TaskUpdates, _, _),
    assert_update_tasks_individually(TaskIds, generate_task_update(_),
                                     Transaction1),

    %% Test that current tasks can be updated all at once
    assert_update_tasks_together(TaskIds, generate_task_update(_),
                                 Transaction1),

    meck:expect(cluster_compat_mode, is_cluster_76, fun () -> false end),

    %% Confirm that update_task/1 returns ok for mixed version clusters.
    %% The transaction shouldn't run
    TransactionError = fun (_, _) -> error end,
    assert_create_tasks_individually(Tasks, TransactionError),

    %% Confirm that update_task/2 returns ok for mixed version clusters
    assert_update_tasks_individually(TaskIds, TaskUpdates, TransactionError),

    %% Confirm that update_tasks/1 returns ok for mixed version clusters
    assert_create_tasks_together(Tasks, TransactionError),

    %% Confirm that update_tasks/2 returns ok for mixed version clusters
    assert_update_tasks_together(TaskIds, TaskUpdates, TransactionError).

all_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [fun cleanup_test__/0,
      fun get_tasks_test__/0,
      fun get_default_tasks_test__/0,
      fun update_task_test__/0]}.
-endif.
