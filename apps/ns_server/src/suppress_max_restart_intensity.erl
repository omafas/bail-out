%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

%% @doc
%% suppress_max_restart_intensity provides a mechanism to specify a supervision
%% hierarchy such that the child process restarting more than max_r
%% (intensity) times in max_t (period) time does not trigger a
%% max_restart_intensity shutdown, it instead continues to restart after some
%% specified number of seconds. Note that max_r (intensity) restarts are
%% still performed as quickly as possible, but the next (max_r + 1) is
%% delayed by the specified restart timeout.
%%
%% The supervision hierarchy implemented by suppress_max_restart_intensity is
%% as follows:
%%
%%                          Original Supervisor
%%                                   |
%%                 Avoid Max Restart Intensity Supervisor
%%                                   |
%%                          Supervisor Cushion
%%                                   |
%%                 Inherited MaxR and MaxT Supervisor [1]
%%                                   |
%%                              Child Proc
%%
%% [1] MaxR And MaxT values should be those of the Original Supervisor
%%
%% suppress_max_restart_intensity can be used by wrapping a, mostly standard,
%% supervisor:child_spec() in a call to suppress_max_restart_intensity:spec(),
%% as follows:
%%
%%     suppress_max_restart_intensity:spec(ChildSpec)
%%
%% Before considering the differences between the child_spec() that
%% suppress_max_restart_intensity take, and that of supervisor:child_spec(), it
%% is first worth mentioning that supervisor:child_spec() supports two
%% different types of specifications, a tuple, and a map. Whilst the map
%% specification is now the preferred choice (per the Erlang docs), the tuple
%% specification is still supported. As such, suppress_max_restart_intensity
%% supports both map and tuple type child specifications.
%%
%% The child_spec() supported by suppress_max_restart_intensity differs with
%% respect to the restart parameter. It differs in two regards:
%%
%% 1) The restart parameter of supervisor:child_spec() supports the values
%%    'permanent', 'transient', and 'temporary'.
%%    suppress_max_restart_intensity supports only the 'permanent' restart
%%    type. The 'temporary' restart type does not make sense in
%%    suppress_max_restart_intensity as the child is never restarted. The
%%    'transient' restart type could fit into suppress_max_restart_intensity,
%%    but is not trivially supportable as max_restart_intensity returns a
%%    shutdown status that causes a transient child to not be restarted at
%%    all. As such, only a 'permanent' restart type is currently supported by
%%    suppress_max_restart_intensity.
%%
%% 2) suppress_max_restart_intensity requires and supports additional
%%    parameters specifies as part of the restart parameter. These parameters
%%    are as follows:
%%
%%      a) delay - Governs how long is waited after hitting
%%                 max_restart_intensity before attempting to restart the child
%%                 process. This parameter is required.
%%
%%      b) inherited_max_r - The max_r value of the original supervisor.
%%                           Governs how many times we will attempt to restart
%%                           within inherited_max_t seconds before delaying the
%%                           next series of restarts. This parameter is
%%                           required.
%%
%%      c) inherited_max_t - The max_t value of the original supervisor. Governs
%%                           the time period in which we are allowed to restart
%%                           inherited_max_r times before delaying the next
%%                           series of restarts. This parameter is specified
%%                           in seconds. This parameter is required.
%%
%%    When a map child_spec() is specified, these parameters may be specified
%%    as keys within the child_spec(). When a tuple child_spec() is specified
%%    these parameters may be specified as follows:
%%
%%      a) {Id, Start, {permanent, Delay}, Shutdown, Type, Modules}
%%
%%      b) {Id, Start, {permanent, Delay, Inherited_max_r, Inherited_max_t},
%%          Shutdown, Type, Modules}
%%
%%
%% A note on child IDs and restartable.
%%
%% The child ID passed into suppress_max_restart_intensity is not used for
%% the first child in the supervision tree (avoid_max_restart_intensity_sup).
%% This is intentional to avoid callers attempting to get the Pid of the child
%% process from the child ID via the top level supervisor. Name registration
%% should generally be used instead. In the case of restartable(:spec()),
%% this means that one cannot pass the original child ID to
%% restartable:restart(). Two options exist to work with children requiring
%% both restartable and suppress_max_restart_intensity, depending on how they
%% are setup:
%%
%%     Option A:
%%
%%         Wrap suppress_max_restart_intensity spec with restartable spec:
%%
%%             restartable:spec(
%%                 suppress_max_restart_intensity(
%%                     ChildSpec)
%%
%%         The restartable can be used by providing the name of the top level
%%         supervisor that we spawn here, the avoid_max_restart_intensity
%%         supervisor:
%%
%%             restartable:restart(
%%                 OriginalSupPid,
%%                 suppress_max_restart_intensity:top_level_child_name(
%%                     OriginalChildName)
%%
%%    Option B:
%%
%%        Wrap restartable spec with suppress_max_restart_intensity spec:
%%
%%            suppress_max_restart_intensity:spec(
%%                restartable:spec(
%%                    ChildSpec)
%%
%%        The restartable can be used from the original supervisor by providing
%%        the original child name and original supervisor pid:
%%
%%            restartable:restart(
%%                suppress_max_restart_intensity:actual_child_pid(
%%                    OriginalSupRef,
%%                    OriginalChildName)
%%
%% @doc

-module(suppress_max_restart_intensity).

-include("ns_common.hrl").

-define(AVOID_MAX_RESTART_INTENSITY_SUP_ID(Id),
        {?MODULE, avoid_max_restart_intensity_sup, Id}).

-define(SUPERVISOR_CUSHION_ID(Id),
        {?MODULE, supervisor_cushion, Id}).

-define(INHERITED_MAX_R_MAX_T_SUP_ID(Id),
        {?MODULE, inherited_max_r_max_t_sup, Id}).

-define(MAX_R_TO_AVOID_MAX_RESTART_INTENSITY, 1000000).
-define(MAX_T_TO_AVOID_MAX_RESTART_INTENSITY, 1).

%% API
-export([spec/1,
         top_level_child_name/1,
         actual_child_pid/2]).

%% Internal exports
-export([init/1,
         avoid_max_restart_intensity_sup_link/1,
         inherited_max_r_max_t_sup_link/1]).

%% Supervisor supports map child_specs(). Maps are, IMO, much more readable, so
%% this module is implemented in terms of maps rather than tuples where
%% possible, but tuple child_specs() are supported such that this can be used
%% by those preferring that style.

%% Supervisor.erl unfortunately does not export these types. For the sake of
%% dialyzer they are lifted from supervisor.erl.
-type child_id()     :: term().
-type mfargs()       :: {M :: module(), F :: atom(), A :: [term()] | undefined}.
-type significant()  :: boolean().
-type shutdown()     :: 'brutal_kill' | timeout().
-type worker()       :: 'worker' | 'supervisor'.
-type modules()      :: [module()] | 'dynamic'.

%% Similarly lifted from supervisor:sup_flags:intensity|period.
-type intensity() :: non_neg_integer().
-type period()    :: pos_integer().

%% restart_type() drops support for 'temporary' as that will never be
%% restarted by a supervisor and, as such, should not be used with this
%% supervision hierarchy. restart_type() also drops support for 'transient'.
%% supervisor returns a 'shutdown' error when it hits max_restart_intensity,
%% a 'transient' type process exiting with reason 'shutdown' is not restarted.
%% Statuses would have to be remapped within supervisor_cushion to accomplish
%% this.
-type restart_type() :: 'permanent'.

%% Delay, specified in seconds, which we wait before restarting after hitting
%% max_restart_intensity.
-type delay() :: pos_integer().

%% restart_tuple(), used in child_spec_tuple() differs from
%% supervisor:restart() as the extra parameters delay(), intensity(), and
%% period() may be passed to it. delay() is mandatory, whilst intensity() and
%% period() are not (but must be specified together if at all).
-type restart_tuple() :: {restart_type(), delay()} |
                         {restart_type(), delay(), intensity(), period()}.

%% Similarly, whilst supervisor.erl does define a child_spec(), it does not
%% define it in constituent pieces (i.e. map() | term()). Given that we
%% allow additional parameters we need an enhanced type spec.
-type child_spec_map() :: #{id := child_id(),
                            start := mfargs(),
                            restart => restart_type(),
                            significant => significant(),
                            shutdown => shutdown(),
                            type => worker(),
                            modules => modules(),
                            %% Only part of ?MODULE.
                            delay := delay(),
                            inherited_max_r := intensity(),
                            inherited_max_t := period()}.

-type child_spec_tuple() :: {Id :: child_id(),
                             Start :: mfargs(),
                             Restart :: restart_tuple(),
                             Shutdown :: shutdown(),
                             Type :: worker(),
                             Modules :: modules()}.

-type child_spec() :: child_spec_map() | child_spec_tuple().

%%
%% API
%%
-spec spec(child_spec()) -> supervisor:child_spec().
spec(ChildSpec) when is_map(ChildSpec) ->
    %% Dialyzer is pretty good, but it's a static analysis tool not a set of
    %% runtime assertions so validate that the desired child has permanent
    %% restart type just in case.
    case maps:get(restart, ChildSpec) of
        %% permanent is the default if undefined
        undefined -> ok;
        permanent -> ok;
        _ -> erlang:error(bad_arg)
    end,

    avoid_max_restart_intensity_sup_spec(ChildSpec);
%% Pull out the tuple to check that restart == permanent and that the tuple
%% is of the correct form.
spec({_Id, _MFA, {permanent, _Delay, _InheritedMaxR, _InheritedMaxT}, _Shutdown,
      _Type, _Modules} = ChildSpec) ->
    avoid_max_restart_intensity_sup_spec(ChildSpec).

%% Return the name of the child of the top level (above
%% suppress_max_restart_intensity) supervisor (i.e. the name of the first
%% supervisor in the suppress_max_restart_intensity hierarchy).
%% This can be used in association with supervisor:which_children() to find
%% the correct child, if one really needs that information, such as uses in
%% conjunction with restartable.
-spec top_level_child_name(child_id()) -> {atom(), atom(), child_id()}.
top_level_child_name(Name) ->
    ?AVOID_MAX_RESTART_INTENSITY_SUP_ID(Name).


%% Return the pid of the original child, in case some caller needs that. Can
%% be used in association with restartable to find the correct pid to restart.
-spec actual_child_pid(any(), child_id()) -> pid() | undefined.
actual_child_pid(OriginalSupPid, ChildId) ->
    OriginalSupChildren = supervisor:which_children(OriginalSupPid),
    [{_, AvoidMaxRChildPid, _, _}] =
        filter_children(?AVOID_MAX_RESTART_INTENSITY_SUP_ID(ChildId),
                        OriginalSupChildren),

    AvoidMaxRChildren = supervisor:which_children(AvoidMaxRChildPid),
    [{_, SupervisorCushionPid, _, _}] =
        filter_children(?SUPERVISOR_CUSHION_ID(ChildId),
                        AvoidMaxRChildren),

    InheritedMaxRMaxTPid = supervisor_cushion:child_pid(SupervisorCushionPid),
    InheritedMaxRMaxTChildren = supervisor:which_children(InheritedMaxRMaxTPid),
    [{_, ActualChildPid, _, _}] =
        filter_children(ChildId, InheritedMaxRMaxTChildren),

    ActualChildPid.

%%
%% Internal Exported API
%%

-spec avoid_max_restart_intensity_sup_link(child_spec_map()) ->
          supervisor:startlink_ret().
avoid_max_restart_intensity_sup_link(ChildSpec) when is_map(ChildSpec) ->
    %% Avoiding naming this supervisor as only atoms can be used for names
    %% and the name only provides a slight readability benefit
    supervisor:start_link(?MODULE,
                          [avoid_max_restart_intensity_sup, ChildSpec]).

%% The various supervisors that we spawn in ?MODULE require an 'init/1'
%% function to startup that requires the sup_flags(). Args are passed in as a
%% list so type specs aren't as restrictive as they ideally would be. We
%% pattern match the first element of the list to determine which supervisor
%% we are starting up.
-spec init([avoid_max_restart_intensity_sup | sup_b | child_spec_map()]) ->
          {ok, {supervisor:sup_flags(), [child_spec_map()]}}.
init([avoid_max_restart_intensity_sup, ChildSpec]) when is_map(ChildSpec) ->
    {ok, {{one_for_one,
           ?MAX_R_TO_AVOID_MAX_RESTART_INTENSITY,
           ?MAX_T_TO_AVOID_MAX_RESTART_INTENSITY},
          [supervisor_cushioned_spec(ChildSpec)]}};
init([inherited_max_r_max_t_sup,
      #{inherited_max_r := MaxR,
        inherited_max_t := MaxT} = ChildSpec]) ->
    {ok, {{one_for_one, MaxR, MaxT}, [ChildSpec]}}.


%% Once we are below the supervisor_cushion in this supervision hierarchy we
%% no longer need our enhanced type specs. Whilst we could drop the extra
%% keys from the ChildSpec map here it's extra work that isn't required as
%% supervisor.erl will just ignore them. We'll use the base supervisor type
%% spec from here on down the supervision hierarchy though.
-spec supervisor_cushioned_spec(child_spec_map()) ->
          supervisor:child_spec().
supervisor_cushioned_spec(#{id := Id,
                            delay := Delay} = ChildSpec) ->
    #{id => ?SUPERVISOR_CUSHION_ID(Id),
      start => {supervisor_cushion, start_link,
                [?INHERITED_MAX_R_MAX_T_SUP_ID(Id),
                 delay_for_supervisor_cushion(Delay), infinity,
                 ?MODULE, inherited_max_r_max_t_sup_link, [ChildSpec],
                 #{always_delay => true}]},
      %% We only support permanent restart here
      restart => permanent,
      %% supervisor type children should always have infinity timeouts to
      %% avoid a race between unlinking the process and shutting down.
      shutdown => infinity,
      type => supervisor}.

-spec inherited_max_r_max_t_sup_link(supervisor:child_spec()) ->
          supervisor:startlink_ret().
inherited_max_r_max_t_sup_link(ChildSpec) ->
    %% Avoiding naming this supervisor as only atoms can be used for names
    %% and the name only provides a slight readability benefit
    supervisor:start_link(?MODULE, [inherited_max_r_max_t_sup, ChildSpec]).

%%
%% Internal functions
%%
-spec avoid_max_restart_intensity_sup_spec(child_spec()) ->
          supervisor:child_spec().
avoid_max_restart_intensity_sup_spec(#{id := Id} = ChildSpec) ->
    #{id => ?AVOID_MAX_RESTART_INTENSITY_SUP_ID(Id),
      start => {?MODULE, avoid_max_restart_intensity_sup_link, [ChildSpec]},
      %% We only support permanent restart here
      restart => permanent,
      %% supervisor type children should always have infinity timeouts to
      %% avoid a race between unlinking the process and shutting down
      shutdown => infinity,
      type => supervisor};
%% Required for use in restartable which can only deal with tuple specs.
avoid_max_restart_intensity_sup_spec(
  {Id, _MFA, _Restart, _Shutdown, _Type, _Modules} = ChildSpec) ->
    {?AVOID_MAX_RESTART_INTENSITY_SUP_ID(Id),
     {?MODULE, avoid_max_restart_intensity_sup_link,
      [convert_child_spec_tuple_to_map(ChildSpec)]},
     permanent, infinity, supervisor, [?MODULE]}.


-spec delay_for_supervisor_cushion(pos_integer()) -> pos_integer().
delay_for_supervisor_cushion(Delay) ->
    erlang:convert_time_unit(Delay, second, millisecond).

-spec convert_child_spec_tuple_to_map(child_spec_tuple()) -> child_spec_map().
convert_child_spec_tuple_to_map(
  {Id, MFA, {Restart, Delay, InheritedMaxR, InheritedMaxT}, Shutdown, Type,
   Modules}) ->
    #{id => Id,
      start => MFA,
      restart => Restart,
      delay => Delay,
      shutdown => Shutdown,
      type => Type,
      modules => Modules,
      inherited_max_r => InheritedMaxR,
      inherited_max_t => InheritedMaxT}.

filter_children(FilterName, Children) ->
    lists:filter(
        fun({Name, _, _, _}) ->
            case Name of
                FilterName -> true;
                _ -> false
            end
        end, Children).
