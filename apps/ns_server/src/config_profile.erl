%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(config_profile).

-include("ns_common.hrl").

-export([set_data/1,
         name/0,
         get/0,
         search/1,
         search/2,
         get_value/2,
         get_bool/1,
         is_serverless/0,
         is_provisioned/0,
         load/0,
         load/1]).

-ifdef(TEST).
-export([load_profile_for_test/1,
         load_default_profile_for_test/0,
         unload_profile_for_test/1,
         unload_profile_for_test/0]).
-endif.

-define(PROFILE_FILE, "/etc/couchbase.d/config_profile").

%% Key used to retreive configuration profiles from application env.
-define(CONFIG_PROFILE, config_profile).

-spec(set_data(term()) -> 'ok').
set_data(Value) ->
    persistent_term:put(?CONFIG_PROFILE, Value).

-spec(name() -> string()).
name() ->
    search_profile_key(name, ?DEFAULT_PROFILE_STR).

-spec(get() -> list()).
get() ->
    persistent_term:get(?CONFIG_PROFILE).

-spec(get_value(term(), term()) -> term()).
get_value(Key, Default) ->
    proplists:get_value(Key, config_profile:get(), Default).

-spec(get_bool(term()) -> boolean()).
get_bool(Key) ->
    proplists:get_bool(Key, config_profile:get()).

-spec(search(term()) -> term()).
search(Key) ->
    search_profile_key(Key).

-spec(search(term(), term()) -> term()).
search(Key, Default) ->
    search_profile_key(Key, Default).

%% @doc WARNING: Please only use this in certain situations. Instead you should
%% be enabling features by individual flags inside the profile and not a blanket
%% check for if we are using the "serverless" profile. You have been warned.
-spec(is_serverless() -> boolean()).
is_serverless() ->
    case name() of
        ?SERVERLESS_PROFILE_STR ->
            true;
        _ ->
            false
    end.

%% @doc WARNING: Please only use this in certain situations. Instead you should
%% be enabling features by individual flags inside the profile and not a blanket
%% check for if we are using the "provisioned" profile. You have been warned.
-spec(is_provisioned() -> boolean()).
is_provisioned() ->
    case name() of
        ?PROVISIONED_PROFILE_STR ->
            true;
        _ ->
            false
    end.

-spec(search_profile_key(term()) -> term()).
search_profile_key(Key) ->
    search_profile_key(Key, false).

-spec(search_profile_key(term(), term()) -> term()).
search_profile_key(Key, Default) ->
    ProfileData = config_profile:get(),
    case lookup_profile_key(ProfileData, Key) of
        {value, Value} ->
            Value;
        _ ->
            Default
    end.

-spec(lookup_profile_key(list(), term()) -> {value, term()} | false).
lookup_profile_key(ProfileData, Key) when is_list(ProfileData) ->
    case proplists:lookup(Key, ProfileData) of
        {_, Value} ->
            {value, Value};
        _ ->
            false
    end.

-spec(load() -> string()).
load() ->
    load(?PROFILE_FILE).

-spec(load(string()) -> string()).
load(?PROFILE_FILE = Path) ->
    case file:read_file(Path) of
        {ok, <<>>} ->
            ?DEFAULT_PROFILE_STR;
        {ok, <<_:1/binary, _/binary>> = Binary} ->
            string:trim(binary_to_list(Binary));
        {error, Err} ->
            ?log_warning("Could not load profile file (~p) due to error: ~p."
                         " Returning 'default'.",
                         [Path, Err]),
            ?DEFAULT_PROFILE_STR
    end.

-ifdef(TEST).
load_default_profile_for_test() ->
    load_profile_for_test(?DEFAULT_PROFILE_STR).

load_profile_for_test(Profile) ->
    {ok, Cwd} = file:get_cwd(),
    Path = filename:join([Cwd, "etc", Profile ++ "_profile"]),
    {ok, Data} = file:consult(Path),
    set_data(Data).

%% Convenience function to allow us to use eunit setup/teardown helpers.
unload_profile_for_test(_) ->
    unload_profile_for_test().
unload_profile_for_test() ->
    persistent_term:erase(?CONFIG_PROFILE).

-endif.
