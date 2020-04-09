%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2018 Couchbase, Inc.
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
%%
-module(path_config).

-export([component_path/1, component_path/2,
         tempfile/2, tempfile/3, minidump_dir/0, ensure_directories/0]).

%% used by ns_config_default
-export([default_memcached_config_path/0]).

component_path_key(tmp) -> path_config_tmpdir;
component_path_key(data) -> path_config_datadir;
component_path_key(bin) -> path_config_bindir;
component_path_key(lib) -> path_config_libdir;
component_path_key(etc) -> path_config_etcdir;
component_path_key(sec) -> path_config_secdir.

-spec component_path(etc | tmp | data | lib | bin | sec) -> string().
component_path(NameAtom) ->
    try ets:lookup(path_config_override, component_path_key(NameAtom)) of
        [{_,X}|_] -> X;
        _ ->
            erlang:error({empty_for, NameAtom})
    catch error:badarg ->
            {ok, RV} = application:get_env(ns_server,
                                           component_path_key(NameAtom)),
            RV
    end.

-spec component_path(etc | tmp | data | lib | bin | sec, string()) -> string().
component_path(NameAtom, SubPath) ->
    filename:join(component_path(NameAtom), SubPath).

tempfile(Dir, Prefix, Suffix) ->
    Unique = erlang:unique_integer(),
    Pid = os:getpid(),
    Filename = Prefix ++ integer_to_list(Unique) ++ "_" ++
               Pid ++ Suffix,
    filename:join(Dir, Filename).

tempfile(Prefix, Suffix) ->
    Dir = component_path(tmp),
    tempfile(Dir, Prefix, Suffix).

default_memcached_config_path() ->
    filename:join(component_path(data, "config"), "memcached.json").

minidump_dir() ->
    path_config:component_path(data, "crash").

ensure_directories() ->
    ok = misc:mkdir_p(component_path(data)),
    ok = misc:ensure_writable_dir(component_path(tmp)).
