% Copyright 2022-Present Couchbase, Inc.
%
% Use of this software is governed by the Business Source License included in
% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
% file, in accordance with the Business Source License, use of this software
% will be governed by the Apache License, Version 2.0, included in the file
% licenses/APL2.txt.

RelativePath = proplists:get_value(storage_path, prometheus_cfg:settings()),
Settings = ns_config:read_key_fast(stats_settings, []),
Nodes = [node() | nodes()],
Log = fun (F, A) ->
          catch ale:info(ns_server, "(stats cleaning script) " ++ F, A)
      end,
Log("Preparing stats cleaning.~nRelativePath=~p~nSettings=~p~nNodes=~p",
    [RelativePath, Settings, Nodes]),
%% Making sure RelativePath is not empty, so we are not removing the whole
%% data dir accidentally:
true = is_list(RelativePath),
true = (length(RelativePath) > 0),
ns_config:set(stats_settings,
              misc:update_proplist(Settings, [{enabled, false}])),
timer:sleep(1000),
Res =
    lists:map(
      fun (N) ->
          try
              Log("Waiting for ns_config / node ~p", [N]),
              rpc:call(N, ns_config, sync_announcements, []),
              Log("Waiting for prometheus_cfg / node ~p", [N]),
              gen_server:call({prometheus_cfg, N}, settings, 60000),
              Log("Building stats path / node ~p", [N]),
              Path = rpc:call(N, path_config, component_path,
                              [data, RelativePath]),
              Log("Removing stats dir ~p / node ~p", [Path, N]),
              ok = rpc:call(N, misc, rm_rf, [Path]),
              Log("Cleaning stats dir done / node ~p", [N]),
              {N, ok}
          catch
              C:E ->
                  Log("Exception ~p:~p / node ~p", [C, E, N]),
                  {N, {error, {C,E}}}
          end
      end, Nodes),

Log("Restoring stats settings", []),
ns_config:set(stats_settings, Settings),
Log("Done (~p)", [Res]),

{json, {[{Node, iolist_to_binary(io_lib:format("~10000p", [R]))}
         || {Node, R} <- Res]}}.
