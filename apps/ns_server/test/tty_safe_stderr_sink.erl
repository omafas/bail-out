%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(tty_safe_stderr_sink).

-behaviour(gen_server2).
-compile(nowarn_export_all).
-compile(export_all).

start_link(Name) ->
    gen_server2:start_link({local, Name}, ?MODULE, [], []).

handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call({log, Msg}, _From, State) ->
    io:put_chars(standard_error, Msg),
    {reply, ok, State}.

meta() ->
    [{type, preformatted}].
