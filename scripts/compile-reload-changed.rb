#!/usr/bin/env ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2015-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

require_relative "rest-methods"

include RESTMethods

Dir.chdir(File.join(File.dirname(__FILE__), "..")) do
  system("make") || raise
end

set_node!("127.0.0.1:9000")

# this is based on distel's reload_modules (under 3-clause BSD)
# https://github.com/massemanet/distel/blob/master/src/distel.erl#L104
rv = post!("/diag/eval", <<HERE)
T = fun(L) -> lists:keyfind(time, 1, L) end,
Tm = fun(M) -> T(M:module_info(compile)) end,
Tf = fun(F) -> {ok,{_,[{_,I}]}}=beam_lib:chunks(F,[compile_info]),T(I) end,
ReloadFn = fun (Self, SendHidden) ->
  case SendHidden of
    true ->
      rpc:multicall(nodes(hidden), erlang, apply, [Self, [Self, false]]);
    false ->
      [begin c:l(M),M end || {M,F} <- code:all_loaded(), not is_atom(F), F =/= [], Tm(M)<Tf(F)]
  end
end,
{rpc:multicall(erlang, apply, [ReloadFn, [ReloadFn, true]]),
 rpc:multicall(erlang, apply, [ReloadFn, [ReloadFn, false]])}
HERE

puts
puts rv
