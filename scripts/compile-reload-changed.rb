#!/usr/bin/env ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2015 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
