#!/usr/bin/env ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2012 Couchbase, Inc.
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

# we're going to try to rely on ruby 1.9+ ordered hashes

require 'json'
require 'pp'

# ev = JSON.parse(<<HERE)
# {"vbucket":1022,"type":"vbucketMoveStart","ts":1354035127.735102,"pid":"<0.7572.0>","node":"n_3@10.17.30.106","bucket":"default","chainBefore":["n_3@10.17.30.106"],"chainAfter":["10.17.30.106:11997"]}
# HERE

def reformat_ev(ev)
  new_ev = {}
  %w[type ts vbucket node pid].each do |k|
    new_ev[k] = ev[k] if ev.has_key? k
  end
  new_ev.update(ev)
  new_ev
end

# pp ev
# pp reformat_ev(ev)

STDIN.each_line do |l|
  puts reformat_ev(JSON.parse(l)).to_json
end
