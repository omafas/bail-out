#!/usr/bin/env ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2012-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

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
