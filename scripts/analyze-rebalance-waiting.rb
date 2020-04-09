#!/usr/bin/env ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2013 Couchbase, Inc.
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

require 'json'
require 'pp'
require 'set'

begin
  filename = ARGV[0] || (raise "need filename arg")
  $events = IO.readlines(filename).map {|l| JSON.parse(l)}
  $events = $events.sort_by {|e| e["ts"]}
end

rebalance_end = $events.reverse.detect {|ev| ev["type"] == "bucketRebalanceEnded"}

raise "couln't find rebalance" unless rebalance_end

rebalance_start = $events.reverse.detect {|ev| ev["type"] == "bucketRebalanceStarted" && ev["pid"] == rebalance_end["pid"]}

raise "cound't locate start of rebalance: #{rebalance_end.inspect}" unless rebalance_end

raise unless rebalance_start["ts"] <= rebalance_end["ts"]

range = (rebalance_start["ts"]..rebalance_end["ts"])
$events = $events.select {|ev| range.include? ev["ts"]}

wait_type_pairs = [["waitIndexUpdatedStarted", "waitIndexUpdatedEnded"],
                   ["checkpointWaitingStarted", "checkpointWaitingEnded"]]

wait_types = wait_type_pairs.flatten.to_set

wait_events = $events.select {|ev| wait_types.include? ev["type"]}

wait_events.each do |ev|
  type = ev["type"]
  vb = ev["vbucket"]
  raise unless wait_types.include? type
  case type
  when "checkpointWaitingStarted"
    cp_id = ev["checkpointId"]
    ending = wait_events.detect {|cev| cev["type"] == "checkpointWaitingEnded" && cev["checkpointId"] == cp_id && cev["vbucket"] == vb && cev["node"] == ev["node"]}
    raise "failed to find ending for #{ev.inspect}" unless ending
    raise if ending["starting"]
    ending["starting"] = ev
    ev["ending"] = ending
  when "waitIndexUpdatedStarted"
    ending = wait_events.detect {|cev| cev["type"] == "waitIndexUpdatedEnded" && cev["vbucket"] == vb && cev["node"] == ev["node"]}
    raise "failed to find ending for #{ev.inspect}" unless ending
    raise if ending["starting"]
    ending["starting"] = ev
    ev["ending"] = ending
  when "waitIndexUpdatedEnded", "checkpointWaitingEnded"
    starting = ev["starting"]
    raise "found #{type} without known starting #{ev.inspect}" unless starting
  else
    raise
  end
end

waitings = []

wait_events.each do |ev|
  ending = ev["ending"]
  next unless ending
  waitings << [ending["ts"] - ev["ts"], ev, ending]
end

checkpoint_waitings = waitings.select {|r| r[1]["type"] == "checkpointWaitingStarted"}
index_waitings = waitings.select {|r| r[1]["type"] == "waitIndexUpdatedStarted"}

raise unless (checkpoint_waitings + index_waitings).to_set == waitings.to_set

puts "total waitings: #{waitings.map {|r| r[0]}.reduce(&:+)}"
puts "total index waitings: #{index_waitings.map {|r| r[0]}.reduce(&:+)}"
puts "total checkpoint waitings: #{checkpoint_waitings.map {|r| r[0]}.reduce(&:+)}"
puts "total vbucket moves: #{$events.select {|ev| ev["type"] == "vbucketMoveStart"}.size}"
