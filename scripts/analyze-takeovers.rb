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

require 'rubygems'
require 'active_support/all'
require 'json'
require 'aggregate'
require 'pp'

$events = IO.readlines(ARGV[0] || raise("need path to events")).map {|l| JSON.parse(l).symbolize_keys!}

$by_pid = {}

$events.each do |ev|
  next unless ev.has_key? :pid
  ($by_pid[ev[:pid]] ||= []) << ev
end

def aggregate(ev, agg)
  start_ev, = $by_pid[ev[:pid]].select {|cev| cev[:type] == 'ebucketmigratorStart'}
  unless start_ev
    # puts "no start event for #{ev}"
    return
  end
  duration = ev[:ts] - start_ev[:ts]
  duration = (duration * 1000000).to_i
  # puts "dur: #{duration}"
  agg << duration
end

takeover_agg = []

$events.each do |ev|
  next unless ev[:type] == 'ebucketmigratorTerminate' && ev[:takeover] == true
  aggregate(ev, takeover_agg)
end

replica_building_agg = []

$events.each do |ev|
  next unless ev[:type] == 'ebucketmigratorTerminate' && ev[:takeover] == false
  next unless ev[:name] =~ /\Areplication_building/
  aggregate(ev, replica_building_agg)
end

# puts takeover_agg
# exit

def finalize_agg(samples, num_bins = 60)
  max = [samples.max.to_i, 1].max
  width = (max + num_bins - 1) / num_bins
  agg = Aggregate.new(0, width * num_bins, width)
  samples.each {|s| agg << s}
  agg
end

takeover_agg = finalize_agg(takeover_agg)
replica_building_agg = finalize_agg(replica_building_agg)

puts "==  TAKEOVER  =="
puts takeover_agg.to_s(120)
puts
puts "== REPLICA BUILDING  =="
puts replica_building_agg.to_s(120)
