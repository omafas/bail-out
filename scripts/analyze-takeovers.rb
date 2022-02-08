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
