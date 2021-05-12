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

$nodes = Set.new

$events.each do |ev|
  next unless ev["type"] == "updateFastForwardMap" || ev["type"] == "vbucketMoveStart"
  (ev["chainBefore"] + ev["chainAfter"]).each {|n| $nodes << n}
end

$nodes = $nodes.to_a.sort

$vbucket_to_move_start = {}

$events.each do |ev|
  next unless ev["type"] == "vbucketMoveStart"
  vb = ev["vbucket"]
  raise if $vbucket_to_move_start[vb]
  $vbucket_to_move_start[vb] = ev
end

def mild_next it
  it.next
rescue StopIteration
  nil
end

def move_affects_node(node, ev)
  ev["chainBefore"][0] == node ||
    ev["chainAfter"][0] == node
end

def next_node_event(node, it)
  while (ev = mild_next(it))
    case ev["type"]
    when "vbucketMoveStart"
      return ev if move_affects_node node, ev
    when "vbucketMoveDone", "backfillPhaseEnded", "checkpointWaitingStarted", "checkpointWaitingEnded"
      move_event = $vbucket_to_move_start[ev["vbucket"]]
      return ev if move_affects_node(node, move_event)
    when "waitIndexUpdatedStarted", "waitIndexUpdatedEnded", "indexingInitiated"
      return ev if ev["node"] == node
    end
  end
end

$timelines = []

$nodes.each do |node|
  iter = $events.each

  timeline = []

  state = :idle
  prev_state = state
  move_count = 0
  move_start_ts = nil
  backfill_start_ts = nil
  backfill_vbucket = nil

  while (ev = next_node_event(node, iter))
    if prev_state != state
      puts "changed state #{prev_state} -> #{state}"
      prev_state = state
    end

    type = ev["type"]
    ts = ev["ts"]

    puts "processing: #{ev.inspect}"

    if type == "vbucketMoveStart"
      raise if state == :backfill
      move_count += 1
      backfill_start_ts = ts
      move_start_ts = ts unless state == :moving
      backfill_vbucket = ev["vbucket"]
      state = :backfill
      next
    end

    if state == :idle
      raise "expected vbucketMoveStart in idle. Have: #{ev.inspect}"
    end

    case type
    when "backfillPhaseEnded"
      raise "expected backfill for #{ev.inspect} got #{state.inspect}" unless state == :backfill
      raise "expecte vbucket #{backfill_vbucket} for #{ev.inspect}" unless backfill_vbucket == ev["vbucket"]
      state = :moving
      timeline << [backfill_start_ts, ts, :backfill]
    when "vbucketMoveDone"
      move_count -= 1
      if move_count == 0
        raise unless state == :moving
        timeline << [move_start_ts, ts, :move]
        state = :idle
      end
    else
      # other event types are ignored yet
    end
  end

  raise "bad final state: #{state}" unless state == :idle

  $timelines << [node, timeline]
end


$latest_time = $timelines.flatten.select {|e| e.kind_of?(Numeric)}.max
$earliest_time = $timelines.flatten.select {|e| e.kind_of?(Numeric)}.min

def ts_to_y(ts)
  10 + (ts - $earliest_time)
end

# pp $timelines

require 'rasem'

def do_svg(filename, width, height)
  inst = Rasem::SVGImage.new(width, height)
  yield inst
  inst.close
  File.open(filename, "w") do |f|
    f << inst.output
  end
end

$width_per_node = 300

lower_edge = ts_to_y($latest_time)+10
right_edge = $width_per_node * ($timelines.size + 1)

do_svg(ARGV[0]+".svg", right_edge, lower_edge) do |img|
  pos = $width_per_node*0.5
  time = 100
  while time < ($latest_time - $earliest_time)
    y = ts_to_y(time + $earliest_time)
    width = ((time % 1000 == 0) ? 8 : 2)
    img.line(0, y, right_edge, y, :'stroke-width' => width, :stroke => 'blue', :opacity => 0.2)
    img.text(pos + 6, y + 20 + width * 0.5 + 6, time.to_s, "font-size" => "20px")
    time += 100
  end

  $timelines.each_with_index do |(_node, lines), idx|
    lines.reverse!
    pos = (idx + 1 + 0.5) * $width_per_node
    # general timeline
    img.line pos, 0, pos, lower_edge, :'stroke-width' => 1, :opacity => 1.0, :stroke => '#000'

    lines.each do |(start, done, type)|
      start_y = ts_to_y(start)
      done_y = ts_to_y(done)
      case type
      when :move
        img.line pos, start_y, pos, done_y, :'stroke-width' => 12, :opacity => 1.0, :stroke => 'yellow'
      when :backfill
        img.line(pos-32, start_y, pos+32, start_y, :'stroke-width' => 1, :stroke => 'black')
        img.line pos, start_y, pos, done_y, :'stroke-width' => 24, :opacity => 1.0, :stroke => 'green'
      else
        img.line pos, start_y, pos, done_y, :'stroke-width' => 32, :opacity => 1.0, :stroke => 'red'
      end
    end
  end
end

puts "events range: #{Time.at($earliest_time)}..#{Time.at($latest_time)} (#{$latest_time-$earliest_time})"
