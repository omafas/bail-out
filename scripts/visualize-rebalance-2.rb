#!/usr/bin/env ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2013-Present Couchbase, Inc.
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

$vbucket_to_events = {}

$events.each do |ev|
  next if ev["type"] == "updateFastForwardMap"
  if (vb = ev["vbucket"])
    arr = ($vbucket_to_events[vb] ||= [])
    arr << ev
  elsif ev["vbuckets"] && (ev["name"] =~ /\Areplication_building_/ || ev["takeover"] == true)
    ev["vbuckets"].each do |vb|
      arr = ($vbucket_to_events[vb] ||= [])
      arr << ev
    end
  end
end

$vbucket_to_events = $vbucket_to_events.to_a.sort_by(&:first)

def setup_time_bounds!
  sorted_events = $vbucket_to_events.map(&:last).flatten.sort_by {|ev| ev["ts"]}

  $latest_time = sorted_events[-1]["ts"]
  $earliest_time = sorted_events[0]["ts"]
end

setup_time_bounds!

def ts_to_y(ts)
  10 + (ts - $earliest_time)
end

require 'rasem'

def do_svg(filename, width, height)
  inst = Rasem::SVGImage.new(width, height)
  yield inst
  inst.close
  File.open(filename, "w") do |f|
    f << inst.output
  end
end

$width_per_node = 50
$width_per_vbucket = $width_per_node * $nodes.size + 50

lower_edge = ts_to_y($latest_time)+10
right_edge = $width_per_vbucket * ($vbucket_to_events.size + 1)

do_svg(ARGV[0]+".svg", right_edge, lower_edge) do |img|
  def img.rel_line(x, y, dx, dy, style = {})
    line(x,y,x+dx,y+dy,style)
  end

  pos = $width_per_node*0.5
  time = 100
  while time < ($latest_time - $earliest_time)
    y = ts_to_y(time + $earliest_time)
    width = ((time % 1000 == 0) ? 8 : 2)
    img.line(0, y, right_edge, y, :'stroke-width' => width, :stroke => 'blue', :opacity => 0.2)
    img.text(pos + 6, y + 20 + width * 0.5 + 6, time.to_s, "font-size" => "20px")
    time += 100
  end

  $vbucket_to_events.sort_by {|(_, evs)| evs[0]["ts"]}.each_with_index do |(vb, events), idx|
    corner_x = $width_per_vbucket * idx

    img.rel_line(corner_x + $width_per_vbucket, 0,
                 0, lower_edge, :stroke => 'black')

    nodes_base_x = corner_x + ($width_per_vbucket - $nodes.size * $width_per_node) / 2
    node_to_x_pre = $nodes.each_with_index.map {|n, i| [n, nodes_base_x + i * $width_per_node]}.flatten
    node_to_x = Hash[*node_to_x_pre]

    # puts "$nodes:\n#{node_to_x_pre.pretty_inspect}"
    # puts "node_to_x:\n#{node_to_x.pretty_inspect}"
    # Kernel.exit
    start_at = events[0]["ts"]
    e_at = events[-1]["ts"]
    node_to_x.each_value do |x|
      img.line x, ts_to_y(start_at), x, ts_to_y(e_at), 'stroke-width' => 1, :opacity => 1.0, :stroke => '#000'
    end

    img.text(($width_per_vbucket + 1) * idx - 50, ts_to_y(start_at) + 2, vb.to_s, "font-size" => "20px")

    bf_end_ev = events.detect {|ev| ev["type"] == "backfillPhaseEnded"}
    next unless bf_end_ev

    move_start_ev = events.detect {|ev| ev["type"] == "vbucketMoveStart"}
    move_end_ev = events.detect {|ev| ev["type"] == "vbucketMoverTerminate"}

    raise unless move_start_ev && move_end_ev

    backfill_nodes = move_start_ev.values_at("chainBefore", "chainAfter").map(&:first).compact

    backfill_nodes.each do |node|
      x = node_to_x[node]
      raise "node: #{node.inspect}, move_start_ev: #{move_start_ev.inspect}" unless x
      start_y = ts_to_y(move_start_ev["ts"])
      done_y = ts_to_y(bf_end_ev["ts"])
      img.line x, start_y, x, done_y, :'stroke-width' => 24, :opacity => 1.0, :stroke => 'green'
    end

    waiting_evs = events.select {|ev| %w(checkpointWaitingStarted checkpointWaitingEnded).include? ev["type"]}
    # puts "waiting_evs:\n#{waiting_evs.pretty_inspect}"
    waiting_evs.each do |ev|
      case ev["type"]
      when "checkpointWaitingStarted"
        ending = waiting_evs.detect {|cev| cev["type"] == "checkpointWaitingEnded" && cev["node"] == ev["node"] && ev["ts"] <= cev["ts"]}
        raise unless ending
        raise "already have starting event: #{ending["starting"].inspect} for #{ev.inspect}" if ending["starting"]
        ending["starting"] = ev
        ev["ending"] = ending
      when "checkpointWaitingEnded"
        raise "ev: #{ev.inspect}" unless ev["starting"]
      else
        raise
      end
    end

    takeover_start_ev = events.detect {|ev| ev["type"] == "ebucketmigratorStart" && ev["takeover"] == true}
    takeover_end_ev = events.detect {|ev| ev["type"] == "ebucketmigratorTerminate" && ev["takeover"] == true}

    if takeover_start_ev
      raise unless takeover_end_ev
      takeover_dest = move_start_ev["chainAfter"][0]
      x = node_to_x[takeover_dest]
      ts0 = takeover_start_ev["ts"]
      ts1 = takeover_end_ev["ts"]
      # puts "seen takeover for vb #{vb} at #{ts0}..#{ts1}"
      img.line x-20, ts_to_y(ts0), x+20, ts_to_y(ts0), :'stroke-width' => 4, :opacity => 1.0, :stroke => '#000'
      img.line x, ts_to_y(ts0), x, ts_to_y(ts1), :'stroke-width' => 24, :opacity => 1.0, :stroke => 'magenta'
    end

    waiting_evs.each do |ev|
      next unless ev["type"] == "checkpointWaitingStarted"
      start_y = ts_to_y(ev["ts"])
      done_y = ts_to_y(ev["ending"]["ts"])
      x = node_to_x[ev["node"]]
      raise unless x
      img.line x, start_y, x, done_y, :'stroke-width' => 12, :opacity => 1.0, :stroke => 'yellow'
    end

    img.rel_line(corner_x, ts_to_y(move_end_ev["ts"]),
                 $width_per_vbucket, 0,
                 :'stroke-width' => 1, :opacity => 1.0, :stroke => 'red')

    before_pos_y = ts_to_y(move_end_ev["ts"] + 20)
    move_start_ev["chainBefore"].each_with_index do |node, node_idx|
      img.text node_to_x[node], before_pos_y, node_idx.to_s, "font-size" => "14px"
    end

    after_pos_y = ts_to_y(move_end_ev["ts"] + 50)
    move_start_ev["chainAfter"].each_with_index do |node, node_idx|
      img.text node_to_x[node], after_pos_y, node_idx.to_s, "font-size" => "14px"
    end

    events.each do |ev|
      ts = ev["ts"]
      img.rel_line(corner_x + $width_per_vbucket, ts_to_y(ts), -5, 0,
                   :'stroke-width' => 1, :opacity => 1.0, :stroke => '#000')
    end
  end
end
