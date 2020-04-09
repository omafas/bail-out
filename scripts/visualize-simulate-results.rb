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

require 'json'
require 'pp'

begin
  filename = ARGV[0] || (raise "need filename arg")
  $events = IO.readlines(filename).map {|l| JSON.parse(l)}
  $events = $events.sort_by {|e| e["ts"]}
end

# pp $events

$in_flight = {}

def inc_node(hash, node, by)
  hash[node] ||= 0
  hash[node] += by
end

def note_move_started_on(node)
  $in_flight[node] ||= 0
  if $in_flight[node] == 0
    output_event :move, :start, node
  end
  output_event :backfill, :start, node
  $in_flight[node] += 1
end

def note_backfill_done_on(node)
  raise unless $in_flight[node] > 0
  output_event :backfill, :done, node
end

def note_move_done_on(node)
  raise unless $in_flight[node] > 0
  $in_flight[node] -= 1
  if $in_flight[node] == 0
    output_event :move, :done, node
  end
end

$output_events = []

$now = nil

def output_event(type, subtype, node)
  raise unless $now
  $output_events << [$now, type, subtype, node]
end

$events.each do |ev|
  type = ev["type"]
  subtype = ev["subtype"]
  ts = ev["ts"]
  $now = ts
  case type
  when "move"
    src = ev["chainBefore"][0]
    dst = ev["chainAfter"][0]
    vb = ev["vb"]
    case subtype
    when "start"
      note_move_started_on(src)
      note_move_started_on(dst) unless dst == src
    when "backfill_done"
      note_backfill_done_on(src)
      note_backfill_done_on(dst) unless dst == src
    when "done"
      note_move_done_on(src)
      note_move_done_on(dst) unless dst == src
    end
  when "compact"
    node = ev["node"]
    output_event :compact, ((subtype == "done") ? :done : :start), node
  end

end

$output_events = $output_events.sort do |(now_a, _, subtype_a, _), (now_b, _, subtype_b, _)|
  rv = now_a - now_b
  if rv == 0
    rv = case [subtype_a, subtype_b]
         when [:done, :start]
           -1
         when [:done, :done]
           0
         when [:start, :start]
           0
         else
           1
         end
  end
  rv
end

# pp $output_events

$timelines = {}

$open_things = {}

$output_events.each do |(ts, type, subtype, node)|
  $timelines[node] ||= []
  k = [node, type]
  case subtype
  when :start
    $open_things[k] = ts
  when :done
    start_ts = $open_things[k]
    raise "shit: #{k.inspect} #{ts}" unless start_ts
    $open_things.delete k
    $timelines[node] << [start_ts, ts, type]
  end
end

raise unless $open_things.keys.empty?


$timelines = $timelines.to_a.map do |(node, events)|
  new_events = events.sort_by {|(start, done,_)| [start, start - done]}
  [node, new_events]
end.sort

latest_time = $timelines.flatten.select {|e| e.kind_of?(Numeric)}.max

latest_time *= 1.10

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

$width_per_node = 200

do_svg(ARGV[0]+".svg", $width_per_node * $timelines.size, latest_time) do |img|
  $timelines.each_with_index do |(_node, lines), idx|
    pos = (idx + 0.5) * $width_per_node
    # general timeline
    img.line pos, 0, pos, latest_time, :'stroke-width' => 1, :opacity => 1.0, :stroke => '#000'

    lines.each do |(start, done, type)|
      case type
      when :move
        img.line pos, start, pos, done, :'stroke-width' => 12, :opacity => 1.0, :stroke => 'yellow'
      when :backfill
        img.line(pos-32, start, pos+32, start, :'stroke-width' => 1, :stroke => 'black')
        img.line pos, start, pos, done, :'stroke-width' => 24, :opacity => 1.0, :stroke => 'green'
      else
        img.line pos, start, pos, done, :'stroke-width' => 32, :opacity => 1.0, :stroke => 'red'
      end
    end
  end
end
