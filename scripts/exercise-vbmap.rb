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

VBMAP_PATH = File.join(File.dirname(__FILE__), "../deps/gocode/bin/vbmap")
# VBMAP_PATH = File.join(File.dirname(__FILE__), "../deps/vbmap/vbmap")

$no_diag = true

def run_promised_vbmap(nodes, replicas, tags, slaves = 10, vbuckets = 1024)
  raise unless nodes % tags == 0
  tag_size = nodes / tags
  tags_string = (0...nodes).map {|i| "#{i}:#{i / tag_size}"}.join(",")
  base_args = [VBMAP_PATH, "--num-nodes", nodes.to_s, "--num-replicas", replicas.to_s,
               "--tags", tags_string, "--num-slaves", slaves.to_s, "--num-vbuckets", vbuckets.to_s, "--relax-all"]
  base_args << "--diag=/dev/null" if $no_diag
  stuff = IO.popen(base_args, "r") do |f|
    f.read
  end
  [$?.success?, stuff]
end

def each_factor(n)
  top = Math.sqrt(n).to_i
  yield 1
  yield n
  (2..top).each do |c|
    next unless (n % c) == 0
    yield c
    yield n / c
  end
end

unless ARGV.empty?
  $no_diag = false
  args = ARGV.map {|e| e.to_i}
  puts "ARGS: #{args.inspect}"
  rv = run_promised_vbmap(*args)
  exit(rv ? 0 : 1)
end

ok_count = 0
notok_count = 0

def check_output!(output, nodes, tags)
  tag_size = nodes / tags
  vbucket_map = output.split("\n").zip(0...1024).map {|(l, vb)| raise unless l =~ /(\d+):\s+(.*)/; raise "#{$1} #{vb}" unless $1.to_i == vb; $2.split(" ").map(&:to_i)}

  strict_violations = 0
  weak_violations = 0

  vbucket_map.each_with_index do |row0, vb|
    row = row0.map {|v| v / tag_size}

    strictly_broken = false
    weakly_broken = false

    row.size.times do |j|
      from = row[j]
      j.times do |k|
        to = row[k]

        weakly_broken ||= (from == to)
        strictly_broken ||= (j == 0 && from == to)
      end
    end

    if strictly_broken
      strict_violations += 1
    elsif weakly_broken
      weak_violations += 1
    end
  end

  if strict_violations + weak_violations != 0
    puts "\nnote: rack-awareness strictly broken for #{strict_violations} vbucket(s), weakly broken for #{weak_violations} vbucket(s)"
  end
end

trap("SIGINT") {STDERR.puts "sigint!"; STDERR.flush; exit(2)}

(2..100).each do |nodes|
  each_factor(nodes) do |tags|
    next if tags == 1
    (0..3).each do |replicas|
      # next if replicas < 2
      next if replicas > tags - 1
      slaves = [nodes-1, 10].min
      print "testing #{nodes} nodes, #{tags} tags, #{replicas} replicas: "
      ok, output = run_promised_vbmap(nodes, replicas, tags, slaves)
      check_output!(output, nodes, tags)
      print "#{ok ? "OK" : "NOT OK"}\n"
      if ok
        ok_count += 1
      else
        notok_count += 1
      end
    end
  end
end

puts "ok-d: #{ok_count}, notok-d: #{notok_count}"
