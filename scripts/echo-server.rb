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

require 'socket'

BIND_PORT = ARGV[0] ? ARGV[0].to_i : 22222

def run_accept_loop(server_sock)
  while true
    client = server_sock.accept
    puts "got client #{client}"
    Thread.new { run_client_loop(client) }
  end
end

def run_client_loop(client)
  puts "starting client loop: #{client}"
  while true
    stuff = client.readpartial(16384)
    puts "#{client}: got #{stuff.inspect}"
    client.write stuff
  end
rescue EOFError
  client.close rescue nil
  puts "ended client: #{client}"
  return
rescue Exception => exc
  puts "got exception: #{exc}"
end

run_accept_loop(TCPServer.new(BIND_PORT))

