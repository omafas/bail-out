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
require 'json'

BIND_PORT = ARGV[0].to_i

PROXY_HOST = ARGV[1]
PROXY_PORT = ARGV[2].to_i

PROXY2_HOST = ARGV[3]
PROXY2_PORT = ARGV[4].to_i

HOST = ARGV[5]
PORT = ARGV[6].to_i

def run_accept_loop(server_sock)
  while true
    client = server_sock.accept
    puts "got client #{client}"
    Thread.new { run_client_loop(client) }
  end
end

class IO
  def send_payload(payload)
    pj = if payload.kind_of?(String)
           payload.dup
         else
           payload.to_json
         end
    pj.force_encoding(Encoding::ASCII_8BIT)
    tosend = [pj.size].pack("N") + pj
    self.write tosend
  end
  def recv_payload
    STDOUT.puts "before read of size"
    sz = self.read(4)
    STDOUT.puts "sz: #{sz.inspect}"
    sz.force_encoding(Encoding::ASCII_8BIT)
    sz = sz.unpack("N")[0]
    JSON.parse(self.read(sz))
  ensure
    STDOUT.puts "leaving recv_payload"
  end

  def setup_proxy_connection(host, port, proxy_port, cert_der)
    send_payload({
                   :proxyHost => host,
                   :proxyPort => proxy_port,
                   :port => port
                 })
    send_payload("asd")
  end
end

def run_pipe_loop(from, to)
  while true
    data = begin
             from.readpartial(16384)
           rescue EOFError, Errno::EPIPE => exc
             puts "#{from} -> #{to}: got: #{exc.inspect}"
             to.close rescue nil
             return
           end
    puts "#{from} -> #{to}: #{data}"
    to.write data
  end
ensure
  puts "ended pipe loop #{from} -> #{to}"
end

def run_client_loop(client)
  puts "starting client loop: #{client}"
  client2 = if ENV['IGNORE_PROXY']
              TCPSocket.new(HOST, PORT)
            else
              client2 = TCPSocket.new(PROXY_HOST, PROXY_PORT)
              client2.setup_proxy_connection(HOST, PORT, PROXY2_PORT, "asd")
              # puts "#{client}: sending connect payload"
              # client2.send_payload({:host => HOST, :port => PORT,
              #                        :proxyHost => PROXY2_HOST,
              #                        :proxyPort => PROXY2_PORT,
              #                        :cert => "asdasd"})
              puts "#{client}: waiting for reply"
              reply = begin
                        puts "Before recv_payload"
                        client2.recv_payload
                      rescue Exception => eee
                        puts "got eee: #{eee.inspect}"
                        raise eee
                      end
              puts "#{client}: got back #{reply.inspect}"
              if reply["type"] != "ok"
                puts "failed to establish proxy connection: #{reply.inspect}"
                raise "failed to establish proxy connection: #{reply.inspect}"
              end
              client2
            end
  upstream_sender = Thread.new {run_pipe_loop(client, client2)}
  downstream_sender = Thread.new {run_pipe_loop(client2, client)}
  begin
    upstream_sender.join
    downstream_sender.join
  ensure
    client.close rescue nil
    client2.close rescue nil
    upstream_sender.join rescue nil
    downstream_sender.join rescue nil
  end
rescue Exception => exc
  client.close rescue nil
  puts "got exception: #{exc.inspect}\n#{exc.backtrace.join("\n")}"
ensure
  puts "ended client: #{client}"
end

run_accept_loop(TCPServer.new(BIND_PORT))
