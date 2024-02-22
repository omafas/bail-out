#!/usr/bin/ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2013-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

require 'socket'
require 'openssl'
require 'json'
require 'thread'
require 'pp'

module IOMethods
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
  def recv_raw_payload
    sz = self.read(4)
    sz.force_encoding(Encoding::ASCII_8BIT)
    sz = sz.unpack("N")[0]
    self.read(sz)
  end
  def recv_payload
    JSON.parse(self.recv_raw_payload)
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

Counters = Object.new

class << Counters
  STATS = %w[bytes_upstream_to_downstream
             bytes_downstream_to_upstream
             upstream_connections_established
             upstream_connections_closed
             downstream_connections_established
             downstream_connections_closed
             bytes_downstream_to_final
             bytes_final_to_downstream].map(&:intern)

  def init!
    @stats = {}
    STATS.each do |k|
      @stats[k] = 0
    end
    @mutex = Mutex.new
  end

  Counters.init!

  attr_reader :stats

  def bump!(stat, by = 1)
    @mutex.synchronize do
      now = @stats[stat]
      raise unless now
      now += by
      @stats[stat] = now
    end
  rescue Exception => exc
    abort "exception in Counters #{exc}\n#{exc.backtrace.join("\n")}"
  end
end

class IO
  include IOMethods
end

def each_accept(acceptable)
  while true
    socket = acceptable.accept
    yield socket
  end
end

def spawn_protected_thread(pipe_wr)
  Thread.new do
    begin
      yield
    ensure
      pipe_wr << '\0'
    end
  end
end

def handle_upstream_conn(client)
  payload = client.recv_payload
  puts "payload: #{payload.inspect}"
  host = payload["proxyHost"]
  proxy_port = payload["proxyPort"]
  port = payload["port"]
  raise "host: #{host}" unless host.kind_of?(String)
  raise "proxy_port: #{proxy_port}" unless proxy_port.kind_of?(Integer)
  raise "port: #{port}" unless port.kind_of?(Integer)
  cert_der = client.recv_raw_payload

  downstream_raw = TCPSocket.open(host, proxy_port)
  puts "connected to downstream: #{host.inspect}:#{proxy_port.inspect}"
  ssl_ctx = OpenSSL::SSL::SSLContext.new
  params = ssl_ctx.set_params(:verify_mode => OpenSSL::SSL::VERIFY_NONE)
  puts "ssl params: #{params.inspect}"
  # ssl_ctx.options ||= ::OpenSSL::SSL::OP_NO_COMPRESSION
  puts "doing ssl on downstream"
  downstream = OpenSSL::SSL::SSLSocket.new(downstream_raw, ssl_ctx)
  downstream.connect
  puts "got ssl on downstream"

  puts "peer_cert: #{downstream.peer_cert}"

  downstream.extend IOMethods
  puts "sending final port payload: #{port}"
  downstream.send_payload({:port => port})
  downstream.flush
  payload2 = downstream.recv_payload
  raise "bad payload2: #{payload2.inspect}" unless payload2["type"] == "ok"

  client.send_payload({:type => "ok"})

  Counters.bump! :upstream_connections_established

  rd, wr = IO.pipe
  t1 = spawn_protected_thread(wr) do
    pipe_data_from_to(client, downstream, :bytes_upstream_to_downstream)
  end

  t2 = spawn_protected_thread(rd) do
    pipe_data_from_to(downstream, client, :bytes_downstream_to_upstream)
  end

  rd.readbyte

  t1.join
  t2.join

rescue Exception => exc
  puts "got exception: #{exc}\n#{exc.backtrace.join("\n")}"
ensure
  client.close rescue nil
  downstream.close rescue nil
  Counters.bump! :upstream_connections_closed
end

def pipe_data_from_to(from, to, stat)
  buf = ""
  while not from.eof?
    data = from.readpartial(65536, buf)
    to << data
    to.flush
    Counters.bump! stat, data.size
  end
end

def run_upstream_listener(port)
  socket = TCPServer.new(port)
  puts "running upstream at port: #{port}"
  each_accept(socket) do |client|
    puts "accepted upstream socket"
    Thread.new { handle_upstream_conn(client) }
  end
end

def handle_downstream_conn(client)
  client.extend IOMethods
  puts "reading downstream payload"
  payload = client.recv_payload
  port = payload["port"]
  raise "port: #{port}" unless port.kind_of?(Integer)

  puts "got payload: #{payload.inspect}.\nConnecting to final downstream"

  downstream = TCPSocket.new("127.0.0.1", port)

  client.send_payload({:type => "ok"})
  client.flush
  puts "replied to client"

  Counters.bump! :downstream_connections_established

  rd, wr = IO.pipe
  t1 = spawn_protected_thread(wr) do
    pipe_data_from_to(client, downstream, :bytes_downstream_to_final)
  end

  t2 = spawn_protected_thread(rd) do
    pipe_data_from_to(downstream, client, :bytes_final_to_downstream)
  end

  rd.readbyte

  t1.join
  t2.join
rescue Exception => exc
  puts "got exception: #{exc}\n#{exc.backtrace.join("\n")}"
ensure
  Counters.bump! :downstream_connections_closed
  client.close rescue nil
  downstream.close rescue nil
end

def run_downstream_listener(port, key_file, cert_file)
  key = ::OpenSSL::PKey.read(File.read(key_file))
  cert = OpenSSL::X509::Certificate.new(File.read(cert_file))
  ctx = OpenSSL::SSL::SSLContext.new
  ctx.key = key
  ctx.cert = cert
  ctx.options = ::OpenSSL::SSL::OP_NO_COMPRESSION
  server_sock = ::OpenSSL::SSL::SSLServer.new(TCPServer.new(port), ctx)

  puts "running downstream at port: #{port}"
  each_accept(server_sock) do |client|
    puts "accepted downstream socket"
    Thread.new { handle_downstream_conn(client) }
  end
end

def run!(upstream_port, downstream_port)
  upstream_t = Thread.new do
    run_upstream_listener(upstream_port)
  end
  upstream_t.abort_on_exception = true

  run_downstream_listener(downstream_port,
                          "/root/src/test/go-ssh-bench/key.pem",
                          "/root/src/test/go-ssh-bench/cert.pem")
end

def must_port(v, name)
  unless v =~ /\A[0-9]+\z/
    raise "need port for #{name}. Got #{v.inspect}"
  end
  v.to_i
end

Thread.new do
  Thread.current.abort_on_exception = true
  l = (STDIN.readline rescue "EOF")
  STDOUT.puts "Got request: #{l.chomp} on stdin"
  STDIN.flush
  exit
end

trap("USR1") do
  puts "Stats:\n"
  puts Counters.stats.dup.pretty_inspect
  abort
end

run!(must_port(ARGV[1], "upstream"),
     must_port(ARGV[0], "downstream"))
