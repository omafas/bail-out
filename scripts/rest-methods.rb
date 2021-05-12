# @author Couchbase <info@couchbase.com>
# @copyright 2015-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
require 'restclient'
require 'json'
require 'pp'
require 'active_support'
require 'cgi'
require 'thread'

RestClient.log = RestClient.create_log 'stdout'

# monkey-patch RestClient for more detailed response logging
class RestClient::Request
  def log_response res
    return unless RestClient.log

    size = @raw_response ? File.size(@tf.path) : (res.body.nil? ? 0 : res.body.size)
    if !@raw_response && size > 0 && size < 1024
      RestClient.log << "# => #{res.code} #{res.class.to_s.gsub(/^Net::HTTP/, '')} | #{(res['Content-type'] || '').gsub(/;.*$/, '')} #{size} bytes:#{res.body}\n"
    else
      RestClient.log << "# => #{res.code} #{res.class.to_s.gsub(/^Net::HTTP/, '')} | #{(res['Content-type'] || '').gsub(/;.*$/, '')} #{size} bytes\n"
    end
  end
end

$username = 'Administrator'
$password = "asdasd"

require 'ostruct'
$opts = OpenStruct.new({
                         :quota => 900,
                         :quota_given => true,
                         :replicas => 0,
                         :no_default_bucket => true})


module RESTMethods
  def req!(method, path, payload = nil, headers = nil)
    base_url = Thread.current[:base_url] || $base_url
    opts = {
      :method => method, :url => base_url + path, :payload => payload, :headers => headers || {}
    }
    if $username
      opts[:user] = $username
      opts[:password] = $password
    end
    RestClient::Request.execute(opts)
  end

  def post!(path, payload, headers = nil)
    req!(:post, path, payload, headers)
  end

  def postj!(path, payload, headers = nil)
    JSON.parse(req!(:post, path, payload, headers).body)
  end

  def put!(path, payload, headers = nil)
    req!(:put, path, payload, headers)
  end

  def get!(path, headers = nil)
    req!(:get, path, nil, headers)
  end

  def getj!(path, headers = nil)
    JSON.parse(get!(path, headers).body)
  end

  def build_base_uri(hostname)
    hostname += ":8091" unless hostname =~ /:/
    "http://#{hostname}"
  end

  def set_node!(hostname)
    $base_url = build_base_uri(hostname)
  end

  def switching_node(hostname)
    old_base = $base_url
    set_node!(hostname)
    yield
  ensure
    $base_url = old_base
  end

  def switching_username(username)
    old, $username = $username, username
    yield
  ensure
    $username = old
  end

  def setup_node!(hostname, no_default_bucket = $opts.no_default_bucket)
    switching_node(hostname) do
      # wait_rebalancing
      quota = $opts.quota
      if $opts.quota_given
        post!("/pools/default",
              :memoryQuota => quota)
      end
      unless no_default_bucket
        post!("/pools/default/buckets",
              :name => "default",
              :ramQuotaMB => $opts.sane_default_bucket ? quota/2 : quota,
              :authType => 'sasl',
              :saslPassword => '',
              :replicaNumber => $opts.replicas.to_s)
      end
      post!("/settings/web", {:port => "SAME", :username => $username, :password => $password})
    end
  end

  def each_possible_node
    port = 9000
    while true
      yield "127.0.0.1:#{port}"
      port += 1
    end
  end

  def poll_condition
    while true
      break if yield
      sleep 0.02
    end
  end

  def discover_nodes!
    good_nodes = []
    each_possible_node do |host|
      set_node! host
      begin
        get! "/pools"
        good_nodes << host
      rescue Exception
        break
      end
    end
    $all_nodes = good_nodes
  end

  def do_every_node!
    $all_nodes.each do |host|
      set_node! host
      yield
    end
  end

  def uncluster_everything!
    threads = $all_nodes.map do |host|
      Thread.new do
        Thread.current[:base_url] = build_base_uri(host)
        uuid_before = getj!("/pools")["uuid"]
        post!("/controller/ejectNode",
              :otpNode => "zzzzForce")
        poll_condition do
          (getj!("/pools")["uuid"] != uuid_before) rescue false
        end
      end
    end
    threads.each(&:join)
    nil
  end

  def add_node! node, cluster_node
    switching_node cluster_node do
      post!("/controller/addNode",
            :user => $username,
            :password => $password,
            :hostname => node)
    end
  end
end
