#!/usr/bin/ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2015-2016 Couchbase, Inc.
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

require 'minitest/autorun'
require_relative 'base-test'


class TestCBAuth < Minitest::Test
  include RESTMethods

  SKIP_SETUP = ENV['CBAUTH_TEST_SKIP_SETUP']

  def all
    $all_nodes
  end

  def setup
    unless SKIP_SETUP
      uncluster_everything!
      # this is sadly needed so far
      sleep 4
      setup_node! all.first
    end
    set_node! all.first
    unless SKIP_SETUP
      all[1..-1].each {|n| add_node! n, all.first}
      rebalance!
    end
  end

  def teardown
    $base_url = nil
  end

  def print_cmd! cmd
    puts
    puts "========================================================="
    puts cmd
    puts "========================================================="
  end

  def rebalance!
    nodes = all.map do |n|
      switching_node(n) {getj!("/nodes/self")}["otpNode"]
    end
    print_cmd! "rebalance"
    post!("/controller/rebalance",
          :knownNodes => nodes.join(","),
          :ejectedNodes => "")
    task = []
    poll_condition do
      task = getj!("/pools/default/tasks")[0]
      task["status"] == "notRunning"
    end
    assert_equal nil, task["errorMessage"]
  end

  def create_bucket! name, password = ""
    print_cmd! "create_bucket " + name
    post!("/pools/default/buckets",
          :name => name,
          :threadsNumber => 3,
          :replicaIndex => 1,
          :replicaNumber => 1,
          :ramQuotaMB => 100,
          :bucketType => "membase",
          :authType => "sasl",
          :saslPassword => password)
  end

  def sh(*args)
    puts(args.join(" "))
    system(*args) || raise
  end

  def test_basic_stuff
    unless SKIP_SETUP
      create_bucket! "other", "apassword"
      create_bucket! "default"
      create_bucket! "foo", ""
    end

    puts post!("/diag/eval", 'ns_orchestrator:ensure_janitor_run({bucket, "default"})')
    puts post!("/diag/eval", 'ns_orchestrator:ensure_janitor_run({bucket, "other"})')
    puts post!("/diag/eval", 'ns_orchestrator:ensure_janitor_run({bucket, "foo"})')

    sh "go build -o /tmp/cbauth-example github.com/couchbase/cbauth/cmd/cbauth-example"
    sh "go build -o /tmp/multi-bucket-demo github.com/couchbase/cbauth/cmd/multi-bucket-demo"

    base_url = "http://#{all.first}"

    ENV["NS_SERVER_CBAUTH_URL"] = base_url + "/_cbauth"
    ENV["NS_SERVER_CBAUTH_RPC_URL"] = base_url + "/cbauthtest"
    ENV["NS_SERVER_CBAUTH_USER"] = $username
    ENV["NS_SERVER_CBAUTH_PWD"] = $password

    sh "/tmp/multi-bucket-demo --serverURL=#{base_url}"
    sh "/tmp/multi-bucket-demo --serverURL=#{base_url} --bucketName=default"
    sh "/tmp/multi-bucket-demo --serverURL=#{base_url} --bucketName=other"
    15.times do |i|
      sh "/tmp/multi-bucket-demo --serverURL=#{base_url} --bucketName=other -keyToSet='foo#{i+100}'"
    end

    puts "waiting for :44443 to be free"
    poll_condition do
      (TCPSocket.new("127.0.0.1", 44443).tap(&:close) && false) rescue true
    end
    puts ":44443 is free"

    token = post!("/diag/eval", 'menelaus_util:reply_text(Req, menelaus_ui_auth:generate_token({"Administrator", admin}), 200), done.')
    token_headers = {
      "Ns_server-Ui" => "yes",
      "Cookie" => "ui-auth-#{all.first.gsub(":", "%3A")}=#{token}"
    }
    token_headers2 = {
      "Ns_server-Ui" => "yes",
      "Ns_server-Auth-Token" => token
    }

    IO.popen("/tmp/cbauth-example --listen=127.0.0.1:44443 --mgmtURL=#{base_url}", "r+") do |f|
      poll_condition do
        TCPSocket.new("127.0.0.1", 44443).tap(&:close) rescue false
      end
      switching_node("127.0.0.1:44443") do
        getj! "/h/#{all.last}/other"
        getj! "/bucket/other"
        switching_username nil do
          getj! "/bucket/default"
          getj! "/bucket/other", token_headers
          getj! "/bucket/other", token_headers2
          assert_raises RestClient::Unauthorized do
            getj! "/bucket/other"
          end
        end
      end
      f.close_write
      f.read
    end
  end
end
