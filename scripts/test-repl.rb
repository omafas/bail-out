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

require_relative 'base-test'
require 'minitest/autorun'

class TestRepl < Minitest::Test
  include RESTMethods

  def all
    $all_nodes
  end

  def setup
    uncluster_everything!
    assert(all.size >= 2)
    # this is sadly needed so far
    sleep 4
    setup_node! all.first
    set_node! all.first
    cluster_two!
  end

  def teardown
    $base_url = "127.0.0.1:666677"
  end

  def cluster_two!
    add_node! all.last, all.first
    @nodenames = [all.first, all.last].map do |n|
      ni = switching_node(n) {getj!("/nodes/self")}
      ni["otpNode"]
    end
  end

  def print_cmd! cmd
    puts
    puts "========================================================="
    puts cmd
    puts "========================================================="
  end

  def create_bucket! name
    print_cmd! "create_bucket " + name
    post!("/pools/default/buckets",
          :name => name,
          :threadsNumber => 3,
          :replicaIndex => 1,
          :replicaNumber => 1,
          :ramQuotaMB => 100,
          :bucketType => "membase")
  end

  def create_document! bucket, vbucket, key, value
    print_cmd! "create_document bucket = #{bucket}, vbucket = #{vbucket}, key = #{key}, value = #{value}"
    resp = postj! "/diag/eval", "ns_server_testrunner_api:add_document(\"#{bucket}\",#{vbucket},\"#{key}\",\"#{value}\")."
    assert_equal "ok", resp["result"]
  end

  def get_document_replica bucket, vbucket, key
    print_cmd! "get_document_replica bucket = #{bucket}, vbucket = #{vbucket}, key = #{key}"
    postj! "/diag/eval", "ns_server_testrunner_api:get_document_replica(\"#{bucket}\",#{vbucket},\"#{key}\")."
  end

  def wait_for_document_replica bucket, vbucket, key
    print_cmd! "wait_for_document_replica bucket = #{bucket}, vbucket = #{vbucket}, key = #{key}"
    poll_condition do
      resp = get_document_replica bucket, vbucket, key
      if resp["result"] == "ok"
        yield resp["value"]
      else
        assert_equal "key_enoent", resp["status"]
        false
      end
    end
  end

  def rebalance!
    print_cmd! "rebalance"
    post!("/controller/rebalance",
          :knownNodes => @nodenames.join(","),
          :ejectedNodes => "")
    task = []
    poll_condition do
      task = getj!("/pools/default/tasks")[0]
      task["status"] == "notRunning"
    end
    assert_equal nil, task["errorMessage"]
  end

  def get_first_active_vbucket bucket
    vbuckets = postj! "/diag/eval", "ns_server_testrunner_api:get_active_vbuckets(\"#{bucket}\")."
    assert vbuckets.length > 0
    vbuckets[0]
  end

  def test_replication
    bucket = "bucket_of_stuff"
    key = "key1"
    value = "{a=1}"

    create_bucket! bucket
    rebalance!

    vbucket = 0
    switching_node(all.first) do
      vbucket = get_first_active_vbucket bucket
      create_document! bucket, vbucket, key, value
    end

    switching_node(all.last) do
      wait_for_document_replica bucket, vbucket, key do |v|
        assert_equal value, v
        true
      end
    end
  end
end
