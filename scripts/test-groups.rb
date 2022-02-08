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

class TestGroups < Minitest::Test
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
  end

  def teardown
    $base_url = "127.0.0.1:666677"
  end

  def cluster_two!
    add_node! all.last, all.first
    @hostnames = [all.first, all.last].map do |n|
      ni = switching_node(n) {getj!("/nodes/self")}
      ni["hostname"]
    end
  end

  def test_basic
    cluster_two!

    server_groups = getj! "/pools/default/serverGroups"
    groups = server_groups["groups"]
    assert groups
    assert groups.size == 1
    g = groups.first
    assert "Group 1", g["name"]
    assert_equal "/pools/default/serverGroups/0", g["uri"]
    nodes = g["nodes"]
    assert_equal 2, g["nodes"].size
    group_hostnames = nodes.map {|ni| ni["hostname"]}.sort
    assert_equal @hostnames.sort, group_hostnames
  end

  def test_group_ops
    post! "/pools/default/serverGroups", :name => "group 2"
    server_groups = getj!("/pools/default/serverGroups")["groups"]
    assert_equal(["Group 1", "group 2"], server_groups.map {|g| g["name"]})

    second_group = server_groups.detect {|g| g["name"] == "group 2"}["uri"]

    put!("/pools/default/serverGroups/0", :name => "group renamed")

    server_groups = getj!("/pools/default/serverGroups")["groups"]

    assert_equal(["group 2", "group renamed"], server_groups.map {|g| g["name"]}.sort)

    assert_equal("group renamed", server_groups.detect {|g| g["uri"] = "/pools/default/serverGroups/0"}["name"])

    assert_raises RestClient::ResourceNotFound do
      put!("/pools/default/serverGroups/nonexistant", :name => "group renamed")
    end

    assert_raises RestClient::ResourceNotFound do
      post!("/pools/default/serverGroups/0", :name => "group 2")
    end

    assert_raises RestClient::BadRequest do
      put!("/pools/default/serverGroups/0", :name => "group 2")
    end

    req!(:delete, second_group)

    server_groups = getj! "/pools/default/serverGroups"
    assert_equal(["group renamed"], server_groups["groups"].map {|g| g["name"]})

    assert_equal 1, server_groups["groups"].size
    assert_equal "/pools/default/serverGroups/0", server_groups["groups"][0]["uri"]
  end

  def test_simple_move
    post! "/pools/default/serverGroups", :name => "group 2"
    server_groups = getj!("/pools/default/serverGroups")

    assert_raises RestClient::BadRequest do
      put! server_groups["uri"], [].to_json
    end

    assert_raises RestClient::Conflict do
      baduri = server_groups["uri"] + "1"
      put! baduri, [].to_json
    end

    assert_raises RestClient::BadRequest do
      new_server_groups = JSON.parse(server_groups.to_json)
      new_server_groups["groups"].each {|g| g["nodes"] = []}
      put! new_server_groups["uri"], new_server_groups.to_json
    end

    new_server_groups = JSON.parse(server_groups.to_json)
    group_2 = new_server_groups["groups"].detect {|g| g["name"] == "group 2"}
    group_1 = (new_server_groups["groups"] - [group_2]).first

    assert_equal 1, group_1["nodes"].size
    assert_equal 0, group_2["nodes"].size

    group_2["nodes"].concat(group_1["nodes"])
    group_1["nodes"] = []

    put! new_server_groups["uri"], new_server_groups.to_json
    new_server_groups = getj!("/pools/default/serverGroups")
    group_2 = new_server_groups["groups"].detect {|g| g["name"] == "group 2"}
    group_1 = (new_server_groups["groups"] - [group_2]).first

    assert_equal 0, group_1["nodes"].size
    assert_equal 1, group_2["nodes"].size
    refute_equal new_server_groups["uri"], server_groups["uri"]
  end

  def test_add_to_group
    post! "/pools/default/serverGroups", :name => "group 2"
    post! "/pools/default/serverGroups", :name => "group 3"
    post! "/pools/default/serverGroups", :name => "group 4"

    server_groups = getj!("/pools/default/serverGroups")

    group_3_info = server_groups["groups"].detect {|g| g["name"] == "group 3"}

    assert_equal(group_3_info["uri"] + "/addNode", group_3_info["addNodeURI"])

    assert_raises RestClient::ResourceNotFound do
      post!(group_3_info["addNodeURI"] + "nonexistant",
            :user => $username,
            :password => $password,
            :hostname => all[1])
    end

    post!(group_3_info["addNodeURI"],
          :user => $username,
          :password => $password,
          :hostname => all[1])

    add_node! all.last, all.first


    all_mapped = all.map do |n|
      ni = switching_node(n) {getj!("/nodes/self")}
      ni["hostname"]
    end

    server_groups = getj!("/pools/default/serverGroups")

    expec = {
      "Group 1" => [all_mapped.first, all_mapped.last].sort,
      "group 2" => [],
      "group 3" => [all_mapped[1]],
      "group 4" => []
    }

    actual = server_groups["groups"].inject({}) do |h, g|
      h[g["name"]] = g["nodes"].map {|ni| ni["hostname"]}.sort
      h
    end

    assert_equal expec, actual
  end

  def test_bad_group_reassign
    post! "/pools/default/serverGroups", :name => "group 2"
    post! "/pools/default/serverGroups", :name => "group 3"
    post! "/pools/default/serverGroups", :name => "group 4"

    server_groups = getj!("/pools/default/serverGroups")

    group_3_info = server_groups["groups"].detect {|g| g["name"] == "group 3"}

    assert_equal(group_3_info["uri"] + "/addNode", group_3_info["addNodeURI"])

    post!(group_3_info["addNodeURI"],
          :user => $username,
          :password => $password,
          :hostname => all[1])

    add_node! all.last, all.first

    server_groups = getj!("/pools/default/serverGroups")

    bad_groups1 = JSON.parse(server_groups.to_json)
    bad_groups1["groups"].detect {|g| g["name"] == "Group 1"}["nodes"].pop

    begin
      put! bad_groups1["uri"], bad_groups1.to_json
      flunk
    rescue RestClient::BadRequest => exc
      response = JSON.parse(exc.response)
      assert_equal 2, response.size
      assert_equal "Bad input", response[0]
      stuff = response[1]
      expected_stuff = bad_groups1["groups"].map do |g|
        {
          "uri" => g["uri"],
          "name" => g["name"],
          "nodeNames" => g["nodes"].map {|ni| ni["otpNode"]}
        }
      end
      assert_equal expected_stuff, stuff
    end
  end

  def test_default_group
    add_node! all[1], all.first

    post! "/pools/default/serverGroups", :name => "group 2"
    server_groups = getj!("/pools/default/serverGroups")
    group_2 = server_groups["groups"].detect {|g| g["name"] == "group 2"}
    group_1 = server_groups["groups"].detect {|g| g["name"] == "Group 1"}
    assert_equal "/pools/default/serverGroups/0", group_1["uri"]

    assert_equal 2, group_1["nodes"].size

    # not empty yet
    assert_raises RestClient::BadRequest do
      req!(:delete, "/pools/default/serverGroups/0")
    end

    # move nodes to group 2
    group_2["nodes"] = group_1["nodes"]
    group_1["nodes"] = []

    put! server_groups["uri"], server_groups.to_json

    # verify move of nodes
    server_groups = getj!("/pools/default/serverGroups")
    group_2 = server_groups["groups"].detect {|g| g["name"] == "group 2"}
    group_1 = server_groups["groups"].detect {|g| g["name"] == "Group 1"}
    assert_equal "/pools/default/serverGroups/0", group_1["uri"]

    assert_equal 0, group_1["nodes"].size
    assert_equal 2, group_2["nodes"].size

    # now delete
    req!(:delete, "/pools/default/serverGroups/0")

    # verify lack of default group
    server_groups = getj!("/pools/default/serverGroups")
    group_2 = server_groups["groups"].detect {|g| g["name"] == "group 2"}
    group_1 = server_groups["groups"].detect {|g| g["name"] == "Group 1"}
    assert_nil group_1
    assert_equal 2, group_2["nodes"].size

    assert_equal 1, server_groups["groups"].size

    # see if we can successfully add node after default group is dead
    add_node! all[2], all.first

    server_groups = getj!("/pools/default/serverGroups")
    group_2 = server_groups["groups"].detect {|g| g["name"] == "group 2"}
    assert_equal 3, group_2["nodes"].size
    assert_equal 1, server_groups["groups"].size

    # see if we can create group with same name as (dead) default group
    post! "/pools/default/serverGroups", :name => "Group 1"
    server_groups = getj!("/pools/default/serverGroups")
    group_1 = server_groups["groups"].detect {|g| g["name"] == "Group 1"}
    refute_equal "/pools/default/serverGroups/0", group_1["uri"]
    assert_equal 0, group_1["nodes"].size
  end
end

