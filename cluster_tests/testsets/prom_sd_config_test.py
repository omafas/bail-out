# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This tests the /prometheus_sd_config endpoint which is intended to be
# used to configure prometheus (MB-55047) to get scrape targets on the
# fly instead of hard coding them in a file.
#
# The API accepts query parameters:
#
#   type={json|yaml}
#   disposition={inline|attachment}
#   port={secure|insecure}
#   network={default|external}
#
# The first of the options is the default. When disposition is "attachment"
# the extension for filename will be based on "type".

import testlib
import json
import yaml
import time


class PromSdConfigTest(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(min_num_nodes=3)

    def setup(self):
        self.alt_addr_url = "/node/controller/setupAlternateAddresses/external"
        self.is_enterprise = self.cluster.is_enterprise

    def teardown(self):
        for node in self.cluster._nodes:
            testlib.delete(node, self.alt_addr_url)

    def build_hostname(self, node_num):
        return f"{node_num}.{node_num}.{node_num}.{node_num}"

    def build_portnum(self, node_num, secure=False):
        Secure = "1" if secure else ""
        return f"{Secure}{node_num}{node_num}{node_num}{node_num}"

    # Setup alternate addresses and mgmt ports
    def setup_alt_addresses(self):
        node_num = 1

        for node in self.cluster._nodes:
            alt_hostname = self.build_hostname(node_num)
            mgmt_port = self.build_portnum(node_num)
            mgmtSSL_port = self.build_portnum(node_num, True)
            print(f"Setting alternate address for node {alt_hostname} "
                  f"port {mgmt_port} SSL port {mgmtSSL_port}")

            node_num += 1
            testlib.put_succ(node, self.alt_addr_url,
                             data={"hostname": f"{alt_hostname}",
                                   "mgmt": f"{mgmt_port}",
                                   "mgmtSSL": f"{mgmtSSL_port}"})

    # Verify the alternate addresses and kv ports
    def verify_alt_addresses(self):
        retries = 60
        while retries > 0:
            if self.verify_alt_addresses_inner():
                return
            time.sleep(0.5)
            retries -= 1
        raise RuntimeError("alternate addresses did not come up")

    def verify_alt_addresses_inner(self):
        r = testlib.get_succ(self.cluster, "/pools/default/nodeServices")
        rj = r.json()
        nodesExt = rj['nodesExt']
        assert (len(nodesExt) != 0)
        for node in nodesExt:
            if 'alternateAddresses' not in node:
                return False
            altaddr = node['alternateAddresses']
            external = altaddr['external']
            hostname = external['hostname']
            ports = external['ports']
            port = ports['mgmt']
            portSSL = ports['mgmtSSL']
            # The nodes in nodesExt can be in any order. We expect to find
            # nodes with names <n>.<n>.<n>.<n> and management ports
            # <n><n><n><n> and 1<n><n><n><n>. For example: node named
            # 2.2.2.2 with mgmt ports 2222 and 12222.
            node_num = int(hostname.split('.')[0])
            assert (hostname == self.build_hostname(node_num))
            assert (port == int(self.build_portnum(node_num)))
            assert (portSSL == int(self.build_portnum(node_num, True)))
        return True

    def build_url(self, ret_type, disposition, port, network,
                  return_cluster_labels=None):
        assert (ret_type in ["json", "yaml"])
        assert (disposition in ["inline", "attachment"])
        assert (port in ["secure", "insecure"])
        assert (network in ["default", "external"])
        assert (return_cluster_labels in
                [None, "none", "uuidOnly", "uuidAndName"])

        # Save args for validating the response
        self.ret_type = ret_type
        self.disposition = disposition
        self.port = port
        self.network = network
        self.return_cluster_labels = return_cluster_labels

        url = f"/prometheus_sd_config?type={ret_type}&" \
              f"disposition={disposition}&port={port}&network={network}"

        if return_cluster_labels is not None:
            url += f"&clusterLabels={return_cluster_labels}"

        print(f"\nTesting url: {url}")
        return url

    def get_sd_config(self, ret_type="json", disposition="inline",
                      port="secure", network="default"):
        url = self.build_url(ret_type, disposition, port, network)
        return testlib.get_succ(self.cluster, url)

    def verify_sd_config(self):
        if self.is_enterprise:
            # Requires SSL ports which are only supported on EE
            self.validate_response(self.get_sd_config(disposition="attachment"))
        self.validate_response(self.get_sd_config(ret_type="yaml",
                                                  port="insecure"))
        self.validate_response(self.get_sd_config(ret_type="yaml",
                                                  port="insecure",
                                                  network="external"))
        self.validate_response(self.get_sd_config(ret_type="yaml",
                                                  port="insecure",
                                                  network="external",
                                                  disposition="attachment"))

    def validate_response(self, resp):
        if self.ret_type == "json":
            r = resp.json()
        else:  # yaml
            yaml_resp = yaml.safe_load(resp.text)
            r = json.loads(json.dumps(yaml_resp))
        targets = r[0]["targets"]

        # Labels are returned only if they were "asked" for. This is to
        # ensure backwards compatibility.
        assert ("labels" not in r[0])

        if self.disposition == "attachment":
            content = resp.headers['content-disposition']
            expected = f'attachment; ' \
                f'filename="couchbase_sd_config_.{self.ret_type}"'
            assert (content == expected)

        node_num = 1
        for node in self.cluster._nodes:
            if self.network == "default":
                host = node.host
                port = node.port
                if self.port == "secure":
                    port = f"1{port}"
            else:  # external
                host = self.build_hostname(node_num)
                port = self.build_portnum(node_num, self.port == "secure")
            expected_hostname = f"{testlib.maybe_add_brackets(host)}:{port}"

            assert (expected_hostname in targets)
            targets.remove(expected_hostname)
            node_num += 1

        assert (len(targets) == 0)

    def verify_label_info(self):
        # Give the cluster a name
        testlib.post_succ(self.cluster, "/pools/default",
                          data={"clusterName": "prom_sd_config"})
        # Get the cluster UUID
        r = testlib.get_succ(self.cluster, "/pools/default/nodeServices")
        rj = r.json()
        clusterUUID = rj["clusterUUID"]

        for type in ["yaml", "json"]:
            for labels_wanted in ["none", "uuidOnly", "uuidAndName"]:
                url = self.build_url(ret_type=type,
                                     disposition="inline",
                                     port="insecure",
                                     network="default",
                                     return_cluster_labels=labels_wanted)
                resp = testlib.get_succ(self.cluster, url)
                self.validate_label_info_response(clusterUUID, resp)

    def validate_label_info_response(self, expected_cluster_uuid, resp):
        if self.ret_type == "json":
            r = resp.json()
        else:  # yaml
            yaml_resp = yaml.safe_load(resp.text)
            r = json.loads(json.dumps(yaml_resp))

        if self.return_cluster_labels == "uuidAndName":
            labels = r[0]["labels"]
            uuid_with_dashes = labels["cluster_uuid"]
            assert (uuid_with_dashes.replace("-", "") == expected_cluster_uuid)
            assert (labels["cluster_name"] == "prom_sd_config")
        elif self.return_cluster_labels == "uuidOnly":
            labels = r[0]["labels"]
            uuid_with_dashes = labels["cluster_uuid"]
            assert (uuid_with_dashes.replace("-", "") == expected_cluster_uuid)
            assert ("cluster_name" not in labels)
        else:
            assert ("labels" not in r[0])

    def negative_tests(self):
        # Failures occur when alternate addresses/ports are not configured
        url = self.build_url("json", "inline", "insecure", "external")
        testlib.get_fail(self.cluster, url, 400)
        url = self.build_url("json", "inline", "secure", "external")
        testlib.get_fail(self.cluster, url, 400)

    def sd_config_test(self):
        # Some negative tests before alternate addresses are specified
        self.negative_tests()

        # Setup alternate addresses/ports
        self.setup_alt_addresses()
        self.verify_alt_addresses()
        # Positive tests
        self.verify_sd_config()

        # Optional label info
        self.verify_label_info()
