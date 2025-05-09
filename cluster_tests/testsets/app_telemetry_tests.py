# @author Couchbase <info@couchbase.com>
# @copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import random
import string

import socket
from contextlib import AbstractContextManager

from websockets.uri import parse_uri
from websockets.client import ClientProtocol
from websockets.http11 import Response
from websockets.frames import Frame, Opcode
from websockets.exceptions import InvalidStatus

import testlib


class AppTelemetryTests(testlib.BaseTestSet):
    def __init__(self, cluster):
        super().__init__(cluster)
        self.initial_app_telemetry_config = None

    @staticmethod
    def requirements():
        return [testlib.ClusterRequirements(edition="Enterprise",
                                            balanced=True,
                                            min_num_nodes=2,
                                            afamily='ipv4')]

    def setup(self):
        self.initial_app_telemetry_config = testlib.get_succ(
            self.cluster, "/settings/appTelemetry").json()
        testlib.post_succ(self.cluster, "/settings/appTelemetry",
                          data={"enabled": "true"})

        # Decrease scrape interval to avoid having to wait long for scrape
        testlib.diag_eval(self.cluster,
                          "ns_config:set(app_telemetry,"
                          "              [{enabled, true},"
                          "               {scrape_interval_seconds, 0}]).")

    def teardown(self):
        testlib.post_succ(self.cluster, "/settings/appTelemetry",
                          json=self.initial_app_telemetry_config)

    def simple_test(self):
        info = testlib.get_succ(self.cluster,
                                "/pools/default/nodeServices").json()
        nodes_ext = info.get('nodesExt')
        node0_ext = nodes_ext[0]
        node0_uuid = node0_ext['nodeUUID']
        node0_services = node0_ext['services']
        node0_port = node0_services['mgmt']
        hostname = self.cluster.connected_nodes[0].host
        node0_host = f"{hostname}:{node0_port}"
        node0 = self.cluster.get_node_from_hostname(node0_host)
        node0_path = node0_ext.get('appTelemetryPath')
        testlib.assert_eq(node0_path, '/_appTelemetry')

        node1_ext = nodes_ext[1]
        node1_uuid = node1_ext['nodeUUID']
        node1_services = node1_ext['services']
        node1_port = node1_services['mgmt']
        node1_host = f"{hostname}:{node1_port}"
        node1 = self.cluster.get_node_from_hostname(node1_host)

        (username, password) = self.cluster.auth
        with WebsocketConnection(hostname, node0_port,
                                 username, password, node0_path) as conn:
            resp = conn.connect()
            testlib.assert_eq(resp.status_code, 101)
            # Receive GET_TELEMETRY command
            frame = conn.get_next_frame()
            testlib.assert_eq(frame.opcode, Opcode.BINARY)
            testlib.assert_eq(frame.data, b'\x00')
            # Generate metric values
            metric_0, metric_1, metric_2 = [
                ''.join(random.choices(string.ascii_lowercase, k=10))
                for _ in range(3)]
            value = random.randrange(0, 1000000000)
            # Test multiple lines, multiple metrics, multiple nodes, and
            # a line fragmented over multiple frames, with an interleaved ping
            frame_1 = (b'\x00' + make_metric(metric_0, node0_uuid, 0) +
                       b'\n' + make_metric(metric_1, node1_uuid, value) +
                       f"\n{metric_2}{{node_uuid=".encode('utf-8'))

            # Add a long label to test a larger frame
            long_label = ''.join("x" for _ in range(100000))
            frame_2 = (f"\"{node0_uuid}\",x=\"{long_label}\"}} {value} "
                       "1695747260").encode('utf-8')
            # Send the lines of metrics as a fragmented message over multiple
            # frames, with a ping interleaved
            conn.send_binary(frame_1, fin=False)
            conn.send_ping(b'')
            # Wait for ping response
            frame = conn.get_next_frame()
            testlib.assert_eq(frame.opcode, Opcode.PONG)
            # Send rest of fragmented message
            conn.send_continuation(frame_2, fin=True)
            # Receive second GET_TELEMETRY message, to confirm nothing crashed
            frame = conn.get_next_frame()
            testlib.assert_eq(frame.opcode, Opcode.BINARY)
            testlib.assert_eq(frame.data, b'\x00')

        testlib.poll_for_condition(
            lambda:
            metrics_have_values(
                node0,
                {
                    # Test case for local node with zero value not ignored
                    f"{metric_0}{{le=\"0.001\"}}": "0",
                    # Test case for metric reported across fragment frames
                    f"{metric_2}{{}}": f"{value}"
                 }) and
            metrics_have_values(
                node1,
                {
                    # Test case for remote node
                    f"{metric_1}{{le=\"0.001\"}}": f"{value}"
                }),
            sleep_time=1, timeout=5)

    def disabled_test(self):
        # Disable app telemetry
        testlib.post_succ(self.cluster, "/settings/appTelemetry",
                          json={"enabled": "false"})
        try:
            info = testlib.get_succ(self.cluster,
                                    "/pools/default/nodeServices").json()
            nodes_ext = info.get('nodesExt')
            node0_ext = nodes_ext[0]
            node0_path = node0_ext.get('appTelemetryPath')
            # /_appTelemetry should not be advertised
            testlib.assert_eq(node0_path, None)

            testlib.get_fail(self.cluster, "/_appTelemetry", 404)
        finally:
            # Re-enable app telemetry
            testlib.post_succ(self.cluster, "/settings/appTelemetry",
                              json={"enabled": "true"})

    def max_clients_reached_test(self):
        info = testlib.get_succ(self.cluster,
                                "/pools/default/nodeServices").json()
        nodes_ext = info.get('nodesExt')
        node0_ext = nodes_ext[0]
        node0_services = node0_ext['services']
        node0_port = node0_services['mgmt']
        node0_path = node0_ext.get('appTelemetryPath')
        testlib.assert_eq(node0_path, '/_appTelemetry')

        (username, password) = self.cluster.auth
        hostname = self.cluster.connected_nodes[0].host
        try:
            testlib.post_succ(self.cluster, "/settings/appTelemetry",
                              json={"maxScrapeClientsPerNode": 1})

            with WebsocketConnection(hostname, node0_port, username, password,
                                     node0_path) as conn1:
                resp = conn1.connect()
                testlib.assert_eq(resp.status_code, 101)

                # Don't exit the first connection context, as that will close
                # the connection
                with WebsocketConnection(hostname, node0_port, username,
                                         password, node0_path) as conn2:
                    resp = conn2.connect(expected_exception=InvalidStatus)
                    testlib.assert_eq(resp.status_code, 429)
        finally:
            # Increase max clients back to default
            testlib.post_succ(self.cluster, "/settings/appTelemetry",
                              json={"maxScrapeClientsPerNode": 1024})

    def disconnect_on_disable_test(self):
        info = testlib.get_succ(self.cluster,
                                "/pools/default/nodeServices").json()
        nodes_ext = info.get('nodesExt')
        node0_ext = nodes_ext[0]
        node0_services = node0_ext['services']
        node0_port = node0_services['mgmt']
        node0_path = node0_ext.get('appTelemetryPath')
        testlib.assert_eq(node0_path, '/_appTelemetry')

        (username, password) = self.cluster.auth
        hostname = self.cluster.connected_nodes[0].host

        try:
            with WebsocketConnection(hostname, node0_port, username, password,
                                     node0_path) as conn:
                resp = conn.connect()
                testlib.assert_eq(resp.status_code, 101)
                # Get first scrape
                frame = conn.get_next_frame()
                testlib.assert_eq(frame.opcode, Opcode.BINARY)
                testlib.assert_eq(frame.data, b'\x00')
                # Disable app telemetry
                testlib.post_succ(self.cluster, "/settings/appTelemetry",
                                  json={"enabled": "false"})
                # Send response to first scrape (to avoid getting timed out)
                conn.send_binary(b'\x00')
                # Receive CLOSE frame
                try:
                    frame = conn.get_next_frame()
                    testlib.assert_eq(frame.opcode, Opcode.CLOSE)
                except ConnectionError:
                    # See MB-65238. The websocket may be closed before receiving
                    # the response to the CLOSE message, which is sent in
                    # WebsocketConnection._get_and_send_data
                    pass
        finally:
            # Re-enable app telemetry
            testlib.post_succ(self.cluster, "/settings/appTelemetry",
                              json={"enabled": "true"})


def make_metric(metric, uuid, value):
    return (f"{metric}{{le=\"0.001\","
            f"agent=\"agent\","
            f"bucket=\"anything\","
            f"node_uuid=\"{uuid}\"}}"
            f" {value} 1695747260").encode('utf-8')


def metrics_have_values(node, expected_metrics):
    resp = testlib.get_succ(node, "/metrics")
    for line in resp.text.splitlines():
        parts = line.split(' ')
        if len(parts) == 2:
            metric, value = parts
            expected_value = expected_metrics.pop(metric, None)
            if expected_value is not None and value != expected_value:
                return False
    return len(expected_metrics) == 0


class WebsocketConnection(AbstractContextManager):

    def __init__(self, host, port, username, password, path):
        self.host = host
        self.port = port
        self.sock = None
        url = f"ws://{username}:{password}@{host}:{port}{path}"
        self.protocol = ClientProtocol(parse_uri(url))
        self.events = []

    def __enter__(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=5)
        self.sock.settimeout(5)
        return self

    def connect(self, expected_exception=None) -> Response:
        # Perform handshake
        handshake = self.protocol.connect()
        self.protocol.send_request(handshake)
        self._get_and_send_data()
        event = self._get_next_event()
        if expected_exception is not None:
            assert isinstance(self.protocol.handshake_exc, expected_exception)
        elif self.protocol.handshake_exc is not None:
            raise self.protocol.handshake_exc
        return event

    def get_next_frame(self) -> Frame:
        event = self._get_next_event()
        assert isinstance(event, Frame)
        return event

    def send_binary(self, binary, fin=True):
        self.protocol.send_binary(binary, fin)
        self._get_and_send_data()

    def send_ping(self, binary):
        self.protocol.send_ping(binary)
        self._get_and_send_data()

    def send_continuation(self, binary, fin=True):
        self.protocol.send_continuation(binary, fin)
        self._get_and_send_data()

    def _get_next_event(self):
        # Only expect to receive an event if we haven't already
        if len(self.events) == 0:
            # Receive from network
            self._receive_to_buffer()
            # Update list of events
            events = self.protocol.events_received()
            self.events += events
        # Check that an event was returned
        if len(self.events) == 0:
            return None
        else:
            # Return next event
            return self.events.pop(0)

    def _receive_to_buffer(self, buffer_size=1024):
        data = self.sock.recv(buffer_size)
        self.protocol.receive_data(data)
        self._get_and_send_data()

    def _get_and_send_data(self):
        for data in self.protocol.data_to_send():
            self.sock.send(data)

    def __exit__(self, *exc_details):
        self.sock.close()


