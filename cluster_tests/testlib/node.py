# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
from testlib.util import strings_to_services


class Node:
    def __init__(self, host, port, auth):
        host_with_brackets = testlib.maybe_add_brackets(host)
        self.url = f"http://{host_with_brackets}:{port}"
        self.hostname_cached = None
        self.host = host
        self.port = port
        self.auth = auth
        self.data_path_cache = None
        self.tls_port_cache = None
        self.services_cached = None

    def __str__(self):
        return self.hostname()

    def __repr__(self):
        return self.__dict__.__repr__()

    def data_path(self):
        if self.data_path_cache is None:
            r = testlib.diag_eval(self, "path_config:component_path(data).")
            self.data_path_cache = r.text.strip('\"')

        return self.data_path_cache

    def addr(self, afamily=None):
        afamily_param = ''
        if afamily is not None:
            afamily_param = ', ' + ('inet6' if afamily == 'ipv6' else 'inet')
        data = f'misc:extract_node_address(node(){afamily_param}).'
        r = testlib.post_succ(self, '/diag/eval', data=data)
        return r.text.strip('"')

    def hostname(self):
        if self.hostname_cached is None:
            r = testlib.get_succ(self, '/nodes/self')
            self.hostname_cached = r.json()['hostname']
        return self.hostname_cached

    def afamily(self):
        afamily = testlib.get_succ(self, '/nodes/self').json()['addressFamily']
        assert afamily in ['inet', 'inet6'], f'unexpected afamily: {afamily}'
        return 'ipv6' if afamily == 'inet6' else 'ipv4'

    def tls_port(self):
        if self.tls_port_cache is None:
            data = 'service_ports:get_port(ssl_rest_port).'
            r = testlib.post_succ(self, '/diag/eval', data=data)
            self.tls_port_cache = int(r.text)
        return self.tls_port_cache

    def https_url(self):
        port = self.tls_port()
        host = testlib.maybe_add_brackets(self.host)
        return f'https://{host}:{port}'

    def otp_port(self, encryption=None):
        encryption_param = 'cb_dist:external_encryption()'
        if encryption is not None:
            encryption_param = 'true' if encryption else 'false'
        data = '{port, P, _} = cb_epmd:get_port(' \
                                'node(), ' \
                                'cb_dist:address_family(), ' \
                                f'{encryption_param}),' \
               'P.'
        r = testlib.post_succ(self, '/diag/eval', data=data)
        return int(r.text)

    def otp_node(self):
        # Don't use cached name as the otpNode may change when the node is added
        # or removed from a cluster
        r = testlib.get_succ(self, '/nodes/self')
        return r.json()['otpNode']


    def get_ns_server_pid(self):
        r = testlib.diag_eval(self, "os:getpid().")
        return int(r.text.replace('"',""))

    def get_services(self):
        if self.services_cached is None:
            r = testlib.get_succ(self, '/nodes/self')
            self.services_cached = strings_to_services(r.json()['services'])
        return self.services_cached

    def set_alternate_address(self, alt_address):
        testlib.put_succ(self,
                         '/node/controller/setupAlternateAddresses/external',
                         data={"hostname": alt_address})

    def get_alternate_addresses(self):
        r = testlib.get_succ(self, '/nodes/self')
        # .get('xyz') over ['xyz'] because we want to return None if this does
        # not exist for tests
        return r.json().get('alternateAddresses')
