# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
from testlib.util import strings_to_services, Service
import requests
import os
import subprocess


class Node:
    def __init__(self, host, port, auth):
        self.host_with_brackets = testlib.maybe_add_brackets(host)
        self.url = f"http://{self.host_with_brackets}:{port}"
        self.hostname_cached = None
        self.host = host
        self.port = port
        self.auth = auth
        self.dbdir_cache = None
        self.data_path_cache = None
        self.tmp_path_cache = None
        self.logs_path_cache = None
        self.port_cache = {}
        self.tls_port_cache = {}
        self.services_cached = None
        self.session = None

    def __str__(self):
        return self.hostname()

    def __repr__(self):
        return self.__dict__.__repr__()

    def __lt__(self, other):
        return self.hostname() < other.hostname()

    def dbdir(self):
        if self.dbdir_cache is None:
            r = testlib.diag_eval(
                  self, "{ok, Dir} = ns_storage_conf:this_node_dbdir(), Dir.")
            self.dbdir_cache = r.text.strip('\"')

        return self.dbdir_cache

    def data_path(self):
        if self.data_path_cache is None:
            r = testlib.diag_eval(self, "path_config:component_path(data).")
            self.data_path_cache = r.text.strip('\"')

        return self.data_path_cache

    def tmp_path(self):
        if self.tmp_path_cache is None:
            r = testlib.diag_eval(self, "path_config:component_path(tmp).")
            self.tmp_path_cache = r.text.strip('\"')

        return self.tmp_path_cache

    def logs_path(self):
        if self.logs_path_cache is None:
            r = testlib.diag_eval(self, "{ok, Dir} = application:get_env(ns_server, error_logger_mf_dir), Dir.")
            self.logs_path_cache = r.text.strip('\"')

        return self.logs_path_cache

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

    def service_port(self, service: Service):
        if self.port_cache.get(service) is None:
            data = f'service_ports:get_port({service.port_atom()}).'
            r = testlib.post_succ(self, '/diag/eval', data=data)
            self.port_cache[service] = int(r.text)
        return self.port_cache[service]

    def tls_service_port(self, service: Service = None):
        if self.tls_port_cache.get(service) is None:
            if service is None:
                data = 'service_ports:get_port(ssl_rest_port).'
            else:
                data = f'service_ports:get_port({service.tls_port_atom()}).'
            r = testlib.post_succ(self, '/diag/eval', data=data)
            self.tls_port_cache[service] = int(r.text)
        return self.tls_port_cache[service]

    def service_url(self, service: Service):
        if service is None:
            port = self.port
        else:
            port = self.service_port(service)
        if service == Service.KV:
            prefix = "couchbase"
        else:
            prefix = "http"
        return f"{prefix}://{self.host_with_brackets}:{port}"

    def https_service_url(self, service: Service = None):
        port = self.tls_service_port(service)
        return f'https://{self.host_with_brackets}:{port}'

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

    def get_default_session(self):
        if self.session is None:
            self.session = requests.Session()
        return self.session

    def get_localtoken(self, master_password=None):
        token_path = os.path.join(self.data_path(), "localtoken")
        if master_password is None:
            master_password = ''
        return self.run_cbcat(master_password, token_path)

    def run_cbcat(self, master_password, encrypted_file_path):
        cbcat_path = testlib.get_utility_path("cbcat")
        gosecrets_cfg_path = os.path.join(self.data_path(), "config",
                                          "gosecrets.cfg")
        args = [cbcat_path, '--with-gosecrets', gosecrets_cfg_path,
                '--password', '-', encrypted_file_path]

        print(f'running cbcat with args: {args}')
        r = subprocess.run(args, input=master_password, text=True,
                           capture_output=True)

        assert r.returncode != 2, \
               f'Could not read file {encrypted_file_path}: ' \
               'Invalid master password'

        assert r.returncode == 0, \
               f'Could not read localtoken: cbcat returned non zero return ' \
               f'code: {r.returncode} \n' \
               f'stderr: {r.stderr}\n' \
               f'stdout: {r.stdout}'

        return r.stdout.rstrip()

    def get_cluster_membership(self):
        r = testlib.get_succ(self, '/nodes/self')
        return r.json().get('clusterMembership')