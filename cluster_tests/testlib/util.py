# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
from enum import Enum
from typing import Union, List, Dict
import re


class Service(Enum):
    KV = "kv"
    INDEX = "index"
    QUERY = "n1ql"
    FTS = "fts"
    BACKUP = "backup"
    EVENTING = "eventing"
    CBAS = "cbas"
    # Views isn't really a service, from the perspective of cluster_connect
    VIEWS = None

    def port_atom(self):
        return {Service.KV: "memcached_port",
                Service.QUERY: "query_port",
                Service.CBAS: "cbas_http_port",
                Service.VIEWS: "capi_port"}[self]

    def tls_port_atom(self):
        return {
            Service.KV: "memcached_ssl_port",
            Service.QUERY: "ssl_query_port"
        }[self]


def services_to_strings(services: Union[List[Service],
                                        Dict[str, List[Service]]]):
    if isinstance(services, list):
        for service in services:
            if not isinstance(service, Service):
                raise ValueError(f"Invalid service: {service}. Must be an "
                                 f"instance of the enum Service")
        return service_list_to_strings(services)
    elif isinstance(services, dict):
        for node, service_list in services.items():
            if not re.match(r"n[0-9]+", node):
                raise ValueError(f"Invalid node: '{node}'. Must be of the form "
                                 "'n[0-9]+'")
            if not isinstance(service_list, list):
                raise ValueError(f"Invalid service list: '{service_list}'. "
                                 f"Must be a list of Services")
        return {node: service_list_to_strings(service_list)
                for node, service_list in services.items()}
    else:
        raise ValueError(f"Invalid services: '{services}'. Must be a list of "
                         f"Service or dict from node to Services")


def service_list_to_strings(services: List[Service]):
    return [service.value for service in services
            # Ignore non-optional services (e.g. views)
            if service.value is not None]

def strings_to_services(services: List[str]):
    return list(map(lambda service: {"kv": Service.KV,
                              "index": Service.INDEX,
                              "n1ql": Service.QUERY,
                              "fts": Service.FTS,
                              "backup": Service.BACKUP,
                              "eventing": Service.EVENTING,
                              "cbas": Service.CBAS}[service],
                services))

def service_to_memory_quota_key(service: Service):
    return {Service.KV: "memoryQuota",
            Service.INDEX: "indexMemoryQuota",
            Service.QUERY: "queryMemoryQuota",
            Service.FTS: "ftsMemoryQuota",
            Service.EVENTING: "eventingMemoryQuota",
            Service.CBAS: "cbasMemoryQuota"}[service]
