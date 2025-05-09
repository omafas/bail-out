# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Dict, List, Union

import testlib
from testlib.cluster import Cluster, build_cluster
from testlib import get_succ
from testlib.util import Service, services_to_strings
import random


MIN_MEM_QUOTA = 256
MAX_VBUCKET_NUM = 1024


class ClusterRequirements:
    @testlib.no_output_decorator
    def __init__(self,
                 edition=None, num_nodes=None, min_num_nodes=None,
                 memsize=None, min_memsize=None, num_connected=None,
                 min_num_connected=None, afamily=None, services=None,
                 master_password_state=None, num_vbuckets=None,
                 encryption=None, balanced=None, buckets=None,
                 test_generated_cluster=None, dev_preview=None):

        def maybe(ReqClass, *args):
            if all(x is None for x in args):
                return None
            return ReqClass(*args)
        self.requirements = \
            {
                'edition': maybe(Edition, edition),
                'num_nodes': maybe(NumNodes,
                                   num_nodes, min_num_nodes, num_connected,
                                   min_num_connected),
                'memsize': maybe(MemSize, memsize, min_memsize),
                'afamily': maybe(AFamily, afamily),
                'services': maybe(Services, services),
                'master_password_state': maybe(MasterPasswordState,
                                               master_password_state),
                'num_vbuckets': maybe(NumVbuckets, num_vbuckets),
                'encryption': maybe(N2nEncryption, encryption),
                'balanced': maybe(Balanced, balanced),
                'buckets': maybe(Buckets, buckets),
                'test_generated_cluster': maybe(TestGeneratedCluster,
                                                test_generated_cluster),
                'dev_preview': maybe(DevPreview, dev_preview)
            }

    def __str__(self):
        all_reqs = sorted(self.as_list(), key= lambda x: x.__class__.__name__)
        immutable_requirements = list(filter(lambda x: not x.can_be_met(),
                                             all_reqs))
        mutable_requirements = list(filter(lambda x: x.can_be_met(),
                                           all_reqs))
        # List the requirements with mutables last, so that compatible
        # configurations would be adjacent when ordered by string
        requirements = immutable_requirements + mutable_requirements
        return ', '.join([str(req) for req in requirements])

    def __repr__(self):
        return str(self)

    @staticmethod
    def get_default_start_args():
        return {
                # Don't rename first node when second node joins the cluster,
                # as this makes node removal / addition testing more complicated
                'dont_rename': True,
                # Wait until nodes are up before cluster_run_lib returns
                'wait_for_start': True,
                # Without this we would have cluster outputs overlapping test
                # output
                'nooutput': True,
                'num_nodes': 1,
                # Reduce the default num vbuckets to 16 to aid with faster
                # rebalance time. If a specific test needs 1024 vbuckets,
                # it can be passed down as a requirement.
                'num_vbuckets': 16,
                # The BYPASS_SASLAUTHD env affects external authentication,
                # and as a consequence affects external authentication tests.
                # Setting explicit value for BYPASS_SASLAUTHD here makes tests
                # more deterministic. None is used by default because
                # extenal authentication should not be passed by by default.
                'env': {'BYPASS_SASLAUTHD': None}
        }

    def update(self, new_requirements):
        for k in new_requirements.requirements:
            self.requirements[k] = deepcopy(new_requirements.requirements[k])

    @staticmethod
    def get_default_connect_args(start_args):
        return {
                'protocol': "ipv4",
                'num_nodes': start_args['num_nodes'],
                # We don't want to create a bucket by default as we rely on
                # testset requirements to handle bucket creation and teardown
                'create_bucket': False,
                # Not every testset needs a balanced cluster. Those that do
                # should specify the balanced=True requirement to override this
                'do_rebalance': False
               }

    @staticmethod
    def get_default_disconnected_args(address, start_args, connect_args):
        start = start_args['start_index'] + connect_args['num_nodes']
        num = start_args['num_nodes'] - connect_args['num_nodes']
        return {
                'start_index': start,
                'num_nodes': num,
                'protocol': connect_args['protocol'],
                'hostname': address
               }

    def as_list(self):
        return list(filter(lambda x: x is not None, self.requirements.values()))

    @testlib.no_output_decorator
    def create_cluster(self, auth, cluster_index, tmp_cluster_dir,
                       first_node_index, connect=True):

        start_args = {'start_index': first_node_index,
                      'root_dir': f"{tmp_cluster_dir}-{cluster_index}"}
        start_args.update(self.get_default_start_args())
        for requirement in self.as_list():
            start_args.update(requirement.start_args)

        connect_args = {'start_index': first_node_index}
        connect_args.update(self.get_default_connect_args(start_args))
        for requirement in self.as_list():
            connect_args.update(requirement.connect_args)

        # We use the raw ip address instead of 'localhost', as it isn't accepted
        # by the addNode or doJoinCluster endpoints
        # IPV6 uses [::1] instead of 127.0.0.1
        address = "::1" if connect_args['protocol'] == "ipv6" else "127.0.0.1"
        disconnected_args = self.get_default_disconnected_args(address,
                                                               start_args,
                                                               connect_args)

        cluster = build_cluster(address=address,
                                auth=auth,
                                cluster_index=cluster_index,
                                start_args=start_args,
                                connect_args=connect_args,
                                connect=connect,
                                disconnected_args=disconnected_args)

        cluster.set_requirements(self)
        # should not really repair anything, just checking that all
        # requirements are satisfied after cluster creation
        unmet_requirements = cluster.maybe_repair_cluster_requirements()
        if len(unmet_requirements) > 0:
            unmet_str = ', '.join(str(r) for r in unmet_requirements)
            raise RuntimeError("Newly created cluster still has the following "
                               f"requirements unmet: {unmet_str}")

        return cluster

    # Given a cluster, checks if any requirements are not satisfied, and
    # returns the unsatisfied requirements
    @testlib.no_output_decorator
    def is_satisfiable(self, cluster):
        unsatisfied = []
        satisfiable = True
        for requirement in self.as_list():
            if not requirement.is_met(cluster):
                unsatisfied.append(requirement)
                if not requirement.can_be_met():
                    satisfiable = False
        return satisfiable, unsatisfied

    @testlib.no_output_decorator
    def get_unmet_requirements(self, cluster):
        _, unmet_requirements = self.is_satisfiable(cluster)
        return unmet_requirements

    # Determines whether this set of requirements will be satisfiable with a
    # cluster satisfying some 'other' ClusterRequirements
    @testlib.no_output_decorator
    def satisfied_by(self, other):
        for requirement in self.as_list():
            if not (any(requirement == other_requirement
                        for other_requirement in other.requirements)):
                return False
        return True

    @testlib.no_output_decorator
    def intersect(self, other):
        new_reqs = ClusterRequirements()
        for k in self.requirements:
            r1 = self.requirements[k]
            r2 = other.requirements[k]
            if r1 == r2:
                new_reqs.requirements[k] = r1
            elif r1 is None:
                new_reqs.requirements[k] = r2
            elif r2 is None:
                new_reqs.requirements[k] = r1
            else:
                res, new_r = r1.intersect(r2)
                if res:
                    new_reqs.requirements[k] = new_r
                else:
                    return False, new_reqs

        return True, new_reqs

    def randomize_unset_requirements(self):
        req_dict = {}
        for k in self.requirements:
            r = self.requirements[k]
            if r is not None:
                req_dict.update(r.get())

        generation_order = [('edition', Edition),
                            ('num_nodes', NumNodes),
                            ('memsize', MemSize),
                            ('afamily', AFamily),
                            ('services', Services),
                            ('master_password_state', MasterPasswordState),
                            ('num_vbuckets', NumVbuckets),
                            ('encryption', N2nEncryption),
                            ('balanced', Balanced),
                            ('buckets', Buckets),
                            ('test_generated_cluster', TestGeneratedCluster),
                            ('dev_preview', DevPreview)]
        for req_name, req_class in generation_order:
            if self.requirements[req_name] is None:
                new_req = req_class.random(req_dict)
                self.requirements[req_name] = new_req
                req_dict.update(new_req.get())

        for k in self.requirements:
            assert self.requirements[k] is not None, \
                   f'please define randomization for "{k}" requirement'


class Requirement(ABC):
    def __init__(self, **kwargs):
        # In order to provide a string representation of the requirement, we
        # need to be provided with a names and values in the form of kwargs
        self._kwargs = kwargs

        # Override to make a requirement that depends on arguments for
        # cluster_run_lib.start()
        self.start_args = {}
        # Override to make a requirement that depends on arguments for
        # cluster_run_lib.connect()
        self.connect_args = {}

    def __str__(self):
        return ",".join([f"{key}={value}"
                        for key, value in self._kwargs.items()
                        if value is not None])

    def __repr__(self):
        return self.__dict__.__repr__()

    def __eq__(self, other):
        return str(self) == str(other)

    @abstractmethod
    def is_met(self, cluster):
        raise NotImplementedError()

    # Override if make_met can be called on an existing cluster
    def can_be_met(self):
        return False

    # Override to provide a way of satisfying a requirement after the cluster
    # has already been created
    def make_met(self, cluster):
        raise RuntimeError(f"Cannot change Requirement {self} after cluster "
                           f"created")

    def intersect(self, other):
        return False, None

    def get(self):
        return self._kwargs

    @staticmethod
    def random(requirements_dict):
        raise NotImplementedError()


class Edition(Requirement):
    editions = ["Community", "Enterprise", "Serverless", "Provisioned"]

    def __init__(self, edition):
        super().__init__(edition=edition)

        if edition not in Edition.editions:
            raise ValueError(f"Edition must be in {Edition.editions}")

        self.edition = edition

        if self.edition == "Community":
            self.start_args = {'force_community': True,
                               'run_serverless': False}
        elif self.edition == "Enterprise":
            self.start_args = {'force_community': False,
                               'run_serverless': False}
        elif self.edition == "Serverless":
            self.start_args = {'force_community': False,
                               'run_serverless': True}
        elif self.edition == "Provisioned":
            self.start_args = {'force_community': False,
                               'run_provisioned': True}

    def __str__(self):
        return self.edition

    def is_met(self, cluster: Cluster):
        if self.edition == "Community":
            return not cluster.is_enterprise and not cluster.is_serverless
        elif self.edition == "Enterprise":
            return cluster.is_enterprise and not cluster.is_serverless
        elif self.edition == "Serverless":
            return cluster.is_enterprise and cluster.is_serverless
        elif self.edition == "Provisioned":
            return cluster.is_enterprise and cluster.is_provisioned

    @staticmethod
    def random(req_dict):
        available_editions = Edition.editions.copy()
        if not Edition.is_community_supported(req_dict.get('deploy', None)) \
           or req_dict.get('encryption', False) \
           or req_dict.get('afamily', None) == 'ipv6':
            available_editions.remove('Community')
        if not Edition.is_serverless_supported(req_dict.get('deploy', None)):
            available_editions.remove('Serverless')
        random_edition = random.choice(available_editions)
        return Edition(random_edition)

    @staticmethod
    def is_community_supported(deploy):
        if deploy is None:
            return True
        supported_configurations = Services.community_configurations

        if isinstance(deploy, list):
            return deploy in supported_configurations

        for n in deploy:
            if deploy[n] not in supported_configurations:
                return False
        return True

    @staticmethod
    def is_serverless_supported(deploy):
        if deploy is None:
            return True

        # When Analytics is started using the serverless profile, it expects
        # the blob storage settings to be set before the cluster is
        # initialized to enable bootstrapping in compute-storage separation
        # mode. We don't do that in cluster_run environment currently, so
        # for that reason we don't use Analytics in tests for serverless.
        if isinstance(deploy, list):
            return 'cbas' not in deploy

        for n in deploy:
            if 'cbas' in deploy[n]:
                return False
        return True


class NumNodes(Requirement):
    def __init__(self, num_nodes, min_num_nodes, num_connected,
                 min_num_connected):
        super().__init__(num_nodes=num_nodes, min_num_nodes=min_num_nodes,
                         num_connected=num_connected,
                         min_num_connected=min_num_connected)
        # Check requirement values are valid
        if num_nodes is None and min_num_nodes is None:
            raise ValueError("num_nodes and min_num_nodes can't both be None")
        if num_nodes is not None and min_num_nodes is not None:
            raise ValueError("num_nodes and min_num_nodes are mutually " \
                             "exclusive")
        if num_connected is not None and min_num_connected is not None:
            raise ValueError("num_connected and min_num_connected are " \
                             "mutually exclusive")
        if num_nodes is not None and num_nodes < 1:
            raise ValueError(f"num_nodes must be a positive integer")
        if min_num_nodes is not None and min_num_nodes < 1:
            raise ValueError(f"min_num_nodes must be a positive integer")
        if num_connected is not None and num_connected < 1:
            raise ValueError("num_connected must be at least 1")
        if min_num_connected is not None and min_num_connected < 1:
            raise ValueError("min_num_connected must be at least 1")
        if num_connected is not None:
            if num_nodes is not None:
                if num_connected > num_nodes:
                    raise ValueError("num_nodes cannot be less than " \
                                     "num_connected")
            elif min_num_nodes is not None:
                if num_connected > min_num_nodes:
                    raise ValueError("min_num_nodes cannot be less than " \
                                     "num_connected")
        if min_num_connected is not None:
            if num_nodes is not None:
                if min_num_connected > num_nodes:
                    raise ValueError("num_nodes cannot be less than " \
                                     "min_num_connected")
            elif min_num_nodes is not None:
                if min_num_connected > min_num_nodes:
                    raise ValueError("min_num_nodes cannot be less than " \
                                     "min_num_connected")

        self.num_nodes = num_nodes
        self.min_num_nodes = min_num_nodes
        self.num_connected = num_connected
        self.min_num_connected = min_num_connected
        # We use None as a placeholder for when we want all nodes connected
        if self.num_connected is None and self.min_num_connected is None:
            self.num_connected = self.num_nodes
            self.min_num_connected = self.min_num_nodes
        self.start_args = {'num_nodes': num_nodes if num_nodes is not None
                                                  else min_num_nodes}
        self.connect_args = \
            {'num_nodes': self.num_connected if self.num_connected is not None
                                             else self.min_num_nodes}

    def __str__(self):
        def format_num(num1, min_num2):
            if num1 is not None:
                return f"{num1}"
            elif min_num2 is not None:
                return f"min {min_num2}"
            else:
                return ""

        show_connected = False
        if self.num_connected is not None and \
           self.num_connected != self.num_nodes and \
           self.num_connected != self.min_num_nodes:
            show_connected = True
        elif self.min_num_connected is not None and \
             self.min_num_connected != self.min_num_nodes and \
             self.min_num_connected != self.num_nodes:
            show_connected = True

        total = format_num(self.num_nodes, self.min_num_nodes)
        connected = format_num(self.num_connected, self.min_num_connected)

        if show_connected:
            return f"{total} node(s) ({connected} connected)"
        return f"{total} node(s)"

    def is_met(self, cluster):
        if self.num_nodes is not None and \
           len(cluster._nodes) != self.num_nodes:
            return False
        if self.num_connected is not None and \
           len(cluster.connected_nodes) != self.num_connected:
            return False
        if self.min_num_nodes is not None and \
           len(cluster._nodes) < self.min_num_nodes:
            return False
        if self.min_num_connected is not None and \
           len(cluster.connected_nodes) < self.min_num_connected:
            return False
        return True


    def intersect(self, other):
        res1, new_min, new_num = intersect_limits(self.min_num_nodes,
                                                  self.num_nodes,
                                                  other.min_num_nodes,
                                                  other.num_nodes)

        res2, new_min_con, new_con = intersect_limits(self.min_num_connected,
                                                      self.num_connected,
                                                      other.min_num_connected,
                                                      other.num_connected)
        if res1 and res2:
            return True, NumNodes(new_num, new_min, new_con, new_min_con)
        return False, None

    @staticmethod
    def random(req_dict):
        return NumNodes(random.randint(1, 4), None, None, None)


class MemSize(Requirement):
    def __init__(self, memsize, min_memsize):
        super().__init__(memsize=memsize, min_memsize=min_memsize)
        if memsize is not None and min_memsize is not None:
            raise ValueError("memsize and min_memsize are mutually exclusive")
        if memsize is not None and memsize < MIN_MEM_QUOTA:
            raise ValueError("memsize must be a positive " \
                             f"integer >= {MIN_MEM_QUOTA}")
        if min_memsize is not None and min_memsize < MIN_MEM_QUOTA:
            raise ValueError("min_memsize must be a positive " \
                             f"integer >= {MIN_MEM_QUOTA}")
        self.memsize = memsize
        self.min_memsize = min_memsize
        if memsize is not None:
            self.memsize_to_use = memsize
        elif min_memsize is not None:
            self.memsize_to_use = min_memsize
        self.connect_args = {'memsize': self.memsize_to_use}

    def __str__(self):
        if self.memsize is not None:
            return f"kv quota {self.memsize}MB"
        else:
            return f"min kv quota {self.min_memsize}MB"

    def is_met(self, cluster):
        memsize = cluster.memory_quota()
        if self.memsize is not None:
            return memsize == self.memsize
        return memsize >= self.min_memsize

    def can_be_met(self):
        return True

    def make_met(self, cluster):
        testlib.post_succ(cluster, "/pools/default",
                          data={"memoryQuota": self.memsize_to_use})
        cluster.memsize = self.memsize_to_use

    def intersect(self, other):
        res, new_min, new_mem = intersect_limits(self.min_memsize,
                                                 self.memsize,
                                                 other.min_memsize,
                                                 other.memsize)
        if res:
            return True, MemSize(new_mem, new_min)
        return False, None

    @staticmethod
    def random(req_dict):
        return MemSize(random.randint(MIN_MEM_QUOTA, 2048), None)


class AFamily(Requirement):
    def __init__(self, afamily):
        super().__init__(afamily=afamily)
        self.afamily = afamily
        self.connect_args = {"protocol": afamily}

    def __str__(self):
        return self.afamily

    def is_met(self, cluster):
        # The address family is labeled using "inet" in /pools/nodes
        afamily_translate = {
            "ipv4": "inet",
            "ipv6": "inet6"
        }
        res = get_succ(cluster, "/pools/nodes")
        return all([node["addressFamily"] == afamily_translate[self.afamily]
                    for node in res.json()["nodes"]])

    @staticmethod
    def random(req_dict):
        support_ipv6 = (req_dict.get('edition', None) != 'Community')
        afamily = random.choice(['ipv4', 'ipv6']) if support_ipv6 else 'ipv4'
        return AFamily(afamily)


class Services(Requirement):
    community_configurations = [[Service.KV],
                                [Service.KV, Service.INDEX, Service.QUERY],
                                [Service.KV, Service.INDEX, Service.QUERY,
                                 Service.FTS]]

    def __init__(self, deploy: Union[List[Service], Dict[str, List[Service]]]):
        self.deploy = services_to_strings(deploy)
        super().__init__(deploy=self.deploy)
        self.connect_args = {"deploy": self.deploy}

    def __str__(self):
        if isinstance(self.deploy, list):
            return "deploy: " + " ".join(self.deploy)
        else:
            def format_node(key, value):
                return f"{key}(" + " ".join(value) + ")"
            return "deploy: " + " ".join([format_node(key, self.deploy[key])
                                          for key in self.deploy])

    def is_met(self, cluster):
        for i, node in enumerate(sorted(cluster.connected_nodes)):
            # We can't take information for all nodes from a single /pools/nodes
            # because we don't know which node is which
            res = get_succ(node, "/pools/nodes").json()
            this_node_services = []
            for node_info in res['nodes']:
                if 'thisNode' in node_info and node_info['thisNode']:
                    this_node_services = node_info['services']

            services_to_check = []
            if isinstance(self.deploy, list):
                services_to_check = self.deploy
            else:
                nname = f'n{i}'
                if nname in self.deploy:
                    services_to_check = self.deploy[nname]

            for s in services_to_check:
                if s not in this_node_services:
                    return False

        return True

    @staticmethod
    def random(req_dict):
        community = (req_dict.get('edition', None) == 'Community')
        serverless = (req_dict.get('edition', None) == 'Serverless')
        if community:
            services = random.choice(Services.community_configurations)
        else:
            services = list(Service)
            # When Analytics is started using the serverless profile, it
            # expects the blob storage settings to be set before the cluster is
            # initialized to enable bootstrapping in compute-storage separation
            # mode. We don't do that in cluster_run environment currently, so
            # for that reason we don't use Analytics in tests for serverless.
            if serverless:
                services.remove(Service.CBAS)
            services.remove(Service.KV)
            services = [Service.KV] + random.sample(
                                        services,
                                        k=random.randint(0, len(services) - 1))
        return Services(services)


# We are not enforcing it when creating a cluster (like by setting something
# in connect_args), because
# (1) in practice we only need to check that master password is not set, which
#     is the default behavior
# (2) it would be an extra work with no real benefit
#
# We also are not implementing make_met() because in order to do that we would
# need to use the API that we want to test (which would be strange)
class MasterPasswordState(Requirement):
    def __init__(self, state):
        super().__init__(master_password_state=state)
        self.master_password_state=state

    def __str__(self):
        if self.master_password_state == 'default':
            return "master pass not set"
        else:
            return f"master pass state: {self.master_password_state}"

    def is_met(self, cluster):
        for n in cluster.connected_nodes:
            r = testlib.get(n, "/nodes/self/secretsManagement")
            if r.status_code == 200:
                r = r.json()
                pass_state = r['encryptionService']['passwordState']
                if pass_state != self.master_password_state:
                    return False
            elif r.status_code == 400 and \
                 'endpoint requires enterprise edition' in r.text:
                if self.master_password_state != 'default':
                    return False
            else:
                return False
        return True

    @staticmethod
    def random(req_dict):
        return MasterPasswordState('default')


class NumVbuckets(Requirement):
    def __init__(self, num_vbuckets):
        super().__init__(num_vbuckets=num_vbuckets)

        if num_vbuckets <= 0:
            raise ValueError("num_vbuckets needs to be > 0")

        if num_vbuckets > MAX_VBUCKET_NUM:
            raise ValueError(f"num_vbuckets needs to be <= {MAX_VBUCKET_NUM}")

        self.num_vbuckets = num_vbuckets
        self.start_args = {"num_vbuckets": num_vbuckets}

    def __str__(self):
        return f'{self.num_vbuckets} vbuckets'

    def is_met(self, cluster):
        def get_default_num_vbuckets(bucket_type):
            func = f"ns_bucket:get_default_num_vbuckets({bucket_type})"
            r = testlib.diag_eval(cluster, code=func)
            return r.content.decode('ascii')

        # The default number of vbuckets for 'magma' is different than for
        # 'couchstore' so we check for either.
        couchstore_num_vbuckets = int(get_default_num_vbuckets("couchstore"))
        magma_num_vbuckets = int(get_default_num_vbuckets("magma"))

        return (couchstore_num_vbuckets == self.num_vbuckets or
                magma_num_vbuckets == self.num_vbuckets)

    @staticmethod
    def random(req_dict):
        return NumVbuckets(random.randint(16, MAX_VBUCKET_NUM))


class N2nEncryption(Requirement):
    def __init__(self, encryption):
        super().__init__(encryption=encryption)
        self.encryption = encryption
        self.connect_args = {"encryption": encryption}

    def __str__(self):
        if self.encryption:
            return "n2n encryption"
        else:
            return "no n2n encryption"

    def is_met(self, cluster):
        res = get_succ(cluster, "/pools/nodes")
        return all([node["nodeEncryption"] == self.encryption
                    for node in res.json()["nodes"]])

    def can_be_met(self):
        return True

    def make_met(self, cluster):
        cluster.toggle_n2n_encryption(enable=self.encryption)

    @staticmethod
    def random(req_dict):
        community = (req_dict.get('edition', None) == 'Community')
        if community:
            encryption = False
        else:
            encryption = random.choice([True, False])
        return N2nEncryption(encryption)


class Balanced(Requirement):
    def __init__(self, balanced):
        if not balanced:
            # It doesn't make sense to require an unbalanced cluster, as this
            # tells us nothing about the way in which the cluster is unbalanced.
            # We could extend this requirement to support requiring unbalanced,
            # at a later date, if a use-case arises.
            raise ValueError("balanced must be None or True")
        super().__init__(balanced=True)
        self.balanced = balanced
        self.connect_args = {'do_rebalance': True,
                             'do_wait_for_rebalance': True}

    def __str__(self):
        if self.balanced:
            return "balanced"
        else:
            return "unbalanced"

    def is_met(self, cluster):
        for n in cluster.connected_nodes:
            r = testlib.get(n, '/pools/default')
            if r.status_code != 200:
                return False
            data = r.json()
            if not data['balanced']:
                return False
        return True

    def can_be_met(self):
        return True

    def make_met(self, cluster):
        cluster.rebalance()

    @staticmethod
    def random(_req_dict):
        return Balanced(True)


# Specify a list of buckets required by a testset. Any buckets from other
# testsets will be removed, unless this requirement is not specified.
class Buckets(Requirement):

    def __init__(self, buckets):
        super().__init__(buckets=buckets)
        self.buckets = buckets

    def __str__(self):
        if len(self.buckets) == 0:
            return "no buckets"
        res = []
        for bucket in self.buckets:
            bucket_str = " ".join([f"{key}={value}"
                                    for key, value in bucket.items()
                                    if key != 'name'])
            res.append(f"{bucket['name']}({bucket_str})")
        return "buckets: " + " ".join(res)

    @staticmethod
    def check_prop(prop, bucket_info, expected_value):
        if prop == "ramQuota":
            # Convert both to int, to avoid confusing to debug issues when using
            # a string in the requirement
            return (int(bucket_info['quota']['rawRAM'] / 1_048_576) ==
                    int(expected_value))
        else:
            return bucket_info[prop] == expected_value

    def is_met(self, cluster):
        buckets = testlib.get_succ(cluster, "/pools/default/buckets").json()
        missing_buckets = deepcopy(self.buckets)
        undesired_buckets = []
        for bucket in buckets:
            desired = False
            for desired_bucket in self.buckets:
                if all(self.check_prop(prop, bucket, value)
                       for prop, value in desired_bucket.items()):
                    missing_buckets.remove(desired_bucket)
                    desired = True
            if not desired:
                undesired_buckets.append(bucket)

        if len(undesired_buckets) > 0:
            print(f"Undesired bucket(s) found: {undesired_buckets}.\n"
                  f"Desired: {self.buckets}")
            return False
        if len(missing_buckets) > 0:
            print(f"Missing desired bucket(s): {missing_buckets}.\n"
                  f"Found: {buckets}")
            return False
        return True

    def can_be_met(self):
        return True

    def make_met(self, cluster):
        testlib.delete_all_buckets(cluster)
        for bucket in self.buckets:
            cluster.create_bucket(bucket, sync=True)

    @staticmethod
    def random(req_dict):
        ram_quota = req_dict.get("memsize")
        if ram_quota is None:
            # Default to 100MB if the cluster doesn't have a specified memsize
            ram_quota = 100
        return Buckets(
            random.choice([[],
                           [{"name": "default",
                             "ramQuota": ram_quota}]]))


# Requirement that the test is run on a test generated cluster, rather than an
# existing (user supplied) cluster.
class TestGeneratedCluster(Requirement):
    def __init__(self, tg):
        self.test_generated_cluster = tg
        super().__init__(test_generated_cluster = tg)

    def __str__(self):
        if self.test_generated_cluster:
            return "test generated"
        else:
            return "user supplied"

    def is_met(self, cluster):
        return not cluster.is_existing_cluster()

    def can_be_met(self):
        return False

    @staticmethod
    def random(req_dict):
        # This doesn't really make sense to randomise. The test, if specified,
        # cannot run on a user supplied cluster, so that should be respected or
        # we will see failures. Anything not specifying this should be able to
        # run on both, but the user may have supplied a cluster so we should use
        # that if possible, i.e. leave the value as the implicit False.
        req = req_dict.get("test_generated_cluster")
        return TestGeneratedCluster(req is True)


# Intersects two limits where each limit is defined either by exact match,
# or by a min value. Note that min and exact params are mutually exclusive.
# If intersection is empty, the function returns False, None, None.
# If intersection is not empty, it returns True, NewMin, NewExact.
def intersect_limits(min_val1, exact_val1, min_val2, exact_val2):
    if exact_val1 is not None:
        if exact_val2 is not None:
            if exact_val1 != exact_val2:
                return False, None, None
        else: # min_val2 is set
            if exact_val1 < min_val2:
                return False, None, None
        return True, None, exact_val1
    else: # min_val1 is set
        if exact_val2 is not None:
            if exact_val2 < min_val1:
                return False, None, None
            return True, None, exact_val2
        else: # min_val2 is set
            return True, max(min_val1, min_val2), None


class DevPreview(Requirement):
    def __init__(self, enabled):
        super().__init__(dev_preview=enabled)
        self.enabled = enabled
        self.start_args = {'dev_preview_default': enabled}

    def __str__(self):
        if self.enabled:
            return "dev preview"
        else:
            return "not dev preview"

    def is_met(self, cluster):
        # Check developer preview status through pools endpoint
        r = testlib.get(cluster, "/pools")
        if r.status_code == 200:
            return r.json().get("isDeveloperPreview", False) == self.enabled
        return False

    def can_be_met(self):
        return False

    @staticmethod
    def random(req_dict):
        return DevPreview(False)
