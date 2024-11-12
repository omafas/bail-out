# @author Couchbase <info@couchbase.com>
# @copyright 2011-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import os
import os.path
import subprocess
import sys
import shlex
import socket
import fnmatch
import platform
import errno
import shutil
import urllib.request
import urllib.parse
import urllib.error
import json
from functools import reduce
import time
from urllib.error import URLError

base_direct_port = 12000
base_api_port = 9000
base_couch_port = 9500
base_projector_port = 10000
base_xdcr_port = 13000
base_indexer_port = 9100
base_fts_port = 9200
base_eventing_port = 9300
base_cbas_port = 9600
base_prometheus_port = 7900
base_backup_http_port= 7100
base_backup_https_port= 17100
base_backup_grpc_port = 7200

node_start_timeout_s = 60
default_username = "Administrator"
default_pass = "asdasd"
default_idx_storage_mode_ep = "plasma"
default_idx_storage_mode_ce = "forestdb"

script_dir = os.path.dirname(os.path.realpath(__file__))
ns_server_dir = os.path.dirname(script_dir)
configpath = os.path.join(ns_server_dir, "build", "cluster_run.configuration")

NUM_SERVERLESS_GROUPS = 3


def read_configuration():
    with open(configpath) as f:
        def fn(line):
            k, v = line.strip().split('=')
            return k, shlex.split(v)[0]

        return dict(fn(line) for line in f.readlines())


config = read_configuration()
PREFIX = config['prefix']

valid_bucket_types = ["ephemeral", "membase", "memcached"]
valid_service_types = {"kv", "n1ql", "index", "fts", "cbas", "eventing",
        "backup", "none"}

def setup_extra_ns_server_app_file(force_community, start_index):
    # The extra/ebin directory contains modified versions of files also
    # contained in other directories.  The ones in extra/ebin are listed
    # in the path directory such that they will take precedence when
    # loaded.  Note the -pa option used when starting erl reverses the
    # order of the list.
    extra_dirname = f"{ns_server_dir}/extra"
    extra_ebin_dirname = "{}/n_{}".format(extra_dirname, start_index)
    extra_ebin_path = extra_ebin_dirname + "/ebin"
    returned_path = None

    # Clean up any residual files from prior runs.
    try:
        shutil.rmtree(extra_ebin_dirname)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            pass
        else:
            raise

    if force_community:
        found_enterprise = False
        with open(f"{ns_server_dir}/ebin/ns_server.app", "r") as src_f:
            lines = src_f.readlines()

        lines_out = ""
        for line in lines:
            # The way to change Enterprise edition to Community edition is to
            # simply change the "vsn" in the ns_server app.
            if "vsn" in line and "enterprise" in line:
                line = line.replace("enterprise", "community")
                # Ensure only one line containing "vsn" and "enterprise".
                assert found_enterprise is False
                found_enterprise = True
            lines_out = lines_out + line

        if found_enterprise:
            # Any errors here are "real" so we want exceptions thrown
            os.makedirs(extra_ebin_path)

            with open(f"{extra_ebin_path}/ns_server.app", "w") as dst_f:
                dst_f.write(lines_out)

            returned_path = extra_ebin_path

    return returned_path


def setup_path(ns_server_app_path):
    def ebin_search(path_name):
        dirs = os.walk(path_name)
        ebins = []

        for d, _, _ in dirs:
            if os.path.basename(d) == "ebin":
                ebins.append(d)

        return ebins

    path = ebin_search(f'{ns_server_dir}/ebin') + \
           ebin_search(f'{ns_server_dir}/deps')

    if ns_server_app_path is not None:
        # The ns_server_app_path needs to be first in the path. We remove
        # it from what was found and append it to the path (it's at the
        # end as the -pa argument used when starting erl reverses the
        # order).
        path.append(ns_server_app_path)

    couchdb_lib_path = "{0}/lib/couchdb/erlang/lib".format(PREFIX)
    couchpath = ebin_search(couchdb_lib_path)
    couch_plugins = ebin_search("{0}/lib/couchdb/plugins".format(PREFIX))

    if len(couchpath) == 0:
        msg = ("⛔️ Fatal error: Unable to locate CouchDB libs ('ebin' subdir) "
               f"under path '{couchdb_lib_path}'.\n"
               f"Searched using config file '{configpath}':\n"
               f"\n    {config}\n\n"
               "💡 Check the 'prefix' key points to the correct "
               "server installation prefix - typically "
               "'<REPO_CHECKOUT>/install'.")
        sys.exit(msg)

    # Note the paths are passed via "-pa" to the erl process where their
    # ordering is reversed.
    return couchpath + path + couch_plugins


def maybe_mk_node_couch_config(i, ini_file_name, root_dir):
    ini_dir = os.path.dirname(ini_file_name)

    # If ini file exists, then don't overwrite it.
    if os.path.isfile(ini_file_name):
        return

    try:
        os.mkdir(ini_dir)
    except os.error:
        pass

    abs_root_dir = os.path.abspath(root_dir)

    with open(ini_file_name, "w") as f:
        f.write("[httpd]\n")
        f.write("port={0}\n".format(base_couch_port + i))
        f.write("[couchdb]\n")
        f.write("database_dir={0}/data/n_{1}/data\n".format(abs_root_dir, i))
        f.write("view_index_dir={0}/data/n_{1}/data\n".format(abs_root_dir, i))
        f.write("max_dbs_open=10000\n")
        f.write("[upr]\n")
        f.write("port={0}\n".format(base_direct_port + i * 2))
        f.write("[dcp]\n")
        f.write("port={0}\n".format(base_direct_port + i * 2))


def couch_configs(i, root_dir):
    ini_file_name = os.path.join(root_dir, "couch", f"n_{i}_conf.ini")
    maybe_mk_node_couch_config(i, ini_file_name, root_dir)
    return ["{0}/etc/couchdb/default.ini".format(PREFIX),
            "{0}/etc/couchdb/default.d/capi.ini".format(PREFIX),
            "{0}/etc/couchdb/default.d/geocouch.ini".format(PREFIX),
            ini_file_name]


def os_specific(args, params):
    """Add os-specific junk to the cluster startup."""
    if platform.system() == 'Windows':
        args += ["dont_suppress_stderr_logger", "false"]
    else:
        args += ["dont_suppress_stderr_logger", "true"]
    if platform.system() == 'Darwin':
        import resource
        # OS X has a pretty tiny default fd limit.  Let's increase it
        # (if it hasn't already been).
        (soft, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 4096:
            resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))
        params['env'] = {"ERL_MAX_PORTS": "4096"}
        params['env'].update(os.environ)


def prepare_start_cluster(force_community, start_index):
    ns_server_app_path = setup_extra_ns_server_app_file(force_community,
                                                        start_index)

    ebin_path = setup_path(ns_server_app_path)
    return ebin_path


def quote_string_for_erl(s):
    return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"'


def generate_ssl_dist_optfile(datadir):
    cfg_dir = os.path.join(datadir, "config")
    in_file = os.path.join(ns_server_dir, "etc", "ssl_dist_opts.in")
    out_file = os.path.join(cfg_dir, "ssl_dist_opts")

    if not os.path.exists(cfg_dir):
        os.makedirs(cfg_dir, 0o755)

    with open(in_file) as f:
        content = f.read().replace('@CONFIG_PREFIX@', cfg_dir)

    with open(out_file, "w") as f:
        f.write(content)

    return out_file

def abs_path_join(*args):
    return os.path.abspath(os.path.join(*args))


def erlang_args_for_node(i, ebin_path, extra_args, args_prefix, root_dir):
    logdir = abs_path_join(root_dir, "logs", f"n_{i}")

    args = args_prefix + ["erl", "+MMmcs" "30",
                          "+A", "16", "+sbtu",
                          "+sbwt", "none",
                          "+P", "327680", "-pa"] + ebin_path
    args += [
        "-setcookie", "nocookie",
        "-kernel", "logger", "[{handler, default, undefined}]",
        "-couch_ini"] + couch_configs(i, root_dir)

    datadir = abs_path_join(root_dir, 'data', f'n_{i}')
    tempdir = abs_path_join(root_dir, 'tmp')
    nodefile = os.path.join(datadir, "nodefile")
    ssloptfile = generate_ssl_dist_optfile(datadir)
    cb_dist_config = os.path.join(datadir, "config", "dist_cfg")
    hosts_file = os.path.join(ns_server_dir, "etc", "hosts.cfg")
    static_config = os.path.join(ns_server_dir, "etc", "static_config.in")

    args += [
        "-name", "babysitter_of_n_{0}@cb.local".format(i),
        "-proto_dist", "cb",
        "-ssl_dist_optfile", ssloptfile,
        "-epmd_module", "cb_epmd",
        "-no_epmd",
        "-hidden",
        "-kernel", "dist_config_file", quote_string_for_erl(cb_dist_config),
        "-kernel", "inetrc", f"\"{hosts_file}\"",
        "-kernel", "prevent_overlapping_partitions", "false",
        "-ns_server", "config_path", f'"{static_config}"',
        "error_logger_mf_dir", quote_string_for_erl(logdir),
        "path_config_etcdir", f'"{os.path.join(ns_server_dir, "priv")}"',
        "approot", quote_string_for_erl(ns_server_dir + "/../build/ui-build/public"),
        "path_config_bindir", quote_string_for_erl(PREFIX + "/bin"),
        "path_config_libdir", quote_string_for_erl(PREFIX + "/lib"),
        "path_config_datadir", quote_string_for_erl(datadir),
        "path_config_tmpdir", quote_string_for_erl(tempdir),
        "path_config_secdir", quote_string_for_erl(PREFIX + "/etc/security"),
        "path_audit_log", quote_string_for_erl(logdir),
        "rest_port", str(base_api_port + i),
        "query_port", str(base_couch_port - 1 - i),
        "ssl_query_port", str(10000 + base_couch_port - 1 - i),
        "projector_port", str(base_projector_port + i),
        "projector_ssl_port", str(base_projector_port + i),
        "ssl_rest_port", str(10000 + base_api_port + i),
        "capi_port", str(base_couch_port + i),
        "ssl_capi_port", str(10000 + base_couch_port + i),
        "memcached_port", str(base_direct_port + i * 2),
        "memcached_dedicated_port", str(base_direct_port - i * 4 - 1),
        "memcached_ssl_port", str(base_direct_port - i * 4 - 2),
        "memcached_dedicated_ssl_port", str(base_direct_port - i * 4 - 3),
        "memcached_prometheus", str(base_direct_port - i * 4 - 4),
        "nodefile", quote_string_for_erl(nodefile),
        "short_name", quote_string_for_erl('n_{0}'.format(i)),
        "xdcr_rest_port", str(base_xdcr_port + i),
        "indexer_admin_port", str(base_indexer_port + i * 6),
        "indexer_scan_port", str(base_indexer_port + i * 6 + 1),
        "indexer_http_port", str(base_indexer_port + i * 6 + 2),
        "indexer_https_port", str(10000 + base_indexer_port + i * 6 + 2),
        "indexer_stinit_port", str(base_indexer_port + i * 6 + 3),
        "indexer_stcatchup_port", str(base_indexer_port + i * 6 + 4),
        "indexer_stmaint_port", str(base_indexer_port + i * 6 + 5),
        "fts_http_port", str(base_fts_port + i * 2),
        "fts_ssl_port", str(10000 + base_fts_port + i * 2),
        "fts_grpc_port", str(base_fts_port + i * 2 + 1),
        "fts_grpc_ssl_port", str(10000 + base_fts_port + i * 2 + 1),
        "eventing_http_port", str(base_eventing_port + i),
        "eventing_https_port", str(10000 + base_eventing_port + i),
        "eventing_debug_port", str(base_eventing_port + i * 6 + 1),
        "cbas_http_port", str(base_cbas_port + i * 15),
        "cbas_cc_http_port", str(base_cbas_port + i * 15 + 1),
        "cbas_cc_cluster_port", str(base_cbas_port + i * 15 + 2),
        "cbas_cc_client_port", str(base_cbas_port + i * 15 + 3),
        "cbas_console_port", str(base_cbas_port + i * 15 + 4),
        "cbas_cluster_port", str(base_cbas_port + i * 15 + 5),
        "cbas_data_port", str(base_cbas_port + i * 15 + 6),
        "cbas_result_port", str(base_cbas_port + i * 15 + 7),
        "cbas_messaging_port", str(base_cbas_port + i * 15 + 8),
        "cbas_debug_port", str(base_cbas_port + i * 15 + 9),
        "cbas_parent_port", str(base_cbas_port + i * 15 + 10),
        "cbas_admin_port", str(base_cbas_port + i * 15 + 11),
        "cbas_replication_port", str(base_cbas_port + i * 15 + 12),
        "cbas_metadata_port", str(base_cbas_port + i * 15 + 13),
        "cbas_metadata_callback_port", str(base_cbas_port + i * 15 + 14),
        "cbas_ssl_port", str(10000 + base_cbas_port + i),
        "prometheus_http_port", str(base_prometheus_port + i),
        "backup_http_port", str(base_backup_http_port + i),
        "backup_https_port", str(base_backup_https_port + i),
        "backup_grpc_port", str(base_backup_grpc_port + i),

    ] + extra_args

    return args

def find_primary_addr(ipv6):
    family = socket.AF_INET6 if ipv6 else socket.AF_INET
    dns_addr = "2001:4860:4860::8844" if ipv6 else "8.8.8.8"
    s = socket.socket(family, socket.SOCK_DGRAM)
    try:
        s.connect((dns_addr, 53))
        if ipv6:
            addr, port, _, _ = s.getsockname()
        else:
            addr, port = s.getsockname()

        return addr
    except socket.error:
        return None
    finally:
        s.close()

def start_cluster(num_nodes=1,
                  dont_start=False,
                  start_index=0,
                  dont_rename=False,
                  static_cookie=False,
                  loglevel='debug',
                  prepend_extras=False,
                  pluggable_config=[],
                  disable_autocomplete="{disable_autocomplete,false}",
                  pretend_version=None,
                  ipv6=False,
                  force_community=False,
                  run_serverless=False,
                  run_provisioned=False,
                  num_vbuckets=None,
                  dev_preview_default=None,
                  args=[],
                  root_dir=ns_server_dir,
                  wait_for_start=False,
                  nooutput=False,
                  env={}):

    extra_args = []
    if not dont_rename:
        primary_addr = find_primary_addr(ipv6)
        if primary_addr is None:
            print("was unable to detect 'internet' address of this machine."
                  + " node rename will be disabled")
        else:
            extra_args += ["rename_ip", '"' + primary_addr + '"']

    if prepend_extras:
        prepend_args = args[0:]
    else:
        prepend_args = []
        extra_args += args[0:]

    if static_cookie:
        extra_args += ["-ns_server", "dont_reset_cookie", "true"]

    if dont_start:
        extra_args += ["-run", "t", "fake_loggers"]
    else:
        extra_args += ["-noinput"]
        extra_args += ["-run", "child_erlang", "child_start",
                       "ns_babysitter_bootstrap"]
        extra_args += ["-ns_babysitter", "handle_ctrl_c", "true"]

    extra_args += ["-ns_server", "loglevel_stderr", loglevel]

    plugins_dir = os.path.join(ns_server_dir, '..', 'install',
                               'etc', 'couchbase')
    if os.path.isdir(plugins_dir):
        for f in os.listdir(plugins_dir):
            if fnmatch.fnmatch(f, 'pluggable-ui-*.json'):
                pluggable_config.append(os.path.join(plugins_dir, f))

    if pluggable_config:
        extra_args += ["-ns_server", "ui_plugins",
                       quote_string_for_erl(','.join(pluggable_config))]

    ui_env = [disable_autocomplete]

    extra_args += ["-ns_server", "ui_env", '[' + ','.join(ui_env) + ']']

    if pretend_version is not None:
        extra_args += ["-ns_server",
                       "pretend_version", '"{}"'.format(pretend_version)]

    if dev_preview_default is not None:
        extra_args += ["-ns_server", "developer_preview_enabled_default",
                       "true" if dev_preview_default else "false"]

    ebin_path = prepare_start_cluster(force_community, start_index)

    def start_node(node_num):
        logdir = os.path.join(root_dir, "logs", f"n_{node_num}")
        try:
            os.makedirs(logdir)
        except OSError:
            pass

        args = erlang_args_for_node(node_num, ebin_path, extra_args,
                                    prepend_args, root_dir)

        params = {}

        os_specific(args, params)

        if 'env' not in params:
            params['env'] = {}
            params['env'].update(os.environ)

        if 'CB_FORCE_PROFILE' not in params['env']:
            params['env']['CB_FORCE_PROFILE'] = "default"

        if run_serverless:
            params['env']['CB_FORCE_PROFILE'] = "serverless"

        if run_provisioned:
            params['env']['CB_FORCE_PROFILE'] = "provisioned"

        if num_vbuckets:
            params['env']['COUCHBASE_NUM_VBUCKETS'] = f"{num_vbuckets}"

        path = params['env']['PATH']
        path = (PREFIX + "/bin") + os.pathsep + path
        if 'ERL_FULLSWEEP_AFTER' not in params['env']:
            params['env']['ERL_FULLSWEEP_AFTER'] = '512'
        params['env']['PATH'] = path

        crash_dump_base = 'erl_crash.dump.n_%d' % node_num
        params['env']['ERL_CRASH_DUMP_BASE'] = crash_dump_base
        params['env']['ERL_CRASH_DUMP'] = crash_dump_base + '.babysitter'

        for k in env:
            params['env'].pop(k, None)
            if env[k] is not None:
                params['env'][k] = env[k]

        params['close_fds'] = True
        if platform.system() == "Windows":
            params['close_fds'] = False

        w = None
        r = None

        if "-noinput" in args:
            (r, w) = os.pipe()

            params['stdin'] = r

            if 'setpgrp' in os.__dict__ and params.get('close_fds'):
                # this puts child out of our process group. So that
                # Ctrl-C doesn't deliver SIGINT to it, leaving us
                # ability to it shutdown carefully or otherwise
                params['preexec_fn'] = os.setpgrp

        if nooutput:
            params['stdout'] = subprocess.DEVNULL
            params['stderr'] = subprocess.DEVNULL

        pr = subprocess.Popen(args, **params)
        if w is not None:
            os.close(r)

        # Squirrel away the write descriptor for the pipe into the
        # subprocess.Popen object
        pr.write_side = w

        return pr

    processes = [start_node(i + start_index) for i in range(num_nodes)]

    if wait_for_start:
        wait_nodes_up(num_nodes, start_index, node_start_timeout_s)

    return processes


def wait_nodes_up(num_nodes=1, start_index=0, timeout_s=node_start_timeout_s,
                  node_urls=None, verbose=True):
    def print_if_verbose(*args, **kwargs):
        if verbose:
            print(*args, **kwargs)
    start = time.time()
    deadline = start + timeout_s

    # Wait for node to be responsive. Returns the last response or error
    def wait_node_up(url):
        last_error = None
        print_if_verbose(f"Waiting for node {url}", end="")
        sys.stdout.flush()
        while time.time() < deadline:
            try:
                http_get_json(url + "/pools")
                time_delta = time.time() - start
                print_if_verbose(f" UP [took: {time_delta:.2f}s timeout:{timeout_s}s]")
                return
            except Exception as e:
                last_error = e.__str__()
                print_if_verbose('.', end='')
                sys.stdout.flush()
                time.sleep(0.5)
        print_if_verbose(f" TIMEOUT {timeout_s}s")
        raise RuntimeError(f"Node {url} wait timed out "
                           f"(last error: {last_error})")
    if node_urls is None:
        [wait_node_up(f"http://localhost:{base_api_port + start_index + i}")
         for i in range(num_nodes)]
    else:
        [wait_node_up(node_url) for node_url in node_urls]


def kill_nodes(nodes, terminal_attrs=None, urls=None):
    if urls is not None:
        sync_loggers(urls)
    for n in nodes:
        if n.write_side is not None:
            print("Closing %d\n" % n.write_side)
            # this line does graceful shutdown versus quick
            os.write(n.write_side, b'shutdown\n')
        else:
            try:
                n.kill()
            except OSError:
                pass

    for n in nodes:
        n.wait()

    if terminal_attrs is not None:
        import termios
        termios.tcsetattr(sys.stdin, termios.TCSANOW, terminal_attrs)


# Wait for final errors to be logged
def sync_loggers(urls):
    try:
        for url in urls:
            http_post(url + "/diag/eval", "ale:sync_all_sinks().")
    except URLError as e:
        print(f"Error encountered syncing loggers: {e.reason}\n"
              f"Sleeping for 1 second to give the cluster the opportunity "
              f"to flush logs.")
        time.sleep(1)


def bool_request_value(value):
    return "1" if value else "0"


class PasswordManager(urllib.request.HTTPPasswordMgr):
    def __init__(self, username, password):
        self.auth = (username, password)

    def find_user_password(self, realm, authuri):
        return self.auth


def do_encode(input_string):
    return input_string.encode()


def http_get_json(url):
    return json.loads(http_get(url))


def http_get(url, timeout=60):
    password_mgr = PasswordManager(default_username, default_pass)
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
    o = urllib.request.build_opener(handler)
    return o.open(url, timeout=timeout).read()


def http_post(url, data, timeout=60):
    password_mgr = PasswordManager(default_username, default_pass)
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
    o = urllib.request.build_opener(handler)
    encoded_data = do_encode(data)
    return o.open(url, encoded_data, timeout=timeout).read()


def connect(num_nodes=0,
            start_index=0,
            deploy=['kv'],
            buckettype="membase",
            memsize=256,
            indexmemsize=256,
            index_storage_mode=None,
            replicas=1,
            replica_index=True,
            protocol="ipv4",
            encryption=False,
            do_rebalance=True,
            do_wait_for_rebalance=False,
            storage_backend="couchstore",
            serverless_groups=False,
            create_bucket=True,
            bucket_weight=50,
            bucket_width=1):
    if isinstance(deploy, list):
        services = deploy
        deploy = dict(("n%d" % i, services[:]) for i in range(num_nodes))

    if "kv" not in deploy.get("n0", []):
        deploy["n0"] = deploy.get("n0", []) + ["kv"]

    if num_nodes == 0 or buckettype not in valid_bucket_types or \
            int(memsize) < 256 or int(replicas) > 3 or \
            not set(deploy.keys()) <= \
            set(["n" + str(i) for i in range(num_nodes)]) or \
            not set(reduce(lambda x, y: x + y, deploy.values(), [])) <= \
            valid_service_types:
        return 1

    password_mgr = PasswordManager(default_username, default_pass)
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
    o = urllib.request.build_opener(handler)

    print(
        f"Connecting {num_nodes} nodes, bucket type {buckettype}, "
        f"mem size {memsize} "
        f"with {replicas} replica copies, password {default_pass}, "
        f"with a storage backend of {storage_backend}. "
        f"Deployment plan: {deploy}\n")

    base_port = 9000 + start_index

    addr = "127.0.0.1" if protocol == "ipv4" else "[::1]"
    services = deploy["n0"]
    print("Connecting node 0 with services {0}".format(str(services)))

    info = json.loads(o.open("http://{0}:{1}/pools".format(
        addr, base_port)).read())
    community_edition = info['isEnterprise'] is not True
    serverless = info['configProfile'] == 'serverless'
    if not serverless and serverless_groups:
        print(f"Must use a serverless configuration to create groups.")
        return 1
    if serverless:
        do_wait_for_rebalance = True

    if index_storage_mode is not None:
        indStorageMode = index_storage_mode
    elif community_edition:
        indStorageMode = default_idx_storage_mode_ce
    else:
        indStorageMode = default_idx_storage_mode_ep

    data = do_encode(
             f'afamily={protocol}&' \
             f'nodeEncryption={"on" if encryption else "off"}&' \
             f'memoryQuota={memsize}&' \
             f'indexMemoryQuota={indexmemsize}&' \
             'port=SAME&' \
             f'username={default_username}&' \
             f'password={default_pass}&' \
             f'indexerStorageMode={indStorageMode}&' \
             f'services=' + ",".join(services))
    o.open(f'http://{addr}:{base_port}/clusterInit', data).read()

    # Creating the groups (availability zones) for serverless.
    if serverless_groups:
        # Only need to create "Group 2" and "Group 3", since Group 1 is the
        # default
        for j in range(2, NUM_SERVERLESS_GROUPS + 1):
            data = do_encode(f"name=Group {j}")
            o.open(f"http://{addr}:{base_port}/pools/default/serverGroups",
                   data).read()
        # Dictionary which matches the group name to the URI
        group_name_uri = {}
        server_group_response = json.loads(o.open(
            f"http://{addr}:{base_port}/pools/default/serverGroups").read())
        for group in server_group_response["groups"]:
            group_name_uri[group['name']] = group['uri']

    if create_bucket:
        data_string = ("name=default" +
                       "&bucketType=" + buckettype +
                       "&storageBackend=" + storage_backend +
                       "&ramQuotaMB=" + str(memsize)
                       )
        if buckettype != "memcached":
            data_string += "&replicaNumber=" + str(replicas)
        if buckettype != "ephemeral":
            data_string += "&replicaIndex=" + bool_request_value(replica_index)
        if serverless:
            data_string += f"&width={bucket_width}&weight={bucket_weight}"
        bucket_data_string = do_encode(data_string)
        # When using serverless with a width > 1, we need to wait for the
        # rebalance to complete before creating a bucket, otherwise it is safe
        # to create the bucket beforehand.
        if not do_wait_for_rebalance:
            o.open("http://{0}:{1}/pools/default/buckets"
                   .format(addr, base_port),
                   bucket_data_string).read()

    for i in range(1, num_nodes):
        port = base_port + i
        services = deploy.get("n" + str(i), [])
        if not services:
            services = ["kv"]
        if services == ['none']:
            services = ""
        print("Connecting node {0} with services {1}".format(i, str(services)))
        cluster_member_port = base_port if community_edition else \
            base_port + 10000
        if serverless_groups:
            # If using serverless, add the node to a group,
            # otherwise joinCluster
            data = do_encode(
                f"user={default_username}&password={default_pass}&" +
                f"hostname={addr}:{port + 10000}" +
                f"&services={','.join(services)}")
            group_uri = \
                f"{group_name_uri[f'Group {i % NUM_SERVERLESS_GROUPS + 1}']}"
            o.open(f"http://{addr}:{base_port}{group_uri}/addNode", data).read()
        else:
            data = do_encode(
                f"user={default_username}&password={default_pass}&" +
                "clusterMemberHostIp={0}".format(addr) +
                "&clusterMemberPort={0}".format(cluster_member_port) +
                "&services={0}".format(",".join(services)))
            o.open("http://{0}:{1}/node/controller/doJoinCluster".format(
                addr, port), data).read()

    if do_rebalance:
        print("Getting node list")
        info = json.loads(o.open("http://{0}:{1}/nodeStatuses".format(
            addr, base_port)).read())

        print("Servers added, triggering rebalance.")
        data = do_encode(urllib.parse.urlencode(
            {'knownNodes': ",".join([info[k]['otpNode'] for k in info]),
                'ejectedNodes': ''}))

        o.open("http://{0}:{1}/controller/rebalance".format(addr, base_port),
               data).read()

        if do_wait_for_rebalance:
            err = wait_for_rebalance("http://{0}:{1}".format(addr, base_port))
            if err is not None:
                print(err)
                return 1
            if create_bucket:
                o.open("http://{0}:{1}/pools/default/buckets"
                       .format(addr, base_port),
                       bucket_data_string).read()
    return 0


# Query the tasks endpoint for the rebalance task, and check if it is
# running
def is_rebalance_running(url):
    rebalance_running = False
    (tasks, ) = http_get_json(url + "/pools/default/tasks"),

    for task in tasks:
        if task.get("type") == "rebalance" and \
                task.get("status") == "running":
            rebalance_running = True
    return rebalance_running


def rebalance_error(url):
    (tasks, ) = http_get_json(url + "/pools/default/tasks"),
    for task in tasks:
        if task.get("type") == "rebalance":
            return task.get("errorMessage")
    return None


# Wait for a rebalance to complete, by checking every half second for up to
# 10mins, and returning whether or not the rebalance completed in that time
def wait_for_rebalance(url, timeout_s=600, interval_s=0.5, verbose=False):
    def print_if_verbose(*args, **kwargs):
        if verbose:
            print(*args, **kwargs)

    if is_rebalance_running(url):
        print_if_verbose(f"Waiting up to {timeout_s}s for rebalance to "
                         f"finish", end='')
        timeout_time = time.time() + timeout_s
        while is_rebalance_running(url):
            if time.time() > timeout_time:
                print_if_verbose("Timed out waiting for rebalance")
                return "timeout"
            print_if_verbose('.', end='')
            sys.stdout.flush()
            time.sleep(interval_s)

        print_if_verbose(" Finished.")
    return rebalance_error(url)
