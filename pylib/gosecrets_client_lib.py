#!/usr/bin/env python3
# @author Couchbase <info@couchbase.com>
# @copyright 2014-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import sys
import os
import traceback
import subprocess
import base64
from installed_script_helpers import basedir, find_binary
import json
import signal


global_debug_on = False


def read_keys(key_specs, config_path, gosecrets_path=None,
              password="", all_must_succeed=False):
    if gosecrets_path is None:
        gosecrets_path = find_binary("gosecrets")

    if gosecrets_path is None:
        raise BadArg(f'Failed to find gosecrets')

    if config_path is None or not os.path.exists(config_path):
        raise BadArg(f'gosecrets.cfg does not exist')

    debug(f'Using {gosecrets_path} with config {config_path}')

    proc = subprocess.Popen([gosecrets_path, '--config', config_path],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    try:
        send_command(proc, 'init', password)

        (init_res, data) = wait_response(proc)

        if init_res != 'success':
            error_msg = data.decode()
            if error_msg == "key decrypt failed: cipher: message authentication failed":
                raise BadMasterPassword("Incorrect master password")
            raise InitFailed(f'Initialization failed\n{error_msg}')

        keys_map = {}
        for spec in key_specs:

            if spec['type'] == 'keyPath':
                send_command(proc, 'read_key_file', spec['path'])
            elif spec['type'] == 'keyId':
                send_command(proc, 'read_key', spec['kind'], spec['id'])
            elif spec['type'] == 'searchDekKind':
                send_command(proc, 'search_key', spec['kind'], spec['id'])
            elif spec['type'] == 'search':
                send_command(proc, 'search_key', '', spec['id'])
            else:
                raise BadArg(f'unknown key spec type: {spec["type"]}')

            key_id = spec['id']

            (res, data) = wait_response(proc)
            if res == 'success':
                data_parsed = json.loads(data)
                key_type = data_parsed['type']
                key_b64 = data_parsed['info']['key']
                keys_map[key_id] = {'result': key_type,
                                    'response': {'key': key_b64}}
            else:
                if all_must_succeed:
                    error = f'Could not get key "{key_id}": {data.decode()}'
                    raise NoKey(error)
                keys_map[key_id] = {'result': 'error',
                                    'response': data.decode()}
    finally:
        terminate_gosecrets(proc)

    return keys_map


def terminate_gosecrets(proc):
    proc.terminate()
    try:
        output, _ = proc.communicate(timeout=5)
        code = proc.returncode
        if code != 0 and code != -int(signal.SIGTERM):
            debug(f'Gosecrets exited with unexpected code: {code}')
            debug(f'Gosecrets output: {output}')
    except subprocess.TimeoutExpired:
        debug('Gosecrets failed to terminate in time, killing...')
        proc.kill()


def send_command(proc, cmd, *args):
    data = None
    if cmd == 'init':
        data = encode_init(*args)
    elif cmd == 'read_key':
        data = encode_read_key(*args)
    elif cmd == 'read_key_file':
        data = encode_read_key_file(*args)
    elif cmd == 'search_key':
        data = encode_search_key(*args)
    else:
        raise ValueError
    debug(f'<== {cmd} {args}')
    proc.stdin.write(data)
    proc.stdin.flush()


def wait_response(proc):

    def read_exact(size):
        data = b''
        remains = size
        while remains > 0:
            new = proc.stdout.read(remains)
            if len(new) == 0:
                return b''
            data += new
            remains -= len(new)
        return data

    last_log_message = None
    while True:
        size_bin = read_exact(4)
        if len(size_bin) == 0:
            # Unexpected eof usually means panic. In this case the very last
            # log message is very helpful for investigation.
            maybe_print_last_log(last_log_message)
            raise UnexpectedReply('Unexpected eof from gosecrets')
        size = int.from_bytes(size_bin, "big")
        msg = read_exact(size)
        if len(msg) == 0:
            maybe_print_last_log(last_log_message)
            raise UnexpectedReply('Unexpected eof from gosecrets')
        reply_type = msg[0]
        reply = msg[1:]
        if reply_type == ord('S'):
            debug(f'==> success')
            return ('success', reply)

        if reply_type == ord('E'):
            debug(f'==> error {reply.decode()}')
            return ('error', reply)

        if reply_type == ord('L'):
            text = reply.decode()
            debug(text)
            if not global_debug_on:
                last_log_message = text
            continue

        raise UnexpectedReply(f'Received unexpected reply: {reply_type}')


def maybe_print_last_log(last_log):
    if last_log is not None:
        print_err(f'Last gosecrets log:\n{last_log}')


def encode_init(password):
    if password is None:
        return encode_msg(b'\x01\x00')

    return encode_msg(b'\x01\x01' + password.encode('latin-1'))


def encode_read_key(key_usage, key_name):
    return encode_msg(b'\x0f' +
                      encode_msg(key_usage.encode()) +
                      encode_msg(key_name.encode()))


def encode_read_key_file(key_file):
    return encode_msg(b'\x10' + encode_msg(key_file.encode()))


def encode_search_key(key_kind, key_name):
    return encode_msg(bytes([24]) +
                      encode_msg(key_kind.encode()) +
                      encode_msg(key_name.encode()))


def encode_msg(msg):
    size = len(msg)
    cmd = size.to_bytes(4, 'big') + msg
    return cmd


class BadMasterPassword(Exception):
    pass


class BadArg(Exception):
    pass


class InitFailed(Exception):
    pass


class NoKey(Exception):
    pass


class UnexpectedReply(Exception):
    pass


def debug(s):
    if global_debug_on:
        print_err(s)


def print_err(s):
    print(s, file=sys.stderr)


def script_exception_handler(e):
    error = str(e)
    if isinstance(e, BadArg):
        code = 1
    elif isinstance(e, BadMasterPassword):
        code = 2
    elif isinstance(e, NoKey):
        code = 3
    elif isinstance(e, UnexpectedReply):
        code = 4
    elif isinstance(e, InitFailed):
        code = 5
    else:
        code = 19
        error = traceback.format_exc()
    print_err(error)
    exit(code)


def set_debug(new_debug):
    global global_debug_on
    global_debug_on = new_debug
