#!/usr/bin/env python3

#   Copyright 2023-Present Couchbase, Inc.
#
#   Use of this software is governed by the Business Source License included
#   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
#   in that file, in accordance with the Business Source License, use of this
#   software will be governed by the Apache License, Version 2.0, included in
#   the file licenses/APL2.txt.
#
"""

A script that uses cbc-pillowfight to generate index workload on all collections
in a cluster. This is achieved by changing the Field_1 and Field_2 that are
generated in cbc-pillowfight, causing the index to process the change in the
document values.

"""

import sys
import time

import requests
import random
import subprocess
import argparse
from multiprocessing import Pool

DEFAULT_USERNAME = "Administrator"
DEFAULT_PASSWORD = "asdasd"
pillowfight_dir = f"../../install/bin/cbc-pillowfight"


def get_ports(connect_string,
              user_creds):
    """ Get the query and memcached port connection strings """
    response = requests.get(f"{connect_string}/pools/default/nodeServices",
                            auth=user_creds)
    query_port = response.json()["nodesExt"][0]["services"]["n1ql"]
    memcached_port = response.json()["nodesExt"][0]["services"]["kv"]

    # Remove the port then add the new ports.
    no_port_connection_string = ':'.join(connect_string.split(":")[:-1])
    query_connection_string = f"{no_port_connection_string}:{query_port}/" \
                              f"query/service"
    memcached_connection_string = \
        f"couchbase://{''.join(no_port_connection_string.split('://')[1:])}" \
        f":{memcached_port}"
    return query_connection_string, memcached_connection_string


def create_indexes(query_connection,
                   bucket_name,
                   collection,
                   user_creds,
                   num_documents,
                   prefix,
                   n=1):
    """ Takes a collection and produces an index in the form
    CREATE INDEX ON {collection} WHERE Field_n LIKE x%

    :param user_creds: username & password in a requests.auth object.
    :param query_connection: Connection strin to the query service REST API
    :param bucket_name: Bucket Name
    :param collection: {scope_name}.{collection_name}
    :param n: The Field that is being indexed e.g. Field_4
    :param num_documents: number of documents that pillowfight is iterating on.
    :param prefix: Prefix of the documents.
    """
    rand_doc = f"{prefix}{random.randint(0, num_documents):020}"
    response = requests.post(
        f"{query_connection}", auth=user_creds,
        data={'statement':
                  f'SELECT Field_{n} FROM `{bucket_name}`.{collection} USE KEYS'
                  f' ["{rand_doc}"]'})

    if response.json()["metrics"]["resultCount"] == 0:
        print(f"Document with Field_{n} not found in "
              f"{bucket_name}.{collection}")
        print(response.json())
        print(f"random doc: {rand_doc}")
        print(f"Conn: {query_connection}")
        sys.exit()
    else:
        # Grab the first character of a random document's Field n value.
        #
        # Due to the way that the random strings are generated in pillowfight,
        # only a set number of strings are generated, so this ensures that the
        # index will definitely contain a valid string.

        first_char = response.json()["results"][0][f"Field_{n}"][0]
        index_create = f'CREATE INDEX idx_workload_field_{n} ON ' \
                       f'`{bucket_name}`.{collection}(META().id) ' \
                       f'WHERE Field_{n} LIKE "{first_char}%";'
        index_drop = f'DROP INDEX idx_workload_field_{n} ON ' \
                     f'`{bucket_name}`.{collection};'
        response = requests.post(
            f"{query_connection}", auth=user_creds,
            data={'statement': index_create})
        # If the response is invalid, print the error.
        if response.status_code != 200:
            print(f"Creating indexes at: {query_connection} returned a "
                  f"status code: {response.status_code} {response.reason}"
                  f"\nresponse body: {response.content}")


def get_all_collection_names(bucket_name,
                             user_creds,
                             connection_string):
    response = requests.get(f"{connection_string}/pools/default/"
                            f"buckets/{bucket_name}/scopes", auth=user_creds)
    collection_names = []
    all_scopes_collections = response.json()
    for scope in all_scopes_collections["scopes"]:
        for collection in scope["collections"]:
            collection_names.append(
                f"`{scope['name']}`.`{collection['name']}`")
    return collection_names


def pillowfight(memcached_string,
                bucket_name,
                collection,
                username,
                password,
                num_items,
                prefix,
                threads,
                expiry,
                verbose,
                cycles,
                pop_only=False):
    print(f"Started pillowfight on {collection}")
    # The minimum document size and maximum document size are different to
    # produce different values in the json, when -m == -M, the values in the
    # document will be the exact same.
    pillowfight_args = [pillowfight_dir,
                        "-U", f"{memcached_string}/{bucket_name}",
                        "-u", f"{username}",
                        "-P", f"{password}",
                        "--collection", f"{collection}",
                        "-J", "-R",  # Make a random JSON document.
                        "-e", f"{expiry}",  # TTL time in seconds
                        "-m", "100",  # Min document size
                        "-M", "200",  # Max document size
                        "-B", "10",  # Batch size
                        "-I", f"{num_items}",  # Number of documents
                        "-t", f"{threads}"  # Number of threads
                        ]
    if pop_only:
        pillowfight_args.extend(["--populate-only"])
    else:
        pillowfight_args.extend([
            "-r", "100",       # Make pillowfight do 100% writes
            "-c", f"{cycles}"  # Number of cycles
        ])
    if prefix is not None:
        pillowfight_args.extend(["-p", prefix])
    subprocess.run(pillowfight_args, capture_output=not verbose)
    print(f"Finished pillowfighting {collection}")


def argument_parsing():
    arg_parser = argparse.ArgumentParser(
        formatter_class=lambda prog: argparse.RawTextHelpFormatter(
            prog,
            max_help_position=32))
    arg_parser.add_argument(
        "--spec", "-U",
        help="The connection string e.g. http://localhost:8091",
        metavar="",
        default="http://localhost:9000")
    arg_parser.add_argument(
        "--username", "-u",
        help="Username",
        default=DEFAULT_USERNAME,
        metavar="")
    arg_parser.add_argument(
        "--password", "-P",
        help="Password",
        default=DEFAULT_PASSWORD,
        metavar="")
    arg_parser.add_argument(
        "--bucket", "-b",
        help="Bucket name",
        metavar="",
        default="default")
    arg_parser.add_argument(
        "--num-items", "-I",
        type=int,
        help="Number of documents to be inserted",
        metavar="",
        default=500)
    arg_parser.add_argument(
        "--num-threads", "-t",
        type=int,
        help="Number of threads to use",
        metavar="",
        default=1)
    arg_parser.add_argument(
        "--num-cycles", "-c",
        type=int,
        help="Number of cycles to be run until exiting",
        metavar="",
        default=100)
    arg_parser.add_argument(
        "--expiry", "-e",
        type=int,
        help="Sets TTL. Default: 600 seconds",
        metavar="",
        default=300)
    arg_parser.add_argument(
        "--prefix", "-p",
        help="Key prefix for the documents",
        metavar="",
        default=""
    )
    arg_parser.add_argument(
        "--verbose", "-v",
        help="whether the pillowfight text should be printed out",
        action="store_true"
    )
    arg_parser.add_argument(
        "--loop",
        type=int,
        help="Number of times that each collection will be pillowfighted on",
        default=1
    )
    arg_parser.add_argument(
        "--processes",
        type=int,
        help="Number of pillowfights that will be ran simultaneously",
        default=1
    )
    args = arg_parser.parse_args()
    print(args)

    gen_params = {
        "connection_string": args.spec,
        "loop": args.loop,
        "processes": args.processes,
    }

    pillowfight_params = {
        "username": args.username,
        "password": args.password,
        "bucket_name": args.bucket,
        "num_items": args.num_items,
        "threads": args.num_threads,
        "cycles": args.num_cycles,
        "expiry": args.expiry,
        "prefix": args.prefix,
        "verbose": args.verbose
    }

    index_params = {
        "num_documents": args.num_items,
        "prefix": args.prefix,
        "bucket_name": args.bucket,
    }
    return {k: v for k, v in gen_params.items() if v is not None}, \
        {k: v for k, v in pillowfight_params.items() if v is not None}, \
        {k: v for k, v in index_params.items() if v is not None}


def main():
    def kw_func(coll):
        kw = pf_params.copy()
        kw["collection"] = coll.replace("`", "")
        return kw

    gen_params, pf_params, index_params = argument_parsing()
    user_creds = requests.auth.HTTPBasicAuth(pf_params["username"],
                                             pf_params["password"])
    query_conn, memcached_conn = get_ports(gen_params["connection_string"],
                                           user_creds)
    bucket_name = pf_params["bucket_name"]
    pf_params["memcached_string"] = memcached_conn
    collection_names = get_all_collection_names(
        bucket_name=pf_params["bucket_name"],
        user_creds=user_creds,
        connection_string=gen_params["connection_string"])

    # The number of times that pillowfight is run for each collection the
    # main loop.
    num_loops = gen_params.pop("loop")
    # Number of CPUs that are used for "multiprocess" module, this allows for
    # parallelism of running multiple pillowfights simultaneously.
    num_processes = gen_params.pop("processes")

    # First add some documents, then create indexes on them
    for coll in collection_names:
        pillowfight(collection=coll.replace("`", ""),
                    pop_only=True,
                    **pf_params)
        # Make indexes on Field_1 and Field_2
        for x in range(1, 3):
            create_indexes(query_connection=query_conn,
                           collection=coll,
                           n=x,
                           user_creds=user_creds,
                           **index_params)

    print("Finished populating collections\n\n")
    # MAIN LOOP
    # Create a multiprocessing.pool to allow for parallelism
    with Pool(processes=num_processes) as p:
        # Add all the pillowfights to the respective collections to processes
        processes = [p.apply_async(pillowfight, kwds=kw_func(coll))
                     for coll in collection_names]
        # Pillowfight n number of times on each collection.
        for n in range(num_loops):
            result = [p.get() for p in processes]

    print("Dropping indexes:")
    for coll in collection_names:
        print(f"\t{coll}")
        for i in range(1, 3):
            index_drop = f'DROP INDEX idx_workload_field_{i} ON ' \
                         f'`{bucket_name}`.{coll};'
            response = requests.post(
                query_conn, auth=user_creds,
                data={'statement': index_drop})
            if response.status_code != 200:
                print(f"Failed to drop index idx_workload_field_{i} on {coll}\n"
                      f"REST response return {response.status_code} with: \n"
                      f"{response.content}")


if __name__ == "__main__":
    main()
