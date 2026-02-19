#!/usr/bin/env python3

import sys
import os
import argparse
import signal
import logging

# Add cluster_tests to path to import mock_smtp_server
script_dir = os.path.dirname(os.path.abspath(__file__))
cluster_tests_dir = os.path.join(script_dir, '..', 'cluster_tests')
sys.path.insert(0, cluster_tests_dir)

from testlib.mock_smtp_server import start_mock_smtp_server


def main():
    args = parse_args()

    print(f"Starting mock SMTP server on {args.host}:{args.port}")
    print(f"Port 0 will use an available ephemeral port")

    # Display log file path if provided
    if args.log_file:
        print(f"SMTP logs will be written to: {args.log_file}")

    # Start the mock SMTP server with debug logging
    server = start_mock_smtp_server(host=args.host, port=args.port,
                                    use_tls=args.tls, log_level=logging.DEBUG,
                                    log_file_path=args.log_file)

    # Display actual port if ephemeral was requested
    if args.port == 0:
        print(f"Server started on port: {server.port}")

    print("Press Ctrl+C to stop the server")

    # Register signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("\nShutting down mock SMTP server...")
        server.stop_server()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Keep the main thread alive
    import time
    while True:
        time.sleep(1)


def parse_args():
    arg_parser = argparse.ArgumentParser(
        prog='start_mock_smtp_server.py',
        description='Start a mock SMTP server for testing email functionality'
    )
    arg_parser.add_argument(
        '--host', '-H',
        type=str,
        default='127.0.0.1',
        metavar='<address>',
        help='Host to bind to (default: 127.0.0.1)'
    )
    arg_parser.add_argument(
        '--port', '-p',
        type=int,
        default=0,
        metavar='<port>',
        help='Port to bind to (0 for ephemeral port, default: 0)'
    )
    arg_parser.add_argument(
        '--tls', '-t',
        action='store_true',
        default=False,
        help='Enable TLS support (not yet implemented)'
    )
    arg_parser.add_argument(
        '--log-file', '-l',
        type=str,
        default=None,
        metavar='<path>',
        help='Path to file for writing logs (optional)'
    )
    return arg_parser.parse_args()


if __name__ == '__main__':
    main()
