import asyncio
import threading
import logging
import socket
import time
import ssl
import shutil
import subprocess
import tempfile
import os
from email import message_from_string
from email.header import decode_header
from aiosmtpd.controller import Controller


class CapturedEmail:
    """Stores information about a captured email."""
    def __init__(self, sender: str, recipients: list, subject: str, body: str):
        self.sender = sender
        self.recipients = recipients
        self.subject = subject
        self.body = body

    def __repr__(self):
        return f"CapturedEmail(sender={self.sender}, " \
               f"recipients={self.recipients}, subject={self.subject})"


class MockSMTPHandler:
    """SMTP handler that captures all emails."""

    def __init__(self):
        self.captured_emails = []
        self._lock = threading.Lock()

    async def handle_DATA(self, server, session, envelope):
        """Handle received email data (aiosmtpd uses uppercase method names)."""
        self.logger = logging.getLogger('mail.log')
        try:
            # Parse the email
            content = envelope.content.decode('utf-8', errors='replace')
            msg = message_from_string(content)
            self.logger.info(f"Received email: From={envelope.mail_from}, "
                             f"To={list(envelope.rcpt_tos)}, "
                             f"Subject={msg['Subject']}")

            # Extract subject
            subject_parts = decode_header(msg['Subject'] or '')
            subject = ''.join(part[0].decode(part[1] or 'utf-8')
                              if isinstance(part[0], bytes) else part[0]
                              for part in subject_parts)

            # Extract body
            body = self._extract_body(msg)

            captured = CapturedEmail(
                sender=envelope.mail_from,
                recipients=list(envelope.rcpt_tos),
                subject=subject,
                body=body
            )

            with self._lock:
                self.captured_emails.append(captured)

        except Exception as e:
            self.logger.error(f"Error processing email: {e}")

        return '250 OK'

    def _extract_body(self, msg):
        """Extract the body content from an email message."""
        if msg.is_multipart():
            body_parts = []
            for part in msg.walk():
                if part.get_content_maintype() == 'text':
                    payload = part.get_payload(decode=True)
                    if payload is not None:
                        body_parts.append(payload.decode('utf-8',
                                                         errors='replace'))
            return '\n'.join(body_parts)
        else:
            payload = msg.get_payload(decode=True)
            if payload is None:
                return ""
            return payload.decode('utf-8', errors='replace')

    def get_captured_emails(self):
        with self._lock:
            return list(self.captured_emails)

    def clear_captured_emails(self):
        with self._lock:
            self.captured_emails.clear()


def _generate_self_signed_cert(temp_dir):
    """
    Generate a self-signed certificate and key using openssl.
    Returns (cert_path, key_path) or raises RuntimeError.
    """
    cert_path = os.path.join(temp_dir, 'cert.pem')
    key_path = os.path.join(temp_dir, 'key.pem')

    openssl_path = shutil.which('openssl')
    if not openssl_path:
        raise RuntimeError("openssl not found in PATH; cannot generate "
                           "self-signed certificate")

    cmd = [
        openssl_path, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
        '-keyout', key_path, '-out', cert_path,
        '-subj', '/CN=localhost', '-days', '1'
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to generate self-signed cert: "
                           f"{e.stderr.decode()}")
    return cert_path, key_path


class SMTPServerWrapper:
    """
    Wrapper for aiosmtpd server with thread-based API compatible with smtpd
    behavior.
    """

    def __init__(self, handler, host='127.0.0.1', port=0,
                 log_level=logging.DEBUG, log_file_path=None,
                 use_tls=False, require_starttls=None,
                 cert_file=None, key_file=None):
        self.handler = handler
        self.host = host
        self.port = port
        self.log_level = log_level
        self.log_file_path = log_file_path
        self.use_tls = use_tls
        self.require_starttls = require_starttls
        self.cert_file = cert_file
        self.key_file = key_file
        self.loop = None
        self.server = None
        self.thread = None
        self._stop_event = threading.Event()
        self._temp_cert_dir = None

    def start(self):
        """Start the SMTP server."""
        self._stop_event.clear()

        # Configure aiosmtpd logging level
        smtp_logger = logging.getLogger('mail.log')
        smtp_logger.setLevel(self.log_level)
        # Clear existing handlers and prevent propagation to root logger
        smtp_logger.handlers = []
        smtp_logger.propagate = False

        # Configure file handler if log_file_path is specified
        if self.log_file_path:
            # Add file handler
            log_handler = logging.FileHandler(self.log_file_path)
        else:
            # No log file specified, use console handler
            log_handler = logging.StreamHandler()
        log_handler.setLevel(self.log_level)
        log_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(log_formatter)
        smtp_logger.addHandler(log_handler)

        # Also configure aiosmtpd's internal loggers to prevent console
        # output
        for logger_name in ['aiosmtpd.smtp', 'aiosmtpd.server']:
            aiosmtpd_logger = logging.getLogger(logger_name)
            aiosmtpd_logger.setLevel(self.log_level)
            aiosmtpd_logger.handlers = []
            aiosmtpd_logger.propagate = False
            aiosmtpd_logger.addHandler(log_handler)

        # Prepare TLS context if use_tls is enabled
        tls_context = None
        if self.use_tls:
            cert_file = self.cert_file
            key_file = self.key_file

            # If no cert/key provided, generate self-signed or use fallback
            if not cert_file or not key_file:
                try:
                    self._temp_cert_dir = tempfile.mkdtemp(
                        prefix='mock_smtp_tls_')
                    cert_file, key_file = _generate_self_signed_cert(
                        self._temp_cert_dir)
                    print(f"Generated temporary self-signed certificate in "
                          f"{self._temp_cert_dir}")
                except RuntimeError as e:
                    raise RuntimeError(
                        f"Cannot enable TLS: {e}; fallback certs not found"
                    )

            tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls_context.load_cert_chain(cert_file, key_file)

        def start_server():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            # For port 0, find an available port first
            if self.port == 0:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((self.host, 0))
                self.port = s.getsockname()[1]
                s.close()

            # Build Controller kwargs
            controller_kwargs = {
                'hostname': self.host,
                'port': self.port,
            }
            if tls_context:
                controller_kwargs['tls_context'] = tls_context
                # Determine require_starttls value
                req_tls = self.require_starttls
                if req_tls is None:
                    req_tls = True  # Default to require STARTTLS when TLS on
                controller_kwargs['require_starttls'] = req_tls

            self.server = Controller(self.handler, **controller_kwargs)
            self.server.start()

            tls_msg = " (STARTTLS enabled)" if tls_context else ""
            print(f"Mock SMTP server started on "
                  f"{self.host}:{self.port}{tls_msg}")

            # Run until stop event
            try:
                while not self._stop_event.is_set():
                    self.loop.run_until_complete(asyncio.sleep(0.1))
            except asyncio.CancelledError:
                pass
            finally:
                try:
                    if self.server and not self._stop_event.is_set():
                        self.server.stop()
                    self.loop.close()
                except:
                    pass

        self.thread = threading.Thread(target=start_server, daemon=True)
        self.thread.start()

        # Wait for server to be ready by attempting to connect
        # Retry with 0.1s intervals up to 5 seconds
        max_attempts = 50
        attempt = 0
        print(f"Waiting for SMTP server at {self.host}:{self.port}"
              " to be ready...")
        while attempt < max_attempts:
            try:
                test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_socket.connect((self.host, self.port))
                test_socket.close()
                print(f"SMTP server at {self.host}:{self.port} is ready")
                return
            except (ConnectionRefusedError, OSError):
                attempt += 1
                print(f"Connection attempt {attempt}/{max_attempts} failed, "
                      "retrying...")
                time.sleep(0.1)
            except Exception as e:
                attempt += 1
                print(f"Connection attempt {attempt}/{max_attempts} failed "
                      f"with error: {e}, retrying...")
                time.sleep(0.1)
            finally:
                if 'test_socket' in locals():
                    try:
                        test_socket.close()
                    except:
                        pass

        raise RuntimeError("SMTP server failed to start at "
                           f"{self.host}:{self.port} "
                           f"after {max_attempts * 0.1} seconds")

    def stop(self):
        """Stop the SMTP server."""
        if self._stop_event.is_set():
            # Already stopping or stopped
            return

        self._stop_event.set()
        if self.server:
            self.server.stop()
        if self.thread:
            self.thread.join(timeout=5)

        # Clean up temporary certificate directory if created
        if self._temp_cert_dir and os.path.exists(self._temp_cert_dir):
            try:
                shutil.rmtree(self._temp_cert_dir)
            except Exception:
                pass

        print(f"Mock SMTP server stopped")


class SMTPServerRunner(threading.Thread):
    """Runs the mock SMTP server (maintains API compatibility)."""

    def __init__(self, host='127.0.0.1', port=25, log_level=logging.DEBUG,
                 log_file_path=None):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.log_level = log_level
        self.log_file_path = log_file_path
        self.wrapper = None
        self._stop_event = threading.Event()

        self.handler = None

    def run(self):
        """Run the asyncore loop until stopped."""
        print("WARNING: SMTPServerRunner.run() should not be called directly. "
              "Use start_server() instead.")

    def start_server(self, use_tls=False, require_starttls=None,
                     cert_file=None, key_file=None):
        """Start the SMTP server."""
        self.handler = MockSMTPHandler()
        self.wrapper = SMTPServerWrapper(
            self.handler, self.host, self.port,
            self.log_level, self.log_file_path,
            use_tls=use_tls, require_starttls=require_starttls,
            cert_file=cert_file, key_file=key_file)
        self.wrapper.start()
        self.port = self.wrapper.port
        return self.port

    def stop_server(self):
        """Stop the SMTP server."""
        if self._stop_event.is_set():
            # Already stopping or stopped
            return

        self._stop_event.set()
        if self.wrapper:
            self.wrapper.stop()

    @property
    def captured_emails(self):
        if self.handler:
            return self.handler.get_captured_emails()
        return []

    def clear_emails(self):
        if self.handler:
            self.handler.clear_captured_emails()


def start_mock_smtp_server(host='127.0.0.1', port=0, use_tls=False,
                           log_level=logging.DEBUG, log_file_path=None,
                           require_starttls=None, cert_file=None,
                           key_file=None):
    """
    Convenience function to start a mock SMTP server.

    Args:
        host: Host to bind to (default: 127.0.0.1)
        port: Port to bind to (0 for ephemeral port)
        use_tls: Whether to support STARTTLS
        log_level: Logging level (default: logging.DEBUG)
        log_file_path: Path to file for writing logs (optional)
        require_starttls: If True, require clients to use STARTTLS before
                          mail commands. Defaults to True when use_tls=True.
        cert_file: Path to TLS certificate file (optional; auto-generated if
                   not provided and use_tls=True)
        key_file: Path to TLS private key file (optional; auto-generated if
                  not provided and use_tls=True)

    Returns:
        SMTPServerRunner instance
    """
    runner = SMTPServerRunner(host=host, port=port, log_level=log_level,
                              log_file_path=log_file_path)
    runner.start_server(use_tls=use_tls, require_starttls=require_starttls,
                        cert_file=cert_file, key_file=key_file)
    return runner
