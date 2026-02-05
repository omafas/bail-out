import asyncio
import threading
import logging
import socket
import time
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


class SMTPServerWrapper:
    """
    Wrapper for aiosmtpd server with thread-based API compatible with smtpd
    behavior.
    """

    def __init__(self, handler, host='127.0.0.1', port=0,
                 log_level=logging.DEBUG, log_file_path=None):
        self.handler = handler
        self.host = host
        self.port = port
        self.log_level = log_level
        self.log_file_path = log_file_path
        self.loop = None
        self.server = None
        self.thread = None
        self._stop_event = threading.Event()

    def start(self):
        """Start the SMTP server."""
        self._stop_event.clear()

        # Configure aiosmtpd logging level
        smtp_logger = logging.getLogger('mail.log')
        smtp_logger.setLevel(self.log_level)

        # Configure file handler if log_file_path is specified
        if self.log_file_path:
            # Set logger to INFO level to capture INFO and ERROR messages

            # Clear existing handlers and prevent propagation to root logger
            smtp_logger.handlers = []
            smtp_logger.propagate = False

            # Add file handler
            file_handler = logging.FileHandler(self.log_file_path)
            file_handler.setLevel(self.log_level)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            smtp_logger.addHandler(file_handler)

            # Also configure aiosmtpd's internal loggers to prevent console
            # output
            for logger_name in ['aiosmtpd.smtp', 'aiosmtpd.server']:
                aiosmtpd_logger = logging.getLogger(logger_name)
                aiosmtpd_logger.setLevel(self.log_level)
                aiosmtpd_logger.handlers = []
                aiosmtpd_logger.propagate = False
                aiosmtpd_logger.addHandler(file_handler)

        def start_server():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            # For port 0, find an available port first
            if self.port == 0:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((self.host, 0))
                self.port = s.getsockname()[1]
                s.close()

            self.server = Controller(
                self.handler,
                hostname=self.host,
                port=self.port
            )
            self.server.start()

            print(f"Mock SMTP server started on {self.host}:{self.port}")

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

    def start_server(self, use_tls=False):
        """Start the SMTP server."""
        if use_tls:
            print("WARNING: TLS support not yet implemented")

        self.handler = MockSMTPHandler()
        self.wrapper = SMTPServerWrapper(self.handler, self.host, self.port,
                                         self.log_level, self.log_file_path)
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
                           log_level=logging.DEBUG, log_file_path=None):
    """
    Convenience function to start a mock SMTP server.

    Args:
        host: Host to bind to (default: 127.0.0.1)
        port: Port to bind to (0 for ephemeral port)
        use_tls: Whether to support STARTTLS (not yet implemented)
        log_level: Logging level (default: logging.DEBUG)
        log_file_path: Path to file for writing logs (optional)

    Returns:
        SMTPServerRunner instance
    """
    runner = SMTPServerRunner(host=host, port=port, log_level=log_level,
                              log_file_path=log_file_path)
    runner.start_server(use_tls=use_tls)
    return runner
