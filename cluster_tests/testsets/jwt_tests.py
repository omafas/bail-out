# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import os
import testlib
from testlib.util import Service
import json
import jwt
import time
import http.server
import threading
import socketserver
import subprocess


class MockJWKSServer:
    """Mock HTTP server for serving JWKS"""

    def __init__(self, initial_jwks):
        self.jwks = initial_jwks
        self.port = self._get_free_port()
        self.is_available = True

        class Handler(http.server.SimpleHTTPRequestHandler):
            def do_GET(handler_self):
                if not self.is_available:
                    handler_self.send_response(503)
                    handler_self.end_headers()
                    return

                handler_self.send_response(200)
                handler_self.send_header("Content-Type", "application/json")
                handler_self.send_header(
                    "Cache-Control", "public, max-age=3600"
                )  # 1 hour cache
                handler_self.end_headers()
                handler_self.wfile.write(json.dumps(self.jwks).encode())

            def log_message(self, format, *args):
                pass

        self.handler = Handler
        self.httpd = socketserver.TCPServer(("", self.port), self.handler)
        self.server_thread = None

    def _get_free_port(self):
        with socketserver.TCPServer(("", 0), None) as s:
            return s.server_address[1]

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        self.is_available = True

    def stop(self):
        if self.server_thread:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.server_thread.join()
            self.server_thread = None

    @property
    def url(self):
        return f"http://localhost:{self.port}"

    def set_unavailable(self):
        """Simulate server being down"""
        self.is_available = False

    def set_available(self):
        """Restore server availability"""
        self.is_available = True

    def rotate_keys(self, new_jwks):
        """Update the served JWKS"""
        self.jwks = new_jwks


class JWTTests(testlib.BaseTestSet):
    test_bucket = "test-jwt-bucket"

    def __init__(self, cluster):
        super().__init__(cluster)
        # Use single endpoint for all JWT operations
        self.endpoint = "/settings/jwt"
        self.external_users = []

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise",
                                           dev_preview=True,
                                           num_vbuckets=16,
                                           buckets=[
                                               {'name': JWTTests.test_bucket,
                                                     'ramQuota': 100}],
                                           services=[Service.KV, Service.QUERY])

    def setup(self):
        # Set shorter intervals for testing
        testlib.set_config_key(self.cluster,
                               "{menelaus_web_jwt, jwks_uri_refresh_min_s}", 1)
        testlib.set_config_key(self.cluster,
                                "{jwt_cache, jwks_cooldown_interval_ms}",
                                1000)
        # Disabled by default
        testlib.get_fail(self.cluster, self.endpoint, expected_code=404)

    def teardown(self):
        testlib.delete_config_key(self.cluster,
                                  "{menelaus_web_jwt, jwks_uri_refresh_min_s}")
        testlib.delete_config_key(self.cluster,
                                  "{jwt_cache, jwks_cooldown_interval_ms}")
        testlib.delete_config_key(self.cluster, "oauthbearer_enabled")
        testlib.delete_succ(self.cluster, self.endpoint)
        testlib.get_fail(self.cluster, self.endpoint, expected_code=404)

        # Clean up all test users
        for username in self.external_users:
            testlib.delete_succ(
                self.cluster, f"/settings/rbac/users/external/{username}"
            )

        # Clean up all test groups
        test_groups = [
            "jwt_bucket_admins",
            "jwt_data_admins",
        ]
        for group in test_groups:
            testlib.delete_succ(self.cluster, f"/settings/rbac/groups/{group}")

    def create_test_groups(self):
        """Create RBAC groups used by tests"""
        groups = [
            ("jwt_bucket_admins", "bucket_admin[*],query_select[*]"
             ",query_use_sequential_scans[*]"),
            ("jwt_data_admins", "data_reader[*],data_writer[*]"),
        ]
        for name, roles in groups:
            group_data = {"roles": roles, "description":
                          f"JWT test group for {name}"}
            testlib.put_succ(
                self.cluster, f"/settings/rbac/groups/{name}", data=group_data
            )

    def load_jwks(self):
        """Load JWKS from test resources"""
        jwks_path = os.path.join(testlib.get_resources_dir(), "jwt",
                                 "jwks.json")
        with open(jwks_path, "r") as f:
            return json.load(f)

    def configure_jwt(self, maps=None):
        """Configure JWT with optional mapping patterns"""
        issuer = {
            "name": "test-issuer",
            "audienceHandling": "any",
            "subClaim": "sub",
            "audClaim": "aud",
            "audiences": ["test-audience"],
            "signingAlgorithm": "RS256",
            "publicKeySource": "jwks",
            "jwks": self.jwks,
            "jitProvisioning": True,
        }
        if maps:
            issuer.update(maps)

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [issuer],
        }
        testlib.put_succ(self.cluster, self.endpoint, json=payload)

    @staticmethod
    def get_supported_algorithms():
        """Return the list of all supported JWT signing algorithms."""
        return [
            "RS256", "RS384", "RS512", "PS256", "PS384", "PS512",
            "ES256", "ES256K", "ES384", "ES512", "EdDSA"
        ]

    @staticmethod
    def get_algorithms_for_key_prefix(key_prefix):
        """Maps a PEM file prefix to compatible JWT algorithms.

        Args:
            key_prefix: The key prefix from filename (e.g., "rsa", "es256")

        Returns:
            List of compatible algorithms for this key type
        """
        mapping = {
            "rsa": ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512"],
            "es256": ["ES256"],
            "es256k": ["ES256K"],
            "es384": ["ES384"],
            "es512": ["ES512"],
            "ed25519": ["EdDSA"],
            "ed448": ["EdDSA"]
        }

        if key_prefix not in mapping:
            raise ValueError(f"Unknown key prefix: {key_prefix}")

        return mapping[key_prefix]

    def verify_pem_resources(self):
        """Verify that all required PEM files exist and cover all supported
        algorithms.

        This function:
        1. Checks that all expected PEM files exist in the resources directory
        2. Verifies that the PEM files cover all algorithms in
           get_supported_algorithms()

        Returns:
            Tuple of (pem_files, resources_dir)

        Raises:
            RuntimeError: If prerequisites are not met
        """
        supported_algorithms = set(self.get_supported_algorithms())
        resources_dir = os.path.join(testlib.get_resources_dir(), "jwt")

        pem_files = [
            f for f in os.listdir(resources_dir)
            if f.endswith(".pem") and "invalid" not in f and "private" not in f
        ]

        # We should have test files for RSA, ES256, ES256K, ES384, ES512,
        # ED25519, and ED448
        expected_pem_count = 7
        assert len(pem_files) == expected_pem_count, (
            f"Expected {expected_pem_count} PEM files but found "
            f"{len(pem_files)}: {pem_files}"
        )

        covered_algorithms = set()
        for pem_file in pem_files:
            key_prefix = pem_file.split("_")[1]

            try:
                # Add all algorithms compatible with this key type
                algorithms = self.get_algorithms_for_key_prefix(key_prefix)
                covered_algorithms.update(algorithms)
            except ValueError as e:
                raise RuntimeError(f"Error processing {pem_file}: {str(e)}")

        missing_algorithms = supported_algorithms - covered_algorithms
        if missing_algorithms:
            raise RuntimeError(
                f"The following algorithms are not covered by PEM files: "
                f"{missing_algorithms}"
            )

        return pem_files, resources_dir

    # REST API Configuration Tests
    def basic_api_test(self):
        # Load a JWKS that contains valid ES256 and RSA keys.
        jwks_path = os.path.join(testlib.get_resources_dir(), "jwt",
                                 "jwks.json")
        with open(jwks_path, "r") as f:
            jwks = json.load(f)

        # Test adding first issuer
        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [
                {
                    "name": "iss1",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["abc"],
                    "signingAlgorithm": "RS256",
                    "publicKeySource": "jwks",
                    "jwks": jwks,
                }
            ],
        }

        testlib.put_succ(self.cluster, self.endpoint, json=payload)

        # Verify first issuer was added
        r = testlib.get_succ(self.cluster, self.endpoint)
        assert len(r.json()["issuers"]) == 1
        assert r.json()["issuers"][0]["name"] == "iss1"

        # Test adding second issuer
        payload["issuers"].append(
            {
                "name": "iss2",
                "audienceHandling": "all",
                "subClaim": "identity",
                "audClaim": "audience",
                "audiences": ["test_audience"],
                "signingAlgorithm": "ES256",
                "publicKeySource": "jwks",
                "jwks": jwks,
            }
        )

        testlib.put_succ(self.cluster, self.endpoint, json=payload)

        # Verify both issuers exist
        r = testlib.get_succ(self.cluster, self.endpoint)
        assert len(r.json()["issuers"]) == 2
        issuer_names = [i["name"] for i in r.json()["issuers"]]
        assert "iss1" in issuer_names
        assert "iss2" in issuer_names

        # Test removing first issuer
        payload["issuers"] = [
            iss for iss in payload["issuers"] if iss["name"] != "iss1"
        ]
        testlib.put_succ(self.cluster, self.endpoint, json=payload)

        # Verify only second issuer remains
        r = testlib.get_succ(self.cluster, self.endpoint)
        assert len(r.json()["issuers"]) == 1
        assert r.json()["issuers"][0]["name"] == "iss2"

        # Test invalid algorithm
        payload["issuers"].append(
            {
                "name": "iss3",
                "audienceHandling": "all",
                "subClaim": "identity",
                "audClaim": "audience",
                "audiences": ["test_audience"],
                "signingAlgorithm": "EdDSA",
                "publicKeySource": "jwks",
                "jwks": jwks,
            }
        )

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert (
            r.json()["errors"]["issuers"][1]["jwks"]
            == f"No suitable keys in JWKS for signing algorithm: 'EdDSA'"
        )

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [
                {
                    "name": "iss1",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["abc"],
                    "signingAlgorithm": "RS256",
                    "publicKeySource": "jwks",
                    "jwks": jwks,
                },
                {
                    "name": "iss2",
                    "audienceHandling": "all",
                    "subClaim": "identity",
                    "audClaim": "audience",
                    "audiences": ["test_audience"],
                    "signingAlgorithm": "ES256",
                    "publicKeySource": "jwks",
                    "jwks": jwks,
                },
                {
                    "name": "iss1",
                    "audienceHandling": "all",
                    "subClaim": "identity",
                    "audClaim": "audience",
                    "audiences": ["test_audience"],
                    "signingAlgorithm": "ES256",
                    "publicKeySource": "jwks",
                    "jwks": jwks,
                },
            ],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert (r.json()["errors"]["issuers"] ==
                "Duplicate issuer names not allowed")

        # Test adding issuer with publicKeySource as jwks but without jwks
        payload["issuers"] = [
            {
                "name": "iss2",
                "audienceHandling": "any",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["abc"],
                "signingAlgorithm": "RS256",
                "publicKeySource": "jwks",
            }
        ]

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert r.json()["errors"]["issuers"][0]["_"] == "jwks is required"
        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [
                {
                    "name": "issuer2",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["abc"],
                    "signingAlgorithm": "RS256",
                    "publicKeySource": "pem",
                }
            ],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert r.json()["errors"]["issuers"][0]["_"] == "publicKey is required"

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [
                {
                    "name": "issuer3",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["abc"],
                    "signingAlgorithm": "RS256",
                    "publicKeySource": "jwks_uri",
                }
            ],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert r.json()["errors"]["issuers"][0]["_"] == "jwksUri is required"

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [
                {
                    "name": "issuer4",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["abc"],
                    "signingAlgorithm": "RS256",
                }
            ],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert (
            r.json()["errors"]["issuers"][0]["_"]
            == "publicKeySource required for algorithm"
        )

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [
                {
                    "name": "issuer5",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["abc"],
                    "signingAlgorithm": "HS256",
                }
            ],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert (
            r.json()["errors"]["issuers"][0]["_"]
            == "sharedSecret required for HMAC algorithm"
        )

    def invalid_pem_test(self):
        """Test JWT authentication with invalid or mismatched PEM keys.

        Tests:
        1. RSA key with insufficient bit length
        2. ES256 key used with ES384 algorithm (curve mismatch)
        3. RSA key used with ES256 algorithm (completely different key type)
        4. ES384 key used with EdDSA algorithm
        """
        resources_dir = os.path.join(testlib.get_resources_dir(), "jwt")

        # Test 1: RSA key with insufficient bit length
        invalid_pem = "mock_rsa_public_invalid.pem"
        pem_path = os.path.join(resources_dir, invalid_pem)
        if not os.path.exists(pem_path):
            raise RuntimeError(
                f"Required test file not found: {invalid_pem}. "
            )

        with open(pem_path, "r") as f:
            invalid_rsa_key = f.read()

            issuer_config = {
                "name": "issuer_short_RS256",
                "audienceHandling": "all",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["test_short_RS256"],
                "signingAlgorithm": "RS256",
                "publicKeySource": "pem",
                "publicKey": invalid_rsa_key,
            }

            payload = {
                "enabled": True,
                "jwksUriRefreshIntervalS": 14400,
                "issuers": [issuer_config],
            }

            r = testlib.put_fail(
                self.cluster, self.endpoint, expected_code=400, json=payload
            )
            assert (
                r.json()["errors"]["issuers"][0]["publicKey"]
                == "The specified key has 1024 bits. Key length should be "
                "between 2048 and 16384"
            )

        # Test 2: ES256 key used with ES384 algorithm (curve mismatch)
        es256_pem = "mock_es256_public.pem"
        es256_path = os.path.join(resources_dir, es256_pem)
        if not os.path.exists(es256_path):
            raise RuntimeError(f"Required test file not found: {es256_pem}")

        with open(es256_path, "r") as f:
            es256_key = f.read()

            issuer_config = {
                "name": "issuer_es_mismatch",
                "audienceHandling": "any",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["test-audience"],
                "signingAlgorithm": "ES384",  # Mismatch with ES256 key
                "publicKeySource": "pem",
                "publicKey": es256_key,
            }

            payload = {
                "enabled": True,
                "jwksUriRefreshIntervalS": 14400,
                "issuers": [issuer_config],
            }

            r = testlib.put_fail(
                self.cluster, self.endpoint, expected_code=400, json=payload
            )
            err_msg = r.json()["errors"]["issuers"][0]["publicKey"]
            assert "Mismatch between algorithm in key" in err_msg, (
                f"Expected mismatch error for ES256 key with ES384 algorithm"
            )

        # Test 3: RSA key used with ES256 algorithm (different key type)
        rsa_pem = "mock_rsa_public.pem"
        rsa_path = os.path.join(resources_dir, rsa_pem)
        if not os.path.exists(rsa_path):
            raise RuntimeError(f"Required test file not found: {rsa_pem}")

        with open(rsa_path, "r") as f:
            rsa_key = f.read()

            issuer_config = {
                "name": "issuer_rsa_es_mismatch",
                "audienceHandling": "any",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["test-audience"],
                "signingAlgorithm": "ES256",
                "publicKeySource": "pem",
                "publicKey": rsa_key,
            }

            payload = {
                "enabled": True,
                "jwksUriRefreshIntervalS": 14400,
                "issuers": [issuer_config],
            }

            r = testlib.put_fail(
                self.cluster, self.endpoint, expected_code=400, json=payload
            )
            err_msg = r.json()["errors"]["issuers"][0]["publicKey"]
            assert "Invalid key for 'ES256' signing algorithm" in err_msg, (
                f"Expected invalid key error for RSA key with ES256 algorithm"
            )

        # Test 4: ES384 key used with EdDSA algorithm
        es384_pem = "mock_es384_public.pem"
        es384_path = os.path.join(resources_dir, es384_pem)
        if not os.path.exists(es384_path):
            raise RuntimeError(f"Required test file not found: {es384_pem}")

        with open(es384_path, "r") as f:
            es384_key = f.read()

            issuer_config = {
                "name": "issuer_es384_eddsa_mismatch",
                "audienceHandling": "any",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["test-audience"],
                "signingAlgorithm": "EdDSA",
                "publicKeySource": "pem",
                "publicKey": es384_key,
            }

            payload = {
                "enabled": True,
                "jwksUriRefreshIntervalS": 14400,
                "issuers": [issuer_config],
            }

            r = testlib.put_fail(
                self.cluster, self.endpoint, expected_code=400, json=payload
            )
            err_msg = r.json()["errors"]["issuers"][0]["publicKey"]
            assert "Mismatch between algorithm in key" in err_msg, (
                f"Expected mismatch error for ES384 key with EdDSA algorithm"
            )

    def invalid_jwks_test(self):
        jwks_path = os.path.join(testlib.get_resources_dir(), "jwt",
                                "mock_invalid_jwks1.json")
        if not os.path.exists(jwks_path):
            raise RuntimeError(f"Required test file not found: "
                                f"{invalid_jwks}. ")
        with open(jwks_path, "r") as f:
            jwks = json.load(f)

        issuer_config = {
            "name": "ES256_issuer",
            "audienceHandling": "all",
            "subClaim": "sub",
            "audClaim": "aud",
            "audiences": ["test_ES256"],
            "signingAlgorithm": "ES256",
            "publicKeySource": "jwks",
            "jwks": jwks,
        }

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [issuer_config],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert (
            r.json()["errors"]["issuers"][0]["jwks"]
            == "Duplicate 'kid' found in JWKS"
            )

        jwks_path = os.path.join(testlib.get_resources_dir(), "jwt",
                                "mock_invalid_jwks2.json")
        if not os.path.exists(jwks_path):
            raise RuntimeError(f"Required test file not found: "
                                f"{invalid_jwks}. ")
        with open(jwks_path, "r") as f:
            jwks = json.load(f)

        issuer_config = {
            "name": "RS256_issuer",
            "audienceHandling": "all",
            "subClaim": "sub",
            "audClaim": "aud",
            "audiences": ["test_RS256"],
            "signingAlgorithm": "RS256",
            "publicKeySource": "jwks",
            "jwks": jwks,
        }

        payload = {
            "enabled": True,
            "jwksUriRefreshIntervalS": 14400,
            "issuers": [issuer_config],
        }

        r = testlib.put_fail(
            self.cluster, self.endpoint, expected_code=400, json=payload
        )
        assert (
            r.json()["errors"]["issuers"][0]["jwks"]
            == "The specified key has 504 bits. Key length should be "
            "between 2048 and 16384; The specified key has 488 bits. Key "
            "length should be between 2048 and 16384"
        )

    # Authentication Tests with Setup
    def auth_setup(self):
        """Setup required for authentication tests"""
        self.create_test_groups()
        self.jwks = self.load_jwks()
        self.base_claims = {
            "iss": "test-issuer",
            "sub": "test-user",
            "aud": "test-audience",
            "exp": int(time.time()) + 3600,
        }

    def direct_auth_test(self):
        """Test JWT authentication without any mapping patterns"""
        self.auth_setup()

        # First test with JIT provisioning disabled
        self.configure_jwt({"jitProvisioning": False})

        claims = self.base_claims.copy()
        claims["groups"] = ["jwt_bucket_admins"]
        token = self.create_token(claims)

        headers = {"Authorization": f"Bearer {token}"}
        r = testlib.get(
            self.cluster, "/pools/default/buckets", auth=None, headers=headers
        )
        assert r.status_code == 401

        # Then test with JIT provisioning enabled
        self.configure_jwt({"jitProvisioning": True})

        r = testlib.get(
            self.cluster, "/pools/default/buckets", auth=None, headers=headers
        )
        assert r.status_code == 200

    def mapped_auth_test(self):
        """Test JWT authentication with mapping patterns and nested claims"""
        self.auth_setup()
        self.configure_jwt(
            {
                "groupsClaim": "user.profile.groups",
                "rolesClaim": "user.roles",
                "groupsMaps": [
                    "^prefix-(.*)-suffix$ jwt_\\1_admins",
                    "^data-(.*)$ jwt_\\1_admins",
                ],
                "rolesMaps": ["^admin-(.*)$ security_\\1"],
            }
        )

        group_test_cases = [
            ("prefix-bucket-suffix", 200),  # Should map to jwt_bucket_admins
            ("data-data", 200),  # Should map to jwt_data_admins
            ("unknown-group", 401),  # No mapping match
        ]

        for group, expected_code in group_test_cases:
            claims = self.base_claims.copy()
            claims["user"] = {"profile": {"groups": [group]}}
            token = self.create_token(claims)

            headers = {"Authorization": f"Bearer {token}"}
            r = testlib.get(
                self.cluster,
                "/pools/default/buckets",
                auth=None,
                headers=headers,
            )
            assert r.status_code == expected_code

        role_test_cases = [
            ("admin-admin", 200),  # Should map to security_admin
            ("unknown-role", 401),  # No mapping match
        ]

        for role, expected_code in role_test_cases:
            claims = self.base_claims.copy()
            claims["user"] = {"roles": [role]}
            token = self.create_token(claims)

            headers = {"Authorization": f"Bearer {token}"}
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == expected_code

        # Test case for missing nested path
        claims = self.base_claims.copy()
        claims["user"] = {
            "groups": [
                "prefix-bucket-suffix"
            ]
        }
        token = self.create_token(claims)

        headers = {"Authorization": f"Bearer {token}"}
        r = testlib.get(
            self.cluster, "/pools/default/buckets", auth=None, headers=headers
        )
        assert r.status_code == 401

    def validation_test(self):
        """Test JWT validation (exp, nbf, aud, signature)"""
        self.auth_setup()
        self.configure_jwt()
        claims = self.base_claims.copy()
        claims["groups"] = ["jwt_bucket_admins"]

        # Test cases: (claim_modifier, alg, key_id, expected_code)
        test_cases = [
            # expired
            (
                lambda c: {**c, "exp": int(time.time()) - 3600},
                "RS256",
                "2011-04-29",
                401,
            ),
            # future
            (
                lambda c: {**c, "nbf": int(time.time()) + 3600},
                "RS256",
                "2011-04-29",
                401,
            ),
            # wrong audience
            (lambda c: {**c, "aud": "wrong-audience"}, "RS256", "2011-04-29",
             401),
            # wrong signature
            (lambda c: c, "ES256", "es256-key", 401),
        ]

        for modify_claims, alg, key_id, expected_code in test_cases:
            token = self.create_token(modify_claims(claims), key_id=key_id,
                                      alg=alg)
            headers = {"Authorization": f"Bearer {token}"}
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == expected_code

    @staticmethod
    def create_token(claims, key_id="2011-04-29", alg="RS256"):
        """Create a signed JWT for testing.
        Args:
            claims: Dictionary of JWT claims
            key_id: Key ID to use in JWT header
            alg: Signing algorithm (RS256, ES256, etc)
        Returns:
            Signed JWT string
        """
        # For RSA algorithms (RS* and PS*), use the shared RSA key
        if alg.startswith("RS") or alg.startswith("PS"):
            key_prefix = "rsa"
        else:
            key_prefix = alg.lower()

        # Handle rotated key
        if key_id == "rotated-key":
            key_file = f"mock_{key_prefix}_private_rotated.pem"
        else:
            key_file = f"mock_{key_prefix}_private.pem"

        with open(os.path.join(testlib.get_resources_dir(), "jwt", key_file),
                  "r") as f:
            private_key = f.read()

        headers = {"kid": key_id}
        return jwt.encode(claims, private_key, algorithm=alg, headers=headers)

    def load_hmac_secrets(self):
        """Load HMAC secrets from test resources"""
        secrets_path = os.path.join(
            testlib.get_resources_dir(), "jwt", "mock_hmac_secrets.json"
        )
        with open(secrets_path, "r") as f:
            return json.load(f)

    def hmac_test(self):
        """Test JWT authentication with HMAC algorithms"""
        self.auth_setup()

        # Load HMAC secrets
        secrets = self.load_hmac_secrets()

        # Test each HMAC algorithm
        for alg, config in secrets.items():
            # Configure JWT with HMAC
            issuer_config = {
                "name": f"hmac-issuer-{alg}",
                "audienceHandling": "any",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["test-audience"],
                "signingAlgorithm": alg,
                "sharedSecret": config["secret"],
                "jitProvisioning": True,
            }

            payload = {
                "enabled": True,
                "jwksUriRefreshIntervalS": 14400,
                "issuers": [issuer_config],
            }

            testlib.put_succ(self.cluster, self.endpoint, json=payload)

            # Create and test a token
            claims = self.base_claims.copy()
            claims["iss"] = f"hmac-issuer-{alg}"
            claims["groups"] = ["jwt_bucket_admins"]

            token = jwt.encode(claims, config["secret"], algorithm=alg)

            headers = {"Authorization": f"Bearer {token}"}
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 200

            # Test with wrong secret
            wrong_token = jwt.encode(claims, "wrong-secret", algorithm=alg)
            headers = {"Authorization": f"Bearer {wrong_token}"}
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 401

    def jwks_uri_test(self):
        """Test JWT authentication with JWKS URI"""
        self.auth_setup()

        # Load test keys
        with open(os.path.join(testlib.get_resources_dir(), "jwt",
                               "jwks.json")) as f:
            initial_jwks = json.load(f)

        with open(
            os.path.join(testlib.get_resources_dir(), "jwt",
                         "jwks_rotated.json")
        ) as f:
            rotated_jwks = json.load(f)

        with MockJWKSServer(initial_jwks) as server:
            # Configure JWT to use JWKS URI
            issuer_config = {
                "name": "jwks-uri-issuer",
                "audienceHandling": "any",
                "subClaim": "sub",
                "audClaim": "aud",
                "audiences": ["test-audience"],
                "signingAlgorithm": "RS256",
                "publicKeySource": "jwks_uri",
                "jwksUri": server.url,
                "jitProvisioning": True,
                "jwksUriHttpTimeoutMs": 5000,
            }

            payload = {
                "enabled": True,
                "jwksUriRefreshIntervalS": 14400,
                "issuers": [issuer_config],
            }

            testlib.put_succ(self.cluster, self.endpoint, json=payload)

            # Test successful key fetch and token validation
            claims = self.base_claims.copy()
            claims["iss"] = "jwks-uri-issuer"
            claims["groups"] = ["jwt_bucket_admins"]
            token = self.create_token(claims)

            headers = {"Authorization": f"Bearer {token}"}
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 200

            # Test server unavailable, settings change forces invalidation
            server.set_unavailable()
            testlib.put_succ(self.cluster, self.endpoint, json=payload)
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 401

            # Test key rotation with auto-refresh
            server.set_available()
            # Update settings with short refresh interval for key rotation test
            payload["jwksUriRefreshIntervalS"] = 1
            testlib.put_succ(self.cluster, self.endpoint, json=payload)
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 200

            server.rotate_keys(rotated_jwks)
            time.sleep(2)  # Wait for auto-refresh to happen

            # Old token is still present in the rotated JWKS
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 200

            # New token with rotated key
            token = self.create_token(claims, key_id="rotated-key")
            headers = {"Authorization": f"Bearer {token}"}
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 200

            # Test invalid JWKS response, settings change forces refetch
            server.rotate_keys({"keys": []})  # Empty JWKS
            testlib.put_succ(self.cluster, self.endpoint, json=payload)
            r = testlib.get(
                self.cluster, "/pools/default/buckets", auth=None,
                headers=headers
            )
            assert r.status_code == 401

    def access_forbidden_test(self):
        """Test that access_forbidden audit events are properly triggered with
        JWT auth.
        """
        self.auth_setup()

        read_only_group = "jwt_readonly_users"
        group_data = {
            "roles": "data_reader[*]",  # Only read permissions, no write
            "description": "JWT test group for read-only users"
        }
        testlib.put_succ(
            self.cluster, f"/settings/rbac/groups/{read_only_group}",
             data=group_data
        )

        # Configure JWT with our read-only group mapping
        self.configure_jwt(
            {
                "jitProvisioning": True,
                "groupsMaps": [
                    "^readonly$ jwt_readonly_users"
                ]
            }
        )

        # Create a token for a user with only read permissions
        claims = self.base_claims.copy()
        claims["groups"] = ["readonly"]
        token = self.create_token(claims)
        headers = {"Authorization": f"Bearer {token}"}

        # First verify the user can read buckets (should succeed)
        r = testlib.get_succ(
            self.cluster, "/pools/default/buckets", auth=None, headers=headers
        )

        # Try to create a bucket (should fail with 403 Forbidden)
        # This should trigger an access_forbidden audit event
        bucket_data = {
            "name": "test_bucket",
            "ramQuotaMB": 100,
            "bucketType": "membase"
        }
        r = testlib.post_fail(
            self.cluster,
            "/pools/default/buckets",
            auth=None,
            headers=headers,
            data=bucket_data,
            expected_code=403  # Expect forbidden
        )

    def oauthbearer_memcached_test(self):
        """Test OAUTHBEARER authentication with memcached using mcstat"""
        testlib.set_config_key(self.cluster, "oauthbearer_enabled", True)

        # Create a test bucket for mcstat
        test_bucket = "test-jwt-bucket"
        node = self.cluster.connected_nodes[0]

        self.auth_setup()
        self.create_test_groups()
        self.jwks = self.load_jwks()
        self.configure_jwt()

        # Use a consistent username
        username = "testuser"

        # Create a JWT with appropriate claims
        payload = {
            "sub": username,
            "groups": ["jwt_bucket_admins"],
            "exp": int(time.time()) + 3600,
            "aud": "test-audience",
            "iss": "test-issuer"
        }

        token = self.create_token(payload)

        # Create an invalid token (expired)
        invalid_payload = dict(payload)
        invalid_payload["exp"] = int(time.time()) - 3600  # Expired token
        invalid_token = self.create_token(invalid_payload)

        memcached_port = node.tls_service_port(Service.KV)

        mcstat_path = testlib.get_utility_path("mcstat")
        if not os.path.exists(mcstat_path):
            assert False, f"mcstat utility not found at {mcstat_path}"

        cmd = [
            mcstat_path,
            "-h", node.host,
            "-p", str(memcached_port),
            "--sasl_mechanism", "OAUTHBEARER",
            "--user", username,
            "--password", token,
            "--bucket", self.test_bucket,
            "--tls"
        ]

        # Wait for OAUTHBEARER authentication to succeed, we need to poll to
        # wait for external_auth to be enabled in memcached once JWT is enabled.
        testlib.poll_for_condition(
            lambda: subprocess.run(
                cmd, capture_output=True, text=True).returncode == 0,
            sleep_time=0.5,
            timeout=30
        )

        bad_test_cases = [
            {
                "desc": "Invalid token (expired) authentication",
                "token": invalid_token,
                "username": username,
                "expected_success": False,
            },
            {
                "desc": "Mismatched username authentication",
                "token": token,
                "username": "wronguser",
                "expected_success": False,
            }
        ]

        for tc in bad_test_cases:
            test_cmd = [
                mcstat_path,
                "-h", node.host,
                "-p", str(memcached_port),
                "--sasl_mechanism", "OAUTHBEARER",
                "--user", tc["username"],
                "--password", tc["token"],
                "--bucket", self.test_bucket,
                "--tls"
            ]

            result = subprocess.run(test_cmd, capture_output=True, text=True)

            assert result.returncode != 0, \
                "OAUTHBEARER authentication should fail but succeeded"
            assert "Authentication failed" in result.stderr, \
                "Expected error message not found"

    def pem_auth_test(self):
        """Test JWT authentication with PEM public key configurations.

        For each PEM file, test authentication with all compatible algorithms.
        """
        self.auth_setup()

        all_pem_files, resources_dir = self.verify_pem_resources()

        for pem_file in all_pem_files:
            key_prefix = pem_file.split("_")[1]
            pub_file = pem_file
            priv_file = pub_file.replace("public", "private")

            with open(os.path.join(resources_dir, pub_file), "r") as f:
                public_key = f.read()

            with open(os.path.join(resources_dir, priv_file), "r") as f:
                private_key = f.read()

            compatible_algorithms = self.get_algorithms_for_key_prefix(
                key_prefix
            )

            for alg in compatible_algorithms:
                issuer_config = {
                    "name": f"pem-issuer-{alg}-{key_prefix}",
                    "audienceHandling": "any",
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audiences": ["test-audience"],
                    "signingAlgorithm": alg,
                    "publicKeySource": "pem",
                    "publicKey": public_key,
                    "jitProvisioning": True
                }

                payload = {
                    "enabled": True,
                    "jwksUriRefreshIntervalS": 14400,
                    "issuers": [issuer_config]
                }

                testlib.put_succ(self.cluster, self.endpoint, json=payload)

                claims = self.base_claims.copy()
                claims["iss"] = f"pem-issuer-{alg}-{key_prefix}"
                claims["groups"] = ["jwt_bucket_admins"]

                token = jwt.encode(claims, private_key, algorithm=alg)

                headers = {"Authorization": f"Bearer {token}"}
                r = testlib.get(
                    self.cluster,
                    "/pools/default/buckets",
                    auth=None,
                    headers=headers
                )
                assert r.status_code == 200, (
                    f"Authentication failed for key '{key_prefix}' "
                    f"with algorithm '{alg}'"
                )

    def jwt_query_test(self):
        """Test JWT authentication with Query service using direct HTTP
        requests.
        """
        self.auth_setup()
        self.create_test_groups()
        self.jwks = self.load_jwks()
        self.configure_jwt()

        node = self.cluster.connected_nodes[0]

        # Base test case that should succeed
        valid_payload = {
            "sub": "testuser",
            "groups": ["jwt_bucket_admins"],
            "exp": int(time.time()) + 3600,
            "aud": "test-audience",
            "iss": "test-issuer"
        }

        valid_token = self.create_token(valid_payload)
        headers = {
            "Authorization": f"Bearer {valid_token}",
            "Content-Type": "application/json"
        }

        query_payload = {
            "statement": f"SELECT * FROM `{self.test_bucket}`"
        }

        # Poll until Query service accepts the request with valid token
        testlib.poll_for_condition(
            lambda: testlib.request(
                "POST",
                node,
                "/query/service",
                https=True,
                service=Service.QUERY,
                headers=headers,
                json=query_payload,
                auth=None
            ).status_code == 200,
            sleep_time=1,
            timeout=60
        )

        test_cases = [
            {
                "desc": "Expired token",
                "payload_changes": {"exp": int(time.time()) - 3600},
                "remove_fields": ["groups"]
            },
            {
                "desc": "Valid token but no groups",
                "payload_changes": {},
                "remove_fields": ["groups"]
            }
        ]

        for tc in test_cases:
            payload = valid_payload.copy()
            payload.update(tc["payload_changes"])
            for field in tc["remove_fields"]:
                payload.pop(field, None)

            token = self.create_token(payload)
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            r = testlib.request(
                "POST",
                node,
                "/query/service",
                https=True,
                service=Service.QUERY,
                headers=headers,
                json=query_payload,
                auth=None,
                expected_code=401
            )

    def external_user_test(self):
        """Test authentication with external users having different permissions.

        Tests:
        1. External user with group only (jwt_data_admins)
        2. External user with role only (security_admin)
        3. External user with both group and role
        """
        self.auth_setup()
        self.create_test_groups()

        users = [
            {
                "name": "group_user",
                "data": {"groups": ["jwt_data_admins"]},
            },
            {
                "name": "role_user",
                "data": {"roles": "security_admin"},
            },
            {
                "name": "combined_user",
                "data": {
                    "roles": "security_admin",
                    "groups": ["jwt_data_admins"],
                },
            },
        ]

        for user in users:
            testlib.put_succ(
                self.cluster,
                f"/settings/rbac/users/external/{user['name']}",
                data=user["data"],
            )
            self.external_users.append(user["name"])

        self.configure_jwt({"jitProvisioning": False})

        test_cases = [
            {
                "username": "group_user",
                "expected_bucket_read": True,
                "expected_jwt_read": False,
            },
            {
                "username": "role_user",
                "expected_bucket_read": False,
                "expected_jwt_read": True,
            },
            {
                "username": "combined_user",
                "expected_bucket_read": True,
                "expected_jwt_read": True,
            },
        ]

        for tc in test_cases:
            claims = self.base_claims.copy()
            claims["sub"] = tc["username"]
            token = self.create_token(claims)
            headers = {"Authorization": f"Bearer {token}"}

            # Test bucket data read access (requires data_reader role)
            if tc["expected_bucket_read"]:
                r = testlib.get_succ(
                    self.cluster,
                    f"/pools/default/buckets/{self.test_bucket}/docs",
                    auth=None,
                    headers=headers,
                )
            else:
                r = testlib.get_fail(
                    self.cluster,
                    f"/pools/default/buckets/{self.test_bucket}/docs",
                    auth=None,
                    headers=headers,
                    expected_code=403,
                )

            # Test JWT settings read access (requires security_admin role)
            if tc["expected_jwt_read"]:
                r = testlib.get_succ(
                    self.cluster,
                    self.endpoint,
                    auth=None,
                    headers=headers,
                )
            else:
                r = testlib.get_fail(
                    self.cluster,
                    self.endpoint,
                    auth=None,
                    headers=headers,
                    expected_code=403,
                )

        # Now test with JIT provisioning enabled
        self.configure_jwt({"jitProvisioning": True})

        jit_test_cases = [
            {
                "username": "jit_group_user",
                "groups": ["jwt_data_admins"],
                "expected_bucket_read": True,
                "expected_jwt_read": False,
            },
            {
                "username": "jit_role_user",
                "roles": "security_admin",
                "expected_bucket_read": False,
                "expected_jwt_read": True,
            },
            {
                "username": "jit_combined_user",
                "groups": ["jwt_data_admins"],
                "roles": "security_admin",
                "expected_bucket_read": True,
                "expected_jwt_read": True,
            },
            {
                "username": "jit_missing_perms_user",
                "groups": ["jwt_data_admins"],
                "roles": "data_writer[*]",  # Different role from group
                "expected_bucket_read": True,  # Still has read from group
                "expected_jwt_read": False,
            },
        ]

        for tc in jit_test_cases:
            claims = self.base_claims.copy()
            claims["sub"] = tc["username"]
            if "groups" in tc:
                claims["groups"] = tc["groups"]
            if "roles" in tc:
                claims["roles"] = tc["roles"]
            token = self.create_token(claims)
            headers = {"Authorization": f"Bearer {token}"}

            # Test bucket data read access (requires data_reader role)
            if tc["expected_bucket_read"]:
                r = testlib.get_succ(
                    self.cluster,
                    f"/pools/default/buckets/{self.test_bucket}/docs",
                    auth=None,
                    headers=headers,
                )
            else:
                r = testlib.get_fail(
                    self.cluster,
                    f"/pools/default/buckets/{self.test_bucket}/docs",
                    auth=None,
                    headers=headers,
                    expected_code=403,
                )

            # Test JWT settings read access
            if tc["expected_jwt_read"]:
                r = testlib.get_succ(
                    self.cluster,
                    self.endpoint,
                    auth=None,
                    headers=headers,
                )
            else:
                r = testlib.get_fail(
                    self.cluster,
                    self.endpoint,
                    auth=None,
                    headers=headers,
                    expected_code=403,
                )

        # Test with invalid token (should fail)
        invalid_claims = self.base_claims.copy()
        invalid_claims["exp"] = int(time.time()) - 3600  # Expired token
        invalid_token = self.create_token(invalid_claims)
        headers = {"Authorization": f"Bearer {invalid_token}"}
        r = testlib.get_fail(
            self.cluster,
            f"/pools/default/buckets/{self.test_bucket}/docs",
            auth=None,
            headers=headers,
            expected_code=401,
        )
