package kmip

import "crypto/tls"

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// DefaultServerTLSConfig fills in good defaults for server TLS configuration
func DefaultServerTLSConfig(config *tls.Config) {
	config.MinVersion = tls.VersionTLS12
	config.PreferServerCipherSuites = true
	config.ClientAuth = tls.RequireAndVerifyClientCert
}

// DefaultClientTLSConfig fills in good defaults for client TLS configuration
func DefaultClientTLSConfig(config *tls.Config) {
	config.MinVersion = tls.VersionTLS12
}
