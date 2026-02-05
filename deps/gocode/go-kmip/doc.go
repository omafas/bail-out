// Package kmip implements KMIP protocol
//
// KMIP protocol is used to access KMS solutions: generating keys, certificates,
// accessing stored objects, etc.
//
// KMIP is using TTLV-like encoding, which is implemented in this packaged
// as encoding/decoding of Go struct types. Go struct fields are annotated with
// `kmip` tags which specify KMIP tag names. Field is encoded/decoded according
// to its tag, type.
//
// Two high-level objects are implemented: Server and Client. Server listens for
// TLS connections, does initial handshake and processes batch requests from the
// clients. Processing of specific operations is delegated to operation handlers.
// Client objects establishes connection with the KMIP server and allows sending
// any number of requests over the connection.
//
// Not all the KMIP operations have corresponding Go structs, missing ones should
// be added to operations.go, and dynamic type dispatch to
// RequestBatchItem/ResponseBatchItem.BuildFieldValue methods.
package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
