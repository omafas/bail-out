go-kmip
=======

[![Build Status](https://travis-ci.org/smira/go-kmip.svg?branch=master)](https://travis-ci.org/smira/go-kmip)
[![codecov](https://codecov.io/gh/smira/go-kmip/branch/master/graph/badge.svg)](https://codecov.io/gh/smira/go-kmip)
[![Documentation](https://godoc.org/github.com/smira/go-kmip?status.svg)](http://godoc.org/github.com/smira/go-kmip)
[![Go Report Card](https://goreportcard.com/badge/github.com/smira/go-kmip)](https://goreportcard.com/report/github.com/smira/go-kmip)

go-kmip implements subset of [KMIP 1.4](http://docs.oasis-open.org/kmip/spec/v1.4/os/kmip-spec-v1.4-os.html) protocol.

Basic TTLV encoding/decoding is fully implemented, as well as the basic client/server operations. 
Other operations and fields could be implemented by adding required Go structures with KMIP tags.

KMIP protocol is used to access KMS solutions: generating keys, certificates,
accessing stored objects, etc.

KMIP is using TTLV-like encoding, which is implemented in this packaged
as encoding/decoding of Go struct types. Go struct fields are annotated with
`kmip` tags which specify KMIP tag names. Field is encoded/decoded according
to its tag, type.

Two high-level objects are implemented: Server and Client. Server listens for
TLS connections, does initial handshake and processes batch requests from the
clients. Processing of specific operations is delegated to operation handlers.
Client objects establishes connection with the KMIP server and allows sending
any number of requests over the connection.

This package doesn't implement any actual key processing or management - it's outside
the scope of this package.

License
-------

This code is licensed under [MPL 2.0](https://www.mozilla.org/en-US/MPL/2.0/).
