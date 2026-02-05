package gcputils

import (
	"bytes"
	"flag"
	"testing"
	"time"
)

// This is a stand alone sanity test that can be used to test the gcputils
// against a live GCP Key Manager Service. It can be used for development and
// debugging of the gcputils package.
//
// Steps to build and run:
// 1. Build the gcputils package:
// 		go build -o gcputils .
// 2. Run the test:
// 		go test -key-id=<keyResourceId>
// where <keyResourceId> is the ID of the GCP KMS key to use for the test.
// 		For example:
// 			go test -key-id=projects/couchbase-engineering/locations/global/keyRings/test/cryptoKeys/quickstart
// 3. If the test fails, check the Key ID is correct and the GCP credentials are
//    valid. By default the test will use the GCP credentials from the enviroment
//    it is being run in.

// This must be provided as argument to example: "go test -key-id=<keySourceId>"
var keyId = flag.String("key-id", "", "Key ID")

// These are optional and can be provided to "go test" to override the default
// gcp credentials enviroment options.
var pathToServiceFile = flag.String("path-to-service-file", "", "Path to service file")

func TestGcpEncryptDecrypt(t *testing.T) {
	if *keyId == "" {
		t.Fatalf("Key ID is not set, use: go test -key-id=<keyResourceId>")
	}

	opArgs := OperationArgs{
		KeyResourceId:     *keyId,
		PathToServiceFile: *pathToServiceFile,
		TimeoutDuration:   5 * time.Second,
	}

	plainTextEncrypt := []byte("These are my secrets")
	AD := []byte("This is my additional authenticated data")

	encryptedText, err := KmsEncrypt(opArgs, plainTextEncrypt, AD)
	if err != nil {
		t.Fatalf("Could not encrypt data: %s", err.Error())
	}

	plainTextDecrypt, err := KmsDecrypt(opArgs, encryptedText, AD)
	if err != nil {
		t.Fatalf("Could not decrypt data: %s", err.Error())
	}

	if !bytes.Equal(plainTextEncrypt, plainTextDecrypt) {
		t.Fatalf("Mismatch: %s != %s", string(plainTextEncrypt), string(plainTextDecrypt))
	}
}
