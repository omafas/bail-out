package hashicorputils

import (
	"bytes"
	"flag"
	"testing"
	"time"
)

// This is a stand alone sanity test that can be used to test the hashicorputils
// against a live HashiCorp Vault Service.
// It can be used for development and debugging of the hashicorputils package.
// Steps to build and run:
// 1. Build the hashicorputils package:
// 		go build -o hashicorputils .
// 2. Run the test:
// 		go test -key-url=<keyURL> -passphrase=<passphrase>
//              -select-ca-opt=<selectCaOpt> -key-path=<keyPath>
//              -cert-path=<certPath> -cb-ca-path=<cbCaPath>
//              -timeout-duration=<timeoutDuration>
// where <keyURL> is the key URL in HashiCorp Vault URL of key to use for the
// test. Example: -key-url=https://localhost:8200/navKey
// 3. If the test fails, check the Key URL and token are correct

var keyURL = flag.String("key-url", "", "Key URL")
var passphrase = flag.String("passphrase", "", "Passphrase")
var selectCaOpt = flag.String("select-ca-opt", "", "Select CA Option")
var keyPath = flag.String("key-path", "", "Key Path")
var certPath = flag.String("cert-path", "", "Cert Path")
var cbCaPath = flag.String("cb-ca-path", "", "CB CA Path")
var timeoutDuration = flag.Int("timeout-duration", 60000, "Timeout Duration")

func getClientConfig(Value int) OperationArgs {
	return OperationArgs{
		KeyURL:              *keyURL,
		DecryptedPassphrase: []byte(*passphrase),
		TimeoutDuration:     time.Duration(Value) * time.Millisecond,
		SelectCaOpt:         *selectCaOpt,
		KeyPath:             *keyPath,
		CertPath:            *certPath,
		CbCaPath:            *cbCaPath,
	}
}

func TestHashicorpEncryptDecrypt(t *testing.T) {
	clientCfg := getClientConfig(*timeoutDuration)
	data := []byte("This is test data")
	AD := []byte("Test ADD")
	AD2 := []byte("Test ADD2")
	encryptedData, err := KmsEncrypt(clientCfg, data, AD)
	if err != nil {
		t.Fatalf("failed to encrypt data: %s", err.Error())
		return
	}

	decryptedData, err := KmsDecrypt(clientCfg, encryptedData, AD2)
	if err == nil {
		t.Fatalf("expected error because invalid AD, got nil")
	}

	decryptedData, err = KmsDecrypt(clientCfg, encryptedData, AD)
	if err != nil {
		t.Errorf("failed to decrypt data: %s", err.Error())
	}

	if !bytes.Equal(data, decryptedData) {
		t.Fatalf("data mismatch: %s != %s", string(data), string(decryptedData))
	}
}
