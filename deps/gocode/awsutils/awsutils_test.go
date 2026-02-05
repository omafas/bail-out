package awsutils

import (
	"bytes"
	"flag"
	"testing"
)

// This is a stand alone sanity test that can be used to test the awsutils
// against a live AWS Key Manager Service. It can be used for development and
// debugging of the awsutils package.
//
// Steps to build and run:
// 1. Build the awsutils package:
// 		go build -o awsutils .
// 2. Run the test:
// 		go test -key-arn=arn:<keyArn>
// where <keyArn> is the ARN of the KMS key to use for the test.
// 		For example:
// 			go test -key-arn=arn:aws:kms:us-east-1:705067739613:key/8df5ad16-458c-4275-83c9-1503c568a530
// 3. If the test fails, check the Key ARN is correct and the AWS credentials
//    are valid. By default the test will use the AWS credentials from the
//    enviroment it is being run in.

// This must be provided as argument to example: "go test -key-arn=arn:<keyArn>"
var keyArn = flag.String("key-arn", "", "Key ARN")

// These are optional and can be provided to "go test" to override the default
// aws credentials enviroment options.
var region = flag.String("region", "", "Region")
var configFile = flag.String("config-file", "", "Config File")
var credsFile = flag.String("creds-file", "", "Creds File")
var profile = flag.String("profile", "", "Profile")
var useIMDS = flag.Bool("use-imds", false, "Use IMDS")

func TestAwsEncryptDecrypt(t *testing.T) {
	if *keyArn == "" {
		t.Fatalf("Key ARN is not set, use: go test -key-arn=<key-arn>")
	}

	options := AwsConfigOpts{
		Region:     *region,
		ConfigFile: *configFile,
		CredsFile:  *credsFile,
		Profile:    *profile,
		UseIMDS:    *useIMDS,
	}

	plainTextEncrypt := []byte("These are my secrets")
	encryptedText, err := KmsEncryptData(*keyArn, plainTextEncrypt, "testAD", options)
	if err != nil {
		t.Fatalf("Could not encrypt data: %s", err.Error())
	}

	plainTextDecrypt, err := KmsDecryptData(*keyArn, encryptedText, "testAD", options)
	if err != nil {
		t.Fatalf("Could not decrypt data: %s", err.Error())
	}

	if !bytes.Equal(plainTextEncrypt, plainTextDecrypt) {
		t.Fatalf("Mismatch: %s != %s", string(plainTextEncrypt), string(plainTextDecrypt))
	}
}
