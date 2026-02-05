// @author Couchbase <info@couchbase.com>
// @copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/ns_server/deps/gocode/awsutils"
)

type awsStoredKey struct {
	baseStoredKey
	KeyArn       string `json:"keyArn"`
	Region       string `json:"region"`
	ConfigFile   string `json:"configFile"`
	CredsFile    string `json:"credsFile"`
	Profile      string `json:"profile"`
	UseIMDS      bool   `jspn:"useIMDS"`
	ReqTimeoutMs int    `json:"reqTimeoutMs"`
}

// Implementation of storedKeyIface for aws keys

func newAwsKey(name, kind, creationTime string, data []byte) (*awsStoredKey, error) {
	var awsk awsStoredKey
	err := json.Unmarshal(data, &awsk)
	if err != nil {
		return nil, fmt.Errorf("invalid json: %v", data)
	}
	awsk.baseStoredKey = baseStoredKey{Name: name, Kind: kind, CreationTime: creationTime}
	return &awsk, nil
}

func (k *awsStoredKey) name() string {
	return k.Name
}

func (k *awsStoredKey) kind() string {
	return k.Kind
}

func (k *awsStoredKey) needRewrite(settings *storedKeyConfig, state *StoredKeysState, ctx *storedKeysCtx) (bool, int, error) {
	keyIface, vsn, _, err := readKeyRaw(settings, k.Name)
	if err != nil {
		var keyNotFoundErr ErrKeyNotFound
		if !errors.As(err, &keyNotFoundErr) {
			logDbg("key %s read error: %s", k.Name, err.Error())
		}
		return true, vsn, nil
	}
	onDiskKey, ok := keyIface.(*awsStoredKey)
	if !ok {
		logDbg("key %s changed type, rewriting", k.Name)
		return true, vsn, nil
	}
	return !reflect.DeepEqual(k, onDiskKey), vsn, nil
}

func (k *awsStoredKey) ad() []byte {
	return []byte("")
}

func (k *awsStoredKey) asBytes() ([]byte, error) {
	return []byte(
		string(awskmKey) +
			k.Name +
			k.Kind +
			k.KeyArn +
			k.Region +
			k.ConfigFile +
			k.CredsFile +
			k.Profile +
			strconv.FormatBool(k.UseIMDS) +
			k.CreationTime), nil
}

func (k *awsStoredKey) encryptMe(state *StoredKeysState, ctx *storedKeysCtx) error {
	// Nothing to encrypt here, configuration should contain no secrets
	return nil
}

func (k *awsStoredKey) decryptMe(validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) error {
	return nil
}

func (k *awsStoredKey) checkAWSTestKey() (bool, error) {
	if k.KeyArn == "TEST_AWS_KEY_ARN" {
		if k.CredsFile != "" {
			credsData, err := os.ReadFile(k.CredsFile)
			if err == nil {
				if strings.TrimSpace(string(credsData)) == "TEST_BAD_AWS_CREDS" {
					return true, fmt.Errorf("test encryption error")
				}
			}
		}
		return true, nil
	}
	if k.KeyArn == "TEST_AWS_BAD_KEY_ARN" {
		return true, fmt.Errorf("test encryption error")
	}
	return false, nil
}

func getAwsConfigOpts(k *awsStoredKey) (*awsutils.AwsConfigOpts, error) {
	if err := validateTimeout(k.ReqTimeoutMs); err != nil {
		return nil, err
	}

	return &awsutils.AwsConfigOpts{
		Region:          k.Region,
		ConfigFile:      k.ConfigFile,
		CredsFile:       k.CredsFile,
		Profile:         k.Profile,
		UseIMDS:         k.UseIMDS,
		TimeoutDuration: time.Duration(k.ReqTimeoutMs) * time.Millisecond,
	}, nil
}

func (k *awsStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	if isTestKey, err := k.checkAWSTestKey(); isTestKey {
		if err != nil {
			return nil, err
		}
		// This code should be used for test purposes only
		logDbg("Encrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmEncrypt(zero_key, data, AD), nil
	}

	opts, err := getAwsConfigOpts(k)
	if err != nil {
		return nil, err
	}
	// AD parameters are strings in Kms :(
	strAD := base64.StdEncoding.EncodeToString(AD)
	return awsutils.KmsEncryptData(k.KeyArn, data, strAD, *opts)
}

func (k *awsStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if isTestKey, err := k.checkAWSTestKey(); isTestKey {
		if err != nil {
			return nil, err
		}
		// This code should be used for test purposes only
		logDbg("Decrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmDecrypt(zero_key, data, AD)
	}

	opts, err := getAwsConfigOpts(k)
	if err != nil {
		return nil, err
	}
	// AD parameters are strings in Kms :(
	strAD := base64.StdEncoding.EncodeToString(AD)
	return awsutils.KmsDecryptData(k.KeyArn, data, strAD, *opts)
}

func (k *awsStoredKey) unmarshal(data json.RawMessage) error {
	err := json.Unmarshal(data, k)
	if err != nil {
		return fmt.Errorf("invalid raw key json: %s", err.Error())
	}
	return nil
}

func (k *awsStoredKey) marshal() (storedKeyType, []byte, error) {
	data, err := json.Marshal(k)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return awskmKey, data, nil
}

func (k *awsStoredKey) usesSecretManagementKey() bool {
	return false
}

func (k *awsStoredKey) canBeCached() bool {
	return false
}
