// @author Couchbase <info@couchbase.com>
// @copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package gcputils

import (
	"context"
	"fmt"
	"time"

	cloudkms "cloud.google.com/go/kms/apiv1"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"
)

type OperationArgs struct {
	KeyResourceId     string
	PathToServiceFile string
	TimeoutDuration   time.Duration
}

type gcpKeeper struct {
	keyID  string
	client *cloudkms.KeyManagementClient
}

func (k *gcpKeeper) Close() error {
	return k.client.Close()
}

func KmsEncrypt(opArgs OperationArgs, plainText []byte, AD []byte) ([]byte, error) {
	if len(plainText) == 0 {
		return nil, fmt.Errorf("no data to encrypt")
	}

	if err := validateArgs(opArgs); err != nil {
		return nil, err
	}

	ctx, cancel := getContextWithTimeout(opArgs.TimeoutDuration)
	defer cancel()

	keeper, err := getGCPKeeper(ctx, opArgs.KeyResourceId, opArgs.PathToServiceFile)
	if err != nil {
		return nil, fmt.Errorf("get keeper error: %s", err.Error())
	}

	defer keeper.Close()
	res, err := keeper.client.Encrypt(ctx,
		&kmspb.EncryptRequest{
			Plaintext:                   plainText,
			Name:                        keeper.keyID,
			AdditionalAuthenticatedData: AD})
	if err != nil {
		return nil, fmt.Errorf("could not encrypt data: %w", err)
	}

	return res.GetCiphertext(), nil
}

func KmsDecrypt(opArgs OperationArgs, cipherText []byte, AD []byte) ([]byte, error) {
	if len(cipherText) == 0 {
		return nil, fmt.Errorf("no data to decrypt")
	}

	if err := validateArgs(opArgs); err != nil {
		return nil, err
	}

	ctx, cancel := getContextWithTimeout(opArgs.TimeoutDuration)
	defer cancel()

	keeper, err := getGCPKeeper(ctx, opArgs.KeyResourceId, opArgs.PathToServiceFile)
	if err != nil {
		return nil, fmt.Errorf("get keeper error: %s", err.Error())
	}

	defer keeper.Close()
	res, err := keeper.client.Decrypt(ctx,
		&kmspb.DecryptRequest{
			Ciphertext:                  cipherText,
			Name:                        keeper.keyID,
			AdditionalAuthenticatedData: AD})
	if err != nil {
		return nil, fmt.Errorf("could not decrypt data: %w", err)
	}

	return res.GetPlaintext(), nil
}

func validateArgs(opArgs OperationArgs) error {
	if opArgs.KeyResourceId == "" {
		return fmt.Errorf("key resource ID is required")
	}

	return nil
}

func getContextWithTimeout(timeoutDuration time.Duration) (context.Context, context.CancelFunc) {
	if timeoutDuration > 0 {
		return context.WithTimeout(context.Background(), timeoutDuration)
	}
	return context.Background(), func() {}
}

func getGCPKeeper(ctx context.Context, keyID string, pathToServiceFile string) (*gcpKeeper, error) {
	var clientOpts option.ClientOption

	if pathToServiceFile != "" {
		clientOpts = option.WithCredentialsFile(pathToServiceFile)
	}

	keeper := &gcpKeeper{keyID: keyID}

	var err error
	if clientOpts == nil {
		keeper.client, err = cloudkms.NewKeyManagementClient(ctx)
	} else {
		keeper.client, err = cloudkms.NewKeyManagementClient(ctx, clientOpts)
	}

	if err != nil {
		return nil, fmt.Errorf("could not get GCP KMS client: %w", err)
	}

	return keeper, nil
}
