// @author Couchbase <info@couchbase.com>
// @copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package awsutils

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"gocloud.dev/secrets"
	"gocloud.dev/secrets/awskms"
)

type AwsConfigOpts struct {
	Region     string
	ConfigFile string
	CredsFile  string
	Profile    string
	UseIMDS    bool
}

func keeperGetError(err error) error {
	return fmt.Errorf("failed to get keeper due to error: %w", err)
}

func getAwsCfgs(opts AwsConfigOpts) *[]func(*config.LoadOptions) error {
	var configs []func(*config.LoadOptions) error

	if opts.Region != "" {
		configs = append(configs, config.WithRegion(opts.Region))
	}

	if opts.ConfigFile != "" {
		configs = append(configs, config.WithSharedConfigFiles([]string{opts.ConfigFile}))
	}

	if opts.CredsFile != "" {
		configs = append(configs, config.WithSharedCredentialsFiles([]string{opts.CredsFile}))
	}

	if opts.Profile != "" {
		configs = append(configs, config.WithSharedConfigProfile(opts.Profile))
	}

	imdsState := imds.ClientDisabled
	if opts.UseIMDS {
		imdsState = imds.ClientEnabled
	}

	configs = append(configs, config.WithEC2IMDSClientEnableState(imdsState))

	return &configs
}

func getAwsSecretsKeeper(ctx context.Context,
	keyArn string, opts AwsConfigOpts, AD string) (*secrets.Keeper, error) {
	addnlCfgs := getAwsCfgs(opts)
	cfg, err := config.LoadDefaultConfig(ctx, *addnlCfgs...)
	if err != nil {
		return nil, keeperGetError(err)
	}

	client, err := awskms.DialV2(cfg)
	if err != nil {
		return nil, keeperGetError(err)
	}
	keeperOpts := &awskms.KeeperOptions{EncryptionContext: map[string]string{"AD": AD}}
	keeper := awskms.OpenKeeperV2(client, keyArn, keeperOpts)
	return keeper, nil
}

func KmsEncryptData(keyArn string, data []byte, AD string, opts AwsConfigOpts) ([]byte, error) {
	ctx := context.Background()
	keeper, err := getAwsSecretsKeeper(ctx, keyArn, opts, AD)
	if err != nil {
		return nil, err
	}

	defer keeper.Close()

	encryptedData, err := keeper.Encrypt(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("could not encrypt data error: %w", err)
	}
	return encryptedData, nil
}

func KmsDecryptData(keyArn string, data []byte, AD string, opts AwsConfigOpts) ([]byte, error) {
	ctx := context.Background()
	keeper, err := getAwsSecretsKeeper(ctx, keyArn, opts, AD)
	if err != nil {
		return nil, err
	}

	defer keeper.Close()

	decryptedData, err := keeper.Decrypt(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("could not decrypt data error: %w", err)
	}

	return decryptedData, nil
}
