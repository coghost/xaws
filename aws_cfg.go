package xaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func NewAwsConfig(ak, sk, region string) (aws.Config, error) {
	ctx := context.TODO()
	prov := credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     ak,
			SecretAccessKey: sk,
		},
	}
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(prov),
	)

	return cfg, err
}

// NewConfigWithSecret will use ~/.aws/credentials to load secret from secrets manager,
// then use the loaded secret to work on aws resources
func NewConfigWithSecret(secretName string) (aws.Config, error) {
	raw, err := GetSecretWithDefault(secretName)
	if err != nil {
		return aws.Config{}, err
	}

	return newConfigWithSecret(raw)
}

// NewConfigFromSecretWithAkSk, ak, sk is used to get secret from scrects manager.
//
// usage:
//   - require `ak/sk` from Account which only has access to aws:SecretsManager
func NewConfigFromSecretWithAkSk(ak, sk, secretName string) (aws.Config, error) {
	raw, err := GetSecretByAkSk(ak, sk, secretName)
	if err != nil {
		return aws.Config{}, err
	}

	return newConfigWithSecret(raw)
}

func newConfigWithSecret(raw string) (aws.Config, error) {
	var (
		cfg aws.Config
		err error
	)

	auth, err := SecretToAuth(raw)
	if err != nil {
		return cfg, err
	}

	cfg, err = NewAwsConfig(auth.AwsAccessKeyId, auth.AwsSecretAccessKey, "")
	if err != nil {
		panic(err)
	}

	return cfg, nil
}
