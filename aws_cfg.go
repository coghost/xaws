package xaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func NewAwsConfig(ak, sk, region string) (aws.Config, error) {
	ctx := context.TODO()
	cp := credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     ak,
			SecretAccessKey: sk,
		},
	}
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(cp),
	)
	return cfg, err
}
