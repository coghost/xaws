package xaws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type Auth struct {
	AwsAccessKeyID     string `json:"aws_access_key_id"`
	AwsSecretAccessKey string `json:"aws_secret_access_key"`
}

func SecretToAuth(raw string) (*Auth, error) {
	auth := &Auth{}

	err := json.Unmarshal([]byte(raw), &auth)
	if err != nil {
		return nil, err
	}

	return auth, nil
}

func MustGetSecretWithDefault(secretName string) string {
	str, err := GetSecretWithDefault(secretName)
	if err != nil {
		panic(err)
	}

	return str
}

func GetSecretWithDefault(secretName string) (string, error) {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return "", fmt.Errorf("cannot load config: %w", err)
	}

	return getSecret(config, secretName)
}

func MustGetSecret(ak, sk, secretName string) string {
	str, err := GetSecretByAkSk(ak, sk, secretName)
	if err != nil {
		panic(err)
	}

	return str
}

func GetSecretByAkSk(ak, sk, secretName string) (string, error) {
	config, err := NewAwsConfig(ak, sk, "")
	if err != nil {
		return "", fmt.Errorf("cannot load config: %w", err)
	}

	return getSecret(config, secretName)
}

func getSecret(config aws.Config, secretName string) (string, error) {
	// Create Secrets Manager client
	svc := secretsmanager.NewFromConfig(config)

	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(secretName),
		VersionStage: aws.String("AWSCURRENT"), // VersionStage defaults to AWSCURRENT if unspecified
	}

	result, err := svc.GetSecretValue(context.TODO(), input)
	if err != nil {
		// For a list of exceptions thrown, see
		// https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
		return "", fmt.Errorf("cannot get secret: %w", err)
	}

	return *result.SecretString, nil
}
