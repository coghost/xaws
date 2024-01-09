package xaws

import (
	"fmt"
)

func ExampleNewAwsConfig() {
	_, err := NewAwsConfig("ak", "sk", "region")
	fmt.Println(err == nil)
	// Output:
	// true
}

func ExampleNewS3Wrapper() {
	cfg, err := NewAwsConfig("ak", "sk", "region")
	fmt.Println(err == nil)
	s3 := NewS3Wrapper("test", cfg)
	fmt.Println(s3 == nil)
	// Output:
	// true
	// false
}

func ExampleS3Wrapper_Upload() {
	cfg, _ := NewAwsConfig("ak", "sk", "region")
	s3 := NewS3Wrapper("test", cfg)
	s3.Upload("local/file", "/s3path/xxx")
}

func ExampleS3Wrapper_MustUpload() {
	cfg, _ := NewAwsConfig("ak", "sk", "region")
	s3 := NewS3Wrapper("test", cfg)
	s3.MustUpload("local/file", "/s3path/xxx")
}

func ExampleNewSqsWrapper() {
	cfg, err := NewAwsConfig("ak", "sk", "region")
	fmt.Println(err == nil)
	sqs := NewSqsWrapper("test", cfg, 1, 60)
	fmt.Println(sqs == nil)
	// Output:
	// true
	// false
}

func ExampleSqsWrapper_SendMsg() {
	cfg, _ := NewAwsConfig("ak", "sk", "region")
	sqs := NewSqsWrapper("test", cfg, 1, 60)
	sqs.SendMsg("test message")
}

func ExampleSqsWrapper_MustSendMsg() {
	cfg, _ := NewAwsConfig("ak", "sk", "region")
	sqs := NewSqsWrapper("test", cfg, 1, 60)
	sqs.MustSendMsg("test message")
}
