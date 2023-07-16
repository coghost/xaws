package xaws

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/coghost/xutil"
)

type S3Wrapper struct {
	Config aws.Config
	Client *s3.Client

	Bucket string
	// upload timeout
	Timeout int
}

func NewS3Wrapper(bucket string, config aws.Config, timeout int) *S3Wrapper {
	sw := &S3Wrapper{
		Config: config,
		Client: s3.NewFromConfig(config),
		Bucket: bucket,
		// timeout
		Timeout: timeout,
	}
	return sw
}

func (w *S3Wrapper) ListBuckets() (*s3.ListBucketsOutput, error) {
	client := s3.NewFromConfig(w.Config)
	res, err := client.ListBuckets(context.TODO(), nil)
	return res, err
}

func (w *S3Wrapper) Upload(localFile, s3path string) (*manager.UploadOutput, error) {
	var cancelFn func()
	ctx := context.Background()
	ctx, cancelFn = context.WithTimeout(ctx, time.Duration(w.Timeout)*time.Second)
	if cancelFn != nil {
		defer cancelFn()
	}

	raw, err := os.Open(localFile)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()

	// refer:
	// https://github.com/awsdocs/aws-doc-sdk-examples/blob/b320aeae1a/go/example_code/s3/upload_arbitrary_sized_stream.go
	go func() {
		gw := gzip.NewWriter(writer)
		if _, e := io.Copy(gw, raw); e != nil {
			panic(e)
		}
		e := raw.Close()
		xutil.PanicIfErr(e)
		e = gw.Close()
		xutil.PanicIfErr(e)
		e = writer.Close()
		xutil.PanicIfErr(e)
	}()

	up := manager.NewUploader(w.Client)
	r, err := up.Upload(ctx, &s3.PutObjectInput{
		Bucket:          &w.Bucket,
		Key:             aws.String(s3path),
		Body:            reader,
		ContentEncoding: aws.String("gzip"),
	})

	return r, err
}

func (w *S3Wrapper) MustUpload(localFile, s3path string) {
	_, err := xutil.EnsureByRetry(
		func() error {
			_, e := w.Upload(localFile, s3path)
			return e
		},
	)

	if err != nil {
		panic(err)
	}
}

// DownloadFile gets an object from a bucket and stores it in a local file.
func (w *S3Wrapper) DownloadFile(objectKey string, fileName string) error {
	result, err := w.Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(w.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return err
	}
	defer result.Body.Close()
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		return err
	}
	_, err = file.Write(body)
	return err
}

// UploadLargeObject uses an upload manager to upload data to an object in a bucket.
// The upload manager breaks large data into parts and uploads the parts concurrently.
func (w *S3Wrapper) UploadLargeObject(bucketName string, objectKey string, largeObject []byte) error {
	largeBuffer := bytes.NewReader(largeObject)
	var partMiBs int64 = 10
	uploader := manager.NewUploader(w.Client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * 1024 * 1024
	})
	_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(objectKey),
		Body:            largeBuffer,
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		log.Printf("Couldn't upload large object to %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
	}

	return err
}
