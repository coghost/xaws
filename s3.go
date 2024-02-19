package xaws

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gookit/goutil/fsutil"

	"github.com/rs/zerolog/log"
)

const (
	_defaultTimeoutSecs = 60
	_defaultBatchSize   = 10
	_retryTimes         = 3
	_defaultSaveTo      = "/tmp"
)

const (
	_dotgz = ".gz"
)

var ErrGzSuffixRequired = errors.New("non gz format: .gz is required")

type S3Wrapper struct {
	Config aws.Config
	Client *s3.Client

	Bucket string
	// upload timeout
	Timeout int

	SaveTo string
}

func NewS3Wrapper(bucket string, cfg aws.Config, opts ...S3OptionFunc) *S3Wrapper {
	opt := &S3Options{timeout: _defaultTimeoutSecs, saveTo: _defaultSaveTo}
	bindS3Options(opt, opts...)

	return &S3Wrapper{
		Config: cfg,
		Client: s3.NewFromConfig(cfg),
		Bucket: bucket,
		// timeout
		Timeout: opt.timeout,
		SaveTo:  opt.saveTo,
	}
}

func MustNewS3WrapperWithDefaultConfig(bucket string, opts ...S3OptionFunc) *S3Wrapper {
	w, err := NewS3WrapperWithDefaultConfig(bucket, opts...)
	if err != nil {
		panic(err)
	}

	return w
}

func NewS3WrapperWithDefaultConfig(bucket string, opts ...S3OptionFunc) (*S3Wrapper, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	return NewS3Wrapper(bucket, cfg, opts...), nil
}

func (w *S3Wrapper) ListBuckets() (*s3.ListBucketsOutput, error) {
	client := s3.NewFromConfig(w.Config)
	return client.ListBuckets(context.TODO(), nil)
}

func (w *S3Wrapper) UploadToBucketWithAutoGzipped(localFile, s3path, bucket string) (*manager.UploadOutput, error) {
	// var cancelFn func()
	// ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(w.Timeout)*time.Second)
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
		panicIfErr(e)
		e = gw.Close()
		panicIfErr(e)
		e = writer.Close()
		panicIfErr(e)
	}()

	up := manager.NewUploader(w.Client)
	r, err := up.Upload(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(s3path),
		Body:            reader,
		ContentEncoding: aws.String("gzip"),
	})

	return r, err
}

func (w *S3Wrapper) UploadWithAutoGzipped(localFile, s3path string) (*manager.UploadOutput, error) {
	return w.UploadToBucketWithAutoGzipped(localFile, s3path, w.Bucket)
}

func (w *S3Wrapper) MustUploadWithAutoGzipped(localFile, s3path string) {
	err := retry.Do(
		func() error {
			_, e := w.UploadWithAutoGzipped(localFile, s3path)
			return e
		},
		retry.Attempts(_retryTimes),
	)

	panicIfErr(err)
}

func (w *S3Wrapper) GetObjectContent(objectKey string) ([]byte, error) {
	result, err := w.Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(w.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

// Deprecated: please use get object in the future
//
//	@receiver w
//	@param objectKey
//	@param fileName
//	@return error
func (w *S3Wrapper) DownloadFile(objectKey string, fileName string) error {
	body, err := w.GetObjectContent(objectKey)
	if err != nil {
		return err
	}

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(body)

	return err
}

func (w *S3Wrapper) GetObject(objectKey string) ([]byte, error) {
	b, err := w.HasObject(objectKey)
	if err != nil {
		log.Error().Err(err).Msg("got error when check file exist status")
		return nil, err
	}

	if !b {
		return nil, nil
	}

	return w.GetObjectContent(objectKey)
}

// Deprecated: please use get object in the future
// Download downloads objectKey to default folder(w.SaveTo) with same name
//   - if file existed, directly return the filename
//
// about opt.folderLevel:
//   - `-1`: use objectKey
//   - `0`: only name
//   - else with folders
func (w *S3Wrapper) Download(objectKey string, opts ...S3OptionFunc) string {
	opt := &S3Options{folderLevel: 1}
	bindS3Options(opt, opts...)

	name := fsutil.Name(objectKey)

	if lvl := opt.folderLevel; lvl != 0 {
		if lvl == -1 {
			name = objectKey
		}

		arr := strings.Split(objectKey, "/")
		total := len(arr) - 1

		if lvl <= total {
			name = strings.Join(arr[total-lvl:], "/")
		}
	}

	if opt.savedName != "" {
		name = opt.savedName
	}

	dst := fsutil.JoinPaths(w.SaveTo, name)

	if fsutil.PathExist(dst) {
		return dst
	}

	fsutil.MkParentDir(dst)

	err := w.DownloadFile(objectKey, dst)
	if err != nil {
		log.Error().Err(err).Msg("cannot download file")
		return ""
	}

	return dst
}

func (w *S3Wrapper) HasObject(objectKey string) (bool, error) {
	_, err := w.Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(w.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				return false, nil
			default:
				return false, err
			}
		}
	}

	return true, err
}

// UploadLargeObject uses an upload manager to upload data to an object in a bucket.
// The upload manager breaks large data into parts and uploads the parts concurrently.
func (w *S3Wrapper) UploadLargeObject(bucketName string, objectKey string, largeObject []byte) error {
	var (
		partMiBs int64 = 10
		kilo     int64 = 1024
	)

	largeBuffer := bytes.NewReader(largeObject)
	uploader := manager.NewUploader(w.Client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * kilo * kilo
	})

	if _, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(objectKey),
		Body:            largeBuffer,
		ContentEncoding: aws.String("gzip"),
	}); err != nil {
		log.Printf("Couldn't upload large object to %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
		return err
	}

	return nil
}

// UploadRawData uploads and save raw data to s3 object key(no encoding:gzip supported).
func (w *S3Wrapper) MustUploadRawData(objectKey string, raw []byte, opts ...S3OptionFunc) {
	err := w.UploadRawData(objectKey, raw, opts...)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot upload raw data")
	}
}

func (w *S3Wrapper) PutObject(objectKey string, raw []byte, opts ...S3OptionFunc) error {
	return w.UploadRawData(objectKey, raw, opts...)
}

func (w *S3Wrapper) UploadRawData(objectKey string, raw []byte, opts ...S3OptionFunc) error {
	opt := &S3Options{bucket: w.Bucket, withGz: true}
	bindS3Options(opt, opts...)

	if opt.withGz {
		if fsutil.Suffix(objectKey) != _dotgz {
			objectKey += _dotgz
		}
	}

	ul := manager.NewUploader(w.Client)

	_, err := ul.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(opt.bucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(raw),
	})

	return err
}

func (w *S3Wrapper) UploadRawDataToGz(raw string, objectKey string) error {
	if fsutil.Suffix(objectKey) != _dotgz {
		return ErrGzSuffixRequired
	}

	minLen := 12
	name := "/tmp/" + randSeq(minLen) + "_.txt"

	fsutil.MustSave(name, raw)
	// defer os.Remove(name)

	_, err := w.UploadWithAutoGzipped(name, objectKey)

	return err
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

// ListObjects list all available objects in bucket with prefix.
//
//	@param prefix
//	@param opts
//
//	@return []string: list s3 files found
//	@return error
func (w *S3Wrapper) ListObjects(prefix string, opts ...S3OptionFunc) ([]string, error) {
	opt := &S3Options{}
	bindS3Options(opt, opts...)

	var found []string

	var (
		nextToken       *string
		emptyGzFileSize int64 = 22
		emptyFileSize   int64 = 0
	)

	for {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(w.Bucket),
			Prefix: aws.String(prefix),
			// MaxKeys: aws.Int32(maxKeysIn),
			// pagination
			ContinuationToken: nextToken,
		}

		if opt.maxKeys != 0 {
			maxKeysIn := int32(opt.maxKeys)
			input.MaxKeys = &maxKeysIn
		}

		resp, err := w.Client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			return found, err
		}

		for _, item := range resp.Contents {
			name := *item.Key
			isGz := fsutil.Suffix(name) == _dotgz

			if !opt.withEmptyFile && isGz && *item.Size <= emptyGzFileSize {
				// this is just a rough detection
				continue
			}

			if !opt.withEmptyFile && *item.Size == emptyFileSize {
				continue
			}

			found = append(found, *item.Key)
			if opt.maxKeys != 0 && len(found) >= opt.maxKeys {
				break
			}
		}

		if !*resp.IsTruncated {
			break
		}

		if opt.maxKeys != 0 && len(found) > opt.maxKeys {
			found = found[:opt.maxKeys]
			break
		}

		nextToken = resp.NextContinuationToken
	}

	return found, nil
}
