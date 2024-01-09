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

func (w *S3Wrapper) UploadToBucket(localFile, bucket, s3path string) (*manager.UploadOutput, error) {
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

func (w *S3Wrapper) Upload(localFile, s3path string) (*manager.UploadOutput, error) {
	return w.UploadToBucket(localFile, w.Bucket, s3path)
}

func (w *S3Wrapper) MustUpload(localFile, s3path string) {
	err := retry.Do(
		func() error {
			_, e := w.Upload(localFile, s3path)
			return e
		},
		retry.Attempts(_retryTimes),
	)

	panicIfErr(err)
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

	if body, err := io.ReadAll(result.Body); err != nil {
		return err
	} else {
		_, err = file.Write(body)
		return err
	}
}

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

func (w *S3Wrapper) IsExisted(objectKey string) (bool, error) {
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
func (w *S3Wrapper) MustUploadRawData(raw string, objectKey string, opts ...S3OptionFunc) {
	err := w.UploadRawData(raw, objectKey, opts...)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot upload raw data")
	}
}

func (w *S3Wrapper) UploadRawData(raw, objectKey string, opts ...S3OptionFunc) error {
	opt := &S3Options{bucket: w.Bucket, withGz: true}
	bindS3Options(opt, opts...)

	if opt.withGz {
		raw = MustGzipStrToStr(raw)

		if fsutil.Suffix(objectKey) != _dotgz {
			objectKey += _dotgz
		}
	}

	ul := manager.NewUploader(w.Client)

	_, err := ul.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(opt.bucket),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(raw),
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

	_, err := w.Upload(name, objectKey)

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

func MustGzipStrToStr(raw string) string {
	bt := MustGzipStr(raw)
	return string(bt)
}

func MustGzipStr(raw string) []byte {
	v, err := GzipStr(raw)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot convert to gzip buffer")
	}

	return v.Bytes()
}

func GzipStr(raw string) (*bytes.Buffer, error) {
	var buf bytes.Buffer

	gzwr, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := gzwr.Write([]byte(raw)); err != nil {
		return nil, err
	}

	if err := gzwr.Close(); err != nil {
		return nil, err
	}

	return &buf, nil
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
		resp, err := w.Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  aws.String(w.Bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: int32(opt.maxKeys),
			// pagination
			ContinuationToken: nextToken,
		})
		if err != nil {
			return found, err
		}

		for _, item := range resp.Contents {
			name := *item.Key
			isGz := fsutil.Suffix(name) == _dotgz

			if !opt.withEmptyFile && isGz && item.Size <= emptyGzFileSize {
				// this is just a rough detection
				continue
			}

			if !opt.withEmptyFile && item.Size == emptyFileSize {
				continue
			}

			found = append(found, *item.Key)
			if opt.maxKeys != 0 && len(found) >= opt.maxKeys {
				break
			}
		}

		if !resp.IsTruncated {
			break
		}

		nextToken = resp.NextContinuationToken
	}

	return found, nil
}
