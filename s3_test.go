package xaws

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/joho/godotenv"
	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
)

type S3Suite struct {
	suite.Suite
	wrapper    *S3Client
	testBucket string
	testPrefix string
}

func TestS3(t *testing.T) {
	suite.Run(t, new(S3Suite))
}

func (s *S3Suite) SetupSuite() {
	err := godotenv.Load()
	s.Require().Nil(err, "Error loading .env file: %v", err)

	s.testBucket = os.Getenv("TEST_S3_BUCKET")
	s.Require().NotEmpty(s.testBucket, "TEST_S3_BUCKET environment variable is not set")

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	region := os.Getenv("AWS_REGION")

	s.Require().NotEmpty(accessKeyID, "AWS_ACCESS_KEY_ID environment variable is not set")
	s.Require().NotEmpty(secretAccessKey, "AWS_SECRET_ACCESS_KEY environment variable is not set")

	if region == "" {
		region = "us-west-2"
	}

	cfg, err := NewAwsConfig(accessKeyID, secretAccessKey, region)
	s.Require().Nil(err, "Failed to create AWS config: %v", err)

	s.wrapper = NewS3Wrapper(s.testBucket, cfg)
	s.testPrefix = "tests/"
}

func (s *S3Suite) TearDownSuite() {
	// List all objects under the test prefix
	objects, err := s.wrapper.ListObjects(s.testPrefix)
	if err != nil {
		s.T().Logf("Failed to list objects for cleanup: %v", err)
		return
	}

	var wg sync.WaitGroup
	for _, obj := range objects {
		wg.Add(1)
		go func(object string) {
			defer wg.Done()
			err := s.wrapper.DeleteObject(object)
			if err != nil {
				s.T().Logf("Failed to delete test object %s: %v", object, err)
			}
		}(obj)
	}
	wg.Wait()

	// Verify that all objects have been deleted
	remainingObjects, err := s.wrapper.ListObjects(s.testPrefix)
	if err != nil {
		s.T().Logf("Failed to verify cleanup: %v", err)
	} else if len(remainingObjects) > 0 {
		s.T().Logf("Some objects were not deleted: %v", remainingObjects)
	}
}

func (s *S3Suite) TearDownTest() {
	testPrefix := fmt.Sprintf("%s%s/", s.testPrefix, s.T().Name())
	objects, err := s.wrapper.ListObjects(testPrefix)
	if err != nil {
		s.T().Logf("Failed to list objects for test cleanup: %v", err)
		return
	}

	for _, obj := range objects {
		err := s.wrapper.DeleteObject(obj)
		if err != nil {
			s.T().Logf("Failed to delete test object %s: %v", obj, err)
		}
	}
}

func (s *S3Suite) TestUploadAndGetObject() {
	s.T().Parallel()
	testObject := fmt.Sprintf("%stest-object-%s.txt", s.testPrefix, s.T().Name())
	testContent := []byte("This is a test content for S3 integration test.")

	err := s.wrapper.UploadRawData(testObject, testContent)
	s.Nil(err, "Failed to upload object")

	retrievedContent, err := s.wrapper.GetObject(testObject)
	s.Nil(err, "Failed to get object")
	s.Equal(testContent, retrievedContent, "Retrieved content doesn't match uploaded content")
}

func (s *S3Suite) TestHasObject() {
	s.T().Parallel()
	testObj := fmt.Sprintf("%shas-object-test-%s.txt", s.testPrefix, s.T().Name())

	exists, err := s.wrapper.HasObject(testObj)
	s.Nil(err, "Error checking for non-existing object")
	s.False(exists, "Object should not exist initially")

	err = s.wrapper.UploadRawData(testObj, []byte("test content"))
	s.Nil(err, "Failed to upload object for HasObject test")

	exists, err = s.wrapper.HasObject(testObj)
	s.Nil(err, "Error checking for existing object")
	s.True(exists, "Object should exist after upload")
}

func (s *S3Suite) TestListObjects() {
	s.T().Parallel()
	testObjects := []string{"test1.txt", "test2.txt", "test3.txt"}
	for i, obj := range testObjects {
		testObjects[i] = fmt.Sprintf("%s%s-%s", s.testPrefix, obj, s.T().Name())
		err := s.wrapper.UploadRawData(testObjects[i], []byte("test content"))
		s.Nil(err, "Failed to upload test object")
	}

	objects, err := s.wrapper.ListObjects(s.testPrefix)
	s.Nil(err, "Failed to list objects")
	s.GreaterOrEqual(len(objects), len(testObjects), "Should list at least the uploaded test objects")
}

func (s *S3Suite) TestDeleteObject() {
	s.T().Parallel()
	testObj := fmt.Sprintf("%sdelete-test-object-%s.txt", s.testPrefix, s.T().Name())

	err := s.wrapper.UploadRawData(testObj, []byte("delete me"))
	s.Nil(err, "Failed to upload test object for deletion")

	exists, err := s.wrapper.HasObject(testObj)
	s.Nil(err, "Error checking for existing object")
	s.True(exists, "Object should exist before deletion")

	err = s.wrapper.DeleteObject(testObj)
	s.Nil(err, "Failed to delete object")

	exists, err = s.wrapper.HasObject(testObj)
	s.Nil(err, "Error checking for deleted object")
	s.False(exists, "Object should not exist after deletion")
}

func (s *S3Suite) TestListBuckets() {
	s.T().Parallel()
	buckets, err := s.wrapper.ListBuckets()
	s.Nil(err, "Failed to list buckets")
	s.NotEmpty(buckets.Buckets, "No buckets found")
}

func (s *S3Suite) TestUploadToBucketWithAutoGzipped() {
	s.T().Parallel()
	testFile := fmt.Sprintf("%stest-gzip-%s.txt", s.testPrefix, s.T().Name())
	testContent := []byte("This is a test content for gzip upload.")

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test-gzip-*.txt")
	s.Require().NoError(err, "Failed to create temporary file")
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(testContent)
	s.Require().NoError(err, "Failed to write to temporary file")
	tmpFile.Close()

	// Test with and without .gz suffix
	testCases := []string{testFile, testFile + ".gz"}

	for _, tc := range testCases {
		objectKey := tc
		if !strings.HasSuffix(objectKey, ".gz") {
			objectKey += ".gz"
		}

		_, err = s.wrapper.UploadToBucketWithAutoGzipped(tmpFile.Name(), tc, s.testBucket)
		s.Require().NoError(err, "Failed to upload gzipped file")

		// Test retrieving the gzipped content
		gzippedContent, err := s.wrapper.GetObject(objectKey)
		s.Require().NoError(err, "Failed to get gzipped object")

		// Verify the gzipped content is different from the original
		s.NotEqual(testContent, gzippedContent, "Gzipped content should be different from original")

		// Test retrieving and auto-ungzipping the content
		ungzippedContent, err := s.wrapper.GetObject(objectKey, WithAutoUnGzip(true))
		s.Require().NoError(err, "Failed to get and auto-ungzip object")

		// Verify the ungzipped content matches the original
		s.Equal(testContent, ungzippedContent, "Ungzipped content should match original")

		// Delete the test object
		err = s.wrapper.DeleteObject(objectKey)
		s.Require().NoError(err, "Failed to delete test object")

		// Verify the object has been deleted
		exists, err := s.wrapper.HasObject(objectKey)
		s.Require().NoError(err, "Error checking if object was deleted")
		s.False(exists, "Object should have been deleted")
	}
}

func (s *S3Suite) TestGetObject() {
	s.T().Parallel()
	// Create unique prefixes for this test to ensure proper cleanup
	testPrefix := fmt.Sprintf("%sgetobject-test-%s/", s.testPrefix, s.T().Name())

	// Define test objects
	testObject := testPrefix + "testfile.txt"
	emptyObject := testPrefix + "empty.txt"
	nonExistentObject := testPrefix + "nonexistent.txt"
	pp.Println(testObject)

	testContent := []byte("This is a test content for GetObject.")

	// Defer cleanup
	defer func() {
		// List all objects under the test prefix
		objects, err := s.wrapper.ListObjects(testPrefix)
		s.Require().NoError(err, "Failed to list objects for cleanup")

		// Delete all objects
		for _, obj := range objects {
			err := s.wrapper.DeleteObject(obj)
			if err != nil {
				s.T().Logf("Failed to delete test object %s: %v", obj, err)
			}
		}
	}()

	// Upload test content
	err := s.wrapper.UploadRawData(testObject, testContent)
	s.Require().NoError(err, "Failed to upload object for GetObject test")

	// Use GetObject to retrieve the content
	retrievedContent, err := s.wrapper.GetObject(testObject)
	s.Require().NoError(err, "Failed to get object")
	s.Equal(testContent, retrievedContent, "Retrieved content doesn't match uploaded content")

	// Test writing to a local file (optional, to demonstrate file operations)
	tempDir := os.TempDir()
	fileName := filepath.Base(testObject)
	localFilePath := filepath.Join(tempDir, fileName)
	err = os.WriteFile(localFilePath, retrievedContent, 0o644)
	s.Require().NoError(err, "Failed to write retrieved content to local file")

	// Verify the content of the written file
	fileContent, err := os.ReadFile(localFilePath)
	s.Require().NoError(err, "Failed to read local file")
	s.Equal(testContent, fileContent, "Local file content doesn't match uploaded content")

	// Clean up
	os.Remove(localFilePath)

	// Test GetObject with non-existent object
	nonExistentContent, err := s.wrapper.GetObject(nonExistentObject)
	s.Require().NoError(err, "GetObject should not return an error for non-existent object")
	s.Nil(nonExistentContent, "Content should be nil for non-existent object")

	// Verify that the non-existent object really doesn't exist
	exists, err := s.wrapper.HasObject(nonExistentObject)
	s.Require().NoError(err, "HasObject should not return an error")
	s.False(exists, "Non-existent object should not exist")

	// Test GetObject with empty object
	err = s.wrapper.UploadRawData(emptyObject, []byte{})
	s.Require().NoError(err, "Failed to upload empty object")
	pp.Println(emptyObject)

	emptyContent, err := s.wrapper.GetObject(emptyObject)
	s.Require().NoError(err, "Failed to get empty object")
	s.Empty(emptyContent, "Retrieved content of empty object should be empty")
}

func (s *S3Suite) TestUploadLargeObject() {
	s.T().Parallel()
	testObject := fmt.Sprintf("%slarge-object-test-%s.txt", s.testPrefix, s.T().Name())
	largeContent := make([]byte, 15*1024*1024) // 15 MB of data

	err := s.wrapper.UploadLargeObject(s.testBucket, testObject, largeContent)
	s.Nil(err, "Failed to upload large object")

	retrievedContent, err := s.wrapper.GetObject(testObject)
	s.Nil(err, "Failed to get large object")
	s.Equal(largeContent, retrievedContent, "Retrieved large content doesn't match uploaded content")
}

func (s *S3Suite) TestUploadRawDataToGz() {
	s.T().Parallel()
	testObject := fmt.Sprintf("%sraw-data-gzip-test-%s.txt.gz", s.testPrefix, s.T().Name())
	testContent := "This is a test content for raw data gzip upload."

	err := s.wrapper.UploadRawDataToGz(testContent, testObject)
	s.Nil(err, "Failed to upload raw data to gzip")

	retrievedContent, err := s.wrapper.GetObject(testObject)
	s.Nil(err, "Failed to get gzipped object")
	s.Equal([]byte(testContent), retrievedContent, "Retrieved content doesn't match uploaded content")
}

func (s *S3Suite) TestListObjectsWithOptions() {
	s.T().Parallel()
	testPrefix := fmt.Sprintf("%slist-options-test-%s/", s.testPrefix, s.T().Name())
	testObjects := []string{"test1.txt", "test2.txt", "test3.txt"}

	for _, obj := range testObjects {
		err := s.wrapper.UploadRawData(testPrefix+obj, []byte("test content"))
		s.Nil(err, "Failed to upload test object")
	}

	// Test with maxKeys option
	objects, err := s.wrapper.ListObjects(testPrefix, WithMaxKeys(2))
	s.Nil(err, "Failed to list objects with maxKeys")
	s.Equal(2, len(objects), "Should list exactly 2 objects")

	// Test with withEmptyFile option
	emptyObject := testPrefix + "empty.txt"
	err = s.wrapper.UploadRawData(emptyObject, []byte{})
	s.Nil(err, "Failed to upload empty object")

	objects, err = s.wrapper.ListObjects(testPrefix, WithEmptyFile(true))
	s.Nil(err, "Failed to list objects including empty files")
	s.Contains(objects, emptyObject, "Should include empty file in the list")
}

func (s *S3Suite) TestDeleteObj() {
	s3uri := "s3://crunchbase-data-sync-bucket/news-activities/data/metadata.json"
	err := s.wrapper.DeleteObject(s3uri)
	// objects, err := s.wrapper.ListObjects(s.testPrefix)
	s.Nil(err)
	// pp.Println(objects)
}
