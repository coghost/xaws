package xaws

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
)

type SqsSuite struct {
	suite.Suite
	w    *SqsClient
	name string
}

func TestSqs(t *testing.T) {
	suite.Run(t, new(SqsSuite))
}

func (s *SqsSuite) SetupSuite() {
	err := godotenv.Load(".sqs.prod.env")
	s.Require().Nil(err, "Error loading env file: %v", err)

	s.name = os.Getenv("SQS_QUEUE_NAME")
	s.Require().NotEmpty(s.name, "SQS_QUEUE_NAME environment variable is not set")

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

	s.w = NewSqsClient(s.name, cfg, 10, 60)
}

func (s *SqsSuite) TearDownSuite() {
}

func (s *SqsSuite) purgeQueue() {
	s.T().Helper()
	s.T().Log("Purging the queue...")
	_, err := s.w.ClearQueue()
	s.Require().NoError(err, "Failed to purge queue")

	// Wait for a short time to ensure the purge operation completes
	time.Sleep(3 * time.Second)

	// Verify that the queue is empty
	remainingMessages, err := s.w.GetRemainedItems()
	s.Require().NoError(err)
	s.Require().Equal(int64(0), remainingMessages, "Queue should be empty after purging")
	s.T().Log("Queue purged successfully")
}

func (s *SqsSuite) TestSendMsg() {
	// Test successful message send
	message := "Test message"
	response, err := s.w.SendMsg(message)
	s.Require().NoError(err)
	s.Require().NotNil(response)
	s.Require().NotEmpty(response.MessageId)

	// Test sending an empty message
	emptyMessage := ""
	response, err = s.w.SendMsg(emptyMessage)
	s.Require().ErrorIs(err, ErrEmptyMessageBody)
	s.Require().Nil(response)

	// Test sending a long message (close to but not exceeding 256KB limit)
	longMessage := strings.Repeat("a", 256*1024-1)
	response, err = s.w.SendMsg(longMessage)
	s.Require().NoError(err)
	s.Require().NotNil(response)
	s.Require().NotEmpty(response.MessageId)

	// Test sending a message that exceeds the 256KB limit
	tooLongMessage := strings.Repeat("a", 256*1024+1)
	response, err = s.w.SendMsg(tooLongMessage)
	s.Require().ErrorIs(err, ErrMessageTooLong)
	s.Require().Nil(response)

	// Test sending a message with valid special characters
	specialMessage := "!@$%^&*()_+{}|:\"<>?[]\\;',./"
	response, err = s.w.SendMsg(specialMessage)
	s.Require().NoError(err)
	s.Require().NotNil(response)
	s.Require().NotEmpty(response.MessageId)

	// Test sending a message with Unicode characters
	unicodeMessage := "こんにちは世界"
	response, err = s.w.SendMsg(unicodeMessage)
	s.Require().NoError(err)
	s.Require().NotNil(response)
	s.Require().NotEmpty(response.MessageId)

	// Purge the queue after sending messages
	s.purgeQueue()
}

func (s *SqsSuite) TestSendJSONMessage() {
	// Read the JSON file
	jsonPath := filepath.Join("assets", "demo.json")
	jsonData, err := os.ReadFile(jsonPath)
	s.Require().NoError(err, "Failed to read JSON file")

	// Validate JSON
	var jsonContent interface{}
	err = json.Unmarshal(jsonData, &jsonContent)
	s.Require().NoError(err, "Invalid JSON content")

	// Send the JSON content as a message
	response, err := s.w.SendMsg(string(jsonData))
	s.Require().NoError(err, "Failed to send JSON message")
	s.Require().NotNil(response)
	s.Require().NotEmpty(response.MessageId)

	// Retrieve the message to verify its content
	receivedMsg, err := s.w.GetMsg()
	s.Require().NoError(err, "Failed to retrieve message")
	s.Require().Len(receivedMsg.Messages, 1, "Expected to receive 1 message")

	// Verify the content of the received message
	var receivedContent interface{}
	err = json.Unmarshal([]byte(*receivedMsg.Messages[0].Body), &receivedContent)
	s.Require().NoError(err, "Received message is not valid JSON")
	s.Require().Equal(jsonContent, receivedContent, "Received message does not match sent message")

	s.purgeQueue()
}

func (s *SqsSuite) TestCreateQueue() {
	// Generate a unique queue name for testing
	testQueueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())

	// Test creating a new queue
	queueURL, err := s.w.CreateQueue(testQueueName)
	s.Require().NoError(err, "Failed to create queue")
	s.Require().NotEmpty(queueURL, "Queue URL should not be empty")
	s.Require().Contains(queueURL, testQueueName, "Queue URL should contain the queue name")

	// Verify that the queue was created by trying to get its URL
	retrievedURL, err := s.w.GetQueueURL(testQueueName)
	s.Require().NoError(err, "Failed to get queue URL")
	s.Require().Equal(queueURL, retrievedURL, "Retrieved queue URL should match the created queue URL")

	// Clean up: Delete the test queue
	err = s.w.DeleteQueue(testQueueName)
	s.Require().NoError(err, "Failed to delete test queue")

	// Verify that the queue was deleted
	_, err = s.w.GetQueueURL(testQueueName)
	s.Require().Error(err, "Queue should no longer exist")
}

func (s *SqsSuite) Test_0402_ReadMsgs() {
	ch := make(chan *SqsResp, 1)
	defer close(ch)

	go s.w.ReadMessages(ch, WithMax(4))

	timeout := time.After(10 * time.Second)
	messagesProcessed := 0

	for {
		select {
		case resp := <-ch:
			switch resp.Status {
			case SqsReadError:
				s.T().Error("Error occurred while reading messages")
				return
			case SqsReadMaximumReached:
				s.T().Log("Required number of messages matched")
				goto DummyLogic
			case SqsReadAllConsumed:
				s.T().Log("All messages are consumed")
				goto DummyLogic
			case SqsReadSuccess:
				messagesProcessed++
				s.T().Logf("Processed message: %s", *resp.Msg)
			}
		case <-timeout:
			s.T().Fatal("Test timed out after 10 seconds")
			return
		}
	}

DummyLogic:
	s.T().Log("All messages are handled.")
	for i := 0; i < 10; i++ {
		s.T().Log("Dummy logic iteration:", i)
	}
}

func (s *SqsSuite) Test_91_DeleteMsg() {
	r, e := s.w.GetMsg()
	s.Nil(e)
	_, e = s.w.DeleteMsg(r.Messages[0].ReceiptHandle)
	s.Nil(e)
}

func (s *SqsSuite) Test_92_deleteQueue() {
	e := s.w.DeleteQueue(s.name)
	s.Nil(e)
}

func (s *SqsSuite) Test_01_GetQueues() {
	res, e := s.w.GetQueues()
	s.Nil(e)
	s.NotEmpty(res.QueueUrls)
	pp.Println(res.QueueUrls)
}

func (s *SqsSuite) Test_02_GetQueueUrl() {
	r, e := s.w.GetQueueURL(s.w.QueueName)
	s.Nil(e)
	s.Contains(r, s.w.QueueName)
	pp.Println(r)
}

func (s *SqsSuite) Test_0301_SendMsgBatch() {
	messages := []string{"first", "second", "third"}
	for i := 0; i < 20; i++ {
		messages = append(messages, fmt.Sprintf("auto %d", i))
	}
	ss := ChunkSlice(messages, 10)
	for _, sl := range ss {
		r, e := s.w.SendMsgBatch(sl)
		s.Nil(e)
		s.Equal(len(sl), len(r.Successful))
	}
}

func (s *SqsSuite) Test_04_GetMsg() {
	pp.Default.SetExportedOnly(true)
	op, e := s.w.GetMsgs()
	s.Nil(e)
	pp.Println(op)
	pp.Println(len(op.Messages))
	fmt.Println(*op.Messages[0].Body)
}

func (s *SqsSuite) Test_06_remained() {
	r, e := s.w.GetRemainedItems()
	s.Nil(e)
	s.GreaterOrEqual(r, int64(0))
	pp.Println("total", r)
}
