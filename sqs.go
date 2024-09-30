package xaws

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/coghost/xpretty"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"
)

const (
	MaxBatchSize = 10
)

var (
	ErrEmptyMessageBody  = errors.New("message body cannot be empty")
	ErrInvalidUTF8       = errors.New("message contains invalid UTF-8 characters")
	ErrMessageTooLong    = errors.New("message exceeds maximum allowed length")
	ErrQueueNameMismatch = errors.New("queue name does not match the one set during initialization")
	ErrMessageEmpty      = errors.New("message is empty")
	ErrSendBatchFailed   = errors.New("failed to send some messages in batch")
)

type SqsClient struct {
	Config aws.Config
	Client *sqs.Client
	awsCtx context.Context
	// upload timeout
	Timeout int

	QueueName string
	QueueURL  string

	batchSize int
	SendCache []string
}

func NewSqsClient(queue string, cfg aws.Config, batchSize int, timeout int) *SqsClient {
	wrapper := &SqsClient{
		Config:    cfg,
		Client:    sqs.NewFromConfig(cfg),
		awsCtx:    context.TODO(),
		QueueName: queue,
		batchSize: batchSize,
		// timeout
		Timeout: timeout,
	}
	wrapper.SetQueueURL(wrapper.QueueName)
	xpretty.DummyLog("connected to queue:", wrapper.QueueName)

	return wrapper
}

func NewSqsClientWithDefaultConfig(queue string, batchSize int) (*SqsClient, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	return NewSqsClient(queue, cfg, batchSize, _defaultTimeoutSecs), nil
}

func (w *SqsClient) SetQueueURL(name string) {
	if name == "" {
		return
	}

	w.QueueName = name
	w.QueueURL, _ = w.GetQueueURL(w.QueueName)
}

func (w *SqsClient) CreateQueue(name string) (string, error) {
	output, err := w.Client.CreateQueue(w.awsCtx, &sqs.CreateQueueInput{
		QueueName: &name,
		Attributes: map[string]string{
			"DelaySeconds":           "0",
			"MessageRetentionPeriod": "86400",
		},
	})

	w.SetQueueURL(name)

	return *output.QueueUrl, err
}

// PurgeQueue removes all messages from the queue
func (w *SqsClient) PurgeQueue() error {
	_, err := w.Client.PurgeQueue(w.awsCtx, &sqs.PurgeQueueInput{
		QueueUrl: &w.QueueURL,
	})

	return err
}

// ClearQueue removes all messages from the queue without deleting the queue itself.
// It returns the number of messages cleared and any error encountered.
func (w *SqsClient) ClearQueue() (int, error) {
	messagesCleared := 0

	for {
		// Receive up to 10 messages (maximum allowed by SQS)
		result, err := w.GetMsgs(BatchSize(MaxBatchSize))
		if err != nil {
			return messagesCleared, fmt.Errorf("failed to receive messages: %w", err)
		}

		// If no messages, queue is empty
		if len(result.Messages) == 0 {
			break
		}

		// Delete received messages
		for _, msg := range result.Messages {
			_, err := w.DeleteMsg(msg.ReceiptHandle)
			if err != nil {
				return messagesCleared, fmt.Errorf("failed to delete message: %w", err)
			}

			messagesCleared++
		}
	}

	return messagesCleared, nil
}

func (w *SqsClient) DeleteQueue(name string) error {
	url := w.MustGetQueueURL(name)
	if url != w.QueueURL {
		return ErrQueueNameMismatch
	}

	_, err := w.Client.DeleteQueue(
		w.awsCtx,
		&sqs.DeleteQueueInput{
			QueueUrl: &url,
		},
	)

	return err
}

// GetQueues returns a list of queue names
func (w *SqsClient) GetQueues() (*sqs.ListQueuesOutput, error) {
	result, err := w.Client.ListQueues(w.awsCtx, nil)
	return result, err
}

// GetQueueURL gets the URL of an Amazon SQS queue
// Inputs:
//
//	queueName is the name of the queue
//
// Output:
//
//	If success, the URL of the queue and nil
//	Otherwise, an empty string and an error from the call to
func (w *SqsClient) GetQueueURL(name string) (string, error) {
	res, err := w.Client.GetQueueUrl(w.awsCtx, &sqs.GetQueueUrlInput{QueueName: &name})
	if err == nil {
		return *res.QueueUrl, nil
	}

	return "", err
}

func (w *SqsClient) MustGetQueueURL(name string) string {
	r, e := w.GetQueueURL(name)
	panicIfErr(e)

	return r
}

// SendMsg sends a single message to the SQS queue.
//
// This method uses a context with a timeout for the operation. The timeout duration is set by the Timeout field of the SqsClient.
//
// Parameters:
//   - message: A string containing the body of the message to be sent.
//
// Returns:
//   - *sqs.SendMessageOutput: The response from the SQS service, containing details about the sent message.
//     This includes fields like MessageId, which uniquely identifies the message within the queue.
//   - error: An error object which will be non-nil if the send operation failed.
//
// The method will return an error if:
//   - The context times out before the message is sent.
//   - There's a network issue preventing communication with SQS.
//   - The SQS service returns an error (e.g., invalid queue URL, permissions issues).
//   - The message body exceeds the maximum size limit for SQS messages (256 KB).
//
// Example usage:
//
//	client := NewSqsClient(queueName, awsConfig, 10, 60)
//	response, err := client.SendMsg("Hello, SQS!")
//	if err != nil {
//	    log.Printf("Failed to send message: %v", err)
//	    return
//	}
//	log.Printf("Message sent successfully, ID: %s", *response.MessageId)
//
// Note:
// This method is not thread-safe. If you need to send messages concurrently,
// consider using separate SqsClient instances or implement your own synchronization.
func (w *SqsClient) SendMsg(message string) (*sqs.SendMessageOutput, error) {
	if message == "" {
		return nil, ErrEmptyMessageBody
	}

	if !utf8.ValidString(message) {
		return nil, ErrInvalidUTF8
	}

	if len(message) > 256*1024 {
		return nil, ErrMessageTooLong
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(w.Timeout)*time.Second)
	if cancelFn != nil {
		defer cancelFn()
	}

	res, err := w.Client.SendMessage(
		ctx,
		&sqs.SendMessageInput{
			MessageBody: aws.String(message),
			QueueUrl:    &w.QueueURL,
		},
	)

	return res, err
}

func (w *SqsClient) MustSendMsg(message string) *sqs.SendMessageOutput {
	res, err := w.SendMsg(message)
	panicIfErr(err)

	return res
}

// SendMsgWithRetry sends a message to SQS with retry logic.
//
// Parameters:
//   - message: The message to be sent.
//   - retries: The number of retry attempts.
//
// Returns:
//   - *sqs.SendMessageOutput: The response from SQS if successful.
//   - error: An error if all retry attempts fail, nil otherwise.
func (w *SqsClient) SendMsgWithRetry(message string, retries uint) (*sqs.SendMessageOutput, error) {
	var (
		output *sqs.SendMessageOutput
		err    error
	)

	retryErr := retry.Do(
		func() error {
			output, err = w.SendMsg(message)
			return err
		},
		retry.Attempts(retries),
	)

	if retryErr != nil {
		return nil, fmt.Errorf("failed to send message after %d attempts: %w", retries, retryErr)
	}

	return output, nil
}

// MustSendMsgWithRetry is a wrapper around SendMsgWithRetry that panics on error.
//
// This method should be used with caution, as it will panic if the message cannot be sent.
func (w *SqsClient) MustSendMsgWithRetry(message string, retries uint) *sqs.SendMessageOutput {
	output, err := w.SendMsgWithRetry(message, retries)
	panicIfErr(err)

	return output
}

// SendManyMessages sends any number of messages to the SQS queue using batch operations.
// It automatically splits the messages into batches of up to 10 (the SQS maximum).
func (w *SqsClient) SendManyMessages(messages []string) (int, error) {
	const maxBatchSize = 10

	totalSent := 0

	for i := 0; i < len(messages); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(messages) {
			end = len(messages)
		}

		batch := messages[i:end]

		result, err := w.SendMsgBatch(batch)
		if err != nil {
			return totalSent, fmt.Errorf("error sending batch: %w", err)
		}

		totalSent += len(result.Successful)

		if len(result.Failed) > 0 {
			return totalSent, fmt.Errorf("%w: failed to send %d messages in batch", ErrSendBatchFailed, len(result.Failed))
		}
	}

	return totalSent, nil
}

// SendMsgBatch sends multiple messages to the SQS queue in a single batch operation.
//
// This method allows sending up to 10 messages in a single API call, which can improve
// throughput and reduce costs when sending large numbers of messages.
//
// Parameters:
//   - messages: A slice of strings, where each string is the body of a message to be sent.
//
// Returns:
//   - *sqs.SendMessageBatchOutput: The response from the SQS service, containing details about the sent messages.
//     This includes information about successful and failed message sends.
//   - error: An error object which will be non-nil if the batch send operation failed entirely.
//
// The method will return an error if:
//   - The messages slice is empty (ErrMessageEmpty).
//   - There's a network issue preventing communication with SQS.
//   - The SQS service returns an error (e.g., invalid queue URL, permissions issues).
//
// Note:
//   - The maximum number of messages in a batch is 10. If more than 10 messages are provided,
//     only the first 10 will be sent.
//   - Each message in the batch can be up to 256 KB in size.
//   - The total size of all messages in the batch cannot exceed the SQS maximum batch size (256 KB).
//   - If some messages in the batch fail to send, the method will not return an error. Check the
//     Failed field of the output to identify any messages that were not sent successfully.
//
// Example usage:
//
//	messages := []string{"Message 1", "Message 2", "Message 3"}
//	response, err := client.SendMsgBatch(messages)
//	if err != nil {
//	    log.Printf("Failed to send batch: %v", err)
//	    return
//	}
//	log.Printf("Successfully sent %d messages", len(response.Successful))
//	if len(response.Failed) > 0 {
//	    log.Printf("Failed to send %d messages", len(response.Failed))
//	}
//
// This method is not thread-safe. If you need to send batches concurrently,
// consider using separate SqsClient instances or implement your own synchronization.
func (w *SqsClient) SendMsgBatch(messages []string) (*sqs.SendMessageBatchOutput, error) {
	if len(messages) == 0 {
		return nil, ErrMessageEmpty
	}

	var entries []types.SendMessageBatchRequestEntry

	for i, message := range messages {
		et := types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%d", i+1)),
			MessageBody: aws.String(message),
		}
		entries = append(entries, et)
	}

	res, err := w.Client.SendMessageBatch(
		w.awsCtx,
		&sqs.SendMessageBatchInput{
			Entries:  entries,
			QueueUrl: &w.QueueURL,
		})

	return res, err
}

// GetMsgs retrieves multiple messages from the SQS queue.
//
// This method allows for flexible configuration of the message retrieval process
// through the use of functional options.
//
// Parameters:
//   - opts: Variadic SqsOptFunc that allow customization of the retrieval process.
//     Available options include:
//   - WaitTimeSeconds(int): Sets the duration (in seconds) for which the call waits for a message to arrive.
//   - BatchSize(int): Sets the maximum number of messages to return (1-10).
//
// Returns:
//   - *sqs.ReceiveMessageOutput: The response from the SQS service, containing the retrieved messages and metadata.
//   - error: An error object which will be non-nil if the retrieval operation failed.
//
// The method will return an error if:
//   - There's a network issue preventing communication with SQS.
//   - The SQS service returns an error (e.g., invalid queue URL, permissions issues).
//
// Example usage:
//
//	messages, err := client.GetMsgs(WaitTimeSeconds(20), BatchSize(5))
//	if err != nil {
//	    log.Printf("Failed to retrieve messages: %v", err)
//	    return
//	}
//	for _, msg := range messages.Messages {
//	    log.Printf("Received message: %s", *msg.Body)
//	}
//
// Note:
// - If no options are provided, default values will be used (waitTimeSeconds: 3, batchSize: client's default batch size).
// - The actual number of messages returned might be fewer than requested, depending on the queue's contents.
// - Long polling is used by default, which can help reduce empty responses and API calls.
func (w *SqsClient) GetMsgs(opts ...SqsOptFunc) (*sqs.ReceiveMessageOutput, error) {
	const (
		_waitTimeSeconds = 10
	)

	opt := SqsOpts{waitTimeSeconds: _waitTimeSeconds, batchSize: w.batchSize}
	bindSqsOpts(&opt, opts...)

	return w.Client.ReceiveMessage(
		w.awsCtx,
		&sqs.ReceiveMessageInput{
			QueueUrl:            &w.QueueURL,
			MaxNumberOfMessages: int32(opt.batchSize),
			WaitTimeSeconds:     int32(opt.waitTimeSeconds),
		})
}

// GetMsg retrieves a single message from the SQS queue.
//
// This method is a convenience wrapper around GetMsgs, setting the batch size to 1.
// It uses the same underlying SQS ReceiveMessage API call but is optimized for
// retrieving a single message at a time.
//
// Returns:
//   - *sqs.ReceiveMessageOutput: The response from the SQS service, containing
//     at most one message and associated metadata.
//   - error: An error object which will be non-nil if the retrieval operation failed.
//
// The method will return an error if:
//   - There's a network issue preventing communication with SQS.
//   - The SQS service returns an error (e.g., invalid queue URL, permissions issues).
//
// Example usage:
//
//	message, err := client.GetMsg()
//	if err != nil {
//	    log.Printf("Failed to retrieve message: %v", err)
//	    return
//	}
//	if len(message.Messages) > 0 {
//	    log.Printf("Received message: %s", *message.Messages[0].Body)
//	} else {
//	    log.Println("No messages available")
//	}
//
// Note:
//   - This method uses the default wait time as configured in the SqsClient.
//   - If the queue is empty, the returned ReceiveMessageOutput will contain no messages,
//     but the error will still be nil.
//   - For more control over the retrieval process (e.g., wait time), use GetMsgs directly.
func (w *SqsClient) GetMsg() (*sqs.ReceiveMessageOutput, error) {
	return w.GetMsgs(BatchSize(1))
}

func (w *SqsClient) MustGetMsgs(opts ...SqsOptFunc) []*string {
	res, e := w.GetMsgs(opts...)
	panicIfErr(e)

	var arr []*string
	for _, msg := range res.Messages {
		arr = append(arr, msg.Body)
	}

	return arr
}

func (w *SqsClient) MustGetMsg() *types.Message {
	output, err := w.GetMsg()
	panicIfErr(err)

	if len(output.Messages) == 0 {
		return nil
	}

	return &output.Messages[0]
}

// SqsReadStatus represents the status of SQS read operations
type SqsReadStatus int

const (
	// SqsReadError indicates an error occurred during the read operation
	SqsReadError SqsReadStatus = iota

	// SqsReadSuccess indicates a successful read of a message
	SqsReadSuccess

	// SqsReadAllConsumed indicates all messages in the queue have been consumed
	SqsReadAllConsumed

	// SqsReadMaximumReached indicates the maximum number of requested messages has been reached
	SqsReadMaximumReached
)

// String returns the string representation of SqsReadStatus
func (s SqsReadStatus) String() string {
	return [...]string{"Error", "Success", "AllConsumed", "MaximumReached"}[s]
}

type SqsResp struct {
	Status SqsReadStatus
	Msg    *string
}

func NewSqsResp(msg *string, status SqsReadStatus) *SqsResp {
	return &SqsResp{Status: status, Msg: msg}
}

func (w *SqsClient) GoReadMessages(ch chan *SqsResp, opts ...SqsOptFunc) {
	go w.ReadMessages(ch, opts...)
}

func (w *SqsClient) ReadMessages(chanResp chan *SqsResp, opts ...SqsOptFunc) {
	opt := SqsOpts{max: 0}
	bindSqsOpts(&opt, opts...)

	failed := NewSqsResp(nil, SqsReadError)

	got := 0
OUTER:
	for {
		remained, err := w.GetRemainedItems()
		log.Debug().Err(err).Int64("remain", remained).Msg("remained messages")

		if err != nil {
			chanResp <- failed
		}

		if remained == 0 {
			chanResp <- NewSqsResp(nil, SqsReadAllConsumed)
			break
		}

		msgs, err := w.GetMsgs()
		if err != nil {
			chanResp <- failed
		}

		for _, msg := range msgs.Messages {
			chanResp <- NewSqsResp(msg.Body, SqsReadSuccess)
			got += 1
			if opt.max != 0 && got >= opt.max {
				chanResp <- NewSqsResp(nil, SqsReadMaximumReached)
				break OUTER
			}
		}
	}
}

func (w *SqsClient) DeleteMsg(handle *string) (*sqs.DeleteMessageOutput, error) {
	return w.Client.DeleteMessage(
		w.awsCtx,
		&sqs.DeleteMessageInput{
			QueueUrl:      &w.QueueURL,
			ReceiptHandle: handle,
		})
}

func (w *SqsClient) MustDeleteMsg(handle *string) *sqs.DeleteMessageOutput {
	r, err := w.DeleteMsg(handle)
	if err != nil {
		panic(err)
	}

	return r
}

func (w *SqsClient) GetRemainedItems(opts ...SqsOptFunc) (int64, error) {
	opt := &SqsOpts{}
	bindSqsOpts(opt, opts...)

	qurl := w.QueueURL
	if opt.queueName != "" {
		qurl = w.MustGetQueueURL(opt.queueName)
	}

	attr := types.QueueAttributeNameApproximateNumberOfMessages
	res, err := w.Client.GetQueueAttributes(w.awsCtx,
		&sqs.GetQueueAttributesInput{
			QueueUrl:       &qurl,
			AttributeNames: []types.QueueAttributeName{attr},
		},
	)
	n := cast.ToInt64(res.Attributes[string(attr)])

	return n, err
}

func ChunkSlice(slice []string, chunkSize int) [][]string {
	var chunks [][]string

	for {
		if len(slice) == 0 {
			break
		}

		// necessary check to avoid slicing beyond slice capacity
		if len(slice) < chunkSize {
			chunkSize = len(slice)
		}

		chunks = append(chunks, slice[0:chunkSize])
		slice = slice[chunkSize:]
	}

	return chunks
}
