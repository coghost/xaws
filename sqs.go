package xaws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/coghost/xpretty"
	"github.com/coghost/xutil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"
)

type Roles string

const (
	Q_EMPTY    Roles = ""
	Q_C        Roles = "c"   // create
	Q_R        Roles = "r"   // read
	Q_D        Roles = "d"   // delete
	Q_ADMIN    Roles = "crd" // crd
	Q_PRODUCER Roles = "cr"  // cr
	Q_CONSUMER Roles = "rd"  // rd
)

var ErrRoleViolation = errors.New("role violation: current role is not allowed to perform this operation")

type SqsWrapper struct {
	Config aws.Config
	Client *sqs.Client
	awsCtx context.Context
	// upload timeout
	Timeout int

	QueueName string
	QueueUrl  string

	BatchSize int
	SendCache []string

	Role Roles
}

func NewSqsWrapper(queue string, cfg aws.Config, batchSize int, timeout int) *SqsWrapper {
	sw := &SqsWrapper{
		Config:    cfg,
		Client:    sqs.NewFromConfig(cfg),
		awsCtx:    context.TODO(),
		QueueName: queue,
		BatchSize: batchSize,
		// timeout
		Timeout: timeout,
	}
	sw.SetQueueUrl(sw.QueueName)
	xpretty.DummyLog("connected to queue:", sw.QueueName)
	return sw
}

func (w *SqsWrapper) SetQueueUrl(name string) {
	if name == "" {
		return
	}
	w.QueueName = name
	w.QueueUrl, _ = w.GetQueueUrl(w.QueueName)
}

func (w *SqsWrapper) CreateQueue(name string) (string, error) {
	r, e := w.Client.CreateQueue(w.awsCtx, &sqs.CreateQueueInput{
		QueueName: &name,
		Attributes: map[string]string{
			"DelaySeconds":           "0",
			"MessageRetentionPeriod": "86400",
		},
	})

	w.SetQueueUrl(name)
	return *r.QueueUrl, e
}

func (w *SqsWrapper) DeleteQueue(name string) error {
	url := w.MustGetQueueUrl(name)
	if url != w.QueueUrl {
		return fmt.Errorf("name passed in (%s) is not same with set in init (%s)", name, w.QueueName)
	}

	_, e := w.Client.DeleteQueue(
		w.awsCtx,
		&sqs.DeleteQueueInput{
			QueueUrl: &url,
		},
	)
	return e
}

// GetQueues returns a list of queue names
func (w *SqsWrapper) GetQueues() (*sqs.ListQueuesOutput, error) {
	result, err := w.Client.ListQueues(w.awsCtx, nil)
	return result, err
}

// GetQueueUrl gets the URL of an Amazon SQS queue
// Inputs:
//
//	queueName is the name of the queue
//
// Output:
//
//	If success, the URL of the queue and nil
//	Otherwise, an empty string and an error from the call to
func (w *SqsWrapper) GetQueueUrl(name string) (string, error) {
	res, err := w.Client.GetQueueUrl(w.awsCtx, &sqs.GetQueueUrlInput{QueueName: &name})
	if err == nil {
		return *res.QueueUrl, err
	}

	return "", err
}

func (w *SqsWrapper) MustGetQueueUrl(name string) string {
	r, e := w.GetQueueUrl(name)
	if e != nil {
		panic(e)
	}
	return r
}

func (w *SqsWrapper) SendMsg(message string) (r *sqs.SendMessageOutput, err error) {
	var cancelFn func()
	ctx := context.Background()
	ctx, cancelFn = context.WithTimeout(ctx, time.Duration(w.Timeout)*time.Second)
	if cancelFn != nil {
		defer cancelFn()
	}

	r, err = w.Client.SendMessage(
		ctx,
		&sqs.SendMessageInput{
			MessageBody: aws.String(message),
			QueueUrl:    &w.QueueUrl,
		},
	)
	return r, err
}

func (w *SqsWrapper) MustSendMsg(message string) (r *sqs.SendMessageOutput) {
	if r, err := w.SendMsg(message); err != nil {
		panic(err)
	} else {
		return r
	}
}

func (w *SqsWrapper) MustSendMsgByRetry(message string, retries int) (r *sqs.SendMessageOutput) {
	_, err := xutil.EnsureByRetry(
		func() error {
			var e error
			r, e = w.SendMsg(message)
			return e
		},
		retries,
	)

	if err != nil {
		panic(err)
	}
	return r
}

func (w *SqsWrapper) SendMsgBatch(messages []string) (r *sqs.SendMessageBatchOutput, err error) {
	if len(messages) == 0 {
		return nil, err
	}

	var entries []types.SendMessageBatchRequestEntry
	for i, message := range messages {
		et := types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%d", i+1)),
			MessageBody: aws.String(message),
		}
		entries = append(entries, et)
	}

	r, err = w.Client.SendMessageBatch(
		w.awsCtx,
		&sqs.SendMessageBatchInput{
			Entries:  entries,
			QueueUrl: &w.QueueUrl,
		})

	return r, err
}

func (w *SqsWrapper) chunkSlice(slice []string, chunkSize int) [][]string {
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

func (w *SqsWrapper) GetMsgs(opts ...SqsOptFunc) (r *sqs.ReceiveMessageOutput, err error) {
	opt := SqsOpts{max: 1, receiveTimeSeconds: 2, batch: w.BatchSize}
	bindSqsOpts(&opt, opts...)

	r, err = w.Client.ReceiveMessage(
		w.awsCtx,
		&sqs.ReceiveMessageInput{
			QueueUrl:            &w.QueueUrl,
			MaxNumberOfMessages: int32(opt.batch),
			WaitTimeSeconds:     int32(opt.receiveTimeSeconds),
		})
	return
}

func (w *SqsWrapper) GetMsg() (r *sqs.ReceiveMessageOutput, err error) {
	return w.GetMsgs(WithBatch(1))
}

func (w *SqsWrapper) MustGetMsgs(opts ...SqsOptFunc) []*string {
	r, e := w.GetMsgs(opts...)
	if e != nil {
		panic(e)
	}

	var arr []*string
	for _, msg := range r.Messages {
		arr = append(arr, msg.Body)
	}

	return arr
}

func (w *SqsWrapper) MustGetMsg() *string {
	r, e := w.GetMsg()
	xutil.PanicIfErr(e)
	return r.Messages[0].Body
}

const (
	READ_SQS_ERROR = iota
	READ_SQS_OK
	READ_SQS_ALL
	READ_SQS_MAXIMUM
)

type SqsResp struct {
	Type int
	Msg  *string
}

func NewSqsResp(msg *string, t int) *SqsResp {
	return &SqsResp{Type: t, Msg: msg}
}

func (w *SqsWrapper) GoReadMessages(ch chan *SqsResp, opts ...SqsOptFunc) {
	go w.ReadMessages(ch, opts...)
}

func (w *SqsWrapper) ReadMessages(ch chan *SqsResp, opts ...SqsOptFunc) {
	opt := SqsOpts{max: 0}
	bindSqsOpts(&opt, opts...)

	failed := NewSqsResp(nil, READ_SQS_ERROR)

	got := 0
OUTER:
	for {
		remained, err := w.GetRemainedItems()
		log.Debug().Err(err).Int64("remain", remained).Msg("remained messages")
		if err != nil {
			ch <- failed
		}

		if remained == 0 {
			ch <- NewSqsResp(nil, READ_SQS_ALL)
			break
		}

		msgs, err := w.GetMsgs()
		if err != nil {
			ch <- failed
		}

		for _, msg := range msgs.Messages {
			ch <- NewSqsResp(msg.Body, READ_SQS_OK)
			got += 1
			if opt.max != 0 && got >= opt.max {
				ch <- NewSqsResp(nil, READ_SQS_MAXIMUM)
				break OUTER
			}
		}
	}
}

func (w *SqsWrapper) DeleteMsg(handle *string) (r *sqs.DeleteMessageOutput, err error) {
	r, err = w.Client.DeleteMessage(
		w.awsCtx,
		&sqs.DeleteMessageInput{
			QueueUrl:      &w.QueueUrl,
			ReceiptHandle: handle,
		})
	return
}

func (w *SqsWrapper) MustDeleteMsg(handle *string) (r *sqs.DeleteMessageOutput) {
	r, err := w.DeleteMsg(handle)
	if err != nil {
		panic(err)
	}
	return r
}

func (w *SqsWrapper) CheckRole(least Roles) error {
	if !strings.Contains(string(w.Role), string(least)) {
		return ErrRoleViolation
	}
	return nil
}

func (w *SqsWrapper) GetRemainedItems() (int64, error) {
	attr := types.QueueAttributeNameApproximateNumberOfMessages
	op, err := w.Client.GetQueueAttributes(w.awsCtx,
		&sqs.GetQueueAttributesInput{
			QueueUrl:       &w.QueueUrl,
			AttributeNames: []types.QueueAttributeName{attr}},
	)
	n := cast.ToInt64(op.Attributes[string(attr)])
	return n, err
}
