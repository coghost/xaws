package xaws

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
	"github.com/ungerik/go-dry"
)

type SqsSuite struct {
	suite.Suite
	w    *SqsWrapper
	name string
}

func TestSqs(t *testing.T) {
	suite.Run(t, new(SqsSuite))
}

func (s *SqsSuite) SetupSuite() {
	s.name = "sqs-crud-test"
	lines, e := dry.FileGetLines("/tmp/aws.sqs.cfg")
	if e != nil {
		panic(e)
	}
	ak, sk, region, name := lines[0], lines[1], lines[2], s.name

	cfg, _ := NewAwsConfig(ak, sk, region)
	s.w = NewSqsWrapper(name, cfg, 10, 60)
}

func (s *SqsSuite) TearDownSuite() {
}

func (s *SqsSuite) Test_00_create() {
	v, e := s.w.CreateQueue(s.w.QueueName)
	s.Nil(e)
	pp.Println(v)
	s.True(strings.HasSuffix(v, s.w.QueueName))
	s.w.SetQueueUrl(s.w.QueueName)
}

func (s *SqsSuite) Test_01_GetQueues() {
	res, e := s.w.GetQueues()
	s.Nil(e)
	s.Greater(len(res.QueueUrls), 0)
}

func (s *SqsSuite) Test_02_GetQueueUrl() {
	r, e := s.w.GetQueueUrl(s.w.QueueName)
	s.Nil(e)
	s.Contains(r, s.w.QueueName)
}

func (s *SqsSuite) Test_0300_SendMsg() {
	msg := "I am a test message!"
	r, e := s.w.SendMsg(msg)
	s.Nil(e)
	s.NotNil(r.MessageId)
}

func (s *SqsSuite) Test_0301_SendMsgBatch() {
	messages := []string{"first", "second", "third"}
	for i := 0; i < 20; i++ {
		messages = append(messages, fmt.Sprintf("auto %d", i))
	}
	ss := s.w.chunkSlice(messages, 10)
	for _, sl := range ss {
		r, e := s.w.SendMsgBatch(sl)
		s.Nil(e)
		s.Equal(len(sl), len(r.Successful))
	}
}

func (s *SqsSuite) Test_04_GetMsg() {
	r, e := s.w.GetMsg()
	s.Nil(e)
	s.NotNil(*r.Messages[0].Body)
	v := s.w.MustGetMsg()
	pp.Println(v)
}

func (s *SqsSuite) Test_0402_ReadMsgs() {
	ch := make(chan *SqsResp, 1)
	defer close(ch)
	go s.w.ReadMessages(ch, WithMax(17))

LOOP:
	for {
		select {
		case cap := <-ch:
			if cap.Type == READ_SQS_ERROR {
				pp.Println("error happened")
				pp.Println(cap)
				break LOOP
			} else if cap.Type == READ_SQS_MAXIMUM {
				pp.Println("required messages matched")
				pp.Println(cap)
				break LOOP
			} else if cap.Type == READ_SQS_ALL {
				pp.Println("all messages are consumed")
				pp.Println(cap)
				break LOOP
			} else {
				pp.Println("bingo")
				pp.Println(cap)
			}
		case <-time.After(time.Duration(10) * time.Second):
			panic(fmt.Sprintf("timeout of (%d)s getting new captured page", 10))
		}
	}
}

func (s *SqsSuite) Test_06_remained() {
	r, e := s.w.GetRemainedItems()
	s.Nil(e)
	// 23 is calculated from 0401
	s.Equal(int64(24), r)
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
