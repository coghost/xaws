package xaws

import (
	"testing"

	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
)

type SchedulerSuite struct {
	suite.Suite
	w *SchedulerWrapper
}

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

func (s *SchedulerSuite) SetupSuite() {
	s.w, _ = NewSchedulerWrapper("")
}

func (s *SchedulerSuite) TearDownSuite() {
}

func (s *SchedulerSuite) Test_00_list() {
	op, err := s.w.ListSchedulers("non-existed-scheduler")
	s.Nil(err)

	s.Empty(op.Schedules)
}

func (s *SchedulerSuite) Test_01_upsert() {
	target := "arn:aws:lambda:us-west-2:722688396086:function:CC_Redis_handler"
	rolaArn := "arn:aws:iam::722688396086:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA_CC_Redis"
	rawJson := `{
  "async_mode": false,
  "dry_run": false,
  "site": 368,
  "task": "update_keywords"
}`
	// every 15 minutes
	schedule := "rate(1 minutes)"
	// 02:10 UTC
	schedule = "cron(0 2 * * ? *)"

	err := s.w.Upsert("go-trigger-kr", schedule, target, rolaArn, rawJson)
	if err != nil {
		pp.Println(err.Error())
	}

	s.Nil(err)
}
