package xaws

import (
	"strings"
	"testing"

	"github.com/coghost/xdtm"
	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
)

type EventSuite struct {
	suite.Suite
	w *EventWrapper
}

func TestEvent(t *testing.T) {
	suite.Run(t, new(EventSuite))
}

func (s *EventSuite) SetupSuite() {
	s.w, _ = NewEventWrapperWithDefaultConfig()
}

func (s *EventSuite) TearDownSuite() {
}

const ruleName = "kr-trigger"

func (s *EventSuite) Test_00_list() {
	// s.w.ListRules()
	err1 := s.w.PutRule(ruleName, "rate(5 minutes)")
	s.Nil(err1)
}

func (s *EventSuite) Test_01_target() {
	// name := "kr-trigger"
	// roleArn := "arn:aws:iam::722688396086:role/invoke_lambda"
	ta := "arn:aws:lambda:us-west-2:722688396086:function:CC_Redis_handler"
	tid := "kr-trigger" + strings.Split(xdtm.StrNow(), " ")[0]
	rawJson := `{
  "async_mode": false,
  "site": 368,
  "task": "update_keywords"
}`
	s.w.PutTarget(ruleName, ta, tid, rawJson)
}

func (s *EventSuite) Test_02_delete() {
	// s.w.ListRules()
	// name := "gen-kr-keywords"
	err := s.w.DeleteRule(ruleName)
	pp.Println(err.Error())

	s.Nil(err)
}
