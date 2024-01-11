package xaws

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type OpensearchSuite struct {
	suite.Suite
	w *OpensearchWrapper
}

func TestOpensearch(t *testing.T) {
	suite.Run(t, new(OpensearchSuite))
}

func (s *OpensearchSuite) SetupSuite() {
	s.w = NewOpensearchWrapper()
}

func (s *OpensearchSuite) TearDownSuite() {
}
