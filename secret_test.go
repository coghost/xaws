package xaws

import (
	"encoding/json"
	"testing"

	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
)

type SecretSuite struct {
	suite.Suite
}

func TestSecret(t *testing.T) {
	suite.Run(t, new(SecretSuite))
}

func (s *SecretSuite) SetupSuite() {
}

func (s *SecretSuite) TearDownSuite() {
}

func (s *SecretSuite) Test_01() {
	str := MustGetSecretWithDefault("test-key")
	pp.Println(str)

	auth := &Auth{}
	err := json.Unmarshal([]byte(str), &auth)
	s.Nil(err)

	pp.Println(auth)
}
