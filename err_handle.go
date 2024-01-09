package xaws

import (
	"errors"
	"fmt"
)

var (
	ErrRoleViolation = errors.New("role violation: current role is not allowed to perform this operation")
	ErrMessgeEmpty   = errors.New("message is empty")

	ErrQueueNameNotMatch = errors.New("queue name not match")
)

func QueueNameNotMatchError(msg string) error {
	return fmt.Errorf("QueueNameNotMatch %w : %s", ErrQueueNameNotMatch, msg)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
