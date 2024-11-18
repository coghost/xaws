package xaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/rs/zerolog/log"
)

// https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-run-lambda-schedule.html
// https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/scheduler/client/create_schedule.html
type SchedulerWrapper struct {
	client *scheduler.Client

	GroupName string
}

func NewSchedulerWrapper(groupName string) (*SchedulerWrapper, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	if groupName == "" {
		groupName = "default"
	}

	return &SchedulerWrapper{
		client: scheduler.NewFromConfig(cfg, func(o *scheduler.Options) {
			o.RetryMaxAttempts = 0
		}),
		GroupName: groupName,
	}, nil
}

// ListSchedulers list.
func (w *SchedulerWrapper) ListSchedulers(name string) (*scheduler.ListSchedulesOutput, error) {
	output, err := w.client.ListSchedules(context.TODO(), &scheduler.ListSchedulesInput{
		NamePrefix: aws.String(name),
		GroupName:  aws.String(w.GroupName),
	})

	return output, err
}

// Upsert create or update a scheduler.
func (w *SchedulerWrapper) Upsert(name string, schedule, targetArn, roleArn, jsonStr string) error {
	output, err := w.ListSchedulers(name)
	if err != nil {
		return err
	}

	msg := "created"

	if len(output.Schedules) == 0 {
		err = w.Create(name, schedule, targetArn, roleArn, jsonStr)
	} else {
		msg = "updated"
		err = w.Update(name, schedule, targetArn, roleArn, jsonStr)
	}

	log.Info().Str("name", name).Str("target", targetArn).Str("schedule", schedule).Msg(msg)

	return err
}

// Create create a scheduler.
func (w *SchedulerWrapper) Create(name, schedule, targetArn, roleArn, jsonStr string) error {
	_, err := w.client.CreateSchedule(context.TODO(), &scheduler.CreateScheduleInput{
		FlexibleTimeWindow: &types.FlexibleTimeWindow{
			Mode: types.FlexibleTimeWindowModeOff,
		},
		Name:               aws.String(name),
		ScheduleExpression: aws.String(schedule),
		State:              types.ScheduleStateEnabled,
		Target:             newTarget(targetArn, roleArn, jsonStr),
		GroupName:          aws.String(w.GroupName),
	})

	return err
}

// Update updates a scheduler.
func (w *SchedulerWrapper) Update(name string, schedule, targetArn, roleArn, jsonStr string) error {
	_, err := w.client.UpdateSchedule(context.TODO(), &scheduler.UpdateScheduleInput{
		FlexibleTimeWindow: &types.FlexibleTimeWindow{
			Mode: types.FlexibleTimeWindowModeOff,
		},
		Name:               aws.String(name),
		ScheduleExpression: aws.String(schedule),
		State:              types.ScheduleStateEnabled,
		Target:             newTarget(targetArn, roleArn, jsonStr),
		GroupName:          aws.String(w.GroupName),
	})

	return err
}

func (w *SchedulerWrapper) Disable(name string, schedule, targetArn, roleArn, jsonStr string) error {
	_, err := w.client.UpdateSchedule(context.TODO(), &scheduler.UpdateScheduleInput{
		FlexibleTimeWindow: &types.FlexibleTimeWindow{
			Mode: types.FlexibleTimeWindowModeOff,
		},
		Name:               aws.String(name),
		ScheduleExpression: aws.String(schedule),
		State:              types.ScheduleStateDisabled,
		Target:             newTarget(targetArn, roleArn, jsonStr),
		GroupName:          aws.String(w.GroupName),
	})

	return err
}

func newTarget(targetArn, roleArn, jsonStr string) *types.Target {
	return &types.Target{
		Arn:     aws.String(targetArn),
		RoleArn: aws.String(roleArn),
		Input:   aws.String(jsonStr),
		RetryPolicy: &types.RetryPolicy{
			MaximumRetryAttempts: aws.Int32(0),
		},
	}
}

// DeleteSchedule delete a schedule.
func (w *SchedulerWrapper) DeleteSchedule(name string) (*scheduler.DeleteScheduleOutput, error) {
	output, err := w.client.DeleteSchedule(context.TODO(), &scheduler.DeleteScheduleInput{
		Name:      aws.String(name),
		GroupName: aws.String(w.GroupName),
	})

	return output, err
}
