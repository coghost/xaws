package xaws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/k0kubun/pp/v3"
	"github.com/rs/zerolog/log"
)

type EventWrapper struct {
	client *eventbridge.Client
}

func NewEventWrapper(cfg aws.Config) (*EventWrapper, error) {
	return &EventWrapper{
		client: eventbridge.NewFromConfig(cfg),
	}, nil
}

func NewEventWrapperWithDefaultConfig() (*EventWrapper, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	return NewEventWrapper(cfg)
}

// ListRules lists all rules available.
func (w *EventWrapper) ListRules() []types.Rule {
	output, err := w.client.ListRules(context.TODO(), &eventbridge.ListRulesInput{})
	if err != nil {
		log.Error().Err(err).Msg("cannot list rules")
	}

	return output.Rules
}

// PutRule put a rule.
func (w *EventWrapper) PutRule(name string, schedule string) error {
	// roleArn := "arn:aws:iam::722688396086:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA_invoke"
	_, err := w.client.PutRule(context.TODO(), &eventbridge.PutRuleInput{
		Name:               aws.String(name),
		Description:        aws.String(fmt.Sprintf("trigger %s", name)),
		ScheduleExpression: aws.String(schedule),
		State:              types.RuleStateEnabled,
		// RoleArn:            aws.String(roleArn),
	})

	return err
}

// DeleteRule delete a rule with name.
func (w *EventWrapper) DeleteRule(name string) error {
	_, err := w.client.DeleteRule(context.TODO(), &eventbridge.DeleteRuleInput{
		Name: aws.String(name),
	})

	return err
}

// ListTargets list targets of a rule.
func (w *EventWrapper) ListTargets(name string) {
	output, err := w.client.ListTargetsByRule(context.TODO(), &eventbridge.ListTargetsByRuleInput{
		Rule: aws.String(name),
	})
	if err != nil {
		log.Error().Err(err).Msg("cannot get targets of rule")
	}

	pp.Println(output.Targets)
}

// PutTarget put target to a rule.
func (w *EventWrapper) PutTarget(name string, targetArn, targetId, jsonStr string) {
	output, err := w.client.PutTargets(context.TODO(), &eventbridge.PutTargetsInput{
		Rule: aws.String(name),
		Targets: []types.Target{
			{
				Arn: aws.String(targetArn),
				Id:  aws.String(targetId),
				// RoleArn: aws.String(roleArn),
				Input: aws.String(jsonStr),
			},
		},
	})
	if err != nil {
		log.Error().Err(err).Msg("cannot put targets")
	}

	pp.Println(output)
}
