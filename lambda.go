package xaws

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/coghost/xpretty"
	"github.com/rs/zerolog/log"
)

type FunctionWrapper struct {
	client *lambda.Client

	funcName  string
	dryRun    bool
	asyncMode bool
}

func NewFunctionWrapper(funcName string, dryRun bool, cfg aws.Config) (*FunctionWrapper, error) {
	c := lambda.NewFromConfig(cfg, func(o *lambda.Options) {
		o.RetryMaxAttempts = 0
	})

	return &FunctionWrapper{
		client:   c,
		funcName: funcName,
		dryRun:   dryRun,
	}, nil
}

func NewFunctionWrapperWithDefaultConfig(funcName string, dryRun bool) (*FunctionWrapper, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), func(lo *config.LoadOptions) error {
		lo.RetryMaxAttempts = 0
		return nil
	})
	if err != nil {
		return nil, err
	}

	return NewFunctionWrapper(funcName, dryRun, cfg)
}

// GetConfig gets data about function.
func (w *FunctionWrapper) GetConfig() (*types.FunctionConfiguration, error) {
	op, err := w.client.GetFunction(context.TODO(), &lambda.GetFunctionInput{
		FunctionName: aws.String(w.funcName),
	})
	if err != nil {
		return nil, err
	}

	return op.Configuration, nil
}

// List lists up to maxItems for account.
func (w *FunctionWrapper) List(maxItems int) ([]types.FunctionConfiguration, error) {
	var fns []types.FunctionConfiguration

	paginator := lambda.NewListFunctionsPaginator(w.client, &lambda.ListFunctionsInput{
		MaxItems: aws.Int32(int32(maxItems)),
	})

	for paginator.HasMorePages() && len(fns) < maxItems {
		po, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		fns = append(fns, po.Functions...)
	}

	return fns, nil
}

// InvokeSync invokes the lambda function specified by name.
func (w *FunctionWrapper) InvokeSync(payload []byte, getLog bool) (*lambda.InvokeOutput, error) {
	return w.Invoke(payload, getLog, true)
}

// InvokeAsync invokes the function asynchronously.
func (w *FunctionWrapper) InvokeAsync(payload []byte, getLog bool) (*lambda.InvokeOutput, error) {
	return w.Invoke(payload, false, false)
}

func (w *FunctionWrapper) Invoke(payload []byte, getLog bool, asyncMode bool) (*lambda.InvokeOutput, error) {
	logType := types.LogTypeNone
	if getLog {
		logType = types.LogTypeTail
	}

	w.asyncMode = asyncMode

	invocType := types.InvocationTypeRequestResponse
	if asyncMode {
		invocType = types.InvocationTypeEvent
	}

	if w.dryRun {
		w.doDryRun("invoke", string(payload))
		return nil, nil
	}

	output, err := w.client.Invoke(context.TODO(), &lambda.InvokeInput{
		FunctionName: aws.String(w.funcName),
		LogType:      logType,
		Payload:      payload,

		InvocationType: invocType,
	})

	return output, err
}

func (w *FunctionWrapper) PrintInvokeOutput(output *lambda.InvokeOutput) {
	if output == nil || w.asyncMode {
		return
	}

	if output.LogResult == nil {
		log.Warn().Msg("log result is nil")
		return
	}

	raw, err := base64.StdEncoding.DecodeString(*output.LogResult)
	if err != nil {
		log.Error().Err(err).Msg("cannot decode log result.")
	}

	xpretty.CyanPrintf("%[1]s >Lambda Output< %[1]s\n", strings.Repeat("=", 32))
	xpretty.CyanPrintf("%s\n", raw)

	if len(output.Payload) > 0 {
		xpretty.CyanPrintf("%[1]s >Payload Output< %[1]s\n", strings.Repeat("-", 32))
		xpretty.CyanPrintf("%s\n", output.Payload)
	}

	xpretty.CyanPrintf("%[1]s >Lambda Output< %[1]s\n", strings.Repeat("=", 32))
}

// doDryRun only prints the func name and its command.
func (w *FunctionWrapper) doDryRun(name, cmd string) {
	xpretty.CyanPrintf("%[1]s >Dry Run< %[1]s\n", strings.Repeat("=", 32))
	xpretty.YellowPrintf("[%s]: %s\n", name, cmd)
	xpretty.CyanPrintf("%[1]s >Dry Run< %[1]s\n", strings.Repeat("=", 32))
}

const (
	lambdaTimeout = 900
)

func (w *FunctionWrapper) Create(functionName string, handlerName string, iamRoleArn *string, data []byte) types.State {
	var state types.State

	var mem int32 = 512

	_, err := w.client.CreateFunction(context.TODO(), &lambda.CreateFunctionInput{
		Code:          &types.FunctionCode{ZipFile: data},
		FunctionName:  aws.String(functionName),
		Role:          iamRoleArn,
		Handler:       aws.String(handlerName),
		Architectures: []types.Architecture{types.ArchitectureArm64},
		Publish:       true,
		Runtime:       types.RuntimeProvidedal2,
		Timeout:       aws.Int32(lambdaTimeout),
		MemorySize:    aws.Int32(mem),
	})
	if err != nil {
		var resConflict *types.ResourceConflictException
		if errors.As(err, &resConflict) {
			log.Printf("Function %v already exists.\n", functionName)

			state = types.StateActive
		} else {
			log.Fatal().Msgf("Couldn't create function %v. Here's why: %v\n", functionName, err)
		}
	} else {
		waiter := lambda.NewFunctionActiveV2Waiter(w.client)
		funcOutput, err := waiter.WaitForOutput(context.TODO(), &lambda.GetFunctionInput{
			FunctionName: aws.String(functionName),
		}, 1*time.Minute)

		if err != nil {
			log.Fatal().Msgf("Couldn't wait for function %v to be active. Here's why: %v\n", functionName, err)
		} else {
			state = funcOutput.Configuration.State
		}
	}

	return state
}
