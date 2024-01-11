package xaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/rs/zerolog/log"
)

type OpensearchWrapper struct {
	Config aws.Config
	Client *opensearch.Client
}

func NewOpensearchWrapper() *OpensearchWrapper {
	cfg := MustNewDefaultConfig()

	return &OpensearchWrapper{
		Config: cfg,
		Client: opensearch.NewFromConfig(cfg),
	}
}

// DescribeNodes
func (w *OpensearchWrapper) DescribeDomainNodes(name string) []types.DomainNodesStatus {
	output, err := w.Client.DescribeDomainNodes(context.TODO(), &opensearch.DescribeDomainNodesInput{
		DomainName: aws.String(name),
	})
	if err != nil {
		log.Error().Err(err).Msg("cannot describe domain nodes")
	}

	return output.DomainNodesStatusList
}
