package xaws

type SqsOpts struct {
	batchSize int
	max       int

	queueName string

	waitTimeSeconds int
}

type SqsOptFunc func(o *SqsOpts)

func bindSqsOpts(opt *SqsOpts, opts ...SqsOptFunc) {
	for _, f := range opts {
		f(opt)
	}
}

func WaitTimeSeconds(i int) SqsOptFunc {
	return func(o *SqsOpts) {
		o.waitTimeSeconds = i
	}
}

func WithMax(i int) SqsOptFunc {
	return func(o *SqsOpts) {
		o.max = i
	}
}

func BatchSize(i int) SqsOptFunc {
	return func(o *SqsOpts) {
		o.batchSize = i
	}
}

func WithQueueName(s string) SqsOptFunc {
	return func(o *SqsOpts) {
		o.queueName = s
	}
}
