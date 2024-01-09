package xaws

type SqsOpts struct {
	batch int
	max   int

	queueName string

	receiveTimeSeconds int
}

type SqsOptFunc func(o *SqsOpts)

func bindSqsOpts(opt *SqsOpts, opts ...SqsOptFunc) {
	for _, f := range opts {
		f(opt)
	}
}

func WithReceiveTimeSeconds(i int) SqsOptFunc {
	return func(o *SqsOpts) {
		o.receiveTimeSeconds = i
	}
}

func WithMax(i int) SqsOptFunc {
	return func(o *SqsOpts) {
		o.max = i
	}
}

func WithBatch(i int) SqsOptFunc {
	return func(o *SqsOpts) {
		o.batch = i
	}
}

func WithQueueName(s string) SqsOptFunc {
	return func(o *SqsOpts) {
		o.queueName = s
	}
}
