package xaws

type SqsOpts struct {
	batch int
	max   int

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
