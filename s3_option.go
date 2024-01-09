package xaws

type S3Options struct {
	saveTo  string
	timeout int

	folderLevel int
	savedName   string

	bucket string
	withGz bool

	withEmptyFile bool
	maxKeys       int
}

type S3OptionFunc func(o *S3Options)

func bindS3Options(opt *S3Options, opts ...S3OptionFunc) {
	for _, f := range opts {
		f(opt)
	}
}

func WithGz(b bool) S3OptionFunc {
	return func(o *S3Options) {
		o.withGz = b
	}
}

func WithEmptyFile(b bool) S3OptionFunc {
	return func(o *S3Options) {
		o.withEmptyFile = b
	}
}

func WithMaxKeys(n int) S3OptionFunc {
	return func(o *S3Options) {
		o.maxKeys = n
	}
}

func WithTimeout(n int) S3OptionFunc {
	return func(o *S3Options) {
		o.timeout = n
	}
}

func WithSaveTo(s string) S3OptionFunc {
	return func(o *S3Options) {
		o.saveTo = s
	}
}

func WithBucket(s string) S3OptionFunc {
	return func(o *S3Options) {
		o.bucket = s
	}
}

// WithFolderLevel is how many level of folder kept from s3uri
func WithFolderLevel(n int) S3OptionFunc {
	return func(o *S3Options) {
		o.folderLevel = n
	}
}

func WithSavedName(s string) S3OptionFunc {
	return func(o *S3Options) {
		o.savedName = s
	}
}
