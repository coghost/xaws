package xaws

import (
	"fmt"
	"io/fs"
	"testing"

	"github.com/coghost/xdtm"
	"github.com/gookit/goutil/strutil"
	"github.com/stretchr/testify/suite"
	"github.com/ungerik/go-dry"
)

// const bucketName = "a-unique-bucket-name"
const bucketName = "nebu-programmatic-data"

type S3Suite struct {
	suite.Suite
	w *S3Wrapper
}

func TestS3(t *testing.T) {
	suite.Run(t, new(S3Suite))
}

func (s *S3Suite) SetupSuite() {
	var err error
	s.w, err = NewS3WrapperWithDefaultConfig(bucketName)
	s.Nil(err)
}

func (s *S3Suite) TearDownSuite() {
}

func (s *S3Suite) Test01ListBuckets() {
	_, err := s.w.ListBuckets()
	// limited has no permission to list buckets
	s.NotNil(err)
}

func (s *S3Suite) Test02Upload() {
	type args struct {
		name string
		ext  string
		add  bool
	}

	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "upload existed json",
			args: args{
				name: "/tmp/sample.json",
				ext:  "json",
				add:  true,
			},
			wantErr: nil,
		},
		{
			name: "upload existed html",
			args: args{
				name: "/tmp/sample.html",
				ext:  "html",
				add:  true,
			},
			wantErr: nil,
		},
		{
			name: "upload not existed file",
			args: args{
				name: "/tmp/notexisted",
				ext:  "html",
				add:  false,
			},
			wantErr: &fs.PathError{},
		},
	}

	for _, tt := range tests[:1] {
		fpth := tt.args.name
		if tt.args.add {
			now := xdtm.StrNow()
			fpth = fmt.Sprintf("%s_%s.%s", tt.args.name, now, tt.args.ext)
			dry.FileSetString(fpth, "this is a demo text")
		}
		rs := strutil.RandomChars(8)
		name := fmt.Sprintf("6/%s.%s", rs, tt.args.ext)
		r, e := s.w.Upload(fpth, name)

		if tt.wantErr == nil {
			s.Equal(tt.wantErr, e, tt.name)
			s.Contains(r.Location, name, tt.name)
		} else {
			s.IsType(tt.wantErr, e, tt.name)
		}

		if tt.wantErr != nil {
			s.Panics(func() {
				s.w.MustUpload(fpth, name)
			})
		}
	}
}

func (s *S3Suite) Test_0301_uploadRaw() {
	data := `
	### what to do
	- test 0301
	- test 0302
	`
	err := s.w.UploadRawDataToGz(data, "plain/p2g.gz")
	s.Nil(err)
}

func (s *S3Suite) Test_0302_download() {
	// err := s.w.DownloadTo("plain/abc.txt.gz", "/tmp/a/b/c/d")
	// s.Nil(err)
	// pp.Println(randSeq(10))
}
