package gofakes3_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
)

type TT struct {
	*testing.T
}

func (t TT) OK(err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (t TT) OKAll(vs ...interface{}) {
	t.Helper()
	for _, v := range vs {
		if err, ok := v.(error); ok && err != nil {
			t.Fatal(err)
		}
	}
}

func TestCreateBucket(t *testing.T) {
	//@TODO(jb): implement them for sanity reasons

	tt := TT{t}

	backend, err := s3bolt.NewFile("tests3.db")
	tt.OK(err)

	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	config := aws.NewConfig()
	config.WithEndpoint(ts.URL)
	config.WithRegion("mine")

	svc := s3.New(session.New(), config)

	tt.OKAll(svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("BucketName"),
	}))
	tt.OKAll(svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String("BucketName"),
	}))
	tt.OKAll(svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("BucketName"),
		Key:    aws.String("ObjectKey"),
		Body:   bytes.NewReader([]byte("{\"test\": \"foo\"}")),
		Metadata: map[string]*string{
			"Key": aws.String("MetadataValue"),
		},
	}))
	tt.OKAll(svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("BucketName"),
		Key:    aws.String("ObjectKey"),
	}))
}
