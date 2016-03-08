package gofakes3_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
)

func TestCreateBucket(t *testing.T) {
	//@TODO(jb): implement them for sanity reasons

	faker := gofakes3.New("tests3.db")
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	config := aws.NewConfig()
	config.WithEndpoint(ts.URL)
	config.WithRegion("mine")

	svc := s3.New(session.New(), config)

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("BucketName"),
	})
	_, err = svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String("BucketName"),
	})
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("BucketName"),
		Key:    aws.String("ObjectKey"),
		Body:   bytes.NewReader([]byte("{\"test\": \"foo\"}")),
		Metadata: map[string]*string{
			"Key": aws.String("MetadataValue"),
		},
	})
	_, err = svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("BucketName"),
		Key:    aws.String("ObjectKey"),
	})
	if err != nil {
		t.Errorf("ERROR:\n%+v\n", err)
		return
	}

}
