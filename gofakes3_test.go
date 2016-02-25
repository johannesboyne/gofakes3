package gofakes3_test

import (
	"fmt"

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
	resp, err := svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String("BucketName"), // Required
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println(resp)

}
