package gofakes3_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
)

const autoBucket = "autobucket"

func newAutoBucketTestServer(t *testing.T) *testServer {
	t.Helper()
	return newTestServer(t,
		withoutInitialBuckets(),
		withFakerOptions(gofakes3.WithAutoBucket(true)))
}

func TestAutoBucketPutObject(t *testing.T) {
	autoSrv := newAutoBucketTestServer(t)
	defer autoSrv.Close()
	svc := autoSrv.s3Client()

	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(autoBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	if err != nil {
		t.Fatal(err)
	}
	autoSrv.assertObject(autoBucket, "object", nil, "hello")
}

func TestAutoBucketGetObject(t *testing.T) {
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(autoBucket),
		Key:    aws.String("object"),
	})
	if !hasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Fatal(err)
	}
}

func TestAutoBucketDeleteObject(t *testing.T) {
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(autoBucket),
		Key:    aws.String("object"),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketGetBucketLocation(t *testing.T) {
	autoSrv := newAutoBucketTestServer(t)
	defer autoSrv.Close()
	svc := autoSrv.s3Client()

	_, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(autoBucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketDeleteObjectVersion(t *testing.T) {
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket:    aws.String(autoBucket),
		Key:       aws.String("object"),
		VersionId: aws.String("version"),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketDeleteObjectsVersion(t *testing.T) {
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
		Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{
				{Key: aws.String("object1"), VersionId: aws.String("version1")},
				{Key: aws.String("object2"), VersionId: aws.String("version2")},
			},
		},
		Bucket: aws.String(autoBucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketListMultipartUploads(t *testing.T) {
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(autoBucket),
	})
	if !hasErrorCode(err, gofakes3.ErrNoSuchUpload) {
		t.Fatal(err)
	}
}

func TestAutoBucketGetBucketVersioning(t *testing.T) {
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.GetBucketVersioning(&s3.GetBucketVersioningInput{
		Bucket: aws.String(autoBucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}
