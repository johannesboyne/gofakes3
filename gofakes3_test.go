package gofakes3_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

const defaultBucket = "mybucket"

var defaultDate = time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC)

type testServer struct {
	gofakes3.TT
	gofakes3.TimeSourceAdvancer
	*gofakes3.GoFakeS3

	backend gofakes3.Backend
	server  *httptest.Server

	// if this is nil, no buckets are created. by default, a starting bucket is
	// created using the value of the 'defaultBucket' constant.
	initialBuckets []string
}

type testServerOption func(ts *testServer)

func withoutInitialBuckets() testServerOption { return func(ts *testServer) { ts.initialBuckets = nil } }
func withInitialBuckets(buckets ...string) testServerOption {
	return func(ts *testServer) { ts.initialBuckets = buckets }
}

func newTestServer(t *testing.T, opts ...testServerOption) *testServer {
	t.Helper()
	var ts = testServer{
		TT:             gofakes3.TT{t},
		initialBuckets: []string{defaultBucket},
	}
	for _, o := range opts {
		o(&ts)
	}

	if ts.backend == nil {
		if ts.TimeSourceAdvancer == nil {
			ts.TimeSourceAdvancer = gofakes3.FixedTimeSource(defaultDate)
		}
		ts.backend = s3mem.New(s3mem.WithTimeSource(ts.TimeSourceAdvancer))
	}

	ts.GoFakeS3 = gofakes3.New(ts.backend,
		gofakes3.WithTimeSource(ts.TimeSourceAdvancer),
		gofakes3.WithTimeSkewLimit(0),
	)
	ts.server = httptest.NewServer(ts.GoFakeS3.Server())

	for _, bucket := range ts.initialBuckets {
		ts.TT.OK(ts.backend.CreateBucket(bucket))
	}

	return &ts
}

func (ts *testServer) url(url string) string {
	return fmt.Sprintf("%s/%s", ts.server.URL, strings.TrimLeft(url, "/"))
}

func (ts *testServer) createBucket(bucket string) {
	ts.Helper()
	if err := ts.backend.CreateBucket(bucket); err != nil {
		ts.Fatal("create bucket failed", err)
	}
}

func (ts *testServer) putString(bucket, key string, meta map[string]string, in string) {
	ts.Helper()
	ts.OK(ts.backend.PutObject(bucket, key, meta, strings.NewReader(in)))
}

func (ts *testServer) objectExists(bucket, key string) bool {
	ts.Helper()
	obj, err := ts.backend.HeadObject(bucket, key)
	if err != nil {
		if gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) {
			return false
		} else {
			ts.Fatal(err)
		}
	}
	return obj != nil
}

func (ts *testServer) objectAsString(bucket, key string) string {
	ts.Helper()
	obj, err := ts.backend.GetObject(bucket, key)
	ts.OK(err)

	defer obj.Contents.Close()
	data, err := ioutil.ReadAll(obj.Contents)
	ts.OK(err)

	return string(data)
}

func (ts *testServer) s3Client() *s3.S3 {
	config := aws.NewConfig()
	config.WithEndpoint(ts.server.URL)
	config.WithRegion("region")
	config.WithCredentials(credentials.NewStaticCredentials("dummy-access", "dummy-secret", ""))
	config.WithS3ForcePathStyle(true) // Removes need for subdomain
	svc := s3.New(session.New(), config)
	return svc
}

func (ts *testServer) Close() {
	ts.server.Close()
}

func TestCreateBucket(t *testing.T) {
	//@TODO(jb): implement them for sanity reasons

	ts := newTestServer(t)
	defer ts.Close()

	svc := ts.s3Client()

	ts.OKAll(svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("testbucket"),
	}))
	ts.OKAll(svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String("testbucket"),
	}))
	ts.OKAll(svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("testbucket"),
		Key:    aws.String("ObjectKey"),
		Body:   bytes.NewReader([]byte(`{"test": "foo"}`)),
		Metadata: map[string]*string{
			"Key": aws.String("MetadataValue"),
		},
	}))
	ts.OKAll(svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("testbucket"),
		Key:    aws.String("ObjectKey"),
	}))
}

func TestListBuckets(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	svc := ts.s3Client()

	assertBuckets := func(expected ...string) {
		t.Helper()
		rs, err := svc.ListBuckets(&s3.ListBucketsInput{})
		ts.OK(err)

		var found []string
		for _, bucket := range rs.Buckets {
			found = append(found, *bucket.Name)
		}

		sort.Strings(expected)
		sort.Strings(found)
		if !reflect.DeepEqual(found, expected) {
			t.Fatalf("buckets:\nexp: %v\ngot: %v", expected, found)
		}
	}

	assertBucketTime := func(bucket string, created time.Time) {
		t.Helper()
		rs, err := svc.ListBuckets(&s3.ListBucketsInput{})
		ts.OK(err)

		for _, v := range rs.Buckets {
			if *v.Name == bucket {
				if *v.CreationDate != created {
					t.Fatal("time mismatch for bucket", bucket, "expected:", created, "found:", *v.CreationDate)
				}
				return
			}
		}
		t.Fatal("bucket", bucket, "not found")
	}

	assertBuckets()

	ts.createBucket("test")
	assertBuckets("test")
	assertBucketTime("test", defaultDate)

	ts.createBucket("test2")
	assertBuckets("test", "test2")
	assertBucketTime("test2", defaultDate)

	ts.Advance(1 * time.Minute)

	ts.createBucket("test3")
	assertBuckets("test", "test2", "test3")

	assertBucketTime("test", defaultDate)
	assertBucketTime("test2", defaultDate)
	assertBucketTime("test3", defaultDate.Add(1*time.Minute))
}

func TestCreateObject(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	ts.OKAll(svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	}))

	obj := ts.objectAsString(defaultBucket, "object")
	if obj != "hello" {
		t.Fatal("object creation failed")
	}
}

func TestCreateObjectMD5(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	{ // md5 is valid base64 but does not match content:
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket:     aws.String(defaultBucket),
			Key:        aws.String("invalid"),
			Body:       bytes.NewReader([]byte("hello")),
			ContentMD5: aws.String("bnVwCg=="),
		})
		if !s3HasErrorCode(err, gofakes3.ErrBadDigest) {
			t.Fatal("expected BadDigest error, found", err)
		}
	}

	{ // md5 is invalid base64:
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket:     aws.String(defaultBucket),
			Key:        aws.String("invalid"),
			Body:       bytes.NewReader([]byte("hello")),
			ContentMD5: aws.String("!*@&(*$&"),
		})
		if !s3HasErrorCode(err, gofakes3.ErrInvalidDigest) {
			t.Fatal("expected InvalidDigest error, found", err)
		}
	}

	if ts.objectExists(defaultBucket, "invalid") {
		t.Fatal("unexpected object")
	}
}

func s3HasErrorCode(err error, code gofakes3.ErrorCode) bool {
	if err, ok := err.(awserr.Error); ok {
		return code == gofakes3.ErrorCode(err.Code())
	}
	return false
}
