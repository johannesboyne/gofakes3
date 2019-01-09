package gofakes3_test

import (
	"bytes"
	"encoding/xml"
	"io"
	"mime/multipart"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
)

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

func TestDeleteMulti(t *testing.T) {
	deletedKeys := func(rs *s3.DeleteObjectsOutput) []string {
		deleted := make([]string, len(rs.Deleted))
		for idx, del := range rs.Deleted {
			deleted[idx] = *del.Key
		}
		sort.Strings(deleted)
		return deleted
	}

	assertDeletedKeys := func(t *testing.T, rs *s3.DeleteObjectsOutput, expected ...string) {
		t.Helper()
		found := deletedKeys(rs)
		if !reflect.DeepEqual(found, expected) {
			t.Fatal("multi deletion failed", found, "!=", expected)
		}
	}

	t.Run("one-file", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		svc := ts.s3Client()

		ts.putString(defaultBucket, "foo", nil, "one")
		ts.putString(defaultBucket, "bar", nil, "two")
		ts.putString(defaultBucket, "baz", nil, "three")

		rs, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(defaultBucket),
			Delete: &s3.Delete{
				Objects: []*s3.ObjectIdentifier{
					{Key: aws.String("foo")},
				},
			},
		})
		ts.OK(err)
		assertDeletedKeys(t, rs, "foo")
		ts.assertLs(defaultBucket, "", nil, []string{"bar", "baz"})
	})

	t.Run("multiple-files", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		svc := ts.s3Client()

		ts.putString(defaultBucket, "foo", nil, "one")
		ts.putString(defaultBucket, "bar", nil, "two")
		ts.putString(defaultBucket, "baz", nil, "three")

		rs, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(defaultBucket),
			Delete: &s3.Delete{
				Objects: []*s3.ObjectIdentifier{
					{Key: aws.String("bar")},
					{Key: aws.String("foo")},
				},
			},
		})
		ts.OK(err)
		assertDeletedKeys(t, rs, "bar", "foo")
		ts.assertLs(defaultBucket, "", nil, []string{"baz"})
	})
}

func TestCreateObjectBrowserUpload(t *testing.T) {
	addFile := func(tt gofakes3.TT, w *multipart.Writer, object string, b []byte) {
		tt.Helper()
		tt.OK(w.WriteField("key", object))

		mw, err := w.CreateFormFile("file", "upload")
		tt.OK(err)
		n, err := mw.Write(b)
		if n != len(b) {
			tt.Fatal("len mismatch", n, "!=", len(b))
		}
		tt.OK(err)
	}

	upload := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader) (*http.Response, error) {
		w.Close()
		req, err := http.NewRequest("POST", ts.url("/"+bucket), body)
		ts.OK(err)
		req.Header.Set("Content-Type", w.FormDataContentType())
		return httpClient().Do(req)
	}

	assertUpload := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader) {
		res, err := upload(ts, bucket, w, body)
		ts.OK(err)
		if res.StatusCode != http.StatusOK {
			ts.Fatal("bad status", res.StatusCode)
		}
	}

	assertUploadFails := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader, expectedCode gofakes3.ErrorCode) {
		res, err := upload(ts, bucket, w, body)
		ts.OK(err)
		if res.StatusCode != expectedCode.Status() {
			ts.Fatal("bad status", res.StatusCode, "!=", expectedCode.Status())
		}
		defer res.Body.Close()
		var errResp gofakes3.ErrorResponse
		dec := xml.NewDecoder(res.Body)
		ts.OK(dec.Decode(&errResp))

		if errResp.Code != expectedCode {
			ts.Fatal("bad code", errResp.Code, "!=", expectedCode)
		}
	}

	t.Run("single-upload", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		addFile(ts.TT, w, "yep", []byte("stuff"))
		assertUpload(ts, defaultBucket, w, &b)
		ts.assertObject(defaultBucket, "yep", nil, "stuff")
	})

	t.Run("multiple-files-fails", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		addFile(ts.TT, w, "yep", []byte("stuff"))
		addFile(ts.TT, w, "nup", []byte("bork"))
		assertUploadFails(ts, defaultBucket, w, &b, gofakes3.ErrIncorrectNumberOfFilesInPostRequest)
	})

	t.Run("key-too-large", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		addFile(ts.TT, w, strings.Repeat("a", gofakes3.KeySizeLimit+1), []byte("yep"))
		assertUploadFails(ts, defaultBucket, w, &b, gofakes3.ErrKeyTooLong)
	})
}

func s3HasErrorCode(err error, code gofakes3.ErrorCode) bool {
	if err, ok := err.(awserr.Error); ok {
		return code == gofakes3.ErrorCode(err.Code())
	}
	return false
}
