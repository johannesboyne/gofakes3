package gofakes3_test

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
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

	ts.backendCreateBucket("test")
	assertBuckets("test")
	assertBucketTime("test", defaultDate)

	ts.backendCreateBucket("test2")
	assertBuckets("test", "test2")
	assertBucketTime("test2", defaultDate)

	ts.Advance(1 * time.Minute)

	ts.backendCreateBucket("test3")
	assertBuckets("test", "test2", "test3")

	assertBucketTime("test", defaultDate)
	assertBucketTime("test2", defaultDate)
	assertBucketTime("test3", defaultDate.Add(1*time.Minute))
}

func TestCreateObject(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	out, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	ts.OK(err)

	if *out.ETag != `"5d41402abc4b2a76b9719d911017c592"` { // md5("hello")
		ts.Fatal("bad etag", out.ETag)
	}

	obj := ts.backendGetString(defaultBucket, "object", nil)
	if obj != "hello" {
		t.Fatal("object creation failed")
	}
}

func TestCreateObjectMetadataSizeLimit(t *testing.T) {
	ts := newTestServer(t, withFakerOptions(
		gofakes3.WithMetadataSizeLimit(1),
	))
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:   aws.String(defaultBucket),
		Key:      aws.String("object"),
		Metadata: map[string]*string{"too": aws.String("big")},
		Body:     bytes.NewReader([]byte("hello")),
	})
	if !hasErrorCode(err, gofakes3.ErrMetadataTooLarge) {
		t.Fatal(err)
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
		if !s3HasErrorCode(err, gofakes3.ErrInvalidDigest) {
			t.Fatal("expected InvalidDigest error, found", err)
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

	if ts.backendObjectExists(defaultBucket, "invalid") {
		t.Fatal("unexpected object")
	}
}

func TestCreateObjectWithMissingContentLength(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	client := ts.rawClient()
	body := []byte{}
	rq, err := http.NewRequest("PUT", client.URL(fmt.Sprintf("/%s/yep", defaultBucket)).String(), maskReader(bytes.NewReader(body)))
	if err != nil {
		panic(err)
	}
	client.SetHeaders(rq, body)
	rs, _ := client.Do(rq)
	if rs.StatusCode != http.StatusLengthRequired {
		t.Fatal()
	}
}

func TestCreateObjectWithInvalidContentLength(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	client := ts.rawClient()

	body := []byte{1, 2, 3}
	rq, err := http.NewRequest("PUT", client.URL(fmt.Sprintf("/%s/yep", defaultBucket)).String(), maskReader(bytes.NewReader(body)))
	if err != nil {
		panic(err)
	}

	client.SetHeaders(rq, body)
	rq.Header.Set("Content-Length", "quack")
	raw, err := client.SendRaw(rq)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), rq)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Body.Close()

	if rs.StatusCode != http.StatusBadRequest {
		t.Fatal(rs.StatusCode, "!=", http.StatusBadRequest)
	}
}

func TestCopyObject(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	srcMeta := map[string]string{
		"Content-Type":   "text/plain",
		"X-Amz-Meta-One": "src",
		"X-Amz-Meta-Two": "src",
	}
	ts.backendPutString(defaultBucket, "src-key", srcMeta, "content")

	out, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("dst-key"),
		CopySource: aws.String("/" + defaultBucket + "/src-key"),
		Metadata: map[string]*string{
			"Two":   aws.String("dst"),
			"Three": aws.String("dst"),
		},
	})
	ts.OK(err)

	if *out.CopyObjectResult.ETag != `"9a0364b9e99bb480dd25e1f0284c8555"` { // md5("content")
		ts.Fatal("bad etag", *out.CopyObjectResult.ETag)
	}

	obj, err := ts.backend.GetObject(defaultBucket, "dst-key", nil)
	ts.OK(err)

	defer obj.Contents.Close()
	data, err := ioutil.ReadAll(obj.Contents)
	ts.OK(err)

	if string(data) != "content" {
		t.Fatal("object copying failed")
	}

	if v := obj.Metadata["X-Amz-Meta-One"]; v != "src" {
		t.Fatalf("bad Content-Type: %q", v)
	}

	if v := obj.Metadata["X-Amz-Meta-Two"]; v != "dst" {
		t.Fatalf("bad Content-Encoding: %q", v)
	}

	if v := obj.Metadata["X-Amz-Meta-Three"]; v != "dst" {
		t.Fatalf("bad Content-Encoding: %q", v)
	}
}

func TestDeleteBucket(t *testing.T) {
	t.Run("delete-empty", func(t *testing.T) {
		ts := newTestServer(t, withoutInitialBuckets())
		defer ts.Close()
		svc := ts.s3Client()

		ts.backendCreateBucket("test")
		ts.OKAll(svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String("test"),
		}))
	})

	t.Run("delete-fails-if-not-empty", func(t *testing.T) {
		ts := newTestServer(t, withoutInitialBuckets())
		defer ts.Close()
		svc := ts.s3Client()

		ts.backendCreateBucket("test")
		ts.backendPutString("test", "test", nil, "test")
		_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String("test"),
		})
		if !hasErrorCode(err, gofakes3.ErrBucketNotEmpty) {
			t.Fatal("expected ErrBucketNotEmpty, found", err)
		}
	})
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

		ts.backendPutString(defaultBucket, "foo", nil, "one")
		ts.backendPutString(defaultBucket, "bar", nil, "two")
		ts.backendPutString(defaultBucket, "baz", nil, "three")

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

		ts.backendPutString(defaultBucket, "foo", nil, "one")
		ts.backendPutString(defaultBucket, "bar", nil, "two")
		ts.backendPutString(defaultBucket, "baz", nil, "three")

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

func TestGetBucketLocation(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	ts.backendPutString(defaultBucket, "foo", nil, "one")

	out, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(defaultBucket),
	})
	if err != nil {
		t.Fatal("get-bucket-location-failed", err)
	}

	if out.LocationConstraint != nil {
		t.Fatal("location-constraint-not-empty", *out.LocationConstraint)
	}
}

func TestGetObjectRange(t *testing.T) {
	assertRange := func(ts *testServer, key string, hdr string, expected []byte, fail bool) {
		ts.Helper()
		svc := ts.s3Client()
		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String(key),
			Range:  aws.String(hdr),
		})
		if fail != (err != nil) {
			ts.Fatal("failure expected:", fail, "found:", err)
		}
		if !fail {
			ts.OK(err)
			defer obj.Body.Close()

			out, err := ioutil.ReadAll(obj.Body)
			ts.OK(err)
			if !bytes.Equal(expected, out) {
				ts.Fatal("range failed", hdr, err)
			}
		}
	}

	in := randomFileBody(1024)

	for idx, tc := range []struct {
		hdr      string
		expected []byte
		fail     bool
	}{
		{"bytes=0-", in, false},
		{"bytes=1-", in[1:], false},
		{"bytes=0-0", in[:1], false},
		{"bytes=0-1", in[:2], false},
		{"bytes=1023-1023", in[1023:1024], false},

		// if the requested end is beyond the real end, returns "remainder of the representation"
		{"bytes=1023-1025", in[1023:1024], false},

		// if the requested start is beyond the real end, it should fail
		{"bytes=1024-1024", []byte{}, true},

		// suffix-byte-range-spec:
		{"bytes=-0", []byte{}, true},
		{"bytes=-1", in[1023:1024], false},
		{"bytes=-1024", in, false},
		{"bytes=-1025", in, true},
	} {
		t.Run(fmt.Sprintf("%d/%s", idx, tc.hdr), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			ts.backendPutBytes(defaultBucket, "foo", nil, in)
			assertRange(ts, "foo", tc.hdr, tc.expected, tc.fail)
		})
	}
}

func TestGetObjectRangeInvalid(t *testing.T) {
	assertRangeInvalid := func(ts *testServer, key string, hdr string) {
		svc := ts.s3Client()
		_, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String(key),
			Range:  aws.String(hdr),
		})
		if !hasErrorCode(err, gofakes3.ErrInvalidRange) {
			ts.Fatal("expected ErrInvalidRange, found", err)
		}
	}

	in := randomFileBody(1024)

	for idx, tc := range []struct {
		hdr string
	}{
		{"boats=0-0"},
		{"bytes="},
	} {
		t.Run(fmt.Sprintf("%d/%s", idx, tc.hdr), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			ts.backendPutBytes(defaultBucket, "foo", nil, in)
			assertRangeInvalid(ts, "foo", tc.hdr)
		})
	}
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

	assertUpload := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader, etag string) {
		res, err := upload(ts, bucket, w, body)
		ts.OK(err)
		if res.StatusCode != http.StatusOK {
			ts.Fatal("bad status", res.StatusCode, tryDumpResponse(res, true))
		}
		if etag != "" && res.Header.Get("ETag") != etag {
			ts.Fatal("bad etag", res.Header.Get("ETag"), etag)
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
		assertUpload(ts, defaultBucket, w, &b, `"c13d88cb4cb02003daedb8a84e5d272a"`)
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

func TestVersioning(t *testing.T) {
	assertVersioning := func(ts *testServer, mfa string, status string) {
		ts.Helper()
		svc := ts.s3Client()
		bv, err := svc.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: aws.String(defaultBucket)})
		ts.OK(err)
		if aws.StringValue(bv.MFADelete) != mfa {
			ts.Fatal("unexpected MFADelete")
		}
		if aws.StringValue(bv.Status) != status {
			ts.Fatalf("unexpected Status %q, expected %q", aws.StringValue(bv.Status), status)
		}
	}

	setVersioning := func(ts *testServer, status gofakes3.VersioningStatus) {
		ts.Helper()
		svc := ts.s3Client()
		ts.OKAll(svc.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: aws.String(defaultBucket),
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String(string(status)),
			},
		}))
	}

	t.Run("", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()

		// Bucket that has never been versioned should return empty strings:
		assertVersioning(ts, "", "")
	})

	t.Run("enable", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()

		setVersioning(ts, "Enabled")
		assertVersioning(ts, "", "Enabled")
	})

	t.Run("suspend", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()

		setVersioning(ts, gofakes3.VersioningSuspended)
		assertVersioning(ts, "", "")

		setVersioning(ts, gofakes3.VersioningEnabled)
		setVersioning(ts, gofakes3.VersioningSuspended)
		assertVersioning(ts, "", "Suspended")
	})

	t.Run("no-versioning-suspend", func(t *testing.T) {
		ts := newTestServer(t, withFakerOptions(
			gofakes3.WithoutVersioning(),
		))
		defer ts.Close()

		setVersioning(ts, "Suspended")
		assertVersioning(ts, "", "")
	})

	t.Run("no-versioning-enable", func(t *testing.T) {
		ts := newTestServer(t, withFakerOptions(
			gofakes3.WithoutVersioning(),
		))
		defer ts.Close()

		svc := ts.s3Client()
		_, err := svc.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: aws.String(defaultBucket),
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String("Enabled"),
			},
		})
		if !hasErrorCode(err, gofakes3.ErrNotImplemented) {
			ts.Fatal("expected ErrNotImplemented, found", err)
		}
	})
}

func TestObjectVersions(t *testing.T) {
	create := func(ts *testServer, bucket, key string, contents []byte, version string) {
		ts.Helper()
		svc := ts.s3Client()
		out, err := svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(contents),
		})
		ts.OK(err)
		if aws.StringValue(out.VersionId) != version {
			t.Fatal("version ID mismatch. found:", aws.StringValue(out.VersionId), "expected:", version)
		}
	}

	get := func(ts *testServer, bucket, key string, contents []byte, version string) {
		ts.Helper()
		svc := ts.s3Client()
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		if version != "" {
			input.VersionId = aws.String(version)
		}
		out, err := svc.GetObject(input)
		ts.OK(err)
		defer out.Body.Close()
		bts, err := ioutil.ReadAll(out.Body)
		ts.OK(err)
		if !bytes.Equal(bts, contents) {
			ts.Fatal("body mismatch. found:", string(bts), "expected:", string(contents))
		}
	}

	deleteVersion := func(ts *testServer, bucket, key, version string) {
		ts.Helper()
		svc := ts.s3Client()
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		if version != "" {
			input.VersionId = aws.String(version)
		}
		ts.OKAll(svc.DeleteObject(input))
	}

	deleteDirect := func(ts *testServer, bucket, key, version string) {
		ts.Helper()
		svc := ts.s3Client()
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		out, err := svc.DeleteObject(input)
		ts.OK(err)
		if aws.StringValue(out.VersionId) != version {
			t.Fatal("version ID mismatch. found:", aws.StringValue(out.VersionId), "expected:", version)
		}
	}

	list := func(ts *testServer, bucket string, versions ...string) {
		ts.Helper()
		svc := ts.s3Client()
		out, err := svc.ListObjectVersions(&s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		ts.OK(err)

		var found []string
		for _, ver := range out.Versions {
			found = append(found, aws.StringValue(ver.VersionId))
		}
		for _, ver := range out.DeleteMarkers {
			found = append(found, aws.StringValue(ver.VersionId))
		}

		// Unfortunately, the S3 client API destroys the order of Versions and
		// DeleteMarkers, which are sibling elements in the XML body but separated
		// into different lists by the client:
		sort.Strings(found)
		sort.Strings(versions)
		if !reflect.DeepEqual(found, versions) {
			ts.Fatal("versions mismatch. found:", found, "expected:", versions)
		}
	}

	// XXX: version IDs are brittle; we control the seed, but the format may
	// change at any time.
	const v1 = "3/60O30C1G60O30C1G60O30C1G60O30C1G60O30C1G60O30C1H03F9QN5V72K21OG="
	const v2 = "3/60O30C1G60O30C1G60O30C1G60O30C1G60O30C1G60O30C1I00G5II3TDAF7GRG="
	const v3 = "3/60O30C1G60O30C1G60O30C1G60O30C1G60O30C1G60O30C1J01VFV0CD31ES81G="

	t.Run("put-list-delete-versions", func(t *testing.T) {
		ts := newTestServer(t, withVersioning())
		defer ts.Close()

		create(ts, defaultBucket, "object", []byte("body 1"), v1)
		get(ts, defaultBucket, "object", []byte("body 1"), "")
		list(ts, defaultBucket, v1)

		create(ts, defaultBucket, "object", []byte("body 2"), v2)
		get(ts, defaultBucket, "object", []byte("body 2"), "")
		list(ts, defaultBucket, v1, v2)

		create(ts, defaultBucket, "object", []byte("body 3"), v3)
		get(ts, defaultBucket, "object", []byte("body 3"), "")
		list(ts, defaultBucket, v1, v2, v3)

		get(ts, defaultBucket, "object", []byte("body 1"), v1)
		get(ts, defaultBucket, "object", []byte("body 2"), v2)
		get(ts, defaultBucket, "object", []byte("body 3"), v3)
		get(ts, defaultBucket, "object", []byte("body 3"), "")

		deleteVersion(ts, defaultBucket, "object", v1)
		list(ts, defaultBucket, v2, v3)
		deleteVersion(ts, defaultBucket, "object", v2)
		list(ts, defaultBucket, v3)
		deleteVersion(ts, defaultBucket, "object", v3)
		list(ts, defaultBucket)
	})

	t.Run("delete-direct", func(t *testing.T) {
		ts := newTestServer(t, withVersioning())
		defer ts.Close()

		create(ts, defaultBucket, "object", []byte("body 1"), v1)
		list(ts, defaultBucket, v1)
		create(ts, defaultBucket, "object", []byte("body 2"), v2)
		list(ts, defaultBucket, v1, v2)

		get(ts, defaultBucket, "object", []byte("body 2"), "")

		deleteDirect(ts, defaultBucket, "object", v3)
		list(ts, defaultBucket, v1, v2, v3)

		svc := ts.s3Client()
		_, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String("object"),
		})
		if !hasErrorCode(err, gofakes3.ErrNoSuchKey) {
			ts.Fatal("expected ErrNoSuchKey, found", err)
		}
	})

	t.Run("list-never-versioned", func(t *testing.T) {
		ts := newTestServer(t, withVersioning())
		defer ts.Close()

		const neverVerBucket = "neverver"
		ts.backendCreateBucket(neverVerBucket)

		ts.backendPutString(neverVerBucket, "object", nil, "body 1")
		list(ts, neverVerBucket, "null") // S300005
	})
}

func TestListBucketPages(t *testing.T) {
	createData := func(ts *testServer, prefix string, n int64) []string {
		keys := make([]string, n)
		for i := int64(0); i < n; i++ {
			key := fmt.Sprintf("%s%d", prefix, i)
			ts.backendPutString(defaultBucket, key, nil, fmt.Sprintf("body-%d", i))
			keys[i] = key
		}
		return keys
	}

	assertKeys := func(ts *testServer, rs *listBucketResult, keys ...string) {
		found := make([]string, len(rs.Contents))
		for i := 0; i < len(rs.Contents); i++ {
			found[i] = aws.StringValue(rs.Contents[i].Key)
		}
		if !reflect.DeepEqual(found, keys) {
			t.Fatal("key mismatch:", keys, "!=", found)
		}
	}

	for idx, tc := range []struct {
		keys, pageKeys int64
	}{
		{9, 2},
		{8, 3},
		{7, 4},
		{6, 5},
		{5, 6},
	} {
		t.Run(fmt.Sprintf("list-page-basic/%d", idx), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()
			keys := createData(ts, "", tc.keys)

			rs := ts.mustListBucketV1Pages(nil, tc.pageKeys, "")
			if len(rs.CommonPrefixes) > 0 {
				t.Fatal()
			}
			assertKeys(ts, rs, keys...)

			rs = ts.mustListBucketV2Pages(nil, tc.pageKeys, "")
			if len(rs.CommonPrefixes) > 0 {
				t.Fatal()
			}
			assertKeys(ts, rs, keys...)
		})

		t.Run(fmt.Sprintf("list-page-prefix/%d", idx), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			// junk keys with no prefix to ensure that we are actually limiting the output.
			// these should not show up in the output.
			createData(ts, "", tc.keys)

			// these are the actual keys we expect to see:
			keys := createData(ts, "test", tc.keys)

			prefix := gofakes3.NewPrefix(aws.String("test"), nil)

			rs := ts.mustListBucketV1Pages(&prefix, tc.pageKeys, "")
			if len(rs.CommonPrefixes) > 0 {
				t.Fatal()
			}
			assertKeys(ts, rs, keys...)

			rs = ts.mustListBucketV2Pages(&prefix, tc.pageKeys, "")
			if len(rs.CommonPrefixes) > 0 {
				t.Fatal()
			}
			assertKeys(ts, rs, keys...)
		})

		t.Run(fmt.Sprintf("list-page-prefix-delim/%d", idx), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			// junk keys with no prefix to ensure that we are actually limiting the output.
			// these should not show up in the output.
			createData(ts, "", tc.keys)

			// these are the actual keys we expect to see:
			keys := createData(ts, "test/", tc.keys)

			// add some common prefixes:
			createData(ts, "test/prefix1/", 2)
			createData(ts, "test/prefix2/", 2)

			prefix := gofakes3.NewFolderPrefix("test/")

			rs := ts.mustListBucketV1Pages(&prefix, tc.pageKeys, "")
			assertKeys(ts, rs, keys...)

			rs = ts.mustListBucketV2Pages(&prefix, tc.pageKeys, "")
			assertKeys(ts, rs, keys...)

			// FIXME: there are some unanswered questions for the assumer about
			// how CommonPrefixes interacts with paging; CommonPrefixes should be
			// checked once we've established how S3 actually behaves.
		})
	}
}

// Ensure that a backend that does not support pagination can use the fallback if enabled:
func TestListBucketPagesFallback(t *testing.T) {
	createData := func(ts *testServer, prefix string, n int64) []string {
		keys := make([]string, n)
		for i := int64(0); i < n; i++ {
			key := fmt.Sprintf("%s%d", prefix, i)
			ts.backendPutString(defaultBucket, key, nil, fmt.Sprintf("body-%d", i))
			keys[i] = key
		}
		return keys
	}

	t.Run("fallback-disabled", func(t *testing.T) {
		ts := newTestServer(t,
			withBackend(&backendWithUnimplementedPaging{s3mem.New()}),
			withFakerOptions(gofakes3.WithUnimplementedPageError()),
		)
		defer ts.Close()
		createData(ts, "", 5)
		_, err := ts.listBucketV1Pages(nil, 2, "")
		if !hasErrorCode(err, gofakes3.ErrNotImplemented) {
			t.Fatal(err)
		}
	})

	t.Run("fallback-enabled", func(t *testing.T) {
		ts := newTestServer(t, withBackend(&backendWithUnimplementedPaging{s3mem.New()}))
		defer ts.Close()
		createData(ts, "", 5)
		r := ts.mustListBucketV1Pages(nil, 2, "")

		// Without pagination, should just fall back to returning all keys:
		if len(r.Contents) != 5 {
			t.Fatal()
		}
	})
}

func s3HasErrorCode(err error, code gofakes3.ErrorCode) bool {
	if err, ok := err.(awserr.Error); ok {
		return code == gofakes3.ErrorCode(err.Code())
	}
	return false
}

func tryDumpResponse(rs *http.Response, body bool) string {
	b, _ := httputil.DumpResponse(rs, body)
	return string(b)
}
