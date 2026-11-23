package gofakes3_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// tagging builds a tag set from key/value pairs.
func tagging(kv ...string) *s3types.Tagging {
	tags := &s3types.Tagging{TagSet: []s3types.Tag{}}
	for i := 0; i+1 < len(kv); i += 2 {
		tags.TagSet = append(tags.TagSet, s3types.Tag{Key: aws.String(kv[i]), Value: aws.String(kv[i+1])})
	}
	return tags
}

// assertTags checks the tags GetObjectTagging returns for key.
func assertTags(t *testing.T, ts *testServer, key string, want map[string]string) {
	t.Helper()
	out, err := ts.s3Client().GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, tag := range out.TagSet {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tags of %q: got %v, want %v", key, got, want)
	}
}

func TestObjectTagging(t *testing.T) {
	runWithAllBackends(t, testObjectTagging)
}

func testObjectTagging(t *testing.T, ts *testServer) {
	svc := ts.s3Client()
	ctx := context.TODO()
	bucket, key := aws.String(defaultBucket), aws.String("object")

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   bucket,
		Key:      key,
		Body:     strings.NewReader("hello"),
		Metadata: map[string]string{"test": "test"},
	})
	ts.OK(err)
	assertTags(t, ts, "object", map[string]string{})

	_, err = svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:  bucket,
		Key:     key,
		Tagging: tagging("env", "test", "team", "a b&c=d"),
	})
	ts.OK(err)
	if body := ts.backendGetString(defaultBucket, "object", nil); body != "hello" {
		t.Fatalf("PutObjectTagging changed the object to %q", body)
	}
	assertTags(t, ts, "object", map[string]string{"env": "test", "team": "a b&c=d"})

	obj, err := svc.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
	ts.OK(err)
	obj.Body.Close()
	if obj.Metadata["test"] != "test" {
		t.Fatalf("PutObjectTagging dropped the metadata: %v", obj.Metadata)
	}
	if n := aws.ToInt32(obj.TagCount); n != 2 {
		t.Fatalf("x-amz-tagging-count: got %d, want 2", n)
	}

	_, err = svc.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: bucket, Key: key})
	ts.OK(err)
	if body := ts.backendGetString(defaultBucket, "object", nil); body != "hello" {
		t.Fatalf("DeleteObjectTagging changed the object to %q", body)
	}
	assertTags(t, ts, "object", map[string]string{})
}

// PutObject takes tags from x-amz-tagging, and an overwrite replaces them.
func TestPutObjectTaggingHeader(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("object"),
		Body:    strings.NewReader("hello"),
		Tagging: aws.String("a=1&b=two%20words"),
	})
	ts.OK(err)
	assertTags(t, ts, "object", map[string]string{"a": "1", "b": "two words"})

	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
		Body:   strings.NewReader("world"),
	})
	ts.OK(err)
	assertTags(t, ts, "object", map[string]string{})
}

func TestObjectTaggingInvalid(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()
	ts.backendPutString(defaultBucket, "object", nil, "hello")

	var tooMany []string
	for i := 0; i < 11; i++ {
		tooMany = append(tooMany, fmt.Sprintf("k%d", i), "v")
	}
	for _, tc := range []struct {
		name string
		tags *s3types.Tagging
	}{
		{"too many tags", tagging(tooMany...)},
		{"empty key", tagging("", "v")},
		{"key too long", tagging(strings.Repeat("k", 129), "v")},
		{"value too long", tagging("k", strings.Repeat("v", 257))},
		{"reserved prefix", tagging("aws:k", "v")},
		{"duplicate key", tagging("k", "1", "k", "2")},
	} {
		_, err := svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket:  aws.String(defaultBucket),
			Key:     aws.String("object"),
			Tagging: tc.tags,
		})
		if !hasErrorCode(err, gofakes3.ErrInvalidTag) {
			t.Errorf("%s: expected InvalidTag, got %v", tc.name, err)
		}
	}
	assertTags(t, ts, "object", map[string]string{})

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("other"),
		Body:    strings.NewReader("hello"),
		Tagging: aws.String("k=1&k=2"),
	})
	if !hasErrorCode(err, gofakes3.ErrInvalidTag) {
		t.Fatalf("x-amz-tagging with a duplicate key: expected InvalidTag, got %v", err)
	}
}

// Tagging requests for a missing object must neither succeed nor create it.
func TestObjectTaggingMissingObject(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()
	bucket, key := aws.String(defaultBucket), aws.String("missing")

	_, err := svc.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: bucket, Key: key})
	if !hasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("GetObjectTagging: expected NoSuchKey, got %v", err)
	}
	_, err = svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: bucket, Key: key, Tagging: tagging("k", "v")})
	if !hasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("PutObjectTagging: expected NoSuchKey, got %v", err)
	}
	_, err = svc.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: bucket, Key: key})
	if !hasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("DeleteObjectTagging: expected NoSuchKey, got %v", err)
	}
	if exists, _ := ts.backendObjectExists(defaultBucket, "missing"); exists {
		t.Fatal("a tagging request created the object")
	}
}

// Bucket tags are not stored, but tagging requests must not reach the bucket
// handlers: DeleteBucket removes an empty bucket.
func TestBucketTagging(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()
	bucket := aws.String(defaultBucket)

	_, err := svc.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: bucket})
	if !hasErrorCode(err, gofakes3.ErrNoSuchTagSet) {
		t.Errorf("GetBucketTagging: expected NoSuchTagSet, got %v", err)
	}
	_, err = svc.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: bucket, Tagging: tagging("k", "v")})
	if !hasErrorCode(err, gofakes3.ErrNotImplemented) {
		t.Errorf("PutBucketTagging: expected NotImplemented, got %v", err)
	}
	_, err = svc.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: bucket})
	ts.OK(err)

	exists, err := ts.backend.BucketExists(defaultBucket)
	ts.OK(err)
	if !exists {
		t.Fatal("DeleteBucketTagging deleted the bucket")
	}
}

func TestCopyObjectTagging(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("src"),
		Body:    strings.NewReader("hello"),
		Tagging: aws.String("a=1"),
	})
	ts.OK(err)

	for _, tc := range []struct {
		name      string
		directive s3types.TaggingDirective
		tagging   *string
		want      map[string]string
	}{
		{"default copies the source tags", "", nil, map[string]string{"a": "1"}},
		{"REPLACE takes the request tags", s3types.TaggingDirectiveReplace, aws.String("b=2"), map[string]string{"b": "2"}},
		{"REPLACE without tags", s3types.TaggingDirectiveReplace, nil, map[string]string{}},
	} {
		_, err := svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:           aws.String(defaultBucket),
			Key:              aws.String("dst"),
			CopySource:       aws.String(defaultBucket + "/src"),
			TaggingDirective: tc.directive,
			Tagging:          tc.tagging,
		})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		assertTags(t, ts, "dst", tc.want)
	}
}

// racingBackend replaces an object right after GetObject has read it, as a
// concurrent PUT would, while races is positive.
type racingBackend struct {
	gofakes3.Backend
	races atomic.Int32
}

func (b *racingBackend) GetObject(bucket, key string, rnge *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	obj, err := b.Backend.GetObject(bucket, key, rnge)
	if n := b.races.Add(-1); err == nil && n >= 0 {
		body := fmt.Sprintf("replaced %d", n)
		if _, err := b.Backend.PutObject(bucket, key, map[string]string{}, strings.NewReader(body), int64(len(body)), nil); err != nil {
			obj.Contents.Close()
			return nil, err
		}
	}
	return obj, err
}

// PutObjectTagging writes the object again. It must not overwrite an object
// that was replaced after it was read; it tags the new one instead.
func TestObjectTaggingConcurrentWrite(t *testing.T) {
	backend := &racingBackend{Backend: s3mem.New()}
	ts := newTestServer(t, withBackend(backend))
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()
	ts.backendPutString(defaultBucket, "object", nil, "hello")

	backend.races.Store(1)
	_, err := svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket: aws.String(defaultBucket), Key: aws.String("object"), Tagging: tagging("k", "v"),
	})
	ts.OK(err)
	backend.races.Store(0)
	if body := ts.backendGetString(defaultBucket, "object", nil); body != "replaced 0" {
		t.Fatalf("got %q, want the object written concurrently", body)
	}
	assertTags(t, ts, "object", map[string]string{"k": "v"})

	// The object changes before every attempt: give up with 409.
	backend.races.Store(100)
	_, err = svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket: aws.String(defaultBucket), Key: aws.String("object"), Tagging: tagging("k", "w"),
	})
	backend.races.Store(0)
	if !hasErrorCode(err, gofakes3.ErrConditionalRequestConflict) {
		t.Fatalf("expected ConditionalRequestConflict, got %v", err)
	}
	if body := ts.backendGetString(defaultBucket, "object", nil); !strings.HasPrefix(body, "replaced") || body == "replaced 0" {
		t.Fatalf("got %q, want the last object written concurrently", body)
	}
	assertTags(t, ts, "object", map[string]string{})
}

// mergingBackend merges the metadata of the previous object into every PUT, as
// the bundled backends did before. Other backends may still do so.
type mergingBackend struct {
	gofakes3.Backend
}

func (b *mergingBackend) PutObject(bucket, key string, meta map[string]string, input io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	if err := gofakes3.MergeMetadata(b.Backend, bucket, key, meta); err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	return b.Backend.PutObject(bucket, key, meta, input, size, conditions)
}

func TestDeleteObjectTaggingMergingBackend(t *testing.T) {
	ts := newTestServer(t, withBackend(&mergingBackend{Backend: s3mem.New()}))
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("object"),
		Body:    strings.NewReader("hello"),
		Tagging: aws.String("a=1"),
	})
	ts.OK(err)
	_, err = svc.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(defaultBucket), Key: aws.String("object"),
	})
	ts.OK(err)
	assertTags(t, ts, "object", map[string]string{})
}

func TestObjectTaggingVersioned(t *testing.T) {
	ts := newTestServer(t, withVersioning())
	defer ts.Close()
	svc := ts.s3Client()
	ctx := context.TODO()
	bucket, key := aws.String(defaultBucket), aws.String("object")

	v1, err := svc.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("v1")})
	ts.OK(err)
	v2, err := svc.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("v2")})
	ts.OK(err)

	// Tagging an older version is refused: writing it again would make it current.
	_, err = svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket: bucket, Key: key, VersionId: v1.VersionId, Tagging: tagging("k", "v"),
	})
	if !hasErrorCode(err, gofakes3.ErrNotImplemented) {
		t.Fatalf("tagging an older version: expected NotImplemented, got %v", err)
	}

	tagged, err := svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket: bucket, Key: key, VersionId: v2.VersionId, Tagging: tagging("k", "v"),
	})
	ts.OK(err)
	if body := ts.backendGetString(defaultBucket, "object", nil); body != "v2" {
		t.Fatalf("current version after tagging: got %q, want v2", body)
	}
	assertTags(t, ts, "object", map[string]string{"k": "v"})

	for _, tc := range []struct {
		version *string
		want    int
	}{
		{tagged.VersionId, 1},
		{v1.VersionId, 0},
	} {
		out, err := svc.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: bucket, Key: key, VersionId: tc.version})
		ts.OK(err)
		if len(out.TagSet) != tc.want {
			t.Fatalf("version %s: got %d tags, want %d", aws.ToString(tc.version), len(out.TagSet), tc.want)
		}
	}
}
