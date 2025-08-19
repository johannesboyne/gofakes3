package gofakes3_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
	bolt "go.etcd.io/bbolt"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// backendTestCase represents a backend configuration for testing
type backendTestCase struct {
	name    string
	backend gofakes3.Backend
	cleanup func() // Optional cleanup function
}

// createAllBackends creates instances of all available backends for testing
func createAllBackends(t *testing.T) []backendTestCase {
	t.Helper()

	var backends []backendTestCase

	// 1. s3mem backend
	backends = append(backends, backendTestCase{
		name:    "s3mem",
		backend: s3mem.New(s3mem.WithTimeSource(gofakes3.FixedTimeSource(defaultDate))),
		cleanup: nil,
	})

	// 2. s3bolt backend
	tempBoltFile, err := ioutil.TempFile("", "gofakes3-test-*.db")
	if err != nil {
		t.Fatal("Failed to create temp bolt file:", err)
	}
	tempBoltFile.Close()

	boltDB, err := bolt.Open(tempBoltFile.Name(), 0600, nil)
	if err != nil {
		os.Remove(tempBoltFile.Name())
		t.Fatal("Failed to open bolt database:", err)
	}

	boltBackend := s3bolt.New(boltDB, s3bolt.WithTimeSource(gofakes3.FixedTimeSource(defaultDate)))

	backends = append(backends, backendTestCase{
		name:    "s3bolt",
		backend: boltBackend,
		cleanup: func() {
			boltDB.Close()
			os.Remove(tempBoltFile.Name())
		},
	})

	// 3. s3afero SingleBucket backend (use defaultBucket name to match test expectations)
	singleBackend, err := s3afero.SingleBucket(defaultBucket, afero.NewMemMapFs(), nil)
	if err != nil {
		t.Fatal("Failed to create s3afero single backend:", err)
	}

	backends = append(backends, backendTestCase{
		name:    "s3afero-single",
		backend: singleBackend,
		cleanup: nil,
	})

	// 4. s3afero MultiBucket backend
	multiBackend, err := s3afero.MultiBucket(afero.NewMemMapFs())
	if err != nil {
		t.Fatal("Failed to create s3afero multi backend:", err)
	}

	backends = append(backends, backendTestCase{
		name:    "s3afero-multi",
		backend: multiBackend,
		cleanup: nil,
	})

	return backends
}

// runWithAllBackends runs a test function against all backend implementations
func runWithAllBackends(t *testing.T, testFunc func(*testing.T, *testServer)) {
	backends := createAllBackends(t)

	for _, backendCase := range backends {
		t.Run(backendCase.name, func(t *testing.T) {
			// Ensure cleanup happens even if test fails
			if backendCase.cleanup != nil {
				defer backendCase.cleanup()
			}

			// Create test server with this backend
			var ts *testServer
			if backendCase.name == "s3afero-single" {
				// SingleBucket backend already has the bucket, don't try to create it
				ts = newTestServer(t, withBackend(backendCase.backend), withoutInitialBuckets())
			} else {
				ts = newTestServer(t, withBackend(backendCase.backend))
			}
			defer ts.Close()

			// Run the test function
			testFunc(t, ts)
		})
	}
}

func TestConditionalPutIfNoneMatch(t *testing.T) {
	runWithAllBackends(t, testConditionalPutIfNoneMatch)
}

func testConditionalPutIfNoneMatch(t *testing.T, ts *testServer) {
	svc := ts.s3Client()

	bucket := aws.String(defaultBucket)
	key := aws.String("test-object")
	body := []byte("test content")

	// Test 1: If-None-Match with * should succeed when object doesn't exist
	_, err := svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      bucket,
		Key:         key,
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		t.Fatal("Expected success when object doesn't exist:", err)
	}

	// Test 2: If-None-Match with * should fail when object exists
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      bucket,
		Key:         key,
		Body:        bytes.NewReader([]byte("new content")),
		IfNoneMatch: aws.String("*"),
	})
	if err == nil {
		t.Fatal("Expected failure when object exists")
	}
	if !hasErrorCode(err, gofakes3.ErrPreconditionFailed) {
		t.Fatal("Expected PreconditionFailed error, got:", err)
	}

	// Verify original content is unchanged
	obj := ts.backendGetString(defaultBucket, "test-object", nil)
	if obj != "test content" {
		t.Fatal("Object content was modified when it shouldn't have been")
	}
}

func TestConditionalPutIfMatch(t *testing.T) {
	runWithAllBackends(t, testConditionalPutIfMatch)
}

func testConditionalPutIfMatch(t *testing.T, ts *testServer) {
	svc := ts.s3Client()

	bucket := aws.String(defaultBucket)
	key := aws.String("test-object")
	body := []byte("test content")

	// Create initial object
	putResp, err := svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatal("Failed to create initial object:", err)
	}
	etag := *putResp.ETag

	// Test 1: If-Match with correct ETag should succeed
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  bucket,
		Key:     key,
		Body:    bytes.NewReader([]byte("updated content")),
		IfMatch: aws.String(etag),
	})
	if err != nil {
		t.Fatal("Expected success with matching ETag:", err)
	}

	// Test 2: If-Match with wrong ETag should fail
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  bucket,
		Key:     key,
		Body:    bytes.NewReader([]byte("another update")),
		IfMatch: aws.String(`"wrong-etag"`),
	})
	if err == nil {
		t.Fatal("Expected failure with wrong ETag")
	}
	if !hasErrorCode(err, gofakes3.ErrPreconditionFailed) {
		t.Fatal("Expected PreconditionFailed error, got:", err)
	}

	// Verify content is the updated content, not the failed update
	obj := ts.backendGetString(defaultBucket, "test-object", nil)
	if obj != "updated content" {
		t.Fatal("Object content is not the expected updated content")
	}
}

func TestConditionalPutNonExistentObject(t *testing.T) {
	runWithAllBackends(t, testConditionalPutNonExistentObject)
}

func testConditionalPutNonExistentObject(t *testing.T, ts *testServer) {
	svc := ts.s3Client()

	bucket := aws.String(defaultBucket)
	key := aws.String("nonexistent-object")

	// Test: If-Match on non-existent object should fail
	_, err := svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  bucket,
		Key:     key,
		Body:    bytes.NewReader([]byte("content")),
		IfMatch: aws.String(`"some-etag"`),
	})
	if err == nil {
		t.Fatal("Expected failure when object doesn't exist")
	}
	if !hasErrorCode(err, gofakes3.ErrPreconditionFailed) {
		t.Fatal("Expected PreconditionFailed error, got:", err)
	}

	// Verify object was not created
	if exists, _ := ts.backendObjectExists(defaultBucket, "nonexistent-object"); exists {
		t.Fatal("Object should not have been created")
	}
}

func TestConditionalPutMultipleConditions(t *testing.T) {
	runWithAllBackends(t, testConditionalPutMultipleConditions)
}

func testConditionalPutMultipleConditions(t *testing.T, ts *testServer) {
	svc := ts.s3Client()

	bucket := aws.String(defaultBucket)
	key := aws.String("test-object")
	body := []byte("test content")

	// Create initial object
	putResp, err := svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatal("Failed to create initial object:", err)
	}
	etag := *putResp.ETag

	// Test: If-Match condition should pass
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  bucket,
		Key:     key,
		Body:    bytes.NewReader([]byte("updated content")),
		IfMatch: aws.String(etag),
	})
	if err != nil {
		t.Fatal("Expected success with If-Match condition:", err)
	}

	// Verify content was updated
	obj := ts.backendGetString(defaultBucket, "test-object", nil)
	if obj != "updated content" {
		t.Fatal("Object content was not updated")
	}
}

func TestConditionalPutCopyOperation(t *testing.T) {
	runWithAllBackends(t, testConditionalPutCopyOperation)
}

func testConditionalPutCopyOperation(t *testing.T, ts *testServer) {
	svc := ts.s3Client()

	bucket := aws.String(defaultBucket)
	sourceKey := aws.String("source-object")
	destKey := aws.String("dest-object")

	// Create source object
	_, err := svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: bucket,
		Key:    sourceKey,
		Body:   bytes.NewReader([]byte("source content")),
	})
	if err != nil {
		t.Fatal("Failed to create source object:", err)
	}

	// Test: Copy operation should not be affected by conditional headers
	// (copy operations always pass nil conditions to backend)
	_, err = svc.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     bucket,
		Key:        destKey,
		CopySource: aws.String(defaultBucket + "/" + *sourceKey),
	})
	if err != nil {
		t.Fatal("Copy operation failed:", err)
	}

	// Verify copy succeeded
	obj := ts.backendGetString(defaultBucket, "dest-object", nil)
	if obj != "source content" {
		t.Fatal("Copy operation did not work correctly")
	}
}

func TestConditionalPutETagComparison(t *testing.T) {
	runWithAllBackends(t, testConditionalPutETagComparison)
}

func testConditionalPutETagComparison(t *testing.T, ts *testServer) {
	svc := ts.s3Client()

	bucket := aws.String(defaultBucket)
	key := aws.String("test-object")
	body := []byte("test content")

	// Create initial object
	putResp, err := svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatal("Failed to create initial object:", err)
	}
	etag := *putResp.ETag

	// Test 1: ETag with quotes should work
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  bucket,
		Key:     key,
		Body:    bytes.NewReader([]byte("updated content 1")),
		IfMatch: aws.String(etag), // ETag already has quotes
	})
	if err != nil {
		t.Fatal("Expected success with quoted ETag:", err)
	}

	// Get new ETag
	getResp, err := svc.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatal("Failed to get object:", err)
	}
	newEtag := *getResp.ETag

	// Test 2: ETag without quotes should also work
	unquotedEtag := newEtag[1 : len(newEtag)-1] // Remove quotes
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  bucket,
		Key:     key,
		Body:    bytes.NewReader([]byte("updated content 2")),
		IfMatch: aws.String(unquotedEtag),
	})
	if err != nil {
		t.Fatal("Expected success with unquoted ETag:", err)
	}
}
