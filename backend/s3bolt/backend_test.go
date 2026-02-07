package s3bolt

import (
	"os"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
)

// setupTestBackend creates a temporary file and backend for testing.
// Returns the backend and a cleanup function.
func setupTestBackend(t *testing.T) (*Backend, func()) {
	tmpFile, err := os.CreateTemp("", "gofakes3-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	boltDB, err := NewFile(tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatal(err)
	}

	cleanup := func() {
		os.Remove(tmpFile.Name())
	}

	return boltDB, cleanup
}

// setupTestBucket creates a test bucket with the given objects.
// Returns the backend and a cleanup function.
func setupTestBucket(t *testing.T, bucketName string, objects []string) (*Backend, func()) {
	boltDB, cleanup := setupTestBackend(t)

	err := boltDB.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	for _, obj := range objects {
		_, err := boltDB.PutObject(bucketName, obj, nil, strings.NewReader(obj), int64(len(obj)), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	return boltDB, cleanup
}

func TestListBucketMarker(t *testing.T) {
	objects := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	boltDB, cleanup := setupTestBucket(t, "test-bucket", objects)
	defer cleanup()

	// Test 1: List all objects without marker
	result, err := boltDB.ListBucket("test-bucket", &gofakes3.Prefix{}, gofakes3.ListBucketPage{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 5 {
		t.Errorf("expected 5 objects, got %d", len(result.Contents))
	}
	if result.IsTruncated {
		t.Error("expected IsTruncated to be false")
	}

	// Test 2: List with marker = "b.txt"
	result, err = boltDB.ListBucket("test-bucket", &gofakes3.Prefix{}, gofakes3.ListBucketPage{
		Marker:    "b.txt",
		HasMarker: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 3 {
		t.Errorf("expected 3 objects after marker 'b.txt', got %d", len(result.Contents))
	}
	// Should return c.txt, d.txt, e.txt (excluding b.txt)
	if result.Contents[0].Key != "c.txt" {
		t.Errorf("expected first object to be 'c.txt', got '%s'", result.Contents[0].Key)
	}

	// Test 3: List with marker and MaxKeys
	result, err = boltDB.ListBucket("test-bucket", &gofakes3.Prefix{}, gofakes3.ListBucketPage{
		Marker:    "a.txt",
		HasMarker: true,
		MaxKeys:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 2 {
		t.Errorf("expected 2 objects with MaxKeys=2, got %d", len(result.Contents))
	}
	if !result.IsTruncated {
		t.Error("expected IsTruncated to be true")
	}
	if result.NextMarker != "c.txt" {
		t.Errorf("expected NextMarker to be 'c.txt', got '%s'", result.NextMarker)
	}

	// Test 4: Use NextMarker to get next page
	result, err = boltDB.ListBucket("test-bucket", &gofakes3.Prefix{}, gofakes3.ListBucketPage{
		Marker:    "c.txt",
		HasMarker: true,
		MaxKeys:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 2 {
		t.Errorf("expected 2 objects in second page, got %d", len(result.Contents))
	}
	// Should return d.txt, e.txt
	if result.Contents[0].Key != "d.txt" {
		t.Errorf("expected first object to be 'd.txt', got '%s'", result.Contents[0].Key)
	}
	if result.IsTruncated {
		t.Error("expected IsTruncated to be false on last page")
	}

	// Test 5: Marker at the end
	result, err = boltDB.ListBucket("test-bucket", &gofakes3.Prefix{}, gofakes3.ListBucketPage{
		Marker:    "e.txt",
		HasMarker: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 0 {
		t.Errorf("expected 0 objects after marker 'e.txt', got %d", len(result.Contents))
	}

	// Test 6: Non-existent marker (should seek to next key after the marker)
	result, err = boltDB.ListBucket("test-bucket", &gofakes3.Prefix{}, gofakes3.ListBucketPage{
		Marker:    "bb.txt",
		HasMarker: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Bolt's Seek returns the key or the next key, so "bb.txt" would seek to "c.txt"
	if len(result.Contents) != 3 {
		t.Errorf("expected 3 objects after non-existent marker 'bb.txt', got %d", len(result.Contents))
	}
	if result.Contents[0].Key != "c.txt" {
		t.Errorf("expected first object to be 'c.txt', got '%s'", result.Contents[0].Key)
	}
}

func TestListBucketPrefixWithMarker(t *testing.T) {
	objects := []string{"a.txt", "b.txt", "folder/a.txt", "folder/b.txt", "folder/c.txt"}
	boltDB, cleanup := setupTestBucket(t, "test-bucket", objects)
	defer cleanup()

	// Test: List with prefix "folder/" (no delimiter) and marker
	result, err := boltDB.ListBucket("test-bucket", &gofakes3.Prefix{Prefix: "folder/"}, gofakes3.ListBucketPage{
		Marker:    "folder/a.txt",
		HasMarker: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Should return folder/b.txt, folder/c.txt (after the marker)
	if len(result.Contents) != 2 {
		t.Errorf("expected 2 objects with prefix and marker, got %d", len(result.Contents))
	}
	if result.Contents[0].Key != "folder/b.txt" {
		t.Errorf("expected first object to be 'folder/b.txt', got '%s'", result.Contents[0].Key)
	}
}