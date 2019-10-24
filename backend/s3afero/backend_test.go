package s3afero

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/spf13/afero"
)

func testingBackends(t *testing.T) []gofakes3.Backend {
	t.Helper()

	single, err := SingleBucket("test", afero.NewMemMapFs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	multi, err := MultiBucket(afero.NewMemMapFs())
	if err != nil {
		t.Fatal(err)
	}
	if err := multi.CreateBucket("test"); err != nil {
		t.Fatal(err)
	}

	backends := []gofakes3.Backend{single, multi}
	return backends
}

func TestPutGet(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run("", func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents := []byte("contents")
			if _, err := backend.PutObject("test", "yep", meta, bytes.NewReader(contents), int64(len(contents))); err != nil {
				t.Fatal(err)
			}
			hasher := md5.New()
			hasher.Write(contents)
			hash := hasher.Sum(nil)

			obj, err := backend.GetObject("test", "yep", nil)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(obj.Metadata, meta) {
				t.Fatal(obj.Metadata, "!=", meta)
			}

			result, err := ioutil.ReadAll(obj.Contents)
			defer obj.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(contents, result) {
				t.Fatal(result, "!=", contents)
			}
			if obj.Size != int64(len(contents)) {
				t.Fatal(obj.Size, "!=", len(contents))
			}
			if !bytes.Equal(obj.Hash, hash) {
				t.Fatal(hex.EncodeToString(obj.Hash), "!=", hex.EncodeToString(hash))
			}
		})
	}
}

func TestPutGetRange(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run("", func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents := []byte("contents")
			expected := contents[1:7]
			if _, err := backend.PutObject("test", "yep", meta, bytes.NewReader(contents), int64(len(contents))); err != nil {
				t.Fatal(err)
			}
			hasher := md5.New()
			hasher.Write(contents)
			hash := hasher.Sum(nil)

			obj, err := backend.GetObject("test", "yep", &gofakes3.ObjectRangeRequest{Start: 1, End: 6})
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(obj.Metadata, meta) {
				t.Fatal(obj.Metadata, "!=", meta)
			}

			result, err := ioutil.ReadAll(obj.Contents)
			defer obj.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(expected, result) {
				t.Fatal(result, "!=", expected)
			}
			if obj.Size != int64(len(contents)) {
				t.Fatal(obj.Size, "!=", len(contents))
			}
			if !bytes.Equal(obj.Hash, hash) {
				t.Fatal(hex.EncodeToString(obj.Hash), "!=", hex.EncodeToString(hash))
			}
		})
	}
}

func TestPutListRoot(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents1 := []byte("contents1")
			if _, err := backend.PutObject("test", "foo", meta, bytes.NewReader(contents1), int64(len(contents1))); err != nil {
				t.Fatal(err)
			}

			contents2 := []byte("contents2")
			if _, err := backend.PutObject("test", "bar", meta, bytes.NewReader(contents2), int64(len(contents2))); err != nil {
				t.Fatal(err)
			}

			result, err := backend.ListBucket("test",
				&gofakes3.Prefix{HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
				gofakes3.ListBucketPage{})
			if err != nil {
				t.Fatal(err)
			}

			if len(result.Contents) != 2 {
				t.Fatal()
			}

			if result.Contents[0].ETag != `"b2d0efbdc48f4b7bf42f8ab76d71f84e"` {
				t.Fatal("etag", result.Contents[0].ETag, "!=", `"b2d0efbdc48f4b7bf42f8ab76d71f84e"`)
			}
			if result.Contents[0].Size != 9 {
				t.Fatal("size", result.Contents[0].Size, "!=", 9)
			}
			if result.Contents[1].ETag != `"4891e2a24026da4dea5b4119e1dc1863"` {
				t.Fatal("etag", result.Contents[1].ETag, "!=", `"4891e2a24026da4dea5b4119e1dc1863"`)
			}
			if result.Contents[1].Size != 9 {
				t.Fatal("size", result.Contents[1].Size, "!=", 9)
			}
		})
	}
}

func TestPutListDir(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents1 := []byte("contents1")
			if _, err := backend.PutObject("test", "foo/bar", meta, bytes.NewReader(contents1), int64(len(contents1))); err != nil {
				t.Fatal(err)
			}

			contents2 := []byte("contents2")
			if _, err := backend.PutObject("test", "foo/baz", meta, bytes.NewReader(contents2), int64(len(contents2))); err != nil {
				t.Fatal(err)
			}

			{
				result, err := backend.ListBucket("test",
					&gofakes3.Prefix{Prefix: "foo/", HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
					gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 2 {
					t.Fatal()
				}
			}

			{
				result, err := backend.ListBucket("test",
					&gofakes3.Prefix{Prefix: "foo/bar", HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
					gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 1 {
					t.Fatal()
				}
			}
		})
	}
}

func TestPutDelete(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents := []byte("contents1")
			if _, err := backend.PutObject("test", "foo", meta, bytes.NewReader(contents), int64(len(contents))); err != nil {
				t.Fatal(err)
			}

			if _, err := backend.DeleteObject("test", "foo"); err != nil {
				t.Fatal(err)
			}

			result, err := backend.ListBucket("test",
				&gofakes3.Prefix{HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
				gofakes3.ListBucketPage{})
			if err != nil {
				t.Fatal(err)
			}

			if len(result.Contents) != 0 {
				t.Fatal()
			}
		})
	}
}

func TestPutDeleteMulti(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents1 := []byte("contents1")
			if _, err := backend.PutObject("test", "foo/bar", meta, bytes.NewReader(contents1), int64(len(contents1))); err != nil {
				t.Fatal(err)
			}

			contents2 := []byte("contents2")
			if _, err := backend.PutObject("test", "foo/baz", meta, bytes.NewReader(contents2), int64(len(contents2))); err != nil {
				t.Fatal(err)
			}

			deleteResult, err := backend.DeleteMulti("test", "foo/bar", "foo/baz")
			if err != nil {
				t.Fatal(err)
			}
			if err := deleteResult.AsError(); err != nil {
				t.Fatal(err)
			}

			bucketContents, err := backend.ListBucket("test",
				&gofakes3.Prefix{HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
				gofakes3.ListBucketPage{})
			if err != nil {
				t.Fatal(err)
			}

			if len(bucketContents.Contents) != 0 {
				t.Fatal()
			}
		})
	}
}

func TestMultiCreateBucket(t *testing.T) {
	// Some bugs surfaced in the bucket creation logic in the MultiBucket backend
	// that only occur when using a real FS.
	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	fs, err := FsPath(tmp)
	if err != nil {
		t.Fatal(err)
	}

	multi, err := MultiBucket(fs)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := multi.BucketExists("test"); ok {
		t.Fatal()
	}
	if err := multi.CreateBucket("test"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := multi.BucketExists("test"); !ok {
		t.Fatal()
	}
}
