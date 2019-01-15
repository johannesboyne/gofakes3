package gofakes3_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
)

func TestMultipartUpload(t *testing.T) {
	const size = defaultUploadPartSize

	for _, tc := range []struct {
		parts int64 // number of parts
		size  int64 // size of all parts but last
		last  int64 // size of last part; if 0, size is used
	}{
		{parts: 1, size: size, last: 0},
		{parts: 10, size: size, last: 1},
		{parts: 2, size: 20 * 1024 * 1024, last: (20 * 1024 * 1024) - 1},

		// FIXME: Unfortunately, larger tests are too slow to be practical on
		// every run; should be skipped by default and enabled with a flag
		// later:
		// {parts: 100, size: 10 * 1024 * 1024, last: 1},
	} {
		t.Run("", func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			var size int64
			if tc.last == 0 {
				size = tc.parts * tc.size
			} else {
				size = (tc.parts-1)*tc.size + tc.last
			}

			body := randomFileBody(size)
			ts.assertMultipartUpload(defaultBucket, "uploadtest", body, nil)
		})
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "obj", nil)
	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("obj/1")})

	ts.assertAbortMultipartUpload(defaultBucket, "obj", "1")
}

func TestListMultipartUploadsWithTheSameObjectKey(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "obj", nil)
	ts.createMultipartUpload(defaultBucket, "obj", nil)
	ts.createMultipartUpload(defaultBucket, "obj", nil)

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("obj/1", "obj/2", "obj/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 1, Uploads: strs("obj/1")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 2, Uploads: strs("obj/1", "obj/2")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "obj/2", Limit: 1, Uploads: strs("obj/2")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "obj/2", Limit: 2, Uploads: strs("obj/2", "obj/3")})
}

func TestListMultipartUploadsWithDifferentObjectKeys(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "foo", nil)
	ts.createMultipartUpload(defaultBucket, "bar", nil)
	ts.createMultipartUpload(defaultBucket, "baz", nil)

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("bar/2", "baz/3", "foo/1")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 1, Uploads: strs("bar/2")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 2, Uploads: strs("bar/2", "baz/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "baz/3", Limit: 1, Uploads: strs("baz/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "baz/3", Limit: 2, Uploads: strs("baz/3", "foo/1")})
}

func TestListMultipartUploadsPrefix(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "foo/bar", nil)
	ts.createMultipartUpload(defaultBucket, "foo/bar", nil)
	ts.createMultipartUpload(defaultBucket, "foo/baz", nil)
	ts.createMultipartUpload(defaultBucket, "foo/nested/yep", nil)
	ts.createMultipartUpload(defaultBucket, "food/bar", nil)
	ts.createMultipartUpload(defaultBucket, "food/baz", nil)
	ts.createMultipartUpload(defaultBucket, "yep/qux", nil)

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("/"),
		Prefixes: strs("foo/", "food/", "yep/")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("fo"),
		Prefixes: strs("foo/", "food/")})

	// No prefix gives you everything:
	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("foo/bar/1", "foo/bar/2", "foo/baz/3", "foo/nested/yep/4", "food/bar/5", "food/baz/6", "yep/qux/7")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit:   3,
		Uploads: strs("foo/bar/1", "foo/bar/2", "foo/baz/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("foo/"),
		Prefixes: strs("foo/nested/"),
		Uploads:  strs("foo/bar/1", "foo/bar/2", "foo/baz/3")})

	// FIXME: This requires further investigation. How does AWS treat the combination
	// of commonprefixes and a limit?
	// ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("foo/"),
	//     Limit:    2,
	//     Prefixes: strs("foo/nested/"),
	//     Uploads:  strs("foo/bar/1", "foo/bar/2")})
}

func TestListMultipartUploadParts(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	id := ts.createMultipartUpload(defaultBucket, "foo", nil)

	parts := []*s3.CompletedPart{
		ts.uploadPart(defaultBucket, "foo", id, 1, []byte("abc")),
		ts.uploadPart(defaultBucket, "foo", id, 2, []byte("def")),
		ts.uploadPart(defaultBucket, "foo", id, 3, []byte("ghi")),
	}

	ts.assertListUploadParts(defaultBucket, "foo", id,
		listUploadPartsOpts{}.withCompletedParts(parts...))

	ts.assertCompleteUpload(defaultBucket, "foo", id, parts, []byte("abcdefghi"))

	// No parts should be returned after the upload is completed:
	ts.assertListUploadPartsFails(gofakes3.ErrNoSuchUpload, defaultBucket, "foo", id, listUploadPartsOpts{})
}
