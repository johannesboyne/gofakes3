package gofakes3_test

import (
	"testing"
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

	ts.createMultipartUpload("testbucket", "obj", nil)
	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Uploads: strs("obj/1")})

	ts.assertAbortMultipartUpload("testbucket", "obj", "1")
}

func TestListMultipartUploadsWithTheSameObjectKey(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload("testbucket", "obj", nil)
	ts.createMultipartUpload("testbucket", "obj", nil)
	ts.createMultipartUpload("testbucket", "obj", nil)

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Uploads: strs("obj/1", "obj/2", "obj/3")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Limit: 1, Uploads: strs("obj/1")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Limit: 2, Uploads: strs("obj/1", "obj/2")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Marker: "obj/2", Limit: 1, Uploads: strs("obj/2")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Marker: "obj/2", Limit: 2, Uploads: strs("obj/2", "obj/3")})
}

func TestListMultipartUploadsWithDifferentObjectKeys(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload("testbucket", "foo", nil)
	ts.createMultipartUpload("testbucket", "bar", nil)
	ts.createMultipartUpload("testbucket", "baz", nil)

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Uploads: strs("bar/2", "baz/3", "foo/1")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Limit: 1, Uploads: strs("bar/2")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Limit: 2, Uploads: strs("bar/2", "baz/3")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Marker: "baz/3", Limit: 1, Uploads: strs("baz/3")})

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{
		Marker: "baz/3", Limit: 2, Uploads: strs("baz/3", "foo/1")})
}

func TestListMultipartUploadsPrefix(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload("testbucket", "foo/bar", nil)
	ts.createMultipartUpload("testbucket", "foo/bar", nil)
	ts.createMultipartUpload("testbucket", "foo/baz", nil)
	ts.createMultipartUpload("testbucket", "foo/nested/yep", nil)
	ts.createMultipartUpload("testbucket", "food/bar", nil)
	ts.createMultipartUpload("testbucket", "food/baz", nil)
	ts.createMultipartUpload("testbucket", "yep/qux", nil)

	ts.assertListMultipartUploads("testbucket", listUploadsOpts{Prefix: prefixFile("foo/"),
		Prefixes: strs("foo/nested/"),
		Uploads:  strs("foo/bar/1", "foo/bar/2", "foo/baz/3")})

	// FIXME: This requires further investigation. How does AWS treat the combination
	// of commonprefixes and a limit?
	// ts.assertListMultipartUploads("testbucket", listUploadsOpts{Prefix: prefixFile("foo/"),
	//     Limit:    2,
	//     Prefixes: strs("foo/nested/"),
	//     Uploads:  strs("foo/bar/1", "foo/bar/2")})
}
