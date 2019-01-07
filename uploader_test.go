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

func TestListMultipartUploadsWithTheSameObjectKey(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload("testbucket", "obj", nil)
	ts.createMultipartUpload("testbucket", "obj", nil)
	ts.createMultipartUpload("testbucket", "obj", nil)

	ts.assertListMultipartUploads("testbucket", "", nil, 0,
		"obj/1", "obj/2", "obj/3")

	ts.assertListMultipartUploads("testbucket", "", nil, 1,
		"obj/1")
	ts.assertListMultipartUploads("testbucket", "", nil, 2,
		"obj/1", "obj/2")

	ts.assertListMultipartUploads("testbucket", "obj/2", nil, 1,
		"obj/2")
	ts.assertListMultipartUploads("testbucket", "obj/2", nil, 2,
		"obj/2", "obj/3")
}

func TestListMultipartUploadsWithDifferentObjectKeys(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload("testbucket", "foo", nil)
	ts.createMultipartUpload("testbucket", "bar", nil)
	ts.createMultipartUpload("testbucket", "baz", nil)

	ts.assertListMultipartUploads("testbucket", "", nil, 0,
		"bar/2", "baz/3", "foo/1")

	ts.assertListMultipartUploads("testbucket", "", nil, 1,
		"bar/2")
	ts.assertListMultipartUploads("testbucket", "", nil, 2,
		"bar/2", "baz/3")

	ts.assertListMultipartUploads("testbucket", "baz/3", nil, 1,
		"baz/3")
	ts.assertListMultipartUploads("testbucket", "baz/3", nil, 2,
		"baz/3", "foo/1")
}
