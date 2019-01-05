package gofakes3_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func TestMultipartUpload(t *testing.T) {
	const size = 5 * 1024 * 1024 // docs say MB, client SDK uses MiB

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
			svc := ts.s3Client()

			uploader := s3manager.NewUploaderWithClient(svc)

			var size int64
			if tc.last == 0 {
				size = tc.parts * tc.size
			} else {
				size = (tc.parts-1)*tc.size + tc.last
			}

			body, hash := randomFileBody(size)

			upParams := &s3manager.UploadInput{
				Bucket:     aws.String(defaultBucket),
				Key:        aws.String("uploadtest"),
				Body:       bytes.NewReader(body),
				ContentMD5: aws.String(hash.Base64()),
			}

			out, err := uploader.Upload(upParams, func(u *s3manager.Uploader) {
				u.LeavePartsOnError = true
				u.PartSize = tc.size
			})
			ts.OK(err)
			_ = out

			ts.assertObject(defaultBucket, "uploadtest", nil, body)
		})
	}
}
