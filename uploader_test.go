package gofakes3_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func TestMultipartUpload(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	uploader := s3manager.NewUploaderWithClient(svc)

	body, hash := randomFileBody(10 * 1024 * 1024) // 5MB is minimum allowed part size.

	upParams := &s3manager.UploadInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("uploadtest"),
		Body:       bytes.NewReader(body),
		ContentMD5: aws.String(hash.Base64()),
	}

	out, err := uploader.Upload(upParams, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = true
	})
	ts.OK(err)
	_ = out

	ts.assertObject(defaultBucket, "uploadtest", nil, body)
}
