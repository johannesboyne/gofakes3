package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// It's not clear from the docs what S3 does when versioning has been enabled,
// then suspended, then you request a version ID that exists.
//
// Turns out it continues to work just fine.
//
// This script also revealed that a bucket that has never had versioning will
// return empty strings for Status and MFADelete.
type S300001GetVersionAfterVersioningSuspended struct{}

func (t *S300001GetVersionAfterVersioningSuspended) Run(ctx *Context) error {
	client := ctx.S3Client()
	config := ctx.Config()

	bucket := aws.String(config.BucketStandard())

	if err := ctx.EnsureVersioningEnabled(client, config.BucketStandard()); err != nil {
		return err
	}

	// FIXME: defer delete object

	key := fmt.Sprintf("%d/%s", time.Now().UnixNano(), ctx.RandString(32))

	var versions = map[string][]byte{}

	for i := 0; i < 3; i++ {
		body := ctx.RandBytes(32)
		rs, err := client.PutObject(&s3.PutObjectInput{
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
			Bucket: bucket,
		})
		if err != nil {
			return err
		}

		ver := aws.StringValue(rs.VersionId)
		if ver == "" {
			return fmt.Errorf("missing version ID")
		}
		versions[ver] = body
	}

	if _, err := client.PutBucketVersioning(&s3.PutBucketVersioningInput{
		Bucket: bucket,
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Suspended"),
		},
	}); err != nil {
		return err
	}

	{
		vers, err := client.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: bucket})
		if err != nil {
			return err
		}
		status := aws.StringValue(vers.Status)
		if status != "Suspended" {
			return fmt.Errorf("unexpected status %q", status)
		}
	}

	readCloseBody := func(rdr io.ReadCloser) ([]byte, error) {
		defer rdr.Close()
		return ioutil.ReadAll(rdr)
	}

	for ver, body := range versions {
		rs, err := client.GetObject(&s3.GetObjectInput{
			Key:       aws.String(key),
			Bucket:    bucket,
			VersionId: aws.String(ver),
		})
		if err != nil {
			return err
		}
		rbody, err := readCloseBody(rs.Body)
		if err != nil {
			return err
		}

		if !bytes.Equal(body, rbody) {
			return fmt.Errorf("version not equal")
		}
	}

	return nil
}
