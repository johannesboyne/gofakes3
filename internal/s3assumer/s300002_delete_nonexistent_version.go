package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// It is not clear from the docs at
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
// what happens if the version or object does not exist. The implication is
// that no error is returned; this is consistent with DeleteObject.
//
// This test confirms that this is indeed the case.
//
// It also highlighted that the delete methods return a 204 status, not a 200.
//
type S300002DeleteNonexistentVersion struct{}

func (s S300002DeleteNonexistentVersion) Run(ctx *Context) error {
	client := ctx.S3Client()
	config := ctx.Config()
	bucket := aws.String(config.BucketStandard())

	key := fmt.Sprintf("%d/%s", time.Now().UnixNano(), ctx.RandString(32))

	if err := ctx.EnsureVersioningEnabled(client, config.BucketStandard()); err != nil {
		return err
	}

	var versionID string

	// Create two versions so we guarantee that even though we are deleting one of
	// them, the object still exists:
	for i := 0; i < 2; i++ {
		body := ctx.RandBytes(32)
		vrs, err := client.PutObject(&s3.PutObjectInput{
			Key:    aws.String(key),
			Bucket: bucket,
			Body:   bytes.NewReader(body),
		})
		if err != nil {
			return err
		}

		if i == 0 {
			// We need to use a real version ID because they have significance on AWS even
			// though the meaning is opaque:
			versionID = aws.StringValue(vrs.VersionId)
			if versionID == "" {
				return fmt.Errorf("version ID missing")
			}
		}
	}

	// Delete should succeed the first time. An object version will remain after this.
	if _, err := client.DeleteObject(&s3.DeleteObjectInput{
		Key:       aws.String(key),
		Bucket:    bucket,
		VersionId: aws.String(versionID),
	}); err != nil {
		return err
	}

	// Now we should get the answer about what S3 actually does when you try to delete
	// a version that is known not to exist!
	if _, err := client.DeleteObject(&s3.DeleteObjectInput{
		Key:       aws.String(key),
		Bucket:    bucket,
		VersionId: aws.String(versionID),
	}); err != nil {
		return err
	}

	// DEBUG: Response s3/DeleteObject Details:
	// ---[ RESPONSE ]--------------------------------------
	// HTTP/1.1 204 No Content

	return nil
}
