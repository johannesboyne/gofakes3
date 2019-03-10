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
// what happens if the object does not exist. The implication is
// that no error is returned; this is consistent with DeleteObject.
//
// This test confirms that this is indeed the case.
//
type S300003DeleteVersionFromNonexistentObject struct{}

func (s S300003DeleteVersionFromNonexistentObject) Run(ctx *Context) error {
	client := ctx.S3Client()
	config := ctx.Config()
	bucket := aws.String(config.BucketStandard())

	key := fmt.Sprintf("%d/%s", time.Now().UnixNano(), ctx.RandString(32))

	if err := ctx.EnsureVersioningEnabled(client, config.BucketStandard()); err != nil {
		return err
	}

	body := ctx.RandBytes(32)
	vrs, err := client.PutObject(&s3.PutObjectInput{
		Key:    aws.String(key),
		Bucket: bucket,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return err
	}

	// We need to use a real version ID because they have significance on AWS even
	// though the meaning is opaque:
	versionID := aws.StringValue(vrs.VersionId)
	if versionID == "" {
		return fmt.Errorf("version ID missing")
	}

	// Delete should succeed the first time. No versions for the object remain and
	// the object should no longer exist.
	if _, err := client.DeleteObject(&s3.DeleteObjectInput{
		Key:       aws.String(key),
		Bucket:    bucket,
		VersionId: aws.String(versionID),
	}); err != nil {
		return err
	}

	rs, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket: bucket,
		Prefix: aws.String(key),
	})
	if err != nil {
		return err
	}
	if len(rs.Versions) > 0 || len(rs.DeleteMarkers) > 0 {
		return fmt.Errorf("versions detected after delete")
	}

	// Now we should get the answer about what S3 actually does when you try to delete
	// a version for an object that is known not to exist:
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
