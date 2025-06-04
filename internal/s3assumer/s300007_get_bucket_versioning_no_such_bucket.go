package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Does GetBucketVersioning return ErrNoSuchBucket when a nonexistent bucket is used?
// Does PutBucketVersioning return ErrNoSuchBucket when a nonexistent bucket is used?
type S300007BucketVersioningNoSuchBucket struct{}

func (s S300007BucketVersioningNoSuchBucket) Run(ctx *Context) error {
	client := ctx.S3Client()

	var b [40]byte
	rand.Read(b[:])
	bucket := hex.EncodeToString(b[:])

	{ // GetBucketVersioning
		rs, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucket),
		})
		_ = rs
		var aerr *s3types.NoSuchBucket
		if !errors.As(err, &aerr) {
			return fmt.Errorf("expected NoSuchBucket, found %s", err)
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	{ // PutBucketVersioning
		rs, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String("gofakes3.shabbyrobe.org"),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		_ = rs
		var aerr *s3types.NoSuchBucket
		if !errors.As(err, &aerr) {
			return fmt.Errorf("expected NoSuchBucket, found %s", err.Error())
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	return nil
}
