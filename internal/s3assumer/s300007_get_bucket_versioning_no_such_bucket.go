package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
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
		rs, err := client.GetBucketVersioning(&s3.GetBucketVersioningInput{
			Bucket: aws.String(bucket),
		})
		_ = rs
		if aerr := (awserr.Error)(nil); errors.As(err, &aerr) {
			if aerr.Code() != s3.ErrCodeNoSuchBucket {
				return fmt.Errorf("expected NoSuchBucket, found %s", aerr.Code())
			}
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	{ // PutBucketVersioning
		rs, err := client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: aws.String("gofakes3.shabbyrobe.org"),
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String("enorbled"),
			},
		})
		_ = rs
		if aerr := (awserr.Error)(nil); errors.As(err, &aerr) {
			if aerr.Code() != s3.ErrCodeNoSuchBucket {
				return fmt.Errorf("expected NoSuchBucket, found %s", aerr.Code())
			}
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	return nil
}
