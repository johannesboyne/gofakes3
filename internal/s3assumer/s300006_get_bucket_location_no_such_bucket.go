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

// Does GetBucketLocation return ErrNoSuchBucket when a nonexistent bucket is used?
type S300006GetBucketLocationNoSuchBucket struct{}

func (s S300006GetBucketLocationNoSuchBucket) Run(ctx *Context) error {
	client := ctx.S3Client()

	var b [40]byte
	rand.Read(b[:])
	bucket := hex.EncodeToString(b[:])

	{ // Sanity check version length
		rs, err := client.GetBucketLocation(&s3.GetBucketLocationInput{
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

	return nil
}
