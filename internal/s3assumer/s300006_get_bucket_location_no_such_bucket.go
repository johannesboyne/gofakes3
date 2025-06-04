package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Does GetBucketLocation return ErrNoSuchBucket when a nonexistent bucket is used?
type S300006GetBucketLocationNoSuchBucket struct{}

func (s S300006GetBucketLocationNoSuchBucket) Run(ctx *Context) error {
	client := ctx.S3Client()

	var b [40]byte
	rand.Read(b[:])
	bucket := hex.EncodeToString(b[:])

	{ // Sanity check version length
		rs, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(bucket),
		})
		_ = rs
		var aerr *types.NoSuchBucket
		if !errors.As(err, &aerr) {
			return fmt.Errorf("expected NoSuchBucket, found %s", err)
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	return nil
}
