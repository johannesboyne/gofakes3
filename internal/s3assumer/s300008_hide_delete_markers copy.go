package main

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S300008HideDeleteMarkers struct{}

func (s S300008HideDeleteMarkers) Run(ctx *Context) error {
	client := ctx.S3Client()
	config := ctx.Config()
	bucket := aws.String(config.BucketStandard())

	if err := ctx.EnsureVersioningEnabled(client, config.BucketStandard()); err != nil {
		return err
	}

	prefix := ctx.RandString(10) + "/"
	key1, key2 := prefix+ctx.RandString(20), prefix+ctx.RandString(20)
	keys := []string{key1, key2}

	versions := map[string][]string{}
	for _, key := range keys {
		for i := 0; i < 2; i++ {
			body := ctx.RandBytes(32)
			vrs, err := client.PutObject(ctx, &s3.PutObjectInput{
				Key:    aws.String(key),
				Bucket: bucket,
				Body:   bytes.NewReader(body),
			})
			if err != nil {
				return err
			}
			versions[key] = append(versions[key], aws.ToString(vrs.VersionId))
		}
	}

	// delete one of the objects
	_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: bucket,
		Delete: &s3types.Delete{
			Objects: []s3types.ObjectIdentifier{
				{
					Key: aws.String(keys[0]),
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// make ordinary list request. It should return only the keys[1].
	page1, err := client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: bucket,
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return err
	}

	l := len(page1.Contents)
	if l != 1 {
		return fmt.Errorf("unexpected number of objects %d but expected %d", l, 1)
	}

	if aws.ToString(page1.Contents[0].Key) != keys[1] {
		return fmt.Errorf("unexpected key %q but expected %q", aws.ToString(page1.Contents[0].Key), keys[1])
	}

	return nil
}
