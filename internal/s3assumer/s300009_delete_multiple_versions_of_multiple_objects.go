package main

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S300009DeleteMultipleVersionsOfMultipleObjects struct{}

func (s S300009DeleteMultipleVersionsOfMultipleObjects) Run(ctx *Context) error {
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
			vrs, err := client.PutObject(&s3.PutObjectInput{
				Key:    aws.String(key),
				Bucket: bucket,
				Body:   bytes.NewReader(body),
			})
			if err != nil {
				return err
			}
			versions[key] = append(versions[key], aws.StringValue(vrs.VersionId))
		}
	}

	objVrs := make([]*s3.ObjectIdentifier, 0, 4)
	for _, key := range keys {
		for _, vrs := range versions[key] {
			objVrs = append(objVrs, &s3.ObjectIdentifier{Key: aws.String(key), VersionId: aws.String(vrs)})
		}
	}

	{ // Sanity check version length
		rs, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket: bucket,
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return err
		}

		// We have uploaded two keys, two versions per key, total of 4
		if len(rs.Versions) != 4 {
			return fmt.Errorf("unexpected number of objects %d but expected %d", len(rs.Versions), 1)
		}
	}

	res, err := client.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: bucket,
		Delete: &s3.Delete{
			Objects: objVrs,
		},
	})
	if err != nil {
		return fmt.Errorf(err.Error(), res)
	}

	{ // Sanity check version length
		rs, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket: bucket,
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return err
		}

		// We have deleted all of the objects so we don't expect any version
		if len(rs.Versions) > 0 {
			return fmt.Errorf("unexpected number of objects %d but expected %d:\n%s", len(rs.Versions), 0, rs)
		}
	}

	return nil
}
