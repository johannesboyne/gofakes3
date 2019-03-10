package main

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Here we are checking to see if a bucket that has never had versioning enabled
// will respond successfully to ListVersions; turns out it will.
//
// Discoveries:
//
// - S3 responds to ListVersions even if the bucket has never been versioned.
//   We will probably need to fake this in in the GoFakeS3 struct to use the
//   normal bucket listing API when the backend does not implement versioning,
//   but this will probably need to wait until we implement proper pagination.
//
// - The API returns the _string_ 'null' for the version ID, which the Go SDK
//   happily returns as the *string* value 'null' (yecch!). GoFakeS3 backend
//   implementers should be able to simply return the empty string; GoFakeS3
//   itself should handle this particular bit of jank once and once only.
//
type S300005ListVersionsWithNeverVersionedBucket struct{}

func (s S300005ListVersionsWithNeverVersionedBucket) Run(ctx *Context) error {
	client := ctx.S3Client()
	config := ctx.Config()
	bucket := aws.String(config.BucketUnversioned())

	if err := ctx.EnsureVersioningNeverEnabled(client, config.BucketUnversioned()); err != nil {
		return err
	}

	prefix := ctx.RandString(50)
	key1, key2 := prefix+ctx.RandString(100), prefix+ctx.RandString(100)
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

	{ // Sanity check version length
		rs, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket: bucket,
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return err
		}

		// We have uploaded two keys, two versions per key, but as we have never
		// enabled versioning, we only expect two results
		if len(rs.Versions) != 2 {
			return fmt.Errorf("unexpected version length")
		}

		for _, ver := range rs.Versions {
			// This is pretty bad... the AWS SDK returns the *STRING* 'null' if there's no version ID.
			if aws.StringValue(ver.VersionId) != "null" {
				return fmt.Errorf("version id was not nil; found %q", aws.StringValue(ver.VersionId))
			}
		}
	}

	return nil
}
