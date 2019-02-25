package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// This test proved that version-id-marker requires key-marker to be passed
// when paginating object versions. This is a huge relief as it would've been
// really painful to support arbitrary combinations.
//
// This test flushed out that we are handling version-id-marker slightly wrong.
// The following debug dump shows that if version-id-marker is present in the
// query but contains an empty string, S3 rejects the request with an invalid
// argument error:
//
//	GET /<bucket>?key-marker=810dd187e5ad28a83e8b208558823cf45d8d6e46b1f0fa56f2712de97213c343bbec1b871a4bfa92859cd142f25b9eb135008e47ad65359c4229b7919384d51dfd8f5b15e88eaec302a38447f86ef5642e03b10eac8ba81db5ddc949d769edf49e123b809800fa28e3ddef44bf96fe65ab422065e3378c9ff0c4c37a68e05f19af9d4755fbbd846cacecdaca95eecf9b9802685e7853&max-keys=1&prefix=810dd187e5ad28a83e8b208558823cf45d8d6e46b1f0fa56f2712de97213c343bbec1b871a4bfa92859cd142f25b9eb13500&version-id-marker=&versions=
// 	<Error>
// 	  <Code>InvalidArgument</Code>
// 	  <Message>A version-id marker cannot be empty.</Message>
// 	  <ArgumentName>version-id-marker</ArgumentName><ArgumentValue></ArgumentValue>
// 	  <RequestId>AC8DD6B6E4826D5C</RequestId>
// 	  <HostId>prA+xN7N52Ovkry/Q8jMhvbCm2MtWiraIk2SOJqleareEgUY41CmpxD1Uvcaq6YiShSGoUlDbvY=</HostId>
// 	</Error>
//
// It looks as if key-marker is not validated the same way - if it's not passed,
// it just starts from the start, and ignores version-id-marker if one is passed.
//
type S300004ListVersionsWithVersionMarkerButNoKeyMarker struct{}

func (s S300004ListVersionsWithVersionMarkerButNoKeyMarker) Run(ctx *Context) error {
	client := ctx.S3Client()
	config := ctx.Config()
	bucket := aws.String(config.BucketStandard())

	if err := ctx.EnsureVersioningEnabled(client, config.BucketStandard()); err != nil {
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
		if len(rs.Versions) != 4 {
			return fmt.Errorf("unexpected version length")
		}
	}

	page1, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket:  bucket,
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return err
	}

	// Passing no key marker, which should be an error:
	if _, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket:          bucket,
		Prefix:          aws.String(prefix),
		MaxKeys:         aws.Int64(1),
		VersionIdMarker: page1.NextVersionIdMarker,
	}); err == nil {
		return fmt.Errorf("expected error")
	} else if serr, ok := err.(awserr.RequestFailure); !ok {
		return fmt.Errorf("unexpected error %v", serr)
	} else if serr.Code() != "InvalidArgument" || !strings.Contains(serr.Message(), "version-id marker cannot be specified without a key marker") {
		return fmt.Errorf("unexpected error %v", serr)
	}

	// Passing key marker but empty version, which should fail:
	if _, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket:          bucket,
		Prefix:          aws.String(prefix),
		MaxKeys:         aws.Int64(1),
		KeyMarker:       page1.NextKeyMarker,
		VersionIdMarker: aws.String(""),
	}); err == nil {
		return fmt.Errorf("expected error")
	} else if serr, ok := err.(awserr.RequestFailure); !ok {
		return fmt.Errorf("unexpected error %v", serr)
	} else if serr.Code() != "InvalidArgument" || !strings.Contains(serr.Message(), "version-id marker cannot be empty") {
		return fmt.Errorf("unexpected error %v", serr)
	}

	// Passing version-id-marker but empty key-marker returns the first page for some reason:
	resultPage, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket:          bucket,
		Prefix:          aws.String(prefix),
		MaxKeys:         aws.Int64(1),
		KeyMarker:       aws.String(""),
		VersionIdMarker: page1.NextVersionIdMarker,
	})
	if err != nil {
		return fmt.Errorf("unexpected error %v", err)
	}

	if aws.StringValue(resultPage.Versions[0].VersionId) != aws.StringValue(page1.Versions[0].VersionId) {
		return fmt.Errorf("unexpected version id %s, expected %s",
			aws.StringValue(resultPage.Versions[0].VersionId), aws.StringValue(page1.Versions[0].VersionId))
	}

	// Passing key marker but no version, which should be fine:
	if _, err := client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket:    bucket,
		Prefix:    aws.String(prefix),
		MaxKeys:   aws.Int64(1),
		KeyMarker: page1.NextKeyMarker,
	}); err != nil {
		return fmt.Errorf("unexpected error")
	}

	return nil
}
