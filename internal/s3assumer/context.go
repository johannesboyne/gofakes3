package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Context struct {
	context.Context
	config Config
	rand   *rand.Rand
}

func (c *Context) Config() Config           { return c.config }
func (c *Context) Rand() *rand.Rand         { return c.rand }
func (c *Context) RandString(sz int) string { return hex.EncodeToString(c.RandBytes(sz)[:sz]) }

func (c *Context) RandBytes(sz int) []byte {
	out := make([]byte, sz)
	c.rand.Read(out)
	return out
}

func (c *Context) S3Client() *s3.S3 {
	config := aws.NewConfig()
	if c.config.S3Endpoint != "" {
		config.WithEndpoint(c.config.S3Endpoint)
	}
	if c.config.S3PathStyle {
		config.WithS3ForcePathStyle(c.config.S3PathStyle)
	}
	if c.config.S3Region != "" {
		config.WithRegion(c.config.S3Region)
	}

	if c.config.Verbose {
		var logger Logger
		config.WithLogLevel(aws.LogDebugWithHTTPBody)
		config.WithLogger(logger)
	}

	svc := s3.New(session.New(), config)
	return svc
}

func (c *Context) EnsureVersioningEnabled(client *s3.S3, bucket string) error {
	vers, err := c.getBucketVersioning(client, bucket)
	if err != nil {
		return err
	}

	status := aws.StringValue(vers.Status)
	if status != "Enabled" {
		if _, err := client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String("Enabled"),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func (c *Context) EnsureVersioningNeverEnabled(client *s3.S3, bucket string) error {
	vers, err := c.getBucketVersioning(client, bucket)
	if err != nil {
		return err
	}

	if aws.StringValue(vers.Status) != "" {
		return fmt.Errorf("unexpected status, found %q", aws.StringValue(vers.Status))
	}

	return nil
}

func (c *Context) getBucketVersioning(client *s3.S3, bucket string) (*s3.GetBucketVersioningOutput, error) {
	vers, err := client.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return nil, err
	}

	status := aws.StringValue(vers.Status)
	if status != "" && status != "Enabled" && status != "Suspended" {
		return nil, fmt.Errorf("unexpected status %q", status)
	}
	mfaDelete := aws.StringValue(vers.MFADelete)
	if mfaDelete != "" && mfaDelete != "Disabled" {
		return nil, fmt.Errorf("unexpected MFADelete %q", aws.StringValue(vers.MFADelete))
	}

	return vers, nil
}

type Logger struct{}

func (l Logger) Log(vs ...interface{}) {
	fmt.Println(vs...)
	fmt.Println()
}
