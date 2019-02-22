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

type Context interface {
	context.Context
	S3Client() *s3.S3
	Config() Config
	Rand() *rand.Rand
	RandString(sz int) string
	RandBytes(sz int) []byte
}

type testContext struct {
	context.Context
	config Config
	rand   *rand.Rand
}

var _ Context = &testContext{}

func (c *testContext) Config() Config           { return c.config }
func (c *testContext) Rand() *rand.Rand         { return c.rand }
func (c *testContext) RandString(sz int) string { return hex.EncodeToString(c.RandBytes(sz)[:sz]) }

func (c *testContext) RandBytes(sz int) []byte {
	out := make([]byte, sz)
	c.rand.Read(out)
	return out
}

func (c *testContext) S3Client() *s3.S3 {
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

	var logger Logger

	config.WithLogLevel(aws.LogDebugWithHTTPBody)
	config.WithLogger(logger)

	svc := s3.New(session.New(), config)
	return svc
}

type Logger struct{}

func (l Logger) Log(vs ...interface{}) {
	fmt.Println(vs...)
	fmt.Println()
}
