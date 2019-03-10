package main

type Config struct {
	S3Endpoint         string
	S3Region           string
	S3PathStyle        bool
	S3TestBucketPrefix string
	Verbose            bool
}

func (c Config) BucketStandard() string {
	return c.S3TestBucketPrefix
}

func (c Config) BucketUnversioned() string {
	return c.S3TestBucketPrefix + ".unversioned"
}
