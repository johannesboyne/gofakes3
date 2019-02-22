package main

type Config struct {
	S3Endpoint   string
	S3Region     string
	S3PathStyle  bool
	S3TestBucket string
	Verbose      bool
}
