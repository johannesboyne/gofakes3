package gofakes3

import (
	"io"
)

type Backend interface {
	ListBuckets() ([]BucketInfo, error)
	GetBucket(name string) (*Bucket, error)
	CreateBucket(name string) error
	BucketExists(name string) (exists bool, err error)
	GetObject(bucketName, objectName string) (*Object, error)

	// HeadObject fetches the Object from the backend, but the Contents will be
	// a no-op ReadCloser.
	HeadObject(bucketName, objectName string) (*Object, error)

	PutObject(bucketName, objectName string, meta map[string]string, input io.Reader) error
}
