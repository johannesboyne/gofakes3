package gofakes3

import (
	"io"
)

type Backend interface {
	ListBuckets() ([]BucketInfo, error)

	// GetBucket should return a NotFound() error if the bucket does not exist.
	GetBucket(name string) (*Bucket, error)

	CreateBucket(name string) error

	BucketExists(name string) (exists bool, err error)

	// GetObject should return a NotFound() error if the object does not exist.
	GetObject(bucketName, objectName string) (*Object, error)

	DeleteObject(bucketName, objectName string) error

	// HeadObject fetches the Object from the backend, but the Contents will be
	// a no-op ReadCloser.
	//
	// HeadObject should return a NotFound() error if the object does not
	// exist.
	HeadObject(bucketName, objectName string) (*Object, error)

	PutObject(bucketName, objectName string, meta map[string]string, input io.Reader) error
}
