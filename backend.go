package gofakes3

import (
	"io"
)

// Backend provides a set of operations to be implemented in order to support
// gofakes3.
//
// The Backend API is not yet stable; if you create your own Backend, breakage
// is likely until this notice is removed.
//
type Backend interface {
	// ListBuckets returns a list of all buckets owned by the authenticated
	// sender of the request.
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
	ListBuckets() ([]BucketInfo, error)

	// GetBucket returns the contents of a bucket. Backends should use the
	// supplied prefix to limit the contents of the bucket and to sort the
	// matched items into the Contents and CommonPrefixes fields.
	//
	// GetBucket must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	GetBucket(name string, prefix Prefix) (*Bucket, error)

	// CreateBucket creates the bucket if it does not already exist. The name
	// should be assumed to be a valid name.
	//
	// If the bucket already exists, a gofakes3.ResourceError with
	// gofakes3.ErrBucketAlreadyExists MUST be returned.
	CreateBucket(name string) error

	BucketExists(name string) (exists bool, err error)

	// DeleteBucket deletes a bucket if and only if it is empty.
	//
	// If the bucket is not empty, gofakes3.ResourceError with
	// gofakes3.ErrBucketNotEmpty MUST be returned.
	//
	// If the bucket does not exist, gofakes3.ErrNoSuchBucket MUST be returned.
	//
	// AWS does not validate the bucket's name for anything other than existence.
	DeleteBucket(name string) error

	// GetObject must return a gofakes3.ErrNoSuchKey error if the object does not exist.
	// See gofakes3.KeyNotFound() for a convenient way to create one.
	GetObject(bucketName, objectName string) (*Object, error)

	// DeleteObject deletes an object from the bucket.
	//
	// DeleteObject must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	//
	// DeleteObject must not return an error if the object does not exist. Source:
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.DeleteObject:
	//
	//	Removes the null version (if there is one) of an object and inserts a
	//	delete marker, which becomes the latest version of the object. If there
	//	isn't a null version, Amazon S3 does not remove any objects.
	//
	DeleteObject(bucketName, objectName string) error

	// HeadObject fetches the Object from the backend, but the Contents will be
	// a no-op ReadCloser.
	//
	// HeadObject should return a NotFound() error if the object does not
	// exist.
	HeadObject(bucketName, objectName string) (*Object, error)

	// PutObject should assume that the key is valid.
	PutObject(bucketName, key string, meta map[string]string, input io.Reader) error
}
