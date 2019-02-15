package gofakes3

import (
	"io"
)

// Object contains the data retrieved from a backend for the specified bucket
// and object key.
//
// You MUST always call Contents.Close() otherwise you may leak resources.
type Object struct {
	Name     string
	Metadata map[string]string
	Size     int64
	Contents io.ReadCloser
	Hash     []byte
	Range    *ObjectRange

	// VersionID will be empty if bucket versioning has not been enabled.
	VersionID VersionID

	// If versioning is enabled for the bucket, this is true if this object version
	// is a delete marker.
	IsDeleteMarker bool
}

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

	// ListBucket returns the contents of a bucket. Backends should use the
	// supplied prefix to limit the contents of the bucket and to sort the
	// matched items into the Contents and CommonPrefixes fields.
	//
	// ListBucket must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	//
	// WARNING: this API does not yet support pagination; it will change when
	// this is implemented.
	ListBucket(name string, prefix Prefix) (*ListBucketResult, error)

	// CreateBucket creates the bucket if it does not already exist. The name
	// should be assumed to be a valid name.
	//
	// If the bucket already exists, a gofakes3.ResourceError with
	// gofakes3.ErrBucketAlreadyExists MUST be returned.
	CreateBucket(name string) error

	// BucketExists should return a boolean indicating the bucket existence, or
	// an error if the backend was unable to determine existence.
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

	// GetObject must return a gofakes3.ErrNoSuchKey error if the object does
	// not exist.  See gofakes3.KeyNotFound() for a convenient way to create
	// one.
	//
	// If the returned Object is not nil, you MUST call Object.Contents.Close(),
	// otherwise you will leak resources.
	//
	// If rnge is nil, it is assumed you want the entire object. If rnge is not
	// nil, but the underlying backend does not support range requests,
	// implementers MUST return ErrNotImplemented.
	//
	// If the backend is a VersionedBackend, GetObject retrieves the latest version.
	GetObject(bucketName, objectName string, rangeRequest *ObjectRangeRequest) (*Object, error)

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

	// PutObject should assume that the key is valid. The map containing meta
	// may be nil.
	//
	// The size can be used if the backend needs to read the whole reader; use
	// gofakes3.ReadAll() for this job rather than ioutil.ReadAll().
	PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64) error

	DeleteMulti(bucketName string, objects ...string) (DeleteResult, error)
}

// VersionedBucket may be optionally implemented by a Backend in order to support
// operations on S3 object versions.
//
// If you don't implement VersionedBackend, requests to GoFakeS3 that attempt to
// make use of versions will return ErrNotImplemented.
//
type VersionedBackend interface {
	// VersioningConfiguration must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	VersioningConfiguration(bucket string) VersioningConfiguration

	// SetVersioningConfiguration must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	SetVersioningConfiguration(bucket string, v VersioningConfiguration) error

	GetObjectVersion(
		bucketName, objectName string,
		versionID VersionID,
		rangeRequest *ObjectRangeRequest) (*Object, error)

	ListBucketVersions(bucketName string, prefix Prefix) (*ListBucketVersionsResult, error)
}
