package gofakes3

import (
	"io"
)

const (
	DefaultBucketVersionKeys = 1000
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

type ObjectDeleteResult struct {
	// Specifies whether the versioned object that was permanently deleted was
	// (true) or was not (false) a delete marker. In a simple DELETE, this
	// header indicates whether (true) or not (false) a delete marker was
	// created.
	IsDeleteMarker bool

	// Returns the version ID of the delete marker created as a result of the
	// DELETE operation. If you delete a specific object version, the value
	// returned by this header is the version ID of the object version deleted.
	VersionID VersionID
}

type ListBucketVersionsPage struct {
	// Specifies the key in the bucket that you want to start listing from.
	KeyMarker string

	// Specifies the object version you want to start listing from.
	VersionIDMarker VersionID

	// Sets the maximum number of keys returned in the response body. The
	// response might contain fewer keys, but will never contain more. If
	// additional keys satisfy the search criteria, but were not returned
	// because max-keys was exceeded, the response contains
	// <isTruncated>true</isTruncated>. To return the additional keys, see
	// key-marker and version-id-marker.
	//
	// MaxKeys MUST be > 0.
	MaxKeys int64
}

type PutObjectResult struct {
	// If versioning is enabled on the bucket, this should be set to the
	// created version ID. If versioning is not enabled, this should be
	// empty.
	VersionID VersionID
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
	// not exist. See gofakes3.KeyNotFound() for a convenient way to create
	// one.
	//
	// If the returned Object is not nil, you MUST call Object.Contents.Close(),
	// otherwise you will leak resources. Implementers should return a no-op
	// implementation of io.ReadCloser.
	//
	// If rnge is nil, it is assumed you want the entire object. If rnge is not
	// nil, but the underlying backend does not support range requests,
	// implementers MUST return ErrNotImplemented.
	//
	// If the backend is a VersionedBackend, GetObject retrieves the latest version.
	GetObject(bucketName, objectName string, rangeRequest *ObjectRangeRequest) (*Object, error)

	// HeadObject fetches the Object from the backend, but reading the Contents
	// will return io.EOF immediately.
	//
	// If the returned Object is not nil, you MUST call Object.Contents.Close(),
	// otherwise you will leak resources. Implementers should return a no-op
	// implementation of io.ReadCloser.
	//
	// HeadObject should return a NotFound() error if the object does not
	// exist.
	HeadObject(bucketName, objectName string) (*Object, error)

	// DeleteObject deletes an object from the bucket.
	//
	// If the backend is a VersionedBackend and versioning is enabled, this
	// should introduce a delete marker rather than actually delete the object.
	//
	// DeleteObject must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	// FIXME: confirm with S3 whether this is the correct behaviour.
	//
	// DeleteObject must not return an error if the object does not exist. Source:
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.DeleteObject:
	//
	//	Removes the null version (if there is one) of an object and inserts a
	//	delete marker, which becomes the latest version of the object. If there
	//	isn't a null version, Amazon S3 does not remove any objects.
	//
	DeleteObject(bucketName, objectName string) (ObjectDeleteResult, error)

	// PutObject should assume that the key is valid. The map containing meta
	// may be nil.
	//
	// The size can be used if the backend needs to read the whole reader; use
	// gofakes3.ReadAll() for this job rather than ioutil.ReadAll().
	PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64) (PutObjectResult, error)

	DeleteMulti(bucketName string, objects ...string) (MultiDeleteResult, error)
}

// VersionedBackend may be optionally implemented by a Backend in order to support
// operations on S3 object versions.
//
// If you don't implement VersionedBackend, requests to GoFakeS3 that attempt to
// make use of versions will return ErrNotImplemented.
//
type VersionedBackend interface {
	// VersioningConfiguration must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	//
	// If the bucket has never had versioning enabled, VersioningConfiguration MUST return
	// empty strings (S300001).
	VersioningConfiguration(bucket string) (VersioningConfiguration, error)

	// SetVersioningConfiguration must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	SetVersioningConfiguration(bucket string, v VersioningConfiguration) error

	// GetObject must return a gofakes3.ErrNoSuchKey error if the object does
	// not exist. See gofakes3.KeyNotFound() for a convenient way to create
	// one.
	//
	// If the returned Object is not nil, you MUST call Object.Contents.Close(),
	// otherwise you will leak resources. Implementers should return a no-op
	// implementation of io.ReadCloser.
	//
	// GetObject must return gofakes3.ErrNoSuchVersion if the version does not
	// exist.
	//
	// If versioning has been enabled on a bucket, but subsequently suspended,
	// GetObjectVersion should still return the object version (S300001).
	GetObjectVersion(
		bucketName, objectName string,
		versionID VersionID,
		rangeRequest *ObjectRangeRequest) (*Object, error)

	// HeadObjectVersion fetches the Object version from the backend, but the Contents will be
	// a no-op ReadCloser.
	//
	// If the returned Object is not nil, you MUST call Object.Contents.Close(),
	// otherwise you will leak resources. Implementers should return a no-op
	// implementation of io.ReadCloser.
	//
	// HeadObjectVersion should return a NotFound() error if the object does not
	// exist.
	HeadObjectVersion(bucketName, objectName string, versionID VersionID) (*Object, error)

	// DeleteObjectVersion permanently deletes a specific object version.
	//
	// DeleteObjectVersion must return a gofakes3.ErrNoSuchBucket error if the bucket
	// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
	//
	// If the bucket exists and either the object does not exist (S300003) or
	// the version does not exist (S300002), you MUST return an empty
	// ObjectDeleteResult and a nil error.
	DeleteObjectVersion(bucketName, objectName string, versionID VersionID) (ObjectDeleteResult, error)

	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html
	ListBucketVersions(bucketName string, prefix Prefix, page ListBucketVersionsPage) (*ListBucketVersionsResult, error)
}
