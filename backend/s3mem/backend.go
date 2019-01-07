package s3mem

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sync"

	"github.com/johannesboyne/gofakes3"
)

type Backend struct {
	buckets    map[string]*bucket
	timeSource gofakes3.TimeSource
	lock       sync.Mutex
}

var _ gofakes3.Backend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource gofakes3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func New(opts ...Option) *Backend {
	b := &Backend{
		buckets: make(map[string]*bucket),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = gofakes3.DefaultTimeSource()
	}
	return b
}

func (db *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	var buckets = make([]gofakes3.BucketInfo, 0, len(db.buckets))
	for _, bucket := range db.buckets {
		buckets = append(buckets, gofakes3.BucketInfo{
			Name:         bucket.name,
			CreationDate: bucket.creationDate,
		})
	}

	return buckets, nil
}

func (db *Backend) GetBucket(name string, prefix gofakes3.Prefix) (*gofakes3.Bucket, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	storedBucket := db.buckets[name]
	if storedBucket == nil {
		return nil, gofakes3.BucketNotFound(name)
	}

	response := gofakes3.NewBucket(name)
	for _, item := range storedBucket.data {
		match := prefix.Match(item.key)
		if match == nil {
			continue

		} else if match.CommonPrefix {
			response.AddPrefix(match.MatchedPart)

		} else {
			response.Add(&gofakes3.Content{
				Key:          item.key,
				LastModified: gofakes3.NewContentTime(item.lastModified),
				ETag:         `"` + hex.EncodeToString(item.hash) + `"`,
				Size:         len(item.data),
			})
		}
	}

	return response, nil
}

func (db *Backend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	}

	db.buckets[name] = &bucket{
		name:         name,
		creationDate: gofakes3.NewContentTime(db.timeSource.Now()),
		data:         map[string]*bucketItem{},
	}
	return nil
}

func (db *Backend) DeleteBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] == nil {
		return gofakes3.ErrNoSuchBucket
	}

	if len(db.buckets[name].data) > 0 {
		return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
	}

	delete(db.buckets, name)

	return nil
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.buckets[name] != nil, nil
}

func (db *Backend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.data[objectName]
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	return &gofakes3.Object{
		Hash:     obj.hash,
		Metadata: obj.metadata,
		Size:     int64(len(obj.data)),
		Contents: noOpReadCloser{},
	}, nil
}

func (db *Backend) GetObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.data[objectName]
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	return &gofakes3.Object{
		Hash:     obj.hash,
		Metadata: obj.metadata,
		Size:     int64(len(obj.data)),

		// The data slice should be completely replaced if the bucket item is edited, so
		// it should be safe to return the data slice directly.
		Contents: readerWithDummyCloser{bytes.NewReader(obj.data)},
	}, nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	bts, err := gofakes3.ReadAll(input, size)
	if err != nil {
		return err
	}

	hash := md5.Sum(bts)

	bucket.data[objectName] = &bucketItem{
		data:         bts,
		hash:         hash[:],
		metadata:     meta,
		key:          objectName,
		lastModified: db.timeSource.Now(),
	}

	return nil
}

func (db *Backend) DeleteObject(bucketName, objectName string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	// S3 does not report an error when attemping to delete a key that does not exist:
	delete(bucket.data, objectName)

	return nil
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.DeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	for _, object := range objects {
		delete(bucket.data, object)
		result.Deleted = append(result.Deleted, gofakes3.ObjectID{
			Key: object,
		})
	}

	return result, nil
}

type readerWithDummyCloser struct{ io.Reader }

func (d readerWithDummyCloser) Close() error { return nil }

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }
