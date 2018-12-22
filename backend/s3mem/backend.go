package s3mem

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
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
			Name: bucket.name,
			// CreationDate: bucket.creationDate,
		})
	}

	return buckets, nil
}

func (db *Backend) GetBucket(name string) (*gofakes3.Bucket, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	storedBucket := db.buckets[name]
	if storedBucket == nil {
		return nil, fmt.Errorf("gofakes3: bucket %q not found", name)
	}

	response := gofakes3.NewBucket(name)
	for _, item := range storedBucket.data {
		response.Contents = append(response.Contents, &gofakes3.Content{
			Key:          item.key,
			LastModified: gofakes3.ContentTime(item.lastModified),
			ETag:         "\"" + hex.EncodeToString(item.hash) + "\"",
			Size:         len(item.data),
			StorageClass: "STANDARD",
		})
	}

	return response, nil
}

func (db *Backend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return fmt.Errorf("gofakes3: bucket %q exists", name)
	}

	db.buckets[name] = &bucket{
		name: name,
		data: map[string]*bucketItem{},
	}
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

	obj, err := db.GetObject(bucketName, objectName)
	if err != nil {
		return nil, err
	}
	obj.Contents = noOpReadCloser{}
	return obj, nil
}

func (db *Backend) GetObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, fmt.Errorf("gofakes3: bucket %q does not exist", bucketName)
	}

	obj := bucket.data[objectName]
	if obj == nil {
		return nil, fmt.Errorf("gofakes3: object %q does not existin bucket %q", objectName, bucketName)
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

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return fmt.Errorf("gofakes3: bucket %q does not exist", bucketName)
	}

	bts, err := ioutil.ReadAll(input)
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

type readerWithDummyCloser struct{ io.Reader }

func (d readerWithDummyCloser) Close() error { return nil }

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }
