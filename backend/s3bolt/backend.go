package s3bolt

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/boltdb/bolt"
	"github.com/johannesboyne/gofakes3"
	"gopkg.in/mgo.v2/bson"
)

type Backend struct {
	bolt       *bolt.DB
	timeSource gofakes3.TimeSource
}

var _ gofakes3.Backend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource gofakes3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func NewFile(file string, opts ...Option) (*Backend, error) {
	if file == "" {
		return nil, fmt.Errorf("gofakes3: invalid bolt file name")
	}
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		return nil, err
	}
	return New(db, opts...), nil
}

func New(bolt *bolt.DB, opts ...Option) *Backend {
	b := &Backend{
		bolt: bolt,
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = gofakes3.DefaultTimeSource()
	}
	return b
}

func (b *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	var buckets []gofakes3.BucketInfo

	err := b.bolt.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			buckets = append(buckets, gofakes3.BucketInfo{string(name), ""})
			return nil
		})
	})

	return buckets, err
}

func (db *Backend) GetBucket(name string) (*gofakes3.Bucket, error) {
	var bucket *gofakes3.Bucket

	mod := gofakes3.ContentTime(db.timeSource.Now())

	err := db.bolt.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(name))
		if b == nil {
			return fmt.Errorf("gofakes3: bucket not found")
		}

		c := b.Cursor()
		bucket = gofakes3.NewBucket(name)

		for k, v := c.First(); k != nil; k, v = c.Next() {
			hash := md5.Sum(v)
			bucket.Contents = append(bucket.Contents, &gofakes3.Content{
				Key:          string(k),
				LastModified: mod,
				ETag:         "\"" + hex.EncodeToString(hash[:]) + "\"",
				Size:         len(v),
				StorageClass: "STANDARD",
			})
		}

		return nil
	})

	return bucket, err
}

func (db *Backend) CreateBucket(name string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte(name)); err != nil {
			return err
		}
		return nil
	})
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		exists = b != nil
		return nil
	})
	return exists, err
}

func (db *Backend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	obj, err := db.GetObject(bucketName, objectName)
	if err != nil {
		return nil, err
	}
	obj.Contents = noOpReadCloser{}
	return obj, nil
}

func (db *Backend) GetObject(bucketName, objectName string) (*gofakes3.Object, error) {
	var t boltObject

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("gofakes3: bucket %q does not exist", bucketName)
		}

		v := b.Get([]byte(objectName))
		if v == nil {
			return fmt.Errorf("gofakes3: object %q does not exist", objectName)
		}

		if err := bson.Unmarshal(v, &t); err != nil {
			return fmt.Errorf("gofakes3: could not unmarshal object at %q/%q: %v", bucketName, objectName, err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return t.Object(), nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader) error {
	bts, err := ioutil.ReadAll(input)
	if err != nil {
		return err
	}

	hash := md5.Sum(bts)

	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("gofakes3: bucket %q does not exist", bucketName)
		}

		data, err := bson.Marshal(&boltObject{
			Metadata: meta,
			Size:     int64(len(bts)),
			Contents: bts,
			Hash:     hash[:],
		})
		if err != nil {
			return err
		}
		if err := b.Put([]byte(objectName), data); err != nil {
			return err
		}
		return nil
	})
}

type boltObject struct {
	Metadata map[string]string
	Size     int64
	Contents []byte
	Hash     []byte
}

func (b *boltObject) Object() *gofakes3.Object {
	return &gofakes3.Object{
		Metadata: b.Metadata,
		Size:     b.Size,
		Contents: readerWithDummyCloser{bytes.NewReader(b.Contents)},
		Hash:     b.Hash,
	}
}

type readerWithDummyCloser struct{ io.Reader }

func (d readerWithDummyCloser) Close() error { return nil }

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }
