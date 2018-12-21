package gofakes3

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/boltdb/bolt"
	"gopkg.in/mgo.v2/bson"
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

	CreateObject(bucketName, objectName string, meta map[string]string, input io.Reader) error
}

type BoltDBBackend struct {
	bolt       *bolt.DB
	timeSource TimeSource
}

var _ Backend = &BoltDBBackend{}

func (b *BoltDBBackend) ListBuckets() ([]BucketInfo, error) {
	var buckets []BucketInfo

	err := b.bolt.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			buckets = append(buckets, BucketInfo{string(name), ""})
			return nil
		})
	})

	return buckets, err
}

func (db *BoltDBBackend) GetBucket(name string) (*Bucket, error) {
	var bucket *Bucket

	mod := db.timeSource.Now().Format(time.RFC3339)

	err := db.bolt.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(name))
		if b == nil {
			return fmt.Errorf("gofakes3: bucket not found")
		}

		c := b.Cursor()
		bucket = newBucket(name)

		for k, v := c.First(); k != nil; k, v = c.Next() {
			hash := md5.Sum(v)
			bucket.Contents = append(bucket.Contents, &Content{
				Key:          string(k),
				LastModified: mod,
				ETag:         "\"" + hex.EncodeToString(hash[:]) + "\"",
				Size:         len(v),
				StorageClass: "STANDARD",
			})

			t := Object{}
			err := bson.Unmarshal(v, &t)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return bucket, err
}

func (db *BoltDBBackend) CreateBucket(name string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte(name)); err != nil {
			return err
		}
		return nil
	})
}

func (db *BoltDBBackend) BucketExists(name string) (exists bool, err error) {
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		exists = b != nil
		return nil
	})
	return exists, err
}

func (db *BoltDBBackend) HeadObject(bucketName, objectName string) (*Object, error) {
	obj, err := db.GetObject(bucketName, objectName)
	if err != nil {
		return nil, err
	}
	obj.Contents = noOpReadCloser{}
	return obj, nil
}

func (db *BoltDBBackend) GetObject(bucketName, objectName string) (*Object, error) {
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

func (db *BoltDBBackend) CreateObject(bucketName, objectName string, meta map[string]string, input io.Reader) error {
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

func (b *boltObject) Object() *Object {
	return &Object{
		Metadata: b.Metadata,
		Size:     b.Size,
		Contents: dummyReadCloser{bytes.NewReader(b.Contents)},
		Hash:     b.Hash,
	}
}

type dummyReadCloser struct{ io.Reader }

func (d dummyReadCloser) Close() error { return nil }

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }
