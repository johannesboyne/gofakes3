package s3bolt

// The schema for the bolt database is described in here. External users of the
// database should consider this an internal implementation detail, subject to
// change without notice or version number changes.
//
// This may change in the future.

import (
	"bytes"
	"time"

	"github.com/boltdb/bolt"
	"github.com/johannesboyne/gofakes3"
	"gopkg.in/mgo.v2/bson"
)

type boltBucket struct {
	CreationDate time.Time
}

type boltObject struct {
	Metadata     map[string]string
	LastModified time.Time
	Size         int64
	Contents     []byte
	Hash         []byte
}

func (b *boltObject) Object() *gofakes3.Object {
	return &gofakes3.Object{
		Metadata: b.Metadata,
		Size:     b.Size,
		Contents: readerWithDummyCloser{bytes.NewReader(b.Contents)},
		Hash:     b.Hash,
	}
}

func bucketMetaKey(name string) []byte {
	return []byte("bucket/" + name)
}

type metaBucket struct {
	*bolt.Tx
	metaName []byte
	bucket   *bolt.Bucket
}

func (mb *metaBucket) deleteS3Bucket(bucket string) error {
	return mb.bucket.Delete(bucketMetaKey(bucket))
}

func (mb *metaBucket) createS3Bucket(bucket string, at time.Time) error {
	bb := &boltBucket{
		CreationDate: at,
	}
	data, err := bson.Marshal(bb)
	if err != nil {
		return err
	}
	if err := mb.bucket.Put(bucketMetaKey(bucket), data); err != nil {
		return err
	}
	return nil
}

func (mb *metaBucket) s3Bucket(bucket string) (*boltBucket, error) {
	bts := mb.bucket.Get(bucketMetaKey(bucket))
	if bts == nil {
		// FIXME: should return an error once database upgrades are supported.
		return nil, nil
	}

	var bb boltBucket
	if err := bson.Unmarshal(bts, &bb); err != nil {
		return nil, err
	}
	return &bb, nil
}
