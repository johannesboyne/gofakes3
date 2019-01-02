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
	bolt           *bolt.DB
	timeSource     gofakes3.TimeSource
	metaBucketName []byte
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
		bolt:           bolt,
		metaBucketName: []byte("_meta"), // Underscore guarantees no overlap with legal S3 bucket names
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = gofakes3.DefaultTimeSource()
	}
	return b
}

// metaBucket returns a utility that manages access to the metadata bucket.
// The returned struct is valid only for the lifetime of the bolt.Tx.
// The metadata bucket may not exist if this is an older database.
func (db *Backend) metaBucket(tx *bolt.Tx) (*metaBucket, error) {
	var bucket *bolt.Bucket
	var err error

	if tx.Writable() {
		bucket, err = tx.CreateBucketIfNotExists(db.metaBucketName)
		if err != nil {
			return nil, err
		}
	} else {
		bucket = tx.Bucket(db.metaBucketName)
		if bucket == nil {
			// FIXME: support legacy databases; remove when versioning is supported.
			return nil, nil
		}
	}

	return &metaBucket{
		Tx:       tx,
		bucket:   bucket,
		metaName: db.metaBucketName,
	}, nil
}

func (db *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	var buckets []gofakes3.BucketInfo

	err := db.bolt.View(func(tx *bolt.Tx) error {
		metaBucket, err := db.metaBucket(tx)
		if err != nil {
			return err
		}

		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if bytes.Equal(name, db.metaBucketName) {
				return nil
			}

			nameStr := string(name)
			info := gofakes3.BucketInfo{Name: nameStr}

			// Attempt to assign metadata. If it isn't found, we will just
			// pretend that's fine for now. This is to support existing
			// databases that have buckets created without associated metadata.
			//
			// FIXME: clean this up when there is an upgrade script to expect
			// that it exists
			if metaBucket != nil {
				bucketInfo, err := metaBucket.s3Bucket(nameStr)
				if err != nil {
					return err
				}
				if bucketInfo != nil {
					info.CreationDate = gofakes3.NewContentTime(bucketInfo.CreationDate)
				}
			}

			// The AWS CLI will fail if there is no creation date:
			if info.CreationDate.IsZero() {
				info.CreationDate = gofakes3.NewContentTime(db.timeSource.Now())
			}

			buckets = append(buckets, info)
			return nil
		})
	})

	return buckets, err
}

func (db *Backend) GetBucket(name string, prefix gofakes3.Prefix) (*gofakes3.Bucket, error) {
	var bucket *gofakes3.Bucket

	mod := gofakes3.NewContentTime(db.timeSource.Now())

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return gofakes3.BucketNotFound(name)
		}

		c := b.Cursor()
		bucket = gofakes3.NewBucket(name)

		for k, v := c.First(); k != nil; k, v = c.Next() {
			key := string(k)
			match := prefix.Match(key)
			if match == nil {
				continue

			} else if match.CommonPrefix {
				bucket.AddPrefix(match.MatchedPart)

			} else {
				hash := md5.Sum(v)
				item := &gofakes3.Content{
					Key:          string(k),
					LastModified: mod,
					ETag:         `"` + hex.EncodeToString(hash[:]) + `"`,
					Size:         len(v),
				}
				bucket.Add(item)
			}
		}

		return nil
	})

	return bucket, err
}

func (db *Backend) CreateBucket(name string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		{ // create bucket metadata
			metaBucket, err := db.metaBucket(tx)
			if err != nil {
				return err
			}
			if err := metaBucket.createS3Bucket(name, db.timeSource.Now()); err != nil {
				return err
			}
		}

		{ // create bucket
			nameBts := []byte(name)
			if tx.Bucket(nameBts) != nil {
				return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
			}
			if _, err := tx.CreateBucket(nameBts); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *Backend) DeleteBucket(name string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		nameBts := []byte(name)

		if bytes.Equal(nameBts, db.metaBucketName) {
			panic("gofakes3: attempted to delete metadata bucket")
		}

		{ // delete bucket
			b := tx.Bucket(nameBts)
			if b == nil {
				return gofakes3.ErrNoSuchBucket
			}
			c := b.Cursor()
			k, _ := c.First()
			if k != nil {
				return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
			}
		}

		{ // delete bucket metadata
			metaBucket, err := db.metaBucket(tx)
			if err != nil {
				return err
			}

			// FIXME: assumes a legacy database, where the bucket may not exist. Clean
			// this up when there is a DB upgrade script.
			if metaBucket != nil {
				if err := metaBucket.deleteS3Bucket(name); err != nil {
					return err
				}
			}
		}

		return tx.DeleteBucket(nameBts)
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
			return gofakes3.BucketNotFound(bucketName)
		}

		v := b.Get([]byte(objectName))
		if v == nil {
			return gofakes3.KeyNotFound(objectName)
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
			return gofakes3.BucketNotFound(bucketName)
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

func (db *Backend) DeleteObject(bucketName, objectName string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return gofakes3.BucketNotFound(bucketName)
		}
		if err := b.Delete([]byte(objectName)); err != nil {
			return fmt.Errorf("gofakes3: delete failed for object %q in bucket %q", objectName, bucketName)
		}
		return nil
	})
}

type readerWithDummyCloser struct{ io.Reader }

func (d readerWithDummyCloser) Close() error { return nil }

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }
