package s3bolt

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
	"gopkg.in/mgo.v2/bson"
)

var (
	emptyPrefix = &gofakes3.Prefix{}
)

type Backend struct {
	bolt           *bolt.DB
	timeSource     gofakes3.TimeSource
	metaBucketName []byte
	sync.Mutex
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
			// Skip internal data structures
			if strings.HasPrefix(string(name), BUCKET_PREFIX) {
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

func (db *Backend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if !page.IsEmpty() {
		return nil, gofakes3.ErrInternalPageNotImplemented
	}

	objects := gofakes3.NewObjectList()
	mod := gofakes3.NewContentTime(db.timeSource.Now())

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return gofakes3.BucketNotFound(name)
		}

		c := b.Cursor()
		var match gofakes3.PrefixMatch

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			key := string(k)
			if !prefix.Match(key, &match) {
				continue

			} else if match.CommonPrefix {
				objects.AddPrefix(match.MatchedPart)

			} else {
				obj, err := db.HeadObject(name, key)
				if err != nil {
					continue
				}

				item := &gofakes3.Content{
					Key:          key,
					LastModified: mod,
					ETag:         fmt.Sprintf(`"%x"`, obj.ETag),
					Size:         obj.Size,
				}
				objects.Add(item)
			}
		}

		return nil
	})

	return objects, err
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
	nameBts := []byte(name)

	if bytes.Equal(nameBts, db.metaBucketName) {
		return gofakes3.ResourceError(gofakes3.ErrInvalidBucketName, name)
	}

	return db.bolt.Update(func(tx *bolt.Tx) error {
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
	obj, err := db.GetObject(bucketName, objectName, &gofakes3.ObjectRangeRequest{Start: -1})
	if err != nil {
		return nil, err
	}
	obj.Contents = s3io.NoOpReadCloser{}
	return obj, nil
}

func (db *Backend) getBlob(bucketName, objectName string, data *[]byte) error {
	return db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return gofakes3.BucketNotFound(bucketName)
		}

		*data = b.Get([]byte(objectName))
		if data == nil || *data == nil {
			return gofakes3.KeyNotFound(objectName)
		}
		return nil
	})
}

func (db *Backend) putBlob(bucketName, objectName string, data *[]byte, createIfNotExists bool) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		var err error

		if tx.Writable() && createIfNotExists {
			b, err = tx.CreateBucketIfNotExists([]byte(bucketName))
			if err != nil {
				return gofakes3.BucketNotFound(bucketName)
			}
		} else {
			b = tx.Bucket([]byte(bucketName))
			if b == nil {
				return gofakes3.BucketNotFound(bucketName)
			}
		}

		return b.Put([]byte(objectName), *data)
	})
}

func (db *Backend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	var t boltObject
	var v []byte

	err := db.getBlob(bucketName, objectName, &v)
	if err != nil {
		return nil, err
	}

	if err = bson.Unmarshal(v, &t); err != nil {
		return nil, fmt.Errorf("gofakes3: could not unmarshal object at %q/%q: %v", bucketName, objectName, err)
	}

	// FIXME: objectName here is a bit of a hack; this can be cleaned up when we have a
	// database migration script.
	return t.Object(objectName, rangeRequest, db)
}

func (db *Backend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {

	etag, err := db.addChunk(input, size)
	if err != nil {
		return result, err
	}

	var data []byte
	data, err = bson.Marshal(&boltObject{
		Name:      objectName,
		Metadata:  meta,
		Size:      size,
		Chunks:    []string{etag},
		ChunkSize: size,
		Hash:      []byte(etag),
	})
	if err != nil {
		return
	}

	var previousBlob []byte
	var oldChunks []string
	_ = db.getBlob(bucketName, objectName, &previousBlob)
	if len(previousBlob) == 0 {
		// No previous data
	} else {
		var previous boltObject
		err = bson.Unmarshal(previousBlob, &previous)
		if err != nil {
			return
		}
		oldChunks = previous.Chunks
	}

	err = db.putBlob(bucketName, objectName, &data, false)

	// Delete old chunks only after the new object was stored
	for _, etag := range oldChunks {
		err = db.deleteChunk(etag)
		if err != nil {
			return
		}
	}

	result = gofakes3.PutObjectResult{
		VersionID: gofakes3.VersionID(etag),
	}
	return
}

func (db *Backend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, err error) {
	err = db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return gofakes3.BucketNotFound(bucketName)
		}

		var t boltObject
		var v []byte

		err := db.getBlob(bucketName, objectName, &v)
		if err != nil {
			return err
		}

		if err = bson.Unmarshal(v, &t); err != nil {
			return err
		}

		for _, etag := range t.Chunks {
			err = db.deleteChunk(etag)
			if err != nil {
				return err
			}
		}
		if err = b.Delete([]byte(objectName)); err != nil {
			return fmt.Errorf("gofakes3: delete failed for object %q in bucket %q", objectName, bucketName)
		}
		return nil
	})

	return
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, err error) {
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return gofakes3.BucketNotFound(bucketName)
		}

		return nil
	})

	for _, object := range objects {
		if _, err = db.DeleteObject(bucketName, object); err != nil {
			log.Println("delete object failed:", err)
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
				Key:     object,
			})
			err = nil
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}
	return result, err
}
