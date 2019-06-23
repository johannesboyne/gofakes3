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
	"github.com/johannesboyne/gofakes3/internal/s3io"
	"gopkg.in/mgo.v2/bson"
)

type boltBucket struct {
	CreationDate time.Time
}

type boltObject struct {
	Name         string
	Metadata     map[string]string
	LastModified time.Time
	Size         int64
	Chunks       []string
	ChunkSize    int64
	Hash         []byte
}

func (b *boltObject) Object(objectName string, rangeRequest *gofakes3.ObjectRangeRequest, backend *Backend) (*gofakes3.Object, error) {
	var rnge *gofakes3.ObjectRange
	var err error

	// Hack to make HEAD requests not hogging memory
	if rangeRequest != nil && rangeRequest.Start == -1 {
		rnge = &gofakes3.ObjectRange{
			Start: -1,
		}
	} else {
		rnge, err = rangeRequest.Range(b.Size)
		if err != nil {
			return nil, err

		}
	}

	var data []byte
	var position, length int64

	if rnge == nil {
		position = 0
		length = b.Size
	} else {
		position = rnge.Start
		length = rnge.Length
	}

	if position != -1 {
		if rnge == nil {
			data = make([]byte, b.Size)
		} else {
			data = make([]byte, length)
		}
		var readBytes int64
		var blob []byte
		for chunkIndex, etag := range b.Chunks {
			// If range starts above current chunk skip it
			if position > ((b.ChunkSize * int64(chunkIndex)) + b.ChunkSize - 1) {
				continue
			}

			var chunkOffset int64
			previousChunks := b.ChunkSize * int64(chunkIndex)
			if previousChunks == 0 || position == 0 {
				chunkOffset = position
			} else {
				chunkOffset = position % previousChunks
			}

			err = backend.getBlob(DATA_BUCKET, etag, &blob)
			if err != nil {
				return nil, err
			}

			n := copy(data[readBytes:], blob[chunkOffset:])
			readBytes += int64(n)
			position = 0
			if readBytes >= length {
				break
			}
		}
	}

	return &gofakes3.Object{
		Name:     objectName,
		Metadata: b.Metadata,
		Size:     b.Size,
		Contents: s3io.ReaderWithDummyCloser{bytes.NewReader(data)},
		Range:    rnge,
		ETag:     b.Hash,
	}, nil
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
