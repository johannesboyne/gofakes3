package s3bolt

import (
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/johannesboyne/gofakes3"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strconv"
	"time"
)

type upload struct {
	gofakes3.MultipartBackendUpload
	Parts map[string]string // Keys are ints, but BSON requires string. use`strconv.Itoa()`
	Sizes map[string]int64  // Same as above, keys have to match!
}

func randomString(chars int) string {
	buf := make([]byte, chars/2)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("%x", buf)
}

const BUCKET_PREFIX = "__INTERNAL__s3bolt__"
const UPLOAD_BUCKET = BUCKET_PREFIX + "uploads"
const DATA_BUCKET = BUCKET_PREFIX + "data"

func uploadMatchesObject(bucket string, key string, up *upload) bool {
	if up == nil {
		return false
	}
	if up.Bucket != bucket {
		return false
	}
	if up.Object != key {
		return false
	}
	return true
}

func (db *Backend) InitiateMultipart(bucketName, key string, meta map[string]string, initiated time.Time) (obj gofakes3.MultipartBackendUpload, err error) {

	obj = gofakes3.MultipartBackendUpload{
		ID:        gofakes3.UploadID(randomString(32)),
		Bucket:    bucketName,
		Object:    key,
		Meta:      meta,
		Initiated: initiated,
	}
	up := upload{
		MultipartBackendUpload: obj,
		Parts:                  map[string]string{},
		Sizes:                  map[string]int64{},
	}

	err = db.setUpload(obj.ID, &up)
	if err != nil {
		return
	}

	return obj, nil
}

func (db *Backend) getUpload(id gofakes3.UploadID) (*upload, error) {
	var data []byte
	var up upload
	err := db.getBlob(UPLOAD_BUCKET, string(id), &data)
	if err != nil {
		return nil, gofakes3.ErrNoSuchUpload
	}
	err = bson.Unmarshal(data, &up)
	if err != nil {
		return nil, err
	}
	if up.Parts == nil {
		up.Parts = map[string]string{}
	}
	if up.Sizes == nil {
		up.Sizes = map[string]int64{}
	}
	return &up, nil
}

func (db *Backend) setUpload(id gofakes3.UploadID, up *upload) (err error) {
	var data []byte
	data, err = bson.Marshal(up)
	if err != nil {
		return
	}

	return db.putBlob(UPLOAD_BUCKET, string(id), &data, true)
}

// addPartAtomic allows multiple parts to be uploaded simultaneously, because it makes sure there only is one
// in-flight part-list update at a time
func (db *Backend) addPartAtomic(id gofakes3.UploadID, partNumber int, etag string, size int64) (err error) {
	db.Lock()
	defer db.Unlock()

	var up *upload
	up, err = db.getUpload(id)
	if err != nil {
		return
	}
	up.Parts[strconv.Itoa(partNumber)] = etag
	up.Sizes[strconv.Itoa(partNumber)] = size
	return db.setUpload(id, up)
}

func (db *Backend) addChunk(input io.Reader, size int64) (etag string, err error) {
	var data []byte
	data, err = gofakes3.ReadAll(input, size)
	if err != nil {
		return
	}

	etag = fmt.Sprintf(`"%x"`, sha512.Sum512(data))
	err = db.putBlob(DATA_BUCKET, etag, &data, true)
	return
}

func (db *Backend) deleteChunk(etag string) (err error) {
	err = db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(DATA_BUCKET))
		if b != nil {
			return b.Delete([]byte(etag))
		}
		return nil
	})

	return
}

func (db *Backend) PutMultipart(bucket, key string, id gofakes3.UploadID, partNumber int, input io.Reader, size int64) (string, error) {
	up, err := db.getUpload(id)
	if err != nil {
		return "", gofakes3.ErrNoSuchUpload
	}

	if !uploadMatchesObject(bucket, key, up) {
		return "", gofakes3.ErrNoSuchUpload
	}

	var etag string
	etag, err = db.addChunk(input, size)
	if err != nil {
		return "", err
	}

	err = db.addPartAtomic(id, partNumber, etag, size)

	return etag, err
}

func (db *Backend) AbortMultipart(bucket, key string, id gofakes3.UploadID) error {
	up, err := db.getUpload(id)
	if err != nil {
		return err
	}

	if !uploadMatchesObject(bucket, key, up) {
		return gofakes3.ErrNoSuchUpload
	}

	return db.destroy(id)
}

func (db *Backend) destroy(id gofakes3.UploadID) (err error) {
	var up *upload
	up, err = db.getUpload(id)
	if err != nil {
		return
	}

	for _, etag := range up.Parts {
		err = db.deleteChunk(etag)
		if err != nil {
			return
		}
	}

	err = db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(UPLOAD_BUCKET))
		if b != nil {
			return b.Delete([]byte(id))
		}
		return nil
	})
	return
}

func (db *Backend) CompleteMultipart(bucket, key string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (ret gofakes3.MultipartBackendUpload, err error) {
	var up *upload
	var size int64
	var last int

	db.Lock()
	defer db.Unlock()
	up, err = db.getUpload(id)
	if err != nil {
		return
	}

	if !uploadMatchesObject(bucket, key, up) {
		err = gofakes3.ErrNoSuchUpload
		return
	}

	var chunks []string
	var chunkSize int64

	for _, part := range input.Parts {
		if last+1 != part.PartNumber {
			err = gofakes3.ErrInvalidPartOrder
			return
		}
		last = part.PartNumber

		if etag, ok := up.Parts[strconv.Itoa(part.PartNumber)]; ok {
			if etag != part.ETag {
				err = gofakes3.ErrInvalidPart
				return
			}
			chunks = append(chunks, etag)
			size += up.Sizes[strconv.Itoa(part.PartNumber)]
			if chunkSize < up.Sizes[strconv.Itoa(part.PartNumber)] {
				chunkSize = up.Sizes[strconv.Itoa(part.PartNumber)]
			}
		} else {
			err = gofakes3.ErrInvalidPart
			return
		}
	}

	chunkHash := sha512.Sum512([]byte(fmt.Sprintf("%+v", chunks)))
	data, err := bson.Marshal(&boltObject{
		Name:      up.Object,
		Metadata:  up.Meta,
		Size:      size,
		Chunks:    chunks,
		ChunkSize: chunkSize,
		Hash:      chunkHash[:],
	})
	if err != nil {
		return
	}

	var previousBlob []byte
	var oldChunks []string
	_ = db.getBlob(up.Bucket, up.Object, &previousBlob)
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

	err = db.putBlob(up.Bucket, up.Object, &data, false)
	if err != nil {
		return
	}

	// Delete old chunks only after the new object was stored
	for _, etag := range oldChunks {
		err = db.deleteChunk(etag)
		if err != nil {
			return
		}
	}

	ret.ID = id
	ret.Bucket = up.Bucket
	ret.Object = up.Object
	ret.Meta = up.Meta
	ret.Initiated = up.Initiated
	ret.ETag = fmt.Sprintf(`"%x"`, chunkHash[:])
	err = db.destroy(id)

	return
}

func (db *Backend) ListOngoingMultiparts(bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	return nil, gofakes3.ErrNotImplemented
}

func (db *Backend) ListOngoingMultipartParts(bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	return nil, gofakes3.ErrNotImplemented
}
