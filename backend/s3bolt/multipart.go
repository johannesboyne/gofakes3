package s3bolt

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/johannesboyne/gofakes3"
	"io"
	"sync"
	"time"
)

type upload struct {
	gofakes3.MultipartBackendUpload
	sync.Mutex
	parts map[string]*part
}

type part struct {
	content    []byte
	size       int64
	partNumber int
}

var uploads = map[string]*upload{}

func randomString(chars int) string {
	buf := make([]byte, chars/2)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("%x", buf)
}

func (db *Backend) InitiateMultipart(bucketName, key string, meta map[string]string, initiated time.Time) (gofakes3.MultipartBackendUpload, error) {

	obj := gofakes3.MultipartBackendUpload{
		ID:        gofakes3.UploadID(randomString(32)),
		Bucket:    bucketName,
		Object:    key,
		Meta:      meta,
		Initiated: initiated,
	}
	up := upload{
		MultipartBackendUpload: obj,
		Mutex:                  sync.Mutex{},
		parts:                  map[string]*part{},
	}
	uploads[string(obj.ID)] = &up

	fmt.Printf("%+v\n", up)

	return obj, nil
}

func (db *Backend) PutMultipart(_, _ string, id gofakes3.UploadID, partNumber int, input io.Reader, size int64) (string, error) {
	data, err := gofakes3.ReadAll(input, size)
	if err != nil {
		return "", err
	}
	uploads[string(id)].Lock()
	defer uploads[string(id)].Unlock()

	etag := randomString(48)

	uploads[string(id)].parts[etag] = &part{
		size:       size,
		content:    data,
		partNumber: partNumber,
	}

	fmt.Printf("ETag: %s, upload size: %d\n", etag, uploads[string(id)].parts[etag].size)

	return etag, gofakes3.ErrNotImplemented
}

func (db *Backend) AbortMultipart(_, _ string, id gofakes3.UploadID) error {
	delete(uploads, string(id)) // No-op if not existent

	return nil
}

func (db *Backend) CompleteMultipart(_, _ string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (ret gofakes3.MultipartBackendUpload, err error) {
	var ok bool
	var up *upload
	var size int64
	var last int
	var readers []io.Reader

	if up, ok = uploads[string(id)]; !ok {
		err = gofakes3.ErrInvalidPart
		return
	}

	for _, part := range input.Parts {
		if last+1 != part.PartNumber {
			fmt.Printf("ErrInvalidPartOrder, last: %d, current: %d\n", last, part.PartNumber)
			err = gofakes3.ErrInvalidPartOrder
			return
		}
		last = part.PartNumber

		if upPart, ok := up.parts[part.ETag]; ok {
			if upPart.partNumber != part.PartNumber {
				fmt.Printf("Partnumber mismatch upPart.partNumber: %d, part.PartNumber: %d\n", upPart.partNumber, part.PartNumber)
				err = gofakes3.ErrInvalidPart
				return
			}
			readers = append(readers, bytes.NewReader(upPart.content))
			size += upPart.size
		} else {
			fmt.Printf("Part with etag %s does not exist\n", part.ETag)
			err = gofakes3.ErrInvalidPart
			return
		}
	}

	res, err := db.PutObject(
		uploads[string(id)].Bucket,
		uploads[string(id)].Object,
		uploads[string(id)].Meta,
		io.MultiReader(readers...),
		size,
	)

	if err != nil {
		return
	}

	ret.ID = id
	ret.Bucket = uploads[string(id)].Bucket
	ret.Object = uploads[string(id)].Object
	ret.Meta = uploads[string(id)].Meta
	ret.Initiated = uploads[string(id)].Initiated
	ret.VersionID = res.VersionID

	res2, err := db.GetObject(ret.Bucket, ret.Object, nil)
	ret.ETag = string(res2.ETag)

	return
}

func (db *Backend) ListOngoingMultiparts(bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	return nil, gofakes3.ErrNotImplemented
}

func (db *Backend) ListOngoingMultipartParts(bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	return nil, gofakes3.ErrNotImplemented
}
