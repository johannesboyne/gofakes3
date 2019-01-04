package gofakes3

import (
	"crypto/md5"
	"fmt"
	"math/big"
	"sync"
)

var add1 = new(big.Int).SetInt64(1)

type multipartUploadPart struct {
	PartNumber string
	ETag       string
	Body       []byte
}

type multipartUpload struct {
	ID     string
	Bucket string
	Object string
	Meta   map[string]string

	// do not attempt to access parts without locking mu
	parts map[string]multipartUploadPart

	// pointer back into linked list:
	listNode *uploadIDNode

	mu sync.Mutex
}

func (mpu *multipartUpload) AddPart(partNumber string, etag string, body []byte) error {
	mpu.mu.Lock()
	defer mpu.mu.Unlock()

	if etag == "" {
		return ErrInvalidPart
	}

	part := multipartUploadPart{
		PartNumber: partNumber,
		Body:       body,
		ETag:       etag,
	}
	mpu.parts[partNumber] = part
	return nil
}

func (mpu *multipartUpload) Reassemble(input *CompleteMultipartUploadRequest) (body []byte, etag string, err error) {
	mpu.mu.Lock()
	defer mpu.mu.Unlock()

	// FIXME: what does AWS do when mpu.Parts > input.Parts? Presumably you may
	// end up uploading more parts than you need to assemble, so it should
	// probably just ignore that?
	if len(input.Parts) > len(mpu.parts) {
		return nil, "", ErrInvalidPart
	}

	if !input.partsAreSorted() {
		return nil, "", ErrInvalidPartOrder
	}

	for _, inPart := range input.Parts {
		upPart, ok := mpu.parts[inPart.PartNumber]
		if !ok {
			return nil, "", ErrorMessagef(ErrInvalidPart, "unexpected part number %s in complete request", inPart.PartNumber)
		}
		if inPart.ETag != upPart.ETag {
			return nil, "", ErrorMessagef(ErrInvalidPart, "unexpected part etag for number %s in complete request", inPart.PartNumber)
		}
	}

	for _, part := range input.Parts {
		body = append(body, mpu.parts[part.PartNumber].Body...)
	}

	hash := fmt.Sprintf("%x", md5.Sum(body))

	return body, hash, nil
}

type uploader struct {
	uploads   map[string]*multipartUpload
	uploadIDs uploadIDList
	uploadID  *big.Int
	mu        sync.Mutex
}

func newUploader() *uploader {
	return &uploader{
		uploads:  make(map[string]*multipartUpload),
		uploadID: new(big.Int),
	}
}

func (u *uploader) Begin(bucket, object string, meta map[string]string) *multipartUpload {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.uploadID.Add(u.uploadID, add1)

	mpu := &multipartUpload{
		ID:     u.uploadID.String(),
		Bucket: bucket,
		Object: object,
		Meta:   meta,
		parts:  make(map[string]multipartUploadPart),
	}

	u.uploads[mpu.ID] = mpu
	return mpu
}

func (u *uploader) Complete(bucket, object, id string) (*multipartUpload, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	up, err := u.getUnlocked(bucket, object, id)
	if err != nil {
		return nil, err
	}
	delete(u.uploads, id)
	return up, nil
}

func (u *uploader) Get(bucket, object, id string) (mu *multipartUpload, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.getUnlocked(bucket, object, id)
}

func (u *uploader) getUnlocked(bucket, object, id string) (mu *multipartUpload, err error) {
	mu, ok := u.uploads[id]
	if !ok {
		return nil, ErrNoSuchUpload
	}

	if mu.Bucket != bucket || mu.Object != object {
		// FIXME: investigate what AWS does here; essentially if you initiate a
		// multipart upload at '/ObjectName1?uploads', then complete the upload
		// at '/ObjectName2?uploads', what happens?
		return nil, ErrNoSuchUpload
	}

	return mu, nil
}
