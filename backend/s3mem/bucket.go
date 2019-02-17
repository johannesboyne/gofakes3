package s3mem

import (
	"bytes"
	"fmt"
	"time"

	"github.com/johannesboyne/gofakes3"
)

type bucketItem struct {
	name         string
	lastModified time.Time
	versionID    gofakes3.VersionID
	deleteMarker bool
	data         []byte
	hash         []byte
	metadata     map[string]string
}

func (bi *bucketItem) toObject(rangeRequest *gofakes3.ObjectRangeRequest) *gofakes3.Object {
	sz := int64(len(bi.data))
	data := bi.data

	rnge := rangeRequest.Range(sz)
	if rnge != nil {
		data = data[rnge.Start : rnge.Start+rnge.Length]
	}

	return &gofakes3.Object{
		Name:           bi.name,
		Hash:           bi.hash,
		Metadata:       bi.metadata,
		Size:           sz,
		Range:          rnge,
		IsDeleteMarker: bi.deleteMarker,
		VersionID:      bi.versionID,

		// The data slice should be completely replaced if the bucket item is edited, so
		// it should be safe to return the data slice directly.
		Contents: readerWithDummyCloser{bytes.NewReader(data)},
	}
}

type bucket struct {
	name         string
	versioning   bool
	versionGen   func() gofakes3.VersionID
	creationDate gofakes3.ContentTime
	data         map[string]*bucketItem

	versions     map[string][]*bucketItem
	versionIndex map[gofakes3.VersionID]versionRef
	versionGaps  int
}

type versionKey struct {
	object  string
	version gofakes3.VersionID
}

type versionRef struct {
	item  *bucketItem
	index int
}

func (b *bucket) setVersioning(enabled bool) {
	b.versioning = enabled
}

func (b *bucket) put(name string, item *bucketItem) {
	// Always generate a version for convenience; we can just mask it on return.
	item.versionID = b.versionGen()

	if b.versioning {
		past := b.data[name]
		if past != nil {
			index := len(b.versions[name])
			b.versions[name] = append(b.versions[name], past)
			if _, ok := b.versionIndex[past.versionID]; ok {
				panic(fmt.Errorf("version ID collision: %s", past.versionID))
			}
			b.versionIndex[past.versionID] = versionRef{past, index}
		}
	}

	b.data[name] = item
}

func (b *bucket) rm(name string, at time.Time) (result gofakes3.ObjectDeleteResult, rerr error) {
	item := b.data[name]
	if item == nil {
		// S3 does not report an error when attemping to delete a key that does not exist
		return result, nil
	}

	if b.versioning {
		item := &bucketItem{lastModified: at, name: name, deleteMarker: true}
		b.put(name, item)
		result.IsDeleteMarker = true
		result.VersionID = item.versionID

	} else {
		delete(b.data, name)
	}

	return result, nil
}

func (b *bucket) rmVersion(name string, versionID gofakes3.VersionID, at time.Time) (result gofakes3.ObjectDeleteResult, rerr error) {
	item := b.data[name]
	if item.versionID == versionID {
		result.VersionID = versionID
		result.IsDeleteMarker = item.deleteMarker
		item.data = nil
		return
	}

	versionRef, ok := b.versionIndex[versionID]
	if !ok {
		// S3 does not report an error when attemping to delete a key that does not exist
		return result, nil
	}

	if b.versions[name][versionRef.index] != versionRef.item {
		panic(fmt.Errorf("version %s failed sanity check", versionID))
	}

	delete(b.versionIndex, versionID)

	b.versions[name][versionRef.index] = nil
	b.versionGaps++

	// FIXME: if versionGaps > threshold, close gaps

	result.VersionID = versionRef.item.versionID
	result.IsDeleteMarker = versionRef.item.deleteMarker

	return result, nil
}
