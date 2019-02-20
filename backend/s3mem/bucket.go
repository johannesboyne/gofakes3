package s3mem

import (
	"bytes"
	"io"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/ryszard/goskiplist/skiplist"
)

type versionGenFunc func() gofakes3.VersionID

type bucket struct {
	name         string
	versioning   bool
	versionGen   versionGenFunc
	creationDate gofakes3.ContentTime

	objects *skiplist.SkipList
}

func newBucket(name string, at time.Time, versionGen versionGenFunc) *bucket {
	return &bucket{
		name:         name,
		creationDate: gofakes3.NewContentTime(at),
		versionGen:   versionGen,
		objects:      skiplist.NewStringMap(),
	}
}

type bucketObject struct {
	name     string
	data     *bucketData
	versions *skiplist.SkipList
}

func (b *bucketObject) Iterator() *bucketObjectIterator {
	var iter skiplist.Iterator
	if b.versions != nil {
		iter = b.versions.Iterator()
	}

	return &bucketObjectIterator{
		data: b.data,
		iter: iter,
	}
}

type bucketObjectIterator struct {
	data     *bucketData
	iter     skiplist.Iterator
	cur      *bucketData
	seenData bool
	done     bool
}

func (b *bucketObjectIterator) Seek(key gofakes3.VersionID) bool {
	if b.iter.Seek(key) {
		return true
	}

	b.iter = nil
	if b.data != nil && b.data.versionID == key {
		return true
	}

	b.data = nil
	b.done = true

	return false
}

func (b *bucketObjectIterator) Next() bool {
	if b.done {
		return false
	}

	if b.iter != nil {
		iterAlive := b.iter.Next()
		if iterAlive {
			b.cur = b.iter.Value().(*bucketData)
			return true
		}

		b.iter.Close()
		b.iter = nil
	}

	if b.data != nil {
		b.cur = b.data
		b.data = nil
		return true
	}

	b.done = true
	return false
}

func (b *bucketObjectIterator) Close() {
	if b.iter != nil {
		b.iter.Close()
	}
	b.done = true
}

func (b *bucketObjectIterator) Value() *bucketData {
	return b.cur
}

type bucketData struct {
	name         string
	lastModified time.Time
	versionID    gofakes3.VersionID
	deleteMarker bool
	body         []byte
	hash         []byte
	etag         string
	metadata     map[string]string
}

func (bi *bucketData) toObject(rangeRequest *gofakes3.ObjectRangeRequest, withBody bool) *gofakes3.Object {
	sz := int64(len(bi.body))
	data := bi.body

	var contents io.ReadCloser
	var rnge *gofakes3.ObjectRange

	if withBody {
		rnge = rangeRequest.Range(sz)
		if rnge != nil {
			data = data[rnge.Start : rnge.Start+rnge.Length]
		}
		// The data slice should be completely replaced if the bucket item is edited, so
		// it should be safe to return the data slice directly.
		contents = readerWithDummyCloser{bytes.NewReader(data)}

	} else {
		contents = noOpReadCloser{}
	}

	return &gofakes3.Object{
		Name:           bi.name,
		Hash:           bi.hash,
		Metadata:       bi.metadata,
		Size:           sz,
		Range:          rnge,
		IsDeleteMarker: bi.deleteMarker,
		VersionID:      bi.versionID,
		Contents:       contents,
	}
}

func (b *bucket) setVersioning(enabled bool) {
	b.versioning = enabled
}

func (b *bucket) object(objectName string) (obj *bucketObject) {
	objIface, _ := b.objects.Get(objectName)
	if objIface == nil {
		return nil
	}
	obj, _ = objIface.(*bucketObject)
	return obj
}

func (b *bucket) objectVersion(objectName string, versionID gofakes3.VersionID) (*bucketData, error) {
	obj := b.object(objectName)
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	if obj.data != nil && obj.data.versionID == versionID {
		return obj.data, nil
	}
	if obj.versions == nil {
		return nil, gofakes3.ErrNoSuchVersion
	}
	versionIface, _ := obj.versions.Get(versionID)
	if versionIface == nil {
		return nil, gofakes3.ErrNoSuchVersion
	}

	return versionIface.(*bucketData), nil
}

func (b *bucket) put(name string, item *bucketData) {
	// Always generate a version for convenience; we can just mask it on return.
	item.versionID = b.versionGen()

	object := b.object(name)
	if object == nil {
		object = &bucketObject{name: name}
		b.objects.Set(name, object)
	}

	if b.versioning {
		if object.data != nil {
			if object.versions == nil {
				object.versions = skiplist.NewCustomMap(func(l, r interface{}) bool {
					return l.(gofakes3.VersionID) < r.(gofakes3.VersionID)
				})
			}
			object.versions.Set(object.data.versionID, object.data)
		}
	}

	object.data = item
}

func (b *bucket) rm(name string, at time.Time) (result gofakes3.ObjectDeleteResult, rerr error) {
	object := b.object(name)
	if object == nil {
		// S3 does not report an error when attemping to delete a key that does not exist
		return result, nil
	}

	if b.versioning {
		item := &bucketData{lastModified: at, name: name, deleteMarker: true}
		b.put(name, item)
		result.IsDeleteMarker = true
		result.VersionID = item.versionID

	} else {
		object.data = nil
		if object.versions == nil || object.versions.Len() == 0 {
			b.objects.Delete(name)
		}
	}

	return result, nil
}

func (b *bucket) rmVersion(name string, versionID gofakes3.VersionID, at time.Time) (result gofakes3.ObjectDeleteResult, rerr error) {
	object := b.object(name)
	if object.data != nil && object.data.versionID == versionID {
		result.VersionID = versionID
		result.IsDeleteMarker = object.data.deleteMarker
		object.data = nil
		return
	}

	if object.versions == nil {
		return result, nil
	}

	versionIface, ok := object.versions.Delete(versionID)
	if !ok {
		// S3 does not report an error when attemping to delete a key that does not exist
		return result, nil
	}

	version := versionIface.(*bucketData)
	result.VersionID = version.versionID
	result.IsDeleteMarker = version.deleteMarker

	return result, nil
}
