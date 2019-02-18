package s3mem

import (
	"bytes"
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
	data     *bucketData
	versions *skiplist.SkipList
}

type bucketData struct {
	name         string
	lastModified time.Time
	versionID    gofakes3.VersionID
	deleteMarker bool
	body         []byte
	hash         []byte
	metadata     map[string]string
}

func (bi *bucketData) toObject(rangeRequest *gofakes3.ObjectRangeRequest) *gofakes3.Object {
	sz := int64(len(bi.body))
	data := bi.body

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

func (b *bucket) put(name string, item *bucketData) {
	// Always generate a version for convenience; we can just mask it on return.
	item.versionID = b.versionGen()

	object := b.object(name)
	if object == nil {
		object = &bucketObject{}
		b.objects.Set(name, object)
	}

	if b.versioning {
		if object.data != nil {
			if object.versions == nil {
				object.versions = skiplist.NewStringMap()
			}
			object.versions.Set(item.versionID, item)
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
