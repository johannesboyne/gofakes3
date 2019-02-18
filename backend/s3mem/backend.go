package s3mem

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"sync"

	"github.com/johannesboyne/gofakes3"
)

type Backend struct {
	buckets          map[string]*bucket
	timeSource       gofakes3.TimeSource
	versionGenerator *versionGenerator
	versionSeed      int64
	versionSeedSet   bool
	versionScratch   []byte
	lock             sync.Mutex
}

var _ gofakes3.Backend = &Backend{}
var _ gofakes3.VersionedBackend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource gofakes3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func WithVersionSeed(seed int64) Option {
	return func(b *Backend) { b.versionSeed = seed; b.versionSeedSet = true }
}

func New(opts ...Option) *Backend {
	b := &Backend{
		buckets: make(map[string]*bucket),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = gofakes3.DefaultTimeSource()
	}
	if b.versionGenerator == nil {
		if b.versionSeedSet {
			b.versionGenerator = newVersionGenerator(uint64(b.versionSeed), 0)
		} else {
			b.versionGenerator = newVersionGenerator(uint64(b.timeSource.Now().UnixNano()), 0)
		}
	}
	return b
}

func (db *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	var buckets = make([]gofakes3.BucketInfo, 0, len(db.buckets))
	for _, bucket := range db.buckets {
		buckets = append(buckets, gofakes3.BucketInfo{
			Name:         bucket.name,
			CreationDate: bucket.creationDate,
		})
	}

	return buckets, nil
}

func (db *Backend) ListBucket(name string, prefix gofakes3.Prefix) (*gofakes3.ListBucketResult, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	storedBucket := db.buckets[name]
	if storedBucket == nil {
		return nil, gofakes3.BucketNotFound(name)
	}

	response := gofakes3.NewListBucketResult(name)
	iter := storedBucket.objects.Iterator()

	for iter.Next() {
		item := iter.Value().(*bucketObject)

		match := prefix.Match(item.data.name)
		if match == nil {
			continue

		} else if match.CommonPrefix {
			response.AddPrefix(match.MatchedPart)

		} else {
			response.Add(&gofakes3.Content{
				Key:          item.data.name,
				LastModified: gofakes3.NewContentTime(item.data.lastModified),
				ETag:         `"` + hex.EncodeToString(item.data.hash) + `"`,
				Size:         int64(len(item.data.body)),
			})
		}
	}

	return response, nil
}

func (db *Backend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	}

	db.buckets[name] = newBucket(name, db.timeSource.Now(), db.nextVersion)
	return nil
}

func (db *Backend) DeleteBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] == nil {
		return gofakes3.ErrNoSuchBucket
	}

	if db.buckets[name].objects.Len() > 0 {
		return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
	}

	delete(db.buckets, name)

	return nil
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.buckets[name] != nil, nil
}

func (db *Backend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	return &gofakes3.Object{
		Name:     objectName,
		Hash:     obj.data.hash,
		Metadata: obj.data.metadata,
		Size:     int64(len(obj.data.body)),
		Contents: noOpReadCloser{},
	}, nil
}

func (db *Backend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		// FIXME: If the current version of the object is a delete marker,
		// Amazon S3 behaves as if the object was deleted and includes
		// x-amz-delete-marker: true in the response.
		//
		// The solution may be to return an object but no error if the object is
		// a delete marker, and let the main GoFakeS3 class decide what to do.
		return nil, gofakes3.KeyNotFound(objectName)
	}

	result := obj.data.toObject(rangeRequest)
	if bucket.versioning {
		result.VersionID = ""
	}

	return result, nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	bts, err := gofakes3.ReadAll(input, size)
	if err != nil {
		return err
	}

	hash := md5.Sum(bts)

	bucket.put(objectName, &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	})

	return nil
}

func (db *Backend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	return bucket.rm(objectName, db.timeSource.Now())
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	now := db.timeSource.Now()

	for _, object := range objects {
		dresult, err := bucket.rm(object, now)
		_ = dresult // FIXME: what to do with rm result in multi delete?

		if err != nil {
			errres := gofakes3.ErrorResultFromError(err)
			if errres.Code == gofakes3.ErrInternal {
				// FIXME: log
			}

			result.Error = append(result.Error, errres)

		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

func (db *Backend) VersioningConfiguration(bucketName string) (versioning gofakes3.VersioningConfiguration, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return versioning, gofakes3.BucketNotFound(bucketName)
	}

	versioning.SetEnabled(bucket.versioning)
	return versioning, nil
}

func (db *Backend) SetVersioningConfiguration(bucketName string, v gofakes3.VersioningConfiguration) error {
	if v.MFADelete {
		return gofakes3.ErrNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	bucket.setVersioning(v.Enabled())

	return nil
}

func (db *Backend) GetObjectVersion(
	bucketName, objectName string,
	versionID gofakes3.VersionID,
	rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {

	// FIXME: It's not clear from the docs what S3 does when versioning has
	// been enabled, then suspended, then you request a version ID that exists.
	// For now, let's presume it will return the version if it exists, even
	// if versioning is suspended.

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	if obj.data != nil && obj.data.versionID == versionID {
		return obj.data.toObject(rangeRequest), nil
	}
	if obj.versions == nil {
		return nil, gofakes3.ErrNoSuchVersion
	}
	versionIface, _ := obj.versions.Get(versionID)
	if versionIface == nil {
		return nil, gofakes3.ErrNoSuchVersion
	}

	version := versionIface.(*bucketData)
	return version.toObject(rangeRequest), nil
}

func (db *Backend) DeleteObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	return bucket.rmVersion(objectName, versionID, db.timeSource.Now())
}

func (db *Backend) ListBucketVersions(bucketName string, prefix gofakes3.Prefix) (*gofakes3.ListBucketVersionsResult, error) {
	return nil, nil
}

// nextVersion assumes the backend's lock is acquired
func (db *Backend) nextVersion() gofakes3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}

type readerWithDummyCloser struct{ io.Reader }

func (d readerWithDummyCloser) Close() error { return nil }

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }
