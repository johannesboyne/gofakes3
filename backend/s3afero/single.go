package s3afero

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
	"github.com/spf13/afero"
)

// SingleBucketBackend is a gofakes3.Backend that allows you to treat an existing
// filesystem as an S3 bucket directly. It does not support multiple buckets.
//
// A second afero.Fs, metaFs, may be passed; if this is nil,
// afero.NewMemMapFs() is used and the metadata will not persist between
// restarts of gofakes3.
//
// It is STRONGLY recommended that the metadata Fs is not contained within the
// `/buckets` subdirectory as that could make a significant mess, but this is
// infeasible to validate, so you're encouraged to be extremely careful!
//
type SingleBucketBackend struct {
	lock      sync.Mutex
	fs        afero.Fs
	metaStore *metaStore
	name      string
}

var _ gofakes3.Backend = &SingleBucketBackend{}

func SingleBucket(name string, fs afero.Fs, metaFs afero.Fs, opts ...SingleOption) (*SingleBucketBackend, error) {
	if err := ensureNoOsFs("fs", fs); err != nil {
		return nil, err
	}

	if metaFs == nil {
		metaFs = afero.NewMemMapFs()
	} else {
		if err := ensureNoOsFs("metaFs", metaFs); err != nil {
			return nil, err
		}
	}

	if err := gofakes3.ValidateBucketName(name); err != nil {
		return nil, err
	}

	b := &SingleBucketBackend{
		name:      name,
		fs:        fs,
		metaStore: newMetaStore(metaFs, modTimeFsCalc(fs)),
	}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (db *SingleBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	var created time.Time

	stat, err := db.fs.Stat("")
	if os.IsNotExist(err) {
		created = time.Now()
	} else if err != nil {
		return nil, err
	} else {
		created = stat.ModTime()
	}

	// FIXME: "birth time" is not available cross-platform.
	// See MultiBucketBackend.ListBuckets for more details.
	return []gofakes3.BucketInfo{
		{Name: db.name, CreationDate: gofakes3.NewContentTime(created)},
	}, nil
}

func (db *SingleBucketBackend) ListBucket(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if bucket != db.name {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if prefix == nil {
		prefix = emptyPrefix
	}
	if !page.IsEmpty() {
		return nil, gofakes3.ErrInternalPageNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	path, part, ok := prefix.FilePrefix()
	if ok {
		return db.getBucketWithFilePrefixLocked(bucket, path, part)
	} else {
		return db.getBucketWithArbitraryPrefixLocked(bucket, prefix)
	}
}

func (db *SingleBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string) (*gofakes3.ObjectList, error) {
	dirEntries, err := afero.ReadDir(db.fs, filepath.FromSlash(prefixPath))
	if err != nil {
		return nil, err
	}

	response := gofakes3.NewObjectList()

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		if entry.IsDir() {
			response.AddPrefix(path.Join(prefixPath, prefixPart))

		} else {
			size := entry.Size()
			mtime := entry.ModTime()

			meta, err := db.metaStore.loadMeta(bucket, objectPath, size, mtime)
			if err != nil {
				return nil, err
			}

			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         `"` + hex.EncodeToString(meta.Hash) + `"`,
				Size:         size,
			})
		}
	}

	return response, nil
}

func (db *SingleBucketBackend) getBucketWithArbitraryPrefixLocked(bucket string, prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {
	response := gofakes3.NewObjectList()

	if err := afero.Walk(db.fs, filepath.FromSlash(bucket), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		objectPath := filepath.ToSlash(path)
		parts := strings.SplitN(objectPath, "/", 2)
		if len(parts) != 2 {
			panic(fmt.Errorf("unexpected path %q", path)) // should never happen
		}
		objectName := parts[1]

		if !prefix.Match(objectName, nil) {
			return nil
		}

		size := info.Size()
		mtime := info.ModTime()
		meta, err := db.metaStore.loadMeta(bucket, objectName, size, mtime)
		if err != nil {
			return err
		}

		response.Add(&gofakes3.Content{
			Key:          objectName,
			LastModified: gofakes3.NewContentTime(mtime),
			ETag:         `"` + hex.EncodeToString(meta.Hash) + `"`,
			Size:         size,
		})

		return nil

	}); err != nil {
		return nil, err
	}

	return response, nil
}

func (db *SingleBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	stat, err := db.fs.Stat(filepath.FromSlash(objectName))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	size, mtime := stat.Size(), stat.ModTime()

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name:     objectName,
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: s3io.NoOpReadCloser{},
	}, nil
}

func (db *SingleBucketBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, err error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	f, err := db.fs.Open(filepath.FromSlash(objectName))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if err != nil && obj == nil {
			f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size, mtime := stat.Size(), stat.ModTime()

	var rdr io.ReadCloser = f
	rnge, err := rangeRequest.Range(size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := f.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, f.Close, rnge.Length)
	}

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name:     objectName,
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Range:    rnge,
		Contents: rdr,
	}, nil
}

func (db *SingleBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {

	if bucketName != db.name {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	objectFilePath := filepath.FromSlash(objectName)
	objectDir := filepath.Dir(objectFilePath)

	if objectDir != "." {
		if err := db.fs.MkdirAll(objectDir, 0777); err != nil {
			return result, err
		}
	}

	f, err := db.fs.Create(objectFilePath)
	if err != nil {
		return result, err
	}

	var closed bool
	defer func() {
		// Unfortunately, afero's MemMapFs updates the mtime if you double-close, which
		// highlights that other afero.Fs implementations may have side effects here::
		if !closed {
			f.Close()
		}
	}()

	hasher := md5.New()
	w := io.MultiWriter(f, hasher)
	if _, err := io.Copy(w, input); err != nil {
		return result, err
	}

	// We have to close here before we stat the file as some filesystems don't update the
	// mtime until after close:
	if err := f.Close(); err != nil {
		return result, err
	}

	closed = true

	stat, err := db.fs.Stat(objectFilePath)
	if err != nil {
		return result, err
	}

	storedMeta := &Metadata{
		File:    objectName,
		Hash:    hasher.Sum(nil),
		Meta:    meta,
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}
	if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), storedMeta); err != nil {
		return result, err
	}

	return result, nil
}

func (db *SingleBucketBackend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	if bucketName != db.name {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	for _, object := range objects {
		if err := db.deleteObjectLocked(bucketName, object); err != nil {
			log.Println("delete object failed:", err)
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
				Key:     object,
			})
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

func (db *SingleBucketBackend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	if bucketName != db.name {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	return result, db.deleteObjectLocked(bucketName, objectName)
}

func (db *SingleBucketBackend) deleteObjectLocked(bucketName, objectName string) error {
	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.fs.Remove(filepath.FromSlash(objectName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := db.metaStore.deleteMeta(db.metaStore.metaPath(bucketName, objectName)); err != nil {
		return err
	}

	return nil
}

// CreateBucket cannot be implemented by this backend. See MultiBucketBackend if you
// need a backend that supports it.
func (db *SingleBucketBackend) CreateBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// DeleteBucket cannot be implemented by this backend. See MultiBucketBackend if you
// need a backend that supports it.
func (db *SingleBucketBackend) DeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

func (db *SingleBucketBackend) BucketExists(name string) (exists bool, err error) {
	return db.name == name, nil
}
