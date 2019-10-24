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

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
	"github.com/spf13/afero"
)

// MultiBucketBackend is a gofakes3.Backend that allows you to create multiple
// buckets within the same afero.Fs. Buckets are stored under the `/buckets`
// subdirectory. Metadata is stored in the `/metadata` subdirectory by default,
// but any afero.Fs can be used.
//
// It is STRONGLY recommended that the metadata Fs is not contained within the
// `/buckets` subdirectory as that could make a significant mess, but this is
// infeasible to validate, so you're encouraged to be extremely careful!
//
type MultiBucketBackend struct {
	lock      sync.Mutex
	baseFs    afero.Fs
	bucketFs  afero.Fs
	metaStore *metaStore
	dirMode   os.FileMode

	// FIXME(bw): values in here should not be used beyond the configuration
	// step; maybe this can be cleaned up later using a builder struct or
	// something.
	configOnly struct {
		metaFs afero.Fs
	}
}

var _ gofakes3.Backend = &MultiBucketBackend{}

func MultiBucket(fs afero.Fs, opts ...MultiOption) (*MultiBucketBackend, error) {
	if err := ensureNoOsFs("fs", fs); err != nil {
		return nil, err
	}

	b := &MultiBucketBackend{
		baseFs:   fs,
		bucketFs: afero.NewBasePathFs(fs, "buckets"),
		dirMode:  0700,
	}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	if b.configOnly.metaFs == nil {
		b.configOnly.metaFs = afero.NewBasePathFs(fs, "metadata")
	}
	b.metaStore = newMetaStore(b.configOnly.metaFs, modTimeFsCalc(fs))

	return b, nil
}

func (db *MultiBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	dirEntries, err := afero.ReadDir(db.bucketFs, "")
	if err != nil {
		return nil, err
	}

	var buckets = make([]gofakes3.BucketInfo, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		if err := gofakes3.ValidateBucketName(dirEntry.Name()); err != nil {
			continue
		}

		buckets = append(buckets, gofakes3.BucketInfo{
			Name: dirEntry.Name(),

			// FIXME: "birth time" is not available cross-platform.
			// https://github.com/djherbis/times provides access to it on supported
			// platforms, but that wouldn't really be compatible with afero.
			// ModTime and some documented caveats might be the least-worst
			// option for this particular backend:
			CreationDate: gofakes3.NewContentTime(dirEntry.ModTime()),
		})
	}

	return buckets, nil
}

func (db *MultiBucketBackend) ListBucket(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if err := gofakes3.ValidateBucketName(bucket); err != nil {
		return nil, gofakes3.BucketNotFound(bucket)
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

func (db *MultiBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string) (*gofakes3.ObjectList, error) {
	bucketPath := path.Join(bucket, prefixPath)

	dirEntries, err := afero.ReadDir(db.bucketFs, filepath.FromSlash(bucketPath))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucket)
	} else if err != nil {
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

func (db *MultiBucketBackend) getBucketWithArbitraryPrefixLocked(bucket string, prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {
	stat, err := db.bucketFs.Stat(filepath.FromSlash(bucket))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucket)
	} else if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("gofakes3: expected %q to be a bucket path", bucket)
	}

	response := gofakes3.NewObjectList()

	if err := afero.Walk(db.bucketFs, filepath.FromSlash(bucket), func(path string, info os.FileInfo, err error) error {
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

func (db *MultiBucketBackend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, err := db.bucketFs.Stat(name); os.IsNotExist(err) {
		if err := db.bucketFs.MkdirAll(name, db.dirMode); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	}
}

func (db *MultiBucketBackend) DeleteBucket(name string) (rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	entries, err := afero.ReadDir(db.bucketFs, name)
	if err != nil {
		return err
	}

	if len(entries) > 0 {
		// This check is slightly racy. If another service outside gofakes3
		// changes the filesystem between this check and the call to Remove,
		// the bucket may be deleted even though there are items in it. You
		// would expect that afero.Fs would raise an error if you tried to
		// delete a directory that had stuff in it, but implementers of
		// afero.Fs may not implement that particular constraint. We have no
		// choice but to fall back on the db's lock and assume that a race
		// won't happen.
		return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
	}

	// FIXME(bw): the error handling logic here is a little janky:
	if err := db.bucketFs.RemoveAll(name); os.IsNotExist(err) {
		rerr = gofakes3.BucketNotFound(name)
	} else if err != nil {
		return err
	}

	if err := db.metaStore.deleteBucket(name); err != nil {
		return err
	}

	return rerr
}

func (db *MultiBucketBackend) BucketExists(name string) (exists bool, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	exists, err = afero.Exists(db.bucketFs, name)
	return
}

func (db *MultiBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fullPath := path.Join(bucketName, objectName)

	stat, err := db.bucketFs.Stat(filepath.FromSlash(fullPath))
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

func (db *MultiBucketBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fullPath := path.Join(bucketName, objectName)

	f, err := db.bucketFs.Open(filepath.FromSlash(fullPath))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if obj == nil && rerr != nil {
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
		Range:    rnge,
		Size:     size,
		Contents: rdr,
	}, nil
}

func (db *MultiBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {

	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	objectPath := path.Join(bucketName, objectName)
	objectFilePath := filepath.FromSlash(objectPath)
	objectDir := filepath.Dir(objectFilePath)

	if objectDir != "." {
		if err := db.bucketFs.MkdirAll(objectDir, db.dirMode); err != nil {
			return result, err
		}
	}

	f, err := db.bucketFs.Create(objectFilePath)
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

	stat, err := db.bucketFs.Stat(objectFilePath)
	if err != nil {
		return result, err
	}

	storedMeta := &Metadata{
		File:    objectPath,
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

func (db *MultiBucketBackend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	return result, db.deleteObjectLocked(bucketName, objectName)
}

func (db *MultiBucketBackend) deleteObjectLocked(bucketName, objectName string) error {
	fullPath := path.Join(bucketName, objectName)

	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.bucketFs.Remove(filepath.FromSlash(fullPath)); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := db.metaStore.deleteMeta(db.metaStore.metaPath(bucketName, objectName)); err != nil {
		return err
	}

	return nil
}

func (db *MultiBucketBackend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

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
