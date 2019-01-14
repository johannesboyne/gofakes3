package s3afero

import (
	"crypto/md5"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/johannesboyne/gofakes3"
	"github.com/spf13/afero"
)

type MultiBucketBackend struct {
	lock      sync.Mutex
	baseFs    afero.Fs
	bucketFs  afero.Fs
	metaStore *metaStore

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
	}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	if b.configOnly.metaFs == nil {
		b.configOnly.metaFs = afero.NewBasePathFs(fs, "metadata")
	}
	b.metaStore = newMetaStore(b.configOnly.metaFs)

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

func (db *MultiBucketBackend) GetBucket(bucket string, prefix gofakes3.Prefix) (*gofakes3.Bucket, error) {
	if err := gofakes3.ValidateBucketName(bucket); err != nil {
		return nil, gofakes3.BucketNotFound(bucket)
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

func (db *MultiBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string) (*gofakes3.Bucket, error) {
	dirEntries, err := afero.ReadDir(db.bucketFs, filepath.FromSlash(prefixPath))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucket)
	} else if err != nil {
		return nil, err
	}

	response := gofakes3.NewBucket(bucket)

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		size := entry.Size()
		mtime := entry.ModTime()

		meta, err := db.metaStore.loadMeta(bucket, objectPath, size, mtime)
		if err != nil {
			return nil, err
		}

		if entry.IsDir() {
			response.AddPrefix(path.Join(prefixPath, prefixPart))
		} else {
			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         `"` + string(meta.Hash) + `"`,
				Size:         size,
			})
		}
	}

	return response, nil
}

func (db *MultiBucketBackend) getBucketWithArbitraryPrefixLocked(name string, prefix gofakes3.Prefix) (*gofakes3.Bucket, error) {
	// FIXME: implement
	return nil, gofakes3.ErrNotImplemented
}

func (db *MultiBucketBackend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if err := db.bucketFs.Mkdir(name, 0600); os.IsNotExist(err) {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	} else {
		return err
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

	meta, err := db.metaStore.loadMeta(bucketName, fullPath, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: noOpReadCloser{},
	}, nil
}

func (db *MultiBucketBackend) GetObject(bucketName, objectName string) (*gofakes3.Object, error) {
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

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size, mtime := stat.Size(), stat.ModTime()

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: f,
	}, nil
}

func (db *MultiBucketBackend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return err
	} else if !exists {
		return gofakes3.BucketNotFound(bucketName)
	}

	objectPath := path.Join(bucketName, objectName)
	objectFilePath := filepath.FromSlash(objectPath)
	objectDir := filepath.Dir(objectFilePath)

	if objectDir != "." {
		if err := db.bucketFs.MkdirAll(objectDir, 0777); err != nil {
			return err
		}
	}

	f, err := db.bucketFs.Create(objectFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	hasher := md5.New()
	w := io.MultiWriter(f, hasher)
	if _, err := io.Copy(w, input); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	stat, err := db.bucketFs.Stat(objectFilePath)
	if err != nil {
		return err
	}

	storedMeta := &Metadata{
		File:    objectPath,
		Hash:    hasher.Sum(nil),
		Meta:    meta,
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}
	if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), storedMeta); err != nil {
		return err
	}

	return nil
}

func (db *MultiBucketBackend) DeleteObject(bucketName, objectName string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return err
	} else if !exists {
		return gofakes3.BucketNotFound(bucketName)
	}

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
