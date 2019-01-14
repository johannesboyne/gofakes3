package s3afero

import (
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"
)

type Metadata struct {
	File    string
	ModTime time.Time
	Size    int64
	Hash    []byte
	Meta    map[string]string
}

type metaPath struct {
	bucket string
	object string
}

func (mp metaPath) FilePath() string {
	return filepath.Join(mp.bucket, mp.object)
}

type metaStore struct {
	fs afero.Fs
}

func newMetaStore(fs afero.Fs) *metaStore {
	b := &metaStore{
		fs: fs,
	}
	return b
}

func (ms *metaStore) metaPath(bucket string, object string) metaPath {
	// FIXME: may need to add path segments but that may be a thing of the past:
	// https://stackoverflow.com/questions/466521/how-many-files-can-i-put-in-a-directory
	h := fnv.New128a()
	h.Write([]byte(object))
	object = strings.Replace(object, "/", "_", -1)
	object = strings.Replace(object, "\\", "_", -1)

	return metaPath{bucket, object + "-" + hex.EncodeToString(h.Sum(nil))}
}

func (ms *metaStore) loadMeta(bucket string, object string, size int64, mtime time.Time) (*Metadata, error) {
	metaPath := ms.metaPath(bucket, object)
	fullPath := metaPath.FilePath()

	bts, err := afero.ReadFile(ms.fs, fullPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var meta Metadata
	if len(bts) > 0 {
		if err := json.Unmarshal(bts, &meta); err != nil {
			return nil, err
		}
	}

	// FIXME: modification time resolution
	if len(meta.Hash) == 0 || meta.Size != size || !mtime.Equal(meta.ModTime) {
		meta.Size = size
		meta.ModTime = mtime
		meta.Hash, err = hashFile(ms.fs, fullPath)
		if err != nil {
			return nil, err
		}
		if err := ms.saveMeta(metaPath, &meta); err != nil {
			return nil, err
		}
	}

	return &meta, nil
}

func (ms *metaStore) saveMeta(path metaPath, meta *Metadata) error {
	bts, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	// Don't care if this fails; it probably already exists (but may not)
	ms.fs.Mkdir(filepath.Dir(path.FilePath()), 0777)

	return afero.WriteFile(ms.fs, path.FilePath(), bts, 0666)
}

func (ms *metaStore) deleteMeta(path metaPath) error {
	if err := ms.fs.Remove(path.FilePath()); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

func (ms *metaStore) deleteBucket(bucket string) error {
	if err := ms.fs.RemoveAll(bucket); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}
