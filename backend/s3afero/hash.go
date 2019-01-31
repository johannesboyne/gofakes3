package s3afero

import (
	"crypto/md5"
	"io"
	"path/filepath"

	"github.com/spf13/afero"
)

func hashFile(fs afero.Fs, path string) (hash []byte, err error) {
	f, err := fs.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := md5.New()
	io.Copy(h, f)
	return h.Sum(nil), nil
}
