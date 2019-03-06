package s3afero

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/johannesboyne/gofakes3"
	"github.com/spf13/afero"
)

var emptyPrefix = &gofakes3.Prefix{}

type readerWithCloser struct {
	io.Reader
	closer func() error
}

var _ io.ReadCloser = &readerWithCloser{}

func limitReadCloser(rdr io.Reader, closer func() error, sz int64) io.ReadCloser {
	return &readerWithCloser{
		Reader: io.LimitReader(rdr, sz),
		closer: closer,
	}
}

func (rwc *readerWithCloser) Close() error {
	if rwc.closer != nil {
		return rwc.closer()
	}
	return nil
}

// ensureNoOsFs makes a best-effort attempt to ensure you haven't used
// afero.OsFs directly in any of these backends; to do so would risk exposing
// you to RemoveAll against your `/` directory.
func ensureNoOsFs(name string, fs afero.Fs) error {
	if _, ok := fs.(*afero.OsFs); ok {
		return fmt.Errorf("gofakes3: invalid OsFs passed to %s,. s3afero backends assume they have control over the filesystem's root. use afero.NewBasePathFs() to avoid misery", name)
	}
	return nil
}

// FsPath returns an afero.Fs rooted to the path provided. If the path is invalid,
// or is less than 2 levels down from the filesystem root, an error is returned.
func FsPath(path string) (afero.Fs, error) {
	if path == "" {
		return nil, fmt.Errorf("gofakes3: empty path")
	}

	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("gofakes3: path %q is not a directory", path)
	}

	parts := strings.Split(path, string(filepath.Separator))
	if len(parts) < 2 { // cheap and nasty footgun check:
		return nil, fmt.Errorf("gofakes3: invalid path %q", path)
	}

	return afero.NewBasePathFs(afero.NewOsFs(), path), nil
}
