package s3afero

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

type noOpReadCloser struct{}

func (d noOpReadCloser) Read(b []byte) (n int, err error) { return 0, io.EOF }

func (d noOpReadCloser) Close() error { return nil }

// FsPath returns an afero.Fs rooted to the path provided.
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
