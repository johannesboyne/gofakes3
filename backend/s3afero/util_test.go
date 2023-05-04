package s3afero

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFsPathCreate(t *testing.T) {
	t.Run("default-fails", func(t *testing.T) {
		d := filepath.Join(t.TempDir(), "fs")
		if _, err := FsPath(d, 0); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("expected not exist error, found", err)
		}
	})

	t.Run("create", func(t *testing.T) {
		d := filepath.Join(t.TempDir(), "fs")
		if _, err := FsPath(d, FsPathCreate); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(d); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("create-nested-fails", func(t *testing.T) {
		d := filepath.Join(t.TempDir(), "fs", "nup")
		if _, err := FsPath(d, 0); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("expected not exist error, found", err)
		}
	})

	t.Run("create-all", func(t *testing.T) {
		d := filepath.Join(t.TempDir(), "fs", "yep")
		if _, err := FsPath(d, FsPathCreateAll); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(d); err != nil {
			t.Fatal(err)
		}
	})
}
