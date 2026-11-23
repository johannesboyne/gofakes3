package s3afero

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/spf13/afero"

	"github.com/johannesboyne/gofakes3"
)

// Copying an object onto itself is how clients change its metadata. On the OS
// file system, writing the destination empties the source before it is read;
// MemMapFs does not show this.
func TestCopyObjectOntoItself(t *testing.T) {
	for _, tc := range []struct {
		name string
		fs   func(t *testing.T) afero.Fs
	}{
		{"mem", func(t *testing.T) afero.Fs { return afero.NewMemMapFs() }},
		{"os", func(t *testing.T) afero.Fs {
			fs, err := FsPath(t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			return fs
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			single, err := SingleBucket("test", tc.fs(t), nil)
			if err != nil {
				t.Fatal(err)
			}
			multi, err := MultiBucket(tc.fs(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := multi.CreateBucket("test"); err != nil {
				t.Fatal(err)
			}

			for _, backend := range []gofakes3.Backend{single, multi} {
				contents := []byte("contents")
				if _, err := backend.PutObject("test", "yep", map[string]string{"foo": "bar"}, bytes.NewReader(contents), int64(len(contents)), nil); err != nil {
					t.Fatal(err)
				}

				meta := map[string]string{"foo": "baz"}
				if _, err := backend.CopyObject("test", "yep", "test", "yep", meta); err != nil {
					t.Fatal(err)
				}

				obj, err := backend.GetObject("test", "yep", nil)
				if err != nil {
					t.Fatal(err)
				}
				result, err := io.ReadAll(obj.Contents)
				obj.Contents.Close()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(result, contents) {
					t.Fatalf("%T: contents after copying onto itself: %q, want %q", backend, result, contents)
				}
				if !reflect.DeepEqual(obj.Metadata, meta) {
					t.Fatalf("%T: metadata after copying onto itself: %v, want %v", backend, obj.Metadata, meta)
				}
			}
		})
	}
}
