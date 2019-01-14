package s3afero

import (
	"fmt"

	"github.com/spf13/afero"
)

func ensureNoOsFs(name string, fs afero.Fs) error {
	if _, ok := fs.(*afero.OsFs); ok {
		return fmt.Errorf("gofakes3: invalid OsFs passed to %s,. s3afero backends assume they have control over the filesystem's root. use afero.NewBasePathFs() to avoid misery", name)
	}
	return nil
}
