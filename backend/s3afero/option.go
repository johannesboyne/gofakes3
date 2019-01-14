package s3afero

import (
	"github.com/spf13/afero"
)

type MultiOption func(b *MultiBucketBackend) error

func MultiWithMetaFs(fs afero.Fs) MultiOption {
	return func(b *MultiBucketBackend) error {
		if err := ensureNoOsFs("MultiWithMetaFs", fs); err != nil {
			return err
		}
		b.configOnly.metaFs = fs
		return nil
	}
}

type SingleOption func(b *SingleBucketBackend) error
