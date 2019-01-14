package s3afero

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/afero"
)

var modBaseTime = time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)

type modTimeCalc func() (time.Duration, error)

func modTimeFsCalc(fs afero.Fs) modTimeCalc {
	return func() (time.Duration, error) {
		return modTimeResolution(fs)
	}
}

// modTimeResolution returns a best-effort guess at the resolution of the file
// modification time for a given afero.Fs.
func modTimeResolution(fs afero.Fs) (dur time.Duration, rerr error) {
	name := ".modtime-resolution"
	tf, err := fs.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return 0, err
	}
	defer fs.Remove(name)

	if err := tf.Close(); err != nil {
		return 0, err
	}

	modEqual := func(dur time.Duration) (equal bool, err error) {
		if err := fs.Chtimes(name, modBaseTime, modBaseTime); err != nil {
			return false, err
		}

		var before, after time.Time

		if st, err := fs.Stat(name); err != nil {
			return false, err
		} else {
			before = st.ModTime()
		}

		if !before.Equal(modBaseTime) {
			return false, fmt.Errorf("cannot set time to base time")
		}

		checkTime := modBaseTime.Add(dur)
		if err := fs.Chtimes(name, checkTime, checkTime); err != nil {
			return false, err
		}

		if st, err := fs.Stat(name); err != nil {
			return false, err
		} else {
			after = st.ModTime()
		}

		return after.Sub(before) == dur, nil
	}

	// FIXME(bw): I was writing a fancy algorithm to search for this, but it's trickier
	// than it first appears. My first attempt was to simply set the mod time to
	// something known to be representable on all the filesystems we support,
	// subtracting 1ns and seeing what that rounds down to... works fine for NTFS
	// but FAT32 rounds up in this situation!
	for _, dur := range []time.Duration{
		1 * time.Nanosecond, // ext4, APFS
		10 * time.Nanosecond,
		100 * time.Nanosecond, // NTFS
		1 * time.Microsecond,
		10 * time.Microsecond,
		100 * time.Microsecond,
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		1 * time.Second, // HFS+
		2 * time.Second, // FAT32
	} {
		if eq, err := modEqual(dur); err != nil {
			return 0, err
		} else if eq {
			return dur, nil
		}
	}

	return 0, fmt.Errorf("gofakes3: could not profile modtime resolution for filesystem")
}
