package gofakes3

import (
	"io"
	"io/ioutil"
	"strconv"
)

func parseClampedInt(in string, defaultValue, min, max int64) (int64, error) {
	var v int64
	if in == "" {
		v = defaultValue
	} else {
		var err error
		v, err = strconv.ParseInt(in, 10, 0)
		if err != nil {
			return defaultValue, ErrInvalidArgument
		}
	}

	if v < min {
		v = min
	} else if v > max {
		v = max
	}

	return v, nil
}

// ReadAll is a fakeS3-centric replacement for ioutil.ReadAll(), for use when
// the size of the result is known ahead of time. It is considerably faster to
// preallocate the entire slice than to allow growslice to be triggered
// repeatedly, especially with larger buffers.
//
// It also reports S3-specific errors in certain conditions, like
// ErrIncompleteBody.
func ReadAll(r io.Reader, size int64) (b []byte, err error) {
	var n int
	b = make([]byte, size)
	n, err = io.ReadFull(r, b)
	if err == io.ErrUnexpectedEOF {
		return nil, ErrIncompleteBody
	} else if err != nil {
		return nil, err
	}

	if n != int(size) {
		return nil, ErrIncompleteBody
	}

	if extra, err := ioutil.ReadAll(r); err != nil {
		return nil, err
	} else if len(extra) > 0 {
		return nil, ErrIncompleteBody
	}

	return b, nil
}

// MultiReadCloser is a fakeS3-centric replacement for io.MultiReader() which
// includes a closing mechanism where supported on the inputs.
type MultiReadCloser struct {
	sources []io.ReadCloser
}

func (mrc *MultiReadCloser) Read(p []byte) (n int, err error) {
	var count = len(mrc.sources)
	for current, source := range mrc.sources {
		n, err = source.Read(p)
		if n > 0 || err != io.EOF {
			if err == io.EOF && current < (count-1) {
				err = nil
			}
			return
		}
	}
	return
}

func (mrc *MultiReadCloser) Close() (err error) {
	for _, source := range mrc.sources {
		// mrc.sources could contain nil values.
		if closer, ok := source.(io.Closer); ok {
			err = closer.Close()
			if err != nil {
				return
			}
		}
	}
	return
}

func NewMultiReadCloser(r ...io.ReadCloser) (rc io.ReadCloser) {
	return &MultiReadCloser{sources: r}
}
