package gofakes3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
)

// hashingReader proxies an existing io.Reader, passing each read block to the
// given hash.Hash. Once the underlying reader returns EOF, the hash is checked.
type hashingReader struct {
	inner    io.Reader
	expected []byte
	hash     hash.Hash
}

func newHashingReader(inner io.Reader, expectedMD5Base64 string) (*hashingReader, error) {
	md5Bytes, err := base64.StdEncoding.DecodeString(expectedMD5Base64)
	if err != nil {
		return nil, ErrInvalidDigest
	}

	return &hashingReader{
		inner:    inner,
		expected: md5Bytes,
		hash:     md5.New(),
	}, nil
}

func (h *hashingReader) Read(p []byte) (n int, err error) {
	n, err = h.inner.Read(p)

	if n != 0 {
		wn, _ := h.hash.Write(p[:n]) // Hash.Write never returns an error.
		if wn != n {
			return n, fmt.Errorf("short write to hasher")
		}
	}

	if err != nil {
		if err == io.EOF {
			hash := h.hash.Sum(nil)
			if !bytes.Equal(hash, h.expected) {
				// FIXME: some more context here would be useful; need to flush out
				// what S3 responds with in this case.
				return n, ErrBadDigest
			}
		}
		return n, err
	}

	return n, nil
}
