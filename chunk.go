package gofakes3

import (
	"fmt"
	"io"
)

type chunkedReader struct {
	inner         io.Reader
	chunkRemain   int
	notFirstChunk bool
}

func newChunkedReader(inner io.Reader) *chunkedReader {
	return &chunkedReader{
		inner:         inner,
		chunkRemain:   0,
		notFirstChunk: false,
	}
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {
	sizeToRead := len(p)
	for sizeToRead > 0 {
		if r.chunkRemain > sizeToRead {
			innerN, err := r.inner.Read(p[n : n+sizeToRead])
			r.chunkRemain -= innerN
			sizeToRead -= innerN
			n += innerN
			if err != nil {
				return n, err
			}
		} else if r.chunkRemain > 0 {
			innerN, err := r.inner.Read(p[n : n+r.chunkRemain])
			r.chunkRemain -= innerN
			n += innerN
			sizeToRead -= innerN
			if err != nil {
				return n, err
			}
		} else {
			if !r.notFirstChunk {
				r.notFirstChunk = true
			} else {
				// skip last chunk's b"\r\n"
				_, err = io.CopyN(io.Discard, r.inner, 2)
				if err != nil {
					return n, err
				}
			}
			// read next chunk header
			chunkSize := 0
			_, err = fmt.Fscanf(r.inner, "%x;", &chunkSize)
			if err != nil {
				return n, err
			}
			r.chunkRemain = chunkSize
			if chunkSize == 0 {
				return n, io.EOF
			}
			_, err = io.CopyN(io.Discard, r.inner, 16+64+2) // "chunk-signature=" + sizeOfHash + "\r\n"
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}
