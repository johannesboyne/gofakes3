package gofakes3

import (
	"bufio"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/bits"
	"strings"
)

// isStreamingPayload reports whether the X-Amz-Content-Sha256 value announces
// an aws-chunked body. Newer SDKs/CLIs (default checksum behaviour) send the
// -TRAILER variants, which append a checksum trailer after the final chunk.
func isStreamingPayload(sha string) bool {
	switch sha {
	case "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
		"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
		"STREAMING-UNSIGNED-PAYLOAD-TRAILER":
		return true
	}
	return false
}

var errChunkHeader = errors.New("expected integer")

// chunkedReader decodes an aws-chunked body:
//
//		hex-size[;chunk-signature=...]\r\n data \r\n ... 0[;chunk-signature=...]\r\n
//		[trailer-name:value\r\n]* \r\n
//
//	Checksum trailers (x-amz-checksum-*) are verified against the decoded data if the reader
//	was told to expect them using verifyTrailer; ErrBadDigest is returned from the
//	Read that reaches the end of the body if they do not match.
type chunkedReader struct {
	inner       *bufio.Reader
	chunkRemain int
	started     bool
	done        bool
	err         error

	// checksums holds a hasher for each checksum announced in X-Amz-Trailer,
	// keyed by lower-cased trailer name.
	checksums map[string]hash.Hash
}

// crc64NVMEPoly is the CRC-64/NVME polynomial as published in the NVM Express
// NVM Command Set Specification (section 5.2.1.3.4), which S3 uses for its
// default CRC64NVME checksum. See
// https://reveng.sourceforge.io/crc-catalogue/17plus.htm (CRC-64/NVME).
const crc64NVMEPoly = 0xad93d23594c93659

var (
	crc64NVMETable = crc64.MakeTable(bits.Reverse64(crc64NVMEPoly))
	crc32cTable    = crc32.MakeTable(crc32.Castagnoli)
)

func newChecksumHash(trailerName string) hash.Hash {
	switch trailerName {
	case "x-amz-checksum-crc32":
		return crc32.NewIEEE()
	case "x-amz-checksum-crc32c":
		return crc32.New(crc32cTable)
	case "x-amz-checksum-crc64nvme":
		return crc64.New(crc64NVMETable)
	case "x-amz-checksum-sha1":
		return sha1.New()
	case "x-amz-checksum-sha256":
		return sha256.New()
	}
	return nil
}

// verifyTrailer enables checking of the checksum trailers named in the value
// of the X-Amz-Trailer header (comma separated). Names that are not a
// supported checksum, such as x-amz-trailer-signature, are ignored.
func (r *chunkedReader) verifyTrailer(trailerHeader string) *chunkedReader {
	for _, name := range strings.Split(trailerHeader, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if h := newChecksumHash(name); h != nil {
			if r.checksums == nil {
				r.checksums = map[string]hash.Hash{}
			}
			r.checksums[name] = h
		}
	}
	return r
}

func newChunkedReader(inner io.Reader) *chunkedReader {
	return &chunkedReader{inner: bufio.NewReader(inner)}
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err = r.read(p)
	if err != nil {
		r.err = err
	}
	return n, err
}

func (r *chunkedReader) read(p []byte) (n int, err error) {
	for n < len(p) {
		if r.done {
			return n, io.EOF
		}
		if r.chunkRemain > 0 {
			sizeToRead := len(p) - n
			if sizeToRead > r.chunkRemain {
				sizeToRead = r.chunkRemain
			}
			innerN, err := r.inner.Read(p[n : n+sizeToRead])
			for _, h := range r.checksums {
				h.Write(p[n : n+innerN])
			}
			r.chunkRemain -= innerN
			n += innerN
			if err != nil {
				return n, err
			}
			continue
		}

		if r.started {
			// skip the CRLF that terminates the previous chunk's data
			if _, err := r.inner.Discard(2); err != nil {
				return n, err
			}
		}
		r.started = true

		size, err := r.readChunkHeader()
		if err != nil {
			return n, err
		}
		if size == 0 {
			if err := r.skipTrailers(); err != nil {
				return n, err
			}
			r.done = true
			return n, io.EOF
		}
		r.chunkRemain = size
	}
	return n, nil
}

// readChunkHeader reads "hex-size[;extensions]\r\n" and returns the size.
func (r *chunkedReader) readChunkHeader() (int, error) {
	var size, digits int = 0, 0

	for {
		b, err := r.inner.ReadByte()
		if err != nil {
			return 0, err
		}
		var v int
		switch {
		case b >= '0' && b <= '9':
			v = int(b - '0')
		case b >= 'a' && b <= 'f':
			v = int(b-'a') + 10
		case b >= 'A' && b <= 'F':
			v = int(b-'A') + 10
		case digits > 0 && (b == ';' || b == '\r'):
			// skip chunk-signature or other extensions up to the newline
			if b == '\r' {
				if err := r.inner.UnreadByte(); err != nil {
					return 0, err
				}
			}

			// skip the rest of the line (extensions) and the CRLF that terminates the chunk header
			if _, err := r.inner.ReadString('\n'); err != nil {
				return 0, err
			}
			return size, nil
		default:
			return 0, errChunkHeader
		}
		size = size<<4 | v
		digits++
		if digits > 8 {
			return 0, errChunkHeader
		}
	}
}

// skipTrailers consumes optional trailing headers (e.g.
// x-amz-checksum-crc32, x-amz-trailer-signature) up to the blank line.
func (r *chunkedReader) skipTrailers() error {
	for {
		line, err := r.inner.ReadString('\n')
		if cerr := r.checkTrailerLine(line); cerr != nil {
			return cerr
		}
		if len(line) <= 2 { // "\r\n", "\n" or empty
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// checkTrailerLine compares a "name:value" trailer line against the checksum
// computed over the decoded data, if one is being tracked for that name.
func (r *chunkedReader) checkTrailerLine(trailer string) error {
	name, value, ok := strings.Cut(strings.TrimSpace(trailer), ":")
	if !ok {
		return nil
	}
	h, ok := r.checksums[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return nil
	}
	if base64.StdEncoding.EncodeToString(h.Sum(nil)) != strings.TrimSpace(value) {
		return ErrBadDigest
	}
	return nil
}
