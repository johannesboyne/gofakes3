package gofakes3

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"hash/crc64"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkedUploadSuccess(t *testing.T) {
	// From https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html#example-signature-calculations-streaming
	// actual data is (65536 + 1024) * 'a'
	// divided into 3 chunks.

	// first chunk
	payload := "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"
	payload += strings.Repeat("a", 65536)
	payload += "\r\n"

	// second chunk
	payload += "400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n"
	payload += strings.Repeat("a", 1024)
	payload += "\r\n"

	// third chunk, with empty chunk-data representing end of request
	payload += "0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n"

	inner := strings.NewReader(payload)
	chunkedReader := newChunkedReader(inner)
	buf, err := ioutil.ReadAll(chunkedReader)
	assert.Equal(t, nil, err)
	assert.Equal(t, string(buf), strings.Repeat("a", 65536+1024))
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("err")
}

func TestChunkedUploadFail(t *testing.T) {
	chunkedReader := newChunkedReader(errReader{})
	buf, err := ioutil.ReadAll(chunkedReader)
	assert.Equal(t, errors.New("err"), err)
	assert.Equal(t, "", string(buf))

	chunkedReader = newChunkedReader(io.MultiReader(
		strings.NewReader("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"),
		errReader{},
	))
	buf, err = ioutil.ReadAll(chunkedReader)
	assert.Equal(t, errors.New("err"), err)
	assert.Equal(t, "", string(buf))

	chunkedReader = newChunkedReader(io.MultiReader(
		strings.NewReader("incorrect_data"),
		errReader{},
	))
	buf, err = ioutil.ReadAll(chunkedReader)
	assert.Equal(t, errors.New("expected integer"), err)
	assert.Equal(t, "", string(buf))

	payload := "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"
	payload += strings.Repeat("a", 200)
	chunkedReader = newChunkedReader(io.MultiReader(
		strings.NewReader(payload),
		errReader{},
	))
	buf, err = ioutil.ReadAll(chunkedReader)
	assert.Equal(t, errors.New("err"), err)
	assert.Equal(t, strings.Repeat("a", 200), string(buf))

	payload = "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"
	payload += strings.Repeat("a", 1024+100)
	chunkedReader = newChunkedReader(io.MultiReader(
		strings.NewReader(payload),
		errReader{},
	))
	buf = make([]byte, 1024)
	n, err := chunkedReader.Read(buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, strings.Repeat("a", 1024), string(buf[:n]))
	assert.Equal(t, 1024, n)

	buf = make([]byte, 65536)
	n, err = chunkedReader.Read(buf)
	assert.Equal(t, errors.New("err"), err)
	assert.Equal(t, strings.Repeat("a", 100), string(buf[:n]))
	assert.Equal(t, 100, n)

	payload = "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"
	payload += strings.Repeat("a", 65536)
	chunkedReader = newChunkedReader(io.MultiReader(
		strings.NewReader(payload),
		errReader{},
	))
	buf = make([]byte, 65536+1024)
	n, err = io.ReadFull(chunkedReader, buf)
	assert.Equal(t, errors.New("err"), err)
	assert.Equal(t, strings.Repeat("a", 65536), string(buf[:n]))
	assert.Equal(t, 65536, n)

	payload = "10000;chunk-signature=ad80c"
	chunkedReader = newChunkedReader(io.MultiReader(
		strings.NewReader(payload),
		errReader{},
	))
	buf = make([]byte, 65536+1024)
	n, err = io.ReadFull(chunkedReader, buf)
	assert.Equal(t, errors.New("err"), err)
	assert.Equal(t, "", string(buf[:n]))
	assert.Equal(t, 0, n)

}

func TestChunkedUploadUnsignedTrailer(t *testing.T) {
	// STREAMING-UNSIGNED-PAYLOAD-TRAILER, as sent by AWS CLI v2 >= 2.23
	payload := "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"

	buf, err := ioutil.ReadAll(newChunkedReader(strings.NewReader(payload)))
	assert.Equal(t, nil, err)
	assert.Equal(t, "hello world", string(buf))
}

func TestChunkedUploadSignedTrailer(t *testing.T) {
	sig := strings.Repeat("a", 64)
	payload := "5;chunk-signature=" + sig + "\r\nhello\r\n0;chunk-signature=" + sig + "\r\n" +
		"x-amz-checksum-crc32:AAAAAA==\r\nx-amz-trailer-signature:" + sig + "\r\n\r\n"

	buf, err := ioutil.ReadAll(newChunkedReader(strings.NewReader(payload)))
	assert.Equal(t, nil, err)
	assert.Equal(t, "hello", string(buf))
}

func TestChunkedUploadCRC64NVMEKnownValue(t *testing.T) {
	// Check value of CRC-64/NVME for "123456789" is 0xae8b14860a799888.
	h := newChecksumHash("x-amz-checksum-crc64nvme")
	h.Write([]byte("123456789"))
	assert.Equal(t, "rosUhgp5mIg=", base64.StdEncoding.EncodeToString(h.Sum(nil)))
}

func TestChunkedUploadVerifyChecksums(t *testing.T) {
	// Checksums of "hello world"
	data := []byte("hello world")

	b64 := base64.StdEncoding.EncodeToString
	be32 := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	be64 := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

	crc32c := crc32.MakeTable(crc32.Castagnoli)
	crc64nvme := crc64.MakeTable(0x9a6c9329ac4bc9b5)
	sha1Sum := sha1.Sum(data)
	sha256Sum := sha256.Sum256(data)

	sums := map[string]string{
		"x-amz-checksum-crc32":     b64(be32(crc32.ChecksumIEEE(data))),
		"x-amz-checksum-crc32c":    b64(be32(crc32.Checksum(data, crc32c))),
		"x-amz-checksum-crc64nvme": b64(be64(crc64.Checksum(data, crc64nvme))),
		"x-amz-checksum-sha1":      b64(sha1Sum[:]),
		"x-amz-checksum-sha256":    b64(sha256Sum[:]),
	}

	for name, sum := range sums {
		t.Run(name, func(t *testing.T) {
			payload := "b\r\nhello world\r\n0\r\n" + name + ":" + sum + "\r\n\r\n"
			buf, err := ioutil.ReadAll(newChunkedReader(strings.NewReader(payload)).verifyTrailer(name))
			assert.NoError(t, err)
			assert.Equal(t, "hello world", string(buf))

			bad := "b\r\nhello worle\r\n0\r\n" + name + ":" + sum + "\r\n\r\n"
			_, err = ioutil.ReadAll(newChunkedReader(strings.NewReader(bad)).verifyTrailer(name))
			assert.Equal(t, ErrBadDigest, err)
		})
	}
}

func TestChunkedUploadChecksumNotVerifiedUnlessDeclared(t *testing.T) {
	payload := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"
	buf, err := ioutil.ReadAll(newChunkedReader(strings.NewReader(payload)))
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(buf))
}
