package gofakes3

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"strings"
	"testing"
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
