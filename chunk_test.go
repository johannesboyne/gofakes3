package gofakes3

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

func TestChunkedUpload(t *testing.T) {
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
