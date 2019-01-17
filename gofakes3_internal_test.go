package gofakes3

import (
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"testing"
)

func TestHttpError(t *testing.T) {
	var g GoFakeS3
	rq := httptest.NewRequest("GET", "/", nil)
	rs := httptest.NewRecorder()
	g.httpError(rs, rq, ErrNoSuchBucket)
	if rs.Code != 404 {
		t.Fatal()
	}
	if rs.Body.Len() == 0 {
		t.Fatal()
	}
	var resp ErrorResponse
	if err := xml.Unmarshal(rs.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	if resp.Code != ErrNoSuchBucket {
		t.Fatal()
	}
}

func TestHttpErrorWriteFailure(t *testing.T) {
	// FIXME: with a pluggable logger, we can intercept the log message to
	// verify the write error is handled.
	var g GoFakeS3
	rq := httptest.NewRequest("GET", "/", nil)
	rs := httptest.NewRecorder()
	g.httpError(&failingResponseWriter{rs}, rq, ErrNoSuchBucket)
	if rs.Code != 404 {
		t.Fatal()
	}
	if rs.Body.Len() != 0 {
		t.Fatal()
	}
}

type failingResponseWriter struct {
	*httptest.ResponseRecorder
}

func (w *failingResponseWriter) Write(buf []byte) (n int, err error) {
	return 0, fmt.Errorf("nope")
}
