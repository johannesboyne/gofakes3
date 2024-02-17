package gofakes3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
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
	var buf bytes.Buffer
	std := log.New(&buf, "", 0)
	logger := StdLog(std)
	var g = GoFakeS3{
		log: logger,
	}

	rq := httptest.NewRequest("GET", "/", nil)
	rs := httptest.NewRecorder()
	g.httpError(&failingResponseWriter{rs}, rq, ErrNoSuchBucket)
	if rs.Code != 404 {
		t.Fatal()
	}
	if rs.Body.Len() != 0 {
		t.Fatal()
	}
	if buf.String() != "ERR nope\n" {
		t.Fatal()
	}
}

func TestHostBucketMiddleware(t *testing.T) {
	for _, tc := range []struct {
		in   string
		host string
		out  string
	}{
		{"/", "foo", "/foo"},
		{"/", "mybucket.localhost", "/mybucket"},
		{"/object", "mybucket.localhost", "/mybucket/object"},
	} {
		t.Run("", func(t *testing.T) {
			var g GoFakeS3
			g.log = DiscardLog()

			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.out {
					t.Fatal(r.URL.Path, "!=", tc.out)
				}
			})

			handler := g.hostBucketMiddleware(inner)
			rq := httptest.NewRequest("GET", tc.in, nil)
			rq.Host = tc.host
			rs := httptest.NewRecorder()
			handler.ServeHTTP(rs, rq)
		})
	}
}

func TestHostBucketBaseMiddleware(t *testing.T) {
	for _, tc := range []struct {
		bases   []string
		host    string
		inPath  string
		outPath string
	}{
		{[]string{"localhost"}, "foo", "/", "/"},
		{[]string{"localhost"}, "localhost", "/", "/"},
		{[]string{"localhost"}, "mybucket.fleebderb", "/", "/"},
		{[]string{"localhost"}, "mybucket.localhost", "/", "/mybucket"},
		{[]string{"localhost"}, "mybucket.localhost", "/object", "/mybucket/object"},
	} {
		t.Run("", func(t *testing.T) {
			var g GoFakeS3
			g.hostBucketBases = tc.bases
			g.log = DiscardLog()

			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.outPath {
					t.Fatal(r.URL.Path, "!=", tc.outPath)
				}
			})

			handler := g.hostBucketBaseMiddleware(inner)
			rq := httptest.NewRequest("GET", tc.inPath, nil)
			rq.Host = tc.host
			rs := httptest.NewRecorder()
			handler.ServeHTTP(rs, rq)
		})
	}
}

type failingResponseWriter struct {
	*httptest.ResponseRecorder
}

func (w *failingResponseWriter) Write(buf []byte) (n int, err error) {
	return 0, fmt.Errorf("nope")
}
