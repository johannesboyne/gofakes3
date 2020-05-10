package gofakes3

import (
	"encoding/xml"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestObjectListAddPrefix(t *testing.T) {
	b := NewObjectList()
	b.AddPrefix("prefix1")
	if len(b.CommonPrefixes) != 1 {
		t.Fatal("unexpected prefixes length")
	}

	// Duplicate prefix should not alter Bucket:
	b.AddPrefix("prefix1")
	if len(b.CommonPrefixes) != 1 {
		t.Fatal("unexpected prefixes length")
	}

	b.AddPrefix("prefix2")
	if len(b.CommonPrefixes) != 2 {
		t.Fatal("unexpected prefixes length")
	}
}

func TestContentTime(t *testing.T) {
	type testMsg struct {
		Foo  string
		Time ContentTime
	}
	const expected = "" +
		"<testMsg>" +
		"<Foo>bar</Foo>" +
		"<Time>2019-01-01T12:00:00Z</Time>" +
		"</testMsg>"

	var v = testMsg{
		Foo:  "bar",
		Time: NewContentTime(time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)),
	}
	out, err := xml.Marshal(&v)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != expected {
		t.Fatalf("unexpected XML output: %s", string(out))
	}
}

func TestContentTimeOmitEmpty(t *testing.T) {
	type testMsg struct {
		Foo  string
		Time ContentTime `xml:",omitempty"`
	}
	const expected = "" +
		"<testMsg>" +
		"<Foo>bar</Foo>" +
		"</testMsg>"

	var v = testMsg{Foo: "bar"}
	out, err := xml.Marshal(&v)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != expected {
		t.Fatalf("unexpected XML output: %s", string(out))
	}
}

func TestErrorResultFromError(t *testing.T) {
	t.Run("any-old-junk", func(t *testing.T) {
		er := ErrorResultFromError(io.EOF)
		if er.Code != ErrInternal {
			t.Fatal()
		}
	})

	t.Run("direct-code", func(t *testing.T) {
		er := ErrorResultFromError(ErrBadDigest)
		if er.Code != ErrBadDigest {
			t.Fatal()
		}
	})

	t.Run("wrapped-code", func(t *testing.T) {
		er := ErrorResultFromError(&ErrorResponse{Code: ErrBadDigest})
		if er.Code != ErrBadDigest {
			t.Fatal()
		}
	})

	t.Run("wrapped-code", func(t *testing.T) {
		er := ErrorResultFromError(KeyNotFound("nup"))
		if er.Code != ErrNoSuchKey {
			t.Fatal()
		}
	})
}

func TestMFADeleteStatus(t *testing.T) {
	type testMsg struct {
		Foo    string
		Status MFADeleteStatus
	}
	const inputTpl = "" +
		"<testMsg>" +
		"<Foo>bar</Foo>" +
		"<Status>%s</Status>" +
		"</testMsg>"

	for _, tc := range []struct {
		in, out string
	}{
		{"Enabled", "Enabled"},
		{"enabled", "Enabled"},
		{"ENABLED", "Enabled"},
		{"Disabled", "Disabled"},
	} {
		var msg testMsg
		if err := xml.Unmarshal([]byte(fmt.Sprintf(inputTpl, tc.in)), &msg); err != nil {
			t.Fatal(err)
		}
		if string(msg.Status) != tc.out {
			t.Fatal()
		}
	}

	var msg testMsg
	if err := xml.Unmarshal([]byte(fmt.Sprintf(inputTpl, "QUACK QUACK")), &msg); err == nil {
		t.Fatal()
	}
}

func TestCopyObjectResult(t *testing.T) {
	res := CopyObjectResult{
		ETag:         `"etag0"`,
		LastModified: NewContentTime(time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)),
	}
	const expected = "" +
		"<CopyObjectResult>" +
		"<ETag>&#34;etag0&#34;</ETag>" +
		"<LastModified>2019-01-01T12:00:00Z</LastModified>" +
		"</CopyObjectResult>"

	out, err := xml.Marshal(&res)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != expected {
		t.Fatalf("unexpected XML output: %s", string(out))
	}
}
