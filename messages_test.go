package gofakes3

import (
	"encoding/xml"
	"testing"
	"time"
)

func TestBucketAddPrefix(t *testing.T) {
	b := NewListBucketResult("yep")
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
