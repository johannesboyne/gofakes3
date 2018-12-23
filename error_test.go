package gofakes3

import (
	"encoding/xml"
	"testing"
	"time"
)

func TestErrorCustomResponseMarshalsAsExpected(t *testing.T) {
	resp := requestTimeTooSkewed(time.Time{}, 123)
	out, err := xml.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	// This is slightly brittle, but it does ensure that the embedded struct
	// has its fields merged with the element rather than being nested:
	expected := `<Error>` +
		`<Code>RequestTimeTooSkewed</Code>` +
		`<Message>The difference between the request time and the current time is too large</Message>` +
		`<ServerTime>0001-01-01T00:00:00Z</ServerTime>` +
		`<MaxAllowedSkewMilliseconds>0</MaxAllowedSkewMilliseconds>` +
		`</Error>`

	if string(out) != expected {
		t.Fatalf("expected:\n%s\nfound:\n%s", expected, out)
	}
}
