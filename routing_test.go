package gofakes3_test

import (
	"net/http"
	"testing"
	"time"
)

func TestRoutingSlashes(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	ts.createBucket("test")
	ts.putString("test", "obj", nil, "yep")

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	assertStatus := func(url string, code int) {
		t.Helper()
		rs, err := client.Head(ts.url(url))
		ts.OK(err)
		if rs.StatusCode != code {
			t.Fatal("expected status", code, "found", rs.StatusCode)
		}
	}

	assertStatus("nope", 404) // sanity check missing URL
	assertStatus("test", 200)
	assertStatus("test/", 200)
	assertStatus("test//", 200) // don't care how many slashes
	assertStatus("test/nope", 404)
	assertStatus("test/obj", 200)
	assertStatus("test/obj/", 200)
	assertStatus("test/obj//", 200)
}
