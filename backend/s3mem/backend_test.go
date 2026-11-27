package s3mem

import (
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
)

// A delimited listing over several pages returns each common prefix once.
func TestDelimiterPaginationReturnsPrefixOnce(t *testing.T) {
	db := New()
	if err := db.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a/1", "a/2", "a/3", "b/1", "c/1", "c/2"} {
		if _, err := db.PutObject("b", key, nil, strings.NewReader(key), int64(len(key)), nil); err != nil {
			t.Fatal(err)
		}
	}
	p := &gofakes3.Prefix{HasDelimiter: true, Delimiter: "/"}

	var seen []string
	page := gofakes3.ListBucketPage{MaxKeys: 1}
	for i := 0; i < 20; i++ {
		out, err := db.ListBucket("b", p, page)
		if err != nil {
			t.Fatal(err)
		}
		for _, cp := range out.CommonPrefixes {
			seen = append(seen, cp.Prefix)
		}
		if !out.IsTruncated {
			break
		}
		page = gofakes3.ListBucketPage{MaxKeys: 1, Marker: out.NextMarker, HasMarker: true}
	}

	if strings.Join(seen, ",") != "a/,b/,c/" {
		t.Fatalf("expected prefixes a/,b/,c/ once each, got %v", seen)
	}
}
