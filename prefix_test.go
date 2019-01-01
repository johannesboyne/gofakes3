package gofakes3

import (
	"testing"
)

func TestPrefixMatch(t *testing.T) {
	// Cheapo helpers for hiding pointer strings, used to increase information density
	// in the test case table:
	s := func(v string) *string { return &v }
	unwrap := func(v *string) string {
		if v != nil {
			return *v
		}
		return ""
	}

	for idx, tc := range []struct {
		key    string
		p      *string
		d      *string
		out    *string
		common bool
	}{
		{key: "foo/bar", p: s("foo"), d: s("/"), out: s("foo/"), common: true},
		{key: "foo/bar", p: s("foo/ba"), d: s("/"), out: s("foo/bar")},
		{key: "foo/bar", p: s("foo/ba/"), d: s("/"), out: nil},
		{key: "foo/bar", p: s("/"), d: s("/"), out: s("foo/"), common: true},

		// Without a delimiter, it's just a boring ol' prefix match:
		{key: "foo/bar", p: s("foo/b"), out: s("foo/b")},
		{key: "foo/bar", p: s("foo/"), out: s("foo/")},
		{key: "foo/bar", p: s("foo"), out: s("foo")},
		{key: "foo/bar", p: s("fo"), out: s("fo")},
		{key: "foo/bar", p: s("f"), out: s("f")},
		{key: "foo/bar", p: s("q"), out: nil},

		// This could be a source of trouble - does "no prefix" mean "match
		// everything" or "match nothing"? What about "empty prefix"? For now,
		// these cases simply document what the curret algorithm is expected to
		// do, but this needs further exploration:
		{key: "foo/bar", p: nil, out: s("foo/bar")},
		{key: "foo/bar", p: s(""), out: s("")},
	} {
		t.Run("", func(t *testing.T) {
			prefix := Prefix{
				HasPrefix:    tc.p != nil,
				HasDelimiter: tc.d != nil,
				Prefix:       unwrap(tc.p),
				Delimiter:    unwrap(tc.d),
			}
			match := prefix.Match(tc.key)
			if (tc.out == nil) != (match == nil) {
				t.Fatal("prefix match failed at index", idx)
			}
			if tc.out != nil {
				if *tc.out != match.MatchedPart {
					t.Fatal("prefix matched part failed at index", idx, *tc.out, "!=", match.MatchedPart)
				}
				if tc.common != match.CommonPrefix {
					t.Fatal("prefix common failed at index", idx)
				}
			}
		})
	}
}
