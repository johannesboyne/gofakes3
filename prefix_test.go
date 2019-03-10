package gofakes3

import (
	"fmt"
	"reflect"
	"testing"
)

func TestPrefixMatch(t *testing.T) {
	// Cheapo helpers for hiding pointer strings, which are used to increase
	// information density in the test case table:
	s := func(v string) *string { return &v }

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
				Prefix:       unwrapStr(tc.p),
				Delimiter:    unwrapStr(tc.d),
			}

			var match PrefixMatch
			matched := prefix.Match(tc.key, &match)
			if (tc.out == nil) != (!matched) {
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

func TestNewPrefix(t *testing.T) {
	s := func(in string) *string { return &in }

	for _, tc := range []struct {
		prefix, delim *string
		out           Prefix
	}{
		{nil, nil, Prefix{}},
		{s("foo"), nil, Prefix{HasPrefix: true, Prefix: "foo"}},
		{nil, s("foo"), Prefix{HasDelimiter: true, Delimiter: "foo"}},
		{s("foo"), s("bar"), Prefix{HasPrefix: true, Prefix: "foo", HasDelimiter: true, Delimiter: "bar"}},
	} {
		t.Run("", func(t *testing.T) {
			exp := NewPrefix(tc.prefix, tc.delim)
			if !reflect.DeepEqual(tc.out, exp) {
				t.Fatal(tc.out, "!=", exp)
			}
		})
	}
}

func TestPrefixFilePrefix(t *testing.T) {
	s := func(v string) *string { return &v }

	for idx, tc := range []struct {
		p, d      *string
		ok        bool
		path, rem string
	}{
		{s("foo/bar"), s("/"), true, "foo", "bar"},
		{s("foo/bar/"), s("/"), true, "foo/bar", ""},
		{s("foo/bar/b"), s("/"), true, "foo/bar", "b"},
		{s("foo"), s("/"), true, "", "foo"},
		{s("foo/"), s("/"), true, "foo", ""},
		{s("/"), s("/"), true, "", ""},
		{s(""), s("/"), true, "", ""},

		{s(""), nil, false, "", ""},
		{s("foo"), nil, false, "", ""},
		{s("foo/bar"), nil, false, "", ""},
		{s("foo-bar"), s("-"), false, "", ""},
	} {
		t.Run(fmt.Sprintf("%d/(%s-%s)", idx, tc.path, tc.rem), func(t *testing.T) {
			prefix := NewPrefix(tc.p, tc.d)

			foundPath, foundRem, ok := prefix.FilePrefix()
			if tc.ok != ok {
				t.Fatal()
			} else if tc.ok {
				if tc.path != foundPath {
					t.Fatal("prefix path", tc.path, "!=", foundPath)
				}
				if tc.rem != foundRem {
					t.Fatal("prefix rem", tc.rem, "!=", foundRem)
				}
			}
		})
	}
}

func unwrapStr(v *string) string {
	if v != nil {
		return *v
	}
	return ""
}
