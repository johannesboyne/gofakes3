package gofakes3

import (
	"strings"
	"testing"
)

func TestParseClampedIntValid(t *testing.T) {
	for _, tc := range []struct {
		in             string
		dflt, min, max int64
		out            int64
	}{
		{in: "", dflt: 1, min: 0, max: 1, out: 1},
		{in: "", dflt: 2, min: 0, max: 1, out: 1},
		{in: "1", dflt: 2, min: 0, max: 100, out: 1},
		{in: "1", dflt: 0, min: 2, max: 100, out: 2},
		{in: "1000", dflt: 0, min: 2, max: 100, out: 100},
	} {
		t.Run("", func(t *testing.T) {
			result, err := parseClampedInt(tc.in, tc.dflt, tc.min, tc.max)
			if err != nil {
				t.Fatal(err)
			}
			if result != tc.out {
				t.Fatal(result, "!=", tc.out)
			}
		})
	}
}

func TestReadAll(t *testing.T) {
	t.Run("simple-read", func(t *testing.T) {
		tt := TT{t}
		b, err := ReadAll(strings.NewReader("test"), 4)
		tt.OK(err)
		if string(b) != "test" {
			t.Fatal(string(b), "!=", "test")
		}
	})

	t.Run("empty-input", func(t *testing.T) {
		tt := TT{t}
		b, err := ReadAll(strings.NewReader(""), 0)
		tt.OK(err)
		if string(b) != "" {
			t.Fatal(string(b), "!=", "")
		}
	})

	t.Run("size-too-large", func(t *testing.T) {
		_, err := ReadAll(strings.NewReader("test"), 5)
		if !HasErrorCode(err, ErrIncompleteBody) {
			t.Fatal("expected ErrIncompleteBody, found", err)
		}
	})

	t.Run("size-too-small", func(t *testing.T) {
		_, err := ReadAll(strings.NewReader("test"), 3)
		if !HasErrorCode(err, ErrIncompleteBody) {
			t.Fatal("expected ErrIncompleteBody, found", err)
		}
	})
}
