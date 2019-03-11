package gofakes3

import (
	"fmt"
	"testing"
)

func TestRangeRequest(t *testing.T) {
	for idx, tc := range []struct {
		inst, inend  int64
		rev          bool
		sz           int64
		outst, outln int64
		fail         bool
	}{
		{inst: 0, inend: RangeNoEnd, sz: 5, outst: 0, outln: 5},
		{inst: 0, inend: 5, sz: 10, outst: 0, outln: 6},
		{inst: 0, inend: 5, sz: 4, outst: 0, outln: 4},
		{inst: 0, inend: 0, sz: 4, outst: 0, outln: 1},
		{inst: 1, inend: 5, sz: 10, outst: 1, outln: 5},

		{rev: true, inend: 10, sz: 10, outst: 0, outln: 10},
		{rev: true, inend: 5, sz: 10, outst: 5, outln: 5},
		{rev: true, inend: 20, sz: 10, outst: 0, outln: 10},

		{fail: true, inst: 0, inend: 0, sz: 0, outst: 0, outln: 0},
		{fail: true, inst: 10, inend: 15, sz: 10, outst: 10, outln: 0},
		{fail: true, inst: 40, inend: 50, sz: 11, outst: 11, outln: 0},
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			orr := ObjectRangeRequest{Start: tc.inst, End: tc.inend, FromEnd: tc.rev}

			rng, err := orr.Range(tc.sz)
			if tc.fail != (err != nil) {
				t.Fatal("failure expected:", tc.fail, "found:", err)
			}
			if !tc.fail {
				if rng.Start != tc.outst {
					t.Fatal("unexpected start:", rng.Start, "expected:", tc.outst)
				}
				if rng.Length != tc.outln {
					t.Fatal("unexpected length:", rng.Length, "expected:", tc.outln)
				}
			}
		})
	}
}
