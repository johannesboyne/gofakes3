package gofakes3

import (
	"testing"
	"time"
)

func TestFixedTimeSource(t *testing.T) {
	start := time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)
	fts := FixedTimeSource(start)
	if fts.Now() != start {
		t.Fatal()
	}
	if fts.Since(start) != 0 {
		t.Fatal()
	}

	fts.Advance(1 * time.Minute)
	if fts.Now() != start.Add(1*time.Minute) {
		t.Fatal()
	}
	if fts.Since(start) != 1*time.Minute {
		t.Fatal()
	}
}
