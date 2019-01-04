package gofakes3

// Initialisation file for tests in the 'gofakes3' package. Internal tests, unit
// tests that use struct internals, etc go in this package.

import (
	"io/ioutil"
	"log"
	"testing"
)

type TT struct {
	*testing.T
}

func (t TT) OK(err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (t TT) OKAll(vs ...interface{}) {
	t.Helper()
	for _, v := range vs {
		if err, ok := v.(error); ok && err != nil {
			t.Fatal(err)
		}
	}
}

func init() {
	// Tests that may cause log output that merits inspection belong in
	// gofakes3_test.
	log.SetOutput(ioutil.Discard)
}
