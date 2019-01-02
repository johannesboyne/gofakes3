package gofakes3

import (
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
