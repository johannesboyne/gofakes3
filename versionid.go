package gofakes3

import (
	"encoding/base64"
	"fmt"
	"sync"
)

type VersionID string

type VersionGenerator struct {
	state uint64
	size  int
	mu    sync.Mutex
}

func NewVersionGenerator(seed uint64, size int) *VersionGenerator {
	if size <= 0 {
		size = 64
	}
	return &VersionGenerator{state: seed}
}

// Next generates a fresh VersionID from the internal RNG.
// If b is passed, it is used as the scratch buffer. If b is smaller than
// Next requires, it is resized. The reallocated memory is returned along
// with the version ID.
//
// Next is safe for concurrent use.
func (v *VersionGenerator) Next(b []byte) (VersionID, []byte) {
	v.mu.Lock()
	neat := v.size/8*8 + 8 // cheap and nasty way to ensure a multiple of 8 definitely greater than size

	if len(b) < neat {
		b = make([]byte, neat)
	}

	// This is a simple inline implementation of http://xoshiro.di.unimi.it/splitmix64.c.
	// It may not ultimately be the right tool for this job but with a large
	// enough size the collision risk should still be minuscule.
	for i := 0; i < neat; i += 8 {
		v.state += 0x9E3779B97F4A7C15
		z := v.state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		b[i], b[i+1], b[i+2], b[i+3], b[i+4], b[i+5], b[i+6], b[i+7] =
			byte(z), byte(z>>8), byte(z>>16), byte(z>>24), byte(z>>32), byte(z>>40), byte(z>>48), byte(z>>56)
	}

	v.mu.Unlock()

	// The version IDs appear to start with '3/' and follow with a base64-URL
	// encoded blast of god knows what. There didn't appear to be any
	// explanation of the format beyond that.
	return VersionID(fmt.Sprintf("3/%s", base64.URLEncoding.EncodeToString(b[:v.size]))), b
}
