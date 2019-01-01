package gofakes3

import "time"

type Option func(g *GoFakeS3)

func WithTimeSource(timeSource TimeSource) Option {
	return func(g *GoFakeS3) { g.timeSource = timeSource }
}

// WithTimeSkewLimit allows you to reconfigure the allowed skew between the
// client's clock and the server's clock. The AWS client SDKs will send the
// "x-amz-date" header containing the time at the client, which is used to
// calculate the skew.
//
// See DefaultSkewLimit for the starting value, set to '0' to disable.
//
func WithTimeSkewLimit(skew time.Duration) Option {
	return func(g *GoFakeS3) { g.timeSkew = skew }
}

// WithMetadataSizeLimit allows you to reconfigure the maximum allowed metadata
// size.
//
// See DefaultMetadataSizeLimit for the starting value, set to '0' to disable.
func WithMetadataSizeLimit(size int) Option {
	return func(g *GoFakeS3) { g.metadataSizeLimit = size }
}
