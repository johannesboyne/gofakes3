package gofakes3

import "time"

type Option func(g *GoFakeS3)

// WithTimeSource allows you to substitute the behaviour of time.Now() and
// time.Since() within GoFakeS3. This can be used to trigger time skew errors,
// or to ensure the output of the commands is deterministic.
//
// See gofakes3.FixedTimeSource(), gofakes3.LocalTimeSource(tz).
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

// WithIntegrityCheck enables or disables Content-MD5 validation when
// putting an Object.
func WithIntegrityCheck(check bool) Option {
	return func(g *GoFakeS3) { g.integrityCheck = check }
}

// WithLogger allows you to supply a logger to GoFakeS3 for debugging/tracing.
// logger may be nil.
func WithLogger(logger Logger) Option {
	return func(g *GoFakeS3) { g.log = logger }
}
