package gofakes3

import "time"

type TimeSource interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type TimeSourceAdvancer interface {
	TimeSource
	Advance(by time.Duration)
}

// FixedTimeSource provides a source of time that always returns the
// specified time.
func FixedTimeSource(at time.Time) TimeSourceAdvancer {
	return &fixedTimeSource{time: at}
}

func DefaultTimeSource() TimeSource {
	timeLocation, err := time.LoadLocation("GMT")
	if err != nil {
		panic(err)
	}
	return &locatedTimeSource{
		timeLocation: timeLocation,
	}
}

type locatedTimeSource struct {
	timeLocation *time.Location
}

func (l *locatedTimeSource) Now() time.Time {
	return time.Now().In(l.timeLocation)
}

func (l *locatedTimeSource) Since(t time.Time) time.Duration {
	return time.Since(t)
}

type fixedTimeSource struct {
	time time.Time
}

func (l *fixedTimeSource) Now() time.Time {
	return l.time
}

func (l *fixedTimeSource) Since(t time.Time) time.Duration {
	return t.Sub(l.time)
}

func (l *fixedTimeSource) Advance(by time.Duration) {
	l.time = l.time.Add(by)
}
