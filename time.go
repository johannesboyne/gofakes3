package gofakes3

import "time"

type TimeSource interface {
	Now() time.Time
	Since(time.Time) time.Duration
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
