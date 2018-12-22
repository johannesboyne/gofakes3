package gofakes3

import "fmt"

type errNotFound struct {
	bucket, object string
}

func (e *errNotFound) Error() string {
	return fmt.Sprintf("gofakes3: not found: bucket %q, object %q", e.bucket, e.object)
}

func IsNotFound(err error) bool {
	_, ok := err.(*errNotFound)
	return ok
}

func NotFound(bucket, object string) error {
	return &errNotFound{bucket: bucket, object: object}
}
