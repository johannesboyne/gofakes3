package gofakes3

import (
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

// ConditionalWriteBackend checks destination conditions atomically when publishing
// a PUT or COPY. Unconditional requests use the ordinary Backend methods.
type ConditionalWriteBackend interface {
	PutObjectConditional(bucket, key string, meta map[string]string, input io.Reader, size int64, conditions *WriteConditions) (PutObjectResult, error)
	CopyObjectConditional(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string, conditions *WriteConditions) (CopyObjectResult, error)
}

// ConditionalMultipartBackend checks destination conditions at multipart publication.
type ConditionalMultipartBackend interface {
	CompleteMultipartUploadConditional(bucket, object string, id UploadID, input *CompleteMultipartUploadRequest, conditions *WriteConditions) (VersionID, string, error)
}

// WriteConditions contains the validated destination ETags of a request.
type WriteConditions struct{ match, none []string }

// ParseWriteConditions parses If-Match and If-None-Match. It returns nil
// when neither header is present.
func ParseWriteConditions(h http.Header) (*WriteConditions, error) {
	var c WriteConditions
	for _, field := range []struct {
		name string
		tags *[]string
	}{{"If-Match", &c.match}, {"If-None-Match", &c.none}} {
		if values := h.Values(field.name); len(values) > 0 {
			tags := splitEntityTags(strings.Join(values, ","))
			if len(tags) == 0 {
				return nil, ErrInvalidArgument
			}
			*field.tags = tags
		}
	}
	if c.match == nil && c.none == nil {
		return nil, nil
	}
	return &c, nil
}

// NeedsETag reports whether validation requires hashing an existing object.
func (c *WriteConditions) NeedsETag() bool {
	return len(c.match) > 0 && c.match[0] != "*" || len(c.none) > 0 && c.none[0] != "*"
}

// Check validates the conditions against the destination. etag is the quoted
// strong ETag when NeedsETag is true and the destination exists.
func (c *WriteConditions) Check(exists bool, etag string) error {
	if len(c.match) > 0 {
		if !exists {
			return ErrNoSuchKey
		}
		if c.match[0] != "*" && !slices.Contains(c.match, etag) {
			return ErrPreconditionFailed
		}
	}
	if exists {
		for _, tag := range c.none {
			if tag == "*" || strings.TrimPrefix(tag, "W/") == etag {
				return ErrPreconditionFailed
			}
		}
	}
	return nil
}

func (c *WriteConditions) putConditions() *PutConditions {
	if c == nil {
		return nil
	}
	p := &PutConditions{}
	if len(c.match) > 0 {
		value := strings.Join(c.match, ",")
		p.IfMatch = &value
	}
	if len(c.none) > 0 {
		value := strings.Join(c.none, ",")
		p.IfNoneMatch = &value
	}
	return p
}

// checkCopySource evaluates the source headers independently of destination conditions.
func checkCopySource(h http.Header, obj *Object) error {
	const prefix = "X-Amz-Copy-Source-"
	headers := make(http.Header)
	for _, name := range []string{"If-Match", "If-None-Match"} {
		for _, v := range h.Values(prefix + name) {
			headers.Add(name, v)
		}
	}
	c, err := ParseWriteConditions(headers)
	if err != nil {
		return err
	}
	if c != nil {
		if err := c.Check(true, FormatETag(obj.Hash)); err != nil {
			return err
		}
	}
	modified, err := http.ParseTime(obj.Metadata["Last-Modified"])
	if err != nil {
		return nil
	}
	if c == nil || len(c.match) == 0 {
		if t, err := http.ParseTime(h.Get(prefix + "If-Unmodified-Since")); err == nil && modified.Truncate(time.Second).After(t) {
			return ErrPreconditionFailed
		}
	}
	if c == nil || len(c.none) == 0 {
		if t, err := http.ParseTime(h.Get(prefix + "If-Modified-Since")); err == nil && !modified.Truncate(time.Second).After(t) {
			return ErrPreconditionFailed
		}
	}
	return nil
}

// Legacy COPY and multipart methods cannot accept destination conditions.
func (g *GoFakeS3) checkDestination(bucket, key string, c *WriteConditions) error {
	obj, err := g.storage.HeadObject(bucket, key)
	if HasErrorCode(err, ErrNoSuchKey) {
		return c.Check(false, "")
	}
	if err != nil {
		return err
	}
	if obj.Contents != nil {
		defer obj.Contents.Close()
	}
	return c.Check(true, FormatETag(obj.Hash))
}

// splitEntityTags accepts quoted lists and single unquoted tags used by S3 clients.
func splitEntityTags(value string) []string {
	value = strings.Trim(value, " \t")
	if value == "*" {
		return []string{"*"}
	}
	if value != "" && value[0] != '"' && !strings.HasPrefix(value, "W/") {
		for i := range len(value) {
			c := value[i]
			if c <= 0x20 || c >= 0x7f || c == '"' || c == ',' || c == '*' {
				return nil
			}
		}
		return []string{`"` + value + `"`}
	}
	var tags []string
	for value != "" {
		end := 0
		if strings.HasPrefix(value, "W/") {
			end = 2
		}
		if len(value) <= end || value[end] != '"' {
			return nil
		}
		end++
		for end < len(value) && value[end] != '"' {
			if value[end] < 0x21 || value[end] == 0x7f {
				return nil
			}
			end++
		}
		if end == len(value) {
			return nil
		}
		end++
		tags = append(tags, value[:end])
		value = strings.TrimLeft(value[end:], " \t")
		if value == "" {
			return tags
		}
		if value[0] != ',' {
			return nil
		}
		value = strings.TrimLeft(value[1:], " \t")
		if value == "" {
			return nil
		}
	}
	return tags
}
