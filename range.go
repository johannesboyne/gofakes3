package gofakes3

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type ObjectRange struct {
	Start, Length int64
}

var zeroRange ObjectRange

func (o ObjectRange) IsZero() bool { return o == zeroRange }

func (o ObjectRange) writeHeader(sz int64, w http.ResponseWriter) {
	if !o.IsZero() {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", o.Start, o.Start+o.Length-1, sz))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", o.Length))
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", sz))
	}
}

type ObjectRangeRequest struct {
	Start, End int64
	FromEnd    bool
}

var zeroRangeRequest ObjectRangeRequest

func (o ObjectRangeRequest) IsZero() bool { return o == zeroRangeRequest }

func (o ObjectRangeRequest) Range(size int64) ObjectRange {
	var start, length int64

	if !o.FromEnd {
		// If no end is specified, range extends to end of the file.
		start = o.Start
		end := o.End
		if end >= size {
			end = size - 1
		}
		if o.End == 0 {
			length = size - o.Start
		} else {
			length = end - o.Start + 1
		}

	} else {
		// If no start is specified, end specifies the range start relative
		// to the end of the file.
		end := o.End
		if end > size {
			end = size
		}
		start = size - end
		length = size - start

	}

	return ObjectRange{Start: start, Length: length}
}

// parseHeader parses a single byte range from the Range header.
//
// Amazon S3 doesn't support retrieving multiple ranges of data per GET request:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func (o *ObjectRangeRequest) parseHeader(s string) error {
	if s == "" {
		return nil
	}

	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return ErrInvalidRange
	}

	ranges := strings.Split(s[len(b):], ",")
	if len(ranges) > 1 {
		return ErrorMessage(ErrNotImplemented, "multiple ranges not supported")
	}

	rnge := strings.TrimSpace(ranges[0])
	if len(rnge) == 0 {
		return nil
	}

	i := strings.Index(rnge, "-")
	if i < 0 {
		return ErrInvalidRange
	}

	start, end := strings.TrimSpace(rnge[:i]), strings.TrimSpace(rnge[i+1:])
	if start == "" {
		o.FromEnd = true

		i, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			return ErrInvalidRange
		}
		o.End = i

	} else {
		i, err := strconv.ParseInt(start, 10, 64)
		if err != nil || i < 0 {
			return ErrInvalidRange
		}
		o.Start = i
		if end != "" {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil || o.Start > i {
				return ErrInvalidRange
			}
			o.End = i
		}
	}

	return nil
}
