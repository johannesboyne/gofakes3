package gofakes3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

const (
	ErrBucketAlreadyExists ErrorCode = "BucketAlreadyExists"

	// Raised when attempting to delete a bucket that still contains items.
	ErrBucketNotEmpty ErrorCode = "BucketNotEmpty"

	// You did not provide the number of bytes specified by the Content-Length
	// HTTP header:
	ErrIncompleteBody ErrorCode = "IncompleteBody"

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
	ErrInvalidBucketName ErrorCode = "InvalidBucketName"

	ErrKeyTooLong       ErrorCode = "KeyTooLongError"
	ErrMetadataTooLarge ErrorCode = "MetadataTooLarge"
	ErrMethodNotAllowed ErrorCode = "MethodNotAllowed"
	ErrMalformedXML     ErrorCode = "MalformedXML"

	// See BucketNotFound() for a helper function for this error:
	ErrNoSuchBucket ErrorCode = "NoSuchBucket"

	// See KeyNotFound() for a helper function for this error:
	ErrNoSuchKey ErrorCode = "NoSuchKey"

	ErrRequestTimeTooSkewed ErrorCode = "RequestTimeTooSkewed"
	ErrTooManyBuckets       ErrorCode = "TooManyBuckets"
	ErrNotImplemented       ErrorCode = "NotImplemented"

	ErrInternal ErrorCode = "InternalError"
)

type Error interface {
	error
	ErrorCode() ErrorCode
}

type errorResponse interface {
	Error
	Enrich(requestID string)
}

type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`

	Code      ErrorCode
	Message   string `xml:",omitempty"`
	RequestID string `xml:"RequestId,omitempty"`
}

func ensureErrorResponse(err error, requestID string) Error {
	switch err := err.(type) {
	case errorResponse:
		err.Enrich(requestID)
		return err

	case ErrorCode:
		return &ErrorResponse{
			Code:      err,
			RequestID: requestID,
			Message:   string(err),
		}

	default:
		return &ErrorResponse{
			Code:      ErrInternal,
			Message:   "Internal Error",
			RequestID: requestID,
		}
	}
}

func (e *ErrorResponse) ErrorCode() ErrorCode { return e.Code }

func (r *ErrorResponse) Enrich(requestID string) {
	r.RequestID = requestID
}

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func ErrorMessage(code ErrorCode, message string) error {
	return &ErrorResponse{Code: code, Message: message}
}

type ErrorCode string

func (e ErrorCode) ErrorCode() ErrorCode { return e }
func (e ErrorCode) Error() string        { return string(e) }

func (e ErrorCode) Message() string {
	switch e {
	case ErrNoSuchBucket:
		return "The specified bucket does not exist"
	case ErrRequestTimeTooSkewed:
		return "The difference between the request time and the current time is too large"
	default:
		return ""
	}
}

func (e ErrorCode) Status() int {
	switch e {
	case ErrBucketAlreadyExists,
		ErrBucketNotEmpty:
		return http.StatusConflict

	case ErrIncompleteBody,
		ErrInvalidBucketName,
		ErrKeyTooLong,
		ErrMetadataTooLarge,
		ErrMethodNotAllowed,
		ErrMalformedXML,
		ErrTooManyBuckets:
		return http.StatusBadRequest

	case ErrRequestTimeTooSkewed:
		return http.StatusForbidden

	case ErrNoSuchBucket,
		ErrNoSuchKey:
		return http.StatusNotFound

	case ErrNotImplemented:
		return http.StatusNotImplemented

	case ErrInternal:
		return http.StatusInternalServerError
	}

	return http.StatusInternalServerError
}

func HasErrorCode(err error, code ErrorCode) bool {
	s3err, ok := err.(interface{ ErrorCode() ErrorCode })
	if !ok {
		return false
	}
	return s3err.ErrorCode() == code
}

func IsErrExist(err error) bool {
	return HasErrorCode(err, ErrBucketAlreadyExists)
}

type resourceErrorResponse struct {
	ErrorResponse
	Resource string
}

var _ errorResponse = &resourceErrorResponse{}

func ResourceError(code ErrorCode, resource string) error {
	return &resourceErrorResponse{
		ErrorResponse{Code: code, Message: code.Message()},
		resource,
	}
}

func BucketNotFound(bucket string) error { return ResourceError(ErrNoSuchBucket, bucket) }
func KeyNotFound(key string) error       { return ResourceError(ErrNoSuchKey, key) }

type requestTimeTooSkewedResponse struct {
	ErrorResponse
	ServerTime                 time.Time
	MaxAllowedSkewMilliseconds durationAsMilliseconds
}

var _ errorResponse = &requestTimeTooSkewedResponse{}

func requestTimeTooSkewed(at time.Time, max time.Duration) error {
	code := ErrRequestTimeTooSkewed
	return &requestTimeTooSkewedResponse{
		ErrorResponse{Code: code, Message: code.Message()},
		at, durationAsMilliseconds(max),
	}
}

type durationAsMilliseconds time.Duration

func (m durationAsMilliseconds) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	var s = fmt.Sprintf("%d", time.Duration(m)/time.Millisecond)
	return e.EncodeElement(s, start)
}
