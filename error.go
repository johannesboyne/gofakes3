package gofakes3

import (
	"encoding/xml"
	"fmt"
	"net/http"
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
	Code() ErrorCode
	Message() string
}

type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`

	Code      ErrorCode
	Message   string `xml:",omitempty"`
	Resource  string `xml:",omitempty"`
	RequestID string `xml:"RequestId,omitempty"`
}

func NewErrorResponse(err error, requestID string) ErrorResponse {
	s3err, ok := err.(Error)
	if !ok {
		return ErrorResponse{
			Code:      ErrInternal,
			Message:   "Internal Error",
			RequestID: requestID,
		}
	}

	resp := ErrorResponse{
		Code:      s3err.Code(),
		Message:   s3err.Message(),
		RequestID: requestID,
	}
	if resp.Message == "" {
		resp.Message = s3err.Code().Message()
	}
	if rerr, ok := err.(interface{ Resource() string }); ok {
		resp.Resource = rerr.Resource()
	}

	return resp
}

func ResourceError(code ErrorCode, resource string) error {
	return &resourceError{
		code:     code,
		resource: resource,
	}
}

func ErrorMessage(code ErrorCode, message string) error {
	return &messageError{
		code:    code,
		message: message,
	}
}

func BucketNotFound(bucket string) error {
	return &resourceError{code: ErrNoSuchBucket, resource: bucket}
}

func KeyNotFound(key string) error {
	return &resourceError{code: ErrNoSuchKey, resource: key}
}

type ErrorCode string

var _ Error = ErrorCode("")

func (e ErrorCode) Error() string   { return string(e) }
func (e ErrorCode) Code() ErrorCode { return e }

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

type resourceError struct {
	code     ErrorCode
	resource string // The bucket or object that is involved in the error.
}

var _ Error = &resourceError{}

func (re *resourceError) Code() ErrorCode  { return re.code }
func (re *resourceError) Error() string    { return fmt.Sprintf("%s: %s", re.code, re.resource) }
func (re *resourceError) Message() string  { return "" }
func (re *resourceError) Resource() string { return re.resource }

type messageError struct {
	code    ErrorCode
	message string
}

var _ Error = &messageError{}

func (re *messageError) Code() ErrorCode { return re.code }
func (re *messageError) Error() string   { return fmt.Sprintf("%s: %s", re.code, re.message) }
func (re *messageError) Message() string { return re.message }

func HasErrorCode(err error, code ErrorCode) bool {
	s3err, ok := err.(Error)
	if !ok {
		return false
	}
	return s3err.Code() == code
}

func IsErrExist(err error) bool {
	return HasErrorCode(err, ErrBucketAlreadyExists)
}
