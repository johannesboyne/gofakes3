package gofakes3

import (
	"encoding/xml"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Storage struct {
	XMLName xml.Name  `xml:"ListAllMyBucketsResult"`
	Xmlns   string    `xml:"xmlns,attr"`
	Owner   *UserInfo `xml:"Owner,omitempty"`
	Buckets Buckets   `xml:"Buckets>Bucket"`
}

type UserInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Buckets []BucketInfo

// Names is a deterministic convenience function returning a sorted list of bucket names.
func (b Buckets) Names() []string {
	out := make([]string, len(b))
	for i, v := range b {
		out[i] = v.Name
	}
	sort.Strings(out)
	return out
}

// BucketInfo represents a single bucket returned by the ListBuckets response.
type BucketInfo struct {
	Name string `xml:"Name"`

	// CreationDate is required; without it, boto returns the error "('String
	// does not contain a date:', '')"
	CreationDate ContentTime `xml:"CreationDate"`
}

// CommonPrefix is used in Bucket.CommonPrefixes to list partial delimited keys
// that represent pseudo-directories.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type CompletedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadRequest struct {
	Parts []CompletedPart `xml:"Part"`
}

func (c CompleteMultipartUploadRequest) partsAreSorted() bool {
	return sort.IntsAreSorted(c.partIDs())
}

func (c CompleteMultipartUploadRequest) partIDs() []int {
	inParts := make([]int, 0, len(c.Parts))
	for _, inputPart := range c.Parts {
		inParts = append(inParts, inputPart.PartNumber)
	}
	sort.Ints(inParts)
	return inParts
}

type CompleteMultipartUploadResult struct {
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	ETag     string `xml:"ETag"`
}

type Content struct {
	Key          string       `xml:"Key"`
	LastModified ContentTime  `xml:"LastModified"`
	ETag         string       `xml:"ETag"`
	Size         int64        `xml:"Size"`
	StorageClass StorageClass `xml:"StorageClass"`
}

type ContentTime struct {
	time.Time
}

func NewContentTime(t time.Time) ContentTime {
	return ContentTime{t}
}

func (c ContentTime) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	// This is the format expected by the aws xml code, not the default.
	if !c.IsZero() {
		var s = c.Format("2006-01-02T15:04:05.999Z")
		return e.EncodeElement(s, start)
	}
	return nil
}

type DeleteRequest struct {
	Objects []ObjectID `xml:"Object"`

	// Element to enable quiet mode for the request. When you add this element,
	// you must set its value to true.
	//
	// By default, the operation uses verbose mode in which the response
	// includes the result of deletion of each key in your request. In quiet
	// mode the response includes only keys where the delete operation
	// encountered an error. For a successful deletion, the operation does not
	// return any information about the delete in the response body.
	Quiet bool `xml:"Quiet"`
}

// MultiDeleteResult contains the response from a multi delete operation.
type MultiDeleteResult struct {
	XMLName xml.Name      `xml:"DeleteResult"`
	Deleted []ObjectID    `xml:"Deleted"`
	Error   []ErrorResult `xml:",omitempty"`
}

func (d MultiDeleteResult) AsError() error {
	if len(d.Error) == 0 {
		return nil
	}
	var strs = make([]string, 0, len(d.Error))
	for _, er := range d.Error {
		strs = append(strs, er.String())
	}
	return fmt.Errorf("gofakes3: multi delete failed:\n%s", strings.Join(strs, "\n"))
}

type ErrorResult struct {
	XMLName   xml.Name  `xml:"Error"`
	Key       string    `xml:"Key,omitempty"`
	Code      ErrorCode `xml:"Code,omitempty"`
	Message   string    `xml:"Message,omitempty"`
	Resource  string    `xml:"Resource,omitempty"`
	RequestID string    `xml:"RequestId,omitempty"`
}

func ErrorResultFromError(err error) ErrorResult {
	switch err := err.(type) {
	case *resourceErrorResponse:
		return ErrorResult{
			Resource:  err.Resource,
			RequestID: err.RequestID,
			Message:   err.Message,
			Code:      err.Code,
		}
	case *ErrorResponse:
		return ErrorResult{
			RequestID: err.RequestID,
			Message:   err.Message,
			Code:      err.Code,
		}
	case Error:
		return ErrorResult{Code: err.ErrorCode()}
	default:
		return ErrorResult{Code: ErrInternal}
	}
}

func (er ErrorResult) String() string {
	return fmt.Sprintf("%s: [%s] %s", er.Key, er.Code, er.Message)
}

type InitiateMultipartUpload struct {
	UploadID UploadID `xml:"UploadId"`
}

type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	Contents       []*Content     `xml:"Contents"`

	// prefixes maintains an index of prefixes that have already been seen.
	// This is a convenience for backend implementers like s3bolt and s3mem,
	// which operate on a full, flat list of keys.
	prefixes map[string]bool
}

func NewListBucketResult(name string) *ListBucketResult {
	return &ListBucketResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:  name,
	}
}

func (b *ListBucketResult) Add(item *Content) {
	b.Contents = append(b.Contents, item)
}

func (b *ListBucketResult) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = map[string]bool{}
	} else if b.prefixes[prefix] {
		return
	}
	b.prefixes[prefix] = true
	b.CommonPrefixes = append(b.CommonPrefixes, CommonPrefix{Prefix: prefix})
}

type DeleteMarker struct {
	XMLName      xml.Name    `xml:"DeleteMarker"`
	Key          string      `xml:"Key"`
	VersionID    VersionID   `xml:"VersionId"`
	IsLatest     bool        `xml:"IsLatest"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
	Owner        *UserInfo   `xml:"Owner,omitempty"`
}

func (d DeleteMarker) isVersionItem() {}

type Version struct {
	XMLName      xml.Name    `xml:"Version"`
	Key          string      `xml:"Key"`
	VersionID    VersionID   `xml:"VersionId"`
	IsLatest     bool        `xml:"IsLatest"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
	Size         int64       `xml:"Size"`

	// According to the S3 docs, this is always STANDARD for a Version:
	StorageClass StorageClass `xml:"StorageClass"`

	ETag  string    `xml:"ETag"`
	Owner *UserInfo `xml:"Owner,omitempty"`
}

func (v Version) isVersionItem() {}

type VersionItem interface {
	isVersionItem()
}

type ListBucketVersionsResult struct {
	XMLName        xml.Name       `xml:"ListBucketVersionsResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	Prefix         string         `xml:"Prefix,omitempty"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	MaxKeys        int64          `xml:"MaxKeys"`

	// Marks the last Key returned in a truncated response.
	KeyMarker string `xml:"KeyMarker,omitempty"`

	// When the number of responses exceeds the value of MaxKeys, NextKeyMarker
	// specifies the first key not returned that satisfies the search criteria.
	// Use this value for the key-marker request parameter in a subsequent
	// request.
	NextKeyMarker string `xml:"NextKeyMarker,omitempty"`

	// Marks the last version of the Key returned in a truncated response.
	VersionIDMarker VersionID `xml:"VersionIdMarker,omitempty"`

	// When the number of responses exceeds the value of MaxKeys,
	// NextVersionIdMarker specifies the first object version not returned that
	// satisfies the search criteria. Use this value for the version-id-marker
	// request parameter in a subsequent request.
	NextVersionIDMarker VersionID `xml:"NextVersionIdMarker,omitempty"`

	// AWS responds with a list of either <Version> or <DeleteMarker> objects. The order
	// needs to be preserved and they need to be direct of ListBucketVersionsResult:
	//	<ListBucketVersionsResult>
	//		<DeleteMarker ... />
	//		<Version ... />
	//		<DeleteMarker ... />
	//		<Version ... />
	//	</ListBucketVersionsResult>
	Versions []VersionItem

	// prefixes maintains an index of prefixes that have already been seen.
	// This is a convenience for backend implementers like s3bolt and s3mem,
	// which operate on a full, flat list of keys.
	prefixes map[string]bool
}

func NewListBucketVersionsResult(
	bucketName string,
	prefix Prefix,
	page ListBucketVersionsPage,
) *ListBucketVersionsResult {
	return &ListBucketVersionsResult{
		Xmlns:           "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:            bucketName,
		Prefix:          prefix.Prefix,
		Delimiter:       prefix.Delimiter,
		MaxKeys:         page.MaxKeys,
		KeyMarker:       page.KeyMarker,
		VersionIDMarker: page.VersionIDMarker,
	}
}

func (b *ListBucketVersionsResult) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = map[string]bool{}
	} else if b.prefixes[prefix] {
		return
	}
	b.prefixes[prefix] = true
	b.CommonPrefixes = append(b.CommonPrefixes, CommonPrefix{Prefix: prefix})
}

type ListMultipartUploadsResult struct {
	Bucket string `xml:"Bucket"`

	// Together with upload-id-marker, this parameter specifies the multipart upload
	// after which listing should begin.
	KeyMarker string `xml:"KeyMarker,omitempty"`

	// Together with key-marker, specifies the multipart upload after which listing
	// should begin. If key-marker is not specified, the upload-id-marker parameter
	// is ignored.
	UploadIDMarker UploadID `xml:"UploadIdMarker,omitempty"`

	NextKeyMarker      string   `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker UploadID `xml:"NextUploadIdMarker,omitempty"`

	// Sets the maximum number of multipart uploads, from 1 to 1,000, to return
	// in the response body. 1,000 is the maximum number of uploads that can be
	// returned in a response.
	MaxUploads int64 `xml:"MaxUploads,omitempty"`

	Delimiter string `xml:"Delimiter,omitempty"`

	// Lists in-progress uploads only for those keys that begin with the specified
	// prefix.
	Prefix string `xml:"Prefix,omitempty"`

	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated,omitempty"`

	Uploads []ListMultipartUploadItem `xml:"Upload"`
}

type ListMultipartUploadItem struct {
	Key          string       `xml:"Key"`
	UploadID     UploadID     `xml:"UploadId"`
	Initiator    *UserInfo    `xml:"Initiator,omitempty"`
	Owner        *UserInfo    `xml:"Owner,omitempty"`
	StorageClass StorageClass `xml:"StorageClass,omitempty"`
	Initiated    ContentTime  `xml:"Initiated,omitempty"`
}

type ListMultipartUploadPartsResult struct {
	XMLName xml.Name `xml:"ListPartsResult"`

	Bucket               string       `xml:"Bucket"`
	Key                  string       `xml:"Key"`
	UploadID             UploadID     `xml:"UploadId"`
	StorageClass         StorageClass `xml:"StorageClass,omitempty"`
	Initiator            *UserInfo    `xml:"Initiator,omitempty"`
	Owner                *UserInfo    `xml:"Owner,omitempty"`
	PartNumberMarker     int          `xml:"PartNumberMarker"`
	NextPartNumberMarker int          `xml:"NextPartNumberMarker"`
	MaxParts             int64        `xml:"MaxParts"`
	IsTruncated          bool         `xml:"IsTruncated,omitempty"`

	Parts []ListMultipartUploadPartItem `xml:"Part"`
}

type ListMultipartUploadPartItem struct {
	PartNumber   int         `xml:"PartNumber"`
	LastModified ContentTime `xml:"LastModified,omitempty"`
	ETag         string      `xml:"ETag,omitempty"`
	Size         int64       `xml:"Size"`
}

// MFADeleteStatus is used by VersioningConfiguration.
type MFADeleteStatus bool

func (v *MFADeleteStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		// FIXME: this doesn't seem to detect or report errors if the element is the wrong type.
		return err
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "enabled" {
		*v = true
	} else if s == "disabled" {
		*v = false
	} else {
		return ErrorMessagef(ErrIllegalVersioningConfiguration, "unexpected value %q for MFADeleteStatus, expected 'Enabled' or 'Disabled'", s)
	}
	return nil
}

func (v MFADeleteStatus) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if v {
		return e.EncodeElement("Enabled", start)
	} else {
		return e.EncodeElement("Disabled", start)
	}
}

type ObjectID struct {
	Key string `xml:"Key"`

	// Versions not supported in GoFakeS3 yet.
	VersionID string `xml:"VersionId,omitempty" json:"VersionId,omitempty"`
}

type StorageClass string

func (s StorageClass) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if s == "" {
		s = StorageStandard
	}
	return e.EncodeElement(string(s), start)
}

const (
	StorageStandard StorageClass = "STANDARD"
)

// UploadID uses a string as the underlying type, but the string should only
// represent a decimal integer. See uploader.uploadID for details.
type UploadID string

type VersionID string

type VersioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`

	Status VersioningStatus `xml:"Status"`

	// When enabled, the bucket owner must include the x-amz-mfa request header
	// in requests to change the versioning state of a bucket and to
	// permanently delete a versioned object.
	MFADelete MFADeleteStatus `xml:"MfaDelete"`
}

func (v *VersioningConfiguration) Enabled() bool {
	return v.Status == VersioningEnabled
}

func (v *VersioningConfiguration) SetEnabled(enabled bool) {
	if enabled {
		v.Status = VersioningEnabled
	} else {
		v.Status = VersioningSuspended
	}
}

type VersioningStatus string

func (v *VersioningStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		// FIXME: this doesn't seem to detect or report errors if the element is the wrong type.
		return err
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "enabled" {
		*v = VersioningEnabled
	} else if s == "suspended" {
		*v = VersioningSuspended
	} else {
		return ErrorMessagef(ErrIllegalVersioningConfiguration, "unexpected value %q for Status, expected 'Enabled' or 'Suspended'", s)
	}
	return nil
}

const (
	VersioningEnabled   VersioningStatus = "Enabled"
	VersioningSuspended VersioningStatus = "Suspended"
)
