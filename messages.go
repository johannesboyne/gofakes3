package gofakes3

import (
	"encoding/xml"
	"io"
	"time"
)

type Storage struct {
	XMLName     xml.Name     `xml:"ListAllMyBucketsResult"`
	Xmlns       string       `xml:"xmlns,attr"`
	Id          string       `xml:"Owner>ID"`
	DisplayName string       `xml:"Owner>DisplayName"`
	Buckets     []BucketInfo `xml:"Buckets"`
}

type BucketInfo struct {
	Name         string `xml:"Bucket>Name"`
	CreationDate string `xml:"Bucket>CreationDate"`
}

type Content struct {
	Key          string      `xml:"Key"`
	LastModified ContentTime `xml:"LastModified"`
	ETag         string      `xml:"ETag"`
	Size         int         `xml:"Size"`
	StorageClass string      `xml:"StorageClass"`
}

type ContentTime time.Time

func (c ContentTime) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	// This is the format expected by the aws xml code, not the default.
	var s = time.Time(c).Format("2006-01-02T15:04:05Z")
	return e.EncodeElement(s, start)
}

type Bucket struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	CommonPrefixes []BucketPrefix `xml:"CommonPrefixes,omitempty"`
	Contents       []*Content     `xml:"Contents"`

	// prefixes maintains an index of prefixes that have already been seen.
	// This is a convenience for backend implementers like s3bolt and s3mem,
	// which operate on a full, flat list of keys.
	prefixes map[string]bool
}

func NewBucket(name string) *Bucket {
	return &Bucket{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:  name,
	}
}

func (b *Bucket) Add(item *Content) {
	if item.StorageClass == "" {
		item.StorageClass = "STANDARD"
	}
	b.Contents = append(b.Contents, item)
}

func (b *Bucket) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = map[string]bool{}
	} else if b.prefixes[prefix] {
		return
	}
	b.prefixes[prefix] = true
	b.CommonPrefixes = append(b.CommonPrefixes, BucketPrefix{Prefix: prefix})
}

type BucketPrefix struct {
	Prefix string
}

type Object struct {
	Metadata map[string]string
	Size     int64
	Contents io.ReadCloser
	Hash     []byte
}
