package gofakes3

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

const (
	// From https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html:
	//	"The name for a key is a sequence of Unicode characters whose UTF-8
	//	encoding is at most 1024 bytes long."
	KeySizeLimit = 1024

	// From https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html:
	//	Within the PUT request header, the user-defined metadata is limited to 2
	// 	KB in size. The size of user-defined metadata is measured by taking the
	// 	sum of the number of bytes in the UTF-8 encoding of each key and value.
	//
	// As this does not specify KB or KiB, KB is used; if gofakes3 is used for
	// testing, and your tests show that 2KiB works, but Amazon uses 2KB...
	// that's a much worse time to discover the disparity!
	DefaultMetadataSizeLimit = 2000

	DefaultSkewLimit = 15 * time.Minute
)

type GoFakeS3 struct {
	storage           Backend
	timeSource        TimeSource
	timeSkew          time.Duration
	metadataSizeLimit int
}

type Option func(g *GoFakeS3)

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
	XMLName  xml.Name   `xml:"ListBucketResult"`
	Xmlns    string     `xml:"xmlns,attr"`
	Name     string     `xml:"Name"`
	Prefix   string     `xml:"Prefix"`
	Marker   string     `xml:"Marker"`
	Contents []*Content `xml:"Contents"`
}

func NewBucket(name string) *Bucket {
	return &Bucket{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:  name,
	}
}

type Object struct {
	Metadata map[string]string
	Size     int64
	Contents io.ReadCloser
	Hash     []byte
}

// Setup a new fake object storage
func New(backend Backend, options ...Option) *GoFakeS3 {
	log.Println("locals3 db created or opened")

	s3 := &GoFakeS3{
		storage:           backend,
		timeSkew:          DefaultSkewLimit,
		metadataSizeLimit: DefaultMetadataSizeLimit,
	}
	for _, opt := range options {
		opt(s3)
	}
	if s3.timeSource == nil {
		s3.timeSource = DefaultTimeSource()
	}

	return s3
}

// Create the AWS S3 API
func (g *GoFakeS3) Server() http.Handler {
	r := mux.NewRouter()
	r.Queries("marker", "prefix")
	// BUCKET
	r.HandleFunc("/", g.GetBuckets).Methods("GET")
	r.HandleFunc("/{BucketName}", g.GetBucket).Methods("GET")
	r.HandleFunc("/{BucketName}", g.CreateBucket).Methods("PUT")
	r.HandleFunc("/{BucketName}", g.DeleteBucket).Methods("DELETE")
	r.HandleFunc("/{BucketName}", g.HeadBucket).Methods("HEAD")
	// OBJECT
	r.HandleFunc("/{BucketName}/", g.CreateObjectBrowserUpload).Methods("POST")
	r.HandleFunc("/{BucketName}/{ObjectName:.{1,}}", g.GetObject).Methods("GET")
	r.HandleFunc("/{BucketName}/{ObjectName:.{1,}}", g.CreateObject).Methods("PUT")
	r.HandleFunc("/{BucketName}/{ObjectName:.{0,}}", g.CreateObject).Methods("POST")
	r.HandleFunc("/{BucketName}/{ObjectName:.{1,}}", g.DeleteObject).Methods("DELETE")
	r.HandleFunc("/{BucketName}/{ObjectName:.{0,}}", g.HeadObject).Methods("HEAD")

	wc := &WithCORS{r}
	hf := func(w http.ResponseWriter, rq *http.Request) {
		timeHdr := rq.Header.Get("x-amz-date")

		if g.timeSkew > 0 && timeHdr != "" {
			rqTime, _ := time.Parse("20060102T150405Z", timeHdr)
			at := g.timeSource.Now()
			skew := at.Sub(rqTime)

			if skew < -g.timeSkew || skew > g.timeSkew {
				g.httpError(w, rq, requestTimeTooSkewed(at, g.timeSkew))
				return
			}
		}

		wc.ServeHTTP(w, rq)
	}

	return http.HandlerFunc(hf)
}

type WithCORS struct {
	r *mux.Router
}

func (s *WithCORS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, HEAD")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-Amz-User-Agent, X-Amz-Date, x-amz-meta-from, x-amz-meta-to, x-amz-meta-filename, x-amz-meta-private")

	if r.Method == "OPTIONS" {
		return
	}
	// Bucket name rewriting
	// this is due to some inconsistencies in the AWS SDKs
	re := regexp.MustCompile("(127.0.0.1:\\d{1,7})|(.localhost:\\d{1,7})|(localhost:\\d{1,7})")
	bucket := re.ReplaceAllString(r.Host, "")
	if len(bucket) > 0 {
		log.Println("rewrite bucket ->", bucket)
		p := r.URL.Path
		r.URL.Path = "/" + bucket
		if p != "/" {
			r.URL.Path += p
		}
	}
	log.Println("=>", r.URL)

	s.r.ServeHTTP(w, r)
}

func (g *GoFakeS3) httpError(w http.ResponseWriter, r *http.Request, err error) {
	resp := ensureErrorResponse(err, "") // FIXME: request id
	if resp.ErrorCode() == ErrInternal {
		log.Println(err)
	}

	w.WriteHeader(resp.ErrorCode().Status())

	if r.Method != http.MethodHead {
		w.Header().Set("Content-Type", "application/xml")

		x, err := xml.MarshalIndent(resp, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			log.Println(err)
			return
		}

		w.Write([]byte(xml.Header))
		w.Write(x)
	}
}

// Get a list of all Buckets
func (g *GoFakeS3) GetBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := g.storage.ListBuckets()
	if err != nil {
		g.httpError(w, r, err)
		return
	}

	s := &Storage{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Id:          "fe7272ea58be830e56fe1663b10fafef",
		DisplayName: "GoFakeS3",
		Buckets:     buckets,
	}
	x, err := xml.MarshalIndent(s, "", "  ")
	if err != nil {
		g.httpError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))
	w.Write(x)
}

// GetBucket lists the contents of a bucket.
func (g *GoFakeS3) GetBucket(w http.ResponseWriter, r *http.Request) {
	log.Println("GET BUCKET")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	prefix := r.URL.Query().Get("prefix")

	log.Println("bucketname:", bucketName)
	log.Println("prefix    :", prefix)

	bucket, err := g.storage.GetBucket(bucketName)
	if err != nil {
		g.httpError(w, r, err)
		return
	}

	if prefix != "" {
		idx := 0
		for _, entry := range bucket.Contents {
			if strings.Contains(entry.Key, prefix) {
				bucket.Contents[idx] = entry
				idx++
			}
		}
		bucket.Contents = bucket.Contents[:idx]
	}

	x, err := xml.MarshalIndent(bucket, "", "  ")
	if err != nil {
		g.httpError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))
	w.Write(x)
}

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) CreateBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("CREATE BUCKET:", bucketName)

	if err := ValidateBucketName(bucketName); err != nil {
		g.httpError(w, r, err)
		return
	}

	if err := g.storage.CreateBucket(bucketName); err != nil {
		g.httpError(w, r, err)
		return
	}

	w.Header().Set("Host", r.Header.Get("Host"))
	w.Header().Set("Location", "/"+bucketName)
	w.Write([]byte{})
}

// DeleteBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("DELETE BUCKET:", bucketName)

	if err := g.storage.DeleteBucket(bucketName); err != nil {
		g.httpError(w, r, err)
		return
	}
}

// HeadBucket checks whether a bucket exists.
func (g *GoFakeS3) HeadBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("HEAD BUCKET", bucketName)
	log.Println("bucketname:", bucketName)

	exists, err := g.storage.BucketExists(bucketName)
	if err != nil {
		g.httpError(w, r, err)
		return
	}
	if !exists {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})
}

// GetObject retrievs a bucket object.
func (g *GoFakeS3) GetObject(w http.ResponseWriter, r *http.Request) {
	log.Println("GET OBJECT")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	objectName := vars["ObjectName"]

	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	obj, err := g.storage.GetObject(bucketName, objectName)

	if err != nil {
		g.httpError(w, r, err)
		return
	}
	defer obj.Contents.Close()

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}
	w.Header().Set("Last-Modified", formatHeaderTime(g.timeSource.Now()))
	w.Header().Set("ETag", "\""+hex.EncodeToString(obj.Hash)+"\"")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))

	if _, err := io.Copy(w, obj.Contents); err != nil {
		g.httpError(w, r, err)
		return
	}
}

// CreateObject (Browser Upload) creates a new S3 object.
func (g *GoFakeS3) CreateObjectBrowserUpload(w http.ResponseWriter, r *http.Request) {
	log.Println("CREATE OBJECT THROUGH BROWSER UPLOAD")
	const _24MB = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24MB); nil != err {
		g.httpError(w, r, err)
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	key := r.MultipartForm.Value["key"][0]

	log.Println("(BUC)", bucketName)
	log.Println("(KEY)", key)
	fileHeader := r.MultipartForm.File["file"][0]

	infile, err := fileHeader.Open()
	if err != nil {
		g.httpError(w, r, err)
		return
	}
	defer infile.Close()

	meta := make(map[string]string)
	for hk, hv := range r.MultipartForm.Value {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = formatHeaderTime(g.timeSource.Now())

	if g.metadataSizeLimit > 0 && metadataSize(meta) > g.metadataSizeLimit {
		g.httpError(w, r, ErrMetadataTooLarge)
		return
	}

	if len(key) > KeySizeLimit {
		g.httpError(w, r, ResourceError(ErrKeyTooLong, key))
		return
	}

	if err := g.storage.PutObject(bucketName, key, meta, infile); err != nil {
		g.httpError(w, r, err)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})
}

// CreateObject creates a new S3 object.
func (g *GoFakeS3) CreateObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	objectName := vars["ObjectName"]

	log.Println("CREATE OBJECT:", bucketName, objectName)

	meta := make(map[string]string)
	for hk, hv := range r.Header {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = formatHeaderTime(g.timeSource.Now())

	if len(objectName) > KeySizeLimit {
		g.httpError(w, r, ResourceError(ErrKeyTooLong, objectName))
		return
	}

	if err := g.storage.PutObject(bucketName, objectName, meta, r.Body); err != nil {
		g.httpError(w, r, err)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})
}

// DeleteObject deletes a S3 object from the bucket.
func (g *GoFakeS3) DeleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	objectName := vars["ObjectName"]

	log.Println("DELETE:", bucketName, objectName)

	if err := g.storage.DeleteObject(bucketName, objectName); err != nil {
		g.httpError(w, r, err)
		return
	}

	w.Header().Set("x-amz-delete-marker", "false")
	w.Write([]byte{})
}

// HeadObject retrieves only meta information of an object and not the whole.
func (g *GoFakeS3) HeadObject(w http.ResponseWriter, r *http.Request) {
	log.Println("HEAD OBJECT")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	objectName := vars["ObjectName"]

	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", objectName)

	obj, err := g.storage.HeadObject(bucketName, objectName)
	if err != nil {
		g.httpError(w, r, err)
		return
	}
	defer obj.Contents.Close()

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}
	w.Header().Set("Last-Modified", formatHeaderTime(g.timeSource.Now()))
	w.Header().Set("ETag", "\""+hex.EncodeToString(obj.Hash)+"\"")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Connection", "close")
	w.Write([]byte{})
}

func formatHeaderTime(t time.Time) string {
	// https://github.com/aws/aws-sdk-go/issues/1937 - FIXED
	// https://github.com/aws/aws-sdk-go-v2/issues/178 - Still open
	// .Format("Mon, 2 Jan 2006 15:04:05 MST")

	tc := t.In(time.UTC)
	return tc.Format("Mon, 02 Jan 2006 15:04:05") + " GMT"
}

// This pattern can be used to match both the entire bucket name (including period-
// separated labels) and the individual label components, presuming you have already
// split the string by period.
var bucketNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9\.-]+)[a-z0-9]$`)

// ValidateBucketName applies the rules from the AWS docs:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
//
// 1. Bucket names must comply with DNS naming conventions.
// 2. Bucket names must be at least 3 and no more than 63 characters long.
// 3. Bucket names must not contain uppercase characters or underscores.
// 4. Bucket names must start with a lowercase letter or number.
//
// The DNS RFC confirms that the valid range of characters in an LDH label is 'a-z0-9-':
// https://tools.ietf.org/html/rfc5890#section-2.3.1
//
func ValidateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return ErrorMessage(ErrInvalidBucketName, "bucket name must be >= 3 characters and <= 63")
	}
	if !bucketNamePattern.MatchString(name) {
		return ErrorMessage(ErrInvalidBucketName, "bucket must start and end with 'a-z, 0-9', and contain only 'a-z, 0-9, -' in between")
	}

	if net.ParseIP(name) != nil {
		return ErrorMessage(ErrInvalidBucketName, "bucket names must not be formatted as an IP address")
	}

	// Bucket names must be a series of one or more labels. Adjacent labels are
	// separated by a single period (.). Bucket names can contain lowercase
	// letters, numbers, and hyphens. Each label must start and end with a
	// lowercase letter or a number.
	labels := strings.Split(name, ".")
	for _, label := range labels {
		if !bucketNamePattern.MatchString(label) {
			return ErrorMessage(ErrInvalidBucketName, "label must start and end with 'a-z, 0-9', and contain only 'a-z, 0-9, -' in between")
		}
	}

	return nil
}

func metadataSize(meta map[string]string) int {
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	return total
}
