package gofakes3

import (
	"bytes"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
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

	MaxUploadsLimit       = 1000
	DefaultMaxUploads     = 1000
	MaxUploadPartsLimit   = 1000
	DefaultMaxUploadParts = 1000

	// From the docs: "Part numbers can be any number from 1 to 10,000, inclusive."
	MaxUploadPartNumber = 10000
)

type GoFakeS3 struct {
	storage           Backend
	timeSource        TimeSource
	timeSkew          time.Duration
	metadataSizeLimit int
	integrityCheck    bool
	uploader          *uploader
}

// Setup a new fake object storage
func New(backend Backend, options ...Option) *GoFakeS3 {
	log.Println("locals3 db created or opened")

	s3 := &GoFakeS3{
		storage:           backend,
		timeSkew:          DefaultSkewLimit,
		metadataSizeLimit: DefaultMetadataSizeLimit,
		integrityCheck:    true,
		uploader:          newUploader(),
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
	wc := &WithCORS{http.HandlerFunc(g.routeBase)}

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

func (g *GoFakeS3) httpError(w http.ResponseWriter, r *http.Request, err error) {
	resp := ensureErrorResponse(err, "") // FIXME: request id
	if resp.ErrorCode() == ErrInternal {
		log.Println(err)
	}

	w.WriteHeader(resp.ErrorCode().Status())

	if r.Method != http.MethodHead {
		w.Header().Set("Content-Type", "application/xml")

		w.Write([]byte(xml.Header))
		if err := g.xmlEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			log.Println(err)
			return
		}
	}
}

// Get a list of all Buckets
func (g *GoFakeS3) getBuckets(w http.ResponseWriter, r *http.Request) error {
	buckets, err := g.storage.ListBuckets()
	if err != nil {
		return err
	}

	s := &Storage{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: buckets,
		Owner: &UserInfo{
			ID:          "fe7272ea58be830e56fe1663b10fafef",
			DisplayName: "GoFakeS3",
		},
	}
	x, err := xml.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))
	w.Write(x)
	return nil
}

// GetBucket lists the contents of a bucket.
func (g *GoFakeS3) getBucket(bucketName string, w http.ResponseWriter, r *http.Request) error {
	log.Println("GET BUCKET")

	prefix := prefixFromQuery(r.URL.Query())

	log.Println("bucketname:", bucketName)
	log.Println("prefix    :", prefix)

	bucket, err := g.storage.GetBucket(bucketName, prefix)
	if err != nil {
		return err
	}

	x, err := xml.MarshalIndent(bucket, "", "  ")
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))
	w.Write(x)
	return nil
}

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) createBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	log.Println("CREATE BUCKET:", bucket)

	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	if err := g.storage.CreateBucket(bucket); err != nil {
		return err
	}

	w.Header().Set("Host", r.Header.Get("Host"))
	w.Header().Set("Location", "/"+bucket)
	w.Write([]byte{})
	return nil
}

// DeleteBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) deleteBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	log.Println("DELETE BUCKET:", bucket)
	return g.storage.DeleteBucket(bucket)
}

// HeadBucket checks whether a bucket exists.
func (g *GoFakeS3) headBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	log.Println("HEAD BUCKET", bucket)
	log.Println("bucketname:", bucket)

	exists, err := g.storage.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return ResourceError(ErrNoSuchBucket, bucket)
	}

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})
	return nil
}

// GetObject retrievs a bucket object.
func (g *GoFakeS3) getObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	log.Println("GET OBJECT")

	log.Println("Bucket:", bucket)
	log.Println("└── Object:", object)

	obj, err := g.storage.GetObject(bucket, object)
	if err != nil {
		return err
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
		return err
	}

	return nil
}

// CreateObject (Browser Upload) creates a new S3 object.
func (g *GoFakeS3) createObjectBrowserUpload(bucket string, w http.ResponseWriter, r *http.Request) error {
	log.Println("CREATE OBJECT THROUGH BROWSER UPLOAD")
	const _24MB = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24MB); nil != err {
		// Could also use MaxMessageLengthExceeded:
		return ErrMalformedPOSTRequest
	}

	keyValues := r.MultipartForm.Value["key"]
	if len(keyValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	key := keyValues[0]

	log.Println("(BUC)", bucket)
	log.Println("(KEY)", key)

	fileValues := r.MultipartForm.File["file"]
	if len(fileValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	fileHeader := fileValues[0]

	infile, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer infile.Close()

	meta, err := metadataHeaders(r.MultipartForm.Value, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	if len(key) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, key)
	}

	if err := g.storage.PutObject(bucket, key, meta, infile); err != nil {
		return err
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})

	return nil
}

// CreateObject creates a new S3 object.
func (g *GoFakeS3) createObject(bucket, object string, w http.ResponseWriter, r *http.Request) (err error) {
	log.Println("CREATE OBJECT:", bucket, object)

	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}

	defer r.Body.Close()
	var rdr io.Reader = r.Body

	if g.integrityCheck {
		md5Base64 := r.Header.Get("Content-MD5")
		if md5Base64 != "" {
			rdr, err = newHashingReader(rdr, md5Base64)
			if err != nil {
				return err
			}
		}
	}

	if err := g.storage.PutObject(bucket, object, meta, rdr); err != nil {
		return err
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})

	return nil
}

// deleteObject deletes a S3 object from the bucket.
func (g *GoFakeS3) deleteObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	log.Println("DELETE:", bucket, object)
	if err := g.storage.DeleteObject(bucket, object); err != nil {
		return err
	}
	w.Header().Set("x-amz-delete-marker", "false")
	w.Write([]byte{})
	return nil
}

// deleteMulti deletes multiple S3 objects from the bucket.
// https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
func (g *GoFakeS3) deleteMulti(bucket string, w http.ResponseWriter, r *http.Request) error {
	log.Println("delete multi", bucket)

	var in DeleteRequest

	defer r.Body.Close()
	dc := xml.NewDecoder(r.Body)
	if err := dc.Decode(&in); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}

	keys := make([]string, len(in.Objects))
	for i, o := range in.Objects {
		keys[i] = o.Key
	}

	out, err := g.storage.DeleteMulti(bucket, keys...)
	if err != nil {
		return err
	}

	if in.Quiet {
		out.Deleted = nil
	}

	x, err := xml.MarshalIndent(&out, "", "  ")
	if err != nil {
		return err
	}

	w.Write([]byte(xml.Header))
	w.Write(x)
	return nil
}

// HeadObject retrieves only meta information of an object and not the whole.
func (g *GoFakeS3) headObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	log.Println("HEAD OBJECT")

	log.Println("Bucket:", bucket)
	log.Println("└── Object:", object)

	obj, err := g.storage.HeadObject(bucket, object)
	if err != nil {
		return err
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

	return nil
}

func (g *GoFakeS3) initiateMultipartUpload(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	log.Println("initiate multipart upload", bucket, object)

	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	upload := g.uploader.Begin(bucket, object, meta, g.timeSource.Now())
	out := InitiateMultipartUpload{UploadID: upload.ID}
	return g.xmlEncoder(w).Encode(out)
}

// From the docs:
//	A part number uniquely identifies a part and also defines its position
// 	within the object being created. If you upload a new part using the same
// 	part number that was used with a previous part, the previously uploaded part
// 	is overwritten. Each part must be at least 5 MB in size, except the last
// 	part. There is no size limit on the last part of your multipart upload.
//
func (g *GoFakeS3) putMultipartUploadPart(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	log.Println("put multipart upload", bucket, object, uploadID)

	etag := r.Header.Get("ETag")
	if !validETag(etag) {
		return ErrInvalidPart
	}

	partNumber, err := strconv.ParseInt(r.URL.Query().Get("partNumber"), 10, 0)
	if err != nil || partNumber <= 0 || partNumber > MaxUploadPartNumber {
		return ErrInvalidPart
	}

	// FIXME: this could be a global check.
	if r.Header.Get("Content-Length") == "" {
		return ErrMissingContentLength
	}

	upload, err := g.uploader.Get(bucket, object, uploadID)
	if err != nil {
		// FIXME: What happens with S3 when you abort a multipart upload while
		// part uploads are still in progress? In this case, we will retain the
		// reference to the part even though another request goroutine may
		// delete it; it will be available for GC when this function finishes.
		return err
	}

	defer r.Body.Close()
	var rdr io.Reader = r.Body

	if g.integrityCheck {
		md5Base64 := r.Header.Get("Content-MD5")
		if md5Base64 != "" {
			var err error
			rdr, err = newHashingReader(rdr, md5Base64)
			if err != nil {
				return err
			}
		}
	}

	body, err := ioutil.ReadAll(rdr)
	if err != nil {
		return err
	}

	if int64(len(body)) != r.ContentLength {
		return ErrIncompleteBody
	}

	w.Header().Add("ETag", etag)

	return upload.AddPart(int(partNumber), etag, g.timeSource.Now(), body)
}

func (g *GoFakeS3) abortMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	log.Println("abort multipart upload", bucket, object, uploadID)
	_, err := g.uploader.Complete(bucket, object, uploadID)
	return err
}

func (g *GoFakeS3) completeMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	log.Println("complete multipart upload", bucket, object, uploadID)

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		return err
	}

	var in CompleteMultipartUploadRequest
	if err := xml.Unmarshal(body, &in); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}

	upload, err := g.uploader.Complete(bucket, object, uploadID)
	if err != nil {
		return err
	}

	fileBody, etag, err := upload.Reassemble(&in)
	if err != nil {
		return err
	}

	if err := g.storage.PutObject(bucket, object, upload.Meta, bytes.NewReader(fileBody)); err != nil {
		return err
	}

	return g.xmlEncoder(w).Encode(&CompleteMultipartUploadResult{
		ETag:   etag,
		Bucket: bucket,
		Key:    object,
	})
}

func (g *GoFakeS3) listMultipartUploads(bucket string, w http.ResponseWriter, r *http.Request) error {
	query := r.URL.Query()
	prefix := prefixFromQuery(query)
	marker := uploadListMarkerFromQuery(query)

	maxUploads, err := parseClampedInt(query.Get("max-uploads"), DefaultMaxUploads, 0, MaxUploadsLimit)
	if err != nil {
		return ErrInvalidURI
	}

	out, err := g.uploader.List(bucket, marker, prefix, maxUploads)
	if err != nil {
		return err
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *GoFakeS3) listMultipartUploadParts(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	query := r.URL.Query()

	marker, err := parseClampedInt(query.Get("part-number-marker"), 0, 0, math.MaxInt64)
	if err != nil {
		return ErrInvalidURI
	}

	maxParts, err := parseClampedInt(query.Get("max-parts"), DefaultMaxUploadParts, 0, MaxUploadPartsLimit)
	if err != nil {
		return ErrInvalidURI
	}

	out, err := g.uploader.ListParts(bucket, object, uploadID, int(marker), maxParts)
	if err != nil {
		return err
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *GoFakeS3) xmlEncoder(w io.Writer) *xml.Encoder {
	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")
	return xe
}

func formatHeaderTime(t time.Time) string {
	// https://github.com/aws/aws-sdk-go/issues/1937 - FIXED
	// https://github.com/aws/aws-sdk-go-v2/issues/178 - Still open
	// .Format("Mon, 2 Jan 2006 15:04:05 MST")

	tc := t.In(time.UTC)
	return tc.Format("Mon, 02 Jan 2006 15:04:05") + " GMT"
}

func metadataSize(meta map[string]string) int {
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	return total
}

func metadataHeaders(headers map[string][]string, at time.Time, sizeLimit int) (map[string]string, error) {
	meta := make(map[string]string)
	for hk, hv := range headers {
		if strings.HasPrefix(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = formatHeaderTime(at)

	if sizeLimit > 0 && metadataSize(meta) > sizeLimit {
		return meta, ErrMetadataTooLarge
	}

	return meta, nil
}
