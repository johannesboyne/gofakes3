package gofakes3

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
)

type GoFakeS3 struct {
	storage    Backend
	timeSource TimeSource
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
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type Bucket struct {
	XMLName  xml.Name   `xml:"ListBucketResult"`
	Xmlns    string     `xml:"xmlns,attr"`
	Name     string     `xml:"Name"`
	Prefix   string     `xml:"Prefix"`
	Marker   string     `xml:"Marker"`
	Contents []*Content `xml:"Contents"`
}

func newBucket(name string) *Bucket {
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

type TimeSource interface {
	Now() time.Time
}

type locatedTimeSource struct {
	timeLocation *time.Location
}

func (l *locatedTimeSource) Now() time.Time {
	return time.Now().In(l.timeLocation)
}

// Setup a new fake object storage
func New(dbname string) *GoFakeS3 {
	db, err := bolt.Open(dbname, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("locals3 db created or opened")

	timeLocation, err := time.LoadLocation("GMT")
	if err != nil {
		log.Fatal(err)
	}
	timeSource := &locatedTimeSource{
		timeLocation: timeLocation,
	}

	backend := &BoltDBBackend{
		timeSource: timeSource,
		bolt:       db,
	}

	return &GoFakeS3{timeSource: timeSource, storage: backend}
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

	return &WithCORS{r}
}

type WithCORS struct {
	r *mux.Router
}

func (s *WithCORS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, HEAD")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-Amz-User-Agent, X-Amz-Date, x-amz-meta-from, x-amz-meta-to, x-amz-meta-filename, x-amz-meta-private")
	w.Header().Set("Content-Type", "application/xml")

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

// Get a list of all Buckets
func (g *GoFakeS3) GetBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := g.storage.ListBuckets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>`))
	s := &Storage{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Id:          "fe7272ea58be830e56fe1663b10fafef",
		DisplayName: "GoFakeS3",
		Buckets:     buckets,
	}
	x, err := xml.MarshalIndent(s, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(x)
}

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) CreateBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("CREATE BUCKET:", bucketName)

	if err := g.storage.CreateBucket(bucketName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("bucket created")
	w.Header().Set("Host", r.Header.Get("Host"))
	w.Header().Set("Location", "/"+bucketName)
	w.Write([]byte{})
}

// DeleteBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "delete bucket")
}

// HeadBucket checks whether a bucket exists.
func (g *GoFakeS3) HeadBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("HEAD BUCKET", bucketName)
	log.Println("bucketname:", bucketName)

	exists, err := g.storage.BucketExists(bucketName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer obj.Contents.Close()

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}
	w.Header().Set("Last-Modified", g.timeSource.Now().Format("Mon, 2 Jan 2006 15:04:05 MST"))
	w.Header().Set("ETag", "\""+hex.EncodeToString(obj.Hash)+"\"")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Connection", "close")

	if _, err := io.Copy(w, obj.Contents); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// CreateObject (Browser Upload) creates a new S3 object.
func (g *GoFakeS3) CreateObjectBrowserUpload(w http.ResponseWriter, r *http.Request) {
	log.Println("CREATE OBJECT THROUGH BROWSER UPLOAD")
	const _24MB = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24MB); nil != err {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer infile.Close()

	meta := make(map[string]string)
	for hk, hv := range r.MultipartForm.Value {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = g.timeSource.Now().Format("Mon, 2 Jan 2006 15:04:05 MST")

	if err := g.storage.CreateObject(bucketName, key, meta, infile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	meta["Last-Modified"] = g.timeSource.Now().Format("Mon, 2 Jan 2006 15:04:05 MST")

	if err := g.storage.CreateObject(bucketName, objectName, meta, r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	io.WriteString(w, "delete object")
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}
	w.Header().Set("Last-Modified", g.timeSource.Now().Format("Mon, 2 Jan 2006 15:04:05 MST"))
	w.Header().Set("ETag", "\""+hex.EncodeToString(obj.Hash)+"\"")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Connection", "close")
	w.Write([]byte{})
}
