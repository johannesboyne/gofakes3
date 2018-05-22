package gofakes3

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
)

type GoFakeS3 struct {
	storage      *bolt.DB
	timeLocation *time.Location
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
type Object struct {
	Metadata map[string]string
	Obj      []byte
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

	return &GoFakeS3{storage: db, timeLocation: timeLocation}
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
	var buckets []BucketInfo
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>`))
	err := g.storage.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			buckets = append(buckets, BucketInfo{string(name), ""})
			return nil
		})
	})
	s := &Storage{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Id:          "fe7272ea58be830e56fe1663b10fafef",
		DisplayName: "GoFakeS3",
		Buckets:     buckets,
	}
	x, err := xml.MarshalIndent(s, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(x)
}

// GetBucket lists the contents of a bucket.
func (g *GoFakeS3) GetBucket(w http.ResponseWriter, r *http.Request) {
	log.Println("GET BUCKET")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]

	log.Println("bucketname:", bucketName)
	log.Println("prefix    :", r.URL.Query().Get("prefix"))

	g.storage.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			http.Error(w, "No bucket", http.StatusNotFound)
			return nil
		}
		c := b.Cursor()
		bucketc := &Bucket{
			Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:     "crowdpatent.com",
			Prefix:   r.URL.Query().Get("prefix"),
			Marker:   "",
			Contents: []*Content{},
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if strings.Contains(string(k), r.URL.Query().Get("prefix")) {
				hash := md5.Sum(v)
				bucketc.Contents = append(bucketc.Contents, &Content{
					Key:          string(k),
					LastModified: g.timeNow().Format(time.RFC3339),
					ETag:         "\"" + hex.EncodeToString(hash[:]) + "\"",
					Size:         len(v),
					StorageClass: "STANDARD",
				})
				t := Object{}
				err := bson.Unmarshal(v, &t)
				if err != nil {
					panic(err)
				}
			}
		}

		x, err := xml.MarshalIndent(bucketc, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil
		}
		w.Write(x)
		return nil
	})
}

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) CreateBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("CREATE BUCKET:", bucketName)

	g.storage.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(bucketName))
		if err != nil {
			http.Error(w, "bucket existed", http.StatusBadRequest)
			return err
		}
		log.Println("bucket created")
		w.Header().Set("Host", r.Header.Get("Host"))
		w.Header().Set("Location", "/"+bucketName)
		w.Write([]byte{})
		return nil
	})
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
	g.storage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusNotFound)
		}
		w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
		w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
		w.Header().Set("Server", "AmazonS3")
		w.Write([]byte{})
		return nil
	})
}

// GetObject retrievs a bucket object.
func (g *GoFakeS3) GetObject(w http.ResponseWriter, r *http.Request) {
	log.Println("GET OBJECT")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	g.storage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusNotFound)
		}
		v := b.Get([]byte(vars["ObjectName"]))

		if v == nil {
			log.Println("no object")
			http.Error(w, "object does not exist", http.StatusInternalServerError)
			return nil
		}
		t := Object{}
		err := bson.Unmarshal(v, &t)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		hash := md5.Sum(t.Obj)
		w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
		w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
		for mk, mv := range t.Metadata {
			w.Header().Set(mk, mv)
		}
		w.Header().Set("Last-Modified", g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST"))
		w.Header().Set("ETag", "\""+hex.EncodeToString(hash[:])+"\"")
		w.Header().Set("Server", "AmazonS3")
		w.Header().Set("Content-Length", fmt.Sprintf("%v", len(t.Obj)))
		w.Header().Set("Connection", "close")
		w.Write(t.Obj)
		return nil
	})
}

// CreateObject (Browser Upload) creates a new S3 object.
func (g *GoFakeS3) CreateObjectBrowserUpload(w http.ResponseWriter, r *http.Request) {
	log.Println("CREATE OBJECT THROUGH BROWSER UPLOAD")
	const _24K = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24K); nil != err {
		panic(err)
	}
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	key := r.MultipartForm.Value["key"][0]

	log.Println("(BUC)", bucketName)
	log.Println("(KEY)", key)
	fileHeader := r.MultipartForm.File["file"][0]
	infile, err := fileHeader.Open()
	if nil != err {
		panic(err)
	}
	body, err := ioutil.ReadAll(infile)
	if err != nil {
		panic(err)
	}

	meta := make(map[string]string)
	log.Println(r.MultipartForm)
	for hk, hv := range r.MultipartForm.Value {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST")

	obj := &Object{meta, body}

	g.storage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusNotFound)
			return nil
		}
		log.Println("bucket", bucketName, "found")
		data, err := bson.Marshal(obj)
		if err != nil {
			panic(err)
		}
		err = b.Put([]byte(key), data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("error while creating")
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
		w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
		w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
		w.Header().Set("Server", "AmazonS3")
		w.Write([]byte{})
		return nil
	})
}

// CreateObject creates a new S3 object.
func (g *GoFakeS3) CreateObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]

	log.Println("CREATE OBJECT:", bucketName, vars["ObjectName"])
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	meta := make(map[string]string)
	for hk, hv := range r.Header {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST")

	obj := &Object{meta, body}

	g.storage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusNotFound)
			return nil
		}
		log.Println("bucket", bucketName, "found")
		data, err := bson.Marshal(obj)
		if err != nil {
			panic(err)
		}
		err = b.Put([]byte(vars["ObjectName"]), data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("error while creating")
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
		w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
		w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
		w.Header().Set("Server", "AmazonS3")
		w.Write([]byte{})
		return nil
	})
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
	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	g.storage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusNotFound)
		}
		v := b.Get([]byte(vars["ObjectName"]))

		if v == nil {
			log.Println("no object")
			http.Error(w, "object does not exist", http.StatusInternalServerError)
		}
		t := Object{}
		err := bson.Unmarshal(v, &t)
		if err != nil {
			panic(err)
		}
		hash := md5.Sum(t.Obj)
		w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
		w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
		for mk, mv := range t.Metadata {
			w.Header().Set(mk, mv)
		}
		w.Header().Set("Last-Modified", t.Metadata["Last-Modified"])
		w.Header().Set("ETag", "\""+hex.EncodeToString(hash[:])+"\"")
		w.Header().Set("Server", "AmazonS3")
		w.Write([]byte{})
		return nil
	})
}

func (g *GoFakeS3) timeNow() time.Time {
	return time.Now().In(g.timeLocation)
}
