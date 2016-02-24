//go:generate mockgen -destination mock/s3iface.go  github.com/aws/aws-sdk-go/service/s3/s3iface S3API
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
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
)

type GoFakeS3 struct {
	storage *bolt.DB
}
type Content struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int       `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
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
func New() *GoFakeS3 {
	db, err := bolt.Open("locals3.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("locals3 db created or opened")
	return &GoFakeS3{storage: db}
}

// Start the AWS S3 API, port 9000
func (g *GoFakeS3) StartServer() {
	r := mux.NewRouter()
	r.Queries("marker", "prefix")
	// BUCKET
	r.HandleFunc("/", g.GetBucket).Methods("GET")
	r.HandleFunc("/", g.CreateBucket).Methods("PUT")
	r.HandleFunc("/{BucketName}", g.CreateBucket).Methods("PUT")
	r.HandleFunc("/", g.DeleteBucket).Methods("DELETE")
	r.HandleFunc("/", g.HeadBucket).Methods("HEAD")
	// OBJECT
	r.HandleFunc("/{ObjectName:.{1,}}", g.GetObject).Methods("GET")
	r.HandleFunc("/{ObjectName:.{1,}}", g.CreateObject).Methods("PUT")
	r.HandleFunc("/", g.CreateObject).Methods("POST")
	r.HandleFunc("/{ObjectName:.{1,}}", g.DeleteObject).Methods("DELETE")
	r.HandleFunc("/{ObjectName:.{1,}}", g.HeadObject).Methods("HEAD")

	http.Handle("/", r)
	http.ListenAndServe(":9000", nil)
}

// GetBucket lists the contents of a bucket.
func (g *GoFakeS3) GetBucket(w http.ResponseWriter, r *http.Request) {
	log.Println("GET BUCKET")

	bucketName := strings.Replace(r.Host, ".localhost:9000", "", -1)

	log.Println("bucketname:", bucketName)
	log.Println("prefix    :", r.URL.Query().Get("prefix"))

	g.storage.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			http.Error(w, "No bucket", http.StatusInternalServerError)
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
			fmt.Printf("%v %s %s\n", strings.Contains(string(k), r.URL.Query().Get("prefix")), string(k), r.URL.Query().Get("prefix"))
			if strings.Contains(string(k), r.URL.Query().Get("prefix")) {
				hash := md5.Sum(v)
				bucketc.Contents = append(bucketc.Contents, &Content{
					Key:          string(k),
					LastModified: time.Now(),
					ETag:         "\"" + hex.EncodeToString(hash[:]) + "\"",
					Size:         len(v),
					StorageClass: "STANDARD",
				})
				t := Object{}
				err := bson.Unmarshal(v, &t)
				if err != nil {
					panic(err)
				}
				log.Println(t)
			} else {
				fmt.Printf("%s | %s\n", string(k), r.URL.Query().Get("prefix"))
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
	var bucketName string
	if len(vars["BucketName"]) > 0 {
		bucketName = vars["BucketName"]
	} else {
		bucketName = strings.Replace(r.Host, ".localhost:9000", "", -1)
	}
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

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "delete bucket")
}

// HeadBucket checks whether a bucket exists.
func (g *GoFakeS3) HeadBucket(w http.ResponseWriter, r *http.Request) {
	//@TODO(jb): implement
	vars := mux.Vars(r)
	log.Println("HEAD BUCKET", vars["BucketName"])
	io.WriteString(w, "head bucket")
}

// GetObject retrievs a bucket object.
func (g *GoFakeS3) GetObject(w http.ResponseWriter, r *http.Request) {
	log.Println("GET OBJECT")
	vars := mux.Vars(r)
	bucketName := strings.Replace(r.Host, ".localhost:9000", "", -1)
	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	g.storage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusInternalServerError)
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
		w.Header().Set("Last-Modified", time.Now().Format("Mon, 2 Jan 2006 15:04:05 MST"))
		w.Header().Set("ETag", "\""+hex.EncodeToString(hash[:])+"\"")
		w.Header().Set("Server", "AmazonS3")
		w.Header().Set("Content-Length", fmt.Sprintf("%v", len(t.Obj)))
		w.Header().Set("Connection", "close")
		w.Write(t.Obj)
		return nil
	})
}

// CreateObject creates a new S3 object.
func (g *GoFakeS3) CreateObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var bucketName string
	if len(vars["BucketName"]) > 0 {
		bucketName = vars["BucketName"]
	} else {
		bucketName = strings.Replace(r.Host, ".localhost:9000", "", -1)
	}
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
	meta["Last-Modified"] = time.Now().Format("Mon, 2 Jan 2006 15:04:05 MST")

	obj := &Object{meta, body}
	log.Println(string(body))
	g.storage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusInternalServerError)
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
	bucketName := strings.Replace(r.Host, ".localhost:9000", "", -1)
	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	g.storage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Println("no bucket")
			http.Error(w, "bucket does not exist", http.StatusInternalServerError)
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
		fmt.Printf("The answer is: %s\n", v)
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
