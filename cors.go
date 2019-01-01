package gofakes3

import (
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/gorilla/mux"
)

var (
	corsHeaders = []string{
		"Accept",
		"Accept-Encoding",
		"Authorization",
		"Content-Length",
		"Content-Type",
		"X-Amz-Date",
		"X-Amz-User-Agent",
		"X-CSRF-Token",
		"x-amz-meta-filename",
		"x-amz-meta-from",
		"x-amz-meta-private",
		"x-amz-meta-to",
	}
	corsHeadersString = strings.Join(corsHeaders, ", ")

	bucketRewritePattern = regexp.MustCompile("(127.0.0.1:\\d{1,7})|(.localhost:\\d{1,7})|(localhost:\\d{1,7})")
)

type WithCORS struct {
	r *mux.Router
}

func (s *WithCORS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, HEAD")
	w.Header().Set("Access-Control-Allow-Headers", corsHeadersString)

	if r.Method == "OPTIONS" {
		return
	}

	// Bucket name rewriting
	// this is due to some inconsistencies in the AWS SDKs
	bucket := bucketRewritePattern.ReplaceAllString(r.Host, "")
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
