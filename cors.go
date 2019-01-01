package gofakes3

import (
	"log"
	"net/http"
	"regexp"

	"github.com/gorilla/mux"
)

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
