package gofakes3

import (
	"net/http"
	"strings"
)

var (
	corsHeaders = []string{
		"Accept",
		"Accept-Encoding",
		"Authorization",
		"cache-control",
		"Content-Disposition",
		"Content-Encoding",
		"Content-Length",
		"Content-Type",
		"X-Amz-Date",
		"X-Amz-User-Agent",
		"X-CSRF-Token",
		"x-amz-acl",
		"x-amz-content-sha256",
		"x-amz-meta-filename",
		"x-amz-meta-from",
		"x-amz-meta-private",
		"x-amz-meta-to",
		"x-amz-security-token",
		"x-requested-with",
	}
	corsHeadersString = strings.Join(corsHeaders, ", ")
)

type withCORS struct {
	handler http.Handler

	methods string
	origin  string
	headers string
	expose  string
}

func wrapCORS(handler http.Handler) http.Handler {
	return &withCORS{
		handler: handler,
		methods: "POST, GET, OPTIONS, PUT, DELETE, HEAD",
		origin:  "*",
		headers: corsHeadersString,
		expose:  "ETag",
	}
}

func wrapInsecureCORS(handler http.Handler) http.Handler {
	return &withCORS{
		handler: handler,
		methods: "*",
		origin:  "*",
		headers: "*",
		expose:  "*",
	}
}

func (s *withCORS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	isPreflight := r.Method == "OPTIONS" &&
		r.Header.Get("Access-Control-Request-Method") != "" &&
		r.Header.Get("Origin") != ""

	if isPreflight {
		if s.origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", s.origin)
		}
		if s.methods != "" {
			w.Header().Set("Access-Control-Allow-Methods", s.methods)
		}
		if s.headers != "" {
			w.Header().Set("Access-Control-Allow-Headers", s.headers)
		}
		if s.expose != "" {
			w.Header().Set("Access-Control-Expose-Headers", s.expose)
		}
		return
	}

	s.handler.ServeHTTP(w, r)
}
