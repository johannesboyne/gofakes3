package gofakes3

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWrapInsecureCORSOptionsRequest(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	corsHandler := wrapInsecureCORS(h)

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "foo")
	req.Header.Set("Access-Control-Request-Method", "GET")
	resp := httptest.NewRecorder()
	corsHandler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status OK; got %v", resp.Code)
	}

	headers := resp.Header()
	if headers.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("Expected Access-Control-Allow-Origin: *; got %v", headers.Get("Access-Control-Allow-Origin"))
	}
	if headers.Get("Access-Control-Allow-Methods") != "*" {
		t.Errorf("Expected Access-Control-Allow-Methods: *; got %v", headers.Get("Access-Control-Allow-Methods"))
	}
	if headers.Get("Access-Control-Allow-Headers") != "*" {
		t.Errorf("Expected Access-Control-Allow-Headers: *; got %v", headers.Get("Access-Control-Allow-Headers"))
	}
	if headers.Get("Access-Control-Expose-Headers") != "*" {
		t.Errorf("Expected Access-Control-Expose-Headers: *; got %v", headers.Get("Access-Control-Expose-Headers"))
	}
}

func TestWrapInsecureCORSGetRequest(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	corsHandler := wrapInsecureCORS(h)

	req := httptest.NewRequest("GET", "/", nil)
	resp := httptest.NewRecorder()
	corsHandler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status OK; got %v", resp.Code)
	}

	headers := resp.Header()
	if _, ok := headers["Access-Control-Allow-Origin"]; ok {
		t.Errorf("expected no Access-Control-Allow-Origin header")
	}
	if _, ok := headers["Access-Control-Allow-Methods"]; ok {
		t.Errorf("expected no Access-Control-Allow-Methods header")
	}
	if _, ok := headers["Access-Control-Allow-Headers"]; ok {
		t.Errorf("expected no Access-Control-Allow-Headers header")
	}
	if _, ok := headers["Access-Control-Expose-Headers"]; ok {
		t.Errorf("expected no Access-Control-Expose-Headers header")
	}
}
