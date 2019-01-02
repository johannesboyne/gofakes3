package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		db           string
		host         string
		backendKind  string
		bucket       string
		fixedTimeStr string
	)

	flag.StringVar(&db, "db", "locals3.db", "Database path / name when using bolt backend")
	flag.StringVar(&host, "host", ":9000", "Host to run the service")
	flag.StringVar(&backendKind, "backend", "", "Backend to use to store data (memory, bolt)")
	flag.StringVar(&bucket, "bucket", "fakes3", "Bucket to create by default (required)")
	flag.StringVar(&fixedTimeStr, "time", "", "RFC3339 format. If passed, the server's clock will always see this time; does not affect existing stored dates.")
	flag.Parse()

	if bucket == "" {
		bucket = "fakes3"
	}

	var (
		backend       gofakes3.Backend
		timeSource    gofakes3.TimeSource
		timeSkewLimit = gofakes3.DefaultSkewLimit
	)

	if fixedTimeStr != "" {
		fixedTime, err := time.Parse(time.RFC3339Nano, fixedTimeStr)
		if err != nil {
			return err
		}
		timeSource = gofakes3.FixedTimeSource(fixedTime)
		timeSkewLimit = 0
	}

	switch backendKind {
	case "":
		flag.PrintDefaults()
		fmt.Println()
		return fmt.Errorf("-backend is required")

	case "bolt":
		var err error
		backend, err = s3bolt.NewFile(db, s3bolt.WithTimeSource(timeSource))
		if err != nil {
			return err
		}
		log.Println("using bolt backend with file", db)

	case "mem", "memory":
		backend = s3mem.New(s3mem.WithTimeSource(timeSource))
		log.Println("using memory backend")

	default:
		return fmt.Errorf("unknown backend %q", backendKind)
	}

	if err := backend.CreateBucket(bucket); err != nil && !gofakes3.IsAlreadyExists(err) {
		return fmt.Errorf("gofakes3: could not create initial bucket %q: %v", bucket, err)
	}

	log.Println("created bucket", bucket)

	faker := gofakes3.New(backend,
		gofakes3.WithTimeSource(timeSource),
		gofakes3.WithTimeSkewLimit(timeSkewLimit),
	)
	return listenAndServe(host, faker.Server())
}

func listenAndServe(addr string, handler http.Handler) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Println("using port:", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{Addr: addr, Handler: handler}

	return server.Serve(listener)
}
