package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

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
		db      string
		host    string
		backend string
		bucket  string
	)

	flag.StringVar(&db, "db", "locals3.db", "Database path / name when using bolt backend")
	flag.StringVar(&host, "host", ":9000", "Host to run the service")
	flag.StringVar(&backend, "backend", "", "Backend to use to store data (memory, bolt)")
	flag.StringVar(&bucket, "bucket", "fakes3", "Bucket to create by default (required)")
	flag.Parse()

	if bucket == "" {
		bucket = "fakes3"
	}

	var back gofakes3.Backend
	switch backend {
	case "":
		flag.PrintDefaults()
		fmt.Println()
		return fmt.Errorf("-backend is required")

	case "bolt":
		var err error
		back, err = s3bolt.NewFile(db)
		if err != nil {
			return err
		}
		log.Println("using bolt backend with file", db)

	case "mem", "memory":
		back = s3mem.New()
		log.Println("using memory backend")

	default:
		return fmt.Errorf("unknown backend %q", backend)
	}

	if err := back.CreateBucket(bucket); err != nil && !gofakes3.IsErrExist(err) {
		return fmt.Errorf("gofakes3: could not create initial bucket %q: %v", bucket, err)
	}

	log.Println("created bucket", bucket)

	faker := gofakes3.New(back)
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
