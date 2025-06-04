package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func server() {
	// Create a new S3 backend (using in-memory for this example)
	backend := s3mem.New()

	// Create a standard logger that writes to stderr
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// Create the fake S3 server
	faker := gofakes3.New(backend,
		// Optionally enable features like host bucket addressing or auto bucket creation
		gofakes3.WithAutoBucket(true),                // Automatically create buckets when they're accessed
		gofakes3.WithLogger(gofakes3.StdLog(logger)), // Log messages using the standard logger
	)

	// Create HTTP server
	addr := ":9000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	server := &http.Server{
		Addr:    addr,
		Handler: faker.Server(),
	}

	// Handle graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Run the server in a goroutine
	go func() {
		log.Printf("Starting GoFakeS3 server on %s\n", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Create default bucket example
	log.Println("Creating default 'example-bucket' for convenience")
	err := backend.CreateBucket("example-bucket")
	if err != nil {
		log.Printf("Warning: Failed to create default bucket: %v", err)
	}

	// Wait for interrupt signal
	<-done
	log.Println("Server is shutting down...")

	// Create a deadline for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited gracefully")
}
