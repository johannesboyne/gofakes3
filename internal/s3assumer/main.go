package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	fs := flag.NewFlagSet("", 0)

	var config Config
	fs.StringVar(&config.S3Region, "region", "", "S3 region")
	fs.StringVar(&config.S3Endpoint, "endpoint", "", "S3 endpoint")
	fs.StringVar(&config.S3TestBucket, "bucket", "", "S3 test bucket")
	fs.BoolVar(&config.S3PathStyle, "pathstyle", false, "S3 use path style")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if config.S3TestBucket == "" {
		return fmt.Errorf("--bucket flag required")
	}

	for _, test := range tests {
		testCtx := &testContext{
			config:  config,
			rand:    rng,
			Context: ctx,
		}
		name := testName(test)
		if err := test.Run(testCtx); err != nil {
			fmt.Println("FAIL", name, err)
		}
	}
	return nil
}
