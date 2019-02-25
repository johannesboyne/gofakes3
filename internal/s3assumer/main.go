package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
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

	var patternRaw string

	var config Config
	fs.StringVar(&config.S3Region, "region", "", "S3 region")
	fs.StringVar(&config.S3Endpoint, "endpoint", "", "S3 endpoint")
	fs.StringVar(&config.S3TestBucketPrefix, "bucketprefix", "", "S3 test bucket prefix")
	fs.StringVar(&patternRaw, "run", "", "Limit tests to this pattern")
	fs.BoolVar(&config.Verbose, "verbose", false, "Verbose")
	fs.BoolVar(&config.S3PathStyle, "pathstyle", false, "S3 use path style")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if config.S3TestBucketPrefix == "" {
		return fmt.Errorf("--bucketprefix flag required")
	}

	var pattern *regexp.Regexp

	if patternRaw != "" {
		var err error
		pattern, err = regexp.Compile(patternRaw)
		if err != nil {
			return err
		}
	}

	for _, test := range tests {
		name := testName(test)
		if pattern != nil && !pattern.MatchString(name) {
			continue
		}

		fmt.Println(name)
		testCtx := &Context{
			config:  config,
			rand:    rng,
			Context: ctx,
		}

		fmt.Println(name)
		if err := test.Run(testCtx); err != nil {
			fmt.Println("FAIL", name, err)
		}
	}
	return nil
}
