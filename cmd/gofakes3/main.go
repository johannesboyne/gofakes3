package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/spf13/afero"
)

const usage = `
`

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

type fakeS3Flags struct {
	host            string
	backendKind     string
	initialBucket   string
	fixedTimeStr    string
	noIntegrity     bool
	hostBucket      bool
	hostBucketBases HostList
	autoBucket      bool
	quiet           bool

	boltDb              string
	directFsPath        string
	directFsMeta        string
	directFsBucket      string
	directFsCreatePaths bool

	fsPath        string
	fsMeta        string
	fsCreatePaths bool

	debugCPU  string
	debugHost string
}

func (f *fakeS3Flags) attach(flagSet *flag.FlagSet) {
	flagSet.StringVar(&f.host, "host", ":9000", "Host to run the service")
	flagSet.StringVar(&f.fixedTimeStr, "time", "", "RFC3339 format. If passed, the server's clock will always see this time; does not affect existing stored dates.")
	flagSet.StringVar(&f.initialBucket, "initialbucket", "", "If passed, this bucket will be created on startup if it does not already exist.")
	flagSet.BoolVar(&f.noIntegrity, "no-integrity", false, "Pass this flag to disable Content-MD5 validation when uploading.")
	flagSet.BoolVar(&f.autoBucket, "autobucket", false, "If passed, nonexistent buckets will be created on first use instead of raising an error")
	flagSet.BoolVar(&f.hostBucket, "hostbucket", false, ""+
		"If passed, the bucket name will be extracted from the first segment of the hostname, "+
		"rather than the first part of the URL path. Disables path-based mode. If you require both, use "+
		"-hostbucketbase.")
	flagSet.Var(&f.hostBucketBases, "hostbucketbase", ""+
		"If passed, the bucket name will be presumed to be the hostname segment before the "+
		"host bucket base, i.e. if hostbucketbase is 'example.com' and you request 'foo.example.com', "+
		"the bucket is presumed to be 'foo'. Any other hostname not matching this pattern will use "+
		"path routing. Takes precedence over -hostbucket. Can be passed multiple times, or as a single "+
		"comma separated list")

	// Logging
	flagSet.BoolVar(&f.quiet, "quiet", false, "If passed, log messages are not printed to stderr")

	// Backend specific:
	flagSet.StringVar(&f.backendKind, "backend", "", "Backend to use to store data (memory, bolt, directfs, fs)")
	flagSet.StringVar(&f.boltDb, "bolt.db", "locals3.db", "Database path / name when using bolt backend")

	flagSet.StringVar(&f.directFsPath, "directfs.path", "", "File path to serve using S3. You should not modify the contents of this path outside gofakes3 while it is running as it can cause inconsistencies.")
	flagSet.StringVar(&f.directFsMeta, "directfs.meta", "", "Optional path for storing S3 metadata for your bucket. If not passed, metadata will not persist between restarts of gofakes3.")
	flagSet.StringVar(&f.directFsBucket, "directfs.bucket", "mybucket", "Name of the bucket for your file path; this will be the only supported bucket by the 'directfs' backend for the duration of your run.")
	flagSet.BoolVar(&f.directFsCreatePaths, "directfs.create", false, "Create all paths for direct filesystem backend")

	flagSet.StringVar(&f.fsPath, "fs.path", "", "Path to your S3 buckets. Buckets are stored under the '/buckets' subpath.")
	flagSet.StringVar(&f.fsMeta, "fs.meta", "", "Optional path for storing S3 metadata for your buckets. Defaults to the '/metadata' subfolder of -fs.path if not passed.")
	flagSet.BoolVar(&f.fsCreatePaths, "fs.create", false, "Create all paths for filesystem backends")

	// Debugging:
	flagSet.StringVar(&f.debugHost, "debug.host", "", "Run the debug server on this host")
	flagSet.StringVar(&f.debugCPU, "debug.cpu", "", "Create CPU profile in this file")

	// Deprecated:
	flagSet.StringVar(&f.boltDb, "db", "locals3.db", "Deprecated; use -bolt.db")
	flagSet.StringVar(&f.initialBucket, "bucket", "", `Deprecated; use -initialbucket`)
}

func (f *fakeS3Flags) fsPathFlags() (flags s3afero.FsFlags) {
	if f.fsCreatePaths {
		flags |= s3afero.FsPathCreateAll
	}
	return flags
}

func (f *fakeS3Flags) directFsPathFlags() (flags s3afero.FsFlags) {
	if f.directFsCreatePaths {
		flags |= s3afero.FsPathCreateAll
	}
	return flags
}

func (f *fakeS3Flags) timeOptions() (source gofakes3.TimeSource, skewLimit time.Duration, err error) {
	skewLimit = gofakes3.DefaultSkewLimit

	if f.fixedTimeStr != "" {
		fixedTime, err := time.Parse(time.RFC3339Nano, f.fixedTimeStr)
		if err != nil {
			return nil, 0, err
		}
		source = gofakes3.FixedTimeSource(fixedTime)
		skewLimit = 0
	}

	return source, skewLimit, nil
}

func debugServer(host string) {
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)

	srv := &http.Server{Addr: host}
	srv.Handler = mux
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

func run() error {
	var values fakeS3Flags

	flagSet := flag.NewFlagSet("", 0)
	values.attach(flagSet)

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	stopper, err := profile(values)
	if err != nil {
		return err
	}
	defer stopper()

	if values.debugHost != "" {
		log.Println("starting debug server at", fmt.Sprintf("http://%s/debug/pprof", values.debugHost))
		go debugServer(values.debugHost)
	}

	var backend gofakes3.Backend

	timeSource, timeSkewLimit, err := values.timeOptions()
	if err != nil {
		return err
	}

	switch values.backendKind {
	case "":
		flag.PrintDefaults()
		fmt.Println()
		return fmt.Errorf("-backend is required")

	case "bolt":
		var err error
		backend, err = s3bolt.NewFile(values.boltDb, s3bolt.WithTimeSource(timeSource))
		if err != nil {
			return err
		}
		log.Println("using bolt backend with file", values.boltDb)

	case "mem", "memory":
		if values.initialBucket == "" {
			log.Println("no buckets available; consider passing -initialbucket")
		}
		backend = s3mem.New(s3mem.WithTimeSource(timeSource))
		log.Println("using memory backend")

	case "fs":
		if timeSource != nil {
			log.Println("warning: time source not supported by this backend")
		}

		baseFs, err := s3afero.FsPath(values.fsPath, values.fsPathFlags())
		if err != nil {
			return fmt.Errorf("gofakes3: could not create -fs.path: %v", err)
		}

		var options []s3afero.MultiOption
		if values.fsMeta != "" {
			metaFs, err := s3afero.FsPath(values.fsMeta, values.fsPathFlags())
			if err != nil {
				return fmt.Errorf("gofakes3: could not create -fs.meta: %v", err)
			}
			options = append(options, s3afero.MultiWithMetaFs(metaFs))
		}

		backend, err = s3afero.MultiBucket(baseFs, options...)
		if err != nil {
			return err
		}

	case "directfs":
		if values.initialBucket != "" {
			return fmt.Errorf("gofakes3: -initialbucket not supported by directfs")
		}
		if values.autoBucket {
			return fmt.Errorf("gofakes3: -autobucket not supported by directfs")
		}
		if timeSource != nil {
			log.Println("warning: time source not supported by this backend")
		}

		baseFs, err := s3afero.FsPath(values.directFsPath, values.directFsPathFlags())
		if err != nil {
			return fmt.Errorf("gofakes3: could not create -directfs.path: %v", err)
		}

		var metaFs afero.Fs
		if values.directFsMeta != "" {
			metaFs, err = s3afero.FsPath(values.directFsMeta, values.directFsPathFlags())
			if err != nil {
				return fmt.Errorf("gofakes3: could not create -directfs.meta: %v", err)
			}
		} else {
			log.Println("using ephemeral memory backend for metadata; this will not persist. See -directfs.metapath flag if you need persistence.")
		}

		backend, err = s3afero.SingleBucket(values.directFsBucket, baseFs, metaFs)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown backend %q", values.backendKind)
	}

	if values.initialBucket != "" {
		if err := backend.CreateBucket(values.initialBucket); err != nil && !gofakes3.IsAlreadyExists(err) {
			return fmt.Errorf("gofakes3: could not create initial bucket %q: %v", values.initialBucket, err)
		}
		log.Println("created -initialbucket", values.initialBucket)
	}

	logger := gofakes3.GlobalLog()
	if values.quiet {
		logger = gofakes3.DiscardLog()
	}

	faker := gofakes3.New(backend,
		gofakes3.WithIntegrityCheck(!values.noIntegrity),
		gofakes3.WithTimeSkewLimit(timeSkewLimit),
		gofakes3.WithTimeSource(timeSource),
		gofakes3.WithLogger(logger),
		gofakes3.WithHostBucket(values.hostBucket),
		gofakes3.WithHostBucketBase(values.hostBucketBases.Values...),
		gofakes3.WithAutoBucket(values.autoBucket),
	)

	return listenAndServe(values.host, faker.Server())
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

func profile(values fakeS3Flags) (func(), error) {
	fn := func() {}

	if values.debugCPU != "" {
		f, err := os.Create(values.debugCPU)
		if err != nil {
			return fn, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fn, err
		}
		return pprof.StopCPUProfile, nil
	}

	return fn, nil
}

type HostList struct {
	Values []string
}

func (sl HostList) String() string {
	return strings.Join(sl.Values, ",")
}

func (sl HostList) Type() string { return "[]string" }

func (sl *HostList) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		part = strings.Trim(strings.TrimSpace(part), ".")
		if part == "" {
			return fmt.Errorf("host is empty")
		}
		sl.Values = append(sl.Values, part)
	}
	return nil
}
