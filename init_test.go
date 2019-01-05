package gofakes3_test

// Initialisation file for tests in the 'gofakes3_test' package. Integration tests
// and the like go in this package as we are unable to use backends without the
// '_test' suffix without causing an import cycle.

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

const defaultBucket = "mybucket"

var (
	logFile string

	defaultDate = time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC)
)

func TestMain(m *testing.M) {
	if err := runTestMain(m); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code, ok := err.(errCode)
		if !ok {
			code = 1
		}
		os.Exit(int(code))
	}
	os.Exit(0)
}

func runTestMain(m *testing.M) error {
	flag.StringVar(&logFile, "fakes3.log", "", "Log file (temp file by default)")
	flag.Parse()

	var logOutput *os.File
	var err error

	if logFile == "" {
		logOutput, err = ioutil.TempFile("", "gofakes3-*.log")
	} else {
		logOutput, err = os.Create(logFile)
	}
	if err != nil {
		return err
	}
	defer logOutput.Close()

	fmt.Fprintf(os.Stderr, "log output redirected to %q\n", logOutput.Name())
	log.SetOutput(logOutput)

	if code := m.Run(); code > 0 {
		return errCode(code)
	}
	return nil
}

type errCode int

func (e errCode) Error() string { return fmt.Sprintf("exit code %d", e) }

type lsItems []lsItem

func (cl lsItems) assertContents(tt gofakes3.TT, dirs []string, files []string) {
	tt.Helper()
	cl.assertFiles(tt, files...)
	cl.assertDirs(tt, dirs...)
}

func (cl lsItems) assertDirs(tt gofakes3.TT, names ...string) {
	tt.Helper()
	cl.assertItems(tt, true, names...)
}

func (cl lsItems) assertFiles(tt gofakes3.TT, names ...string) {
	tt.Helper()
	cl.assertItems(tt, false, names...)
}

func (cl lsItems) assertItems(tt gofakes3.TT, isDir bool, names ...string) {
	tt.Helper()
	var found []string
	for _, item := range cl {
		if item.isDir == isDir {
			found = append(found, item.name)
		}
	}
	sort.Strings(found)
	sort.Strings(names)
	if !reflect.DeepEqual(found, names) {
		tt.Fatalf("items:\nexp: %v\ngot: %v", names, found)
	}
}

type lsItem struct {
	name  string
	date  time.Time
	size  int64
	isDir bool
}

type testServer struct {
	gofakes3.TT
	gofakes3.TimeSourceAdvancer
	*gofakes3.GoFakeS3

	backend gofakes3.Backend
	server  *httptest.Server

	// if this is nil, no buckets are created. by default, a starting bucket is
	// created using the value of the 'defaultBucket' constant.
	initialBuckets []string
}

type testServerOption func(ts *testServer)

func withoutInitialBuckets() testServerOption { return func(ts *testServer) { ts.initialBuckets = nil } }
func withInitialBuckets(buckets ...string) testServerOption {
	return func(ts *testServer) { ts.initialBuckets = buckets }
}

func newTestServer(t *testing.T, opts ...testServerOption) *testServer {
	t.Helper()
	var ts = testServer{
		TT:             gofakes3.TT{t},
		initialBuckets: []string{defaultBucket},
	}
	for _, o := range opts {
		o(&ts)
	}

	if ts.backend == nil {
		if ts.TimeSourceAdvancer == nil {
			ts.TimeSourceAdvancer = gofakes3.FixedTimeSource(defaultDate)
		}
		ts.backend = s3mem.New(s3mem.WithTimeSource(ts.TimeSourceAdvancer))
	}

	ts.GoFakeS3 = gofakes3.New(ts.backend,
		gofakes3.WithTimeSource(ts.TimeSourceAdvancer),
		gofakes3.WithTimeSkewLimit(0),
	)
	ts.server = httptest.NewServer(ts.GoFakeS3.Server())

	for _, bucket := range ts.initialBuckets {
		ts.TT.OK(ts.backend.CreateBucket(bucket))
	}

	return &ts
}

func (ts *testServer) url(url string) string {
	return fmt.Sprintf("%s/%s", ts.server.URL, strings.TrimLeft(url, "/"))
}

func (ts *testServer) createBucket(bucket string) {
	ts.Helper()
	if err := ts.backend.CreateBucket(bucket); err != nil {
		ts.Fatal("create bucket failed", err)
	}
}

func (ts *testServer) objectExists(bucket, key string) bool {
	ts.Helper()
	obj, err := ts.backend.HeadObject(bucket, key)
	if err != nil {
		if gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) {
			return false
		} else {
			ts.Fatal(err)
		}
	}
	return obj != nil
}

func (ts *testServer) putString(bucket, key string, meta map[string]string, in string) {
	ts.Helper()
	ts.OK(ts.backend.PutObject(bucket, key, meta, strings.NewReader(in)))
}

func (ts *testServer) objectAsString(bucket, key string) string {
	ts.Helper()
	obj, err := ts.backend.GetObject(bucket, key)
	ts.OK(err)

	defer obj.Contents.Close()
	data, err := ioutil.ReadAll(obj.Contents)
	ts.OK(err)

	return string(data)
}

func (ts *testServer) s3Client() *s3.S3 {
	ts.Helper()
	config := aws.NewConfig()
	config.WithEndpoint(ts.server.URL)
	config.WithRegion("region")
	config.WithCredentials(credentials.NewStaticCredentials("dummy-access", "dummy-secret", ""))
	config.WithS3ForcePathStyle(true) // Removes need for subdomain
	svc := s3.New(session.New(), config)
	return svc
}

func (ts *testServer) assertLs(bucket string, prefix string, expectedPrefixes []string, expectedObjects []string) {
	ts.Helper()
	client := ts.s3Client()
	rs, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(prefix),
	})
	ts.OK(err)

	var ls lsItems
	for _, item := range rs.CommonPrefixes {
		ls = append(ls, lsItem{name: *item.Prefix, isDir: true})
	}
	for _, item := range rs.Contents {
		ls = append(ls, lsItem{name: *item.Key, date: *item.LastModified, size: *item.Size})
	}

	ls.assertContents(ts.TT, expectedPrefixes, expectedObjects)
}

// If meta is nil, the metadata is not checked.
// If meta is map[string]string{}, it is checked against the empty map.
//
// If contents is a string or a []byte, it is compared against the object's contents,
// otherwise a panic occurs.
func (ts *testServer) assertObject(bucket string, object string, meta map[string]string, contents interface{}) {
	ts.Helper()

	obj, err := ts.backend.GetObject(bucket, object)
	ts.OK(err)
	defer obj.Contents.Close()

	data, err := ioutil.ReadAll(obj.Contents)
	ts.OK(err)

	if meta != nil {
		if !reflect.DeepEqual(meta, obj.Metadata) {
			ts.Fatal("metadata mismatch:", meta, "!=", obj.Metadata)
		}
	}

	var checkContents []byte
	switch contents := contents.(type) {
	case nil:
	case string:
		checkContents = []byte(contents)
	case []byte:
		checkContents = contents
	default:
		panic("unexpected contents")
	}

	if !bytes.Equal(checkContents, data) {
		ts.Fatal("data mismatch") // FIXME: more detail
	}

	if int64(len(checkContents)) != obj.Size {
		ts.Fatal("size mismatch:", len(checkContents), "!=", obj.Size)
	}
}

func (ts *testServer) Close() {
	ts.server.Close()
}

type hashValue []byte

func (h hashValue) Base64() string { return base64.StdEncoding.EncodeToString(h) }
func (h hashValue) Hex() string    { return hex.EncodeToString(h) }

var (
	randState = uint64(time.Now().UnixNano()) // FIXME: global seedable testing rand
	randMu    sync.Mutex
)

func randomFileBody(size int64) ([]byte, hashValue) {
	randMu.Lock()
	defer randMu.Unlock()

	neat := size/8*8 + 8 // cheap and nasty way to ensure a multiple of 8 definitely greater than size

	b := make([]byte, neat)

	// Using the default source of randomness was extremely slow; this is a
	// simple inline implementation of http://xoshiro.di.unimi.it/splitmix64.c
	// as we *really* don't care about the quality of the randomness, just that
	// it appears random enough to distribute byte values a bit.
	for i := int64(0); i < neat; i += 8 {
		randState += 0x9E3779B97F4A7C15
		z := randState
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		b[i], b[i+1], b[i+2], b[i+3], b[i+4], b[i+5], b[i+6], b[i+7] =
			byte(z), byte(z>>8), byte(z>>16), byte(z>>24), byte(z>>32), byte(z>>40), byte(z>>48), byte(z>>56)
	}

	b = b[:size]
	h := md5.New()
	h.Write(b)
	return b, hashValue(h.Sum(nil))
}
