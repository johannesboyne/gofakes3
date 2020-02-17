package gofakes3_test

// Initialisation file for tests in the 'gofakes3_test' package. Integration tests
// and the like go in this package as we are unable to use backends without the
// '_test' suffix without causing an import cycle.

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

const (
	defaultBucket = "mybucket"

	// docs say MB, client SDK uses MiB, gofakes3 assumes MB as the lowest common denominator
	// to accept, but we need to satisfy the client SDK in the test suite so this is MiB:
	defaultUploadPartSize = 5 * 1024 * 1024
)

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

// lsItems is used by testServer and testCLI to represent the result of a
// bucket list operation.
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

	backend   gofakes3.Backend
	versioned gofakes3.VersionedBackend
	server    *httptest.Server
	options   []gofakes3.Option

	// if this is nil, no buckets are created. by default, a starting bucket is
	// created using the value of the 'defaultBucket' constant.
	initialBuckets []string
	versioning     bool
}

type testServerOption func(ts *testServer)

func withoutInitialBuckets() testServerOption {
	return func(ts *testServer) { ts.initialBuckets = nil }
}
func withInitialBuckets(buckets ...string) testServerOption {
	return func(ts *testServer) { ts.initialBuckets = buckets }
}
func withVersioning() testServerOption {
	return func(ts *testServer) { ts.versioning = true }
}
func withFakerOptions(opts ...gofakes3.Option) testServerOption {
	return func(ts *testServer) { ts.options = opts }
}
func withBackend(backend gofakes3.Backend) testServerOption {
	return func(ts *testServer) { ts.backend = backend }
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
		ts.backend = s3mem.New(
			s3mem.WithTimeSource(ts.TimeSourceAdvancer),
			s3mem.WithVersionSeed(0))
	}

	fakerOpts := []gofakes3.Option{
		gofakes3.WithTimeSource(ts.TimeSourceAdvancer),
		gofakes3.WithTimeSkewLimit(0),

		// TestMain wires the stdlib's global logger up to a file already,
		// which this takes advantage of:
		gofakes3.WithGlobalLog(),
	}
	fakerOpts = append(fakerOpts, ts.options...)

	ts.GoFakeS3 = gofakes3.New(ts.backend, fakerOpts...)
	ts.server = httptest.NewServer(ts.GoFakeS3.Server())

	for _, bucket := range ts.initialBuckets {
		ts.TT.OK(ts.backend.CreateBucket(bucket))
	}

	if ts.versioning {
		mem, ok := ts.backend.(*s3mem.Backend)
		if !ok {
			panic("backend is not a versioned backend")
		}
		ts.versioned = mem
		for _, bucket := range ts.initialBuckets {
			ts.TT.OK(ts.versioned.SetVersioningConfiguration(bucket, gofakes3.VersioningConfiguration{
				Status: gofakes3.VersioningEnabled,
			}))
		}
	}

	return &ts
}

func (ts *testServer) url(url string) string {
	return fmt.Sprintf("%s/%s", ts.server.URL, strings.TrimLeft(url, "/"))
}

func (ts *testServer) backendCreateBucket(bucket string) {
	ts.Helper()
	if err := ts.backend.CreateBucket(bucket); err != nil {
		ts.Fatal("create bucket failed", err)
	}
}

func (ts *testServer) backendObjectExists(bucket, key string) bool {
	ts.Helper()
	obj, err := ts.backend.HeadObject(bucket, key)
	if err != nil {
		if hasErrorCode(err, gofakes3.ErrNoSuchKey) {
			return false
		} else {
			ts.Fatal(err)
		}
	}
	return obj != nil
}

func (ts *testServer) backendPutString(bucket, key string, meta map[string]string, in string) {
	ts.Helper()
	ts.OKAll(ts.backend.PutObject(bucket, key, meta, strings.NewReader(in), int64(len(in))))
}

func (ts *testServer) backendPutBytes(bucket, key string, meta map[string]string, in []byte) {
	ts.Helper()
	ts.OKAll(ts.backend.PutObject(bucket, key, meta, bytes.NewReader(in), int64(len(in))))
}

func (ts *testServer) backendGetString(bucket, key string, rnge *gofakes3.ObjectRangeRequest) string {
	ts.Helper()
	obj, err := ts.backend.GetObject(bucket, key, rnge)
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

func (ts *testServer) rawClient() *rawClient {
	return newRawClient(httpClient(), ts.server.URL)
}

type multipartUploadOptions struct {
	partSize int64
}

func (ts *testServer) assertMultipartUpload(bucket, object string, body interface{}, options *multipartUploadOptions) {
	ts.Helper()

	if options == nil {
		options = &multipartUploadOptions{}
	}
	if options.partSize <= 0 {
		options.partSize = defaultUploadPartSize
	}

	s3 := ts.s3Client()
	uploader := s3manager.NewUploaderWithClient(s3)

	contents := readBody(ts.TT, body)
	upParams := &s3manager.UploadInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		Body:       bytes.NewReader(contents),
		ContentMD5: aws.String(hashMD5Bytes(contents).Base64()),
	}

	out, err := uploader.Upload(upParams, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = true
		u.PartSize = options.partSize
	})
	ts.OK(err)
	_ = out

	ts.assertObject(bucket, object, nil, body)
}

func (ts *testServer) createMultipartUpload(bucket, object string, meta map[string]string) (uploadID string) {
	ts.Helper()

	svc := ts.s3Client()
	mpu, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	ts.OK(err)
	return *mpu.UploadId
}

func (ts *testServer) uploadPart(bucket, object string, uploadID string, num int64, body []byte) *s3.CompletedPart {
	ts.Helper()

	hash := hashMD5Bytes(body)
	svc := ts.s3Client()
	mpu, err := svc.UploadPart(&s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		Body:       bytes.NewReader(body),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int64(num),
		ContentMD5: aws.String(hash.Base64()),
	})
	ts.OK(err)
	return &s3.CompletedPart{ETag: aws.String(*mpu.ETag), PartNumber: aws.Int64(num)}
}

func (ts *testServer) assertCompleteUpload(bucket, object, uploadID string, parts []*s3.CompletedPart, body interface{}) {
	ts.Helper()

	svc := ts.s3Client()
	mpu, err := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	ts.OK(err)
	_ = mpu // FIXME: assert some of this

	ts.assertObject(bucket, object, nil, body)
}

func (ts *testServer) assertAbortMultipartUpload(bucket, object string, uploadID gofakes3.UploadID) {
	ts.Helper()

	svc := ts.s3Client()
	rs, err := svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(string(uploadID)),
	})
	ts.OK(err)
	_ = rs

	{ // FIXME: Currently, the only way to sanity check this using the HTTP API
		// is to try to list the parts, which should indicate if the upload was
		// successfully removed. Once the upload API becomes a first class
		// citizen, we should be able to call it directly.
		rs, err := svc.ListParts(&s3.ListPartsInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(object),
			UploadId: aws.String(string(uploadID)),
		})
		_ = rs
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == string(gofakes3.ErrNoSuchUpload) {
			return
		}
		ts.Fatal("expected NotFound error, found", err)
	}
}

type listUploadPartsOpts struct {
	Prefix *gofakes3.Prefix
	Marker int64
	Limit  int64

	// Supports s3.CompletedPart or s3.Part:
	Parts []interface{}
}

func (opts listUploadPartsOpts) withCompletedParts(parts ...*s3.CompletedPart) listUploadPartsOpts {
	for _, p := range parts {
		opts.Parts = append(opts.Parts, p)
	}
	return opts
}

func (opts listUploadPartsOpts) input(bucket, object string, uploadID string) *s3.ListPartsInput {
	rq := &s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadID),
	}
	if opts.Limit > 0 {
		rq.MaxParts = aws.Int64(opts.Limit)
	}
	if opts.Marker > 0 {
		rq.PartNumberMarker = aws.Int64(opts.Marker)
	}
	return rq
}

func (ts *testServer) assertListUploadPartsFails(code gofakes3.ErrorCode, bucket, object string, uploadID string, opts listUploadPartsOpts) {
	ts.Helper()

	svc := ts.s3Client()
	_, err := svc.ListParts(opts.input(bucket, object, uploadID))
	if !hasErrorCode(err, code) {
		ts.Fatal("expected", code, "found", err)
	}
}

func (ts *testServer) assertListUploadParts(bucket, object string, uploadID string, opts listUploadPartsOpts) {
	ts.Helper()

	rq := opts.input(bucket, object, uploadID)
	svc := ts.s3Client()
	rs, err := svc.ListParts(rq)
	ts.OK(err)

	if *rs.Bucket != bucket {
		ts.Fatal("bucket mismatch:", *rs.Bucket, "!=", bucket)
	}
	if *rs.Key != object {
		ts.Fatal("object mismatch:", *rs.Key, "!=", object)
	}
	if *rs.UploadId != uploadID {
		ts.Fatal("upload mismatch:", *rs.UploadId, "!=", uploadID)
	}
	if len(rs.Parts) != len(opts.Parts) {
		ts.Fatal("parts mismatch:", rs.Parts, "!=", opts.Parts)
	}

	for idx, found := range rs.Parts {
		switch expected := opts.Parts[idx].(type) {
		case *s3.CompletedPart:
			if *expected.ETag != *found.ETag {
				ts.Fatal("part", idx, "ETag mismatch:", expected.ETag, "!=", found.ETag)
			}
			if *expected.PartNumber != *found.PartNumber {
				ts.Fatal("part", idx, "PartNumber mismatch:", expected.PartNumber, "!=", found.PartNumber)
			}

		case *s3.Part:
			if !reflect.DeepEqual(expected, found) {
				ts.Fatal("part", idx, "mismatch:", expected, "!=", found)
			}

		default:
			ts.Fatalf("unexpected type %T", expected)
		}
	}
}

type listUploadsOpts struct {
	Prefix *gofakes3.Prefix
	Marker string
	Limit  int64

	Prefixes []string
	Uploads  []string
}

// assertListMultipartUploads
//
// If marker is not an empty string, it should be in the format "[<object>][/<uploadID>]".
// Each item in expectedUploads must be in the format "<object>/<uploadID>".
func (ts *testServer) assertListMultipartUploads(
	bucket string,
	opts listUploadsOpts,
) {
	ts.Helper()

	svc := ts.s3Client()
	rq := &s3.ListMultipartUploadsInput{
		Bucket:     aws.String(bucket),
		MaxUploads: aws.Int64(opts.Limit),
	}

	var expectedKeyMarker, expectedUploadIDMarker string
	if opts.Marker != "" {
		parts := strings.SplitN(opts.Marker, "/", 2)
		expectedKeyMarker = parts[0]
		if len(parts) == 2 {
			expectedUploadIDMarker = parts[1]
		}
		rq.KeyMarker = aws.String(expectedKeyMarker)
		rq.UploadIdMarker = aws.String(expectedUploadIDMarker)
	}

	var expectedPrefix, expectedDelimiter string
	if opts.Prefix != nil {
		rq.Prefix = aws.String(opts.Prefix.Prefix)
		rq.Delimiter = aws.String(opts.Prefix.Delimiter)
		expectedPrefix, expectedDelimiter = opts.Prefix.Prefix, opts.Prefix.Delimiter
	}

	rs, err := svc.ListMultipartUploads(rq)
	ts.OK(err)

	{ // assert response fields match input
		var foundPrefix, foundDelimiter, foundKeyMarker, foundUploadIDMarker string
		if rs.Delimiter != nil {
			foundDelimiter = *rs.Delimiter
		}
		if rs.Prefix != nil {
			foundPrefix = *rs.Prefix
		}
		if rs.KeyMarker != nil {
			foundKeyMarker = *rs.KeyMarker
		}
		if rs.UploadIdMarker != nil {
			foundUploadIDMarker = *rs.UploadIdMarker
		}
		if foundPrefix != expectedPrefix {
			ts.Fatal("unexpected prefix", foundPrefix, "!=", expectedPrefix)
		}
		if foundDelimiter != expectedDelimiter {
			ts.Fatal("unexpected delimiter", foundDelimiter, "!=", expectedDelimiter)
		}
		if foundKeyMarker != expectedKeyMarker {
			ts.Fatal("unexpected key marker", foundKeyMarker, "!=", expectedKeyMarker)
		}
		if foundUploadIDMarker != expectedUploadIDMarker {
			ts.Fatal("unexpected upload ID marker", foundUploadIDMarker, "!=", expectedUploadIDMarker)
		}
		var foundUploads int64
		if rs.MaxUploads != nil {
			foundUploads = *rs.MaxUploads
		}
		if opts.Limit > 0 && foundUploads != opts.Limit {
			ts.Fatal("unexpected max uploads", foundUploads, "!=", opts.Limit)
		}
	}

	var foundUploads []string
	for _, up := range rs.Uploads {
		foundUploads = append(foundUploads, fmt.Sprintf("%s/%s", *up.Key, *up.UploadId))
	}

	var foundPrefixes []string
	for _, cp := range rs.CommonPrefixes {
		foundPrefixes = append(foundPrefixes, *cp.Prefix)
	}

	if !reflect.DeepEqual(foundPrefixes, opts.Prefixes) {
		ts.Fatal("common prefix list mismatch:", foundPrefixes, "!=", opts.Prefixes)
	}
	if !reflect.DeepEqual(foundUploads, opts.Uploads) {
		ts.Fatal("upload list mismatch:", foundUploads, "!=", opts.Uploads)
	}
}

// If meta is nil, the metadata is not checked.
// If meta is map[string]string{}, it is checked against the empty map.
//
// If contents is a string, a []byte or an io.Reader, it is compared against
// the object's contents, otherwise a panic occurs.
func (ts *testServer) assertObject(bucket string, object string, meta map[string]string, contents interface{}) {
	ts.Helper()

	obj, err := ts.backend.GetObject(bucket, object, nil)
	ts.OK(err)
	defer obj.Contents.Close()

	data, err := gofakes3.ReadAll(obj.Contents, obj.Size)
	ts.OK(err)

	if meta != nil {
		if !reflect.DeepEqual(meta, obj.Metadata) {
			ts.Fatal("metadata mismatch:", meta, "!=", obj.Metadata)
		}
	}

	checkContents := readBody(ts.TT, contents)
	if !bytes.Equal(checkContents, data) {
		ts.Fatal("data mismatch") // FIXME: more detail
	}

	if int64(len(checkContents)) != obj.Size {
		ts.Fatal("size mismatch:", len(checkContents), "!=", obj.Size)
	}
}

type listBucketResult struct {
	CommonPrefixes []*s3.CommonPrefix
	Contents       []*s3.Object
}

func (ts *testServer) mustListBucketV1Pages(prefix *gofakes3.Prefix, maxKeys int64, marker string) *listBucketResult {
	r, err := ts.listBucketV1Pages(prefix, maxKeys, marker)
	ts.OK(err)
	return r
}

func (ts *testServer) listBucketV1Pages(prefix *gofakes3.Prefix, maxKeys int64, marker string) (*listBucketResult, error) {
	const pageLimit = 20

	svc := ts.s3Client()
	pages := 0
	in := &s3.ListObjectsInput{
		Bucket:  aws.String(defaultBucket),
		MaxKeys: aws.Int64(maxKeys),
	}
	if prefix != nil && prefix.HasDelimiter {
		in.Delimiter = aws.String(prefix.Delimiter)
	}
	if prefix != nil && prefix.HasPrefix {
		in.Prefix = aws.String(prefix.Prefix)
	}
	if marker != "" {
		in.Marker = aws.String(marker)
	}

	var rs listBucketResult
	if err := (svc.ListObjectsPages(in, func(out *s3.ListObjectsOutput, lastPage bool) bool {
		pages++
		if pages > pageLimit {
			panic("stuck in a page loop")
		}
		rs.CommonPrefixes = append(rs.CommonPrefixes, out.CommonPrefixes...)
		rs.Contents = append(rs.Contents, out.Contents...)
		return !lastPage
	})); err != nil {
		return nil, err
	}

	return &rs, nil
}

func (ts *testServer) mustListBucketV2Pages(prefix *gofakes3.Prefix, maxKeys int64, marker string) *listBucketResult {
	r, err := ts.listBucketV2Pages(prefix, maxKeys, marker)
	ts.OK(err)
	return r
}

func (ts *testServer) listBucketV2Pages(prefix *gofakes3.Prefix, maxKeys int64, startAfter string) (*listBucketResult, error) {
	const pageLimit = 20

	svc := ts.s3Client()
	pages := 0
	in := &s3.ListObjectsV2Input{
		Bucket:  aws.String(defaultBucket),
		MaxKeys: aws.Int64(maxKeys),
	}
	if prefix != nil && prefix.HasDelimiter {
		in.Delimiter = aws.String(prefix.Delimiter)
	}
	if prefix != nil && prefix.HasPrefix {
		in.Prefix = aws.String(prefix.Prefix)
	}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}

	var rs listBucketResult
	if err := (svc.ListObjectsV2Pages(in, func(out *s3.ListObjectsV2Output, lastPage bool) bool {
		pages++
		if pages > pageLimit {
			panic("stuck in a page loop")
		}
		rs.CommonPrefixes = append(rs.CommonPrefixes, out.CommonPrefixes...)
		rs.Contents = append(rs.Contents, out.Contents...)
		return !lastPage
	})); err != nil {
		return nil, err
	}

	return &rs, nil
}

func (ts *testServer) Close() {
	ts.server.Close()
}

func hashMD5Bytes(body []byte) hashValue {
	h := md5.New()
	h.Write(body)
	return hashValue(h.Sum(nil))
}

func hashSHA256Bytes(body []byte) hashValue {
	h := sha256.New()
	h.Write(body)
	return hashValue(h.Sum(nil))
}

type hashValue []byte

func (h hashValue) Base64() string { return base64.StdEncoding.EncodeToString(h) }
func (h hashValue) Hex() string    { return hex.EncodeToString(h) }

var (
	randState = uint64(time.Now().UnixNano()) // FIXME: global seedable testing rand
	randMu    sync.Mutex
)

func randomFileBody(size int64) []byte {
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
	return b
}

func readBody(tt gofakes3.TT, body interface{}) []byte {
	switch body := body.(type) {
	case nil:
		return []byte{}
	case string:
		return []byte(body)
	case []byte:
		return body
	case io.Reader:
		out, err := ioutil.ReadAll(body)
		tt.OK(err)
		return out
	default:
		panic("unexpected contents")
	}
}

func prefixFile(prefix string) *gofakes3.Prefix {
	return &gofakes3.Prefix{Delimiter: "/", Prefix: prefix}
}

func prefix(prefix string) *gofakes3.Prefix {
	return &gofakes3.Prefix{Prefix: prefix}
}

func prefixDelim(prefix string, delim string) *gofakes3.Prefix {
	return &gofakes3.Prefix{Prefix: prefix, Delimiter: delim}
}

func strs(s ...string) []string {
	return s
}

// hasErrorCode is like gofakes3.HasErrorCode, but supports awserr.Error as
// well, which we can't do directly in gofakes3 to avoid the dependency.
func hasErrorCode(err error, code gofakes3.ErrorCode) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		return awsErr.Code() == string(code)
	} else {
		return gofakes3.HasErrorCode(err, code)
	}
}

func httpClient() *http.Client {
	return &http.Client{
		Timeout: 2 * time.Second,
	}
}

type backendWithUnimplementedPaging struct {
	gofakes3.Backend
}

func (b *backendWithUnimplementedPaging) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if !page.IsEmpty() {
		return nil, gofakes3.ErrInternalPageNotImplemented
	}
	return b.Backend.ListBucket(name, prefix, page)
}

type rawClient struct {
	client *http.Client
	base   *url.URL
}

func newRawClient(client *http.Client, base string) *rawClient {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	return &rawClient{client: client, base: u}
}

func (c *rawClient) URL(rqpath string) *url.URL {
	u, err := url.Parse(c.base.String())
	if err != nil {
		panic(err)
	}
	u.Path = path.Join(u.Path, rqpath)
	return u
}

func (c *rawClient) Request(method, rqpath string, body []byte) *http.Request {
	u := c.URL(rqpath)
	rq, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	c.SetHeaders(rq, body)
	return rq
}

func (c *rawClient) SetHeaders(rq *http.Request, body []byte) {
	// NOTE: This was put together by using httputil.DumpRequest inside routeBase(). We
	// don't currently implement the Authorization header, so that has been skimmed for
	// now.
	rq.Header.Set("Accept-Encoding", "gzip")
	rq.Header.Set("Authorization", "...") // TODO
	rq.Header.Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
	rq.Header.Set("Content-Md5", hashMD5Bytes(body).Base64())
	rq.Header.Set("User-Agent", "aws-sdk-go/1.17.4 (go1.14rc1; linux; amd64)")
	rq.Header.Set("X-Amz-Date", time.Now().In(time.UTC).Format("20060102T030405-0700"))
	rq.Header.Set("X-Amz-Content-Sha256", hashSHA256Bytes(body).Hex())
}

// SendRaw can be used to bypass Go's http client, which helps us out a lot by taking
// care of some things for us, but which we actually want to test messing up from
// time to time.
func (c *rawClient) SendRaw(rq *http.Request) ([]byte, error) {
	b, err := httputil.DumpRequest(rq, true)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", c.base.Host, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	conn.SetDeadline(deadline)
	if _, err := conn.Write(b); err != nil {
		return nil, err
	}

	var rs []byte
	var scratch = make([]byte, 1024)
	for {
		n, err := conn.Read(scratch)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		rs = append(rs, scratch[:n]...)
	}

	return rs, nil
}

func (c *rawClient) Do(rq *http.Request) (*http.Response, error) {
	return c.client.Do(rq)
}

func maskReader(r io.Reader) io.Reader {
	// http.NewRequest() forces a ContentLength if it recognises
	// the type of reader you pass as the body. This is a cheeky
	// way to bypass that:
	return &maskedReader{r}
}

type maskedReader struct {
	inner io.Reader
}

func (r *maskedReader) Read(b []byte) (n int, err error) {
	return r.inner.Read(b)
}
