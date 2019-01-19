package gofakes3_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
)

func TestCLILsBuckets(t *testing.T) {
	cli := newTestCLI(t, withoutInitialBuckets())
	defer cli.Close()

	if len(cli.lsBuckets()) != 0 {
		t.Fatal()
	}

	cli.backendCreateBucket("foo")
	if !reflect.DeepEqual(cli.lsBuckets().Names(), []string{"foo"}) {
		t.Fatal()
	}

	cli.backendCreateBucket("bar")
	if !reflect.DeepEqual(cli.lsBuckets().Names(), []string{"bar", "foo"}) {
		t.Fatal()
	}
}

func TestCLILsFiles(t *testing.T) {
	cli := newTestCLI(t)
	defer cli.Close()

	if len(cli.lsFiles(defaultBucket, "")) != 0 {
		t.Fatal()
	}

	cli.backendPutString(defaultBucket, "test-one", nil, "hello")
	cli.assertLsFiles(defaultBucket, "",
		nil, []string{"test-one"})

	cli.backendPutString(defaultBucket, "test-two", nil, "hello")
	cli.assertLsFiles(defaultBucket, "",
		nil, []string{"test-one", "test-two"})

	// only "test-one" and "test-two" should pass the prefix match
	cli.backendPutString(defaultBucket, "no-match", nil, "hello")
	cli.assertLsFiles(defaultBucket, "test-",
		nil, []string{"test-one", "test-two"})

	cli.backendPutString(defaultBucket, "test/yep", nil, "hello")
	cli.assertLsFiles(defaultBucket, "",
		[]string{"test/"}, []string{"no-match", "test-one", "test-two"})

	// "test-one" and "test-two" and the directory "test" should pass the prefix match:
	cli.assertLsFiles(defaultBucket, "test",
		[]string{"test/"}, []string{"test-one", "test-two"})

	// listing with a trailing slash should list the directory contents:
	cli.assertLsFiles(defaultBucket, "test/",
		nil, []string{"yep"})
}

func TestCLIRmOne(t *testing.T) {
	cli := newTestCLI(t)
	defer cli.Close()

	cli.backendPutString(defaultBucket, "foo", nil, "hello")
	cli.backendPutString(defaultBucket, "bar", nil, "hello")
	cli.assertLsFiles(defaultBucket, "", nil, []string{"foo", "bar"})

	cli.rm(cli.fileArg(defaultBucket, "foo"))
	cli.assertLsFiles(defaultBucket, "", nil, []string{"bar"})
}

func TestCLIRmMulti(t *testing.T) {
	cli := newTestCLI(t)
	defer cli.Close()

	cli.backendPutString(defaultBucket, "foo", nil, "hello")
	cli.backendPutString(defaultBucket, "bar", nil, "hello")
	cli.backendPutString(defaultBucket, "baz", nil, "hello")
	cli.assertLsFiles(defaultBucket, "", nil, []string{"foo", "bar", "baz"})

	cli.rmMulti(defaultBucket, "foo", "bar", "baz")
	cli.assertLsFiles(defaultBucket, "", nil, nil)
}

func TestCLIDownload(t *testing.T) {
	// NOTE: this must be set to the largest value you plan to test in the test cases.
	var source = randomFileBody(100000000)

	for _, tc := range []struct {
		in []byte
	}{
		{in: nil},
		{in: source[:1]},

		// FIXME: Beyond a certain size, the AWS client switches to using range
		// requests and downloads several parts in parallel. This takes a stab
		// at what that size is, but it isn't an especially robust way to
		// determine what the spill point is:
		{in: source[:1000000]},
		{in: source[:10000000]},
		{in: source[:100000000]},
	} {
		t.Run("", func(t *testing.T) {
			cli := newTestCLI(t)
			defer cli.Close()

			cli.backendPutBytes(defaultBucket, "foo", nil, tc.in)
			out := cli.download(defaultBucket, "foo")
			if !bytes.Equal(out, tc.in) {
				t.Fatal()
			}
		})
	}
}

type testCLI struct {
	*testServer
}

func newTestCLI(t *testing.T, options ...testServerOption) *testCLI {
	return &testCLI{newTestServer(t, options...)}
}

func (tc *testCLI) command(method string, subcommand string, args ...string) *exec.Cmd {
	tc.Helper()

	if method != "s3" && method != "s3api" {
		panic("expected 's3' or 's3api'")
	}

	cmdArgs := append([]string{
		"--output", "json",
		method,
		"--endpoint", tc.server.URL,
		subcommand,
	}, args...)

	cmd := exec.Command("aws", cmdArgs...)

	log.Println("cli args:", cmdArgs)

	cmd.Env = []string{
		"AWS_ACCESS_KEY_ID=key",
		"AWS_SECRET_ACCESS_KEY=secret",
	}
	return cmd
}

func (tc *testCLI) run(method string, subcommand string, args ...string) {
	tc.Helper()
	err := tc.command(method, subcommand, args...).Run()
	if _, ok := err.(*exec.Error); ok {
		tc.Skip("aws cli not found on $PATH")
	}
	tc.OK(err)
}

func (tc *testCLI) output(method string, subcommand string, args ...string) (out []byte) {
	tc.Helper()
	out, err := tc.command(method, subcommand, args...).Output()
	if _, ok := err.(*exec.Error); ok {
		tc.Skip("aws cli not found on $PATH")
	}
	tc.OK(err)
	return out
}

func (tc *testCLI) combinedOutput(method string, subcommand string, args ...string) (out []byte) {
	tc.Helper()
	out, err := tc.command(method, subcommand, args...).CombinedOutput()
	if _, ok := err.(*exec.Error); ok {
		tc.Skip("aws cli not found on $PATH")
	}
	tc.OK(err)
	return out
}

var cliLsDirMatcher = regexp.MustCompile(`^\s*PRE (.*)$`)

func (tc *testCLI) assertLsFiles(bucket string, prefix string, dirs []string, files []string) (items lsItems) {
	tc.Helper()
	items = tc.lsFiles(bucket, prefix)
	items.assertContents(tc.TT, dirs, files)
	return items
}

func (tc *testCLI) lsFiles(bucket string, prefix string) (items lsItems) {
	tc.Helper()

	prefix = strings.TrimLeft(prefix, "/")
	out := tc.combinedOutput("s3", "ls", fmt.Sprintf("s3://%s/%s", bucket, prefix))

	scn := bufio.NewScanner(bytes.NewReader(out))
	for scn.Scan() {
		cur := scn.Text()
		dir := cliLsDirMatcher.FindStringSubmatch(cur)
		if dir != nil {
			items = append(items, lsItem{
				isDir: true,
				name:  dir[1], // first submatch
			})

		} else { // file matching
			var ct cliTime
			var item lsItem
			tc.OKAll(fmt.Sscan(scn.Text(), &ct, &item.size, &item.name))
			item.date = time.Time(ct)
			items = append(items, item)
		}
	}

	return items
}

func (tc *testCLI) lsBuckets() (buckets gofakes3.Buckets) {
	tc.Helper()

	out := tc.combinedOutput("s3", "ls")

	scn := bufio.NewScanner(bytes.NewReader(out))
	for scn.Scan() {
		var ct cliTime
		var b gofakes3.BucketInfo
		tc.OKAll(fmt.Sscan(scn.Text(), &ct, &b.Name))
		b.CreationDate = ct.contentTime()
		buckets = append(buckets, b)
	}

	return buckets
}

func (tc *testCLI) download(bucket, object string) []byte {
	tc.Helper()
	return tc.combinedOutput("s3", "cp", fmt.Sprintf("s3://%s/%s", bucket, object), "-")
}

func (tc *testCLI) rmMulti(bucket string, objects ...string) {
	tc.Helper()

	// delete-objects --bucket fakes3 --delete 'Objects=[{Key=test},{Key=test2}]'

	var delArg struct{ Objects []gofakes3.ObjectID }
	for _, obj := range objects {
		delArg.Objects = append(delArg.Objects, gofakes3.ObjectID{Key: obj})
	}
	bts, err := json.Marshal(delArg)
	if err != nil {
		panic(err)
	}

	args := []string{
		"--bucket", bucket,
		"--delete", string(bts),
	}
	tc.run("s3api", "delete-objects", args...)
}

func (tc *testCLI) rm(fileURL string) {
	tc.Helper()
	tc.run("s3", "rm", fileURL)
}

func (tc *testCLI) fileArg(bucket string, file string) string {
	return fmt.Sprintf("s3://%s", path.Join(bucket, file))
}

func (tc *testCLI) fileArgs(bucket string, files ...string) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = tc.fileArg(bucket, f)
	}
	return out
}

type cliTime time.Time

func (c cliTime) contentTime() gofakes3.ContentTime {
	return gofakes3.NewContentTime(time.Time(c))
}

func (c *cliTime) Scan(state fmt.ScanState, verb rune) error {
	d, err := state.Token(false, nil)
	if err != nil {
		return err
	}
	ds := string(d)

	t, err := state.Token(true, nil)
	if err != nil {
		return err
	}
	ts := string(t)

	// CLI returns time in the machine's timezone:
	tv, err := time.ParseInLocation("2006-01-01 15:04:05", ds+" "+ts, time.Local)
	if err != nil {
		return err
	}

	*c = cliTime(tv.In(time.UTC))

	return nil
}
